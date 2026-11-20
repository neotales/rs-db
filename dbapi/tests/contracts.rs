//! Public contract tests for the driver-neutral API.
use std::{
    collections::VecDeque,
    future::{Future, pending, ready as ready_future},
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Cancellation, Capabilities, DbError, DbFuture, DbType, Delay, ErrorKind,
    Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, Reader, RetryPolicy, RetryRuntime, Row,
    RowLimit, Session, Transaction, TxOptions, Value,
};

fn assert_send<T: Send>(_: &T) {}
fn assert_future_send<T: Future + Send>(_: T) {}
fn block_on<T, F: Future<Output = Result<T, DbError>>>(future: F) -> Result<T, DbError> {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..10_000 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
    }
    panic!("mock future did not resolve");
}
fn ready<T>(future: impl Future<Output = Result<T, DbError>>) -> T {
    block_on(future).unwrap()
}

struct MockPool {
    drops: Arc<AtomicUsize>,
}
impl Pool for MockPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            ..Capabilities::default()
        }
    }
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let drops = Arc::clone(&self.drops);
        Box::pin(async move { Ok(Box::new(MockSession { drops }) as Box<dyn Session>) })
    }
}
struct MockSession {
    drops: Arc<AtomicUsize>,
}
impl Session for MockSession {
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async move {
            std::future::ready(()).await;
            Ok(u64::from(
                !(sql.is_empty() && options.parameters.is_empty()),
            ))
        })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async { Ok(Box::new(BufferedReader::new(Vec::new())) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let drops = Arc::clone(&self.drops);
        Box::pin(async move {
            Ok(Box::new(DropProbe {
                drops,
                _borrow: PhantomData,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

struct DropProbe<'session> {
    drops: Arc<AtomicUsize>,
    _borrow: PhantomData<&'session mut ()>,
}
impl Drop for DropProbe<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl<'session> Transaction<'session> for DropProbe<'session> {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async { Ok(Box::new(BufferedReader::new(Vec::new())) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn traits_are_object_safe_and_futures_send() {
    let pool: Arc<dyn Pool> = Arc::new(MockPool {
        drops: Arc::new(AtomicUsize::new(0)),
    });
    assert!(pool.capabilities().transactions);
    assert_send(&pool);
    assert_future_send(pool.acquire_async());
    let mut session = MockSession {
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let parameter = Parameter::new(DbType::I64, Value::I64(9)).unwrap();
    let statement = String::from("SELECT $1");
    assert_future_send(session.execute_async(&statement, QueryOptions::new().bind(parameter)));
}

#[test]
fn row_access_reports_missing_ambiguous_null_type_and_overflow() {
    let row = Row::new(vec![
        ("same".into(), Value::I64(i64::MAX)),
        ("same".into(), Value::Null(DbType::Text)),
        ("flag".into(), Value::Bool(true)),
    ])
    .unwrap();
    assert_eq!(
        row.get::<i64>("absent").unwrap_err().kind(),
        ErrorKind::Missing
    );
    assert_eq!(
        row.get::<i64>("same").unwrap_err().kind(),
        ErrorKind::Ambiguous
    );
    assert_eq!(
        row.get_ordinal::<i32>(0).unwrap_err().kind(),
        ErrorKind::Overflow
    );
    assert_eq!(
        row.get::<Option<String>>("same").unwrap_err().kind(),
        ErrorKind::Ambiguous
    );
    assert_eq!(
        row.get::<i64>("flag").unwrap_err().kind(),
        ErrorKind::WrongType
    );
    assert!(row.get::<bool>("flag").unwrap());
}

#[test]
fn typed_null_and_float_validation_are_exact() {
    let typed_null = Parameter::new(DbType::Text, Value::Null(DbType::Text)).unwrap();
    assert_eq!(typed_null.db_type(), DbType::Text);
    assert_eq!(typed_null.value(), &Value::Null(DbType::Text));
    assert_eq!(
        Parameter::new(DbType::I64, Value::Null(DbType::Text))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        Parameter::new(DbType::F64, Value::F64(f64::NAN))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    let row = Row::new(vec![
        ("text".into(), Value::Null(DbType::Text)),
        ("int".into(), Value::Null(DbType::I64)),
    ])
    .unwrap();
    assert_eq!(row.get::<Option<String>>("text").unwrap(), None);
    assert_eq!(
        row.require::<String>("text").unwrap_err().kind(),
        ErrorKind::Null
    );
    assert_eq!(
        row.require::<String>("int").unwrap_err().kind(),
        ErrorKind::WrongType
    );
    assert_eq!(row.get_ordinal::<Option<String>>(0).unwrap(), None);
    assert_eq!(
        row.get::<Option<String>>("int").unwrap_err().kind(),
        ErrorKind::WrongType
    );
}

#[test]
fn limits_require_positive_hard_bounds() {
    assert!(RowLimit::new(1, 1).is_ok());
    assert_eq!(
        RowLimit::new(0, 1).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        RowLimit::new(1, 0).unwrap_err().kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        RowLimit::new(0, 1).unwrap_err().reason(),
        Some(neotales_dbapi::InvalidArgumentReason::ZeroFetchRowLimit)
    );
    assert_eq!(
        RowLimit::new(1, 0).unwrap_err().reason(),
        Some(neotales_dbapi::InvalidArgumentReason::ZeroByteLimit)
    );
    let multi = RowLimit::with_max_sets(2, 3, 4).unwrap();
    assert_eq!((multi.rows(), multi.bytes(), multi.max_sets()), (2, 3, 4));
    assert_eq!(RowLimit::new(2, 3).unwrap().max_sets(), 1);
    assert_eq!(
        RowLimit::with_max_sets(1, 1, 0).unwrap_err().reason(),
        Some(neotales_dbapi::InvalidArgumentReason::ZeroSetLimit)
    );
    assert_eq!(
        Row::new(vec![("x".into(), Value::Bool(true))])
            .unwrap()
            .encoded_bytes()
            .unwrap(),
        2
    );
}

#[test]
fn dropping_an_unfinished_transaction_releases_its_owned_handle() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = MockPool {
        drops: Arc::clone(&drops),
    };
    let mut session = ready(pool.acquire_async());
    let transaction = ready(session.begin_async(TxOptions::default()));
    drop(transaction);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn errors_never_retain_driver_details() {
    let error = DbError::new(ErrorKind::Connection);
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("secret"));
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn unknown_write_and_commit_outcomes_are_distinct_and_sanitized() {
    let write = DbError::new(ErrorKind::WriteOutcomeUnknown);
    let commit = DbError::new(ErrorKind::CommitOutcomeUnknown);
    assert_eq!(write.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(commit.kind(), ErrorKind::CommitOutcomeUnknown);
    assert_ne!(write.kind(), commit.kind());
    for error in [write, commit] {
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(error.reason(), None);
    }
    assert!(format!("{write:?} {write}").contains("WriteOutcomeUnknown"));
    assert!(format!("{commit:?} {commit}").contains("CommitOutcomeUnknown"));
}

#[test]
fn invalid_argument_reasons_are_static_and_safe() {
    let error = Parameter::new(DbType::Text, Value::I64(123_456)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(
        error.reason(),
        Some(neotales_dbapi::InvalidArgumentReason::ParameterTypeMismatch)
    );
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("123_456"));
}

struct ScriptedReader {
    remaining: usize,
    late: Option<ErrorKind>,
}
impl ScriptedReader {
    fn new(remaining: usize, late: Option<ErrorKind>) -> Self {
        Self { remaining, late }
    }
    fn row(value: i64) -> Row {
        Row::new(vec![("value".into(), Value::I64(value))]).unwrap()
    }
}
impl Reader for ScriptedReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Box::pin(async { Ok(Some(ScriptedReader::row(7))) })
        } else if let Some(kind) = self.late.take() {
            Box::pin(async move { Err(DbError::new(kind)) })
        } else {
            Box::pin(async { Ok(None) })
        }
    }
    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.remaining = 0;
        Box::pin(async { Ok(false) })
    }
    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.remaining = 0;
        match self.late.take() {
            Some(kind) => Box::pin(async move { Err(DbError::new(kind)) }),
            None => Box::pin(async { Ok(()) }),
        }
    }
}

struct ScriptedSession {
    rows: usize,
    late: Option<ErrorKind>,
}
impl Session for ScriptedSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let reader = ScriptedReader::new(self.rows, self.late);
        Box::pin(async move { Ok(Box::new(reader) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

fn scripted_session(rows: usize, late: Option<ErrorKind>) -> ScriptedSession {
    ScriptedSession { rows, late }
}

#[test]
fn row_read_aliases_preserve_exact_typed_conversion() {
    let row = Row::new(vec![
        ("value".into(), Value::I64(7)),
        ("text".into(), Value::Null(DbType::Text)),
    ])
    .unwrap();
    assert_eq!(row.read::<i64>("value").unwrap(), 7);
    assert_eq!(row.read_at::<i64>(0).unwrap(), 7);
    assert_eq!(
        row.read::<i64>("absent").unwrap_err().kind(),
        ErrorKind::Missing
    );
    assert_eq!(
        row.read_at::<String>(1).unwrap_err().kind(),
        ErrorKind::Null
    );
    assert_eq!(
        row.read_at::<i64>(1).unwrap_err().kind(),
        ErrorKind::WrongType
    );
    assert_eq!(row.read_at::<Option<String>>(1).unwrap(), None);
    assert_eq!(row.value_at(0).unwrap(), Value::I64(7));
    assert_eq!(row.value_at(2).unwrap_err().kind(), ErrorKind::Missing);
}

#[test]
fn buffered_reader_finishes_and_has_next_set_advances() {
    let mut reader = BufferedReader::new(vec![
        Row::new(vec![("value".into(), Value::I64(1))]).unwrap(),
        Row::new(vec![("value".into(), Value::I64(2))]).unwrap(),
    ]);
    assert!(ready(Box::pin(async { reader.next_row_async().await })).is_some());
    // Advancing discards the unread second row and never invents another set.
    assert!(!ready(Box::pin(async {
        reader.has_next_set_async().await
    })));
    assert!(ready(Box::pin(async { reader.next_row_async().await })).is_none());
    ready(Box::pin(async { reader.finish_async().await }));
}

struct MultiSetReader {
    sets: VecDeque<VecDeque<Row>>,
    terminal: Option<ErrorKind>,
}
impl MultiSetReader {
    fn new(sets: Vec<Vec<Row>>, terminal: Option<ErrorKind>) -> Self {
        Self {
            sets: sets.into_iter().map(VecDeque::from).collect(),
            terminal,
        }
    }
}
impl Reader for MultiSetReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        let row = self
            .sets
            .front_mut()
            .and_then(std::collections::VecDeque::pop_front);
        Box::pin(async move { Ok(row) })
    }
    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.sets.pop_front();
        let more = !self.sets.is_empty();
        Box::pin(async move { Ok(more) })
    }
    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.sets.clear();
        match self.terminal.take() {
            Some(kind) => Box::pin(async move { Err(DbError::new(kind)) }),
            None => Box::pin(async { Ok(()) }),
        }
    }
}

#[test]
fn multiframe_readers_advance_discard_and_surface_late_errors() {
    let mut reader = MultiSetReader::new(
        vec![
            vec![ScriptedReader::row(1), ScriptedReader::row(2)],
            vec![ScriptedReader::row(3)],
        ],
        None,
    );
    // Reading one row then advancing discards the unread row of the first set.
    assert_eq!(
        ready(Box::pin(async { reader.next_row_async().await }))
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        1
    );
    assert!(ready(Box::pin(async { reader.has_next_set_async().await })));
    assert_eq!(
        ready(Box::pin(async { reader.next_row_async().await }))
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        3
    );
    assert!(!ready(Box::pin(async {
        reader.has_next_set_async().await
    })));
    ready(Box::pin(async { reader.finish_async().await }));

    let mut reader = MultiSetReader::new(
        vec![vec![ScriptedReader::row(1)], vec![ScriptedReader::row(2)]],
        Some(ErrorKind::Busy),
    );
    assert_eq!(
        block_on(reader.read_all_async()).unwrap_err().kind(),
        ErrorKind::Busy
    );
}

#[derive(Clone)]
struct MultiSetProvider {
    sets: Vec<Vec<Row>>,
    terminal: Option<ErrorKind>,
}
struct MultiSetSession(MultiSetProvider);

impl Pool for MultiSetProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let provider = self.clone();
        Box::pin(async move { Ok(Box::new(MultiSetSession(provider)) as Box<dyn Session>) })
    }
}
impl Session for MultiSetSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let limit = options.resolved_limit(neotales_dbapi::PoolDefaults::default());
        if self.0.sets.len() > limit.max_sets() {
            return Box::pin(async { Err(DbError::new(ErrorKind::Overflow)) });
        }
        let reader = MultiSetReader::new(self.0.sets.clone(), self.0.terminal);
        Box::pin(async move { Ok(Box::new(reader) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let sets = self.0.sets.clone();
        let terminal = self.0.terminal;
        Box::pin(async move {
            Ok(Box::new(MultiSetTransaction { sets, terminal }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

struct MultiSetTransaction {
    sets: Vec<Vec<Row>>,
    terminal: Option<ErrorKind>,
}
impl<'session> Transaction<'session> for MultiSetTransaction {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let limit = options.resolved_limit(neotales_dbapi::PoolDefaults::default());
        if self.sets.len() > limit.max_sets() {
            return Box::pin(async { Err(DbError::new(ErrorKind::Overflow)) });
        }
        let reader = MultiSetReader::new(self.sets.clone(), self.terminal);
        Box::pin(async move { Ok(Box::new(reader) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn row(value: i64) -> Row {
    ScriptedReader::row(value)
}

#[test]
fn wrappers_preserve_result_set_boundaries() {
    let provider = MultiSetProvider {
        sets: vec![vec![row(1), row(2)], vec![], vec![row(3)]],
        terminal: None,
    };
    let limit = RowLimit::with_max_sets(10, 1024, 3).unwrap();
    let mut reader = block_on(provider.fetch_async("", QueryOptions::new().limit(limit))).unwrap();
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        1
    );
    // Advancing discards the unread row in set one, then exposes an empty set two.
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert!(block_on(reader.next_row_async()).unwrap().is_none());
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        3
    );
    assert!(!block_on(reader.has_next_set_async()).unwrap());
    block_on(reader.finish_async()).unwrap();
    drop(reader);

    // The policy wrapper uses the same boundary-preserving buffered reader.
    let policy = PolicyPool::new(provider, PoolPolicy::default());
    let mut reader = block_on(policy.fetch_async("", QueryOptions::new().limit(limit))).unwrap();
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        1
    );
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert!(block_on(reader.next_row_async()).unwrap().is_none());
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        3
    );
    assert!(!block_on(reader.has_next_set_async()).unwrap());
    block_on(reader.finish_async()).unwrap();

    // Transactions use the same policy buffering path.
    let mut session = block_on(policy.acquire_async()).unwrap();
    let mut transaction = block_on(session.begin_async(TxOptions::default())).unwrap();
    let mut reader =
        block_on(transaction.fetch_async("", QueryOptions::new().limit(limit))).unwrap();
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        1
    );
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert!(block_on(reader.next_row_async()).unwrap().is_none());
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        3
    );
    assert!(!block_on(reader.has_next_set_async()).unwrap());
    block_on(reader.finish_async()).unwrap();
    drop(reader);
    block_on(transaction.rollback_async()).unwrap();
}

struct RetrySetProvider {
    attempts: Arc<AtomicUsize>,
    sets: Vec<Vec<Row>>,
}
struct RetrySetSession {
    attempts: Arc<AtomicUsize>,
    sets: Vec<Vec<Row>>,
}
impl Pool for RetrySetProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let attempts = Arc::clone(&self.attempts);
        let sets = self.sets.clone();
        Box::pin(
            async move { Ok(Box::new(RetrySetSession { attempts, sets }) as Box<dyn Session>) },
        )
    }
}
impl Session for RetrySetSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let limit = options.resolved_limit(neotales_dbapi::PoolDefaults::default());
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Box::pin(async { Err(DbError::new(ErrorKind::Busy)) });
        }
        if self.sets.len() > limit.max_sets() {
            return Box::pin(async { Err(DbError::new(ErrorKind::Overflow)) });
        }
        let reader = MultiSetReader::new(self.sets.clone(), None);
        Box::pin(async move { Ok(Box::new(reader) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

#[test]
fn policy_retry_preserves_set_boundaries() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let provider = RetrySetProvider {
        attempts: Arc::clone(&attempts),
        sets: vec![vec![row(1)], vec![], vec![row(2)]],
    };
    let policy = PolicyPool::new(
        provider,
        PoolPolicy {
            retry: RetryPolicy::new(2, Delay::Fixed(Duration::ZERO), false).unwrap(),
            retry_runtime: Some(RetryRuntime::new(
                Arc::new(Cancellation::new()),
                || Box::pin(pending()),
                |_| Box::pin(ready_future(())),
                || 0,
            )),
            ..PoolPolicy::default()
        },
    );
    let limit = RowLimit::with_max_sets(10, 1024, 3).unwrap();
    let mut reader =
        block_on(policy.fetch_async("", QueryOptions::new().replay_safe().limit(limit))).unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        1
    );
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert!(block_on(reader.next_row_async()).unwrap().is_none());
    assert!(block_on(reader.has_next_set_async()).unwrap());
    assert_eq!(
        block_on(reader.next_row_async())
            .unwrap()
            .unwrap()
            .read::<i64>("value")
            .unwrap(),
        2
    );
    assert!(!block_on(reader.has_next_set_async()).unwrap());
    block_on(reader.finish_async()).unwrap();
}

struct UncheckedSetPool(Vec<Vec<Row>>);
struct UncheckedSetSession(Vec<Vec<Row>>);
impl Pool for UncheckedSetPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let sets = self.0.clone();
        Box::pin(async move { Ok(Box::new(UncheckedSetSession(sets)) as Box<dyn Session>) })
    }
}
impl Session for UncheckedSetSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(0) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let reader = MultiSetReader::new(self.0.clone(), None);
        Box::pin(async move { Ok(Box::new(reader) as Box<dyn Reader>) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

#[test]
fn buffering_enforces_set_limits_even_when_a_provider_does_not() {
    let limit = RowLimit::with_max_sets(10, 1024, 1).unwrap();
    let pool = UncheckedSetPool(vec![vec![], vec![]]);
    assert_eq!(
        block_on(pool.fetch_async("", QueryOptions::new().limit(limit)))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Overflow
    );
    let policy = PolicyPool::new(pool, PoolPolicy::default());
    assert_eq!(
        block_on(policy.fetch_async("", QueryOptions::new().limit(limit)))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Overflow
    );
}

#[test]
fn set_limits_count_empty_sets_and_cardinality_uses_the_first_set() {
    let limit = RowLimit::with_max_sets(10, 1024, 1).unwrap();
    let two_empty = MultiSetProvider {
        sets: vec![vec![], vec![]],
        terminal: None,
    };
    assert_eq!(
        block_on(two_empty.fetch_async("", QueryOptions::new().limit(limit)))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Overflow
    );

    let mut session = MultiSetSession(MultiSetProvider {
        sets: vec![vec![row(1)], vec![row(2)]],
        terminal: None,
    });
    let first_row = block_on(session.fetch_one_async(
        "",
        QueryOptions::new().limit(RowLimit::with_max_sets(10, 1024, 2).unwrap()),
    ))
    .unwrap();
    assert_eq!(first_row.read::<i64>("value").unwrap(), 1);

    let mut session = MultiSetSession(MultiSetProvider {
        sets: vec![vec![], vec![row(2)]],
        terminal: None,
    });
    assert_eq!(
        block_on(session.fetch_one_async(
            "",
            QueryOptions::new().limit(RowLimit::with_max_sets(10, 1024, 2).unwrap()),
        ))
        .unwrap_err()
        .kind(),
        ErrorKind::NoRows
    );

    let mut session = MultiSetSession(MultiSetProvider {
        sets: vec![vec![row(1), row(2)], vec![row(3)]],
        terminal: None,
    });
    assert_eq!(
        block_on(session.fetch_one_async(
            "",
            QueryOptions::new().limit(RowLimit::with_max_sets(10, 1024, 2).unwrap()),
        ))
        .unwrap_err()
        .kind(),
        ErrorKind::TooManyRows
    );
}

#[test]
fn later_set_errors_survive_convenience_and_policy_drain() {
    let provider = MultiSetProvider {
        sets: vec![vec![row(1)], vec![row(2)]],
        terminal: Some(ErrorKind::Busy),
    };
    let limit = RowLimit::with_max_sets(10, 1024, 2).unwrap();
    let mut session = MultiSetSession(provider.clone());
    assert_eq!(
        block_on(session.fetch_first_async("", QueryOptions::new().limit(limit)))
            .unwrap_err()
            .kind(),
        ErrorKind::Busy
    );
    let mut session = MultiSetSession(provider.clone());
    assert_eq!(
        block_on(session.fetch_one_async("", QueryOptions::new().limit(limit)))
            .unwrap_err()
            .kind(),
        ErrorKind::Busy
    );
    let mut session = MultiSetSession(provider.clone());
    assert_eq!(
        block_on(session.fetch_scalar_async("", QueryOptions::new().limit(limit)))
            .unwrap_err()
            .kind(),
        ErrorKind::Busy
    );
    let policy = PolicyPool::new(provider, PoolPolicy::default());
    assert_eq!(
        block_on(policy.fetch_async("", QueryOptions::new().limit(limit)))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Busy
    );
}

#[test]
fn convenience_cardinality_and_late_errors_are_reported() {
    let mut session = scripted_session(0, None);
    assert_eq!(
        block_on(
            session.fetch_one_async("", QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()))
        )
        .unwrap_err()
        .kind(),
        ErrorKind::NoRows
    );
    assert_eq!(
        block_on(
            session
                .fetch_scalar_async("", QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()))
        )
        .unwrap_err()
        .kind(),
        ErrorKind::NoRows
    );

    let mut session = scripted_session(2, None);
    assert_eq!(
        block_on(
            session.fetch_one_async("", QueryOptions::new().limit(RowLimit::new(2, 64).unwrap()))
        )
        .unwrap_err()
        .kind(),
        ErrorKind::TooManyRows
    );

    let mut session = scripted_session(1, Some(ErrorKind::Busy));
    assert_eq!(
        block_on(
            session.fetch_first_async("", QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()))
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Busy
    );

    let mut session = scripted_session(1, None);
    let scalar = block_on(
        session.fetch_scalar_async("", QueryOptions::new().limit(RowLimit::new(1, 64).unwrap())),
    )
    .unwrap();
    assert_eq!(scalar, Value::I64(7));
    let first = block_on(
        session.fetch_first_async("", QueryOptions::new().limit(RowLimit::new(1, 64).unwrap())),
    )
    .unwrap()
    .unwrap();
    assert_eq!(first.read::<i64>("value").unwrap(), 7);
}
