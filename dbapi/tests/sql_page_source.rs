//! Contract tests for the caller-authored SQL page source.
//!
//! The fake session returns scripted single-result-set batches and records the exact statement,
//! typed parameters, and options the adapter sent. It is not a provider conformance test; the live
//! SQLite example and integration test live in `neotales-dbapi-sqlite`.

use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    BufferedReader, DbError, DbFuture, DbType, Delay, ErrorKind, LimitSetting, Parameter,
    QueryOptions, Reader, RetryPolicy, RetrySetting, Row, RowLimit, Session, Transaction,
    TxOptions, Value,
    paging::{
        PageSize,
        multi::{
            Continuation, PageBudget, PageRequest, PageSourceStream, PagedPage,
            sql::{SqlPageAssertion, SqlPagePlan, SqlPageSource},
        },
    },
};

const SQL: &str = "SELECT tenant, seq, body FROM events \
                   WHERE (tenant, seq) > ($1, $2) ORDER BY tenant, seq LIMIT $3";

#[derive(Debug)]
struct Recorded {
    sql: String,
    options: QueryOptions,
}

#[derive(Debug)]
enum Response {
    Sets(Vec<Vec<Row>>),
    NoRowsDefault,
    FinishError(Vec<Row>),
    FetchError(ErrorKind),
}

struct FakeSession {
    responses: VecDeque<Response>,
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl FakeSession {
    fn new(responses: Vec<Response>) -> (Self, Arc<Mutex<Vec<Recorded>>>) {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                responses: VecDeque::from(responses),
                recorded: Arc::clone(&recorded),
            },
            recorded,
        )
    }
}

impl Session for FakeSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.recorded.lock().expect("recordings").push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let response = self.responses.pop_front().expect("scripted response");
        Box::pin(async move { respond(response) })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

fn respond(response: Response) -> Result<Box<dyn Reader>, DbError> {
    match response {
        Response::Sets(sets) => Ok(Box::new(BufferedReader::from_sets(sets))),
        Response::NoRowsDefault => Ok(Box::new(NoRowsReader)),
        Response::FinishError(rows) => Ok(Box::new(FinishErrorReader {
            inner: BufferedReader::new(rows),
        })),
        Response::FetchError(kind) => Err(DbError::new(kind)),
    }
}

struct BorrowedSession<'a> {
    responses: &'a Mutex<VecDeque<Response>>,
    recorded: &'a Mutex<Vec<Recorded>>,
}

impl Session for BorrowedSession<'_> {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.recorded.lock().expect("recordings").push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let response = self
            .responses
            .lock()
            .expect("responses")
            .pop_front()
            .expect("scripted response");
        Box::pin(async move { respond(response) })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

/// A reader with no rows and no next set. It deliberately uses the default `read_sets_async`,
/// which reports one empty set; this is the honest shape of a no-row batch and cannot distinguish
/// absence of a result set.
struct NoRowsReader;

impl Reader for NoRowsReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        Box::pin(async { Ok(None) })
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct FinishErrorReader {
    inner: BufferedReader,
}

impl Reader for FinishErrorReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        self.inner.next_row_async()
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Connection)) })
    }
}

#[derive(Debug)]
struct Token {
    source: u64,
    after: (i64, i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Behavior {
    Normal,
    NoContinuationOnLookahead,
    ContinuationWithoutLookahead,
    Retry,
    RetryOff,
    RetryDisabled,
    Limit,
}

struct Plan {
    source: u64,
    start: (i64, i64),
    behavior: Behavior,
    sql: &'static str,
}

impl Plan {
    fn new(behavior: Behavior) -> Self {
        Self::with_sql(behavior, SQL)
    }

    fn with_sql(behavior: Behavior, sql: &'static str) -> Self {
        Self {
            source: 1,
            start: (0, 0),
            behavior,
            sql,
        }
    }

    fn resolve(&self, continuation: Option<Continuation>) -> Result<(i64, i64), DbError> {
        let Some(token) = continuation else {
            return Ok(self.start);
        };
        let token = token
            .downcast::<Token>()
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
        if token.source != self.source {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(token.after)
    }

    fn issue(&self, after: (i64, i64)) -> Continuation {
        Continuation::new(Token {
            source: self.source,
            after,
        })
    }
}

impl SqlPagePlan for Plan {
    fn sql(&self) -> &str {
        self.sql
    }

    fn options(
        &mut self,
        _: PageRequest,
        lookahead: usize,
        continuation: Option<Continuation>,
    ) -> Result<QueryOptions, DbError> {
        let after = self.resolve(continuation)?;
        let parameters = [
            i64_param(after.0)?,
            i64_param(after.1)?,
            i64_param(i64::try_from(lookahead).map_err(|_| DbError::new(ErrorKind::Overflow))?)?,
        ];
        let options = QueryOptions::new().bind_all(&parameters);
        match self.behavior {
            Behavior::Retry => Ok(options.retry(RetrySetting::Use(
                RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(1)), false)
                    .expect("valid retry policy"),
            ))),
            Behavior::RetryOff => Ok(options.retry(RetrySetting::Off)),
            Behavior::RetryDisabled => Ok(options.retry(RetrySetting::Use(RetryPolicy::Disabled))),
            Behavior::Limit => Ok(options.limit(RowLimit::new(99, 65_536)?)),
            _ => Ok(options),
        }
    }

    fn next_continuation(
        &mut self,
        rows: &[Row],
        has_lookahead: bool,
    ) -> Result<Option<Continuation>, DbError> {
        if self.behavior == Behavior::NoContinuationOnLookahead {
            return Ok(None);
        }
        if !has_lookahead && self.behavior != Behavior::ContinuationWithoutLookahead {
            return Ok(None);
        }
        let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
        Ok(Some(self.issue((
            last.read::<i64>("tenant")?,
            last.read::<i64>("seq")?,
        ))))
    }
}

fn i64_param(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}

fn row(tenant: i64, seq: i64, body: &str) -> Row {
    Row::new(vec![
        ("tenant".to_owned(), Value::I64(tenant)),
        ("seq".to_owned(), Value::I64(seq)),
        ("body".to_owned(), Value::Text(body.to_owned())),
    ])
    .expect("valid row")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn budget(rows: usize, bytes: usize, pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, pages).expect("valid page budget")
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..10_000 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
    panic!("scripted future did not resolve");
}

fn stream(
    behavior: Behavior,
    responses: Vec<Response>,
    page_rows: usize,
    budget_rows: usize,
    bytes: usize,
    pages: usize,
) -> (PageSourceStream<'static>, Arc<Mutex<Vec<Recorded>>>) {
    let (session, recorded) = FakeSession::new(responses);
    let source = SqlPageSource::new(
        Box::new(session),
        Plan::new(behavior),
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    (
        PageSourceStream::new(
            Box::new(source),
            page_size(page_rows),
            budget(budget_rows, bytes, pages),
        ),
        recorded,
    )
}

fn values(page: &PagedPage) -> Vec<(i64, i64)> {
    page.rows()
        .iter()
        .map(|row| {
            (
                row.read::<i64>("tenant").expect("tenant"),
                row.read::<i64>("seq").expect("seq"),
            )
        })
        .collect()
}

#[test]
fn passes_provider_sql_and_typed_parameters_unchanged() {
    let (stream, recorded) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![vec![row(1, 1, "a"), row(1, 2, "b")]])],
        2,
        10,
        65_536,
        10,
    );

    let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(values(&pages[0]), vec![(1, 1), (1, 2)]);

    let recorded = recorded.lock().expect("recordings");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].sql, SQL);
    assert_eq!(
        recorded[0].options.parameters,
        vec![
            i64_param(0).unwrap(),
            i64_param(0).unwrap(),
            i64_param(3).unwrap()
        ]
    );
    match recorded[0].options.limit {
        LimitSetting::Set(limit) => {
            assert_eq!(limit.rows(), 3);
            assert_eq!(limit.bytes(), 4_096);
            assert_eq!(limit.max_sets(), 1);
        }
        LimitSetting::Inherit => panic!("the adapter must set a hard limit"),
    }
    assert_eq!(recorded[0].options.retry, RetrySetting::Off);
}

#[test]
fn source_accepts_a_session_borrowing_caller_state() {
    let responses = Mutex::new(VecDeque::from(vec![Response::Sets(vec![vec![row(
        1, 1, "a",
    )]])]));
    let recorded = Mutex::new(Vec::new());
    let source = SqlPageSource::new(
        Box::new(BorrowedSession {
            responses: &responses,
            recorded: &recorded,
        }),
        Plan::new(Behavior::Normal),
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");

    let pages: Vec<PagedPage> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10)).try_collect(),
    )
    .expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(recorded.lock().expect("recordings").len(), 1);
}

#[test]
fn publishes_only_the_page_and_carries_one_lookahead() {
    let (stream, recorded) = stream(
        Behavior::Normal,
        vec![
            Response::Sets(vec![vec![row(1, 1, "a"), row(1, 2, "b"), row(1, 3, "c")]]),
            Response::Sets(vec![vec![row(1, 3, "c")]]),
        ],
        2,
        10,
        65_536,
        10,
    );

    let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
    assert_eq!(pages.len(), 2);
    assert_eq!(values(&pages[0]), vec![(1, 1), (1, 2)]);
    assert_eq!(values(&pages[1]), vec![(1, 3)]);

    let recorded = recorded.lock().expect("recordings");
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].options.parameters[2], i64_param(3).unwrap());
    assert_eq!(recorded[1].options.parameters[0], i64_param(1).unwrap());
    assert_eq!(recorded[1].options.parameters[1], i64_param(2).unwrap());
    assert_eq!(recorded[1].options.parameters[2], i64_param(3).unwrap());
}

#[test]
fn aggregate_row_budget_narrows_the_lookahead_request() {
    let (stream, recorded) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![vec![
            row(1, 1, "a"),
            row(1, 2, "b"),
            row(1, 3, "c"),
        ]])],
        5,
        2,
        65_536,
        10,
    );

    let error = block_on(stream.try_collect::<Vec<_>>()).expect_err("exact budget");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    let recorded = recorded.lock().expect("recordings");
    assert_eq!(recorded.len(), 1);
    match recorded[0].options.limit {
        LimitSetting::Set(limit) => assert_eq!(limit.rows(), 3),
        LimitSetting::Inherit => panic!("the adapter must set a hard limit"),
    }
}

#[test]
fn extra_result_set_is_rejected_before_publication() {
    let (mut stream, _) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![
            vec![row(1, 1, "a")],
            vec![row(1, 2, "b")],
        ])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(block_on(stream.try_next()).expect("terminal").is_none());
}

#[test]
fn default_no_row_reader_is_one_empty_page() {
    let (mut stream, _) = stream(
        Behavior::Normal,
        vec![Response::NoRowsDefault],
        2,
        10,
        65_536,
        10,
    );

    let page = block_on(stream.try_next())
        .expect("stream")
        .expect("one empty page");
    assert_eq!(page.len(), 0);
    assert!(block_on(stream.try_next()).expect("terminal").is_none());
}

#[test]
fn an_additional_empty_result_set_is_rejected() {
    let (mut stream, _) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![vec![row(1, 1, "a")], Vec::new()])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[test]
fn later_statement_error_wins_before_publication() {
    let (mut stream, _) = stream(
        Behavior::Normal,
        vec![Response::FinishError(vec![row(1, 1, "a")])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Connection);
}

#[test]
fn unknown_write_outcome_is_terminal_and_not_retried() {
    let (mut stream, recorded) = stream(
        Behavior::Normal,
        vec![Response::FetchError(ErrorKind::WriteOutcomeUnknown)],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(recorded.lock().expect("recordings").len(), 1);
    assert!(block_on(stream.try_next()).expect("terminal").is_none());
}

#[test]
fn explicit_retry_is_rejected_before_dispatch() {
    let (mut stream, recorded) = stream(
        Behavior::Retry,
        vec![Response::Sets(vec![vec![row(1, 1, "a")]])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(recorded.lock().expect("recordings").is_empty());
}

#[test]
fn explicit_retry_off_is_accepted() {
    let (stream, recorded) = stream(
        Behavior::RetryOff,
        vec![Response::Sets(vec![vec![row(1, 1, "a")]])],
        2,
        10,
        65_536,
        10,
    );

    let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(
        recorded.lock().expect("recordings")[0].options.retry,
        RetrySetting::Off
    );
}

#[test]
fn disabled_retry_override_is_accepted_and_normalized_to_off() {
    let (stream, recorded) = stream(
        Behavior::RetryDisabled,
        vec![Response::Sets(vec![vec![row(1, 1, "a")]])],
        2,
        10,
        65_536,
        10,
    );

    let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(
        recorded.lock().expect("recordings")[0].options.retry,
        RetrySetting::Off
    );
}

#[test]
fn plan_supplied_limit_is_rejected_before_dispatch() {
    let (mut stream, recorded) = stream(
        Behavior::Limit,
        vec![Response::Sets(vec![vec![row(1, 1, "a")]])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert!(recorded.lock().expect("recordings").is_empty());
}

#[test]
fn lookahead_requires_a_continuation() {
    let (mut stream, _) = stream(
        Behavior::NoContinuationOnLookahead,
        vec![Response::Sets(vec![vec![
            row(1, 1, "a"),
            row(1, 2, "b"),
            row(1, 3, "c"),
        ]])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[test]
fn no_lookahead_rejects_a_continuation() {
    let (mut stream, _) = stream(
        Behavior::ContinuationWithoutLookahead,
        vec![Response::Sets(vec![vec![row(1, 1, "a"), row(1, 2, "b")]])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[test]
fn rows_beyond_the_lookahead_fail_closed() {
    let (mut stream, _) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![vec![
            row(1, 1, "a"),
            row(1, 2, "b"),
            row(1, 3, "c"),
            row(1, 4, "d"),
        ]])],
        2,
        10,
        65_536,
        10,
    );

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[test]
fn source_bytes_is_checked_including_the_lookahead() {
    let (session, _) = FakeSession::new(vec![Response::Sets(vec![vec![
        row(1, 1, "a"),
        row(1, 2, "b"),
    ]])]);
    let source = SqlPageSource::new(
        Box::new(session),
        Plan::new(Behavior::Normal),
        16,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    let mut stream = PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10));

    let error = block_on(stream.try_next()).expect_err("error");
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[test]
fn empty_page_completes_without_a_continuation() {
    let (stream, _) = stream(
        Behavior::Normal,
        vec![Response::Sets(vec![Vec::new()])],
        2,
        10,
        65_536,
        10,
    );

    let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
    assert_eq!(pages.len(), 1);
    assert!(pages[0].is_empty());
}

#[test]
fn zero_source_bytes_is_rejected() {
    let (session, _) = FakeSession::new(Vec::new());
    let error = SqlPageSource::new(
        Box::new(session),
        Plan::new(Behavior::Normal),
        0,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect_err("zero byte allowance");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn documented_provider_sql_shapes_are_passed_unchanged() {
    const SAMPLES: [&str; 6] = [
        "SELECT tenant, seq, body FROM events \
         WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?",
        "SELECT tenant, seq, body FROM events \
         WHERE (tenant, seq) > ($1, $2) ORDER BY tenant, seq LIMIT $3",
        "SELECT tenant, seq, body FROM events \
         WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?",
        "SELECT TOP (@P3) tenant, seq, body FROM events \
         WHERE tenant > @P1 OR (tenant = @P1 AND seq > @P2) ORDER BY tenant, seq",
        "SELECT tenant, seq, body FROM events \
         WHERE (tenant, seq) > ({p1:Int64}, {p2:Int64}) ORDER BY tenant, seq LIMIT {p3:Int64}",
        "SELECT tenant, seq, body FROM events \
         WHERE tenant = ? AND (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?",
    ];

    for sql in SAMPLES {
        let (session, recorded) =
            FakeSession::new(vec![Response::Sets(vec![vec![row(1, 1, "a")]])]);
        let source = SqlPageSource::new(
            Box::new(session),
            Plan::with_sql(Behavior::Normal, sql),
            4_096,
            SqlPageAssertion::read_only_replay_safe_and_authorized(),
        )
        .expect("valid source");
        let stream = PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10));
        let pages: Vec<PagedPage> = block_on(stream.try_collect()).expect("pages");
        assert_eq!(pages.len(), 1);
        let recorded = recorded.lock().expect("recordings");
        assert_eq!(recorded[0].sql, sql);
    }
}
