//! Behaviour tests for the blocking adapter.
#![cfg(feature = "blocking")]

use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Cancellation, Capabilities, DbError, DbFuture, Delay, ErrorKind, PolicyPool,
    Pool, PoolPolicy, QueryOptions, Reader, RetryPolicy, RetryRuntime, Row, RowLimit, Session,
    Transaction, TxOptions, Value,
    blocking::BlockingPool,
    sync::{
        Pool as SyncPool,
        paging::{RowIter, SetRow},
    },
};

#[derive(Default)]
struct State {
    session_drops: AtomicUsize,
    reader_finishes: AtomicUsize,
    commit_calls: AtomicUsize,
    rollback_calls: AtomicUsize,
    side_effects: AtomicUsize,
    panic_commit: AtomicBool,
    target: Mutex<Option<Weak<BlockingPool>>>,
}

struct MockPool {
    state: Arc<State>,
}

impl Pool for MockPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            ..Capabilities::default()
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move { Ok(Box::new(MockSession { state }) as Box<dyn Session>) })
    }
}

struct MockSession {
    state: Arc<State>,
}

impl Drop for MockSession {
    fn drop(&mut self) {
        self.state.session_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl MockSession {
    fn state(&self) -> Arc<State> {
        Arc::clone(&self.state)
    }
}

impl Session for MockSession {
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        let state = self.state();
        let sql = sql.to_owned();
        Box::pin(async move {
            match sql.as_str() {
                "unknown" => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
                "cancelled" => Err(DbError::new(ErrorKind::Cancelled)),
                "timeout" => Err(DbError::new(ErrorKind::Timeout)),
                "panic-write" => {
                    state.side_effects.fetch_add(1, Ordering::SeqCst);
                    panic!("mock write side effect");
                }
                "reentrant" => {
                    let target = state
                        .target
                        .lock()
                        .expect("target lock")
                        .as_ref()
                        .and_then(Weak::upgrade);
                    if let Some(pool) = target {
                        match SyncPool::acquire(pool.as_ref()) {
                            Ok(_) => Ok(0),
                            Err(error) => Err(error),
                        }
                    } else {
                        Ok(0)
                    }
                }
                _ if options.timeout
                    == neotales_dbapi::TimeoutSetting::Set(Duration::from_millis(1)) =>
                {
                    Err(DbError::new(ErrorKind::Timeout))
                }
                _ => Ok(1),
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let state = self.state();
        let sql = sql.to_owned();
        Box::pin(async move {
            if sql == "panic-fetch" {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock fetch side effect");
            }
            let (sets, mode) = match sql.as_str() {
                "one" => (vec![vec![row(1)]], ReaderMode::Normal),
                "many" => (vec![vec![row(1), row(2)]], ReaderMode::Normal),
                "sets" => (
                    vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]],
                    ReaderMode::Normal,
                ),
                "finish-error" => (vec![vec![row(1), row(2)]], ReaderMode::FailFinish),
                "panic-next-row" => (vec![vec![row(1)]], ReaderMode::PanicNextRow),
                "panic-has-next" => (vec![vec![row(1)], vec![row(2)]], ReaderMode::PanicHasNext),
                "panic-finish" => (vec![vec![row(1)]], ReaderMode::PanicFinish),
                _ => (vec![Vec::new()], ReaderMode::Normal),
            };
            Ok(Box::new(TrackedReader {
                inner: BufferedReader::from_sets(sets),
                state,
                mode,
            }) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        let state = self.state();
        let sql = sql.to_owned();
        Box::pin(async move {
            if sql == "panic-script" {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock script side effect");
            }
            Ok(())
        })
    }

    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let state = self.state();
        Box::pin(
            async move { Ok(Box::new(MockTransaction { state }) as Box<dyn Transaction<'a> + 'a>) },
        )
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum ReaderMode {
    #[default]
    Normal,
    FailFinish,
    PanicNextRow,
    PanicHasNext,
    PanicFinish,
}

struct TrackedReader {
    inner: BufferedReader,
    state: Arc<State>,
    mode: ReaderMode,
}

impl Reader for TrackedReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        if self.mode == ReaderMode::PanicNextRow {
            let state = Arc::clone(&self.state);
            return Box::pin(async move {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock next_row side effect");
            });
        }
        self.inner.next_row_async()
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        if self.mode == ReaderMode::PanicHasNext {
            let state = Arc::clone(&self.state);
            return Box::pin(async move {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock has_next_set side effect");
            });
        }
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        if self.mode == ReaderMode::PanicFinish {
            let state = Arc::clone(&self.state);
            return Box::pin(async move {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock finish side effect");
            });
        }
        let state = Arc::clone(&self.state);
        let fail = self.mode == ReaderMode::FailFinish;
        Box::pin(async move {
            state.reader_finishes.fetch_add(1, Ordering::SeqCst);
            self.inner.finish_async().await?;
            if fail {
                Err(DbError::new(ErrorKind::Connection))
            } else {
                Ok(())
            }
        })
    }
}

struct MockTransaction {
    state: Arc<State>,
}

impl<'session> Transaction<'session> for MockTransaction {
    fn execute_async<'a>(&'a mut self, sql: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        let state = Arc::clone(&self.state);
        let sql = sql.to_owned();
        Box::pin(async move {
            if sql == "panic-tx-write" {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock transaction write side effect");
            }
            Ok(2)
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

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if state.panic_commit.load(Ordering::SeqCst) {
                state.side_effects.fetch_add(1, Ordering::SeqCst);
                panic!("mock commit side effect");
            }
            state.commit_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.rollback_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn pool(reentrant_target: bool) -> (Arc<BlockingPool>, Arc<State>) {
    let state = Arc::new(State::default());
    let blocking = Arc::new(BlockingPool::new(MockPool {
        state: Arc::clone(&state),
    }));
    if reentrant_target {
        *state.target.lock().expect("target lock") = Some(Arc::downgrade(&blocking));
    }
    (blocking, state)
}

fn limit() -> RowLimit {
    RowLimit::new(16, 4096).expect("valid limit")
}

fn row(value: i64) -> Row {
    Row::new(vec![("n".to_owned(), Value::I64(value))]).expect("valid row")
}

#[test]
fn execute_reader_advance_finish_and_cleanup() {
    let (pool, state) = pool(false);
    assert_eq!(
        SyncPool::execute(pool.as_ref(), "row", QueryOptions::new()).expect("execute"),
        1
    );

    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("sets", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(reader.next_row().expect("row"), Some(row(1)));
    assert_eq!(reader.next_row().expect("end"), None);
    assert!(reader.has_next_set().expect("set"));
    assert_eq!(reader.next_row().expect("empty"), None);
    assert!(reader.has_next_set().expect("set"));
    assert_eq!(reader.next_row().expect("row"), Some(row(2)));
    assert_eq!(reader.next_row().expect("row"), Some(row(3)));
    assert_eq!(reader.next_row().expect("end"), None);
    assert!(!reader.has_next_set().expect("end"));
    reader.finish().expect("finish");
    drop(reader);
    assert_eq!(state.reader_finishes.load(Ordering::SeqCst), 1);
    assert_eq!(session.execute("row", QueryOptions::new()), Ok(1));
    drop(session);
    assert!(state.session_drops.load(Ordering::SeqCst) >= 2);
}

#[test]
fn sync_page_view_runs_over_a_blocking_reader() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let reader = session
        .fetch("sets", QueryOptions::new().limit(limit()))
        .expect("reader");
    let rows: Vec<SetRow> = RowIter::new(reader)
        .collect::<Result<_, DbError>>()
        .expect("sync row iterator");
    assert_eq!(
        rows.iter()
            .map(|item| {
                (
                    item.set_index(),
                    item.row().read::<i64>("n").expect("integer value"),
                )
            })
            .collect::<Vec<_>>(),
        vec![(0, 1), (2, 2), (2, 3)]
    );
    assert_eq!(state.reader_finishes.load(Ordering::SeqCst), 1);
    assert_eq!(session.execute("row", QueryOptions::new()), Ok(1));
}

#[test]
fn dropping_an_unfinished_reader_drains_and_keeps_the_session() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("sets", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(reader.next_row().expect("row"), Some(row(1)));
    drop(reader);
    assert_eq!(state.reader_finishes.load(Ordering::SeqCst), 1);
    assert_eq!(session.execute("row", QueryOptions::new()), Ok(1));
    drop(session);
    assert_eq!(state.session_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_failed_reader_cleanup_discards_the_session() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("finish-error", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert!(reader.next_row().expect("row").is_some());
    drop(reader);
    let discarded = session
        .execute("row", QueryOptions::new())
        .expect_err("session discarded")
        .kind();
    assert!(matches!(
        discarded,
        ErrorKind::Connection | ErrorKind::WriteOutcomeUnknown
    ));
    drop(session);
    assert_eq!(state.session_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn transaction_commit_and_drop_rollback() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut transaction = session.begin(TxOptions::default()).expect("transaction");
    assert_eq!(transaction.execute("row", QueryOptions::new()), Ok(2));
    transaction.commit().expect("commit");
    assert_eq!(state.commit_calls.load(Ordering::SeqCst), 1);

    let transaction = session.begin(TxOptions::default()).expect("transaction");
    drop(transaction);
    assert_eq!(state.rollback_calls.load(Ordering::SeqCst), 1);
    assert_eq!(session.execute("row", QueryOptions::new()), Ok(1));
    drop(session);
}

#[test]
fn commit_and_rollback_have_their_own_calls() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let transaction = session.begin(TxOptions::default()).expect("transaction");
    transaction.rollback().expect("rollback");
    assert_eq!(state.rollback_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.commit_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn pool_convenience_calls_drive_the_reader() {
    let (pool, _) = pool(false);
    assert_eq!(
        SyncPool::fetch_scalar(pool.as_ref(), "one", QueryOptions::new().limit(limit()))
            .expect("scalar"),
        Value::I64(1)
    );
    assert_eq!(
        SyncPool::fetch_one(pool.as_ref(), "many", QueryOptions::new().limit(limit()))
            .expect_err("too many")
            .kind(),
        ErrorKind::TooManyRows
    );
    assert_eq!(
        SyncPool::fetch_first(pool.as_ref(), "many", QueryOptions::new().limit(limit()))
            .expect("first")
            .expect("row"),
        row(1)
    );
}

#[test]
fn errors_keep_their_category_and_options_are_forwarded() {
    let (pool, _) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    assert_eq!(
        session
            .execute("unknown", QueryOptions::new())
            .expect_err("unknown")
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        session
            .execute("cancelled", QueryOptions::new())
            .expect_err("cancelled")
            .kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(
        session
            .execute("timeout", QueryOptions::new())
            .expect_err("timeout")
            .kind(),
        ErrorKind::Timeout
    );
    let options = QueryOptions::new().timeout(Duration::from_millis(1));
    assert_eq!(
        session
            .execute("row", options)
            .expect_err("forwarded timeout")
            .kind(),
        ErrorKind::Timeout
    );
}

#[test]
fn reentrant_calls_fail_instead_of_waiting_on_the_worker() {
    let (pool, _) = pool(true);
    assert_eq!(
        SyncPool::execute(pool.as_ref(), "reentrant", QueryOptions::new())
            .expect_err("reentrant")
            .kind(),
        ErrorKind::Reentrant
    );
}

#[test]
fn blocking_calls_inside_an_async_runtime_do_not_nest_block_on() {
    let (pool, _) = pool(false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        assert_eq!(
            SyncPool::execute(pool.as_ref(), "row", QueryOptions::new()),
            Ok(1)
        );
    });
}

#[test]
fn concurrent_callers_get_independent_sessions() {
    let (pool, state) = pool(false);
    let (done, finished) = mpsc::channel();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let pool = Arc::clone(&pool);
        let done = done.clone();
        workers.push(thread::spawn(move || {
            for _ in 0..8 {
                assert_eq!(
                    SyncPool::execute(pool.as_ref(), "row", QueryOptions::new()),
                    Ok(1)
                );
            }
            done.send(()).expect("done");
        }));
    }
    drop(done);
    for _ in 0..8 {
        finished.recv().expect("worker");
    }
    for worker in workers {
        worker.join().expect("worker");
    }
    assert!(state.session_drops.load(Ordering::SeqCst) >= 64);
}

#[test]
fn lost_reply_after_a_dispatched_write_is_unknown() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let error = session
        .execute("panic-write", QueryOptions::new())
        .expect_err("worker panic");
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    // A later command either cannot be enqueued (Connection) or is enqueued while the worker
    // unwinds and loses its reply (WriteOutcomeUnknown). Neither may report success.
    let later = session
        .execute("row", QueryOptions::new())
        .expect_err("worker gone")
        .kind();
    assert!(matches!(
        later,
        ErrorKind::Connection | ErrorKind::WriteOutcomeUnknown
    ));
}

#[test]
fn lost_reply_after_a_dispatched_fetch_is_unknown() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let error = session
        .fetch("panic-fetch", QueryOptions::new().limit(limit()))
        .err()
        .expect("worker panic");
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
}

#[test]
fn lost_reply_after_a_dispatched_script_is_unknown() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let error = session
        .execute_trusted_script("panic-script", QueryOptions::new())
        .expect_err("worker panic");
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
}

#[test]
fn lost_reply_after_a_dispatched_commit_is_commit_unknown() {
    let (pool, state) = pool(false);
    state.panic_commit.store(true, Ordering::SeqCst);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let transaction = session.begin(TxOptions::default()).expect("transaction");
    let error = transaction.commit().expect_err("worker panic");
    assert_eq!(error.kind(), ErrorKind::CommitOutcomeUnknown);
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    assert_eq!(state.commit_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn lost_reply_after_a_dispatched_transaction_write_is_unknown() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut transaction = session.begin(TxOptions::default()).expect("transaction");
    assert_eq!(
        transaction
            .execute("panic-tx-write", QueryOptions::new())
            .expect_err("worker panic")
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    // Commit either cannot be enqueued (Connection) or is enqueued while the worker unwinds and
    // loses its reply (CommitOutcomeUnknown). Neither may look like a confirmed failure.
    let commit = transaction.commit().expect_err("worker gone").kind();
    assert!(matches!(
        commit,
        ErrorKind::Connection | ErrorKind::CommitOutcomeUnknown
    ));
}

#[test]
fn reader_worker_loss_during_next_row_is_unknown_and_sticky() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("panic-next-row", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(
        reader.next_row().expect_err("worker panic").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    // The worker died before this next command could be enqueued; the batch outcome is still
    // unknown, not a transport failure.
    assert_eq!(
        reader.next_row().expect_err("worker gone").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        reader.finish().expect_err("worker gone").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
}

#[test]
fn reader_worker_loss_during_has_next_set_is_unknown_and_sticky() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("panic-has-next", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(
        reader.has_next_set().expect_err("worker panic").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        reader.has_next_set().expect_err("worker gone").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
}

#[test]
fn reader_worker_loss_during_finish_is_unknown_and_sticky() {
    let (pool, state) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("panic-finish", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(
        reader.finish().expect_err("worker panic").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(state.side_effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        reader.finish().expect_err("worker gone").kind(),
        ErrorKind::WriteOutcomeUnknown
    );
}

#[test]
fn known_reader_error_is_preserved_and_finish_ends_the_lifecycle() {
    let (pool, _) = pool(false);
    let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
    let mut reader = session
        .fetch("finish-error", QueryOptions::new().limit(limit()))
        .expect("reader");
    assert_eq!(
        reader.finish().expect_err("known provider error").kind(),
        ErrorKind::Connection
    );
    assert_eq!(reader.finish(), Ok(()));
}

struct RetryState {
    attempts: AtomicUsize,
    busy_until: usize,
    fetch_attempts: AtomicUsize,
    fetch_busy_until: usize,
    attempt_sleep: Duration,
}

struct RetryPool {
    state: Arc<RetryState>,
}

impl Pool for RetryPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move { Ok(Box::new(RetrySession { state }) as Box<dyn Session>) })
    }
}

struct RetrySession {
    state: Arc<RetryState>,
}

impl Session for RetrySession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let attempt = state.attempts.fetch_add(1, Ordering::SeqCst);
            if !state.attempt_sleep.is_zero() {
                tokio::time::sleep(state.attempt_sleep).await;
            }
            if attempt < state.busy_until {
                Err(DbError::new(ErrorKind::Busy))
            } else {
                Ok(1)
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let attempt = state.fetch_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < state.fetch_busy_until {
                return Err(DbError::new(ErrorKind::Busy));
            }
            Ok(Box::new(BufferedReader::new(vec![row(9)])) as Box<dyn Reader>)
        })
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

fn retry_pool(
    busy_until: usize,
    attempt_sleep: Duration,
    budget: Duration,
    retry_delay: Duration,
) -> (BlockingPool, Arc<RetryState>) {
    retry_pool_with_fetch(busy_until, 0, attempt_sleep, budget, retry_delay)
}

fn retry_pool_with_fetch(
    busy_until: usize,
    fetch_busy_until: usize,
    attempt_sleep: Duration,
    budget: Duration,
    retry_delay: Duration,
) -> (BlockingPool, Arc<RetryState>) {
    let state = Arc::new(RetryState {
        attempts: AtomicUsize::new(0),
        busy_until,
        fetch_attempts: AtomicUsize::new(0),
        fetch_busy_until,
        attempt_sleep,
    });
    let cancellation = Arc::new(Cancellation::new());
    let runtime = RetryRuntime::new(
        Arc::clone(&cancellation),
        move || Box::pin(tokio::time::sleep(budget)),
        move |delay| Box::pin(tokio::time::sleep(delay)),
        || 0,
    );
    let policy = PoolPolicy {
        retry: RetryPolicy::new(3, Delay::Fixed(retry_delay), false).expect("valid policy"),
        retry_runtime: Some(runtime),
        ..PoolPolicy::default()
    };
    let pool = BlockingPool::new(PolicyPool::new(
        RetryPool {
            state: Arc::clone(&state),
        },
        policy,
    ));
    (pool, state)
}

fn replay_safe() -> QueryOptions {
    QueryOptions::new().replay_safe()
}

#[test]
fn pool_level_retry_runs_an_explicitly_replay_safe_operation() {
    let (pool, state) = retry_pool(1, Duration::ZERO, Duration::from_secs(5), Duration::ZERO);
    assert_eq!(
        SyncPool::execute(&pool, "write", replay_safe()).expect("retry success"),
        1
    );
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn pool_level_retry_needs_replay_safety() {
    let (pool, state) = retry_pool(1, Duration::ZERO, Duration::from_secs(5), Duration::ZERO);
    assert_eq!(
        SyncPool::execute(&pool, "write", QueryOptions::default())
            .expect_err("busy")
            .kind(),
        ErrorKind::Busy
    );
    assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn pool_level_fetch_retry_runs_an_explicitly_replay_safe_operation() {
    let (pool, state) =
        retry_pool_with_fetch(0, 1, Duration::ZERO, Duration::from_secs(5), Duration::ZERO);
    assert_eq!(
        SyncPool::fetch_all(&pool, "select", replay_safe().limit(limit())).expect("retry success"),
        vec![row(9)]
    );
    assert_eq!(state.fetch_attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn total_deadline_stops_retry_backoff() {
    let (pool, state) = retry_pool(
        usize::MAX,
        Duration::ZERO,
        Duration::from_millis(100),
        Duration::from_secs(5),
    );
    assert_eq!(
        SyncPool::execute(&pool, "write", replay_safe())
            .expect_err("deadline")
            .kind(),
        ErrorKind::Timeout
    );
    assert!(state.attempts.load(Ordering::SeqCst) <= 1);
}

#[test]
fn deadline_during_a_dispatched_write_is_unknown() {
    let (pool, state) = retry_pool(
        usize::MAX,
        Duration::from_secs(2),
        Duration::from_millis(200),
        Duration::ZERO,
    );
    assert_eq!(
        SyncPool::execute(&pool, "write", replay_safe())
            .expect_err("deadline")
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
}
