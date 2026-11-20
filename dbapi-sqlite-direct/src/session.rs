//! Async and synchronous sessions over one exclusively owned worker.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use neotales_dbapi::sync::{
    BufferedReader as SyncBufferedReader, Reader as SyncReader, Session as SyncSession,
    Transaction as SyncTransaction,
};
use neotales_dbapi::{
    BufferedReader as AsyncBufferedReader, DbError, DbFuture, ErrorKind, ExecuteOptions,
    ImmediateTransaction, PoolDefaults, QueryOptions, Reader, Row, RowLimit, Session, Transaction,
    TxOptions, Value,
};
use rusqlite::InterruptHandle;

use crate::{
    SqliteDurability,
    pool::{Command, PoolShared, Reply, Worker},
    sql,
};

/// Session cancellation and invalidation flags.
#[derive(Default)]
struct SessionState {
    cancelled: Arc<AtomicBool>,
    invalidated: AtomicBool,
}

impl SessionState {
    fn invalidated(&self) -> bool {
        self.invalidated.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Interrupts a worker if the operation future is dropped before it completes.
///
/// The guard does not release the session or its worker lease. The session stays poisoned until it
/// is dropped, so an interrupt raised after completion cannot reach a later statement on the same
/// connection. The worker is quarantined and discarded rather than reused.
struct AbortGuard {
    interrupt: Arc<InterruptHandle>,
    state: Arc<SessionState>,
    armed: bool,
}

impl AbortGuard {
    fn new(interrupt: Arc<InterruptHandle>, state: Arc<SessionState>) -> Self {
        Self {
            interrupt,
            state,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        if self.armed {
            self.state.cancelled.store(true, Ordering::Release);
            self.interrupt.interrupt();
        }
    }
}

/// One exclusively borrowed SQLite session.
///
/// A session owns one worker until it is dropped. It implements both the asynchronous
/// [`Session`] family and the synchronous [`SyncSession`] family on the same connection. Mixing
/// them is supported, but a synchronous call blocks its thread, so do not call one on an async
/// executor thread.
///
/// After a dispatched error, or after an async operation future is dropped, the session is
/// poisoned: the worker is discarded and further calls fail until the session is dropped. A
/// validation error detected before dispatch leaves the session usable.
pub struct SqliteSession {
    worker: Option<Worker>,
    shared: Arc<PoolShared>,
    durability: SqliteDurability,
    state: Arc<SessionState>,
    in_transaction: bool,
}

impl std::fmt::Debug for SqliteSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteSession")
    }
}

impl SqliteSession {
    pub(crate) fn new(worker: Worker, shared: Arc<PoolShared>) -> Self {
        let durability = shared.config.durability;
        let state = Arc::new(SessionState {
            cancelled: Arc::clone(&worker.cancelled),
            ..SessionState::default()
        });
        Self {
            worker: Some(worker),
            shared,
            durability,
            state,
            in_transaction: false,
        }
    }

    fn is_durable(&self) -> bool {
        self.durability == SqliteDurability::Durable
    }

    fn ensure_live(&self) -> Result<(), DbError> {
        if self.state.invalidated() || self.worker.is_none() {
            Err(DbError::new(ErrorKind::Connection))
        } else if self.state.cancelled() {
            Err(DbError::new(ErrorKind::Cancelled))
        } else {
            Ok(())
        }
    }

    fn invalidate(&mut self) {
        self.state.invalidated.store(true, Ordering::Release);
        self.state.cancelled.store(true, Ordering::Release);
        // Closing the channel lets an in-flight worker finish, fail to deliver its reply, and exit.
        self.worker = None;
    }

    fn sender_and_interrupt(
        &self,
    ) -> Result<(std::sync::mpsc::Sender<Command>, Arc<InterruptHandle>), DbError> {
        let worker = self
            .worker
            .as_ref()
            .ok_or_else(|| DbError::new(ErrorKind::Connection))?;
        Ok((worker.sender.clone(), Arc::clone(&worker.interrupt)))
    }

    async fn dispatch_async<T>(
        &mut self,
        command: impl FnOnce(Reply<Result<T, DbError>>) -> Command,
        disconnect: ErrorKind,
    ) -> Result<T, DbError> {
        self.ensure_live()?;
        let (sender, interrupt) = self.sender_and_interrupt()?;
        let (reply, receiver) = Reply::async_channel();
        sender
            .send(command(reply))
            .map_err(|_| DbError::new(ErrorKind::Connection))?;
        let guard = AbortGuard::new(interrupt, Arc::clone(&self.state));
        let delivered = receiver.await;
        guard.disarm();
        match delivered {
            Ok(result) => result,
            Err(_) => Err(DbError::new(disconnect)),
        }
    }

    fn dispatch_sync<T>(
        &mut self,
        command: impl FnOnce(Reply<Result<T, DbError>>) -> Command,
        disconnect: ErrorKind,
    ) -> Result<T, DbError> {
        self.ensure_live()?;
        let (sender, _) = self.sender_and_interrupt()?;
        let (reply, receiver) = Reply::sync_channel();
        sender
            .send(command(reply))
            .map_err(|_| DbError::new(ErrorKind::Connection))?;
        match receiver.recv() {
            Ok(result) => result,
            Err(_) => Err(DbError::new(disconnect)),
        }
    }

    /// Validates an ordinary statement and returns the owned inputs for dispatch.
    fn prepare_statement(
        &self,
        options: &QueryOptions,
        sql: &str,
        transaction_statement: bool,
    ) -> Result<(String, Vec<Value>, bool), DbError> {
        sql::check_sql(sql)?;
        sql::check_one_statement(sql)?;
        if !transaction_statement {
            sql::check_not_transaction_control(sql)?;
        }
        let durable = self.is_durable();
        if durable {
            sql::check_durable_command(sql)?;
        }
        sql::check_arity(sql, options.parameters.as_slice())?;
        Ok((
            sql.to_owned(),
            options
                .parameters
                .iter()
                .map(|parameter| parameter.value().clone())
                .collect(),
            durable,
        ))
    }

    /// Validates a trusted script and returns the owned SQL and durability flag.
    fn prepare_script(&self, sql: &str) -> Result<(String, bool), DbError> {
        sql::check_sql(sql)?;
        let durable = self.is_durable();
        if durable {
            sql::check_durable_script(sql)?;
        }
        Ok((sql.to_owned(), durable))
    }

    async fn dispatch_execute(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        durable: bool,
    ) -> Result<u64, DbError> {
        self.dispatch_async(
            move |reply| Command::Execute {
                sql,
                parameters,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
        .await
    }

    async fn dispatch_fetch(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        limit: RowLimit,
        durable: bool,
    ) -> Result<Vec<Row>, DbError> {
        self.dispatch_async(
            move |reply| Command::Fetch {
                sql,
                parameters,
                limit,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
        .await
    }

    async fn dispatch_script(&mut self, sql: String, durable: bool) -> Result<(), DbError> {
        self.dispatch_async(
            move |reply| Command::Script {
                sql,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
        .await
    }

    fn dispatch_execute_sync(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        durable: bool,
    ) -> Result<u64, DbError> {
        self.dispatch_sync(
            move |reply| Command::Execute {
                sql,
                parameters,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
    }

    fn dispatch_fetch_sync(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        limit: RowLimit,
        durable: bool,
    ) -> Result<Vec<Row>, DbError> {
        self.dispatch_sync(
            move |reply| Command::Fetch {
                sql,
                parameters,
                limit,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
    }

    fn dispatch_script_sync(&mut self, sql: String, durable: bool) -> Result<(), DbError> {
        self.dispatch_sync(
            move |reply| Command::Script {
                sql,
                durable,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        )
    }

    async fn commit_inner(&mut self) -> Result<(), DbError> {
        let durable = self.is_durable();
        let result = self
            .dispatch_async(
                move |reply| Command::Commit { durable, reply },
                ErrorKind::CommitOutcomeUnknown,
            )
            .await;
        self.in_transaction = false;
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    async fn rollback_inner(&mut self) -> Result<(), DbError> {
        let result = self
            .dispatch_async(
                move |reply| Command::Rollback { reply },
                ErrorKind::Connection,
            )
            .await;
        self.in_transaction = false;
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    async fn begin_inner(&mut self, immediate: bool) -> Result<(), DbError> {
        let durable = self.is_durable();
        self.dispatch_async(
            move |reply| Command::Begin {
                immediate,
                durable,
                reply,
            },
            ErrorKind::Connection,
        )
        .await
    }

    fn commit_inner_sync(&mut self) -> Result<(), DbError> {
        let durable = self.is_durable();
        let result = self.dispatch_sync(
            move |reply| Command::Commit { durable, reply },
            ErrorKind::CommitOutcomeUnknown,
        );
        self.in_transaction = false;
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn rollback_inner_sync(&mut self) -> Result<(), DbError> {
        let result = self.dispatch_sync(
            move |reply| Command::Rollback { reply },
            ErrorKind::Connection,
        );
        self.in_transaction = false;
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn begin_inner_sync(&mut self, immediate: bool) -> Result<(), DbError> {
        let durable = self.is_durable();
        self.dispatch_sync(
            move |reply| Command::Begin {
                immediate,
                durable,
                reply,
            },
            ErrorKind::Connection,
        )
    }

    /// Begins a synchronous immediate transaction on this session.
    ///
    /// This is the synchronous counterpart of [`ImmediateTransaction::begin_immediate_async`].
    /// SQLite acquires the write lock at begin, so two writers fail fast with `Busy` instead of
    /// discovering the conflict at commit.
    ///
    /// # Errors
    /// Returns a sanitized provider error; a failed begin discards the worker.
    pub fn begin_immediate(&mut self) -> Result<Box<dyn SyncTransaction<'_> + '_>, DbError> {
        self.begin_sync(true)
    }

    fn begin_sync(
        &mut self,
        immediate: bool,
    ) -> Result<Box<dyn SyncTransaction<'_> + '_>, DbError> {
        self.ensure_live()?;
        match self.begin_inner_sync(immediate) {
            Ok(()) => {
                self.in_transaction = true;
                Ok(Box::new(SqliteSyncTransaction {
                    session: self,
                    done: false,
                }))
            }
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }
}

impl Drop for SqliteSession {
    fn drop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        if self.in_transaction || self.state.invalidated() || self.state.cancelled() {
            // Dropping the worker closes its channel; the thread rolls back and exits.
            return;
        }
        self.shared.push_idle(worker);
    }
}

fn prepare_execute(options: &mut QueryOptions) -> Result<(), DbError> {
    options.prepare_execute_provider(PoolDefaults::default())?;
    check_timeout(options.provider_execute_options())
}

fn prepare_fetch(options: &mut QueryOptions) -> Result<RowLimit, DbError> {
    let limit = options.prepare_provider(PoolDefaults::default())?;
    check_timeout(options.provider_execute_options())?;
    Ok(limit)
}

fn prepare_script(options: &mut QueryOptions) -> Result<(), DbError> {
    options.prepare_script_provider(PoolDefaults::default())?;
    check_timeout(options.provider_execute_options())
}

fn check_timeout(options: ExecuteOptions) -> Result<(), DbError> {
    if options.timeout_ms.is_some() {
        Err(DbError::new(ErrorKind::Unsupported))
    } else {
        Ok(())
    }
}

impl Session for SqliteSession {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            prepare_execute(&mut options)?;
            let (sql, parameters, durable) = self.prepare_statement(&options, sql, false)?;
            let result = self.dispatch_execute(sql, parameters, durable).await;
            if result.is_err() {
                self.invalidate();
            }
            result
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            let limit = prepare_fetch(&mut options)?;
            let (sql, parameters, durable) = self.prepare_statement(&options, sql, false)?;
            match self.dispatch_fetch(sql, parameters, limit, durable).await {
                Ok(rows) => Ok(Box::new(AsyncBufferedReader::new(rows)) as Box<dyn Reader + 'a>),
                Err(error) => {
                    self.invalidate();
                    Err(error)
                }
            }
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            prepare_script(&mut options)?;
            let (sql, durable) = self.prepare_script(sql)?;
            let result = self.dispatch_script(sql, durable).await;
            if result.is_err() {
                self.invalidate();
            }
            result
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            if options.isolation.is_some() || options.read_only == Some(true) {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            self.ensure_live()?;
            match self.begin_inner(false).await {
                Ok(()) => {
                    self.in_transaction = true;
                    Ok(Box::new(SqliteTransaction {
                        session: self,
                        done: false,
                    }) as Box<dyn Transaction<'a> + 'a>)
                }
                Err(error) => {
                    self.invalidate();
                    Err(error)
                }
            }
        })
    }
}

impl ImmediateTransaction for SqliteSession {
    fn begin_immediate_async<'a>(&'a mut self) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            self.ensure_live()?;
            match self.begin_inner(true).await {
                Ok(()) => {
                    self.in_transaction = true;
                    Ok(Box::new(SqliteTransaction {
                        session: self,
                        done: false,
                    }) as Box<dyn Transaction<'a> + 'a>)
                }
                Err(error) => {
                    self.invalidate();
                    Err(error)
                }
            }
        })
    }
}

impl SyncSession for SqliteSession {
    fn execute(&mut self, sql: &str, mut options: QueryOptions) -> Result<u64, DbError> {
        prepare_execute(&mut options)?;
        let (sql, parameters, durable) = self.prepare_statement(&options, sql, false)?;
        let result = self.dispatch_execute_sync(sql, parameters, durable);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn fetch(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<Box<dyn SyncReader + '_>, DbError> {
        let limit = prepare_fetch(&mut options)?;
        let (sql, parameters, durable) = self.prepare_statement(&options, sql, false)?;
        match self.dispatch_fetch_sync(sql, parameters, limit, durable) {
            Ok(rows) => Ok(Box::new(SyncBufferedReader::new(rows))),
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    fn execute_trusted_script(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        prepare_script(&mut options)?;
        let (sql, durable) = self.prepare_script(sql)?;
        let result = self.dispatch_script_sync(sql, durable);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn begin(&mut self, options: TxOptions) -> Result<Box<dyn SyncTransaction<'_> + '_>, DbError> {
        if options.isolation.is_some() || options.read_only == Some(true) {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        self.begin_sync(false)
    }
}

struct SqliteTransaction<'session> {
    session: &'session mut SqliteSession,
    done: bool,
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.session.invalidate();
        }
    }
}

impl<'session> Transaction<'session> for SqliteTransaction<'session> {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            prepare_execute(&mut options)?;
            let (sql, parameters, durable) = self.session.prepare_statement(&options, sql, true)?;
            let result = self
                .session
                .dispatch_execute(sql, parameters, durable)
                .await;
            if result.is_err() {
                self.session.invalidate();
            }
            result
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            let limit = prepare_fetch(&mut options)?;
            let (sql, parameters, durable) = self.session.prepare_statement(&options, sql, true)?;
            match self
                .session
                .dispatch_fetch(sql, parameters, limit, durable)
                .await
            {
                Ok(rows) => Ok(Box::new(AsyncBufferedReader::new(rows)) as Box<dyn Reader + 'a>),
                Err(error) => {
                    self.session.invalidate();
                    Err(error)
                }
            }
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            prepare_script(&mut options)?;
            let (sql, durable) = self.session.prepare_script(sql)?;
            let result = self.session.dispatch_script(sql, durable).await;
            if result.is_err() {
                self.session.invalidate();
            }
            result
        })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            let result = this.session.commit_inner().await;
            this.done = true;
            result
        })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            let result = this.session.rollback_inner().await;
            this.done = true;
            result
        })
    }
}

struct SqliteSyncTransaction<'session> {
    session: &'session mut SqliteSession,
    done: bool,
}

impl Drop for SqliteSyncTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.session.invalidate();
        }
    }
}

impl<'session> SyncTransaction<'session> for SqliteSyncTransaction<'session> {
    fn execute(&mut self, sql: &str, mut options: QueryOptions) -> Result<u64, DbError> {
        prepare_execute(&mut options)?;
        let (sql, parameters, durable) = self.session.prepare_statement(&options, sql, true)?;
        let result = self.session.dispatch_execute_sync(sql, parameters, durable);
        if result.is_err() {
            self.session.invalidate();
        }
        result
    }

    fn fetch(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<Box<dyn SyncReader + '_>, DbError> {
        let limit = prepare_fetch(&mut options)?;
        let (sql, parameters, durable) = self.session.prepare_statement(&options, sql, true)?;
        match self
            .session
            .dispatch_fetch_sync(sql, parameters, limit, durable)
        {
            Ok(rows) => Ok(Box::new(SyncBufferedReader::new(rows))),
            Err(error) => {
                self.session.invalidate();
                Err(error)
            }
        }
    }

    fn execute_trusted_script(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        prepare_script(&mut options)?;
        let (sql, durable) = self.session.prepare_script(sql)?;
        let result = self.session.dispatch_script_sync(sql, durable);
        if result.is_err() {
            self.session.invalidate();
        }
        result
    }

    fn commit(self: Box<Self>) -> Result<(), DbError> {
        let mut this = *self;
        let result = this.session.commit_inner_sync();
        this.done = true;
        result
    }

    fn rollback(self: Box<Self>) -> Result<(), DbError> {
        let mut this = *self;
        let result = this.session.rollback_inner_sync();
        this.done = true;
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        SqliteConfig, SqlitePoolProvider,
        pool::{Command, PANIC_CLEANUP_BARRIERS},
    };
    use neotales_dbapi::ErrorKind;
    use std::{
        future::Future,
        sync::{Arc, Barrier},
        task::{Context, Poll, Waker},
    };

    // The command/VM gap cannot be held open through the public API. Gate that private gap,
    // rather than relying on a sleep or an expensive query to win a scheduling race.
    #[tokio::test]
    async fn cancellation_before_sqlite_starts_is_not_lost() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queued-cancellation.sqlite");
        let provider =
            SqlitePoolProvider::connect_sync(SqliteConfig::new(&path).max_connections(1)).unwrap();
        let mut session = provider.acquire_sync_sqlite().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let mut operation = Box::pin(session.dispatch_async(
            |reply| Command::PausedScript {
                pause_after: 0,
                // A short VM never reaches the periodic progress callback.
                sql: "CREATE TABLE ran_after_cancellation(value INTEGER)".to_owned(),
                entered,
                resume: paused,
                reply,
            },
            ErrorKind::WriteOutcomeUnknown,
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(operation.as_mut().poll(&mut context).is_pending());
        observed
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        drop(operation);
        assert!(session.state.cancelled());
        drop(session);
        provider.close();
        resume.send(()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            provider.wait_for_cleanup(),
        )
        .await
        .unwrap()
        .unwrap();
        let inspector = rusqlite::Connection::open(path).unwrap();
        let count: i64 = inspector
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='ran_after_cancellation'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "cancellation must remain visible when SQLite begins later"
        );
    }

    // A private statement-boundary gate is needed to cancel between two short VMs.
    // Assert on persisted effects, not on whether the internal callback happened.
    #[tokio::test]
    async fn cancellation_between_short_script_statements_stops_the_batch_and_rolls_back() {
        for transactional in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("short-script.sqlite");
            let provider =
                SqlitePoolProvider::connect_sync(SqliteConfig::new(&path).max_connections(1))
                    .unwrap();
            let mut session = provider.acquire_sync_sqlite().unwrap();
            neotales_dbapi::sync::Session::execute(
                &mut session,
                "CREATE TABLE effects(value INTEGER)",
                neotales_dbapi::QueryOptions::new(),
            )
            .unwrap();
            if transactional {
                // Exercise the real transaction lifecycle; the worker must roll back on close.
                session.begin_inner_sync(false).unwrap();
            }
            let (entered, observed) = std::sync::mpsc::channel();
            let (resume, paused) = std::sync::mpsc::channel();
            let mut operation = Box::pin(session.dispatch_async(
                |reply| Command::PausedScript {
                    sql: "INSERT INTO effects VALUES(1); INSERT INTO effects VALUES(2)".to_owned(),
                    pause_after: 1,
                    entered,
                    resume: paused,
                    reply,
                },
                ErrorKind::WriteOutcomeUnknown,
            ));
            let mut context = Context::from_waker(Waker::noop());
            assert!(operation.as_mut().poll(&mut context).is_pending());
            observed
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            drop(operation);
            drop(session);
            provider.close();
            resume.send(()).unwrap();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                provider.wait_for_cleanup(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(provider.unused_connection_slots(), 1);
            let inspector = rusqlite::Connection::open(path).unwrap();
            let values: Vec<i64> = inspector
                .prepare("SELECT value FROM effects ORDER BY value")
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_eq!(values, if transactional { vec![] } else { vec![1] });
        }
    }

    #[test]
    fn worker_panic_maps_to_unknown_outcomes_and_cleanup_failure() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let provider = SqlitePoolProvider::connect_sync(
            SqliteConfig::new(directory.path().join("panic.sqlite")).max_connections(1),
        )
        .expect("provider");

        let caught = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        *PANIC_CLEANUP_BARRIERS
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((Arc::clone(&caught), Arc::clone(&resume)));
        let mut session = provider.acquire_sync_sqlite().expect("session");
        let result = session.dispatch_sync(
            |reply| Command::PanicWithReply { reply },
            ErrorKind::WriteOutcomeUnknown,
        );
        assert_eq!(
            result.expect_err("a panicked worker cannot reply").kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        drop(session);

        // Hold the worker after catch_unwind and before publishing failure. Cleanup must remain
        // pending while the caller's session is already gone.
        caught.wait();
        provider.close();
        let mut cleanup = Box::pin(provider.wait_for_cleanup());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(cleanup.as_mut().poll(&mut context), Poll::Pending));
        resume.wait();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        assert_eq!(
            runtime
                .block_on(cleanup)
                .expect_err("a worker panic is a cleanup failure")
                .kind(),
            ErrorKind::Connection
        );

        let commit_provider = SqlitePoolProvider::connect_sync(
            SqliteConfig::new(directory.path().join("panic-commit.sqlite")).max_connections(1),
        )
        .expect("commit provider");
        let mut commit_session = commit_provider.acquire_sync_sqlite().expect("session");
        let result = commit_session.dispatch_sync(
            |reply| Command::PanicWithReply { reply },
            ErrorKind::CommitOutcomeUnknown,
        );
        assert_eq!(
            result.expect_err("a panicked worker cannot reply").kind(),
            ErrorKind::CommitOutcomeUnknown
        );
        drop(commit_session);
        commit_provider.close();
        assert_eq!(
            runtime
                .block_on(commit_provider.wait_for_cleanup())
                .expect_err("the worker panic is a cleanup failure")
                .kind(),
            ErrorKind::Connection
        );
    }
}
