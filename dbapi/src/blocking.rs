//! A blocking adapter over the asynchronous traits, driven by dedicated worker threads.
//!
//! [`BlockingPool`] implements [`crate::sync::Pool`] by giving each acquired session its own
//! thread. That thread owns a current-thread Tokio runtime, acquires the asynchronous session, and
//! serves commands until the blocking session or its reader/transaction handles are dropped. No
//! caller ever drives a future on its own thread, so this adapter can be called from inside another
//! async runtime without nesting `block_on`.
//!
//! The adapter is deliberately not a general-purpose runtime. It creates one current-thread
//! runtime per blocking operation or session, and enables all Tokio drivers compiled into the
//! process. It does not implement retry itself: pool-level calls run the asynchronous pool's own
//! `execute_async` and `fetch_async`, so wrapping that pool in [`crate::PolicyPool`]
//! preserves pool-level retry, deadline, and telemetry behavior. Session and transaction calls
//! never retry, matching the asynchronous contract.
//!
//! # Blocking versus native synchronous
//!
//! A native synchronous provider should implement [`crate::sync`] directly and stream from its
//! driver. This adapter is for an asynchronous provider that must be called from blocking code. It
//! preserves the asynchronous reader lifecycle (advancing sets, finishing, dropping an unfinished
//! reader) but each reader command is a channel round trip to the worker.
//!
//! # Calling from async code
//!
//! Do not call a blocking adapter directly from an async task. An application that already owns an
//! async executor should await the asynchronous traits, or use its runtime's blocking pool (for
//! example `tokio::task::spawn_blocking`) to call this adapter. Calling a blocking method from a
//! runtime worker can starve that runtime even though this adapter does not nest `block_on`.
//!
//! Calls made from one of this adapter's own worker threads fail with
//! [`crate::ErrorKind::Reentrant`] instead of waiting on the worker that is already executing the
//! caller. Dropping a handle from its own worker thread signals cleanup without joining.
//!
//! # Cleanup and unknown outcomes
//!
//! Dropping a [`BlockingReader`] sends a final drain command to the worker and waits for it unless
//! the caller is itself on the worker thread. If that drain fails, the session is discarded. If a
//! reader method already reported an error, the reader is finished and its own drop behavior
//! handles any connection cleanup. Dropping a [`BlockingTransaction`] sends rollback. A commit or
//! rollback that fails discards the session; a propagated
//! [`crate::ErrorKind::WriteOutcomeUnknown`] or [`crate::ErrorKind::CommitOutcomeUnknown`] keeps its
//! category. A command that could not be enqueued returns [`crate::ErrorKind::Connection`]. A
//! command that was enqueued but whose reply was lost, for example because the worker panicked
//! after dispatch, returns [`crate::ErrorKind::WriteOutcomeUnknown`] for execute, fetch, and
//! script, and [`crate::ErrorKind::CommitOutcomeUnknown`] for commit. A reader whose worker is gone
//! returns `WriteOutcomeUnknown` from `next_row`, `has_next_set`, and `finish`, including when the
//! worker died before a later command could be enqueued; it stays poisoned until dropped. A
//! rollback reply loss returns `Connection`. A known provider error is preserved, and a successful
//! or known-error `finish` ends the reader lifecycle.
//!
//! A call in progress cannot be interrupted by dropping its handle from another thread, because
//! the handle is exclusive and the synchronous call has not returned. Cancellation and deadlines
//! therefore depend on the caller's `QueryOptions`, the provider, and the asynchronous operation's
//! own cancellation contract.

use std::{
    cell::Cell,
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        mpsc::{self, SyncSender},
    },
    thread::{self, JoinHandle},
};

use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    watch,
};

use super::{
    Capabilities, DbError, ErrorKind, Pool as AsyncPool, QueryOptions, Reader as AsyncReader, Row,
    Session as AsyncSession, Transaction as AsyncTransaction, TxOptions,
};
use crate::sync;

thread_local! {
    static WORKER_THREAD: Cell<bool> = const { Cell::new(false) };
}

fn mark_worker_thread() {
    WORKER_THREAD.with(|flag| flag.set(true));
}

fn in_worker_thread() -> bool {
    WORKER_THREAD.with(Cell::get)
}

fn reject_reentrant() -> Result<(), DbError> {
    if in_worker_thread() {
        Err(DbError::new(ErrorKind::Reentrant))
    } else {
        Ok(())
    }
}

type Reply<T> = SyncSender<Result<T, DbError>>;

enum SessionCommand {
    Execute {
        sql: String,
        options: QueryOptions,
        reply: Reply<u64>,
    },
    Fetch {
        sql: String,
        options: QueryOptions,
        reply: SyncSender<Result<UnboundedSender<ReaderCommand>, DbError>>,
    },
    Script {
        sql: String,
        options: QueryOptions,
        reply: Reply<()>,
    },
    Begin {
        options: TxOptions,
        reply: SyncSender<Result<UnboundedSender<TransactionCommand>, DbError>>,
    },
}

enum ReaderCommand {
    NextRow(Reply<Option<Row>>),
    HasNextSet(Reply<bool>),
    Finish(Reply<()>),
    Close { drain: bool, ack: SyncSender<bool> },
}

enum TransactionCommand {
    Execute {
        sql: String,
        options: QueryOptions,
        reply: Reply<u64>,
    },
    Fetch {
        sql: String,
        options: QueryOptions,
        reply: SyncSender<Result<UnboundedSender<ReaderCommand>, DbError>>,
    },
    Script {
        sql: String,
        options: QueryOptions,
        reply: Reply<()>,
    },
    Commit(Reply<()>),
    Rollback(Reply<()>),
    Close(SyncSender<()>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReaderEnd {
    Closed,
    Discard,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionEnd {
    Continue,
    Discard,
    Shutdown,
}

fn worker_error() -> DbError {
    DbError::new(ErrorKind::Connection)
}

fn lost_reply(lost: ErrorKind) -> DbError {
    DbError::new(lost)
}

fn send_session<T>(
    commands: &UnboundedSender<SessionCommand>,
    lost: ErrorKind,
    build: impl FnOnce(Reply<T>) -> SessionCommand,
) -> Result<T, DbError> {
    reject_reentrant()?;
    let (reply, response) = mpsc::sync_channel(1);
    commands.send(build(reply)).map_err(|_| worker_error())?;
    response.recv().map_err(|_| lost_reply(lost))?
}

fn send_reader<T>(
    commands: &UnboundedSender<ReaderCommand>,
    build: impl FnOnce(Reply<T>) -> ReaderCommand,
) -> Result<T, DbError> {
    reject_reentrant()?;
    let (reply, response) = mpsc::sync_channel(1);
    // A fetch may have written. Once the reader worker is gone, no reader command can prove the
    // already-dispatched batch outcome, including a command that could not be enqueued.
    commands
        .send(build(reply))
        .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
    response
        .recv()
        .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?
}

fn send_transaction<T>(
    commands: &UnboundedSender<TransactionCommand>,
    lost: ErrorKind,
    build: impl FnOnce(Reply<T>) -> TransactionCommand,
) -> Result<T, DbError> {
    reject_reentrant()?;
    let (reply, response) = mpsc::sync_channel(1);
    commands.send(build(reply)).map_err(|_| worker_error())?;
    response.recv().map_err(|_| lost_reply(lost))?
}

fn run_worker<T, F>(lost: ErrorKind, work: F) -> Result<T, DbError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, DbError> + Send + 'static,
{
    reject_reentrant()?;
    let (reply, response) = mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("neotales-dbapi-blocking-pool".to_owned())
        .spawn(move || {
            mark_worker_thread();
            let _ = reply.send(work());
        })
        .map_err(|_| worker_error())?;
    let result = response.recv().map_err(|_| lost_reply(lost))?;
    let _ = worker.join();
    result
}

fn shutdown_requested(
    changed: &Result<(), watch::error::RecvError>,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    changed.is_err() || *shutdown.borrow()
}

/// A [`crate::sync::Pool`] backed by an asynchronous [`crate::Pool`] on dedicated workers.
///
/// Each call to [`crate::sync::Pool::acquire`] starts one worker thread that owns its runtime and
/// its asynchronous session. The pool itself only holds a shared reference to the asynchronous
/// pool, so it is cheap to clone and share between blocking callers.
#[derive(Clone)]
pub struct BlockingPool {
    inner: Arc<dyn AsyncPool>,
}

impl fmt::Debug for BlockingPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingPool")
            .field("capabilities", &self.inner.capabilities())
            .finish_non_exhaustive()
    }
}

impl BlockingPool {
    /// Wraps an owned asynchronous pool.
    pub fn new<P: AsyncPool + 'static>(pool: P) -> Self {
        Self::from_shared(Arc::new(pool))
    }

    /// Wraps a shared asynchronous pool.
    #[must_use]
    pub fn from_shared(pool: Arc<dyn AsyncPool>) -> Self {
        Self { inner: pool }
    }
}

impl sync::Pool for BlockingPool {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn defaults(&self) -> super::PoolDefaults {
        self.inner.defaults()
    }

    fn acquire(&self) -> Result<Box<dyn sync::Session>, DbError> {
        reject_reentrant()?;
        Ok(Box::new(BlockingSession::acquire(Arc::clone(&self.inner))?))
    }

    fn execute(&self, sql: &str, options: QueryOptions) -> Result<u64, DbError> {
        options.validate_execute()?;
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        run_worker(ErrorKind::WriteOutcomeUnknown, move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| worker_error())?;
            runtime.block_on(async move { pool.execute_async(&sql, options).await })
        })
    }

    fn fetch(
        &self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        let limit = options.resolved_limit(pool.defaults());
        let sets = run_worker(ErrorKind::WriteOutcomeUnknown, move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| worker_error())?;
            runtime.block_on(async move {
                let mut reader = pool.fetch_async(&sql, options).await?;
                super::collect_sets(reader.as_mut(), limit).await
            })
        })?;
        Ok(Box::new(sync::BufferedReader::from_sets(sets)))
    }
}

/// An asynchronous session owned by a dedicated worker thread.
///
/// The session is exclusive: while a returned reader or transaction handle borrows this session,
/// the compiler rejects another session call. Dropping the session signals the worker to finish any
/// reader or transaction cleanup, drop the asynchronous session, and exit.
pub struct BlockingSession {
    defaults: super::PoolDefaults,
    commands: UnboundedSender<SessionCommand>,
    shutdown: watch::Sender<bool>,
    worker: Option<JoinHandle<()>>,
}

impl fmt::Debug for BlockingSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingSession")
            .field("worker_running", &self.worker.is_some())
            .finish_non_exhaustive()
    }
}

impl BlockingSession {
    fn acquire(pool: Arc<dyn AsyncPool>) -> Result<Self, DbError> {
        let defaults = pool.defaults();
        let (commands, command_rx) = unbounded_channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (ready, ready_rx) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("neotales-dbapi-blocking".to_owned())
            .spawn(move || {
                mark_worker_thread();
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    let _ = ready.send(Err(worker_error()));
                    return;
                };
                runtime.block_on(async move {
                    match pool.acquire_async().await {
                        Ok(session) => {
                            if ready.send(Ok(())).is_err() {
                                return;
                            }
                            serve_session(session, command_rx, shutdown_rx).await;
                        }
                        Err(error) => {
                            let _ = ready.send(Err(error));
                        }
                    }
                });
            })
            .map_err(|_| worker_error())?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                defaults,
                commands,
                shutdown,
                worker: Some(worker),
            }),
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(_) => {
                let _ = worker.join();
                Err(worker_error())
            }
        }
    }
}

impl Drop for BlockingSession {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if !in_worker_thread()
            && let Some(worker) = self.worker.take()
        {
            let _ = worker.join();
        }
    }
}

impl sync::Session for BlockingSession {
    fn defaults(&self) -> super::PoolDefaults {
        self.defaults
    }

    fn execute(&mut self, sql: &str, options: QueryOptions) -> Result<u64, DbError> {
        options.validate_execute()?;
        send_session(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            SessionCommand::Execute {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })
    }

    fn fetch(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        let commands = send_session(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            SessionCommand::Fetch {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })?;
        Ok(Box::new(BlockingReader::new(commands)))
    }

    fn execute_trusted_script(&mut self, sql: &str, options: QueryOptions) -> Result<(), DbError> {
        send_session(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            SessionCommand::Script {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })
    }

    fn begin(
        &mut self,
        options: TxOptions,
    ) -> Result<Box<dyn sync::Transaction<'_> + '_>, DbError> {
        let commands = send_session(&self.commands, ErrorKind::Connection, |reply| {
            SessionCommand::Begin { options, reply }
        })?;
        Ok(Box::new(BlockingTransaction::new(commands, self.defaults)))
    }
}

/// A synchronous reader whose asynchronous reader lives on the session worker.
///
/// The borrow of the parent session or transaction is real: the parent cannot be used or dropped
/// until this handle is dropped. Dropping it drains an unfinished reader on the worker and waits
/// for cleanup to finish.
pub struct BlockingReader<'borrow> {
    commands: UnboundedSender<ReaderCommand>,
    finished: bool,
    worker_lost: bool,
    _borrow: PhantomData<&'borrow mut ()>,
}

impl fmt::Debug for BlockingReader<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingReader")
            .field("finished", &self.finished)
            .field("worker_lost", &self.worker_lost)
            .finish_non_exhaustive()
    }
}

impl BlockingReader<'_> {
    fn new(commands: UnboundedSender<ReaderCommand>) -> Self {
        Self {
            commands,
            finished: false,
            worker_lost: false,
            _borrow: PhantomData,
        }
    }

    fn unavailable(&self) -> Option<DbError> {
        self.worker_lost
            .then(|| DbError::new(ErrorKind::WriteOutcomeUnknown))
    }

    fn record<T>(&mut self, result: Result<T, DbError>) -> Result<T, DbError> {
        if let Err(error) = &result {
            match error.kind() {
                ErrorKind::Reentrant => {}
                ErrorKind::WriteOutcomeUnknown => self.worker_lost = true,
                _ => self.finished = true,
            }
        }
        result
    }

    fn close(&self, drain: bool) {
        let (ack, response) = mpsc::sync_channel(1);
        if self
            .commands
            .send(ReaderCommand::Close { drain, ack })
            .is_ok()
            && !in_worker_thread()
        {
            let _ = response.recv();
        }
    }
}

impl Drop for BlockingReader<'_> {
    fn drop(&mut self) {
        self.close(!self.finished && !self.worker_lost);
    }
}

impl sync::Reader for BlockingReader<'_> {
    fn next_row(&mut self) -> Result<Option<Row>, DbError> {
        if let Some(error) = self.unavailable() {
            return Err(error);
        }
        if self.finished {
            return Ok(None);
        }
        let result = send_reader(&self.commands, ReaderCommand::NextRow);
        self.record(result)
    }

    fn has_next_set(&mut self) -> Result<bool, DbError> {
        if let Some(error) = self.unavailable() {
            return Err(error);
        }
        if self.finished {
            return Ok(false);
        }
        let result = send_reader(&self.commands, ReaderCommand::HasNextSet);
        self.record(result)
    }

    fn finish(&mut self) -> Result<(), DbError> {
        if let Some(error) = self.unavailable() {
            return Err(error);
        }
        if self.finished {
            return Ok(());
        }
        let result = send_reader(&self.commands, ReaderCommand::Finish);
        if result.is_ok() {
            self.finished = true;
            return Ok(());
        }
        self.record(result)
    }
}

/// A synchronous transaction served by the session worker.
///
/// The borrow of the session is real. Dropping an uncommitted transaction sends rollback and waits
/// for it unless the caller is itself on the worker thread.
pub struct BlockingTransaction<'session> {
    defaults: super::PoolDefaults,
    commands: UnboundedSender<TransactionCommand>,
    finished: bool,
    _borrow: PhantomData<&'session mut ()>,
}

impl fmt::Debug for BlockingTransaction<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingTransaction")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl BlockingTransaction<'_> {
    fn new(commands: UnboundedSender<TransactionCommand>, defaults: super::PoolDefaults) -> Self {
        Self {
            commands,
            defaults,
            finished: false,
            _borrow: PhantomData,
        }
    }

    fn close(&self) {
        let (ack, response) = mpsc::sync_channel(1);
        if self.commands.send(TransactionCommand::Close(ack)).is_ok() && !in_worker_thread() {
            let _ = response.recv();
        }
    }
}

impl Drop for BlockingTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.close();
        }
    }
}

impl<'session> sync::Transaction<'session> for BlockingTransaction<'session> {
    fn defaults(&self) -> super::PoolDefaults {
        self.defaults
    }
    fn execute(&mut self, sql: &str, options: QueryOptions) -> Result<u64, DbError> {
        options.validate_execute()?;
        send_transaction(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            TransactionCommand::Execute {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })
    }

    fn fetch(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        let commands = send_transaction(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            TransactionCommand::Fetch {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })?;
        Ok(Box::new(BlockingReader::new(commands)))
    }

    fn execute_trusted_script(&mut self, sql: &str, options: QueryOptions) -> Result<(), DbError> {
        send_transaction(&self.commands, ErrorKind::WriteOutcomeUnknown, |reply| {
            TransactionCommand::Script {
                sql: sql.to_owned(),
                options,
                reply,
            }
        })
    }

    fn commit(mut self: Box<Self>) -> Result<(), DbError> {
        self.finished = true;
        send_transaction(
            &self.commands,
            ErrorKind::CommitOutcomeUnknown,
            TransactionCommand::Commit,
        )
    }

    fn rollback(mut self: Box<Self>) -> Result<(), DbError> {
        self.finished = true;
        send_transaction(
            &self.commands,
            ErrorKind::Connection,
            TransactionCommand::Rollback,
        )
    }
}

async fn serve_session(
    mut session: Box<dyn AsyncSession>,
    mut commands: UnboundedReceiver<SessionCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if shutdown_requested(&changed, &shutdown) {
                    return;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { return; };
                match command {
                    SessionCommand::Execute { sql, options, reply } => {
                        let result = session.execute_async(&sql, options).await;
                        let _ = reply.send(result);
                    }
                    SessionCommand::Fetch { sql, options, reply } => {
                        match session.fetch_async(&sql, options).await {
                            Ok(mut reader) => {
                                let (reader_commands, mut reader_rx) = unbounded_channel();
                                if reply.send(Ok(reader_commands)).is_err() {
                                    let _ = reader.finish_async().await;
                                    return;
                                }
                                match serve_reader(reader.as_mut(), &mut reader_rx, &mut shutdown).await {
                                    ReaderEnd::Closed => {}
                                    ReaderEnd::Discard | ReaderEnd::Shutdown => return,
                                }
                            }
                            Err(error) => {
                                let _ = reply.send(Err(error));
                            }
                        }
                    }
                    SessionCommand::Script { sql, options, reply } => {
                        let result = session.execute_trusted_script_async(&sql, options).await;
                        let _ = reply.send(result);
                    }
                    SessionCommand::Begin { options, reply } => {
                        match session.begin_async(options).await {
                            Ok(transaction) => {
                                let (transaction_commands, mut transaction_rx) = unbounded_channel();
                                if reply.send(Ok(transaction_commands)).is_err() {
                                    let _ = transaction.rollback_async().await;
                                    return;
                                }
                                match serve_transaction(transaction, &mut transaction_rx, &mut shutdown).await {
                                    TransactionEnd::Continue => {}
                                    TransactionEnd::Discard | TransactionEnd::Shutdown => return,
                                }
                            }
                            Err(error) => {
                                let _ = reply.send(Err(error));
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn serve_reader(
    reader: &mut dyn AsyncReader,
    commands: &mut UnboundedReceiver<ReaderCommand>,
    shutdown: &mut watch::Receiver<bool>,
) -> ReaderEnd {
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if shutdown_requested(&changed, shutdown) {
                    let _ = reader.finish_async().await;
                    return ReaderEnd::Shutdown;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return if reader.finish_async().await.is_err() {
                        ReaderEnd::Discard
                    } else {
                        ReaderEnd::Closed
                    };
                };
                match command {
                    ReaderCommand::NextRow(reply) => {
                        let result = reader.next_row_async().await;
                        if reply.send(result).is_err() {
                            let _ = reader.finish_async().await;
                            return ReaderEnd::Closed;
                        }
                    }
                    ReaderCommand::HasNextSet(reply) => {
                        let result = reader.has_next_set_async().await;
                        if reply.send(result).is_err() {
                            let _ = reader.finish_async().await;
                            return ReaderEnd::Closed;
                        }
                    }
                    ReaderCommand::Finish(reply) => {
                        let result = reader.finish_async().await;
                        if reply.send(result).is_err() {
                            return ReaderEnd::Closed;
                        }
                    }
                    ReaderCommand::Close { drain, ack } => {
                        let discard = drain && reader.finish_async().await.is_err();
                        let _ = ack.send(discard);
                        return if discard {
                            ReaderEnd::Discard
                        } else {
                            ReaderEnd::Closed
                        };
                    }
                }
            }
        }
    }
}

async fn serve_transaction<'session>(
    mut transaction: Box<dyn AsyncTransaction<'session> + 'session>,
    commands: &mut UnboundedReceiver<TransactionCommand>,
    shutdown: &mut watch::Receiver<bool>,
) -> TransactionEnd {
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if shutdown_requested(&changed, shutdown) {
                    let _ = transaction.rollback_async().await;
                    return TransactionEnd::Shutdown;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    let _ = transaction.rollback_async().await;
                    return TransactionEnd::Shutdown;
                };
                match command {
                    TransactionCommand::Execute { sql, options, reply } => {
                        let result = transaction.execute_async(&sql, options).await;
                        if reply.send(result).is_err() {
                            let _ = transaction.rollback_async().await;
                            return TransactionEnd::Discard;
                        }
                    }
                    TransactionCommand::Fetch { sql, options, reply } => {
                        let end = {
                            let fetched = transaction.fetch_async(&sql, options)
                                .await;
                            match fetched {
                                Ok(mut reader) => {
                                    let (reader_commands, mut reader_rx) = unbounded_channel();
                                    if reply.send(Ok(reader_commands)).is_err() {
                                        let _ = reader.finish_async().await;
                                        ReaderEnd::Discard
                                    } else {
                                        serve_reader(reader.as_mut(), &mut reader_rx, shutdown).await
                                    }
                                }
                                Err(error) => {
                                    let _ = reply.send(Err(error));
                                    ReaderEnd::Closed
                                }
                            }
                        };
                        if end != ReaderEnd::Closed {
                            let _ = transaction.rollback_async().await;
                            return if end == ReaderEnd::Shutdown {
                                TransactionEnd::Shutdown
                            } else {
                                TransactionEnd::Discard
                            };
                        }
                    }
                    TransactionCommand::Script { sql, options, reply } => {
                        let result = transaction.execute_trusted_script_async(&sql, options).await;
                        if reply.send(result).is_err() {
                            let _ = transaction.rollback_async().await;
                            return TransactionEnd::Discard;
                        }
                    }
                    TransactionCommand::Commit(reply) => {
                        let result = transaction.commit_async().await;
                        let _ = reply.send(result);
                        return if result.is_ok() {
                            TransactionEnd::Continue
                        } else {
                            TransactionEnd::Discard
                        };
                    }
                    TransactionCommand::Rollback(reply) => {
                        let result = transaction.rollback_async().await;
                        let _ = reply.send(result);
                        return if result.is_ok() {
                            TransactionEnd::Continue
                        } else {
                            TransactionEnd::Discard
                        };
                    }
                    TransactionCommand::Close(ack) => {
                        let result = transaction.rollback_async().await;
                        let _ = ack.send(());
                        return if result.is_ok() {
                            TransactionEnd::Continue
                        } else {
                            TransactionEnd::Discard
                        };
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_session_channel_is_a_pre_enqueue_connection_error() {
        let (commands, receiver) = unbounded_channel();
        drop(receiver);
        let result: Result<u64, DbError> =
            send_session(&commands, ErrorKind::WriteOutcomeUnknown, |reply| {
                SessionCommand::Execute {
                    sql: String::new(),
                    options: QueryOptions::default(),
                    reply,
                }
            });
        assert_eq!(result.expect_err("closed").kind(), ErrorKind::Connection);
    }

    #[test]
    fn closed_transaction_commit_channel_is_a_pre_enqueue_connection_error() {
        let (commands, receiver) = unbounded_channel();
        drop(receiver);
        let result: Result<(), DbError> = send_transaction(
            &commands,
            ErrorKind::CommitOutcomeUnknown,
            TransactionCommand::Commit,
        );
        assert_eq!(result.expect_err("closed").kind(), ErrorKind::Connection);
    }

    #[test]
    fn closed_reader_channel_is_an_unknown_batch_outcome() {
        let (commands, receiver) = unbounded_channel();
        drop(receiver);
        let result = send_reader(&commands, ReaderCommand::NextRow);
        assert_eq!(
            result.expect_err("closed").kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }
}
