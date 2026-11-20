//! Bounded pool of blocking SQLite workers.
//!
//! One OS thread owns each physical [`rusqlite::Connection`] for its whole life. Callers exchange
//! owned commands and owned rows with that thread over channels, so no `Connection` handle (which
//! is `Send` but not `Sync`, and uses interior mutability) is ever touched from two threads. Async
//! callers await a reply; synchronous callers block on one. The pool bounds the number of live
//! workers, reuses clean workers for an idle TTL, and releases capacity only when a worker thread
//! has actually stopped.
//!
//! A worker that sees a failed, interrupted, or abandoned command is quarantined: it reports the
//! result, then closes. Its connection is never reused, and SQLite rolls back any open transaction
//! when the connection is dropped. Capacity remains held until the worker exits.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::Barrier;

use neotales_dbapi::{DbError, DbType, ErrorKind, Row, RowLimit, Value};
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, InterruptHandle, OpenFlags, Statement};

use crate::{
    SqliteConfig, SqliteDurability,
    error::{map_sqlite_error, map_write_error},
};

/// Reply channel for one worker command.
pub(crate) enum Reply<T> {
    /// Asynchronous one-shot reply.
    Async(tokio::sync::oneshot::Sender<T>),
    /// Blocking reply.
    Sync(std::sync::mpsc::Sender<T>),
}

impl<T> Reply<T> {
    fn send(self, value: T) -> bool {
        match self {
            Self::Async(sender) => sender.send(value).is_ok(),
            Self::Sync(sender) => sender.send(value).is_ok(),
        }
    }

    pub(crate) fn async_channel() -> (Self, tokio::sync::oneshot::Receiver<T>) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self::Async(sender), receiver)
    }

    pub(crate) fn sync_channel() -> (Self, std::sync::mpsc::Receiver<T>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Self::Sync(sender), receiver)
    }
}

/// A physical connection's interrupt handle, available before any command is dispatched.
struct Opened {
    interrupt: InterruptHandle,
    cancelled: Arc<AtomicBool>,
}

/// Reply channel for the worker-open handshake.
enum OpenReply {
    Async(tokio::sync::oneshot::Sender<Result<Opened, DbError>>),
    Sync(std::sync::mpsc::Sender<Result<Opened, DbError>>),
}

impl OpenReply {
    fn send(self, result: Result<Opened, DbError>) -> bool {
        match self {
            Self::Async(sender) => sender.send(result).is_ok(),
            Self::Sync(sender) => sender.send(result).is_ok(),
        }
    }

    pub(crate) fn async_channel() -> (
        Self,
        tokio::sync::oneshot::Receiver<Result<Opened, DbError>>,
    ) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        (Self::Async(sender), receiver)
    }

    pub(crate) fn sync_channel() -> (Self, std::sync::mpsc::Receiver<Result<Opened, DbError>>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Self::Sync(sender), receiver)
    }
}

/// One owned operation sent to a worker.
pub(crate) enum Command {
    /// Execute one parameterized statement and report affected rows.
    Execute {
        sql: String,
        parameters: Vec<Value>,
        durable: bool,
        reply: Reply<Result<u64, DbError>>,
    },
    /// Materialize one bounded result set.
    Fetch {
        sql: String,
        parameters: Vec<Value>,
        limit: RowLimit,
        durable: bool,
        reply: Reply<Result<Vec<Row>, DbError>>,
    },
    /// Execute a trusted, unparameterized script.
    Script {
        sql: String,
        durable: bool,
        reply: Reply<Result<(), DbError>>,
    },
    /// Begin a deferred or immediate transaction.
    Begin {
        immediate: bool,
        durable: bool,
        reply: Reply<Result<(), DbError>>,
    },
    /// Commit the open transaction.
    Commit {
        durable: bool,
        reply: Reply<Result<(), DbError>>,
    },
    /// Roll back the open transaction.
    Rollback { reply: Reply<Result<(), DbError>> },
    /// Check that the worker is alive and its connection still answers.
    Ping {
        durable: bool,
        reply: Reply<Result<(), DbError>>,
    },
    /// Panic the worker on purpose after a reply channel is created. Test-only.
    #[cfg(test)]
    PanicWithReply { reply: Reply<Result<u64, DbError>> },
    /// Pause after accepting a script but before entering SQLite, to reproduce a lost interrupt.
    #[cfg(test)]
    PausedScript {
        sql: String,
        pause_after: usize,
        entered: std::sync::mpsc::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
        reply: Reply<Result<(), DbError>>,
    },
}

/// Marks one worker's capacity slot.
///
/// The `Drop` runs only when the last handle (the pool-side [`Worker`] and the worker thread) is
/// gone, so a thread that is still finishing an abandoned statement keeps its slot.
struct Capacity {
    shared: Weak<PoolShared>,
}

impl Drop for Capacity {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.release();
        }
    }
}

/// Durability policy state shared by every worker in one pool.
pub(crate) struct DurabilityState {
    policy: SqliteDurability,
    identity: OnceLock<DurableFileIdentity>,
}

/// Immutable worker configuration captured when a thread starts.
#[derive(Clone)]
struct WorkerConfig {
    path: PathBuf,
    busy_timeout: Duration,
    durability: Arc<DurabilityState>,
}

impl WorkerConfig {
    fn durable(&self) -> bool {
        self.durability.policy == SqliteDurability::Durable
    }
}

/// A handle to one live worker thread.
pub(crate) struct Worker {
    pub(crate) sender: std::sync::mpsc::Sender<Command>,
    pub(crate) interrupt: Arc<InterruptHandle>,
    pub(crate) cancelled: Arc<AtomicBool>,
    dirty: Arc<AtomicBool>,
    _permit: Arc<Capacity>,
    idle_since: Instant,
}

impl Worker {
    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn expired(&self, idle_ttl: Duration) -> bool {
        self.idle_since.elapsed() > idle_ttl
    }
}

struct PoolInner {
    idle: Vec<Worker>,
    total: usize,
    closed: bool,
}

/// Shared pool state.
pub(crate) struct PoolShared {
    pub(crate) config: SqliteConfig,
    durability: Arc<DurabilityState>,
    inner: Mutex<PoolInner>,
    condvar: Condvar,
    notified: tokio::sync::Notify,
    cleanup_notified: tokio::sync::Notify,
    closed: AtomicBool,
    cleanup_failed: AtomicBool,
}

#[cfg(test)]
type PanicCleanupBarriers = (Arc<Barrier>, Arc<Barrier>);

#[cfg(test)]
pub(super) static PANIC_CLEANUP_BARRIERS: OnceLock<Mutex<Option<PanicCleanupBarriers>>> =
    OnceLock::new();

impl PoolShared {
    /// Validates configuration and builds empty shared pool state.
    pub(crate) fn new(mut config: SqliteConfig) -> Result<Arc<Self>, DbError> {
        config.validate()?;
        if crate::is_in_memory_database(&config.database_path) {
            // An in-memory database exists only in its one physical connection. Expiring that
            // worker would silently reset the database, so the idle window does not apply.
            config.idle_ttl = Duration::MAX;
        }
        let durability = Arc::new(DurabilityState {
            policy: config.durability,
            identity: OnceLock::new(),
        });
        Ok(Arc::new(Self {
            config,
            durability,
            inner: Mutex::new(PoolInner {
                idle: Vec::new(),
                total: 0,
                closed: false,
            }),
            condvar: Condvar::new(),
            notified: tokio::sync::Notify::new(),
            cleanup_notified: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, PoolInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Releases one worker slot and wakes both synchronous and asynchronous waiters.
    fn release(&self) {
        {
            let mut inner = self.lock();
            inner.total = inner.total.saturating_sub(1);
        }
        self.condvar.notify_all();
        self.notified.notify_waiters();
        self.cleanup_notified.notify_waiters();
    }

    pub(crate) fn push_idle(&self, mut worker: Worker) {
        if worker.is_dirty() || self.is_closed() {
            return;
        }
        worker.idle_since = Instant::now();
        {
            let mut inner = self.lock();
            if inner.closed {
                return;
            }
            inner.idle.push(worker);
        }
        // An idle worker is new capacity for a waiter even though `total` did not change.
        self.condvar.notify_all();
        self.notified.notify_waiters();
    }

    /// Number of connection slots not currently held by a live or opening worker.
    pub(crate) fn unused_slots(&self) -> usize {
        let inner = self.lock();
        self.config
            .max_connections_usize()
            .saturating_sub(inner.total)
    }

    fn worker_config(&self) -> WorkerConfig {
        WorkerConfig {
            path: self.config.database_path().to_path_buf(),
            busy_timeout: self.config.busy_timeout_value(),
            durability: Arc::clone(&self.durability),
        }
    }
}

impl Drop for PoolShared {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        let idle = {
            let mut inner = self.lock();
            inner.closed = true;
            std::mem::take(&mut inner.idle)
        };
        drop(idle);
        self.condvar.notify_all();
        self.notified.notify_waiters();
    }
}

enum Checkout {
    Closed,
    Idle(Worker),
    Reserve(Arc<Capacity>),
    Wait,
}

fn checkout(shared: &Arc<PoolShared>) -> Checkout {
    let mut inner = shared.lock();
    if inner.closed {
        return Checkout::Closed;
    }
    if let Some(worker) = inner.idle.pop() {
        return Checkout::Idle(worker);
    }
    if inner.total < shared.config.max_connections_usize() {
        inner.total += 1;
        return Checkout::Reserve(Arc::new(Capacity {
            shared: Arc::downgrade(shared),
        }));
    }
    Checkout::Wait
}

/// Acquires a worker for an asynchronous caller without blocking the executor.
pub(crate) async fn acquire_async(shared: &Arc<PoolShared>) -> Result<Worker, DbError> {
    loop {
        let notified = shared.notified.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        match checkout(shared) {
            Checkout::Closed => return Err(DbError::new(ErrorKind::Connection)),
            Checkout::Idle(worker) => {
                if worker.expired(shared.config.idle_ttl_value()) || worker.is_dirty() {
                    drop(worker);
                    continue;
                }
                if ping_async(&worker, shared).await {
                    return Ok(worker);
                }
                drop(worker);
            }
            Checkout::Reserve(permit) => return open_async(shared, permit).await,
            Checkout::Wait => notified.await,
        }
    }
}

/// Acquires a worker for a synchronous caller.
pub(crate) fn acquire_sync(shared: &Arc<PoolShared>) -> Result<Worker, DbError> {
    loop {
        match checkout(shared) {
            Checkout::Closed => return Err(DbError::new(ErrorKind::Connection)),
            Checkout::Idle(worker) => {
                if worker.expired(shared.config.idle_ttl_value()) || worker.is_dirty() {
                    drop(worker);
                    continue;
                }
                if ping_sync(&worker, shared) {
                    return Ok(worker);
                }
                drop(worker);
            }
            Checkout::Reserve(permit) => return open_sync(shared, permit),
            Checkout::Wait => {
                // Re-check the idle list as well as the total: a recycled worker does not change
                // `total`, and a notification sent between checkout and this lock would otherwise
                // be lost.
                let mut inner = shared.lock();
                while inner.total >= shared.config.max_connections_usize()
                    && inner.idle.is_empty()
                    && !inner.closed
                {
                    inner = shared
                        .condvar
                        .wait(inner)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                if inner.closed {
                    return Err(DbError::new(ErrorKind::Connection));
                }
            }
        }
    }
}

/// Opens one worker for the provider's initial validation connection.
pub(crate) async fn open_initial_async(shared: &Arc<PoolShared>) -> Result<Worker, DbError> {
    let permit = match checkout(shared) {
        Checkout::Closed | Checkout::Wait => {
            return Err(DbError::new(ErrorKind::Connection));
        }
        Checkout::Idle(worker) => return Ok(worker),
        Checkout::Reserve(permit) => permit,
    };
    open_async(shared, permit).await
}

/// Opens one worker synchronously for an initial validation connection.
pub(crate) fn open_initial_sync(shared: &Arc<PoolShared>) -> Result<Worker, DbError> {
    let permit = match checkout(shared) {
        Checkout::Closed | Checkout::Wait => {
            return Err(DbError::new(ErrorKind::Connection));
        }
        Checkout::Idle(worker) => return Ok(worker),
        Checkout::Reserve(permit) => permit,
    };
    open_sync(shared, permit)
}

fn start_worker(
    shared: &Arc<PoolShared>,
    permit: Arc<Capacity>,
    open: OpenReply,
) -> (std::sync::mpsc::Sender<Command>, Arc<AtomicBool>) {
    let (sender, receiver) = std::sync::mpsc::channel();
    let dirty = Arc::new(AtomicBool::new(false));
    let thread_dirty = Arc::clone(&dirty);
    let config = shared.worker_config();
    let pool = Arc::downgrade(shared);
    let builder = thread::Builder::new().name("neotales-dbapi-sqlite".to_owned());
    // A spawn failure drops the closure, including `open` and `permit`: the caller sees a
    // disconnected reply and the slot is released.
    let _ = builder.spawn(move || worker_main(config, receiver, open, thread_dirty, permit, pool));
    (sender, dirty)
}

async fn open_async(shared: &Arc<PoolShared>, permit: Arc<Capacity>) -> Result<Worker, DbError> {
    let (open, receiver) = OpenReply::async_channel();
    let (sender, dirty) = start_worker(shared, Arc::clone(&permit), open);
    let opened = receiver
        .await
        .map_err(|_| DbError::new(ErrorKind::Connection))??;
    Ok(Worker {
        sender,
        interrupt: Arc::new(opened.interrupt),
        cancelled: opened.cancelled,
        dirty,
        _permit: permit,
        idle_since: Instant::now(),
    })
}

fn open_sync(shared: &Arc<PoolShared>, permit: Arc<Capacity>) -> Result<Worker, DbError> {
    let (open, receiver) = OpenReply::sync_channel();
    let (sender, dirty) = start_worker(shared, Arc::clone(&permit), open);
    let opened = receiver
        .recv()
        .map_err(|_| DbError::new(ErrorKind::Connection))??;
    Ok(Worker {
        sender,
        interrupt: Arc::new(opened.interrupt),
        cancelled: opened.cancelled,
        dirty,
        _permit: permit,
        idle_since: Instant::now(),
    })
}

async fn ping_async(worker: &Worker, shared: &PoolShared) -> bool {
    let (reply, receiver) = Reply::async_channel();
    if worker
        .sender
        .send(Command::Ping {
            durable: shared.durability.policy == SqliteDurability::Durable,
            reply,
        })
        .is_err()
    {
        return false;
    }
    matches!(receiver.await, Ok(Ok(())))
}

fn ping_sync(worker: &Worker, shared: &PoolShared) -> bool {
    let (reply, receiver) = Reply::sync_channel();
    if worker
        .sender
        .send(Command::Ping {
            durable: shared.durability.policy == SqliteDurability::Durable,
            reply,
        })
        .is_err()
    {
        return false;
    }
    matches!(receiver.recv(), Ok(Ok(())))
}

/// Shuts down idle workers and rejects new acquisitions. Active sessions stay caller-owned.
pub(crate) fn close(shared: &Arc<PoolShared>) {
    shared.closed.store(true, Ordering::Release);
    let idle = {
        let mut inner = shared.lock();
        inner.closed = true;
        std::mem::take(&mut inner.idle)
    };
    drop(idle);
    shared.condvar.notify_all();
    shared.notified.notify_waiters();
    shared.cleanup_notified.notify_waiters();
}

/// Waits until every worker thread has stopped after [`close`].
pub(crate) async fn wait_for_cleanup(shared: &Arc<PoolShared>) -> Result<(), DbError> {
    loop {
        let notified = shared.cleanup_notified.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shared.lock().total == 0 {
            break;
        }
        notified.await;
    }
    if shared.cleanup_failed.load(Ordering::Acquire) {
        Err(DbError::new(ErrorKind::Connection))
    } else {
        Ok(())
    }
}

#[allow(clippy::needless_pass_by_value)] // moved into the worker thread
fn worker_main(
    config: WorkerConfig,
    receiver: std::sync::mpsc::Receiver<Command>,
    open: OpenReply,
    dirty: Arc<AtomicBool>,
    permit: Arc<Capacity>,
    pool: Weak<PoolShared>,
) {
    // Retain capacity in this frame through panic reporting. `worker_body` receives another
    // reference, which unwinding can drop before this frame records the cleanup failure.
    let retained_permit = permit;
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        worker_body(config, receiver, open, &dirty, Arc::clone(&retained_permit));
    }));
    #[cfg(test)]
    if outcome.is_err() {
        let barriers = PANIC_CLEANUP_BARRIERS
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((caught, resume)) = barriers {
            caught.wait();
            resume.wait();
        }
    }
    if outcome.is_err() {
        dirty.store(true, Ordering::Release);
        if let Some(shared) = pool.upgrade() {
            shared.cleanup_failed.store(true, Ordering::Release);
            shared.cleanup_notified.notify_waiters();
        }
    }
    drop(retained_permit);
}

#[allow(clippy::needless_pass_by_value)] // moved into the worker thread
fn worker_body(
    config: WorkerConfig,
    receiver: std::sync::mpsc::Receiver<Command>,
    open: OpenReply,
    dirty: &AtomicBool,
    permit: Arc<Capacity>,
) {
    // Keep the slot occupied for as long as this thread can touch the connection.
    let _permit = permit;

    let connection = match open_connection(&config) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = open.send(Err(error));
            return;
        }
    };
    let interrupt = connection.get_interrupt_handle();
    let cancelled = match install_cancellation_latch(&connection) {
        Ok(cancelled) => cancelled,
        Err(error) => {
            let _ = open.send(Err(error));
            return;
        }
    };
    if !open.send(Ok(Opened {
        interrupt,
        cancelled: Arc::clone(&cancelled),
    })) {
        return;
    }

    let mut discard = false;
    while let Ok(command) = receiver.recv() {
        if cancelled.load(Ordering::Acquire) {
            discard = true;
            break;
        }
        if !execute_command(&connection, &config, &cancelled, command) {
            discard = true;
            break;
        }
    }

    if !connection.is_autocommit() {
        // Closing the connection would roll back as well; doing it explicitly keeps the intent
        // visible and avoids relying on drop order for the common case.
        let _ = connection.execute_batch("ROLLBACK");
    }
    if discard {
        dirty.store(true, Ordering::Release);
    }
}

/// Execute one owned command; false means its reply was abandoned and the worker must close.
fn execute_command(
    connection: &Connection,
    config: &WorkerConfig,
    cancelled: &AtomicBool,
    command: Command,
) -> bool {
    match command {
        Command::Execute {
            sql,
            parameters,
            durable,
            reply,
        } => reply.send(run_execute(connection, config, &sql, &parameters, durable)),
        Command::Fetch {
            sql,
            parameters,
            limit,
            durable,
            reply,
        } => reply.send(run_fetch(
            connection,
            config,
            &sql,
            &parameters,
            limit,
            durable,
        )),
        Command::Script {
            sql,
            durable,
            reply,
        } => reply.send(run_script(connection, config, &sql, durable, cancelled)),
        Command::Begin {
            immediate,
            durable,
            reply,
        } => reply.send(run_begin(connection, config, immediate, durable)),
        Command::Commit { durable, reply } => reply.send(run_commit(connection, config, durable)),
        Command::Rollback { reply } => reply.send(run_rollback(connection)),
        Command::Ping { durable, reply } => reply.send(run_ping(connection, config, durable)),
        #[cfg(test)]
        Command::PausedScript {
            sql,
            pause_after,
            entered,
            resume,
            reply,
        } => reply.send(run_paused_script(
            connection,
            &sql,
            cancelled,
            pause_after,
            &entered,
            &resume,
        )),
        #[cfg(test)]
        Command::PanicWithReply { reply } => {
            // Drop the reply sender before panicking so the caller observes a disconnect.
            drop(reply);
            panic!("worker panic test");
        }
    }
}

fn install_cancellation_latch(connection: &Connection) -> Result<Arc<AtomicBool>, DbError> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancellation = Arc::clone(&cancelled);
    // sqlite3_interrupt is edge-triggered: SQLite ignores it when no statement is running,
    // including gaps between script statements. Keep cancellation latched for the entire worker
    // lifetime. Script boundaries check it too: short VMs may never reach this callback.
    connection
        .progress_handler(1000, Some(move || cancellation.load(Ordering::Acquire)))
        .map_err(|_| DbError::new(ErrorKind::Connection))?;
    Ok(cancelled)
}

/// A deterministic gate in the production script stepper, unavailable to callers.
#[cfg(test)]
fn run_paused_script(
    connection: &Connection,
    sql: &str,
    cancelled: &AtomicBool,
    pause_after: usize,
    entered: &std::sync::mpsc::Sender<()>,
    resume: &std::sync::mpsc::Receiver<()>,
) -> Result<(), DbError> {
    run_script_boundaries(connection, sql, cancelled, |completed| {
        if completed == pause_after {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
    })
}

fn open_connection(config: &WorkerConfig) -> Result<Connection, DbError> {
    let connection = Connection::open_with_flags(&config.path, OpenFlags::default())
        .map_err(|error| map_sqlite_error(&error))?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(|error| map_sqlite_error(&error))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| map_sqlite_error(&error))?;
    if config.durable() {
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(|error| map_sqlite_error(&error))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(|error| map_sqlite_error(&error))?;
        verify_durable(&connection, &config.durability)?;
    }
    Ok(connection)
}

/// Reads the connection's journal mode, synchronous level, and main database file.
pub(crate) fn durable_snapshot(connection: &Connection) -> Result<String, DbError> {
    let (journal_mode, synchronous, main_file): (String, i64, Option<String>) = connection
        .query_row(DURABILITY_SQL, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| map_sqlite_error(&error))?;
    let main_file = main_file.unwrap_or_default();
    if journal_mode.eq_ignore_ascii_case("wal")
        && synchronous == DURABLE_SYNCHRONOUS
        && !main_file.trim().is_empty()
        && main_file != ":memory:"
    {
        Ok(main_file)
    } else {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

/// Revalidates the durable policy and the main-file identity.
pub(crate) fn verify_durable(
    connection: &Connection,
    state: &DurabilityState,
) -> Result<(), DbError> {
    let main_file = durable_snapshot(connection)?;
    let actual = DurableFileIdentity::from_path(&main_file)?;
    if let Some(expected) = state.identity.get() {
        if *expected == actual {
            return Ok(());
        }
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    if state.identity.set(actual.clone()).is_ok() || state.identity.get() == Some(&actual) {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

/// Records the main database file a durable provider verified at open.
///
/// At each write and transaction boundary the observed `main` path is resolved again and compared
/// with this identity. A same-path replacement or a retargeted symlink visible at the check fails
/// closed. This is check-time evidence, not inode pinning: an ABA replacement that restores the
/// path before the next check is not detected, and non-Unix platforms compare the canonical path
/// only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableFileIdentity {
    canonical: PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl DurableFileIdentity {
    fn from_path(path: &str) -> Result<Self, DbError> {
        let canonical =
            std::fs::canonicalize(path).map_err(|_| DbError::new(ErrorKind::Connection))?;
        let metadata =
            std::fs::metadata(&canonical).map_err(|_| DbError::new(ErrorKind::Connection))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            Ok(Self {
                canonical,
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Ok(Self { canonical })
        }
    }
}

/// SQLite reports `synchronous=FULL` as the integer 2.
const DURABLE_SYNCHRONOUS: i64 = 2;

/// Reads the connection's journal mode, synchronous level, and main database file in one query.
const DURABILITY_SQL: &str = "SELECT (SELECT journal_mode FROM pragma_journal_mode) AS journal_mode, (SELECT synchronous FROM pragma_synchronous) AS synchronous, (SELECT file FROM pragma_database_list WHERE name = 'main') AS main_file";

fn run_ping(connection: &Connection, config: &WorkerConfig, durable: bool) -> Result<(), DbError> {
    if durable {
        verify_durable(connection, &config.durability)?;
    }
    connection
        .query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
        .map(|_| ())
        .map_err(|_| DbError::new(ErrorKind::Connection))
}

fn run_execute(
    connection: &Connection,
    config: &WorkerConfig,
    sql: &str,
    parameters: &[Value],
    durable: bool,
) -> Result<u64, DbError> {
    if durable {
        verify_durable(connection, &config.durability)?;
    }
    if parameters.is_empty() && is_busy_timeout_assignment(sql) {
        connection
            .execute_batch(sql)
            .map_err(|error| map_write_error(&error))?;
        return Ok(0);
    }
    let mut statement = connection
        .prepare_cached(sql)
        .map_err(|error| map_write_error(&error))?;
    bind_parameters(&mut statement, parameters)?;
    let changed = statement
        .raw_execute()
        .map_err(|error| map_write_error(&error))?;
    u64::try_from(changed).map_err(|_| DbError::new(ErrorKind::Overflow))
}

/// SQLite's busy-timeout assignment returns a row, so execute it as a drained one-statement batch.
fn is_busy_timeout_assignment(sql: &str) -> bool {
    let normalized = sql.trim().to_ascii_lowercase();
    let Some(value) = normalized.strip_prefix("pragma busy_timeout =") else {
        return false;
    };
    let value = value.strip_suffix(';').unwrap_or(value).trim();
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn run_fetch(
    connection: &Connection,
    config: &WorkerConfig,
    sql: &str,
    parameters: &[Value],
    limit: RowLimit,
    durable: bool,
) -> Result<Vec<Row>, DbError> {
    if durable {
        verify_durable(connection, &config.durability)?;
    }
    let mut statement = connection
        .prepare_cached(sql)
        .map_err(|error| map_write_error(&error))?;
    bind_parameters(&mut statement, parameters)?;
    let borrowed_columns = statement.columns();
    let column_name_bytes = borrowed_columns
        .iter()
        .try_fold(0usize, |total, column| {
            total.checked_add(column.name().len())
        })
        .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
    let columns: Vec<(String, DbType)> = borrowed_columns
        .into_iter()
        .map(|column| (column.name().to_owned(), null_type(column.decl_type())))
        .collect();

    let mut rows = statement.raw_query();
    let mut result = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next().map_err(|error| map_write_error(&error))? {
        if result.len() == limit.rows() {
            return Err(DbError::new(ErrorKind::Overflow));
        }
        result.push(materialize_row(
            row,
            &columns,
            column_name_bytes,
            &mut bytes,
            limit.bytes(),
            decode_value,
        )?);
    }
    Ok(result)
}

fn run_script(
    connection: &Connection,
    config: &WorkerConfig,
    sql: &str,
    durable: bool,
    cancelled: &AtomicBool,
) -> Result<(), DbError> {
    if durable {
        verify_durable(connection, &config.durability)?;
    }
    run_script_boundaries(connection, sql, cancelled, |_| {})
}

/// Use SQLite's parser (including trigger bodies), not a semicolon split. Like rusqlite's
/// `execute_batch`, step each statement once, ignoring returned rows and leaving unbound script
/// parameters NULL. Do not add a transaction or implicitly commit one.
fn run_script_boundaries(
    connection: &Connection,
    sql: &str,
    cancelled: &AtomicBool,
    mut boundary: impl FnMut(usize),
) -> Result<(), DbError> {
    let unknown = || DbError::new(ErrorKind::WriteOutcomeUnknown);
    let mut batch = rusqlite::Batch::new(connection, sql);
    let mut completed = 0;
    loop {
        boundary(completed);
        if cancelled.load(Ordering::Acquire) {
            return Err(unknown());
        }
        let Some(mut statement) = batch.next().map_err(|_| unknown())? else {
            return Ok(());
        };
        // Preparation can execute some PRAGMAs, and cancellation can race this check too.
        // This is cooperative cancellation, never evidence that no write occurred.
        if cancelled.load(Ordering::Acquire) {
            return Err(unknown());
        }
        statement.raw_query().next().map_err(|_| unknown())?;
        completed += 1;
    }
}

fn value_ref_bytes(value: ValueRef<'_>) -> usize {
    match value {
        ValueRef::Null => 0,
        ValueRef::Integer(_) | ValueRef::Real(_) => 8,
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.len(),
    }
}

/// Adds one row's repeated column-name and borrowed payload costs before materializing its values.
fn checked_row_bytes<'row>(
    total: usize,
    names_bytes: usize,
    values: impl Iterator<Item = Result<ValueRef<'row>, DbError>>,
    limit: usize,
) -> Result<usize, DbError> {
    let mut row_bytes = names_bytes;
    for value in values {
        row_bytes = row_bytes
            .checked_add(value_ref_bytes(value?))
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
    }
    let total = total
        .checked_add(row_bytes)
        .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
    if total > limit {
        Err(DbError::new(ErrorKind::Overflow))
    } else {
        Ok(total)
    }
}

/// Checks a row's complete borrowed byte cost before allocating owned values or column names.
fn materialize_row(
    row: &rusqlite::Row<'_>,
    columns: &[(String, DbType)],
    names_bytes: usize,
    total_bytes: &mut usize,
    byte_limit: usize,
    mut decode: impl FnMut(ValueRef<'_>, DbType) -> Result<Value, DbError>,
) -> Result<Row, DbError> {
    *total_bytes = checked_row_bytes(
        *total_bytes,
        names_bytes,
        (0..columns.len())
            .map(|index| row.get_ref(index).map_err(|error| map_sqlite_error(&error))),
        byte_limit,
    )?;

    let mut cells = Vec::with_capacity(columns.len());
    for (index, (name, declared)) in columns.iter().enumerate() {
        let raw = row
            .get_ref(index)
            .map_err(|error| map_sqlite_error(&error))?;
        cells.push((name.clone(), decode(raw, *declared)?));
    }
    Row::new(cells)
}

fn run_begin(
    connection: &Connection,
    config: &WorkerConfig,
    immediate: bool,
    durable: bool,
) -> Result<(), DbError> {
    if durable {
        verify_durable(connection, &config.durability)?;
    }
    let sql = if immediate {
        "BEGIN IMMEDIATE"
    } else {
        "BEGIN"
    };
    connection
        .execute_batch(sql)
        .map_err(|error| map_sqlite_error(&error))
}

fn run_commit(
    connection: &Connection,
    config: &WorkerConfig,
    durable: bool,
) -> Result<(), DbError> {
    if durable {
        // A failed pre-commit policy check does not dispatch COMMIT and is not an unknown outcome.
        verify_durable(connection, &config.durability)?;
    }
    connection
        .execute_batch("COMMIT")
        .map_err(|_| DbError::new(ErrorKind::CommitOutcomeUnknown))
}

fn run_rollback(connection: &Connection) -> Result<(), DbError> {
    connection
        .execute_batch("ROLLBACK")
        .map_err(|_| DbError::new(ErrorKind::Connection))
}

fn bind_parameters(statement: &mut Statement<'_>, parameters: &[Value]) -> Result<(), DbError> {
    for (offset, value) in parameters.iter().enumerate() {
        let index = offset + 1;
        let result = match value {
            Value::Null(_) => statement.raw_bind_parameter(index, Option::<i64>::None),
            Value::Bool(value) => statement.raw_bind_parameter(index, i64::from(*value)),
            Value::I64(value) => statement.raw_bind_parameter(index, *value),
            Value::F64(value) => statement.raw_bind_parameter(index, *value),
            Value::Text(value) => statement.raw_bind_parameter(index, value.as_str()),
            Value::Bytes(value) => statement.raw_bind_parameter(index, value.as_slice()),
        };
        result.map_err(|error| map_sqlite_error(&error))?;
    }
    Ok(())
}

fn decode_value(raw: ValueRef<'_>, null_type: DbType) -> Result<Value, DbError> {
    match raw {
        ValueRef::Null => Ok(Value::Null(null_type)),
        ValueRef::Integer(value) => Ok(Value::I64(value)),
        ValueRef::Real(value) => Ok(Value::F64(value)),
        ValueRef::Text(bytes) => std::str::from_utf8(bytes)
            .map(|text| Value::Text(text.to_owned()))
            .map_err(|_| DbError::new(ErrorKind::Decode)),
        ValueRef::Blob(bytes) => Ok(Value::Bytes(bytes.to_vec())),
    }
}

/// Maps a declared SQLite type to the type used for a typed null in that column.
///
/// SQLite has no boolean storage class. A declared type containing `BOOL` is treated as `Bool` for
/// null inference only; returned integers remain `I64`. An expression without a declared type
/// falls back to `Text`.
fn null_type(declared: Option<&str>) -> DbType {
    let Some(name) = declared else {
        return DbType::Text;
    };
    let name = name.to_ascii_uppercase();
    if name.contains("INT") {
        DbType::I64
    } else if name.contains("REAL") || name.contains("FLOA") || name.contains("DOUB") {
        DbType::F64
    } else if name.contains("BLOB") {
        DbType::Bytes
    } else if name.contains("BOOL") {
        DbType::Bool
    } else {
        DbType::Text
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableFileIdentity, materialize_row, null_type};
    use neotales_dbapi::{DbError, DbType, ErrorKind, Value};

    #[test]
    fn declared_types_map_to_typed_null_affinity() {
        assert_eq!(null_type(Some("INTEGER")), DbType::I64);
        assert_eq!(null_type(Some("BIGINT")), DbType::I64);
        assert_eq!(null_type(Some("REAL")), DbType::F64);
        assert_eq!(null_type(Some("DOUBLE")), DbType::F64);
        assert_eq!(null_type(Some("BLOB")), DbType::Bytes);
        assert_eq!(null_type(Some("BOOLEAN")), DbType::Bool);
        assert_eq!(null_type(Some("VARCHAR(20)")), DbType::Text);
        assert_eq!(null_type(None), DbType::Text);
    }

    #[test]
    fn oversized_borrowed_payload_is_rejected_before_decoder_allocates() {
        let connection = rusqlite::Connection::open_in_memory().expect("in-memory test database");
        let mut statement = connection
            .prepare("SELECT zeroblob(1000000) AS payload")
            .expect("query");
        let columns: Vec<(String, DbType)> = statement
            .columns()
            .into_iter()
            .map(|column| (column.name().to_owned(), DbType::Bytes))
            .collect();
        let mut rows = statement.raw_query();
        let row = rows.next().expect("step succeeds").expect("one row");
        let mut decoder_calls = 0usize;
        let mut total_bytes = 0;
        let error = materialize_row(
            row,
            &columns,
            "payload".len(),
            &mut total_bytes,
            64,
            |raw, _| {
                decoder_calls += 1;
                match raw {
                    rusqlite::types::ValueRef::Blob(bytes) => Ok(Value::Bytes(bytes.to_vec())),
                    _ => Err(DbError::new(ErrorKind::Decode)),
                }
            },
        )
        .expect_err("the borrowed payload exceeds the byte limit");

        assert_eq!(error.kind(), ErrorKind::Overflow);
        assert_eq!(
            decoder_calls, 0,
            "the provider must reject the borrowed length before invoking its allocating decoder"
        );
        assert_eq!(
            total_bytes, 0,
            "a rejected row is not charged as materialized"
        );
    }

    #[test]
    fn file_identity_compares_the_same_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("identity.sqlite");
        std::fs::write(&path, b"x").expect("write");
        let first =
            DurableFileIdentity::from_path(path.to_str().expect("utf-8")).expect("identity");
        let second =
            DurableFileIdentity::from_path(path.to_str().expect("utf-8")).expect("identity");
        assert_eq!(first, second);
    }
}
