//! SQLite implementation of the driver-neutral [`neotales_dbapi`] contract, built directly on
//! `rusqlite`.
//!
//! This crate exists to provide the same SQLite contract without `SQLx` anywhere in its dependency
//! graph. It selects `rusqlite` (with `libsqlite3-sys`) because it is the maintained safe wrapper
//! over the SQLite C API, exposes the interrupt handle, declared column types, cached statements,
//! and progress callbacks this contract needs, and vendors SQLite through its `bundled`
//! feature. The default build therefore compiles the vendored SQLite source with `cc` and needs no
//! system package or preinstalled `libsqlite3`. Disable default features to link the platform
//! library instead. The workspace forbids `unsafe` in this crate; all `unsafe` is inside rusqlite.
//!
//! # Worker model
//!
//! A [`rusqlite::Connection`] is `Send` but not `Sync`. Each physical connection is owned for its
//! whole life by one blocking worker thread. Callers send owned commands and receive owned rows
//! over channels, so no connection is touched by two threads and no `Mutex` is held across SQL.
//! Asynchronous callers await a one-shot reply; synchronous callers block on a reply. Pool capacity
//! is bounded by [`SqliteConfig::max_connections`].
//!
//! The pool reuses clean workers while they stay healthy and within [`SqliteConfig::idle_ttl`], and
//! pings them on checkout. A worker is quarantined after any dispatched failure, interrupted or
//! abandoned operation, or panic: it reports the failure, then closes. A closed connection rolls
//! back any open transaction. Capacity stays held until the worker thread has actually stopped, so
//! a dropped future never frees a slot while its statement may still be running.
//!
//! # Semantics retained from the SQLite contract
//!
//! SQLite has no boolean storage class: `Bool` parameters bind as integer 0/1 and fetched integers
//! remain `I64`. NULL values use the column's declared type affinity for typed nulls; an expression
//! without a declared type falls back to `Text` nulls. Ordinary statements accept anonymous `?`
//! placeholders or indexed `?NNN` placeholders, never both, and never rewrite SQL. `?` parameters
//! are bound positionally. `BEGIN IMMEDIATE` is available through [`ImmediateTransaction`].
//! Foreign keys are enabled on every connection, and the configured busy timeout bounds lock waits.
//!
//! There is no statement deadline. An explicitly requested timeout is rejected with
//! `Unsupported` before dispatch rather than silently ignored; the busy timeout is a SQLite
//! lock-wait setting, not a statement deadline. Cancellation is the drop of an async future: the
//! worker is interrupted, scripts check a latched flag at each statement boundary, and a progress
//! callback checks every 1,000 VM instructions. Cancellation is cooperative: preparation and a
//! short statement racing the last check can still finish, and earlier script effects may already
//! be committed. It is not proof of non-application. The connection is discarded, never reused.
//! Busy handlers, user functions, and VFS calls must still return before the worker can stop.
//!
//! # Durability
//!
//! [`SqliteDurability::Inherit`] changes nothing and leaves SQLite's journal mode and `synchronous`
//! setting in effect. [`SqliteDurability::Durable`] opens every connection with
//! `journal_mode=WAL` and `synchronous=FULL`, must be file-backed, and revalidates both settings
//! and the main-file identity at acquisition and before each write, transaction begin, trusted
//! script, and `COMMIT`. A mismatch or unreadable policy discards the connection and returns a
//! sanitized error; a failed pre-`COMMIT` check does not dispatch `COMMIT` and is not reported as
//! an unknown commit. Authorization, foreign keys, busy timeout, and initialisation remain the
//! caller's SQLite semantics.
//!
//! # In-memory databases
//!
//! Every physical SQLite connection to `:memory:` has its own private database. A multi-connection
//! pool would therefore hand out isolated databases under one name, which is a silent correctness
//! trap. In-memory names are accepted only with [`SqliteConfig::max_connections`] set to 1;
//! anything else is rejected at [`SqlitePoolProvider::connect`] with `InvalidArgument`. Durable
//! policy rejects in-memory databases as well.
//!
//! # Errors, retries, and logging
//!
//! Errors retain only a stable [`neotales_dbapi::ErrorKind`]. Driver messages, SQL text, bound
//! values, paths, and VFS names are dropped during translation. An abandoned async operation or a
//! worker panic cannot produce an authoritative result, so execute/fetch/script report
//! `WriteOutcomeUnknown` and commit reports `CommitOutcomeUnknown`; neither is retried. The provider
//! never retries on its own. Retry, telemetry, and SQL capture belong to
//! [`neotales_dbapi::PolicyPool`]; only standalone pool operations with an explicit
//! `ReplaySafety::ExplicitlyReplaySafe` assertion are eligible, and unknown outcomes never are.
//!
//! The crate has no `tracing` or `log` dependency and emits no records. rusqlite's `trace` feature
//! is off, so statement text, rows, and paths are not logged. SQL capture is opt-in on
//! `PolicyPool` because literals and comments can contain secrets.
//!
//! # Example
//!
//! ```rust
//! use std::time::Duration;
//! use neotales_dbapi::{DbError, Pool, QueryOptions, RowLimit, Session};
//! use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};
//!
//! # async fn example() -> Result<(), DbError> {
//! let provider = SqlitePoolProvider::connect(
//!     SqliteConfig::new("app.sqlite")
//!         .busy_timeout(Duration::from_secs(2))
//!         .max_connections(4),
//! )
//! .await?;
//! let mut session = provider.acquire_async().await?;
//! let row = session
//!     .fetch_one_async(
//!         "SELECT 1 AS answer",
//!         QueryOptions::new().limit(RowLimit::new(1, 64)?),
//!     )
//!     .await?;
//! assert_eq!(row.get::<i64>("answer")?, 1);
//! # Ok(())
//! # }
//! ```

mod error;
mod pool;
mod session;
mod sql;

pub use session::SqliteSession;

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use neotales_dbapi::{
    Capabilities, DbError, DbFuture, ErrorKind, ImmediateTransaction, Pool, QueryOptions, Session,
    sync,
};

use pool::PoolShared;

/// Durability policy applied to every physical connection in a [`SqlitePoolProvider`].
///
/// [`Inherit`](Self::Inherit) is the default and changes nothing: SQLite's own journal and
/// synchronization settings apply. [`Durable`](Self::Durable) requests write-ahead logging and
/// `synchronous=FULL` and verifies both values and the main-file identity at acquisition and at
/// every write and transaction boundary. It is a connection policy, not a guarantee against power
/// loss: filesystem and storage-device write caches, WAL checkpointing, backups, and retained event
/// copies remain the application's concern.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SqliteDurability {
    /// Leave SQLite's journal mode and synchronization setting unchanged.
    #[default]
    Inherit,
    /// Require `journal_mode=WAL` and `synchronous=FULL` on every physical connection and at every
    /// write and transaction boundary.
    ///
    /// The database must be file-backed. In-memory names are rejected with `InvalidArgument`. The
    /// policy and main-file identity are read back at acquisition and before every
    /// `execute_async`/`execute`, `fetch_async`/`fetch`, transaction statement, begin, trusted
    /// script, and `COMMIT`. A mismatch, a read failure, or a changed file identity discards the
    /// connection and returns a sanitized error; a failed pre-commit check does not dispatch
    /// `COMMIT`.
    ///
    /// A trusted script on a durable session may not contain the words `pragma`, `attach`,
    /// `detach`, or `load_extension` anywhere, including comments and string literals. That is a
    /// conservative byte exclusion, not a SQL parser, and it may reject valid SQL containing those
    /// words. Durable sessions also reject `ATTACH`/`DETACH` as the leading command of a single
    /// statement.
    ///
    /// WAL needs a filesystem that supports shared memory and locking. A competing connection can
    /// prevent the initial change to WAL; that also fails closed. The main-file identity is
    /// check-time evidence: an ABA replacement that restores the path before the next check is not
    /// detected, and non-Unix platforms compare the canonical path only. Attached databases are
    /// outside this policy. Keep the path stable, preferably absolute, for the provider lifetime.
    Durable,
}

/// Configuration for a file-backed SQLite pool.
pub struct SqliteConfig {
    pub(crate) database_path: PathBuf,
    pub(crate) busy_timeout: Duration,
    pub(crate) max_connections: u32,
    pub(crate) durability: SqliteDurability,
    pub(crate) idle_ttl: Duration,
}

impl SqliteConfig {
    /// Creates configuration with a five-second lock wait, five pooled connections, a sixty-second
    /// idle reuse window, and SQLite's inherited durability settings.
    ///
    /// All names beginning with `file:` are rejected at connect time, including file-backed URIs.
    /// Use a plain filesystem path instead. SQLite URI decoding can otherwise select temporary or
    /// memory databases and change connection semantics in ways this pool cannot safely classify.
    #[must_use]
    pub fn new(database_path: impl Into<PathBuf>) -> Self {
        Self {
            database_path: database_path.into(),
            busy_timeout: Duration::from_secs(5),
            max_connections: 5,
            durability: SqliteDurability::Inherit,
            idle_ttl: Duration::from_secs(60),
        }
    }

    /// Sets the SQLite busy-handler wait bound for every connection.
    ///
    /// This is a lock-wait bound, not a statement deadline. The value must be a positive whole
    /// number of milliseconds no greater than `i32::MAX`. A zero or over-range value is rejected at
    /// [`SqlitePoolProvider::connect`].
    ///
    /// Restored on each checkout, not before each statement in a caller-owned transaction.
    /// It does not bound pool admission or the whole transaction, provide FIFO writer admission,
    /// or guarantee concurrent writers succeed: SQLite can return `Busy` when the wait expires.
    #[must_use]
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }

    /// Sets the maximum number of live physical connections, including idle and leased ones.
    ///
    /// Zero is rejected at [`SqlitePoolProvider::connect`]. In-memory databases require exactly
    /// one connection because each connection would otherwise see a different database.
    #[must_use]
    pub fn max_connections(mut self, count: u32) -> Self {
        self.max_connections = count;
        self
    }

    /// Selects the durability policy applied to every physical connection.
    #[must_use]
    pub fn durability(mut self, policy: SqliteDurability) -> Self {
        self.durability = policy;
        self
    }

    /// Sets how long a clean idle connection may be reused before it is closed on checkout.
    ///
    /// A zero idle window is rejected at [`SqlitePoolProvider::connect`]. An expired connection is
    /// replaced before any SQL is dispatched, so this never replays a statement. An in-memory
    /// database ignores the window: its one physical connection holds the database, and replacing
    /// it would silently reset the data.
    #[must_use]
    pub fn idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = ttl;
        self
    }

    pub(crate) fn busy_timeout_value(&self) -> Duration {
        self.busy_timeout
    }

    pub(crate) fn idle_ttl_value(&self) -> Duration {
        self.idle_ttl
    }

    pub(crate) fn max_connections_usize(&self) -> usize {
        usize::try_from(self.max_connections).unwrap_or(usize::MAX)
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) fn validate(&self) -> Result<(), DbError> {
        let timeout_ms = self.busy_timeout.as_millis();
        if self.database_path.as_os_str().is_empty()
            || self.max_connections == 0
            || timeout_ms == 0
            || timeout_ms > i32::MAX as u128
            || self.idle_ttl.is_zero()
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        // rusqlite's default open flags enable SQLite's URI parser. Reject the scheme at the
        // encoded OS-string level, including non-UTF-8 paths, before any worker is spawned.
        if self
            .database_path
            .as_os_str()
            .as_encoded_bytes()
            .starts_with(b"file:")
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if is_in_memory_database(&self.database_path)
            && (self.max_connections != 1 || self.durability == SqliteDurability::Durable)
        {
            // Multiple physical connections to an in-memory name are separate databases, and a
            // durable policy needs a real file. Refuse instead of handing out isolated databases.
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(())
    }
}

impl fmt::Debug for SqliteConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteConfig")
            .field("database_path", &"[redacted]")
            .field("busy_timeout", &self.busy_timeout)
            .field("max_connections", &self.max_connections)
            .field("durability", &self.durability)
            .field("idle_ttl", &self.idle_ttl)
            .finish()
    }
}

/// Cloneable bounded SQLite provider.
///
/// Clones share one pool. Dropping every clone closes idle workers; active sessions remain
/// caller-owned. Call [`SqlitePoolProvider::close`] and
/// [`SqlitePoolProvider::wait_for_cleanup`] to shut down deterministically.
#[derive(Clone)]
pub struct SqlitePoolProvider {
    shared: Arc<PoolShared>,
}

impl fmt::Debug for SqlitePoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SqlitePoolProvider")
    }
}

impl SqlitePoolProvider {
    /// Opens a file-backed pool using the supplied configuration.
    ///
    /// The database file is created when absent and foreign keys are enabled. The first physical
    /// connection is opened on its worker thread and, with [`SqliteDurability::Durable`], verified
    /// before this call returns. A zero or over-range busy timeout, zero connection limit, zero
    /// idle TTL, any `file:` URI, and an
    /// in-memory name with more than one connection are rejected before any connection is opened.
    ///
    /// # Errors
    /// Returns `InvalidArgument`, `Busy`, `Unsupported`, or `Connection`, without retaining driver
    /// error text.
    pub async fn connect(config: SqliteConfig) -> Result<Self, DbError> {
        let shared = PoolShared::new(config)?;
        let worker = pool::open_initial_async(&shared).await?;
        shared.push_idle(worker);
        Ok(Self { shared })
    }

    /// Opens a pool synchronously, for callers that never use an async runtime.
    ///
    /// This blocks the calling thread while the first connection opens and is verified. It has the
    /// same validation and error rules as [`SqlitePoolProvider::connect`].
    ///
    /// # Errors
    /// Returns `InvalidArgument`, `Busy`, `Unsupported`, or `Connection`, without retaining driver
    /// error text.
    pub fn connect_sync(config: SqliteConfig) -> Result<Self, DbError> {
        let shared = PoolShared::new(config)?;
        let worker = pool::open_initial_sync(&shared)?;
        shared.push_idle(worker);
        Ok(Self { shared })
    }

    /// Returns the configured SQLite busy-handler wait bound.
    #[must_use]
    pub fn busy_timeout(&self) -> Duration {
        self.shared.config.busy_timeout_value()
    }

    /// Returns the durability policy applied to every acquired connection.
    #[must_use]
    pub fn durability(&self) -> SqliteDurability {
        self.shared.config.durability
    }

    /// Returns the configured maximum number of live physical connections.
    #[must_use]
    pub fn max_connections(&self) -> u32 {
        self.shared.config.max_connections
    }

    /// Returns the configured idle reuse window.
    #[must_use]
    pub fn idle_ttl(&self) -> Duration {
        self.shared.config.idle_ttl_value()
    }

    /// Returns connection slots not currently held by a live or opening worker.
    #[must_use]
    pub fn unused_connection_slots(&self) -> usize {
        self.shared.unused_slots()
    }

    /// Acquires an exclusive session with immediate-transaction support.
    ///
    /// The caller must drop the returned session to release its worker. With
    /// [`SqliteDurability::Durable`], the worker's settings and file identity are verified before
    /// the session is returned.
    ///
    /// # Errors
    /// Returns `Connection` when the pool is closed or cannot open a worker, or a sanitized
    /// `Busy`, `Unsupported`, or `Connection` error when a durable connection does not satisfy its
    /// policy.
    pub async fn acquire_sqlite(&self) -> Result<Box<dyn ImmediateTransaction>, DbError> {
        let worker = pool::acquire_async(&self.shared).await?;
        let mut session = SqliteSession::new(worker, Arc::clone(&self.shared));
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout().as_millis());
        session
            .execute_async(&busy_timeout, QueryOptions::new())
            .await?;
        Ok(Box::new(session))
    }

    /// Acquires the concrete synchronous session, which also exposes `begin_immediate`.
    ///
    /// This blocks the calling thread while a worker is acquired and its connection verified.
    ///
    /// # Errors
    /// Returns `Connection` when the pool is closed or cannot open a worker, or a sanitized
    /// `Busy`, `Unsupported`, or `Connection` error when a durable connection does not satisfy its
    /// policy.
    pub fn acquire_sync_sqlite(&self) -> Result<SqliteSession, DbError> {
        let worker = pool::acquire_sync(&self.shared)?;
        let mut session = SqliteSession::new(worker, Arc::clone(&self.shared));
        self.restore_sync_busy_timeout(&mut session)?;
        Ok(session)
    }

    /// Resets the configured timeout on an exclusively checked-out synchronous session.
    fn restore_sync_busy_timeout(&self, session: &mut SqliteSession) -> Result<(), DbError> {
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout().as_millis());
        sync::Session::execute(session, &busy_timeout, QueryOptions::new())?;
        Ok(())
    }

    /// Rejects new acquisitions and shuts down idle workers.
    ///
    /// Active sessions stay caller-owned. Outstanding sessions discard their worker on drop because
    /// the pool is closed. This is idempotent.
    pub fn close(&self) {
        pool::close(&self.shared);
    }

    /// Waits until every worker thread has stopped after [`SqlitePoolProvider::close`].
    ///
    /// Drop active sessions first; this is not a barrier for sessions that are still alive. A
    /// worker that ignores interruption (for example, an unkillable foreign function or VFS call)
    /// keeps the wait pending. Returns the first sanitized cleanup failure, such as a worker panic.
    ///
    /// # Errors
    /// Returns `Connection` if a worker thread panicked or a stored cleanup failure was recorded.
    pub async fn wait_for_cleanup(&self) -> Result<(), DbError> {
        pool::wait_for_cleanup(&self.shared).await
    }
}

impl neotales_dbapi::SqlitePool for SqlitePoolProvider {
    fn busy_timeout(&self) -> Duration {
        self.busy_timeout()
    }

    fn acquire_sqlite_async(&self) -> neotales_dbapi::DbFuture<'_, Box<dyn ImmediateTransaction>> {
        Box::pin(async move { self.acquire_sqlite().await })
    }
}

impl Pool for SqlitePoolProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            isolation_level: false,
            read_only_transactions: false,
            trusted_scripts: true,
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move {
            let worker = pool::acquire_async(&self.shared).await?;
            let mut session = SqliteSession::new(worker, Arc::clone(&self.shared));
            let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout().as_millis());
            session
                .execute_async(&busy_timeout, QueryOptions::new())
                .await?;
            Ok(Box::new(session) as Box<dyn Session>)
        })
    }
}

impl sync::Pool for SqlitePoolProvider {
    fn capabilities(&self) -> Capabilities {
        Pool::capabilities(self)
    }

    fn acquire(&self) -> Result<Box<dyn sync::Session>, DbError> {
        Ok(Box::new(self.acquire_sync_sqlite()?))
    }
}

/// Reports whether a configured name opens an in-memory SQLite database.
///
/// All SQLite URI names are rejected at validation, leaving only the explicit special filename.
fn is_in_memory_database(path: &Path) -> bool {
    path.as_os_str() == ":memory:"
}

#[cfg(test)]
mod tests {
    use super::{SqliteConfig, SqliteDurability, SqlitePoolProvider};
    use std::time::Duration;

    #[test]
    fn debug_output_redacts_configuration_paths() {
        let config = SqliteConfig::new("/secret/provider-path.sqlite");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("provider-path"));
        let provider_debug = format!(
            "{:?}",
            SqlitePoolProvider {
                shared: super::PoolShared::new(config).expect("valid configuration"),
            }
        );
        assert_eq!(provider_debug, "SqlitePoolProvider");
    }

    #[test]
    fn configuration_rejects_unusable_values() {
        let base = || SqliteConfig::new("app.sqlite");
        assert!(base().validate().is_ok());
        assert!(base().max_connections(0).validate().is_err());
        assert!(base().busy_timeout(Duration::ZERO).validate().is_err());
        assert!(base().idle_ttl(Duration::ZERO).validate().is_err());
        assert!(
            base()
                .busy_timeout(Duration::from_millis(i32::MAX as u64 + 1))
                .validate()
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_file_uri_is_rejected_before_pool_creation() {
        use std::os::unix::ffi::OsStringExt;
        let path = std::ffi::OsString::from_vec(b"file:\xff?mode=memory".to_vec());
        assert!(super::PoolShared::new(SqliteConfig::new(path).max_connections(1)).is_err());
    }

    #[test]
    fn in_memory_configuration_requires_one_connection_and_inherited_durability() {
        assert!(
            SqliteConfig::new(":memory:")
                .max_connections(1)
                .validate()
                .is_ok()
        );
        assert!(
            SqliteConfig::new(":memory:")
                .max_connections(2)
                .validate()
                .is_err()
        );
        assert!(
            SqliteConfig::new(":memory:")
                .max_connections(1)
                .durability(SqliteDurability::Durable)
                .validate()
                .is_err()
        );
    }
}
