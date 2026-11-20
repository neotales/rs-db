//! SQLite implementation of the driver-neutral [`neotales_dbapi`] contract.
//!
//! Configure the file-backed pool with [`SqliteConfig`]. The public API does not expose `SQLx` types.
//! SQLite has no boolean storage class: `Bool` parameters bind as integer 0/1 and fetched integers
//! remain `I64`. NULL values use their declared column affinity; untyped NULL expressions fall back
//! to typed `Text` nulls.
//!
//! SQL statement and slow-statement logging are disabled on provider connections so SQL text and
//! embedded literals are not emitted. `SQLx` pool lifecycle logging remains active, but for this
//! configuration the reachable SQLite ping/close errors are the fixed `WorkerCrashed` handshake
//! error and no pool hook is installed, so SQL text, bound values, SQLite messages, and file names
//! cannot reach those lines. The one `SQLx` log that embeds a SQLite error message is reachable
//! only through `SQLx`'s own transaction API, which this provider does not use. The cancellation
//! progress handler replaces any handler on each acquired connection and remains installed across
//! successful transaction completion and idle pool time. It interrupts SQLite VM execution, but
//! cannot interrupt a blocked user function, VFS call, or busy handler; the configured busy timeout
//! bounds lock waits but does not guarantee prompt cancellation.
//!
//! Statement `execute_async` and `fetch_async` accept one executable statement, with optional trailing
//! semicolons/comments. A conservative lexical boundary check recognizes SQLite single/double/
//! backtick/bracket quoting and line/block comments; it is not a SQL parser and rejects ambiguous
//! input, including ordinary `CREATE TRIGGER` bodies containing semicolons. The ordinary
//! positional binder accepts anonymous `?` placeholders or indexed `?NNN` placeholders, but
//! rejects mixing the forms. Use `execute_trusted_script_async` for trusted trigger definitions and
//! intentionally multi-statement trusted SQL.
//! Transaction-control SQL is rejected on standalone pool/session statement APIs; use
//! `Session::begin_async`, `commit_async`, and `rollback_async` through the DBAPI transaction
//! interfaces instead. This check is deliberately limited
//! to an unquoted ASCII leading command token after ASCII space, tab, CR, LF, form feed, and
//! comments. Other prefix forms (including BOM and vertical tab) fail closed with `InvalidArgument`,
//! even when SQLite accepts them. It does not claim to parse SQL generally.
//!
//! Validation failures detected before dispatch (including unsupported options, NUL-containing
//! SQL, placeholder/parameter count mismatches, mixed placeholder forms, and multiple statements)
//! do not invalidate a live session or transaction: they have not sent SQL to SQLite. A dispatched
//! statement failure or interruption invalidates and discards its connection; do not assume an
//! active transaction remains usable.
//!
//! [`SqliteDurability::Durable`] additionally opens every physical connection with
//! `journal_mode=WAL` and `synchronous=FULL`, then reads both settings and the main-file identity
//! back before handing the connection to a caller and again before every write, transaction,
//! trusted-script, and `COMMIT` boundary. A mismatch, a changed file identity, an in-memory
//! database, or a failed read fails closed with a sanitized [`DbError`]; a failed pre-commit check
//! does not dispatch `COMMIT`. A durable trusted script may not contain `pragma`, `attach`,
//! `detach`, or `load_extension` anywhere, including comments and literals: one script call could
//! otherwise change the policy and write before returning. That exclusion is a conservative byte
//! scan, not a SQL parser, and it may reject valid SQL containing those words. This is a
//! connection-level policy, not a promise of power-loss durability: the filesystem, storage
//! device, and their write caches still decide that. It also does not checkpoint the write-ahead
//! log, back up the database, vouch for attached databases, or retain events; those remain the
//! application's responsibility. The default [`SqliteDurability::Inherit`] leaves SQLite's own
//! settings untouched.

use futures_util::TryStreamExt;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, DbType, ErrorKind, ExecuteOptions,
    ImmediateTransaction, Parameter, Pool, PoolDefaults, QueryOptions, Reader, Row, Session,
    Transaction, TxOptions, Value,
};
use sqlx::{
    Column, ConnectOptions, Error as SqlxError, Row as SqlxRow, Sqlite, SqliteConnection,
    SqlitePool, TypeInfo, ValueRef,
    pool::PoolConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

macro_rules! bind_query {
    ($sql:expr, $parameters:expr) => {{
        let mut query = sqlx::query(sqlx::AssertSqlSafe($sql));
        for parameter in $parameters {
            query = match parameter.value() {
                Value::Null(DbType::Bool | DbType::I64) => query.bind(Option::<i64>::None),
                Value::Null(DbType::F64) => query.bind(Option::<f64>::None),
                Value::Null(DbType::Text) => query.bind(Option::<String>::None),
                Value::Null(DbType::Bytes) => query.bind(Option::<Vec<u8>>::None),
                Value::Bool(value) => query.bind(i64::from(*value)),
                Value::I64(value) => query.bind(*value),
                Value::F64(value) => query.bind(*value),
                Value::Text(value) => query.bind(value.as_str()),
                Value::Bytes(value) => query.bind(value.as_slice()),
            };
        }
        query
    }};
}

struct CancellationSignal {
    flag: Arc<AtomicBool>,
    armed: bool,
}

impl CancellationSignal {
    fn new(flag: &Arc<AtomicBool>) -> Self {
        Self {
            flag: Arc::clone(flag),
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CancellationSignal {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::Release);
        }
    }
}

/// SQLite provider configured through [`SqliteConfig`].
///
/// ```
/// use std::time::Duration;
/// use neotales_dbapi::DbError;
/// use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
///
/// # async fn example() -> Result<(), DbError> {
/// let config = SqliteConfig::new("app.sqlite")
///     .busy_timeout(Duration::from_secs(2))
///     .max_connections(4);
/// let provider = SqlitePoolProvider::connect(config).await?;
/// let mut session = provider.acquire_sqlite().await?;
/// let transaction = session.begin_immediate_async().await?;
/// transaction.rollback_async().await?;
/// # Ok(())
/// # }
/// ```
///
/// The optional core policy wrapper adds one-shot convenience methods:
///
/// ```no_run
/// use std::{sync::Arc, time::Duration};
/// use neotales_dbapi::{
///     Cancellation, DbObserver, Delay, Pool, PoolPolicy, PolicyPool, QueryOptions, QuerySetting,
///     RetryPolicy, RetryRuntime, RetrySetting, RowLimit, SqlCaptureHook,
/// };
/// use neotales_dbapi::DbError;
/// use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
///
/// # async fn example() -> Result<(), DbError> {
/// let provider = SqlitePoolProvider::connect(
///     SqliteConfig::new("app.sqlite").busy_timeout(Duration::from_secs(2)),
/// ).await?;
/// let observer: DbObserver = Arc::new(|event| {
///     // Forward only these safe fields to an application-owned telemetry sink.
///     let _safe = (event.operation, event.duration, event.outcome);
/// });
/// let sql_hook: SqlCaptureHook = Arc::new(|sql| Some(sql.to_owned()));
/// let cancellation = Arc::new(Cancellation::new());
/// let retry_runtime = RetryRuntime::new(
///     cancellation,
///     || Box::pin(tokio::time::sleep(Duration::from_secs(5))),
///     |delay| Box::pin(tokio::time::sleep(delay)),
///     || 0, // Replace with the caller's random source for jittered delays.
/// );
/// let pool = PolicyPool::new(provider, PoolPolicy {
///     telemetry_enabled: true,
///     sql_capture_enabled: true, // Explicit: SQL literals/comments may contain secrets.
///     sql_hook: Some(sql_hook),
///     observer: Some(observer),
///     retry: RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(25)), false)
///         .expect("valid retry settings"),
///     retry_runtime: Some(retry_runtime),
///     ..PoolPolicy::default()
/// });
/// let _rows = pool.fetch_async("SELECT 1", QueryOptions::new().limit(RowLimit::new(1, 1024)?)).await?;
/// // No SQL is inferred safe. This call opts into replay only because the caller
/// // knows the operation is safe to repeat.
/// let _count = pool.execute_async(
///     "DELETE FROM expired",
///     QueryOptions::new().replay_safe().retry(RetrySetting::Inherit),
/// ).await?;
/// let _rows_without_sql = pool.fetch_async(
///     "SELECT 2",
///     QueryOptions::new().limit(RowLimit::new(1, 1024)?).sql_capture(QuerySetting::Off),
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SqlitePoolProvider {
    pool: SqlitePool,
    busy_timeout: Duration,
    durability: SqliteDurability,
    identity: Option<DurableFileIdentity>,
}

/// Durability policy applied to every physical connection in a [`SqlitePoolProvider`].
///
/// [`Inherit`](Self::Inherit) is the default and changes nothing: SQLite's own journal and
/// synchronization settings apply. [`Durable`](Self::Durable) requests write-ahead logging and
/// `synchronous=FULL` in the `SQLx` connection options and verifies both values and the main-file
/// identity at acquisition and at every write and transaction boundary. It is a connection
/// policy, not a guarantee against power loss: filesystem and storage-device write caches, WAL
/// checkpointing, and backups remain the application's concern.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SqliteDurability {
    /// Leave SQLite's journal mode and synchronization setting unchanged.
    #[default]
    Inherit,
    /// Require `journal_mode=WAL` and `synchronous=FULL` on every physical connection and at every
    /// write and transaction boundary.
    ///
    /// The database must be file-backed. In-memory names (`:memory:`, `mode=memory` URIs, and the
    /// `memdb` VFS) are rejected with `InvalidArgument`. The policy and main-file identity are read
    /// back before every `execute_async`, `fetch_async`, transaction statement, `begin_async`, `begin_immediate_async`,
    /// trusted script, and `COMMIT`. A mismatch, a read failure, a busy result, or a changed file
    /// identity discards the connection and returns a sanitized `DbError`; a failed pre-commit
    /// check does not dispatch `COMMIT` and is not reported as `CommitOutcomeUnknown`. A single
    /// `PRAGMA` statement is allowed because migrations set `busy_timeout` and the queue reads
    /// `encoding`, but a durability-changing `PRAGMA` makes the next boundary fail closed.
    ///
    /// A trusted script on a durable session may not contain the words `pragma`, `attach`,
    /// `detach`, or `load_extension` anywhere, including comments and string literals. This is a
    /// conservative byte exclusion, not a SQL parser: SQLite needs the literal keyword for those
    /// statements, so the exclusion prevents a script from changing the policy or attaching a
    /// database and writing in one call. It may also reject valid SQL containing those words.
    /// Durable sessions reject `ATTACH`/`DETACH` as leading statements.
    ///
    /// WAL needs a filesystem that supports shared memory and locking. A competing connection can
    /// prevent the initial change to WAL; that also fails closed rather than returning an inherited
    /// setting. The main-file identity is check-time evidence: an ABA replacement that restores the
    /// path before the next check is not detected, and non-Unix platforms compare the canonical path
    /// only. Attached databases are outside this policy. Keep the path stable (prefer an absolute
    /// path) for the provider lifetime.
    Durable,
}

/// Configuration for a file-backed SQLite pool.
pub struct SqliteConfig {
    database_path: PathBuf,
    busy_timeout: Duration,
    max_connections: u32,
    durability: SqliteDurability,
}

impl SqliteConfig {
    /// Creates file-backed configuration with a five-second lock wait, five pooled connections, and
    /// SQLite's inherited durability settings.
    #[must_use]
    pub fn new(database_path: impl Into<PathBuf>) -> Self {
        Self {
            database_path: database_path.into(),
            busy_timeout: Duration::from_secs(5),
            max_connections: 5,
            durability: SqliteDurability::Inherit,
        }
    }

    /// Sets the SQLite busy-handler wait bound.
    ///
    /// Restored on each checkout, not before each statement in a caller-owned transaction.
    /// It bounds a SQLite lock wait, not pool admission or the whole transaction. It does not
    /// provide FIFO writer admission or guarantee concurrent writers succeed; an exhausted wait
    /// can return `Busy`.
    #[must_use]
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }

    /// Sets the maximum number of pooled SQLite connections.
    #[must_use]
    pub fn max_connections(mut self, count: u32) -> Self {
        self.max_connections = count;
        self
    }

    /// Selects the durability policy applied to every physical connection.
    ///
    /// The default is [`SqliteDurability::Inherit`], which matches SQLite's own settings.
    /// [`SqliteDurability::Durable`] requires a file-backed database and fails closed if
    /// `journal_mode=WAL` or `synchronous=FULL` cannot be verified.
    #[must_use]
    pub fn durability(mut self, policy: SqliteDurability) -> Self {
        self.durability = policy;
        self
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
            .finish()
    }
}

impl fmt::Debug for SqlitePoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SqlitePoolProvider")
    }
}

impl SqlitePoolProvider {
    /// Opens a file-backed pool using the supplied provider configuration.
    ///
    /// The database file is created when absent. A zero busy timeout, zero connection limit, or
    /// timeout above SQLite's supported millisecond range is rejected. With
    /// [`SqliteDurability::Durable`], an in-memory database name is rejected, and the first
    /// physical connection is verified before the provider is returned.
    ///
    /// # Errors
    /// Returns `InvalidArgument`, `Busy`, `Unsupported`, or `Connection`, without retaining driver
    /// error text.
    pub async fn connect(config: SqliteConfig) -> Result<Self, DbError> {
        let timeout_ms = config.busy_timeout.as_millis();
        if config.database_path.as_os_str().is_empty()
            || config.max_connections == 0
            || timeout_ms == 0
            || timeout_ms > i32::MAX as u128
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if config.durability == SqliteDurability::Durable
            && is_in_memory_database(&config.database_path)
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        let mut connect_options = SqliteConnectOptions::new()
            .filename(config.database_path)
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(config.busy_timeout)
            .disable_statement_logging();
        if config.durability == SqliteDurability::Durable {
            connect_options = connect_options
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Full);
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(connect_options)
            .await
            .map_err(map_error)?;
        let mut provider = Self {
            pool,
            busy_timeout: config.busy_timeout,
            durability: config.durability,
            identity: None,
        };
        if provider.durability == SqliteDurability::Durable {
            // Fail the open, rather than a later operation, when the first connection cannot
            // satisfy the requested policy. `connect_with` has already opened it; this reads the
            // settings back and discards the connection on a mismatch.
            let mut connection = provider
                .pool
                .acquire()
                .await
                .map_err(|_| DbError::new(ErrorKind::Connection))?;
            let main_file = match durable_snapshot(&mut connection).await {
                Ok(main_file) => main_file,
                Err(error) => {
                    connection.close_on_drop();
                    return Err(error);
                }
            };
            let identity = match DurableFileIdentity::from_path(&main_file) {
                Ok(identity) => identity,
                Err(error) => {
                    connection.close_on_drop();
                    return Err(error);
                }
            };
            provider.identity = Some(identity);
            drop(connection);
        }
        Ok(provider)
    }

    /// Returns the configured SQLite busy-handler wait bound for every newly opened pool connection.
    #[must_use]
    pub const fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    /// Returns the durability policy this provider applies to every acquired connection.
    #[must_use]
    pub const fn durability(&self) -> SqliteDurability {
        self.durability
    }

    /// Acquires an exclusive SQLite session with immediate-transaction support.
    ///
    /// The caller must drop the returned session to release its pool connection. With
    /// [`SqliteDurability::Durable`], the connection's journal mode and synchronous setting are
    /// read back before the session is returned.
    ///
    /// # Errors
    /// Returns `Connection` when the pool cannot acquire a usable SQLite connection, or a
    /// sanitized `Busy`, `Unsupported`, or `Connection` error when a durable connection does not
    /// satisfy its policy.
    pub async fn acquire_sqlite(&self) -> Result<Box<dyn ImmediateTransaction>, DbError> {
        let mut session =
            SqliteSession::acquire(&self.pool, self.durability, self.identity.as_ref()).await?;
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout.as_millis());
        session
            .execute_async(&busy_timeout, QueryOptions::new())
            .await?;
        Ok(Box::new(session))
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
            let mut session =
                SqliteSession::acquire(&self.pool, self.durability, self.identity.as_ref()).await?;
            let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout.as_millis());
            session
                .execute_async(&busy_timeout, QueryOptions::new())
                .await?;
            Ok(Box::new(session) as Box<dyn Session>)
        })
    }
}

struct SqliteSession {
    connection: PoolConnection<Sqlite>,
    cancelled: Arc<AtomicBool>,
    invalidated: Arc<AtomicBool>,
    reusable: bool,
    in_transaction: bool,
    durability: SqliteDurability,
    identity: Option<DurableFileIdentity>,
}

impl SqliteSession {
    async fn acquire(
        pool: &SqlitePool,
        durability: SqliteDurability,
        identity: Option<&DurableFileIdentity>,
    ) -> Result<Self, DbError> {
        let mut session = Self {
            connection: pool
                .acquire()
                .await
                .map_err(|_| DbError::new(ErrorKind::Connection))?,
            cancelled: Arc::new(AtomicBool::new(false)),
            invalidated: Arc::new(AtomicBool::new(false)),
            reusable: false,
            in_transaction: false,
            durability,
            identity: identity.cloned(),
        };
        if durability == SqliteDurability::Durable {
            // The connect options request WAL and FULL, but never hand a connection out on the
            // strength of the request. A failed read or a different value discards this
            // connection, so a pooled replacement cannot bypass the policy.
            let Some(identity) = identity else {
                return Err(DbError::new(ErrorKind::Unsupported));
            };
            verify_durable(&mut session.connection, identity).await?;
        }
        let cancelled = Arc::clone(&session.cancelled);
        session
            .connection
            .lock_handle()
            .await
            .map_err(|_| DbError::new(ErrorKind::Connection))?
            .set_progress_handler(1000, move || !cancelled.load(Ordering::Acquire));
        Ok(session)
    }

    /// Re-reads the policy and main-file identity before a write or transaction boundary.
    ///
    /// A mismatch or failed read discards the connection: the caller must not reuse it, and an
    /// open transaction is rolled back when the connection closes.
    async fn verify_policy(&mut self) -> Result<(), DbError> {
        if self.durability != SqliteDurability::Durable {
            return Ok(());
        }
        let Some(identity) = self.identity.as_ref() else {
            return Err(DbError::new(ErrorKind::Unsupported));
        };
        match verify_durable(&mut self.connection, identity).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    fn raw(&mut self) -> &mut SqliteConnection {
        &mut self.connection
    }

    fn clean(&mut self) -> Result<(), DbError> {
        if self.in_transaction {
            self.connection.close_on_drop();
            self.reusable = true;
            self.in_transaction = false;
            return Err(DbError::new(ErrorKind::Connection));
        }
        self.reusable = true;
        Ok(())
    }

    fn invalidate(&mut self) {
        self.invalidated.store(true, Ordering::Release);
        self.cancelled.store(true, Ordering::Release);
        self.connection.close_on_drop();
        self.reusable = true;
    }

    fn ensure_live(&mut self) -> Result<(), DbError> {
        if self.invalidated.load(Ordering::Acquire) {
            self.invalidate();
            Err(DbError::new(ErrorKind::Connection))
        } else if self.cancelled.load(Ordering::Acquire) {
            self.invalidate();
            Err(DbError::new(ErrorKind::Cancelled))
        } else {
            Ok(())
        }
    }
}

impl Drop for SqliteSession {
    fn drop(&mut self) {
        if !self.reusable || self.cancelled.load(Ordering::Acquire) || self.in_transaction {
            self.cancelled.store(true, Ordering::Release);
            self.connection.close_on_drop();
        }
    }
}

impl Session for SqliteSession {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            check_sql(sql)?;
            check_one_statement(sql)?;
            check_not_transaction_control(sql)?;
            if self.durability == SqliteDurability::Durable {
                check_durable_command(sql)?;
            }
            check_arity(sql, parameters)?;
            self.ensure_live()?;
            self.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.cancelled);
            let query = bind_query!(sql, parameters);
            let result = query.execute(self.raw()).await.map_err(map_write_error);
            match result {
                Ok(done) => {
                    cancellation.disarm();
                    self.reusable = true;
                    Ok(done.rows_affected())
                }
                Err(error) => {
                    drop(cancellation);
                    self.invalidate();
                    Err(error)
                }
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            let limit = options.prepare_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            check_sql(sql)?;
            check_one_statement(sql)?;
            check_not_transaction_control(sql)?;
            if self.durability == SqliteDurability::Durable {
                check_durable_command(sql)?;
            }
            check_arity(sql, parameters)?;
            self.ensure_live()?;
            self.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.cancelled);
            let mut rows = bind_query!(sql, parameters).fetch(self.raw());
            let mut result = Vec::new();
            let mut bytes = 0usize;
            let mut failure = None;
            loop {
                let row = match rows.try_next().await {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(error) => {
                        // Fetch can execute INSERT ... RETURNING. Without proof of read-only SQL,
                        // transport/interruption failures must not authorize replay of a write.
                        failure = Some(map_write_error(error));
                        break;
                    }
                };
                if result.len() == limit.rows() {
                    failure = Some(DbError::new(ErrorKind::Overflow));
                    break;
                }
                let mut cells = Vec::with_capacity(row.len());
                let mut row_failed = false;
                for (index, column) in row.columns().iter().enumerate() {
                    let name = column.name().to_owned();
                    let Some(next_bytes) = bytes.checked_add(name.len()) else {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    };
                    bytes = next_bytes;
                    let value = match decode(&row, index) {
                        Ok(value) => value,
                        Err(error) => {
                            failure = Some(error);
                            row_failed = true;
                            break;
                        }
                    };
                    let Some(next_bytes) = bytes.checked_add(value_bytes(&value)) else {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    };
                    bytes = next_bytes;
                    if bytes > limit.bytes() {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    }
                    cells.push((name, value));
                }
                if row_failed {
                    break;
                }
                result.push(Row::new(cells)?);
            }
            drop(rows);
            if let Some(error) = failure {
                drop(cancellation);
                self.invalidate();
                return Err(error);
            }
            cancellation.disarm();
            self.reusable = true;
            Ok(Box::new(BufferedReader::new(result)) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.prepare_script_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            check_sql(sql)?;
            if self.durability == SqliteDurability::Durable {
                check_durable_script(sql)?;
            }
            self.ensure_live()?;
            self.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.cancelled);
            match sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(&mut *self.connection)
                .await
            {
                Ok(_) => {
                    cancellation.disarm();
                    self.reusable = true;
                    Ok(())
                }
                Err(error) => {
                    drop(cancellation);
                    self.invalidate();
                    Err(map_error(error))
                }
            }
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
            self.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.cancelled);
            if let Err(error) = sqlx::query("BEGIN").execute(self.raw()).await {
                drop(cancellation);
                self.invalidate();
                return Err(map_error(error));
            }
            cancellation.disarm();
            self.in_transaction = true;
            Ok(Box::new(SqliteTransaction {
                session: self,
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

impl ImmediateTransaction for SqliteSession {
    fn begin_immediate_async<'a>(&'a mut self) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            self.ensure_live()?;
            self.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.cancelled);
            if let Err(error) = sqlx::query("BEGIN IMMEDIATE").execute(self.raw()).await {
                drop(cancellation);
                self.invalidate();
                return Err(map_error(error));
            }
            cancellation.disarm();
            self.in_transaction = true;
            Ok(Box::new(SqliteTransaction {
                session: self,
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

impl neotales_dbapi::SqlitePool for SqlitePoolProvider {
    fn busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    fn acquire_sqlite_async(&self) -> neotales_dbapi::DbFuture<'_, Box<dyn ImmediateTransaction>> {
        Box::pin(async move { self.acquire_sqlite().await })
    }
}

struct SqliteTransaction<'session> {
    session: &'session mut SqliteSession,
    done: bool,
}

impl<'session> Transaction<'session> for SqliteTransaction<'session> {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            check_sql(sql)?;
            check_one_statement(sql)?;
            if self.session.durability == SqliteDurability::Durable {
                check_durable_command(sql)?;
            }
            check_arity(sql, parameters)?;
            self.session.ensure_live()?;
            self.session.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.session.cancelled);
            match bind_query!(sql, parameters)
                .execute(self.session.raw())
                .await
            {
                Ok(result) => {
                    cancellation.disarm();
                    Ok(result.rows_affected())
                }
                Err(error) => {
                    drop(cancellation);
                    self.session.invalidate();
                    Err(map_error(error))
                }
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            let limit = options.prepare_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            check_sql(sql)?;
            check_one_statement(sql)?;
            if self.session.durability == SqliteDurability::Durable {
                check_durable_command(sql)?;
            }
            check_arity(sql, parameters)?;
            self.session.ensure_live()?;
            self.session.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.session.cancelled);
            let mut stream = bind_query!(sql, parameters).fetch(self.session.raw());
            let mut result = Vec::new();
            let mut bytes = 0usize;
            let mut failure = None;
            loop {
                let row = match stream.try_next().await {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(error) => {
                        failure = Some(map_error(error));
                        break;
                    }
                };
                if result.len() == limit.rows() {
                    failure = Some(DbError::new(ErrorKind::Overflow));
                    break;
                }
                let mut cells = Vec::new();
                let mut row_failed = false;
                for (index, column) in row.columns().iter().enumerate() {
                    let name = column.name().to_owned();
                    let Some(next_bytes) = bytes.checked_add(name.len()) else {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    };
                    bytes = next_bytes;
                    let value = match decode(&row, index) {
                        Ok(value) => value,
                        Err(error) => {
                            failure = Some(error);
                            row_failed = true;
                            break;
                        }
                    };
                    let Some(next_bytes) = bytes.checked_add(value_bytes(&value)) else {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    };
                    bytes = next_bytes;
                    if bytes > limit.bytes() {
                        failure = Some(DbError::new(ErrorKind::Overflow));
                        row_failed = true;
                        break;
                    }
                    cells.push((name, value));
                }
                if row_failed {
                    break;
                }
                result.push(Row::new(cells)?);
            }
            drop(stream);
            if let Some(error) = failure {
                drop(cancellation);
                self.session.invalidate();
                return Err(error);
            }
            cancellation.disarm();
            Ok(Box::new(BufferedReader::new(result)) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.prepare_script_provider(PoolDefaults::default())?;
            check_options(options.provider_execute_options())?;
            check_sql(sql)?;
            if self.session.durability == SqliteDurability::Durable {
                check_durable_script(sql)?;
            }
            self.session.ensure_live()?;
            self.session.verify_policy().await?;
            let cancellation = CancellationSignal::new(&self.session.cancelled);
            match sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(&mut *self.session.connection)
                .await
            {
                Ok(_) => {
                    cancellation.disarm();
                    Ok(())
                }
                Err(error) => {
                    drop(cancellation);
                    self.session.invalidate();
                    Err(map_error(error))
                }
            }
        })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            if let Err(error) = this.session.ensure_live() {
                this.session.in_transaction = false;
                this.done = true;
                return Err(error);
            }
            if let Err(error) = this.session.verify_policy().await {
                this.session.in_transaction = false;
                this.done = true;
                return Err(error);
            }
            let cancellation = CancellationSignal::new(&this.session.cancelled);
            let result = sqlx::query("COMMIT").execute(this.session.raw()).await;
            if result.is_ok() {
                cancellation.disarm();
                this.session.in_transaction = false;
                this.done = true;
                this.session.clean()
            } else {
                drop(cancellation);
                this.session.invalidate();
                this.session.in_transaction = false;
                this.done = true;
                Err(DbError::new(ErrorKind::CommitOutcomeUnknown))
            }
        })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            let cancellation = CancellationSignal::new(&this.session.cancelled);
            if sqlx::query("ROLLBACK")
                .execute(this.session.raw())
                .await
                .is_ok()
            {
                cancellation.disarm();
                this.session.in_transaction = false;
                this.done = true;
                this.session.clean()
            } else {
                drop(cancellation);
                this.session.invalidate();
                this.session.in_transaction = false;
                this.done = true;
                Err(DbError::new(ErrorKind::Connection))
            }
        })
    }
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.session.invalidate();
            self.session.in_transaction = false;
        }
    }
}

fn check_options(options: ExecuteOptions) -> Result<(), DbError> {
    if options.timeout_ms.is_some() {
        Err(DbError::new(ErrorKind::Unsupported))
    } else {
        Ok(())
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
struct DurableFileIdentity {
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

/// Reads the settings back from one physical connection for [`SqliteDurability::Durable`].
///
/// The table-valued pragma functions read the connection's actual journal mode and synchronous
/// level, not the values requested in the connect options. The main database must also resolve to
/// a file, so a memory-backed name that passed the configuration check cannot be used as a durable
/// queue or archive. Any read error is sanitized by [`map_error`]; a different value returns
/// [`ErrorKind::Unsupported`], and the caller discards the connection.
async fn durable_snapshot(connection: &mut SqliteConnection) -> Result<String, DbError> {
    let (journal_mode, synchronous, main_file): (String, i64, Option<String>) =
        sqlx::query_as(DURABILITY_SQL)
            .fetch_one(&mut *connection)
            .await
            .map_err(map_error)?;
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

/// Revalidates the policy and the main-file identity before a durable boundary.
///
/// A settings mismatch or identity change returns [`ErrorKind::Unsupported`]; a failed read or
/// filesystem metadata call returns a sanitized [`ErrorKind::Connection`] (or a driver-derived
/// sanitized kind). The caller discards the connection.
async fn verify_durable(
    connection: &mut SqliteConnection,
    expected: &DurableFileIdentity,
) -> Result<(), DbError> {
    let main_file = durable_snapshot(connection).await?;
    let actual = DurableFileIdentity::from_path(&main_file)?;
    if actual == *expected {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

/// SQLite reports `synchronous=FULL` as the integer 2.
const DURABLE_SYNCHRONOUS: i64 = 2;

/// Reads the connection's journal mode, synchronous level, and main database file in one query.
const DURABILITY_SQL: &str = "SELECT (SELECT journal_mode FROM pragma_journal_mode) AS journal_mode, (SELECT synchronous FROM pragma_synchronous) AS synchronous, (SELECT file FROM pragma_database_list WHERE name = 'main') AS main_file";

/// Reports whether a configured name opens an in-memory SQLite database.
///
/// SQLite always opens filenames with URI handling enabled in this provider, so the `file:` forms
/// matter even though [`SqliteConfig`] accepts a path. The `memdb` VFS is memory-only as well.
/// A custom memory-backed VFS cannot be identified from its name; the per-connection main-file
/// check and the caller's stable file-path prerequisite cover the rest.
fn is_in_memory_database(path: &Path) -> bool {
    let Some(name) = path.to_str() else {
        return false;
    };
    if name == ":memory:" {
        return true;
    }
    let Some(uri) = name.strip_prefix("file:") else {
        return false;
    };
    let (target, query) = uri.split_once('?').unwrap_or((uri, ""));
    if target == ":memory:" {
        return true;
    }
    query.split('&').any(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key.eq_ignore_ascii_case("mode") && value.eq_ignore_ascii_case("memory"))
            || (key.eq_ignore_ascii_case("vfs") && value.eq_ignore_ascii_case("memdb"))
    })
}

fn check_sql(sql: &str) -> Result<(), DbError> {
    if sql.as_bytes().contains(&0) {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

/// Conservatively rejects additional executable statements and malformed lexical constructs.
/// This scans only SQLite quote/comment boundaries; it is not a general SQL parser.
fn check_one_statement(sql: &str) -> Result<(), DbError> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Single,
        Double,
        Backtick,
        Bracket,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut index = 0;
    let mut has_code = false;
    let mut terminated = false;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            State::Normal => match byte {
                b' ' | b'\t' | b'\r' | b'\n' | b'\x0c' => {}
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    state = State::LineComment;
                    index += 1;
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    state = State::BlockComment;
                    index += 1;
                }
                b';' => {
                    if !terminated {
                        if !has_code {
                            return Err(DbError::new(ErrorKind::InvalidArgument));
                        }
                        terminated = true;
                    }
                }
                b'\'' | b'"' | b'`' | b'[' => {
                    if terminated {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    has_code = true;
                    state = match byte {
                        b'\'' => State::Single,
                        b'"' => State::Double,
                        b'`' => State::Backtick,
                        _ => State::Bracket,
                    };
                }
                _ => {
                    if terminated {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    has_code = true;
                }
            },
            State::Single if byte == b'\'' => {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Double if byte == b'"' => {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Backtick if byte == b'`' => {
                if bytes.get(index + 1) == Some(&b'`') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Bracket if byte == b']' => state = State::Normal,
            State::LineComment if byte == b'\n' => state = State::Normal,
            State::BlockComment if byte == b'*' && bytes.get(index + 1) == Some(&b'/') => {
                state = State::Normal;
                index += 1;
            }
            _ => {}
        }
        index += 1;
    }
    if !matches!(state, State::Normal | State::LineComment) || !has_code {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    Ok(())
}

/// Returns the first unquoted ASCII command token after a narrow set of whitespace/comments.
///
/// This is intentionally SQLite-specific lexical screening, not a general SQL parser. A leading
/// form it does not recognize fails closed with `InvalidArgument`.
fn leading_command(sql: &str) -> Result<&[u8], DbError> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    loop {
        while bytes
            .get(index)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b'\x0c'))
        {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"--") {
            index += 2;
            while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && bytes.get(index..index + 2) != Some(b"*/") {
                index += 1;
            }
            if index + 1 >= bytes.len() {
                return Err(DbError::new(ErrorKind::InvalidArgument));
            }
            index += 2;
            continue;
        }
        break;
    }

    if !bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    let start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    Ok(&bytes[start..index])
}

/// Rejects transaction-control commands on statement methods; callers must use transaction APIs.
fn check_not_transaction_control(sql: &str) -> Result<(), DbError> {
    transaction_control_word(leading_command(sql)?)
}

/// Rejects `ATTACH`/`DETACH` as a single statement on durable sessions.
///
/// Attached databases are outside the per-pool policy, so a durable session must not create one.
/// A form the lexical lead-token scan does not recognize is left to SQLite; this check adds only
/// the `ATTACH`/`DETACH` rejection and is not a general SQL parser.
fn check_durable_command(sql: &str) -> Result<(), DbError> {
    let Ok(token) = leading_command(sql) else {
        return Ok(());
    };
    if token.eq_ignore_ascii_case(b"ATTACH") || token.eq_ignore_ascii_case(b"DETACH") {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

/// Rejects durable trusted scripts that contain a policy-changing or cross-database keyword.
///
/// A trusted script is the only public call that can execute several statements before returning.
/// SQLite requires the literal ASCII keyword for a `PRAGMA`, `ATTACH`, `DETACH`, or
/// `load_extension` statement, so excluding those words anywhere in the script prevents such a
/// statement from running. This is a conservative byte scan, not a SQL parser: a script is also
/// rejected when a keyword appears in a comment or string literal.
fn check_durable_script(sql: &str) -> Result<(), DbError> {
    let bytes = sql.as_bytes();
    if DURABLE_SCRIPT_KEYWORDS
        .iter()
        .any(|keyword| contains_ascii_case_insensitive(bytes, keyword))
    {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

const DURABLE_SCRIPT_KEYWORDS: [&[u8]; 4] = [b"pragma", b"attach", b"detach", b"load_extension"];

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

fn transaction_control_word(token: &[u8]) -> Result<(), DbError> {
    if token.eq_ignore_ascii_case(b"BEGIN")
        || token.eq_ignore_ascii_case(b"COMMIT")
        || token.eq_ignore_ascii_case(b"END")
        || token.eq_ignore_ascii_case(b"ROLLBACK")
        || token.eq_ignore_ascii_case(b"SAVEPOINT")
        || token.eq_ignore_ascii_case(b"RELEASE")
    {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

fn check_arity(sql: &str, parameters: &[Parameter]) -> Result<(), DbError> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Single,
        Double,
        Backtick,
        Bracket,
        LineComment,
        BlockComment,
    }
    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut index = 0;
    let mut maximum = 0usize;
    let mut indexed = false;
    let mut anonymous = false;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            State::Normal => match byte {
                b'\'' => state = State::Single,
                b'"' => state = State::Double,
                b'`' => state = State::Backtick,
                b'[' => state = State::Bracket,
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    state = State::LineComment;
                    index += 1;
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    state = State::BlockComment;
                    index += 1;
                }
                b'?' => {
                    index += 1;
                    let start = index;
                    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                        index += 1;
                    }
                    let number = if index > start {
                        indexed = true;
                        std::str::from_utf8(&bytes[start..index])
                            .ok()
                            .and_then(|digits| digits.parse::<usize>().ok())
                            .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))?
                    } else {
                        anonymous = true;
                        maximum
                            .checked_add(1)
                            .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))?
                    };
                    if number == 0 {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    maximum = maximum.max(number);
                    continue;
                }
                b':' | b'@' | b'$' => return Err(DbError::new(ErrorKind::Unsupported)),
                _ => {}
            },
            State::Single if byte == b'\'' => {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Double if byte == b'"' => {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Backtick if byte == b'`' => state = State::Normal,
            State::Bracket if byte == b']' => state = State::Normal,
            State::LineComment if byte == b'\n' => state = State::Normal,
            State::BlockComment if byte == b'*' && bytes.get(index + 1) == Some(&b'/') => {
                state = State::Normal;
                index += 1;
            }
            _ => {}
        }
        index += 1;
    }
    if indexed && anonymous {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    if maximum == parameters.len() {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::InvalidArgument))
    }
}

fn decode(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<Value, DbError> {
    let raw = row.try_get_raw(index).map_err(map_error)?;
    if raw.is_null() {
        let name = row.columns()[index].type_info().name().to_ascii_uppercase();
        let kind = if name.contains("INT") {
            DbType::I64
        } else if name.contains("REAL") || name.contains("FLOA") || name.contains("DOUB") {
            DbType::F64
        } else if name.contains("BLOB") {
            DbType::Bytes
        } else if name.contains("BOOL") {
            DbType::Bool
        } else {
            DbType::Text
        };
        return Ok(Value::Null(kind));
    }
    let type_info = raw.type_info();
    let type_name = type_info.name();
    match type_name {
        "INTEGER" => row
            .try_get::<i64, _>(index)
            .map(Value::I64)
            .map_err(map_error),
        "REAL" => row
            .try_get::<f64, _>(index)
            .map(Value::F64)
            .map_err(map_error),
        "TEXT" => row
            .try_get::<String, _>(index)
            .map(Value::Text)
            .map_err(map_error),
        "BLOB" => row
            .try_get::<Vec<u8>, _>(index)
            .map(Value::Bytes)
            .map_err(map_error),
        _ => Err(DbError::new(ErrorKind::Decode)),
    }
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Null(_) => 0,
        Value::Bool(_) => 1,
        Value::I64(_) | Value::F64(_) => 8,
        Value::Text(text) => text.len(),
        Value::Bytes(bytes) => bytes.len(),
    }
}

fn map_error(error: SqlxError) -> DbError {
    match error {
        SqlxError::InvalidArgument(_) | SqlxError::Encode(_) => {
            DbError::new(ErrorKind::InvalidArgument)
        }
        SqlxError::ColumnDecode { .. } | SqlxError::Decode(_) => DbError::new(ErrorKind::Decode),
        SqlxError::PoolTimedOut => DbError::new(ErrorKind::Timeout),
        SqlxError::Database(database) => {
            let primary_code = database
                .code()
                .and_then(|code| code.parse::<i32>().ok())
                .map(|code| code & 0xff);
            if matches!(
                database.kind(),
                sqlx::error::ErrorKind::UniqueViolation
                    | sqlx::error::ErrorKind::ForeignKeyViolation
                    | sqlx::error::ErrorKind::NotNullViolation
                    | sqlx::error::ErrorKind::CheckViolation
            ) || primary_code == Some(19)
            {
                DbError::new(ErrorKind::Constraint)
            } else {
                match primary_code {
                    Some(5 | 6) => DbError::new(ErrorKind::Busy),
                    Some(9) => DbError::new(ErrorKind::Cancelled),
                    _ => DbError::new(ErrorKind::Connection),
                }
            }
        }
        _ => DbError::new(ErrorKind::Connection),
    }
}

fn map_write_error(error: SqlxError) -> DbError {
    let mapped = map_error(error);
    match mapped.kind() {
        ErrorKind::Connection | ErrorKind::Timeout | ErrorKind::Cancelled => {
            DbError::new(ErrorKind::WriteOutcomeUnknown)
        }
        _ => mapped,
    }
}

#[cfg(test)]
mod tests {
    use super::{DurableFileIdentity, verify_durable};
    use neotales_dbapi::ErrorKind;
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};
    use std::time::Duration;

    /// Unit test: a private helper with a mismatch branch that a public path reaches only on a
    /// filesystem or SQLite build that silently declines WAL.
    #[tokio::test]
    async fn durable_readback_requires_wal_and_full_on_the_connection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("readback.sqlite");
        let mut connection = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_millis(100))
            .connect()
            .await
            .unwrap();
        let identity = DurableFileIdentity::from_path(path.to_str().unwrap()).unwrap();

        assert_eq!(
            verify_durable(&mut connection, &identity)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported,
            "a rollback-journal connection must not satisfy the durable profile"
        );

        sqlx::raw_sql("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .execute(&mut connection)
            .await
            .unwrap();
        verify_durable(&mut connection, &identity).await.unwrap();
        connection.close().await.unwrap();
    }
}
