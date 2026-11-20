#![doc = include_str!("../README.md")]

use futures_util::TryStreamExt;
use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, DbType, ErrorKind, ExecuteOptions,
    IsolationLevel, Parameter, Pool, PoolDefaults, QueryOptions, Reader, Row, RowLimit, Session,
    Transaction, TxOptions, Value,
};
use sqlx::{
    Column, ConnectOptions, Error as SqlxError, Row as SqlxRow, ValueRef, pool::PoolConnection,
};
use sqlx_mssql_rs_core::{
    Mssql, MssqlConnectOptions, MssqlConnection, MssqlEncryption, MssqlPool, MssqlPoolOptions,
    MssqlResultEvent, MssqlRow,
};
use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use url::Url;
use zeroize::Zeroizing;

macro_rules! bind_query {
    ($sql:expr, $parameters:expr) => {{
        let mut query = sqlx::query(sqlx::AssertSqlSafe($sql));
        for parameter in $parameters {
            query = match parameter.value() {
                Value::Null(DbType::Bool) => query.bind(Option::<bool>::None),
                Value::Null(DbType::I64) => query.bind(Option::<i64>::None),
                Value::Null(DbType::F64) => query.bind(Option::<f64>::None),
                Value::Null(DbType::Text) => query.bind(Option::<String>::None),
                Value::Null(DbType::Bytes) => query.bind(Option::<Vec<u8>>::None),
                Value::Bool(value) => query.bind(*value),
                Value::I64(value) => query.bind(*value),
                Value::F64(value) => query.bind(*value),
                Value::Text(value) => query.bind(value.as_str()),
                Value::Bytes(value) => query.bind(value.as_slice()),
            };
        }
        query
    }};
}

/// TLS policy for [`MssqlConfig`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MssqlTls {
    /// Require encryption and verify the server certificate chain and host name against system
    /// roots.
    VerifyIdentity,
    /// Require encryption and pin the server certificate by exact DER/PEM match.
    ///
    /// Pinning bypasses the system CA chain but still requires the exact configured certificate,
    /// so it is suitable for a private server with a self-signed certificate.
    PinnedCertificate(PathBuf),
    /// Require encryption but do not verify the server certificate or host name.
    ///
    /// This accepts an attacker who can intercept the connection and is an explicit downgrade.
    Insecure,
}

/// Validated SQL Server pool configuration.
///
/// The password is zeroized on drop and never rendered by `Debug`.
pub struct MssqlConfig {
    host: String,
    port: u16,
    user: String,
    password: Zeroizing<String>,
    database: String,
    tls: MssqlTls,
    max_connections: u32,
    acquire_timeout: Duration,
}

impl MssqlConfig {
    /// Creates a configuration that requires verified TLS.
    ///
    /// The host, user, password, and database must be non-empty UTF-8 without NUL bytes; the port
    /// must be nonzero.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: Zeroizing::new(password.into()),
            database: database.into(),
            tls: MssqlTls::VerifyIdentity,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(5),
        }
    }

    /// Selects the TLS policy.
    #[must_use]
    pub fn tls(mut self, tls: MssqlTls) -> Self {
        self.tls = tls;
        self
    }

    /// Sets the maximum number of pooled connections. The default is five.
    #[must_use]
    pub fn max_connections(mut self, count: u32) -> Self {
        self.max_connections = count;
        self
    }

    /// Bounds waiting for a pooled connection, including opening a replacement. The default is
    /// five seconds.
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    fn validate(&self) -> Result<(), DbError> {
        let millis = self.acquire_timeout.as_millis();
        if !valid_text(&self.host, 255)
            || self.port == 0
            || !valid_text(&self.user, 255)
            || self.password.is_empty()
            || self.password.as_bytes().contains(&0)
            || !valid_text(&self.database, 128)
            || self.max_connections == 0
            || millis == 0
            || millis > i32::MAX as u128
            || match &self.tls {
                MssqlTls::PinnedCertificate(path) => path.as_os_str().is_empty(),
                MssqlTls::VerifyIdentity | MssqlTls::Insecure => false,
            }
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn connection_url(&self) -> Result<String, DbError> {
        let mut url = Url::parse("mssql://localhost")
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        url.set_username(&self.user)
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        url.set_password(Some(self.password.as_str()))
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        url.set_host(Some(&self.host))
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        url.set_port(Some(self.port))
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        url.set_path(&format!("/{}", self.database));
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("encrypt", "required");
            match &self.tls {
                MssqlTls::VerifyIdentity => {}
                MssqlTls::PinnedCertificate(path) => {
                    let path = path
                        .to_str()
                        .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))?;
                    query.append_pair("server_certificate", path);
                }
                MssqlTls::Insecure => {
                    query.append_pair("trust_certificate", "true");
                }
            }
            query.append_pair("application_name", "neotales-dbapi-mssql");
        }
        Ok(url.as_str().to_owned())
    }

    fn connect_options(&self) -> Result<MssqlConnectOptions, DbError> {
        let url = self.connection_url()?;
        let mut options: MssqlConnectOptions = url
            .parse()
            .map_err(|_error| DbError::new(ErrorKind::InvalidArgument))?;
        options.encryption(MssqlEncryption::Required);
        options.connect_timeout(self.acquire_timeout);
        Ok(options.disable_statement_logging())
    }
}

impl fmt::Debug for MssqlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlConfig")
            .field("host", &"[redacted]")
            .field("port", &"[redacted]")
            .field("user", &"[redacted]")
            .field("password", &"[redacted]")
            .field("database", &"[redacted]")
            .field("tls", &self.tls)
            .field("max_connections", &self.max_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish_non_exhaustive()
    }
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.as_bytes().contains(&0)
}

/// SQL Server pool implementing the driver-neutral [`neotales_dbapi`] traits.
#[derive(Clone)]
pub struct MssqlPoolProvider {
    pool: MssqlPool,
}

impl fmt::Debug for MssqlPoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MssqlPoolProvider")
    }
}

impl MssqlPoolProvider {
    /// Opens the pool and establishes at least one physical connection.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration, `Timeout` when the acquisition budget
    /// elapses, or a sanitized `Connection` error.
    pub async fn connect(config: MssqlConfig) -> Result<Self, DbError> {
        config.validate()?;
        let pool = MssqlPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .test_before_acquire(true)
            .connect_with(config.connect_options()?)
            .await
            .map_err(map_error)?;
        Ok(Self { pool })
    }

    /// Acquires one exclusive session. Drop the session to return or discard its connection.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, or a sanitized `Connection` error.
    pub async fn acquire_mssql(&self) -> Result<Box<dyn Session>, DbError> {
        Ok(Box::new(MssqlSession::acquire(&self.pool).await?) as Box<dyn Session>)
    }

    /// Acquires one exclusive session as its concrete type.
    ///
    /// This exists for operations that need a provider-specific transaction boundary,
    /// such as [`MssqlSession::begin_app_locked_async`]. Prefer [`Pool::acquire_async`]
    /// for ordinary driver-neutral work. Drop the session to return or discard its
    /// connection.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, or a sanitized `Connection` error.
    pub async fn acquire_mssql_session(&self) -> Result<MssqlSession, DbError> {
        MssqlSession::acquire(&self.pool).await
    }

    /// Closes the pool and waits for checked-out sessions to be dropped.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

impl Pool for MssqlPoolProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            isolation_level: true,
            read_only_transactions: false,
            trusted_scripts: true,
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move {
            Ok(Box::new(MssqlSession::acquire(&self.pool).await?) as Box<dyn Session>)
        })
    }
}

/// Discards a checked-out connection if setup is cancelled before it completes.
struct SetupGuard<'a> {
    connection: &'a mut PoolConnection<Mssql>,
    armed: bool,
}

impl<'a> SetupGuard<'a> {
    fn new(connection: &'a mut PoolConnection<Mssql>) -> Self {
        Self {
            connection,
            armed: true,
        }
    }

    fn connection_mut(&mut self) -> &mut MssqlConnection {
        self.connection
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for SetupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.connection.close_on_drop();
        }
    }
}

/// An exclusive SQL Server session.
///
/// Obtained from [`MssqlPoolProvider::acquire_mssql_session`] when a caller needs the
/// provider-specific [`Self::begin_app_locked_async`] boundary. Drop it to return or
/// discard the physical connection. Caller SQL is still subject to the same restricted
/// statement paths as the driver-neutral [`Session`] trait.
pub struct MssqlSession {
    connection: PoolConnection<Mssql>,
    interrupted: Arc<AtomicBool>,
    in_transaction: bool,
}

impl fmt::Debug for MssqlSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MssqlSession")
    }
}

impl MssqlSession {
    async fn acquire(pool: &MssqlPool) -> Result<Self, DbError> {
        let mut connection = pool.acquire().await.map_err(map_error)?;
        let mut setup = SetupGuard::new(&mut connection);
        // Standalone writes and transaction boundaries assume SQL Server's default autocommit
        // mode. Establish it and read the session option bit back.
        if let Err(error) = sqlx::raw_sql(sqlx::AssertSqlSafe("SET IMPLICIT_TRANSACTIONS OFF"))
            .execute(setup.connection_mut())
            .await
        {
            return Err(map_error(error));
        }
        let options: i64 = match sqlx::query_scalar("SELECT CAST(@@OPTIONS AS BIGINT)")
            .fetch_one(setup.connection_mut())
            .await
        {
            Ok(value) => value,
            Err(error) => return Err(map_error(error)),
        };
        if options & 2 != 0 {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        // A pooled session keeps its last isolation level. Reset to SQL Server's default and
        // read it back so a caller cannot inherit another transaction's isolation.
        if let Err(error) = sqlx::raw_sql(sqlx::AssertSqlSafe(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        ))
        .execute(setup.connection_mut())
        .await
        {
            return Err(map_error(error));
        }
        let isolation: i64 = match sqlx::query_scalar(
            "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        .fetch_one(setup.connection_mut())
        .await
        {
            Ok(value) => value,
            Err(error) => return Err(map_error(error)),
        };
        if isolation != 2 {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        setup.disarm();
        Ok(Self {
            connection,
            interrupted: Arc::new(AtomicBool::new(false)),
            in_transaction: false,
        })
    }

    fn ensure_live(&mut self) -> Result<(), DbError> {
        if self.interrupted.load(Ordering::Acquire) {
            self.connection.close_on_drop();
            Err(DbError::new(ErrorKind::Connection))
        } else {
            Ok(())
        }
    }

    fn invalidate(&mut self) {
        self.interrupted.store(true, Ordering::Release);
        self.connection.close_on_drop();
    }

    async fn reset_isolation(&mut self) -> Result<(), DbError> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        ))
        .execute(&mut *self.connection)
        .await
        .map_err(map_error)?;
        Ok(())
    }

    // A successful TDS completion alone does not prove BEGIN/COMMIT ran: for example,
    // SET NOEXEC can suppress execution. An absent response or unexpected state is fatal.
    async fn verify_transaction_state(&mut self, expected: i64) -> Result<(), DbError> {
        let row = sqlx::query(sqlx::AssertSqlSafe(
            "SELECT CAST(@@TRANCOUNT AS BIGINT), CAST(XACT_STATE() AS BIGINT)",
        ))
        .fetch_one(&mut *self.connection)
        .await
        .map_err(map_error)?;
        let count: i64 = row.try_get(0).map_err(map_error)?;
        let state: i64 = row.try_get(1).map_err(map_error)?;
        if count == expected && state == expected {
            Ok(())
        } else {
            Err(DbError::new(ErrorKind::Connection))
        }
    }

    async fn execute_inner(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        budget: Option<Duration>,
    ) -> Result<u64, DbError> {
        let future = bind_query!(sql, parameters).execute(&mut *self.connection);
        let result = match budget {
            None => future.await.map_err(map_write_error),
            Some(duration) => match tokio::time::timeout(duration, future).await {
                Ok(result) => result.map_err(map_write_error),
                Err(_) => return Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
            },
        };
        result.map(|done| done.rows_affected())
    }

    async fn fetch_inner(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
    ) -> Result<Vec<Vec<Row>>, DbError> {
        let query = bind_query!(sql, parameters);
        let mut stream = self.connection.fetch_events(query);
        let mut result: Vec<Vec<Row>> = Vec::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut deferred_error = None;
        loop {
            let row = match stream.try_next().await {
                Ok(Some(MssqlResultEvent::ResultSetStart(_))) => {
                    if deferred_error.is_some() {
                        continue;
                    }
                    if result.len() == limit.max_sets() {
                        deferred_error = Some(overflow());
                    } else {
                        result.push(Vec::new());
                    }
                    continue;
                }
                Ok(Some(MssqlResultEvent::ResultSetEnd | MssqlResultEvent::NonRow { .. })) => {
                    continue;
                }
                Ok(Some(MssqlResultEvent::Row(row))) => row,
                Ok(None) => break,
                Err(error) => return Err(map_write_error(error)),
            };
            if deferred_error.is_some() {
                continue;
            }
            if rows == limit.rows() {
                deferred_error = Some(overflow());
                continue;
            }
            let decoded = (|| {
                let mut cells = Vec::with_capacity(row.len());
                for (index, column) in row.columns().iter().enumerate() {
                    let name = column.name().to_owned();
                    bytes = bytes.checked_add(name.len()).ok_or_else(overflow)?;
                    let remaining = limit.bytes().saturating_sub(bytes);
                    let value = decode(&row, index, remaining)?;
                    bytes = bytes
                        .checked_add(value_bytes(&value))
                        .ok_or_else(overflow)?;
                    if bytes > limit.bytes() {
                        return Err(overflow());
                    }
                    cells.push((name, value));
                }
                Row::new(cells)
            })();
            let row = match decoded {
                Ok(row) => row,
                Err(error) => {
                    deferred_error = Some(error);
                    continue;
                }
            };
            let Some(set) = result.last_mut() else {
                return Err(DbError::new(ErrorKind::Connection));
            };
            set.push(row);
            rows += 1;
        }
        drop(stream);
        match deferred_error {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    async fn execute_script_inner(
        &mut self,
        sql: &str,
        budget: Option<Duration>,
    ) -> Result<(), DbError> {
        let future = sqlx::raw_sql(sqlx::AssertSqlSafe(sql)).execute(&mut *self.connection);
        match budget {
            None => future.await.map(|_| ()).map_err(map_write_error),
            Some(duration) => match tokio::time::timeout(duration, future).await {
                Ok(result) => result.map(|_| ()).map_err(map_write_error),
                Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
            },
        }
    }

    async fn begin_inner(&mut self, options: TxOptions) -> Result<(), DbError> {
        if options.read_only == Some(true) {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        self.verify_transaction_state(0).await?;
        let level = match options.isolation {
            Some(IsolationLevel::Serializable) => "SERIALIZABLE",
            Some(IsolationLevel::RepeatableRead) => "REPEATABLE READ",
            Some(IsolationLevel::ReadCommitted) | None => "READ COMMITTED",
            Some(IsolationLevel::ReadUncommitted) => "READ UNCOMMITTED",
        };
        let sql = format!("SET TRANSACTION ISOLATION LEVEL {level}");
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&mut *self.connection)
            .await
            .map_err(map_error)?;
        sqlx::raw_sql(sqlx::AssertSqlSafe("BEGIN TRANSACTION"))
            .execute(&mut *self.connection)
            .await
            .map_err(map_error)?;
        self.verify_transaction_state(1).await?;
        self.in_transaction = true;
        Ok(())
    }

    /// Begins a transaction and acquires an exclusive application lock owned by that
    /// transaction, both on this session.
    ///
    /// The lock resource is database-scoped and acquired for the `public` principal with
    /// `@LockOwner = 'Transaction'`. It is released by the server when the transaction
    /// commits, rolls back, or its connection is closed; it is never session-owned and
    /// therefore cannot outlive transaction closure or leak through the pool.
    ///
    /// `resource` must be nonempty, at most 255 UTF-16 code units, and free of NUL.
    /// `timeout` must be a positive integral number of milliseconds no greater than
    /// `i32::MAX` and bounds the server-side wait. The procedure return status is
    /// decoded: `0`/`1` granted, `-1` `Timeout`, `-2` `Cancelled`, `-3` `Busy`,
    /// `-999` `InvalidArgument`, and any other value `Unsupported`. On a negative
    /// status the transaction is rolled back before the session is reused.
    /// Cancellation during begin or lock acquisition discards the physical connection.
    ///
    /// The returned transaction is an ordinary [`Transaction`]; callers remain bound by
    /// the restricted session statements and must acquire the same lock before
    /// inspecting shared state. The lock call is provider-owned constant SQL with the
    /// resource and timeout sent as native RPC parameters; no caller SQL is accepted.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for an invalid resource or timeout, `Unsupported` for a
    /// read-only request, `Timeout` when the lock is not granted, or a sanitized provider
    /// error.
    pub fn begin_app_locked_async<'a>(
        &'a mut self,
        resource: &'a str,
        timeout: Duration,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            let millis = timeout.as_millis();
            if !valid_app_lock_resource(resource)
                || millis == 0
                || millis > i32::MAX as u128
                || !timeout.subsec_nanos().is_multiple_of(1_000_000)
            {
                return Err(invalid());
            }
            if options.read_only == Some(true) {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            let millis = i32::try_from(millis).map_err(|_error| invalid())?;
            self.ensure_live()?;
            let mut guard = LockedBeginGuard {
                session: self,
                armed: true,
            };
            guard.session.begin_inner(options).await?;
            if let Err(error) = guard.session.acquire_app_lock(resource, millis).await {
                guard.session.abort_transaction().await;
                guard.disarm();
                return Err(error);
            }
            guard.disarm();
            drop(guard);
            Ok(Box::new(MssqlTransaction {
                session: self,
                done: false,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }

    async fn acquire_app_lock(&mut self, resource: &str, timeout_ms: i32) -> Result<(), DbError> {
        let status = self
            .connection
            .acquire_application_lock(resource, timeout_ms)
            .await
            .map_err(map_error)?;
        app_lock_status(status)
    }

    async fn abort_transaction(&mut self) {
        let rolled_back = sqlx::raw_sql(sqlx::AssertSqlSafe("ROLLBACK TRANSACTION"))
            .execute(&mut *self.connection)
            .await
            .is_ok();
        self.in_transaction = false;
        if !rolled_back
            || self.verify_transaction_state(0).await.is_err()
            || self.reset_isolation().await.is_err()
        {
            self.invalidate();
        }
    }
}

struct LockedBeginGuard<'a> {
    session: &'a mut MssqlSession,
    armed: bool,
}

impl LockedBeginGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LockedBeginGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.session.invalidate();
        }
    }
}

// SQL Server app-lock return status. Negatives are procedure results, not TDS
// errors, so they must be decoded here rather than collapsed into a timeout.
fn app_lock_status(status: i32) -> Result<(), DbError> {
    match status {
        0 | 1 => Ok(()),
        -1 => Err(DbError::new(ErrorKind::Timeout)),
        -2 => Err(DbError::new(ErrorKind::Cancelled)),
        -3 => Err(DbError::new(ErrorKind::Busy)),
        -999 => Err(DbError::new(ErrorKind::InvalidArgument)),
        _ => Err(DbError::new(ErrorKind::Unsupported)),
    }
}

fn valid_app_lock_resource(resource: &str) -> bool {
    !resource.is_empty()
        && resource.encode_utf16().count() <= 255
        && !resource.as_bytes().contains(&0)
}

impl Drop for MssqlSession {
    fn drop(&mut self) {
        if self.in_transaction || self.interrupted.load(Ordering::Acquire) {
            self.connection.close_on_drop();
        }
    }
}

impl Session for MssqlSession {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            let scan = scan_sql(sql)?;
            check_arity(sql, parameters)?;
            check_no_transaction_control(&scan)?;
            check_no_indirect(&scan)?;
            check_batch_start(sql, &scan)?;
            check_single_write(sql)?;
            self.ensure_live()?;
            let flight = Flight::new(&self.interrupted);
            match self.execute_inner(sql, parameters, budget).await {
                Ok(rows) => {
                    flight.disarm();
                    Ok(rows)
                }
                Err(error) => {
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
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            let scan = scan_sql(sql)?;
            check_arity(sql, parameters)?;
            check_no_transaction_control(&scan)?;
            check_no_indirect(&scan)?;
            check_no_caller_set(&scan)?;
            check_batch_start(sql, &scan)?;
            self.ensure_live()?;
            let flight = Flight::new(&self.interrupted);
            let result = match budget {
                None => self.fetch_inner(sql, parameters, limit).await,
                Some(duration) => {
                    match tokio::time::timeout(duration, self.fetch_inner(sql, parameters, limit))
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
                    }
                }
            };
            match result {
                Ok(rows) => {
                    flight.disarm();
                    Ok(Box::new(BufferedReader::from_sets(rows)) as Box<dyn Reader + 'a>)
                }
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
            options.prepare_script_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            preflight_trusted_script(sql)?;
            self.ensure_live()?;
            let flight = Flight::new(&self.interrupted);
            match self.execute_script_inner(sql, budget).await {
                Ok(()) => {
                    flight.disarm();
                    Ok(())
                }
                Err(error) => {
                    self.invalidate();
                    Err(error)
                }
            }
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            if options.read_only == Some(true) {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            self.ensure_live()?;
            let flight = Flight::new(&self.interrupted);
            if let Err(error) = self.begin_inner(options).await {
                self.invalidate();
                return Err(error);
            }
            flight.disarm();
            Ok(Box::new(MssqlTransaction {
                session: self,
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

struct MssqlTransaction<'session> {
    session: &'session mut MssqlSession,
    done: bool,
}

impl<'session> Transaction<'session> for MssqlTransaction<'session> {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            let scan = scan_sql(sql)?;
            check_arity(sql, parameters)?;
            check_no_transaction_control(&scan)?;
            check_no_indirect(&scan)?;
            check_batch_start(sql, &scan)?;
            check_single_write(sql)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = self.session.execute_inner(sql, parameters, budget).await;
            match result {
                Ok(rows) => {
                    flight.disarm();
                    Ok(rows)
                }
                Err(error) => {
                    self.session.invalidate();
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
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            let scan = scan_sql(sql)?;
            check_arity(sql, parameters)?;
            check_no_transaction_control(&scan)?;
            check_no_indirect(&scan)?;
            check_no_caller_set(&scan)?;
            check_batch_start(sql, &scan)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = match budget {
                None => self.session.fetch_inner(sql, parameters, limit).await,
                Some(duration) => {
                    match tokio::time::timeout(
                        duration,
                        self.session.fetch_inner(sql, parameters, limit),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
                    }
                }
            };
            match result {
                Ok(rows) => {
                    flight.disarm();
                    Ok(Box::new(BufferedReader::from_sets(rows)) as Box<dyn Reader + 'a>)
                }
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
            options.prepare_script_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            preflight_trusted_script(sql)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            match self.session.execute_script_inner(sql, budget).await {
                Ok(()) => {
                    flight.disarm();
                    Ok(())
                }
                Err(error) => {
                    self.session.invalidate();
                    Err(error)
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
            let flight = Flight::new(&this.session.interrupted);
            if this.session.verify_transaction_state(1).await.is_err() {
                this.session.invalidate();
                this.session.in_transaction = false;
                this.done = true;
                return Err(DbError::new(ErrorKind::CommitOutcomeUnknown));
            }
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe("COMMIT TRANSACTION"))
                .execute(&mut *this.session.connection)
                .await;
            this.session.in_transaction = false;
            this.done = true;
            if result.is_err() || this.session.verify_transaction_state(0).await.is_err() {
                this.session.invalidate();
                return Err(DbError::new(ErrorKind::CommitOutcomeUnknown));
            }
            let reset = this.session.reset_isolation().await;
            finish_confirmed_commit(reset, || this.session.invalidate());
            flight.disarm();
            Ok(())
        })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            if let Err(error) = this.session.ensure_live() {
                this.session.in_transaction = false;
                this.done = true;
                return Err(error);
            }
            let flight = Flight::new(&this.session.interrupted);
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe("ROLLBACK TRANSACTION"))
                .execute(&mut *this.session.connection)
                .await;
            this.session.in_transaction = false;
            this.done = true;
            if result.is_err() || this.session.verify_transaction_state(0).await.is_err() {
                this.session.invalidate();
                return Err(DbError::new(ErrorKind::Connection));
            }
            if this.session.reset_isolation().await.is_err() {
                this.session.invalidate();
                return Err(DbError::new(ErrorKind::Connection));
            }
            flight.disarm();
            Ok(())
        })
    }
}

// COMMIT was acknowledged: a subsequent cleanup failure cannot make it uncommitted or unknown.
fn finish_confirmed_commit(reset: Result<(), DbError>, invalidate: impl FnOnce()) {
    if reset.is_err() {
        invalidate();
    }
}

impl Drop for MssqlTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.session.invalidate();
            self.session.in_transaction = false;
            self.done = true;
        }
    }
}

struct Flight {
    interrupted: Arc<AtomicBool>,
    armed: bool,
}

impl Flight {
    fn new(interrupted: &Arc<AtomicBool>) -> Self {
        interrupted.store(false, Ordering::Release);
        Self {
            interrupted: Arc::clone(interrupted),
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        if self.armed {
            self.interrupted.store(true, Ordering::Release);
        }
    }
}

fn budget(options: ExecuteOptions) -> Result<Option<Duration>, DbError> {
    match options.timeout_ms {
        None => Ok(None),
        Some(0) => Err(invalid()),
        Some(millis) => Ok(Some(Duration::from_millis(millis))),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanState {
    Normal,
    Single,
    Double,
    Bracket,
    LineComment,
    Block(usize),
}

#[derive(Debug)]
struct Statement {
    start: usize,
    end: usize,
    first_token: usize,
    last_token: usize,
}

#[derive(Debug)]
struct Scan {
    statements: Vec<Statement>,
    tokens: Vec<String>,
}

/// Conservatively scans T-SQL statement boundaries and `?` positional markers.
///
/// SQL Server has no backslash escapes by default; single and double quotes and brackets are
/// escaped by doubling. Block comments nest. This is not a SQL parser.
#[allow(clippy::too_many_lines)] // One state machine keeps boundary handling auditable.
fn scan_sql(sql: &str) -> Result<Scan, DbError> {
    if sql.as_bytes().contains(&0) {
        return Err(invalid());
    }
    let bytes = sql.as_bytes();
    let mut state = ScanState::Normal;
    let mut statements = Vec::new();
    let mut tokens = Vec::new();
    let mut start: Option<usize> = None;
    let mut start_token = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            ScanState::Normal => {
                if matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c) {
                    index += 1;
                    continue;
                }
                if byte.is_ascii_alphabetic() {
                    if start.is_none() {
                        start = Some(index);
                        start_token = tokens.len();
                    }
                    let begin = index;
                    while bytes
                        .get(index)
                        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                    {
                        index += 1;
                    }
                    tokens.push(sql[begin..index].to_owned());
                    continue;
                }
                match byte {
                    b'-' if bytes.get(index + 1) == Some(&b'-') => {
                        state = ScanState::LineComment;
                        index += 2;
                    }
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        state = ScanState::Block(1);
                        index += 2;
                    }
                    b';' => {
                        if let Some(begin) = start.take() {
                            statements.push(Statement {
                                start: begin,
                                end: index,
                                first_token: start_token,
                                last_token: tokens.len(),
                            });
                        }
                        index += 1;
                    }
                    b'\'' => {
                        if start.is_none() {
                            start = Some(index);
                            start_token = tokens.len();
                        }
                        state = ScanState::Single;
                        index += 1;
                    }
                    b'"' => {
                        if start.is_none() {
                            start = Some(index);
                            start_token = tokens.len();
                        }
                        state = ScanState::Double;
                        index += 1;
                    }
                    b'[' => {
                        if start.is_none() {
                            start = Some(index);
                            start_token = tokens.len();
                        }
                        state = ScanState::Bracket;
                        index += 1;
                    }
                    _ => {
                        if start.is_none() {
                            start = Some(index);
                            start_token = tokens.len();
                        }
                        index += 1;
                    }
                }
            }
            ScanState::Single => {
                if byte == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::Double => {
                if byte == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::Bracket => {
                if byte == b']' {
                    if bytes.get(index + 1) == Some(&b']') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::LineComment => {
                if byte == b'\n' || byte == b'\r' {
                    state = ScanState::Normal;
                }
                index += 1;
            }
            ScanState::Block(depth) => {
                if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    state = ScanState::Block(depth + 1);
                    index += 2;
                } else if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    state = if depth == 1 {
                        ScanState::Normal
                    } else {
                        ScanState::Block(depth - 1)
                    };
                    index += 2;
                } else {
                    index += 1;
                }
            }
        }
    }
    if !matches!(state, ScanState::Normal | ScanState::LineComment) {
        return Err(invalid());
    }
    if let Some(begin) = start {
        statements.push(Statement {
            start: begin,
            end: bytes.len(),
            first_token: start_token,
            last_token: tokens.len(),
        });
    }
    Ok(Scan { statements, tokens })
}

// SQL Server permits the first statement of a batch to be a procedure invocation without EXEC.
// Reject unrecognized leading forms (including quoted identifiers), rather than treating them
// as harmless expressions. This is not a full SQL grammar or a security sandbox.
fn check_batch_start(sql: &str, scan: &Scan) -> Result<(), DbError> {
    let Some(statement) = scan.statements.first() else {
        return Err(invalid());
    };
    check_statement_start(sql, statement.start)
}

fn check_statement_start(sql: &str, start: usize) -> Result<(), DbError> {
    let rest = &sql[start..];
    let end = rest
        .bytes()
        .position(|b| !b.is_ascii_alphanumeric() && b != b'_')
        .unwrap_or(rest.len());
    let keyword = &rest[..end];
    if rest.as_bytes().get(end) == Some(&b'.') {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    if [
        "select",
        "with",
        "insert",
        "update",
        "delete",
        "merge",
        "create",
        "alter",
        "drop",
        "truncate",
        "declare",
        "set",
        "if",
        "begin",
        "waitfor",
        "throw",
        "raiserror",
    ]
    .iter()
    .any(|allowed| token_is(keyword, allowed))
    {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

// This deliberately accepts only a complete, simple INSERT VALUES grammar. A separate complete
// UPDATE grammar is accepted by `check_single_write`. There is no attempt to guess T-SQL statement
// boundaries from semicolons. Broader forms use trusted script/fetch.
#[allow(clippy::too_many_lines)] // Keep the complete accepted grammar together for audit.
fn check_single_insert(sql: &str) -> Result<(), DbError> {
    let unsupported = || DbError::new(ErrorKind::Unsupported);
    let mut tokens = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
        } else if bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'@') {
            let start = i;
            while bytes
                .get(i)
                .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'@'))
            {
                i += 1;
            }
            tokens.push(&sql[start..i]);
        } else if matches!(bytes[i], b'.' | b'(' | b')' | b',' | b';') {
            tokens.push(&sql[i..=i]);
            i += 1;
        } else {
            return Err(unsupported());
        }
    }
    let mut p = 0usize;
    {
        let mut take = |expected: &str| {
            if tokens
                .get(p)
                .is_some_and(|token| token.eq_ignore_ascii_case(expected))
            {
                p += 1;
                true
            } else {
                false
            }
        };
        if !take("INSERT") || !take("INTO") {
            return Err(unsupported());
        }
    }
    let is_ident = |s: &str| {
        s.as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    };
    if !tokens.get(p).is_some_and(|s| is_ident(s)) {
        return Err(unsupported());
    }
    p += 1;
    if tokens.get(p) == Some(&".") {
        p += 1;
        if !tokens.get(p).is_some_and(|s| is_ident(s)) {
            return Err(unsupported());
        }
        p += 1;
    }
    if tokens.get(p) == Some(&"(") {
        p += 1;
        loop {
            if !tokens.get(p).is_some_and(|s| is_ident(s)) {
                return Err(unsupported());
            }
            p += 1;
            if tokens.get(p) != Some(&",") {
                break;
            }
            p += 1;
        }
        if tokens.get(p) != Some(&")") {
            return Err(unsupported());
        }
        p += 1;
    }
    if !tokens
        .get(p)
        .is_some_and(|t| t.eq_ignore_ascii_case("VALUES"))
    {
        return Err(unsupported());
    }
    p += 1;
    if tokens.get(p) != Some(&"(") {
        return Err(unsupported());
    }
    p += 1;
    loop {
        let value = tokens.get(p).ok_or_else(unsupported)?;
        let is_integer = value.bytes().all(|b| b.is_ascii_digit());
        let is_parameter = value.len() > 2
            && value[..2].eq_ignore_ascii_case("@P")
            && value[2..].bytes().all(|b| b.is_ascii_digit());
        if !is_integer && !is_parameter {
            return Err(unsupported());
        }
        p += 1;
        if tokens.get(p) != Some(&",") {
            break;
        }
        p += 1;
    }
    if tokens.get(p) != Some(&")") {
        return Err(unsupported());
    }
    p += 1;
    if tokens.get(p) == Some(&";") {
        p += 1;
    }
    if p == tokens.len() {
        Ok(())
    } else {
        Err(unsupported())
    }
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

/// Maximum nesting accepted in an `UPDATE` predicate.
const MAX_PREDICATE_DEPTH: usize = 16;

/// Tokens accepted by the restricted single-statement grammar.
///
/// Identifiers, literals, and comments are lexed here; operators and quoted forms that can start
/// a new T-SQL statement are not. The grammar has no subquery, function-call, or batch form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestrictedToken<'a> {
    Word(&'a str),
    Integer,
    Text,
    QuotedIdentifier,
    Dot,
    Comma,
    Open,
    Close,
    Semicolon,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Plus,
    Minus,
}

/// Lexes one full restricted statement, skipping whitespace and nested T-SQL comments.
#[allow(clippy::too_many_lines)] // One lexer keeps quoting and comment handling auditable.
fn restricted_tokens(sql: &str) -> Result<Vec<RestrictedToken<'_>>, DbError> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        match byte {
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                index += 2;
                while bytes
                    .get(index)
                    .is_some_and(|byte| !matches!(*byte, b'\n' | b'\r'))
                {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_comment(bytes, index)?;
            }
            b'\'' => {
                index = skip_delimited(bytes, index, b'\'')?;
                tokens.push(RestrictedToken::Text);
            }
            b'[' | b'"' => {
                let close = if byte == b'[' { b']' } else { b'"' };
                index = skip_delimited(bytes, index, close)?;
                tokens.push(RestrictedToken::QuotedIdentifier);
            }
            b'0'..=b'9' => {
                while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                }
                tokens.push(RestrictedToken::Integer);
            }
            b'.' => {
                tokens.push(RestrictedToken::Dot);
                index += 1;
            }
            b',' => {
                tokens.push(RestrictedToken::Comma);
                index += 1;
            }
            b'(' => {
                tokens.push(RestrictedToken::Open);
                index += 1;
            }
            b')' => {
                tokens.push(RestrictedToken::Close);
                index += 1;
            }
            b';' => {
                tokens.push(RestrictedToken::Semicolon);
                index += 1;
            }
            b'=' => {
                tokens.push(RestrictedToken::Equal);
                index += 1;
            }
            b'<' => {
                let (token, width) = match bytes.get(index + 1) {
                    Some(b'>') => (RestrictedToken::NotEqual, 2),
                    Some(b'=') => (RestrictedToken::LessEqual, 2),
                    _ => (RestrictedToken::Less, 1),
                };
                tokens.push(token);
                index += width;
            }
            b'>' => {
                let (token, width) = match bytes.get(index + 1) {
                    Some(b'=') => (RestrictedToken::GreaterEqual, 2),
                    _ => (RestrictedToken::Greater, 1),
                };
                tokens.push(token);
                index += width;
            }
            b'!' if bytes.get(index + 1) == Some(&b'=') => {
                tokens.push(RestrictedToken::NotEqual);
                index += 2;
            }
            b'+' => {
                tokens.push(RestrictedToken::Plus);
                index += 1;
            }
            b'-' => {
                tokens.push(RestrictedToken::Minus);
                index += 1;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = index;
                while bytes
                    .get(index)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    index += 1;
                }
                tokens.push(RestrictedToken::Word(&sql[start..index]));
            }
            b'@' => {
                let start = index;
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    index += 1;
                }
                tokens.push(RestrictedToken::Word(&sql[start..index]));
            }
            _ => return Err(unsupported()),
        }
    }
    Ok(tokens)
}

fn skip_comment(bytes: &[u8], mut index: usize) -> Result<usize, DbError> {
    let mut depth = 1usize;
    index += 2;
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            depth += 1;
            index += 2;
        } else if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            depth -= 1;
            index += 2;
            if depth == 0 {
                return Ok(index);
            }
        } else {
            index += 1;
        }
    }
    Err(invalid())
}

// SQL Server escapes a closing quote or bracket by doubling it.
fn skip_delimited(bytes: &[u8], mut index: usize, close: u8) -> Result<usize, DbError> {
    index += 1;
    while index < bytes.len() {
        if bytes[index] == close {
            index += 1;
            if bytes.get(index) == Some(&close) {
                index += 1;
            } else {
                return Ok(index);
            }
        } else {
            index += 1;
        }
    }
    Err(invalid())
}

fn is_parameter(word: &str) -> bool {
    word.len() > 2
        && word[..2].eq_ignore_ascii_case("@P")
        && word[2..].bytes().all(|byte| byte.is_ascii_digit())
}

fn is_identifier_word(word: &str) -> bool {
    word.as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
}

struct RestrictedUpdate<'a> {
    tokens: &'a [RestrictedToken<'a>],
    position: usize,
    depth: usize,
}

impl<'a> RestrictedUpdate<'a> {
    fn new(tokens: &'a [RestrictedToken<'a>]) -> Self {
        Self {
            tokens,
            position: 0,
            depth: 0,
        }
    }

    fn peek(&self) -> Option<RestrictedToken<'a>> {
        self.tokens.get(self.position).copied()
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn take(&mut self, expected: RestrictedToken<'a>) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn take_word(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(RestrictedToken::Word(word)) if word.eq_ignore_ascii_case(expected) => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn expect(&mut self, expected: RestrictedToken<'a>) -> Result<(), DbError> {
        if self.take(expected) {
            Ok(())
        } else {
            Err(unsupported())
        }
    }

    fn identifier(&mut self) -> Result<(), DbError> {
        match self.peek() {
            Some(RestrictedToken::Word(word)) if is_identifier_word(word) => {
                self.advance();
                Ok(())
            }
            Some(RestrictedToken::QuotedIdentifier) => {
                self.advance();
                Ok(())
            }
            _ => Err(unsupported()),
        }
    }

    fn object(&mut self) -> Result<(), DbError> {
        self.identifier()?;
        if self.take(RestrictedToken::Dot) {
            self.identifier()?;
        }
        Ok(())
    }

    fn column(&mut self) -> Result<(), DbError> {
        self.identifier()?;
        if self.take(RestrictedToken::Dot) {
            self.identifier()?;
        }
        Ok(())
    }

    fn value(&mut self) -> Result<(), DbError> {
        match self.peek() {
            Some(RestrictedToken::Plus | RestrictedToken::Minus) => {
                self.advance();
                if self.take(RestrictedToken::Integer) {
                    Ok(())
                } else {
                    Err(unsupported())
                }
            }
            Some(
                RestrictedToken::Integer
                | RestrictedToken::Text
                | RestrictedToken::QuotedIdentifier,
            ) => {
                self.advance();
                Ok(())
            }
            Some(RestrictedToken::Word(word)) => {
                if word.eq_ignore_ascii_case("null") {
                    self.advance();
                    Ok(())
                } else if word.eq_ignore_ascii_case("n")
                    && self.tokens.get(self.position + 1) == Some(&RestrictedToken::Text)
                {
                    self.position += 2;
                    Ok(())
                } else if is_parameter(word) || is_identifier_word(word) {
                    self.advance();
                    Ok(())
                } else {
                    Err(unsupported())
                }
            }
            _ => Err(unsupported()),
        }
    }

    fn predicate(&mut self) -> Result<(), DbError> {
        self.or_expression()
    }

    fn or_expression(&mut self) -> Result<(), DbError> {
        self.and_expression()?;
        while self.take_word("or") {
            self.and_expression()?;
        }
        Ok(())
    }

    fn and_expression(&mut self) -> Result<(), DbError> {
        self.primary()?;
        while self.take_word("and") {
            self.primary()?;
        }
        Ok(())
    }

    fn primary(&mut self) -> Result<(), DbError> {
        if self.take(RestrictedToken::Open) {
            self.depth += 1;
            if self.depth > MAX_PREDICATE_DEPTH {
                return Err(unsupported());
            }
            self.or_expression()?;
            self.expect(RestrictedToken::Close)?;
            self.depth -= 1;
            Ok(())
        } else {
            self.condition()
        }
    }

    fn condition(&mut self) -> Result<(), DbError> {
        self.column()?;
        if self.take_word("is") {
            let _ = self.take_word("not");
            if self.take_word("null") {
                Ok(())
            } else {
                Err(unsupported())
            }
        } else {
            match self.peek() {
                Some(
                    RestrictedToken::Equal
                    | RestrictedToken::NotEqual
                    | RestrictedToken::Less
                    | RestrictedToken::LessEqual
                    | RestrictedToken::Greater
                    | RestrictedToken::GreaterEqual,
                ) => {
                    self.advance();
                    self.value()
                }
                _ => Err(unsupported()),
            }
        }
    }
}

/// Accepts exactly one complete `UPDATE object SET column = value, ... [WHERE predicate] [;]`
/// statement and rejects trailing tokens, functions, subqueries, and omitted batches.
fn check_single_update(sql: &str) -> Result<(), DbError> {
    let tokens = restricted_tokens(sql)?;
    let mut parser = RestrictedUpdate::new(&tokens);
    if !parser.take_word("update") {
        return Err(unsupported());
    }
    parser.object()?;
    if !parser.take_word("set") {
        return Err(unsupported());
    }
    loop {
        parser.identifier()?;
        parser.expect(RestrictedToken::Equal)?;
        parser.value()?;
        if !parser.take(RestrictedToken::Comma) {
            break;
        }
    }
    if parser.take_word("where") {
        parser.predicate()?;
    }
    let _ = parser.take(RestrictedToken::Semicolon);
    if parser.position == tokens.len() {
        Ok(())
    } else {
        Err(unsupported())
    }
}

/// Accepts the two restricted single-statement write forms: `INSERT ... VALUES` and `UPDATE`.
fn check_single_write(sql: &str) -> Result<(), DbError> {
    check_single_insert(sql).or_else(|_| check_single_update(sql))
}

/// Statically validates one trusted script batch without executing it.
///
/// This runs the same conservative checks that [`Session::execute_trusted_script_async`]
/// and [`Transaction::execute_trusted_script_async`] apply before dispatch:
/// one `?`-free batch with no native parameters, no transaction control,
/// indirect execution, session `SET` statements, hidden batches, or an unrecognized
/// leading statement. `UPDATE ... SET` is accepted only when its whole statement is
/// the restricted ordinary form and every statement in that batch starts with a
/// recognized keyword. It never connects, sends SQL, or proves that the batch parses
/// or that trusted database objects are safe. Use it to reject every planned batch
/// before the first batch has any effect.
///
/// # Errors
/// Returns the same sanitized `InvalidArgument` or `Unsupported` category that
/// execution would return for this batch, without any SQL text.
pub fn preflight_trusted_script(sql: &str) -> Result<(), DbError> {
    let scan = scan_sql(sql)?;
    check_arity(sql, &[])?;
    check_no_transaction_control(&scan)?;
    check_no_indirect(&scan)?;
    check_set_usage(sql, &scan)?;
    check_batch_start(sql, &scan)
}

fn check_arity(sql: &str, parameters: &[Parameter]) -> Result<(), DbError> {
    sqlx_mssql_rs_core::validate_native_parameters(sql, parameters.len()).map_err(|_| invalid())
}

fn token_is(token: &str, expected: &str) -> bool {
    token.eq_ignore_ascii_case(expected)
}

fn check_no_transaction_control(scan: &Scan) -> Result<(), DbError> {
    for (index, token) in scan.tokens.iter().enumerate() {
        if ["commit", "rollback", "save", "use", "go"]
            .iter()
            .any(|keyword| token_is(token, keyword))
        {
            return Err(invalid());
        }
        if token_is(token, "begin")
            && scan.tokens.get(index + 1).is_some_and(|next| {
                ["tran", "transaction", "distributed"]
                    .iter()
                    .any(|keyword| token_is(next, keyword))
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn check_no_indirect(scan: &Scan) -> Result<(), DbError> {
    for token in &scan.tokens {
        if ["exec", "execute", "prepare", "deallocate"]
            .iter()
            .any(|keyword| token_is(token, keyword))
            || token.len() >= 3
                && (token[..3].eq_ignore_ascii_case("sp_")
                    || token[..3].eq_ignore_ascii_case("xp_"))
        {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
    }
    Ok(())
}

// `SET` is the keyword of both a session-option statement and an `UPDATE` assignment list.
// The first form changes execution state (`NOEXEC`, `PARSEONLY`, `ANSI_DEFAULTS`, ...), so it is
// never accepted. A batch containing `SET` is therefore held to validated semicolon boundaries:
// every statement must start with a recognized keyword, and every statement that contains `SET`
// must be one complete restricted `UPDATE`. A newline is not a statement boundary; a batch that
// omits the separator fails the complete-statement grammar instead of hiding a second statement.
fn check_set_usage(sql: &str, scan: &Scan) -> Result<(), DbError> {
    if !scan.tokens.iter().any(|token| token_is(token, "set")) {
        return Ok(());
    }
    for statement in &scan.statements {
        check_statement_start(sql, statement.start)?;
        let statement_has_set = scan.tokens[statement.first_token..statement.last_token]
            .iter()
            .any(|token| token_is(token, "set"));
        if !statement_has_set {
            continue;
        }
        let body = &sql[statement.start..statement.end];
        let keyword_end = body
            .bytes()
            .position(|byte| !byte.is_ascii_alphanumeric() && byte != b'_')
            .unwrap_or(body.len());
        if !token_is(&body[..keyword_end], "update") {
            return Err(unsupported());
        }
        check_single_update(body)?;
    }
    Ok(())
}

// Session fetch paths do not accept writes, so any `SET` token remains conservatively rejected.
fn check_no_caller_set(scan: &Scan) -> Result<(), DbError> {
    if scan.tokens.iter().any(|token| token_is(token, "set")) {
        Err(unsupported())
    } else {
        Ok(())
    }
}

fn decode(row: &MssqlRow, index: usize, remaining: usize) -> Result<Value, DbError> {
    let raw = row.try_get_raw(index).map_err(map_error)?;
    let type_name = raw.type_info().type_name().to_ascii_lowercase();
    if raw.is_null() {
        return Ok(Value::Null(null_type(&type_name)?));
    }
    match type_name.as_str() {
        "bit" | "bitn" => raw
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| DbError::new(ErrorKind::Decode)),
        "tinyint" | "smallint" | "int" | "bigint" | "intn" => raw
            .as_i64()
            .map(Value::I64)
            .ok_or_else(|| DbError::new(ErrorKind::Decode)),
        "real" | "float" | "fltn" => raw
            .as_f64()
            .map(Value::F64)
            .ok_or_else(|| DbError::new(ErrorKind::Decode)),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "xml" | "json" => {
            let value = raw
                .as_str()
                .ok_or_else(|| DbError::new(ErrorKind::Decode))?;
            if value.len() > remaining {
                return Err(overflow());
            }
            Ok(Value::Text(value.to_owned()))
        }
        "binary" | "varbinary" | "image" => {
            let value = raw
                .as_bytes()
                .ok_or_else(|| DbError::new(ErrorKind::Decode))?;
            if value.len() > remaining {
                return Err(overflow());
            }
            Ok(Value::Bytes(value.to_vec()))
        }
        _ => Err(DbError::new(ErrorKind::Decode)),
    }
}

fn null_type(name: &str) -> Result<DbType, DbError> {
    match name {
        "bit" | "bitn" => Ok(DbType::Bool),
        "tinyint" | "smallint" | "int" | "bigint" | "intn" => Ok(DbType::I64),
        "real" | "float" | "fltn" => Ok(DbType::F64),
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "xml" | "json" => {
            Ok(DbType::Text)
        }
        "binary" | "varbinary" | "image" => Ok(DbType::Bytes),
        _ => Err(DbError::new(ErrorKind::Decode)),
    }
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Text(text) => text.len(),
        Value::Bytes(bytes) => bytes.len(),
        _ => 0,
    }
}

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn overflow() -> DbError {
    DbError::new(ErrorKind::Overflow)
}

fn map_error(error: SqlxError) -> DbError {
    match error {
        SqlxError::InvalidArgument(_) | SqlxError::Encode(_) => invalid(),
        SqlxError::ColumnDecode { .. } | SqlxError::Decode(_) => DbError::new(ErrorKind::Decode),
        SqlxError::RowNotFound
        | SqlxError::ColumnNotFound(_)
        | SqlxError::ColumnIndexOutOfBounds { .. } => DbError::new(ErrorKind::Missing),
        SqlxError::PoolTimedOut => DbError::new(ErrorKind::Timeout),
        SqlxError::Database(database) => map_database_error(&*database),
        _ => DbError::new(ErrorKind::Connection),
    }
}

fn map_database_error(database: &(dyn sqlx::error::DatabaseError + 'static)) -> DbError {
    let number = database.code().and_then(|code| code.parse::<i32>().ok());
    match number {
        // Unique, foreign key, check, not-null, and permission-adjacent violations.
        Some(2601 | 2627 | 515 | 530 | 547 | 3621) => DbError::new(ErrorKind::Constraint),
        // Deadlock, lock wait, snapshot conflict.
        Some(1205 | 1222 | 3960) => DbError::new(ErrorKind::Busy),
        // Optimizer/resource timeout.
        Some(8645) => DbError::new(ErrorKind::Timeout),
        // Syntax, unknown object/column, conversion, truncation, permission.
        Some(102 | 105 | 156 | 207 | 208 | 229 | 245 | 2812 | 4104 | 8114 | 8152) => invalid(),
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
    use super::{
        MssqlConfig, MssqlTls, app_lock_status, check_no_caller_set, check_no_indirect,
        check_no_transaction_control, scan_sql, valid_app_lock_resource,
    };
    use neotales_dbapi::{DbType, ErrorKind, Parameter, Value};

    fn parameter() -> Parameter {
        Parameter::new(DbType::I64, Value::I64(1)).unwrap()
    }

    #[test]
    fn native_marker_validation() {
        let parameters = [parameter(), parameter()];
        assert!(super::check_arity("SELECT @P1, @P1, @P2, '@P3' /* @P4 */", &parameters).is_ok());
        for sql in ["SELECT @P1", "SELECT @P2", "SELECT @foo", "SELECT ?"] {
            assert!(super::check_arity(sql, &parameters).is_err(), "{sql}");
        }
    }

    #[test]
    fn app_lock_resources_are_bounded_and_nul_free() {
        for resource in ["", "x\0y"] {
            assert!(!valid_app_lock_resource(resource));
        }
        assert!(valid_app_lock_resource(
            "neotales-migration-mssql/lock/v1/test"
        ));
        assert!(valid_app_lock_resource(&"a".repeat(255)));
        assert!(!valid_app_lock_resource(&"a".repeat(256)));
    }

    #[test]
    fn app_lock_return_codes_are_decoded_without_collapsing_negatives() {
        for granted in [0, 1] {
            assert!(app_lock_status(granted).is_ok());
        }
        for (status, expected) in [
            (-1, ErrorKind::Timeout),
            (-2, ErrorKind::Cancelled),
            (-3, ErrorKind::Busy),
            (-999, ErrorKind::InvalidArgument),
            (-4, ErrorKind::Unsupported),
            (2, ErrorKind::Unsupported),
        ] {
            assert_eq!(app_lock_status(status).unwrap_err().kind(), expected);
        }
    }

    #[test]
    fn preflight_trusted_script_rejects_without_dispatch() {
        assert!(super::preflight_trusted_script("SELECT 1").is_ok());
        assert!(super::preflight_trusted_script("CREATE TABLE dbo.x (id int)").is_ok());
        assert!(super::preflight_trusted_script("UPDATE dbo.x SET id = 1").is_ok());
        assert!(
            super::preflight_trusted_script(
                "UPDATE dbo.x SET id = 1 /* comment */ WHERE id = 2 -- trailing"
            )
            .is_ok()
        );
        for sql in [
            "EXEC sys.sp_getapplock 'x'",
            "SET NOEXEC ON",
            "UPDATE dbo.x SET id = 1 SET NOEXEC ON",
            "UPDATE dbo.x SET id = 1; SET PARSEONLY ON",
            "SET ANSI_DEFAULTS ON\nUPDATE dbo.x SET id = 1",
            "SELECT 1\nSET IMPLICIT_TRANSACTIONS ON",
            "UPDATE dbo.x SET id = 1; dbo.proc",
            "UPDATE dbo.x SET id = (SELECT 1)",
            "UPDATE dbo.x SET id = 1 FROM dbo.y",
        ] {
            assert_eq!(
                super::preflight_trusted_script(sql).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        assert_eq!(
            super::preflight_trusted_script("COMMIT TRANSACTION")
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            super::preflight_trusted_script("SELECT 1\nGO\nSELECT 2")
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn confirmed_commit_reset_failure_discards_connection() {
        let invalidated = std::cell::Cell::new(false);
        super::finish_confirmed_commit(
            Err(neotales_dbapi::DbError::new(ErrorKind::Connection)),
            || invalidated.set(true),
        );
        assert!(invalidated.get());
    }

    #[test]
    fn execute_accepts_only_restricted_single_insert() {
        assert!(super::check_single_insert("INSERT INTO dbo.t (a,b) VALUES (@P1, 2);").is_ok());
        for sql in [
            "SELECT 1\nSELECT 2",
            "INSERT INTO dbo.t VALUES (1) INSERT INTO dbo.t VALUES (2)",
            "INSERT INTO dbo.t VALUES (1); SELECT 2",
            "INSERT INTO dbo.t VALUES ((1))",
            "INSERT INTO dbo.t VALUES (1) -- comment",
        ] {
            assert_eq!(
                super::check_single_insert(sql).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
    }

    #[test]
    fn execute_accepts_complete_restricted_update() {
        for sql in [
            "UPDATE dbo.t SET a = 1",
            "UPDATE dbo.t SET a = @P1, b = 'text' WHERE id = @P2;",
            "UPDATE [dbo].[t] SET [a] = N'it''s', [b] = NULL WHERE [id] = -2",
            "/* lead */ UPDATE t SET a = b WHERE (x = 1 OR y <> 2) AND z IS NOT NULL -- trail",
            "update T set A=+1 where ID>=10",
        ] {
            assert!(super::check_single_update(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SET a = 1",
            "UPDATE dbo.t SET @x = 1",
            "UPDATE dbo.t SET a = (SELECT 1)",
            "UPDATE dbo.t SET a = fn(1)",
            "UPDATE dbo.t SET a = 1 OUTPUT inserted.a",
            "UPDATE dbo.t SET a = 1 FROM dbo.u WHERE u.id = t.id",
            "UPDATE dbo.t SET a = 1; SELECT 2",
            "UPDATE dbo.t SET a = 1; UPDATE dbo.t SET a = 2",
            "UPDATE dbo.t SET a = 1 SET NOEXEC ON",
            "UPDATE dbo.t SET a = 1 WHERE id = 1 SET PARSEONLY ON",
            "UPDATE dbo.t SET a = 1 /* open",
        ] {
            assert!(super::check_single_update(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn set_usage_requires_validated_statement_boundaries() {
        assert!(super::check_single_write("UPDATE dbo.t SET a = @P1").is_ok());
        assert!(super::check_single_write("INSERT INTO dbo.t VALUES (1)").is_ok());
        for sql in [
            "SET NOEXEC ON",
            "UPDATE dbo.t SET a = 1; SET NOEXEC ON",
            "UPDATE dbo.t SET a = 1; dbo.proc",
            "CREATE TABLE dbo.t (a int)\nUPDATE dbo.t SET a = 1",
            "IF 1 = 1 UPDATE dbo.t SET a = 1",
            "UPDATE dbo.t SET a = 1 GO",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert!(
                super::check_set_usage(sql, &scan).is_err(),
                "must reject {sql}"
            );
        }
        assert!(
            super::check_set_usage(
                "CREATE TABLE dbo.t (a int); UPDATE dbo.t SET a = 1 WHERE a = 0;",
                &scan_sql("CREATE TABLE dbo.t (a int); UPDATE dbo.t SET a = 1 WHERE a = 0;")
                    .unwrap()
            )
            .is_ok()
        );
    }

    #[test]
    fn cr_terminates_line_comments_in_every_code_path() {
        let sql = "SELECT 1 -- comment\rSET NOEXEC ON";
        let scan = scan_sql(sql).unwrap();
        assert!(
            scan.tokens
                .iter()
                .any(|token| token.eq_ignore_ascii_case("set")),
            "CR must end the line comment so SET is a code token"
        );
        assert!(check_no_caller_set(&scan).is_err());
        assert!(super::check_set_usage(sql, &scan).is_err());

        // A hidden second statement after CR must fail the complete UPDATE grammar.
        assert!(
            super::check_single_update("UPDATE dbo.t SET a = 1 -- comment\rSET NOEXEC ON").is_err()
        );
        assert!(
            super::check_single_update("UPDATE dbo.t SET a = 1 -- comment\rDROP TABLE dbo.t")
                .is_err()
        );
        // CR and CRLF both continue the same statement after a comment.
        assert!(super::check_single_update("UPDATE dbo.t SET a = 1 -- c\rWHERE id = 2").is_ok());
        assert!(super::check_single_update("UPDATE dbo.t SET a = 1 -- c\r\nWHERE id = 2").is_ok());
        // LF remains a terminator.
        assert!(
            super::check_single_update("UPDATE dbo.t SET a = 1 -- comment\nSET NOEXEC ON").is_err()
        );
    }

    #[test]
    fn scanner_handles_tsql_quotes_and_nested_comments() {
        assert!(scan_sql("SELECT 'a''?b'").is_ok());
        assert!(scan_sql("SELECT \"a\"\"?b\"").is_ok());
        assert!(scan_sql("SELECT [a]]?b]").is_ok());
        for sql in ["SELECT 'x", "SELECT [x", "/* x"] {
            assert_eq!(
                scan_sql(sql).unwrap_err().kind(),
                ErrorKind::InvalidArgument,
                "{sql}"
            );
        }
    }

    #[test]
    fn one_statement_and_transaction_control_are_checked() {
        for sql in [
            "COMMIT",
            "ROLLBACK TRANSACTION",
            "SAVE TRANSACTION p",
            "BEGIN TRANSACTION",
            "BEGIN TRAN",
            "USE master",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_transaction_control(&scan).unwrap_err().kind(),
                ErrorKind::InvalidArgument,
                "{sql}"
            );
        }
        let scan = scan_sql("BEGIN TRY SELECT 1 END TRY BEGIN CATCH SELECT 2 END CATCH").unwrap();
        assert!(check_no_transaction_control(&scan).is_ok());
    }

    #[test]
    fn indirect_and_implicit_transactions_are_rejected() {
        let noexec = scan_sql("SET NOEXEC ON").unwrap();
        assert!(check_no_caller_set(&noexec).is_err());
        for sql in ["EXEC('SELECT 1')", "sp_who", "EXECUTE dbo.p"] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_indirect(&scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        for sql in [
            "SET IMPLICIT_TRANSACTIONS ON",
            "SET XACT_ABORT ON",
            "SET ANSI_DEFAULTS ON",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_caller_set(&scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        let scan = scan_sql("SET NOCOUNT ON").unwrap();
        assert_eq!(
            check_no_caller_set(&scan).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn go_and_control_words_inside_literals_are_ignored() {
        let scan = scan_sql("SELECT 'GO', [GO], \"GO\" -- GO\n/* GO */").unwrap();
        assert!(check_no_transaction_control(&scan).is_ok());
        let scan = scan_sql("SELECT 1\nGO\nSELECT 2").unwrap();
        assert_eq!(
            check_no_transaction_control(&scan).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let scan = scan_sql("SELECT 1\nBEGIN TRANSACTION").unwrap();
        assert_eq!(
            check_no_transaction_control(&scan).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let scan = scan_sql("SET /* comment */ IMPLICIT_TRANSACTIONS ON").unwrap();
        assert_eq!(
            check_no_caller_set(&scan).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn configuration_redacts_and_validates() {
        let config = MssqlConfig::new("db.example", 1433, "sa", "secret-value", "app")
            .tls(MssqlTls::Insecure);
        let debug = format!("{config:?}");
        for value in ["db.example", "sa", "secret-value", "app", "1433"] {
            assert!(!debug.contains(value), "{debug} contains {value}");
        }
        assert!(config.validate().is_ok());
        assert!(
            MssqlConfig::new("", 1433, "sa", "secret", "app")
                .validate()
                .is_err()
        );
        assert!(
            MssqlConfig::new("db.example", 0, "sa", "secret", "app")
                .validate()
                .is_err()
        );
        assert!(
            MssqlConfig::new("db.example", 1433, "sa", "", "app")
                .validate()
                .is_err()
        );
    }
}
