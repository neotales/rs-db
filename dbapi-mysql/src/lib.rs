#![doc = include_str!("../README.md")]

use futures_util::TryStreamExt;
use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, DbType, ErrorKind, ExecuteOptions,
    IsolationLevel, Parameter, Pool, PoolDefaults, QueryOptions, Reader, Row, RowLimit, Session,
    Transaction, TxOptions, Value,
};
use sqlx::{
    Column, ConnectOptions, Error as SqlxError, MySql, Row as SqlxRow, TypeInfo, ValueRef,
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode},
    pool::PoolConnection,
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
use zeroize::Zeroizing;

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

/// TLS policy for a [`MySqlConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MySqlTls {
    /// Encrypt the connection, verify the server certificate against trusted roots (or the
    /// configured CA file), and verify the server host name.
    VerifyIdentity,
    /// Encrypt the connection without verifying the server certificate or host name.
    ///
    /// This accepts an attacker who can intercept the connection. It is an explicit downgrade and
    /// is never selected automatically.
    Insecure,
}

/// Validated `MySQL` pool configuration.
///
/// The password is zeroized when the configuration is dropped and is never included in `Debug`.
/// SQL, host names, database names, and credentials are not retained in error values.
pub struct MySqlConfig {
    host: String,
    port: u16,
    user: String,
    password: Option<Zeroizing<String>>,
    database: String,
    tls: MySqlTls,
    ca_file: Option<PathBuf>,
    plaintext: bool,
    max_connections: u32,
    acquire_timeout: Duration,
}

impl MySqlConfig {
    /// Creates a TLS-required configuration that verifies the server identity.
    ///
    /// Built-in `WebPKI` roots are used until [`Self::ca_file`] selects a private bundle.
    /// The host, user, and database must be non-empty UTF-8 without NUL bytes; the port must be
    /// nonzero. A password is optional and must be supplied separately.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: None,
            database: database.into(),
            tls: MySqlTls::VerifyIdentity,
            ca_file: None,
            plaintext: false,
            max_connections: 5,
            acquire_timeout: Duration::from_secs(5),
        }
    }

    /// Creates a no-TLS configuration for a numeric loopback host only.
    ///
    /// This is intended for a disposable local server. A host name such as `localhost` is rejected
    /// because name resolution can leave the loopback interface. No other constructor disables TLS.
    ///
    /// # Errors
    /// Returns `InvalidArgument` unless the host is a numeric loopback address, or another field is
    /// invalid.
    pub fn plaintext_loopback(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, DbError> {
        let mut config = Self::new(host, port, user, database);
        if !config
            .host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
        {
            return Err(invalid());
        }
        config.plaintext = true;
        config.validate()?;
        Ok(config)
    }

    /// Sets the password. Prefer a value loaded from the caller's secret store.
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(Zeroizing::new(password.into()));
        self
    }

    /// Selects the TLS policy and clears any plaintext-loopback selection.
    #[must_use]
    pub fn tls(mut self, tls: MySqlTls) -> Self {
        self.tls = tls;
        self.plaintext = false;
        self
    }

    /// Uses the given PEM CA bundle for [`MySqlTls::VerifyIdentity`].
    #[must_use]
    pub fn ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// Sets the maximum number of pooled physical connections. The default is five.
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
            || self.database.is_empty()
            || self.database.len() > 64
            || self.database.as_bytes().contains(&0)
            || self
                .password
                .as_ref()
                .is_some_and(|value| value.as_bytes().contains(&0))
            || self.max_connections == 0
            || millis == 0
            || millis > i32::MAX as u128
            || self
                .ca_file
                .as_ref()
                .is_some_and(|path| path.as_os_str().is_empty())
            || self.tls == MySqlTls::Insecure && self.ca_file.is_some()
        {
            return Err(invalid());
        }
        if self.plaintext {
            if !self
                .host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
            {
                return Err(invalid());
            }
        } else if self.tls == MySqlTls::VerifyIdentity {
            // A CA path is optional because the TLS backend carries public roots.
        }
        Ok(())
    }

    fn connect_options(&self) -> MySqlConnectOptions {
        let mut options = MySqlConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.user)
            .database(&self.database)
            // SQLx appends PIPES_AS_CONCAT to the session `sql_mode` by default. Some MySQL
            // protocol servers, including Vitess `vtgate`, reject that addition at connect time.
            // This crate never uses `||`, and the placeholder scanner does not depend on it, so
            // leave the server's configured `sql_mode` alone instead of mutating it.
            .pipes_as_concat(false)
            .disable_statement_logging();
        if let Some(password) = &self.password {
            options = options.password(password.as_str());
        }
        if self.plaintext {
            return options.ssl_mode(MySqlSslMode::Disabled);
        }
        let mode = match self.tls {
            MySqlTls::VerifyIdentity => MySqlSslMode::VerifyIdentity,
            MySqlTls::Insecure => MySqlSslMode::Required,
        };
        let options = options.ssl_mode(mode);
        match self.ca_file.as_deref() {
            Some(path) => options.ssl_ca(path),
            None => options,
        }
    }
}

impl fmt::Debug for MySqlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MySqlConfig")
            .field("host", &"[redacted]")
            .field("port", &"[redacted]")
            .field("user", &"[redacted]")
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("database", &"[redacted]")
            .field("tls", &self.tls)
            .field("plaintext", &self.plaintext)
            .field("max_connections", &self.max_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish_non_exhaustive()
    }
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.as_bytes().contains(&0)
}

/// `MySQL` pool implementing the driver-neutral [`neotales_dbapi`] traits.
#[derive(Clone)]
pub struct MySqlPoolProvider {
    pool: sqlx::MySqlPool,
}

impl fmt::Debug for MySqlPoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MySqlPoolProvider")
    }
}

impl MySqlPoolProvider {
    /// Opens the pool and establishes at least one physical connection.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration, `Timeout` when the acquisition budget
    /// elapses, or a sanitized `Connection` error. No driver text is retained.
    pub async fn connect(config: MySqlConfig) -> Result<Self, DbError> {
        config.validate()?;
        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .test_before_acquire(true)
            .connect_with(config.connect_options())
            .await
            .map_err(map_error)?;
        Ok(Self { pool })
    }

    /// Acquires one exclusive concrete session.
    ///
    /// Callers that hold a session-level lock and cannot guarantee an explicit release must call
    /// [`MySqlSession::discard`] before dropping the session. That marks the physical connection
    /// for discard instead of returning it to the pool with the lock still held.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, a sanitized `Connection` error,
    /// or `Unsupported` when the session's character sets or SQL mode are unsafe to scan.
    pub async fn acquire_mysql_session(&self) -> Result<MySqlSession, DbError> {
        MySqlSession::acquire(&self.pool).await
    }

    /// Acquires one exclusive session. Drop the session to return or discard its connection.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, a sanitized `Connection` error,
    /// or `Unsupported` when the session's character sets or SQL mode are unsafe to scan.
    pub async fn acquire_mysql(&self) -> Result<Box<dyn Session>, DbError> {
        Ok(Box::new(self.acquire_mysql_session().await?))
    }

    /// Closes the pool and waits for checked-out sessions to be dropped.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

impl Pool for MySqlPoolProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            isolation_level: true,
            read_only_transactions: true,
            trusted_scripts: true,
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(
            async move { Ok(Box::new(self.acquire_mysql_session().await?) as Box<dyn Session>) },
        )
    }
}

/// Concrete MySQL session acquired by [`MySqlPoolProvider::acquire_mysql_session`].
///
/// Dropping a normally completed session returns its connection to the pool. A caller that owns a
/// session-level `GET_LOCK` and is cancelled cannot run an explicit release, so it must call
/// [`Self::discard`] before dropping. The discard marks the physical connection for close.
pub struct MySqlSession {
    connection: PoolConnection<MySql>,
    interrupted: Arc<AtomicBool>,
    in_transaction: bool,
}

impl fmt::Debug for MySqlSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MySqlSession")
    }
}

impl MySqlSession {
    async fn acquire(pool: &sqlx::MySqlPool) -> Result<Self, DbError> {
        let mut connection = pool.acquire().await.map_err(map_error)?;
        // Standalone statements and the migration dirty marker depend on autocommit. Establish
        // it explicitly, then read it back. A pooled or inherited session must not silently keep
        // autocommit=0 and defer every write to an unmanaged transaction.
        if let Err(error) = sqlx::raw_sql(sqlx::AssertSqlSafe("SET SESSION autocommit = 1"))
            .execute(&mut *connection)
            .await
        {
            connection.close_on_drop();
            return Err(map_error(error));
        }
        let autocommit: i64 = match sqlx::query_scalar("SELECT @@SESSION.autocommit")
            .fetch_one(&mut *connection)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                connection.close_on_drop();
                return Err(map_error(error));
            }
        };
        if autocommit != 1 {
            connection.close_on_drop();
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        // SQLx sends UTF-8 SQL. Never scan a session whose server-side parser
        // could interpret those bytes using a multibyte encoding such as GBK.
        let utf8: i64 = match sqlx::query_scalar(
            "SELECT @@SESSION.character_set_client = 'utf8mb4' AND \
             @@SESSION.character_set_connection = 'utf8mb4' AND \
             @@SESSION.character_set_results = 'utf8mb4'",
        )
        .fetch_one(&mut *connection)
        .await
        {
            Ok(value) => value,
            Err(error) => {
                connection.close_on_drop();
                return Err(map_error(error));
            }
        };
        if utf8 != 1 {
            connection.close_on_drop();
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        // The scanner assumes backslash escapes and double-quoted strings. Sessions that
        // inherit NO_BACKSLASH_ESCAPES or ANSI_QUOTES would make quote boundaries ambiguous,
        // so fail closed rather than guess. Changes through this provider are rejected separately.
        let mode: String = match sqlx::query_scalar("SELECT @@SESSION.sql_mode")
            .fetch_one(&mut *connection)
            .await
        {
            Ok(mode) => mode,
            Err(error) => {
                connection.close_on_drop();
                return Err(map_error(error));
            }
        };
        if [b"NO_BACKSLASH_ESCAPES".as_slice(), b"ANSI_QUOTES"]
            .iter()
            .any(|unsafe_mode| contains_ascii_case_insensitive(mode.as_bytes(), unsafe_mode))
        {
            connection.close_on_drop();
            return Err(DbError::new(ErrorKind::Unsupported));
        }
        Ok(Self {
            connection,
            interrupted: Arc::new(AtomicBool::new(false)),
            in_transaction: false,
        })
    }

    /// Marks this session's physical connection for discard when the session is dropped.
    ///
    /// Use this when the caller owns a session-level lock and cannot guarantee that an explicit
    /// release will run, for example across a cancellable future. The connection is closed instead
    /// of being returned to the pool, so MySQL releases its session locks on disconnect.
    pub fn discard(&mut self) {
        self.invalidate();
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

    #[allow(clippy::too_many_lines)] // One bounded decoder loop must stay auditable in place.
    async fn fetch_inner(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
    ) -> Result<Vec<Row>, DbError> {
        let mut stream = bind_query!(sql, parameters).fetch(&mut *self.connection);
        let mut result = Vec::new();
        let mut bytes = 0usize;
        loop {
            let row = match stream.try_next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(error) => return Err(map_write_error(error)),
            };
            if result.len() == limit.rows() {
                return Err(overflow());
            }
            let mut cells = Vec::with_capacity(row.len());
            for (index, column) in row.columns().iter().enumerate() {
                let name = column.name().to_owned();
                bytes = bytes.checked_add(name.len()).ok_or_else(overflow)?;
                let value = decode(&row, index)?;
                bytes = bytes
                    .checked_add(value_bytes(&value))
                    .ok_or_else(overflow)?;
                if bytes > limit.bytes() {
                    return Err(overflow());
                }
                cells.push((name, value));
            }
            result.push(Row::new(cells)?);
        }
        drop(stream);
        Ok(result)
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
        if let Some(isolation) = options.isolation {
            let level = match isolation {
                IsolationLevel::Serializable => "SERIALIZABLE",
                IsolationLevel::RepeatableRead => "REPEATABLE READ",
                IsolationLevel::ReadCommitted => "READ COMMITTED",
                IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            };
            let sql = format!("SET TRANSACTION ISOLATION LEVEL {level}");
            sqlx::raw_sql(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut *self.connection)
                .await
                .map_err(map_error)?;
        }
        let access = match options.read_only {
            Some(true) => " READ ONLY",
            Some(false) => " READ WRITE",
            None => "",
        };
        let sql = format!("START TRANSACTION{access}");
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&mut *self.connection)
            .await
            .map_err(map_error)?;
        self.in_transaction = true;
        Ok(())
    }
}

impl Drop for MySqlSession {
    fn drop(&mut self) {
        if self.in_transaction || self.interrupted.load(Ordering::Acquire) {
            self.connection.close_on_drop();
        }
    }
}

impl Session for MySqlSession {
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
            check_one_statement(&scan)?;
            check_arity(&scan, parameters)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
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
            check_one_statement(&scan)?;
            check_arity(&scan, parameters)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
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
                    Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
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
            let scan = scan_sql(sql)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
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
            self.ensure_live()?;
            let flight = Flight::new(&self.interrupted);
            if let Err(error) = self.begin_inner(options).await {
                self.invalidate();
                return Err(error);
            }
            flight.disarm();
            Ok(Box::new(MySqlTransaction {
                session: self,
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

struct MySqlTransaction<'session> {
    session: &'session mut MySqlSession,
    done: bool,
}

impl<'session> Transaction<'session> for MySqlTransaction<'session> {
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
            check_one_statement(&scan)?;
            check_arity(&scan, parameters)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_implicit_commit(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
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
            check_one_statement(&scan)?;
            check_arity(&scan, parameters)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_implicit_commit(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
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
                    Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
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
            let scan = scan_sql(sql)?;
            check_no_transaction_control(sql, &scan)?;
            check_no_implicit_commit(sql, &scan)?;
            check_no_session_state_change(sql, &scan)?;
            check_no_indirect_execution(sql, &scan)?;
            check_no_executable_comment(&scan)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = self.session.execute_script_inner(sql, budget).await;
            match result {
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
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe("COMMIT"))
                .execute(&mut *this.session.connection)
                .await;
            this.session.in_transaction = false;
            this.done = true;
            if result.is_ok() {
                flight.disarm();
                Ok(())
            } else {
                this.session.invalidate();
                Err(DbError::new(ErrorKind::CommitOutcomeUnknown))
            }
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
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe("ROLLBACK"))
                .execute(&mut *this.session.connection)
                .await;
            this.session.in_transaction = false;
            this.done = true;
            if result.is_ok() {
                flight.disarm();
                Ok(())
            } else {
                this.session.invalidate();
                Err(DbError::new(ErrorKind::Connection))
            }
        })
    }
}

impl Drop for MySqlTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.session.invalidate();
            self.session.in_transaction = false;
            self.done = true;
        }
    }
}

/// Marks an operation as in flight. If its future is dropped before completion, the session is
/// poisoned and its physical connection is discarded instead of being returned to the pool.
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
    Backtick,
    LineComment,
    BlockComment,
}

#[derive(Debug)]
struct Scan {
    /// Byte offset of the first code byte of each non-empty statement.
    statements: Vec<usize>,
    placeholders: usize,
    executable_comment: bool,
}

/// Conservatively scans statement boundaries, placeholders, and executable comments.
/// Both MySQL `/*! ... */` and MariaDB `/*M! ... */` forms are recorded, including a
/// lowercase `/*m!` form rejected conservatively.
///
/// This is not a SQL parser. It understands `MySQL` quoting with backslash escapes, doubled quote
/// characters, `#` and `-- ` line comments, and non-nested block comments. Executable version
/// comments are recorded but their contents are not scanned as code. `DELIMITER` is not handled.
#[allow(clippy::too_many_lines)] // A single state machine keeps boundary handling auditable.
fn scan_sql(sql: &str) -> Result<Scan, DbError> {
    if sql.as_bytes().contains(&0) {
        return Err(invalid());
    }
    let bytes = sql.as_bytes();
    let mut state = ScanState::Normal;
    let mut statements = Vec::new();
    let mut placeholders = 0usize;
    let mut executable_comment = false;
    let mut start: Option<usize> = None;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            ScanState::Normal => {
                if matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c) {
                    index += 1;
                    continue;
                }
                match byte {
                    b'#' => {
                        state = ScanState::LineComment;
                        index += 1;
                    }
                    b'-' if bytes.get(index + 1) == Some(&b'-')
                        && bytes
                            .get(index + 2)
                            .is_some_and(|next| next.is_ascii_whitespace() || *next < 0x20) =>
                    {
                        state = ScanState::LineComment;
                        index += 2;
                    }
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        // MySQL uses `/*! ... */`; MariaDB also executes `/*M! ... */`.
                        // The lowercase form is not executable on MariaDB 11.4, but it is
                        // rejected conservatively so a future/case variant cannot bypass this.
                        let marker = bytes.get(index + 2).copied();
                        executable_comment |= marker == Some(b'!')
                            || matches!(marker, Some(b'M' | b'm'))
                                && bytes.get(index + 3) == Some(&b'!');
                        state = ScanState::BlockComment;
                        index += 2;
                    }
                    b';' => {
                        if let Some(statement) = start.take() {
                            statements.push(statement);
                        } else if statements.is_empty() {
                            return Err(invalid());
                        }
                        index += 1;
                    }
                    b'\'' => {
                        start.get_or_insert(index);
                        state = ScanState::Single;
                        index += 1;
                    }
                    b'"' => {
                        start.get_or_insert(index);
                        state = ScanState::Double;
                        index += 1;
                    }
                    b'`' => {
                        start.get_or_insert(index);
                        state = ScanState::Backtick;
                        index += 1;
                    }
                    b'?' => {
                        start.get_or_insert(index);
                        placeholders = placeholders.checked_add(1).ok_or_else(invalid)?;
                        index += 1;
                    }
                    _ => {
                        start.get_or_insert(index);
                        index += 1;
                    }
                }
            }
            ScanState::Single => {
                if byte == b'\\' {
                    index += 2;
                } else if byte == b'\'' {
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
                if byte == b'\\' {
                    index += 2;
                } else if byte == b'"' {
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
            ScanState::Backtick => {
                if byte == b'`' {
                    if bytes.get(index + 1) == Some(&b'`') {
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
                if byte == b'\n' {
                    state = ScanState::Normal;
                }
                index += 1;
            }
            ScanState::BlockComment => {
                if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    state = ScanState::Normal;
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
    if let Some(statement) = start {
        statements.push(statement);
    }
    if statements.is_empty() {
        return Err(invalid());
    }
    Ok(Scan {
        statements,
        placeholders,
        executable_comment,
    })
}

fn check_one_statement(scan: &Scan) -> Result<(), DbError> {
    if scan.statements.len() == 1 {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn check_arity(scan: &Scan, parameters: &[Parameter]) -> Result<(), DbError> {
    if scan.placeholders == parameters.len() {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn leading_token(sql: &str, start: usize) -> Option<&[u8]> {
    let bytes = sql.as_bytes();
    if !bytes.get(start)?.is_ascii_alphabetic() {
        return None;
    }
    let mut end = start;
    while bytes
        .get(end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
    {
        end += 1;
    }
    Some(&bytes[start..end])
}

fn token_is(token: &[u8], candidates: &[&[u8]]) -> bool {
    candidates
        .iter()
        .any(|candidate| token.eq_ignore_ascii_case(candidate))
}

const TRANSACTION_CONTROL: &[&[u8]] = &[
    b"BEGIN",
    b"COMMIT",
    b"ROLLBACK",
    b"SAVEPOINT",
    b"RELEASE",
    b"START",
    b"XA",
];

const UNSAFE_INDIRECT: &[&[u8]] = &[b"CALL", b"EXECUTE", b"PREPARE"];

const IMPLICIT_COMMIT: &[&[u8]] = &[
    b"ALTER",
    b"ANALYZE",
    b"BEGIN",
    b"CACHE",
    b"CHANGE",
    b"COMMIT",
    b"CREATE",
    b"DROP",
    b"FLUSH",
    b"GRANT",
    b"IMPORT",
    b"INSTALL",
    b"LOAD",
    b"LOCK",
    b"OPTIMIZE",
    b"PURGE",
    b"RELEASE",
    b"RENAME",
    b"REPAIR",
    b"RESET",
    b"REVOKE",
    b"ROLLBACK",
    b"SAVEPOINT",
    b"SET",
    b"START",
    b"STOP",
    b"TRUNCATE",
    b"UNINSTALL",
    b"UNLOCK",
    b"XA",
];

fn check_no_transaction_control(sql: &str, scan: &Scan) -> Result<(), DbError> {
    for start in &scan.statements {
        if leading_token(sql, *start).is_some_and(|token| token_is(token, TRANSACTION_CONTROL)) {
            return Err(invalid());
        }
    }
    Ok(())
}

fn check_no_implicit_commit(sql: &str, scan: &Scan) -> Result<(), DbError> {
    if scan.executable_comment {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    for start in &scan.statements {
        if leading_token(sql, *start).is_some_and(|token| token_is(token, IMPLICIT_COMMIT)) {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
    }
    Ok(())
}

/// Rejects database selection and `SET` statements that invalidate session assumptions.
///
/// The check is deliberately a conservative byte scan: a statement that starts with `SET` is
/// rejected when the same SQL text contains an invariant-related token, even in a
/// string literal. Callers may still set unrelated session variables.
fn check_no_session_state_change(sql: &str, scan: &Scan) -> Result<(), DbError> {
    if scan.statements.iter().any(|start| {
        leading_token(sql, *start).is_some_and(|token| token.eq_ignore_ascii_case(b"USE"))
    }) {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    let sets_a_variable = scan.statements.iter().any(|start| {
        leading_token(sql, *start).is_some_and(|token| token.eq_ignore_ascii_case(b"SET"))
    });
    if sets_a_variable
        && [
            b"autocommit".as_slice(),
            b"sql_mode",
            b"transaction",
            b"names",
            b"character",
            b"charset",
            b"collation",
        ]
        .iter()
        .any(|needle| contains_ascii_case_insensitive(sql.as_bytes(), needle))
    {
        Err(DbError::new(ErrorKind::Unsupported))
    } else {
        Ok(())
    }
}

/// Rejects indirect execution that can hide transaction control or DDL from the leading-token scan.
///
/// `CALL` can run a procedure containing `COMMIT` or DDL. `EXECUTE` runs a prepared statement that
/// the scanner never sees. `PREPARE` is rejected with them so a prepared statement cannot be
/// created and run through another path.
fn check_no_indirect_execution(sql: &str, scan: &Scan) -> Result<(), DbError> {
    for start in &scan.statements {
        if leading_token(sql, *start).is_some_and(|token| token_is(token, UNSAFE_INDIRECT)) {
            return Err(DbError::new(ErrorKind::Unsupported));
        }
    }
    Ok(())
}

fn check_no_executable_comment(scan: &Scan) -> Result<(), DbError> {
    if scan.executable_comment {
        Err(DbError::new(ErrorKind::Unsupported))
    } else {
        Ok(())
    }
}

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

fn decode(row: &sqlx::mysql::MySqlRow, index: usize) -> Result<Value, DbError> {
    let raw = row.try_get_raw(index).map_err(map_error)?;
    let type_info = raw.type_info();
    let name = type_info.name();
    if raw.is_null() {
        return Ok(Value::Null(null_type(name)?));
    }
    match name {
        "BOOLEAN" | "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT" => row
            .try_get::<i64, _>(index)
            .map(Value::I64)
            .map_err(map_error),
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED" | "MEDIUMINT UNSIGNED"
        | "BIGINT UNSIGNED" | "YEAR" | "BIT" => {
            let value = row.try_get::<u64, _>(index).map_err(map_error)?;
            let value = i64::try_from(value).map_err(|_| overflow())?;
            Ok(Value::I64(value))
        }
        "FLOAT" | "DOUBLE" => row
            .try_get::<f64, _>(index)
            .map(Value::F64)
            .map_err(map_error),
        "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" => row
            .try_get::<String, _>(index)
            .map(Value::Text)
            .map_err(map_error),
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => row
            .try_get::<Vec<u8>, _>(index)
            .map(Value::Bytes)
            .map_err(map_error),
        _ => Err(DbError::new(ErrorKind::Decode)),
    }
}

fn null_type(name: &str) -> Result<DbType, DbError> {
    match name {
        "NULL" => Ok(DbType::Text),
        "BOOLEAN" | "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT"
        | "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED" | "MEDIUMINT UNSIGNED"
        | "BIGINT UNSIGNED" | "YEAR" | "BIT" => Ok(DbType::I64),
        "FLOAT" | "DOUBLE" => Ok(DbType::F64),
        "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" => {
            Ok(DbType::Text)
        }
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            Ok(DbType::Bytes)
        }
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
        SqlxError::PoolTimedOut => DbError::new(ErrorKind::Timeout),
        SqlxError::Database(database) => map_database_error(&*database),
        _ => DbError::new(ErrorKind::Connection),
    }
}

fn map_database_error(database: &(dyn sqlx::error::DatabaseError + 'static)) -> DbError {
    use sqlx::error::ErrorKind as SqlxKind;

    if matches!(
        database.kind(),
        SqlxKind::UniqueViolation
            | SqlxKind::ForeignKeyViolation
            | SqlxKind::NotNullViolation
            | SqlxKind::CheckViolation
            | SqlxKind::ExclusionViolation
    ) {
        return DbError::new(ErrorKind::Constraint);
    }
    let number = database
        .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
        .map(sqlx::mysql::MySqlDatabaseError::number);
    match number {
        // Lock wait timeout, deadlock.
        Some(1205 | 1213) => DbError::new(ErrorKind::Busy),
        // Query interrupted by KILL.
        Some(1317) => DbError::new(ErrorKind::Cancelled),
        // Maximum statement execution time exceeded.
        Some(3024) => DbError::new(ErrorKind::Timeout),
        // Malformed SQL or unknown/ambiguous object, and write outside a read-only transaction.
        Some(1051 | 1052 | 1054 | 1064 | 1066 | 1136 | 1146 | 1792) => invalid(),
        // This statement is not supported by the prepared-statement protocol; use a trusted script.
        Some(1295) => DbError::new(ErrorKind::Unsupported),
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
mod checkout_tests;

#[cfg(test)]
mod tests {
    use super::{
        MySqlConfig, MySqlTls, check_arity, check_no_executable_comment, check_no_implicit_commit,
        check_no_indirect_execution, check_no_session_state_change, check_no_transaction_control,
        check_one_statement, scan_sql,
    };
    use neotales_dbapi::{DbType, ErrorKind, Parameter, Value};

    fn parameter() -> Parameter {
        Parameter::new(DbType::I64, Value::I64(1)).unwrap()
    }

    #[test]
    fn scanner_counts_only_unquoted_placeholders() {
        let sql = "SELECT ?, '?', \"?\", `?col`, ? -- ?\n, /* ? */ ?";
        assert_eq!(scan_sql(sql).unwrap().placeholders, 3);
        let parameters = [parameter(), parameter(), parameter()];
        assert!(check_arity(&scan_sql(sql).unwrap(), &parameters).is_ok());
    }

    #[test]
    fn scanner_handles_backslash_and_doubled_quotes() {
        assert_eq!(
            scan_sql("SELECT 'it\\'s ?', 'a''b ?'")
                .unwrap()
                .placeholders,
            0
        );
        assert_eq!(scan_sql("SELECT 'a''?b'").unwrap().placeholders, 0);
        assert_eq!(scan_sql("SELECT 'a?b'").unwrap().placeholders, 0);
    }

    #[test]
    fn scanner_rejects_unterminated_forms() {
        for sql in [
            "SELECT 'unterminated",
            "SELECT \"unterminated",
            "SELECT `unterminated",
            "SELECT 1 /* unterminated",
        ] {
            assert_eq!(
                scan_sql(sql).unwrap_err().kind(),
                ErrorKind::InvalidArgument,
                "{sql}"
            );
        }
    }

    #[test]
    fn scanner_rejects_empty_and_leading_semicolons() {
        for sql in ["", "   ", "-- only a comment", "; SELECT 1"] {
            assert_eq!(
                scan_sql(sql).unwrap_err().kind(),
                ErrorKind::InvalidArgument,
                "{sql}"
            );
        }
    }

    #[test]
    fn one_statement_rejects_batches() {
        let scan = scan_sql("SELECT 1; SELECT 2").unwrap();
        assert_eq!(
            check_one_statement(&scan).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert!(check_one_statement(&scan_sql("SELECT 1; -- trailing").unwrap()).is_ok());
        assert!(check_one_statement(&scan_sql("SELECT ';'").unwrap()).is_ok());
    }

    #[test]
    fn arity_mismatch_is_rejected() {
        let scan = scan_sql("SELECT ?, ?").unwrap();
        assert_eq!(
            check_arity(&scan, &[parameter()]).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn transaction_control_is_rejected_on_any_statement() {
        for sql in [
            "COMMIT",
            " start transaction read only",
            "/* lead */ ROLLBACK",
            "BEGIN",
            "XA START 'x'",
            "SELECT 1; COMMIT",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_transaction_control(sql, &scan).unwrap_err().kind(),
                ErrorKind::InvalidArgument,
                "{sql}"
            );
        }
        let scan = scan_sql("SELECT 'COMMIT', \"BEGIN\"").unwrap();
        assert!(check_no_transaction_control("SELECT 'COMMIT', \"BEGIN\"", &scan).is_ok());
    }

    #[test]
    fn implicit_commit_commands_are_rejected_inside_transactions() {
        for sql in [
            "CREATE TABLE t (id BIGINT)",
            " ALTER TABLE t ADD COLUMN x BIGINT",
            "DROP TABLE t",
            "SET autocommit = 1",
            "SELECT 1; TRUNCATE TABLE t",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_implicit_commit(sql, &scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        let scan = scan_sql("INSERT INTO t VALUES (1); SELECT 1").unwrap();
        assert!(check_no_implicit_commit("INSERT INTO t VALUES (1); SELECT 1", &scan).is_ok());
        let scan = scan_sql("SELECT 1; /*!40101 SET autocommit=0 */").unwrap();
        assert_eq!(
            check_no_implicit_commit("SELECT 1; /*!40101 SET autocommit=0 */", &scan)
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn session_state_changes_are_rejected_conservatively() {
        for sql in [
            "SET autocommit = 0",
            "SET SESSION autocommit=0",
            "SET @@session.autocommit = 0",
            "SELECT 1; SET autocommit = 1",
            "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'",
            "SET SESSION sql_mode = 'ANSI_QUOTES'",
            "SET @@SESSION.sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "SET SESSION transaction_isolation = 'READ-COMMITTED'",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_session_state_change(sql, &scan)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        let scan = scan_sql("SELECT 'autocommit' AS word").unwrap();
        assert!(check_no_session_state_change("SELECT 'autocommit' AS word", &scan).is_ok());
        let scan = scan_sql("SET SESSION time_zone = '+00:00'").unwrap();
        assert!(check_no_session_state_change("SET SESSION time_zone = '+00:00'", &scan).is_ok());
    }

    #[test]
    fn changing_database_is_rejected_in_scripts() {
        for sql in ["USE other_db", "SELECT 1; /* comment */ uSe `other_db`"] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_session_state_change(sql, &scan)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn changing_connection_charset_is_rejected() {
        for sql in [
            "SET NAMES gbk",
            "SET /* comment */ NaMeS gbk",
            "SET CHARACTER SET gbk",
            "SET CHARSET gbk",
            "SET @@session.character_set_client = gbk",
            "SET SESSION character_set_connection = gbk",
            "SET SESSION collation_connection = gbk_chinese_ci",
            "SELECT 1; SET NAMES gbk",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_session_state_change(sql, &scan)
                    .unwrap_err()
                    .kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
    }

    #[test]
    fn ansi_quotes_payload_is_one_statement_only_without_ansi_quotes() {
        // Under ANSI_QUOTES this payload closes the identifier at the backslash-quote and
        // executes COMMIT. The provider rejects an inherited ANSI_QUOTES session instead of
        // scanning it as default-mode double-quoted text.
        let payload = "SELECT 1 AS \"a\\\"; COMMIT; SELECT 1 -- \"";
        let scan = scan_sql(payload).unwrap();
        assert_eq!(scan.statements.len(), 1);
        assert!(check_no_transaction_control(payload, &scan).is_ok());
    }

    #[test]
    fn indirect_execution_and_executable_comments_are_rejected() {
        for sql in [
            "CALL do_something()",
            "EXECUTE prepared_statement",
            "PREPARE statement FROM 'SELECT 1'",
            "SELECT 1; EXECUTE statement",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_indirect_execution(sql, &scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        let scan = scan_sql("SELECT 'CALL' AS word").unwrap();
        assert!(check_no_indirect_execution("SELECT 'CALL' AS word", &scan).is_ok());
        for sql in [
            "SELECT 1 /*!50000 , 2 */",
            "SELECT 1 /*M! , 2 */",
            "SELECT 1 /*M!100000 , 2 */",
            "SELECT 1 /*m! , 2 */",
        ] {
            let scan = scan_sql(sql).unwrap();
            assert_eq!(
                check_no_executable_comment(&scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql}"
            );
        }
        let scan = scan_sql("SELECT 1 /* ordinary comment */ AS value").unwrap();
        assert!(check_no_executable_comment(&scan).is_ok());
        let scan = scan_sql("SELECT 1 /* M! not executable */ AS value").unwrap();
        assert!(check_no_executable_comment(&scan).is_ok());
    }

    #[test]
    fn plaintext_requires_numeric_loopback() {
        assert!(MySqlConfig::plaintext_loopback("127.0.0.1", 3306, "root", "test").is_ok());
        assert!(MySqlConfig::plaintext_loopback("::1", 3306, "root", "test").is_ok());
        for host in ["localhost", "db.example.com", "0.0.0.0", "192.0.2.1"] {
            assert_eq!(
                MySqlConfig::plaintext_loopback(host, 3306, "root", "test")
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidArgument,
                "{host}"
            );
        }
    }

    #[test]
    fn configuration_validation_rejects_invalid_values() {
        let config = MySqlConfig::new("", 3306, "root", "test");
        assert_eq!(
            config.validate().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let config = MySqlConfig::new("db.example.com", 0, "root", "test");
        assert_eq!(
            config.validate().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let config = MySqlConfig::new("db.example.com", 3306, "root", "test").max_connections(0);
        assert_eq!(
            config.validate().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let config = MySqlConfig::new("db.example.com", 3306, "root", "test")
            .acquire_timeout(std::time::Duration::ZERO);
        assert_eq!(
            config.validate().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let config = MySqlConfig::new("db.example.com", 3306, "root", "test")
            .tls(MySqlTls::Insecure)
            .ca_file("/tmp/ca.pem");
        assert_eq!(
            config.validate().unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn debug_redacts_connection_identity() {
        let config =
            MySqlConfig::new("db.example.com", 3306, "app", "app").password("secret-value");
        let debug = format!("{config:?}");
        for value in ["db.example.com", "app", "secret-value", "3306"] {
            assert!(!debug.contains(value), "{debug} contains {value}");
        }
    }
}
