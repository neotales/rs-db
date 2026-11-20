#![doc = include_str!("../README.md")]

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, DbType, ErrorKind, ExecuteOptions,
    ImmediateTransaction, Parameter, Pool, PoolDefaults, QueryOptions, Reader, Row, RowLimit,
    Session, Transaction, TxOptions, Value,
};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::dispatcher::{Dispatch, with_default};
use url::{Host, Url};
use zeroize::Zeroizing;

use libsql::{
    Builder, Connection, Transaction as LibsqlTransactionHandle, Value as LibsqlValue,
    params_from_iter,
};

/// Validated remote libSQL configuration.
///
/// The token is zeroized when the configuration is dropped and is never included in `Debug`.
/// The vendor client keeps its own copy for the lifetime of the provider; this crate cannot
/// zeroize that copy. Endpoints, tokens, and SQL are not retained in error values.
pub struct LibsqlConfig {
    endpoint: String,
    token: Zeroizing<String>,
    plaintext: bool,
    max_connections: u32,
    acquire_timeout: Duration,
}

impl LibsqlConfig {
    /// Creates a configuration for a secure `libsql://` or `https://` endpoint.
    ///
    /// Construction defers URL validation until [`LibsqlPoolProvider::connect`]. Use
    /// [`Self::plaintext_loopback`] for a disposable local `sqld` listener.
    #[must_use]
    pub fn remote(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: Zeroizing::new(token.into()),
            plaintext: false,
            max_connections: 8,
            acquire_timeout: Duration::from_secs(5),
        }
    }

    /// Creates a cleartext configuration for a numeric loopback host only.
    ///
    /// The endpoint must use `http://` and a numeric loopback IP address. A host name such as
    /// `localhost` is rejected because name resolution can leave the loopback interface. This is
    /// for a disposable local server and is never selected automatically.
    ///
    /// # Errors
    /// Returns `InvalidArgument` unless the URL is cleartext, numeric loopback, and otherwise valid.
    pub fn plaintext_loopback(
        endpoint: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, DbError> {
        let config = Self {
            endpoint: endpoint.into(),
            token: Zeroizing::new(token.into()),
            plaintext: true,
            max_connections: 8,
            acquire_timeout: Duration::from_secs(5),
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the maximum number of concurrently acquired sessions. The default is eight.
    #[must_use]
    pub fn max_connections(mut self, count: u32) -> Self {
        self.max_connections = count;
        self
    }

    /// Bounds waiting for a session permit and for the initial connectivity check. The default is
    /// five seconds.
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    fn validate(&self) -> Result<(), DbError> {
        let millis = self.acquire_timeout.as_millis();
        if self.endpoint.is_empty()
            || self.endpoint.len() > 2048
            || self.endpoint.as_bytes().contains(&0)
            || self.token.as_bytes().contains(&0)
            || self.token.len() > 8192
            || self.max_connections == 0
            || self.max_connections > 1024
            || millis == 0
            || millis > i32::MAX as u128
        {
            return Err(invalid());
        }
        let url = Url::parse(&self.endpoint).map_err(|_| invalid())?;
        if url.username() != ""
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || url.port() == Some(0)
            || !matches!(url.path(), "" | "/")
        {
            return Err(invalid());
        }
        let Some(host) = url.host() else {
            return Err(invalid());
        };
        if self.plaintext {
            if url.scheme() != "http" || !is_loopback(&host) {
                return Err(invalid());
            }
        } else if !matches!(url.scheme(), "libsql" | "https") {
            return Err(invalid());
        }
        Ok(())
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn token(&self) -> &str {
        self.token.as_str()
    }
}

impl fmt::Debug for LibsqlConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LibsqlConfig")
            .field("endpoint", &"[redacted]")
            .field("token", &"[redacted]")
            .field("plaintext", &self.plaintext)
            .field("max_connections", &self.max_connections)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish_non_exhaustive()
    }
}

fn is_loopback(host: &Host<&str>) -> bool {
    match host {
        Host::Ipv4(address) => address.is_loopback(),
        Host::Ipv6(address) => address.is_loopback(),
        Host::Domain(_) => false,
    }
}

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn overflow() -> DbError {
    DbError::new(ErrorKind::Overflow)
}

fn connection_error() -> DbError {
    DbError::new(ErrorKind::Connection)
}

fn map_code(code: i32) -> DbError {
    match code & 0xff {
        1 | 21 | 25 => invalid(),
        3 | 8 => DbError::new(ErrorKind::Unsupported),
        5 | 6 => DbError::new(ErrorKind::Busy),
        9 => DbError::new(ErrorKind::Cancelled),
        18 => overflow(),
        19 => DbError::new(ErrorKind::Constraint),
        20 => DbError::new(ErrorKind::Decode),
        _ => connection_error(),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_error(error: libsql::Error) -> DbError {
    use libsql::Error;
    match error {
        Error::SqliteFailure(code, _) | Error::RemoteSqliteFailure(code, _, _) => map_code(code),
        Error::Misuse(_)
        | Error::Sqlite3SyntaxError(_, _, _)
        | Error::Sqlite3ParserError(_)
        | Error::TransactionalBatchError(_)
        | Error::ToSqlConversionFailure(_) => invalid(),
        Error::Sqlite3UnsupportedStatement | Error::ReservedBytesNotSupported => {
            DbError::new(ErrorKind::Unsupported)
        }
        Error::NullValue
        | Error::QueryReturnedNoRows
        | Error::ExecuteReturnedRows
        | Error::InvalidColumnName(_)
        | Error::ColumnNotFound(_)
        | Error::InvalidColumnIndex
        | Error::InvalidColumnType
        | Error::InvalidBlobSize(_) => DbError::new(ErrorKind::Decode),
        Error::WalConflict => DbError::new(ErrorKind::Busy),
        _ => connection_error(),
    }
}

fn map_write_error(error: libsql::Error) -> DbError {
    let mapped = map_error(error);
    match mapped.kind() {
        ErrorKind::Connection | ErrorKind::Timeout | ErrorKind::Cancelled => {
            DbError::new(ErrorKind::WriteOutcomeUnknown)
        }
        _ => mapped,
    }
}

fn budget(options: ExecuteOptions) -> Result<Option<Duration>, DbError> {
    match options.timeout_ms {
        None => Ok(None),
        Some(0) => Err(invalid()),
        Some(millis) => Ok(Some(Duration::from_millis(millis))),
    }
}

/// Polls one libSQL future under a no-op tracing dispatcher.
///
/// The vendor crate emits SQL text with `tracing::trace!` from its public execute/prepare methods.
/// A library cannot change the application's global subscriber, but it can make the vendor's calls
/// use a dispatcher that drops every event. The allocation is one box per database operation, not
/// per row.
struct Silent<F> {
    inner: Pin<Box<F>>,
}

impl<F: Future> Silent<F> {
    fn new(future: F) -> Self {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl<F: Future> Future for Silent<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        with_default(no_trace(), || this.inner.as_mut().poll(context))
    }
}

fn no_trace() -> &'static Dispatch {
    static NO_TRACE: OnceLock<Dispatch> = OnceLock::new();
    NO_TRACE.get_or_init(Dispatch::none)
}

fn silent<F: Future>(future: F) -> Silent<F> {
    Silent::new(future)
}

/// Remote libSQL/Turso pool implementing the driver-neutral [`neotales_dbapi`] traits.
#[derive(Clone)]
pub struct LibsqlPoolProvider {
    database: Arc<libsql::Database>,
    permits: Arc<Semaphore>,
    acquire_timeout: Duration,
}

impl fmt::Debug for LibsqlPoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LibsqlPoolProvider")
            .field("acquire_timeout", &self.acquire_timeout)
            .finish_non_exhaustive()
    }
}

impl LibsqlPoolProvider {
    /// Builds the remote client and verifies connectivity with one sanitized `SELECT 1`.
    ///
    /// The probe establishes that TLS, the endpoint, and authentication accept a request. It does
    /// not create or migrate any schema.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration, `Timeout` when the connectivity budget
    /// elapses, or a sanitized `Connection` error. No driver text is retained.
    pub async fn connect(config: LibsqlConfig) -> Result<Self, DbError> {
        config.validate()?;
        let endpoint = config.endpoint().to_owned();
        let token = config.token().to_owned();
        let database = match silent(Builder::new_remote(endpoint, token).build()).await {
            Ok(database) => Arc::new(database),
            Err(error) => return Err(map_error(error)),
        };
        let probe = async {
            let connection = database.connect().map_err(map_error)?;
            silent(connection.query("SELECT 1", ()))
                .await
                .map_err(map_error)?;
            Ok::<(), DbError>(())
        };
        match tokio::time::timeout(config.acquire_timeout, probe).await {
            Ok(result) => result?,
            Err(_) => return Err(DbError::new(ErrorKind::Timeout)),
        }
        Ok(Self {
            database,
            permits: Arc::new(Semaphore::new(config.max_connections as usize)),
            acquire_timeout: config.acquire_timeout,
        })
    }

    /// Acquires one exclusive session. Drop the session to release its permit.
    ///
    /// libSQL remote sessions are independent HTTP clients, not reused sockets. The configured
    /// maximum is a concurrency bound; dropping a session does not preserve server-side state.
    ///
    /// # Errors
    /// Returns `Timeout` when no permit becomes available within the configured budget, or a
    /// sanitized `Connection` error.
    pub async fn acquire_libsql_session(&self) -> Result<LibsqlSession, DbError> {
        let permit = match tokio::time::timeout(
            self.acquire_timeout,
            Arc::clone(&self.permits).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(connection_error()),
            Err(_) => return Err(DbError::new(ErrorKind::Timeout)),
        };
        let connection = self.database.connect().map_err(map_error)?;
        Ok(LibsqlSession {
            connection: Some(connection),
            _permit: permit,
            interrupted: Arc::new(AtomicBool::new(false)),
            poisoned: false,
        })
    }

    /// Acquires one exclusive session as an [`ImmediateTransaction`] trait object.
    ///
    /// # Errors
    /// Returns `Timeout` when no permit becomes available within the configured budget, or a
    /// sanitized `Connection` error.
    pub async fn acquire_libsql(&self) -> Result<Box<dyn ImmediateTransaction>, DbError> {
        Ok(Box::new(self.acquire_libsql_session().await?))
    }
}

impl Pool for LibsqlPoolProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            isolation_level: false,
            read_only_transactions: false,
            trusted_scripts: true,
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(
            async move { Ok(Box::new(self.acquire_libsql_session().await?) as Box<dyn Session>) },
        )
    }
}

/// One exclusive remote libSQL session.
///
/// Dropping a session releases its concurrency permit. A session that was interrupted or whose
/// remote statement failed is poisoned and refuses further calls; acquire a new session instead.
pub struct LibsqlSession {
    connection: Option<Connection>,
    _permit: OwnedSemaphorePermit,
    interrupted: Arc<AtomicBool>,
    poisoned: bool,
}

impl fmt::Debug for LibsqlSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LibsqlSession")
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl LibsqlSession {
    fn ensure_live(&mut self) -> Result<(), DbError> {
        if self.poisoned || self.interrupted.load(Ordering::Acquire) {
            self.discard();
            return Err(connection_error());
        }
        Ok(())
    }

    fn live(&mut self) -> Result<Connection, DbError> {
        self.ensure_live()?;
        self.connection.clone().ok_or_else(|| {
            self.discard();
            connection_error()
        })
    }

    fn discard(&mut self) {
        self.poisoned = true;
        self.interrupted.store(true, Ordering::Release);
        self.connection = None;
    }

    // libSQL's remote query path is a Hrana cursor: rows are pulled lazily rather
    // than returned through the buffering batch API. The row and byte checks happen
    // before the next row is retained, but one row (or one HTTP chunk) is already
    // materialized before its size is known, and the server may buffer its own
    // result. This is a hard bound on the detached result, not a hard transport or
    // process-memory cap on either side.
    async fn fetch_inner(
        connection: &Connection,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
    ) -> Result<Vec<Row>, DbError> {
        let mut rows = connection
            .query(sql, to_params(parameters))
            .await
            .map_err(map_write_error)?;
        let mut result = Vec::new();
        let mut bytes = 0usize;
        loop {
            let Some(row) = rows.next().await.map_err(map_write_error)? else {
                break;
            };
            if result.len() == limit.rows() {
                return Err(overflow());
            }
            let mut cells = Vec::with_capacity(usize::try_from(row.column_count()).unwrap_or(0));
            for index in 0..row.column_count() {
                let name = row.column_name(index).unwrap_or_default().to_owned();
                bytes = bytes.checked_add(name.len()).ok_or_else(overflow)?;
                let value = decode(
                    row.get_value(index)
                        .map_err(|_| DbError::new(ErrorKind::Decode))?,
                )?;
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
        Ok(result)
    }
}

impl Session for LibsqlSession {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            validate_statement(sql, parameters)?;
            let connection = self.live()?;
            let flight = Flight::new(&self.interrupted);
            match execute_inner(&connection, sql, parameters, budget).await {
                Ok(rows) => {
                    flight.disarm();
                    Ok(rows)
                }
                Err(error) => {
                    self.discard();
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
            validate_statement(sql, parameters)?;
            let connection = self.live()?;
            let flight = Flight::new(&self.interrupted);
            match fetch_with_budget(&connection, sql, parameters, limit, budget).await {
                Ok(rows) => {
                    flight.disarm();
                    Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
                }
                Err(error) => {
                    self.discard();
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
            check_sql(sql)?;
            let connection = self.live()?;
            let flight = Flight::new(&self.interrupted);
            match script_with_budget(&connection, sql, budget).await {
                Ok(()) => {
                    flight.disarm();
                    Ok(())
                }
                Err(error) => {
                    self.discard();
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
            check_tx_options(options)?;
            let connection = self.live()?;
            let flight = Flight::new(&self.interrupted);
            let tx = match silent(
                connection.transaction_with_behavior(libsql::TransactionBehavior::Deferred),
            )
            .await
            {
                Ok(tx) => tx,
                Err(error) => {
                    self.discard();
                    return Err(map_error(error));
                }
            };
            flight.disarm();
            Ok(Box::new(LibsqlTransaction {
                session: self,
                tx: Some(tx),
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

impl ImmediateTransaction for LibsqlSession {
    fn begin_immediate_async<'a>(&'a mut self) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            let connection = self.live()?;
            let flight = Flight::new(&self.interrupted);
            let tx = match silent(
                connection.transaction_with_behavior(libsql::TransactionBehavior::Immediate),
            )
            .await
            {
                Ok(tx) => tx,
                Err(error) => {
                    self.discard();
                    return Err(map_error(error));
                }
            };
            flight.disarm();
            Ok(Box::new(LibsqlTransaction {
                session: self,
                tx: Some(tx),
                done: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

struct LibsqlTransaction<'session> {
    session: &'session mut LibsqlSession,
    tx: Option<LibsqlTransactionHandle>,
    done: bool,
}

impl LibsqlTransaction<'_> {
    fn live(&self) -> Result<&LibsqlTransactionHandle, DbError> {
        if self.done {
            return Err(connection_error());
        }
        self.tx.as_ref().ok_or_else(connection_error)
    }
}

impl<'session> Transaction<'session> for LibsqlTransaction<'session> {
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            let budget = budget(options.provider_execute_options())?;
            let parameters = options.parameters.as_slice();
            validate_statement(sql, parameters)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = {
                let tx = self.live()?;
                execute_inner(tx, sql, parameters, budget).await
            };
            match result {
                Ok(rows) => {
                    flight.disarm();
                    Ok(rows)
                }
                Err(error) => {
                    self.session.discard();
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
            validate_statement(sql, parameters)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = {
                let tx = self.live()?;
                fetch_with_budget(tx, sql, parameters, limit, budget).await
            };
            match result {
                Ok(rows) => {
                    flight.disarm();
                    Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
                }
                Err(error) => {
                    self.session.discard();
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
            check_sql(sql)?;
            self.session.ensure_live()?;
            let flight = Flight::new(&self.session.interrupted);
            let result = {
                let tx = self.live()?;
                script_with_budget(tx, sql, budget).await
            };
            match result {
                Ok(()) => {
                    flight.disarm();
                    Ok(())
                }
                Err(error) => {
                    self.session.discard();
                    Err(error)
                }
            }
        })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            if let Err(error) = this.session.ensure_live() {
                this.done = true;
                this.session.discard();
                return Err(error);
            }
            let Some(tx) = this.tx.take() else {
                this.done = true;
                this.session.discard();
                return Err(connection_error());
            };
            let result = silent(tx.commit()).await;
            this.done = true;
            if let Ok(()) = result {
                Ok(())
            } else {
                this.session.discard();
                Err(DbError::new(ErrorKind::CommitOutcomeUnknown))
            }
        })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let mut this = *self;
            if let Err(error) = this.session.ensure_live() {
                this.done = true;
                this.session.discard();
                return Err(error);
            }
            let Some(tx) = this.tx.take() else {
                this.done = true;
                this.session.discard();
                return Err(connection_error());
            };
            let result = silent(tx.rollback()).await;
            this.done = true;
            if let Ok(()) = result {
                Ok(())
            } else {
                this.session.discard();
                Err(connection_error())
            }
        })
    }
}

impl Drop for LibsqlTransaction<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.tx.take();
            self.session.discard();
        }
    }
}

async fn execute_inner(
    connection: &Connection,
    sql: &str,
    parameters: &[Parameter],
    budget: Option<Duration>,
) -> Result<u64, DbError> {
    let future = silent(connection.execute(sql, to_params(parameters)));
    match budget {
        None => future.await.map_err(map_write_error),
        Some(duration) => match tokio::time::timeout(duration, future).await {
            Ok(result) => result.map_err(map_write_error),
            Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
        },
    }
}

async fn fetch_with_budget(
    connection: &Connection,
    sql: &str,
    parameters: &[Parameter],
    limit: RowLimit,
    budget: Option<Duration>,
) -> Result<Vec<Row>, DbError> {
    let future = silent(LibsqlSession::fetch_inner(
        connection, sql, parameters, limit,
    ));
    match budget {
        None => future.await,
        Some(duration) => match tokio::time::timeout(duration, future).await {
            Ok(result) => result,
            Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
        },
    }
}

// The batch path materializes rows in the vendor client before it returns. This
// function is therefore only for trusted scripts; `fetch` uses the cursor path.
async fn script_with_budget(
    connection: &Connection,
    sql: &str,
    budget: Option<Duration>,
) -> Result<(), DbError> {
    let future = silent(connection.execute_batch(sql));
    match budget {
        None => future.await.map(|_| ()).map_err(map_write_error),
        Some(duration) => match tokio::time::timeout(duration, future).await {
            Ok(result) => result.map(|_| ()).map_err(map_write_error),
            Err(_) => Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
        },
    }
}

fn to_params(parameters: &[Parameter]) -> impl libsql::params::IntoParams + '_ {
    params_from_iter(parameters.iter().map(|parameter| match parameter.value() {
        Value::Null(_) => LibsqlValue::Null,
        Value::Bool(value) => LibsqlValue::Integer(i64::from(*value)),
        Value::I64(value) => LibsqlValue::Integer(*value),
        Value::F64(value) => LibsqlValue::Real(*value),
        Value::Text(value) => LibsqlValue::Text(value.clone()),
        Value::Bytes(value) => LibsqlValue::Blob(value.clone()),
    }))
}

fn decode(value: LibsqlValue) -> Result<Value, DbError> {
    match value {
        LibsqlValue::Null => Ok(Value::Null(DbType::Text)),
        LibsqlValue::Integer(value) => Ok(Value::I64(value)),
        LibsqlValue::Real(value) if value.is_finite() => Ok(Value::F64(value)),
        LibsqlValue::Real(_) => Err(DbError::new(ErrorKind::Decode)),
        LibsqlValue::Text(value) => Ok(Value::Text(value)),
        LibsqlValue::Blob(value) => Ok(Value::Bytes(value)),
    }
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Text(text) => text.len(),
        Value::Bytes(bytes) => bytes.len(),
        _ => 0,
    }
}

fn check_tx_options(options: TxOptions) -> Result<(), DbError> {
    if options.isolation.is_some() || options.read_only == Some(true) {
        Err(DbError::new(ErrorKind::Unsupported))
    } else {
        Ok(())
    }
}

fn validate_statement(sql: &str, parameters: &[Parameter]) -> Result<(), DbError> {
    check_sql(sql)?;
    check_one_statement(sql)?;
    check_not_transaction_control(sql)?;
    check_arity(sql, parameters)
}

fn check_sql(sql: &str) -> Result<(), DbError> {
    if sql.as_bytes().contains(&0) {
        Err(invalid())
    } else {
        Ok(())
    }
}

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
                            return Err(invalid());
                        }
                        terminated = true;
                    }
                }
                b'\'' | b'"' | b'`' | b'[' => {
                    if terminated {
                        return Err(invalid());
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
                        return Err(invalid());
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
        return Err(invalid());
    }
    Ok(())
}

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
                return Err(invalid());
            }
            index += 2;
            continue;
        }
        break;
    }

    if !bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        return Err(invalid());
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

fn check_not_transaction_control(sql: &str) -> Result<(), DbError> {
    let token = leading_command(sql)?;
    if token.eq_ignore_ascii_case(b"BEGIN")
        || token.eq_ignore_ascii_case(b"COMMIT")
        || token.eq_ignore_ascii_case(b"END")
        || token.eq_ignore_ascii_case(b"ROLLBACK")
        || token.eq_ignore_ascii_case(b"SAVEPOINT")
        || token.eq_ignore_ascii_case(b"RELEASE")
    {
        Err(invalid())
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
                            .ok_or_else(invalid)?
                    } else {
                        anonymous = true;
                        maximum.checked_add(1).ok_or_else(invalid)?
                    };
                    if number == 0 {
                        return Err(invalid());
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
        return Err(invalid());
    }
    if maximum == parameters.len() {
        Ok(())
    } else {
        Err(invalid())
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

#[cfg(test)]
mod tests {
    use super::{
        LibsqlConfig, budget, check_arity, check_one_statement, check_tx_options, map_error,
        map_write_error, silent,
    };
    use neotales_dbapi::{
        DbType, ErrorKind, ExecuteOptions, IsolationLevel, Parameter, TxOptions, Value,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    struct CountLayer(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> Layer<S> for CountLayer {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn parameter() -> Parameter {
        Parameter::new(DbType::I64, Value::I64(1)).unwrap()
    }

    #[test]
    fn config_rejects_insecure_and_ambiguous_endpoints() {
        for endpoint in [
            "http://db.example.com",
            "ws://127.0.0.1:8080",
            "wss://db.example.com",
            "https://user:password@db.example.com",
            "https://db.example.com/path",
            "https://db.example.com?token=secret",
            "https://db.example.com#fragment",
            "https://db.example.com:0",
            "not a url",
        ] {
            assert!(
                LibsqlConfig::remote(endpoint, "token").validate().is_err(),
                "{endpoint}"
            );
        }
        assert!(
            LibsqlConfig::remote("libsql://db.example.com", "token")
                .validate()
                .is_ok()
        );
        assert!(
            LibsqlConfig::remote("https://db.example.com", "token")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn plaintext_requires_a_numeric_loopback_host() {
        assert!(
            LibsqlConfig::plaintext_loopback("http://127.0.0.1:8080", "")
                .unwrap()
                .validate()
                .is_ok()
        );
        assert!(
            LibsqlConfig::plaintext_loopback("http://[::1]:8080", "")
                .unwrap()
                .validate()
                .is_ok()
        );
        for endpoint in [
            "http://localhost:8080",
            "http://192.0.2.1:8080",
            "https://127.0.0.1:8080",
            "libsql://127.0.0.1:8080",
        ] {
            assert!(
                LibsqlConfig::plaintext_loopback(endpoint, "").is_err(),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn config_debug_redacts_the_endpoint_and_token() {
        let config = LibsqlConfig::remote("https://secret.example.com", "very-secret-token");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret.example.com"));
        assert!(!rendered.contains("very-secret-token"));
        assert!(rendered.contains("[redacted]"));
    }

    #[test]
    fn scanner_accepts_one_statement_with_quoted_semicolons() {
        for sql in [
            "SELECT 1",
            "SELECT ';'",
            "SELECT \"a;b\"",
            "SELECT `a;b`",
            "SELECT [a;b]",
            "SELECT 1; -- trailing comment",
            "/* leading ; */ SELECT 1;;;",
        ] {
            assert!(check_one_statement(sql).is_ok(), "{sql}");
        }
        for sql in [
            "",
            ";",
            "SELECT 1; SELECT 2",
            "SELECT 'unterminated",
            "SELECT 1 /* unterminated",
        ] {
            assert!(check_one_statement(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn arity_counts_only_unquoted_placeholders() {
        let sql = "SELECT ?, '?', \"?\", `?col`, ? -- ?\n, /* ? */ ?";
        let parameters = [parameter(), parameter(), parameter()];
        assert!(check_arity(sql, &parameters).is_ok());
        assert!(check_arity(sql, &parameters[..2]).is_err());

        let indexed = "SELECT ?2, ?1";
        assert!(check_arity(indexed, &parameters[..2]).is_ok());
        assert!(check_arity(indexed, &parameters[..1]).is_err());
        assert!(check_arity("SELECT ?, ?1", &parameters[..2]).is_err());
        assert!(check_arity("SELECT ?0", &[]).is_err());
        assert_eq!(
            check_arity("SELECT :name", &[]).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn transaction_options_are_rejected_before_dispatch() {
        assert!(check_tx_options(TxOptions::default()).is_ok());
        assert!(
            check_tx_options(TxOptions {
                isolation: Some(IsolationLevel::Serializable),
                read_only: None,
            })
            .is_err()
        );
        assert!(
            check_tx_options(TxOptions {
                isolation: None,
                read_only: Some(true),
            })
            .is_err()
        );
        assert!(
            check_tx_options(TxOptions {
                isolation: None,
                read_only: Some(false),
            })
            .is_ok()
        );
    }

    #[test]
    fn zero_timeout_is_rejected() {
        assert!(
            budget(ExecuteOptions {
                timeout_ms: Some(0),
            })
            .is_err()
        );
        assert!(
            budget(ExecuteOptions {
                timeout_ms: Some(1),
            })
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn errors_discard_driver_text() {
        let error = map_error(libsql::Error::SqliteFailure(
            19,
            "secret constraint name".to_owned(),
        ));
        assert_eq!(error.kind(), ErrorKind::Constraint);
        assert!(!format!("{error:?}").contains("secret"));

        let error = map_write_error(libsql::Error::ConnectionFailed(
            "token secret and endpoint".to_owned(),
        ));
        assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[test]
    fn silent_future_suppresses_vendor_events() {
        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::TRACE)
            .with(CountLayer(Arc::clone(&count)));
        tracing::subscriber::with_default(subscriber, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                silent(async {
                    tracing::trace!("SELECT secret FROM table");
                })
                .await;
            });
        });
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }
}
