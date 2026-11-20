#![doc = include_str!("../README.md")]
#![allow(clippy::doc_markdown)]

mod collect;
mod config;
mod connect;
mod params;
mod sql;
mod telemetry;

pub use config::ClickHouseConfig;
pub use telemetry::ClickHouseTelemetry;

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use klickhouse::{Client, KlickhouseError, QueryEvent};
use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, ErrorKind, ExecuteOptions, Parameter, Pool,
    PoolDefaults, QueryOptions, QuerySetting, Reader, Session, TelemetryOutcome, Transaction,
    TxOptions,
};

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

fn unknown() -> DbError {
    DbError::new(ErrorKind::WriteOutcomeUnknown)
}

/// Checks the local syntax and native parameters accepted by `execute_no_count`.
///
/// This is a conservative preflight only: it does not parse ClickHouse SQL completely,
/// contact the server, or establish that execution is safe or will succeed.
///
/// # Errors
/// Returns `InvalidArgument` or `Unsupported` for inputs rejected before dispatch.
pub fn validate_no_count_statement(sql: &str, parameters: &[Parameter]) -> Result<(), DbError> {
    prepare_no_count(sql, parameters).map(|_| ())
}

fn prepare_no_count(sql: &str, parameters: &[Parameter]) -> Result<Vec<(String, String)>, DbError> {
    let scan = sql::scan(sql)?;
    sql::check_write(sql, &scan)?;
    params::bind(&scan, parameters)
}

/// Native-protocol ClickHouse provider implementing the driver-neutral [`neotales_dbapi`] traits.
///
/// Every acquired session owns one exclusive native connection. The driver-neutral
/// `execute_async`, `execute_trusted_script_async`, and `begin_async` return `Unsupported`.
/// The concrete session separately provides [`ClickHouseSession::execute_no_count`].
/// Native progress does not establish an authoritative affected-row count, so no count is invented.
#[derive(Clone)]
pub struct ClickHousePoolProvider {
    config: ClickHouseConfig,
    telemetry: Option<Arc<ClickHouseTelemetry>>,
}

impl fmt::Debug for ClickHousePoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHousePoolProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ClickHousePoolProvider {
    /// Validates the configuration and verifies that the server is ClickHouse 24.8.
    ///
    /// A probe connection is opened and discarded. Every later session opens its own exclusive
    /// native connection and repeats the version gate.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration or an unreadable root bundle,
    /// `Connection` when the endpoint cannot be reached or verified, and `Unsupported` for a
    /// server that is not ClickHouse 24.8.
    pub async fn connect(config: ClickHouseConfig) -> Result<Self, DbError> {
        config.validate()?;
        drop(connect::connect(&config).await?);
        Ok(Self {
            config,
            telemetry: None,
        })
    }

    /// Installs a provider-level observer for concrete no-count writes.
    ///
    /// This does not enable retries or instrument driver-neutral operations.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: ClickHouseTelemetry) -> Self {
        self.telemetry = Some(Arc::new(telemetry));
        self
    }

    /// Opens one exclusive native connection and verifies the server version.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration or an unreadable root bundle,
    /// `Connection` when the connection or version probe fails, and `Unsupported` for a server
    /// that is not ClickHouse 24.8.
    pub async fn acquire_clickhouse(&self) -> Result<ClickHouseSession, DbError> {
        let client = connect::connect(&self.config).await?;
        Ok(ClickHouseSession {
            client,
            defaults: self.config.defaults(),
            poisoned: Arc::new(AtomicBool::new(false)),
            telemetry: self.telemetry.clone(),
        })
    }
}

impl Pool for ClickHousePoolProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: false,
            isolation_level: false,
            read_only_transactions: false,
            trusted_scripts: false,
        }
    }

    fn defaults(&self) -> PoolDefaults {
        self.config.defaults()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move { Ok(Box::new(self.acquire_clickhouse().await?) as Box<dyn Session>) })
    }
}

/// Concrete native ClickHouse session acquired by
/// [`ClickHousePoolProvider::acquire_clickhouse`].
///
/// The session owns one exclusive connection. A fetch that is interrupted, or whose native
/// stream ends without an explicit terminal packet, poisons the session; later operations return
/// `WriteOutcomeUnknown` before dispatch. Acquire a new session after an uncertain outcome.
pub struct ClickHouseSession {
    client: Client,
    defaults: PoolDefaults,
    poisoned: Arc<AtomicBool>,
    telemetry: Option<Arc<ClickHouseTelemetry>>,
}

impl fmt::Debug for ClickHouseSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseSession")
            .field("poisoned", &self.poisoned.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ClickHouseSession {
    /// Executes one trusted CREATE, DROP, or INSERT without reporting an affected-row count.
    ///
    /// SQL is not rewritten. Positional parameters use the same validated native
    /// `{p1:Type}` syntax as fetches. Unquoted SETTINGS tokens are rejected; client
    /// queries explicitly set `async_insert=0` to avoid async-enqueue acknowledgement.
    /// This is not a sandbox: SQL and database objects must be trusted. Success requires
    /// the native EndOfStream packet, not a guarantee of disk durability, replication,
    /// or completion of effects external to ClickHouse. A server error,
    /// timeout, or transport failure after dispatch returns `WriteOutcomeUnknown` and
    /// poisons the session; the server may still have applied or be applying the write.
    /// Dropping the future likewise poisons the session. Reconcile before retrying.
    ///
    /// # Errors
    /// Returns `InvalidArgument` or `Unsupported` before dispatch for invalid options,
    /// parameters, or SQL outside the supported subset. Native failures can also return
    /// `Connection` or `WriteOutcomeUnknown`.
    pub async fn execute_no_count(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        let mut observation = self
            .telemetry
            .as_ref()
            .map(|policy| telemetry::Observation::new(policy, sql, &options));
        if observation.is_some() {
            // This concrete policy handles only these options. Retry settings remain
            // untouched and are rejected by the normal provider validation.
            options.telemetry = QuerySetting::Off;
            options.sql_capture = QuerySetting::Off;
            options.sql_hook = None;
        }
        let result = self.execute_no_count_inner(sql, options).await;
        if let Some(observation) = &mut observation {
            observation.complete(match &result {
                Ok(()) => TelemetryOutcome::Success,
                Err(error) => TelemetryOutcome::Error(error.kind()),
            });
        }
        result
    }

    async fn execute_no_count_inner(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        options.prepare_execute_provider(self.defaults)?;
        let deadline = budget(options.provider_execute_options())?;
        if self.poisoned.load(Ordering::Acquire) {
            return Err(unknown());
        }
        let parameters = prepare_no_count(sql, &options.parameters)?;
        let guard = PoisonGuard::arm(Arc::clone(&self.poisoned));
        let work = async {
            let result = if parameters.is_empty() {
                self.client.query_events(sql, None).await
            } else {
                self.client.query_bound_validated(sql, parameters).await
            };
            let mut events = match result {
                Ok(events) => events,
                Err(error) => {
                    let failure = dispatch_failure(&error);
                    return (failure.rows.map(|_| ()), failure.connection_clean);
                }
            };
            loop {
                match events.next().await {
                    Some(Ok(QueryEvent::Data(_))) => {}
                    Some(Ok(QueryEvent::EndOfStream)) => return (Ok(()), true),
                    Some(Err(_)) | None => return (Err(unknown()), false),
                }
            }
        };
        let (result, clean) = match deadline {
            Some(timeout) => match tokio::time::timeout(timeout, work).await {
                Ok(outcome) => outcome,
                Err(_) => return Err(unknown()),
            },
            None => work.await,
        };
        if clean {
            guard.disarm();
        }
        result
    }
}

impl Session for ClickHouseSession {
    fn defaults(&self) -> PoolDefaults {
        self.defaults
    }

    fn execute_async<'a>(
        &'a mut self,
        _sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(self.defaults)?;
            budget(options.provider_execute_options())?;
            // Native progress does not prove an affected-row count for every write. Reject the
            // operation rather than manufacture a number.
            Err(unsupported())
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            let limit = options.prepare_provider(self.defaults)?;
            let budget = budget(options.provider_execute_options())?;
            if self.poisoned.load(Ordering::Acquire) {
                return Err(unknown());
            }
            let scan = sql::scan(sql)?;
            sql::check_fetch(sql, &scan)?;
            let parameters = params::bind(&scan, &options.parameters)?;
            let guard = PoisonGuard::arm(Arc::clone(&self.poisoned));
            let work = async {
                let events = if parameters.is_empty() {
                    self.client.query_events(sql, None).await
                } else {
                    self.client.query_bound_validated(sql, parameters).await
                };
                match events {
                    Ok(events) => collect::collect(events, limit).await,
                    Err(error) => dispatch_failure(&error),
                }
            };
            let collected = match budget {
                Some(timeout) => {
                    if let Ok(collected) = tokio::time::timeout(timeout, work).await {
                        collected
                    } else {
                        // The future is dropped after dispatch. The native stream aborts the
                        // connection, and the outcome of any server work is unknown.
                        drop(guard);
                        return Err(unknown());
                    }
                }
                None => work.await,
            };
            if collected.connection_clean {
                guard.disarm();
            } else {
                drop(guard);
            }
            let rows = collected.rows?;
            Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.prepare_script_provider(self.defaults)?;
            budget(options.provider_execute_options())?;
            // Scripts require authoritative write outcomes and are deliberately unavailable.
            Err(unsupported())
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        _options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(unsupported()) })
    }
}

fn dispatch_failure(error: &KlickhouseError) -> collect::Collected {
    let (kind, connection_clean) = match error {
        KlickhouseError::QueryNotDispatched => (ErrorKind::Connection, false),
        // A bare protocol error is raised before any query bytes are sent. Post-dispatch
        // failures are wrapped in `QueryOutcomeUnknown` by the native client.
        KlickhouseError::ProtocolError(_) => (ErrorKind::InvalidArgument, true),
        KlickhouseError::QueryOutcomeUnknown(_) | KlickhouseError::InsertOutcomeUnknown(_) => {
            (ErrorKind::WriteOutcomeUnknown, false)
        }
        _ => (ErrorKind::WriteOutcomeUnknown, false),
    };
    collect::Collected {
        rows: Err(DbError::new(kind)),
        connection_clean,
    }
}

fn budget(options: ExecuteOptions) -> Result<Option<Duration>, DbError> {
    match options.timeout_ms {
        None => Ok(None),
        Some(0) => Err(invalid()),
        Some(millis) => Ok(Some(Duration::from_millis(millis))),
    }
}

/// Marks the session poisoned if a fetch future is dropped before the native stream reaches an
/// explicit terminal packet.
struct PoisonGuard {
    poisoned: Arc<AtomicBool>,
    armed: bool,
}

impl PoisonGuard {
    fn arm(poisoned: Arc<AtomicBool>) -> Self {
        Self {
            poisoned,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonGuard {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClickHousePoolProvider, budget, dispatch_failure};
    use neotales_dbapi::{ErrorKind, ExecuteOptions, Pool};
    use std::time::Duration;

    #[test]
    fn capabilities_are_truthful() {
        let provider = ClickHousePoolProvider {
            config: crate::ClickHouseConfig::plaintext_loopback(
                "127.0.0.1",
                9000,
                "default",
                "default",
            )
            .unwrap(),
            telemetry: None,
        };
        let capabilities = provider.capabilities();
        assert!(!capabilities.transactions);
        assert!(!capabilities.isolation_level);
        assert!(!capabilities.read_only_transactions);
        assert!(!capabilities.trusted_scripts);
    }

    #[test]
    fn a_zero_timeout_is_invalid_before_dispatch() {
        assert_eq!(
            budget(ExecuteOptions {
                timeout_ms: Some(0)
            })
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            budget(ExecuteOptions {
                timeout_ms: Some(5)
            })
            .unwrap(),
            Some(Duration::from_millis(5))
        );
        assert_eq!(budget(ExecuteOptions { timeout_ms: None }).unwrap(), None);
    }

    #[test]
    fn dispatch_failures_are_sanitized_and_classified() {
        let not_dispatched = dispatch_failure(&klickhouse::KlickhouseError::QueryNotDispatched);
        assert!(!not_dispatched.connection_clean);
        assert_eq!(
            not_dispatched.rows.unwrap_err().kind(),
            ErrorKind::Connection
        );

        let protocol = dispatch_failure(&klickhouse::KlickhouseError::ProtocolError(
            "detail must not escape".to_owned(),
        ));
        assert!(protocol.connection_clean);
        assert_eq!(
            protocol.rows.unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );

        let unknown =
            dispatch_failure(&klickhouse::KlickhouseError::QueryOutcomeUnknown(Box::new(
                klickhouse::KlickhouseError::ProtocolError("detail".to_owned()),
            )));
        assert!(!unknown.connection_clean);
        assert_eq!(
            unknown.rows.unwrap_err().kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }
}
