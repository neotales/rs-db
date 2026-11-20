//! Driver-neutral async SQL execution and detached, bounded row access.
//!
//! This crate deliberately provides no query builder, ORM, runtime, or driver. A provider owns
//! connection acquisition, type conversion, transaction behavior, and cleanup. All operations use
//! boxed `Send` futures so the traits are object safe. This contract cannot directly wrap browser
//! APIs whose futures are `!Send` (for example, D1 Workers bindings). The [`local`] module mirrors
//! these traits without a `Send` future bound for that case; it is a local-thread API, not a
//! thread-transfer grant.
//!
//! The [`sync`] module provides runtime-neutral synchronous counterparts that reuse the same rows,
//! limits, options, and errors. The optional `blocking` feature adds `blocking::BlockingPool`,
//! which drives an asynchronous pool on dedicated worker threads for callers that cannot await.
//! The `blocking` feature is not supported on wasm32. Async or local traits may be used with
//! providers that support them, but D1's local DBAPI operations currently fail closed.
//!
//! # Example
//!
//! ```no_run
//! use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit};
//!
//! # async fn run(pool: &dyn Pool) -> Result<(), neotales_dbapi::DbError> {
//! let mut session = pool.acquire_async().await?;
//! let params = [Parameter::new(DbType::I64, neotales_dbapi::Value::I64(7))?];
//! // SQLite uses `?`; PostgreSQL uses `$1`. The provider does not rewrite SQL.
//! let mut reader = session.fetch_async("SELECT name FROM users WHERE id = ?",
//!     QueryOptions::new().bind_all(&params).limit(RowLimit::new(10, 4096)?)).await?;
//! while let Some(row) = reader.next_row_async().await? {
//!     if let Some(name) = row.read::<Option<String>>("name")? {
//!         println!("{name}");
//!     }
//! }
//! reader.finish_async().await?;
//! # Ok(())
//! # }
//! ```
//!
//! A reader owns one statement's result sets until it is finished or dropped. `next_row_async` yields
//! rows from the current set. `has_next_set_async` advances, discarding any unread rows in the current
//! set, and reports whether another set exists. `finish_async` drains and discards the full batch so a
//! later statement error is not hidden. A provider with unread results still attached must discard
//! the connection when a reader is dropped; a provider that materializes the bounded batch before
//! returning may already have made the connection reusable.
//!
//! The [`paging`] module adds page and row views over one existing reader. It dispatches no SQL and
//! does not change the reader's parameters, telemetry, or bounded `RowLimit`; the page view exposes
//! result-set boundaries, including empty sets. Its [`paging::multi`] submodule adds explicit
//! multi-request paging where a caller-implemented source owns each request, continuation, and
//! keyset/offset logic. [`local::paging`] mirrors both for `!Send` local futures, and
//! [`sync::paging`] provides native blocking iterators over the synchronous reader.
//!
//! Native positional placeholder syntax is preserved (SQL is never rewritten):
//!
//! ```
//! use neotales_dbapi::{DbType, Parameter, Value};
//! let sqlite_sql = "SELECT name FROM users WHERE id = ?";
//! let sqlite_parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
//! let postgres_sql = "SELECT name FROM users WHERE id = $1 AND active = $2";
//! let postgres_parameters = [
//!     Parameter::new(DbType::I64, Value::I64(7))?,
//!     Parameter::new(DbType::Bool, Value::Bool(true))?,
//! ];
//! # let _ = (sqlite_sql, sqlite_parameters, postgres_sql, postgres_parameters);
//! # Ok::<(), neotales_dbapi::DbError>(())
//! ```

#[cfg(all(feature = "blocking", target_arch = "wasm32"))]
compile_error!(
    "neotales-dbapi's blocking feature is unsupported on wasm32; disable it and consult your provider's supported APIs"
);

#[cfg(all(feature = "blocking", not(target_arch = "wasm32")))]
pub mod blocking;
pub mod local;
pub mod paging;
pub mod sync;

pub use neotales_resilience::{Cancellation, Delay};
use neotales_resilience::{
    RetryOutcome, RetryPolicy as ResilienceRetryPolicy, execute_idempotent_retry,
};
use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

/// A `Send` boxed future returning a sanitized database result.
pub type DbFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 'a>>;

/// SQL logical type used to define typed null parameters and expected row values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbType {
    /// Boolean.
    Bool,
    /// Signed 64-bit integer.
    I64,
    /// 64-bit floating-point number.
    F64,
    /// UTF-8 text.
    Text,
    /// Opaque bytes.
    Bytes,
}

/// A supported, owned parameter or cell value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Null with an explicit SQL type.
    Null(DbType),
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    I64(i64),
    /// Finite 64-bit float.
    F64(f64),
    /// UTF-8 text.
    Text(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
}
impl Value {
    /// Rejects non-finite floating-point values; providers must preserve all other variants exactly.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for NaN or infinity.
    pub fn validate(&self) -> Result<(), DbError> {
        if matches!(self, Self::F64(value) if !value.is_finite()) {
            Err(DbError::invalid(InvalidArgumentReason::NonFiniteFloat))
        } else {
            Ok(())
        }
    }
}

/// A positional parameter whose value must match its declared type exactly.
#[derive(Clone, Debug, PartialEq)]
pub struct Parameter {
    db_type: DbType,
    value: Value,
}
impl Parameter {
    /// Creates and validates one positional parameter without coercion.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for a type mismatch or non-finite float.
    pub fn new(db_type: DbType, value: Value) -> Result<Self, DbError> {
        let matches = matches!((&value, db_type), (Value::Null(t), expected) if *t == expected)
            || matches!(
                (&value, db_type),
                (Value::Bool(_), DbType::Bool)
                    | (Value::I64(_), DbType::I64)
                    | (Value::F64(_), DbType::F64)
                    | (Value::Text(_), DbType::Text)
                    | (Value::Bytes(_), DbType::Bytes)
            );
        if !matches {
            return Err(DbError::invalid(
                InvalidArgumentReason::ParameterTypeMismatch,
            ));
        }
        value.validate()?;
        Ok(Self { db_type, value })
    }
    /// Returns the declared SQL type.
    #[must_use]
    pub const fn db_type(&self) -> DbType {
        self.db_type
    }
    /// Returns the validated owned value.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// Reported provider capabilities; true means the operation is supported as described.
#[allow(clippy::struct_excessive_bools)] // Independent capability flags must be directly reportable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Capabilities {
    /// Interactive transactions on one acquired connection.
    pub transactions: bool,
    /// Requested portable isolation levels can be applied.
    pub isolation_level: bool,
    /// Read-only transactions can be requested.
    pub read_only_transactions: bool,
    /// Trusted script execution is supported.
    pub trusted_scripts: bool,
}

/// Portable transaction isolation level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IsolationLevel {
    /// Serializable isolation.
    Serializable,
    /// Repeatable-read isolation.
    RepeatableRead,
    /// Read-committed isolation.
    ReadCommitted,
    /// Read-uncommitted isolation.
    ReadUncommitted,
}
/// Portable transaction options; omitted values retain provider-defined defaults.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxOptions {
    /// Requested isolation; unsupported requests fail before beginning.
    pub isolation: Option<IsolationLevel>,
    /// Requested read-only/access mode; unsupported requests fail before beginning.
    pub read_only: Option<bool>,
}
/// Statement execution options.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecuteOptions {
    /// Provider-defined timeout in milliseconds; `None` means no requested timeout.
    pub timeout_ms: Option<u64>,
}

/// Per-query inherit/off/on setting for telemetry or SQL capture.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QuerySetting {
    /// Use the configured pool default.
    #[default]
    Inherit,
    /// Disable this query's setting, overriding an enabled pool default.
    Off,
    /// Enable this query's setting, overriding a disabled pool default.
    On,
}

/// Whether a standalone pool operation may use the configured retry policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplaySafety {
    /// Do not retry. SQL shape is never used to infer safety.
    #[default]
    Never,
    /// The caller asserts that repeating this standalone operation is safe.
    ExplicitlyReplaySafe,
}

/// Pool retry configuration backed by `neotales-resilience`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryPolicy {
    /// Execute once. This is the default.
    #[default]
    Disabled,
    /// Retry explicitly replay-safe standalone statements, up to the configured attempt cap.
    Enabled {
        /// Existing resilience retry policy, including bounded attempts and delay shape.
        policy: ResilienceRetryPolicy,
        /// Whether sanitized `Timeout` errors are retry candidates.
        retry_timeouts: bool,
    },
}
impl RetryPolicy {
    /// Builds an enabled retry policy using the resilience crate's bounded retry configuration.
    ///
    /// `retry_timeouts` is off unless explicitly requested. Busy and Connection failures are
    /// candidates by default, but retries still require [`ReplaySafety::ExplicitlyReplaySafe`].
    ///
    /// # Errors
    /// Returns [`InvalidRetryPolicy`] when `max_attempts` is zero.
    pub fn new(
        max_attempts: u32,
        delay: Delay,
        retry_timeouts: bool,
    ) -> Result<Self, InvalidRetryPolicy> {
        let policy =
            ResilienceRetryPolicy::new(max_attempts, delay).map_err(|_| InvalidRetryPolicy)?;
        Ok(Self::Enabled {
            policy,
            retry_timeouts,
        })
    }
}

/// Retry configuration was invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidRetryPolicy;
impl fmt::Display for InvalidRetryPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("retry policy requires at least one attempt")
    }
}
impl Error for InvalidRetryPolicy {}

/// Per-operation retry configuration override.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetrySetting {
    /// Use the containing pool's default.
    #[default]
    Inherit,
    /// Disable retries for this operation.
    Off,
    /// Replace the containing pool's retry policy for this operation.
    Use(RetryPolicy),
}

/// Caller-provided timer, backoff sleeper, cancellation flag, and random source for retries.
///
/// No runtime or task is created by this crate. The timer must measure the complete execution
/// budget (acquisition, attempts, and backoff), and must wake the executor when it expires.
/// Pool defaults share this value across calls; use [`QueryOptions::retry_runtime`] for a
/// cancellation token scoped to an individual operation.
pub struct RetryRuntime {
    cancellation: Arc<Cancellation>,
    timer: Arc<dyn Fn() -> RetryWait + Send + Sync>,
    sleep: Arc<dyn Fn(Duration) -> RetryWait + Send + Sync>,
    random: Arc<dyn Fn() -> u64 + Send + Sync>,
    // Only operation-local copies carry this. Weak ownership prevents retained provider options
    // from extending the operation's lifetime or imposing its gate on later independent calls.
    dispatch: Option<Weak<RetryDispatch>>,
}
impl Clone for RetryRuntime {
    fn clone(&self) -> Self {
        Self {
            cancellation: Arc::clone(&self.cancellation),
            timer: Arc::clone(&self.timer),
            sleep: Arc::clone(&self.sleep),
            random: Arc::clone(&self.random),
            dispatch: self.dispatch.clone(),
        }
    }
}

/// A boxed sendable caller-owned timer or sleeper future.
pub type RetryWait = Pin<Box<dyn Future<Output = ()> + Send>>;

impl RetryRuntime {
    /// Creates retry runtime callbacks. For an enabled, explicitly replay-safe operation the timer
    /// factory is called exactly once, before acquisition, even if the operation succeeds immediately.
    /// The sleeper is called for each backoff. Use a pending timer when no total deadline is desired.
    pub fn new(
        cancellation: Arc<Cancellation>,
        timer: impl Fn() -> RetryWait + Send + Sync + 'static,
        sleep: impl Fn(Duration) -> RetryWait + Send + Sync + 'static,
        random: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancellation,
            timer: Arc::new(timer),
            sleep: Arc::new(sleep),
            random: Arc::new(random),
            dispatch: None,
        }
    }
}

impl fmt::Debug for RetryRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetryRuntime")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Sanitized operation outcome used by the optional observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryOutcome {
    /// The operation succeeded. Row contents and affected-row counts are not included.
    Success,
    /// The operation failed with a stable database error category.
    Error(ErrorKind),
}

/// Fixed-cardinality operation name; SQL is never used as a metric label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// One execute statement.
    Execute,
    /// One bounded fetch statement.
    Fetch,
}

/// Safe telemetry event: no parameters, rows, results, driver text, or connection information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbEvent {
    /// Fixed operation name suitable for a bounded metric label.
    pub operation: Operation,
    /// Elapsed wall time for the statement operation.
    pub duration: Duration,
    /// Sanitized result category.
    pub outcome: TelemetryOutcome,
    /// Optional SQL text when capture was explicitly enabled and the hook retained it.
    pub sql: Option<String>,
}

/// Optional SQL-only hook. Returning `None` suppresses SQL text; returning a string replaces it.
///
/// Panics are contained. A panic payload is intentionally forgotten to prevent a panicking
/// destructor from unwinding after the database operation; each hook panic may leak its payload.
pub type SqlCaptureHook = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;
/// Optional per-pool observer.
///
/// Panics are contained and do not affect database results. Because a panic payload's destructor
/// can itself panic, payloads from callback panics are intentionally forgotten; each such panic
/// may leak its payload.
pub type DbObserver = Arc<dyn Fn(&DbEvent) + Send + Sync>;

/// Pool-level defaults for telemetry, SQL capture, and retry support.
#[derive(Clone)]
pub struct PoolPolicy {
    /// Whether telemetry is enabled by default. No observer means no event is emitted.
    pub telemetry_enabled: bool,
    /// Whether exact SQL capture is enabled by default. SQL literals/comments may contain secrets.
    pub sql_capture_enabled: bool,
    /// SQL-only pool hook, called only when SQL capture is enabled.
    pub sql_hook: Option<SqlCaptureHook>,
    /// Optional observer. Its panic is isolated from the database operation.
    pub observer: Option<DbObserver>,
    /// Retry default for standalone pool `execute_async`/`fetch_async` calls; default is Disabled.
    pub retry: RetryPolicy,
    /// Caller-owned scheduling and cancellation dependencies required when retries are enabled.
    pub retry_runtime: Option<RetryRuntime>,
    /// Default statement timeout for operations that inherit. `None` inherits the wrapped
    /// provider's default; use per-query `no_timeout()` to request disabling it.
    pub default_timeout: Option<Duration>,
    /// Default bounded fetch limit for operations that inherit.
    pub default_limit: RowLimit,
}
impl Default for PoolPolicy {
    fn default() -> Self {
        let defaults = PoolDefaults::default();
        Self {
            telemetry_enabled: defaults.telemetry_enabled,
            sql_capture_enabled: defaults.sql_capture_enabled,
            sql_hook: None,
            observer: None,
            retry: defaults.retry,
            retry_runtime: None,
            default_timeout: defaults.timeout,
            default_limit: defaults.limit,
        }
    }
}
fn policy_defaults(policy: &PoolPolicy) -> PoolDefaults {
    PoolDefaults {
        timeout: policy.default_timeout,
        limit: policy.default_limit,
        telemetry_enabled: policy.telemetry_enabled,
        sql_capture_enabled: policy.sql_capture_enabled,
        retry: policy.retry,
    }
}
impl fmt::Debug for PoolPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PoolPolicy")
            .field("telemetry_enabled", &self.telemetry_enabled)
            .field("sql_capture_enabled", &self.sql_capture_enabled)
            .field("has_sql_hook", &self.sql_hook.is_some())
            .field("has_observer", &self.observer.is_some())
            .field("retry", &self.retry)
            .field("has_retry_runtime", &self.retry_runtime.is_some())
            .field("default_timeout", &self.default_timeout)
            .field("default_limit", &self.default_limit)
            .finish()
    }
}

/// Owned parameters and policy overrides for one pool, session, transaction, or script operation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// Ordered operation-local parameters. Never inherited from pool defaults.
    pub parameters: Vec<Parameter>,
    /// Tri-state timeout override.
    pub timeout: TimeoutSetting,
    /// Tri-state bounded fetch-limit override.
    pub limit: LimitSetting,
    /// Resolved provider timeout, not a separate caller override.
    execute: ExecuteOptions,
    /// Whether telemetry is inherited, disabled, or enabled for this operation.
    pub telemetry: QuerySetting,
    /// Whether SQL capture is inherited, disabled, or enabled for this operation.
    pub sql_capture: QuerySetting,
    /// Per-query SQL hook, replacing the pool hook when present.
    pub sql_hook: Option<SqlCaptureHook>,
    /// Retry policy override for a standalone `PolicyPool` `execute_async`/`fetch_async` operation.
    pub retry: RetrySetting,
    /// Explicit caller assertion required before any retry can occur.
    pub replay_safety: ReplaySafety,
    /// Per-operation retry runtime, replacing the pool default when present.
    pub retry_runtime: Option<RetryRuntime>,
    // An enclosing policy handles telemetry and retry; callers cannot bypass validation.
    policy_applied: bool,
}
impl fmt::Debug for QueryOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryOptions")
            .field("parameter_count", &self.parameters.len())
            .field("timeout", &self.timeout)
            .field("limit", &self.limit)
            .field("execute", &self.execute)
            .field("telemetry", &self.telemetry)
            .field("sql_capture", &self.sql_capture)
            .field("has_sql_hook", &self.sql_hook.is_some())
            .field("retry", &self.retry)
            .field("replay_safety", &self.replay_safety)
            .field("has_retry_runtime", &self.retry_runtime.is_some())
            .finish_non_exhaustive()
    }
}
impl QueryOptions {
    /// Provider-facing execution settings after resolution.
    #[must_use]
    pub fn provider_execute_options(&self) -> ExecuteOptions {
        self.execute
    }
    /// Creates options that inherit every global default, carry no parameters, and remain
    /// non-replay-safe.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one already validated parameter in positional order.
    #[must_use]
    pub fn bind(mut self, parameter: Parameter) -> Self {
        self.parameters.push(parameter);
        self
    }

    /// Appends parameters in slice order.
    #[must_use]
    pub fn bind_all(mut self, parameters: &[Parameter]) -> Self {
        self.parameters.extend_from_slice(parameters);
        self
    }

    /// Appends finite float, text, or byte parameters with exact declared types.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for a non-finite float.
    pub fn bind_f64(mut self, value: f64) -> Result<Self, DbError> {
        self.parameters
            .push(Parameter::new(DbType::F64, Value::F64(value))?);
        Ok(self)
    }

    /// Appends a typed SQL null.
    #[must_use]
    pub fn bind_null(mut self, db_type: DbType) -> Self {
        self.parameters.push(Parameter {
            db_type,
            value: Value::Null(db_type),
        });
        self
    }

    /// Replaces the pool timeout for this operation.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = TimeoutSetting::Set(timeout);
        self
    }

    /// Explicitly requests no timeout for this operation.
    #[must_use]
    pub fn no_timeout(mut self) -> Self {
        self.timeout = TimeoutSetting::Disabled;
        self
    }

    /// Replaces the pool fetch limit for this operation.
    #[must_use]
    pub fn limit(mut self, limit: RowLimit) -> Self {
        self.limit = LimitSetting::Set(limit);
        self
    }

    /// Sets telemetry inheritance/off/on.
    #[must_use]
    pub fn telemetry(mut self, setting: QuerySetting) -> Self {
        self.telemetry = setting;
        self
    }

    /// Sets SQL-capture inheritance/off/on.
    #[must_use]
    pub fn sql_capture(mut self, setting: QuerySetting) -> Self {
        self.sql_capture = setting;
        self
    }

    /// Sets retry inheritance/off/explicit policy.
    #[must_use]
    pub fn retry(mut self, setting: RetrySetting) -> Self {
        self.retry = setting;
        self
    }

    /// Replaces the per-operation SQL capture hook.
    #[must_use]
    pub fn sql_hook(mut self, hook: SqlCaptureHook) -> Self {
        self.sql_hook = Some(hook);
        self
    }

    /// Replaces the retry runtime for this operation.
    #[must_use]
    pub fn retry_runtime(mut self, runtime: RetryRuntime) -> Self {
        self.retry_runtime = Some(runtime);
        self
    }

    /// Marks the operation explicitly replay-safe. Defaults remain `Never`.
    #[must_use]
    pub fn replay_safe(mut self) -> Self {
        self.replay_safety = ReplaySafety::ExplicitlyReplaySafe;
        self
    }

    /// Resolves the timeout against pool defaults.
    #[must_use]
    pub fn resolved_timeout(&self, defaults: PoolDefaults) -> Option<Duration> {
        match self.timeout {
            TimeoutSetting::Inherit => defaults.timeout,
            TimeoutSetting::Disabled => None,
            TimeoutSetting::Set(timeout) => Some(timeout),
        }
    }

    /// Resolves the fetch limit against pool defaults.
    #[must_use]
    pub fn resolved_limit(&self, defaults: PoolDefaults) -> RowLimit {
        match self.limit {
            LimitSetting::Inherit => defaults.limit,
            LimitSetting::Set(limit) => limit,
        }
    }

    /// Resolve provider-facing timeout and reject policy options no wrapper handles.
    ///
    /// # Errors
    /// Returns `Unsupported` for policy facilities unavailable on a bare provider, or
    /// `InvalidArgument` if the timeout cannot be represented in milliseconds.
    pub fn prepare_provider(&mut self, defaults: PoolDefaults) -> Result<RowLimit, DbError> {
        if !self.policy_applied {
            reject_unhandled_pool_options(self)?;
        }
        let limit = self.resolved_limit(defaults);
        self.limit = LimitSetting::Set(limit);
        resolve_provider_timeout(self, defaults.timeout)?;
        Ok(limit)
    }

    /// Reject a fetch-only limit before dispatching a non-fetch statement.
    ///
    /// Inherited pool limits do not apply to execute operations.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when a per-operation fetch limit was supplied.
    pub fn validate_execute(&self) -> Result<(), DbError> {
        if matches!(self.limit, LimitSetting::Set(_)) {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(())
    }

    /// Validate and resolve options for a provider's execute operation.
    ///
    /// # Errors
    /// Returns an error for an explicit fetch limit or unsupported provider settings.
    pub fn prepare_execute_provider(&mut self, defaults: PoolDefaults) -> Result<(), DbError> {
        self.validate_execute()?;
        self.prepare_provider(defaults)?;
        Ok(())
    }

    /// Reject settings that cannot apply to an unparameterized trusted script.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if parameters or a fetch limit were supplied.
    pub fn validate_script(&self) -> Result<(), DbError> {
        if !self.parameters.is_empty() || !matches!(self.limit, LimitSetting::Inherit) {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(())
    }

    /// Resolve a trusted script's provider settings after rejecting inapplicable options.
    ///
    /// # Errors
    /// Returns an error for inapplicable or unsupported settings.
    pub fn prepare_script_provider(&mut self, defaults: PoolDefaults) -> Result<(), DbError> {
        self.validate_script()?;
        self.prepare_provider(defaults)?;
        Ok(())
    }
}

// The low-level provider boundary currently accepts millisecond timeouts. Round up so
// a positive sub-millisecond timeout never accidentally becomes an immediate timeout.
fn resolve_provider_timeout(
    options: &mut QueryOptions,
    default: Option<Duration>,
) -> Result<(), DbError> {
    let timeout = match options.timeout {
        TimeoutSetting::Inherit => default,
        TimeoutSetting::Disabled => None,
        TimeoutSetting::Set(timeout) => Some(timeout),
    };
    options.timeout = timeout.map_or(TimeoutSetting::Disabled, TimeoutSetting::Set);
    options.execute.timeout_ms = timeout
        .map(|value| {
            u64::try_from(
                value
                    .as_millis()
                    .saturating_add(u128::from(value.subsec_nanos() % 1_000_000 != 0)),
            )
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))
        })
        .transpose()?;
    Ok(())
}

// A bare pool has no observer or retry runtime. Reject requests requiring those
// facilities rather than silently dropping them at the provider boundary.
fn reject_unhandled_pool_options(options: &QueryOptions) -> Result<(), DbError> {
    if options.telemetry == QuerySetting::On
        || options.sql_capture == QuerySetting::On
        || options.sql_hook.is_some()
        || matches!(
            options.retry,
            RetrySetting::Use(RetryPolicy::Enabled { .. })
        )
        || options.retry_runtime.is_some()
    {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    Ok(())
}

fn reject_session_retry(options: &QueryOptions) -> Result<(), DbError> {
    if !options.policy_applied
        && (matches!(
            options.retry,
            RetrySetting::Use(RetryPolicy::Enabled { .. })
        ) || options.retry_runtime.is_some())
    {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    Ok(())
}

/// Hard maximums for detached fetch results.
///
/// `rows` and `bytes` are aggregate across every result set. `max_sets` also counts empty
/// sets, so a provider cannot return an unbounded number of empty boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowLimit {
    rows: usize,
    bytes: usize,
    max_sets: usize,
}
impl RowLimit {
    /// Creates a single-result-set limit with non-zero hard row and byte bounds.
    ///
    /// Use [`RowLimit::with_max_sets`] when the statement may return multiple sets.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if either bound is zero.
    pub fn new(rows: usize, bytes: usize) -> Result<Self, DbError> {
        Self::with_max_sets(rows, bytes, 1)
    }
    /// Creates a limit with non-zero row, byte, and aggregate result-set bounds.
    ///
    /// The set bound counts empty sets. Providers must fail, never truncate, when any bound is
    /// exceeded.
    ///
    /// # Errors
    /// Returns `InvalidArgument` with a static reason if any bound is zero.
    pub fn with_max_sets(rows: usize, bytes: usize, max_sets: usize) -> Result<Self, DbError> {
        if rows == 0 {
            return Err(DbError::invalid(InvalidArgumentReason::ZeroFetchRowLimit));
        }
        if bytes == 0 {
            return Err(DbError::invalid(InvalidArgumentReason::ZeroByteLimit));
        }
        if max_sets == 0 {
            return Err(DbError::invalid(InvalidArgumentReason::ZeroSetLimit));
        }
        Ok(Self {
            rows,
            bytes,
            max_sets,
        })
    }
    /// Returns the maximum number of rows across all sets.
    #[must_use]
    pub const fn rows(self) -> usize {
        self.rows
    }
    /// Returns the aggregate byte ceiling across all sets.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    /// Returns the maximum number of result sets, including empty sets.
    #[must_use]
    pub const fn max_sets(self) -> usize {
        self.max_sets
    }
}

impl Default for RowLimit {
    /// A bounded default: 1,000 rows, 1 MiB, one result set.
    fn default() -> Self {
        Self {
            rows: 1_000,
            bytes: 1_048_576,
            max_sets: 1,
        }
    }
}

/// Tri-state timeout override. `Inherit` uses the pool default; `Disabled` explicitly requests
/// no timeout; `Set` replaces the pool default for this operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TimeoutSetting {
    /// Use the pool default.
    #[default]
    Inherit,
    /// Explicitly request no timeout. Use only when the caller owns a separate cancellation path.
    Disabled,
    /// Replace the pool default with this timeout.
    Set(Duration),
}

/// Bounded fetch limit override. There is deliberately no unbounded variant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LimitSetting {
    /// Use the pool default.
    #[default]
    Inherit,
    /// Replace the pool default for this operation.
    Set(RowLimit),
}

/// Defaults supplied by a pool for operations that inherit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolDefaults {
    /// Default statement timeout. `None` means the pool chose no timeout.
    pub timeout: Option<Duration>,
    /// Default bounded fetch limit.
    pub limit: RowLimit,
    /// Whether telemetry is enabled by default.
    pub telemetry_enabled: bool,
    /// Whether exact SQL capture is enabled by default.
    pub sql_capture_enabled: bool,
    /// Default retry policy.
    pub retry: RetryPolicy,
}
impl Default for PoolDefaults {
    fn default() -> Self {
        Self {
            // A portable statement deadline cannot be promised by every provider. A provider
            // may choose an enforceable deadline; unsupported explicit deadlines must fail.
            timeout: None,
            limit: RowLimit::default(),
            telemetry_enabled: false,
            sql_capture_enabled: false,
            retry: RetryPolicy::Disabled,
        }
    }
}

/// Stable, sanitized error category. No driver message or query data is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Invalid argument, including a zero bound or invalid parameter value.
    InvalidArgument,
    /// Constraint violation.
    Constraint,
    /// Database is busy/locked.
    Busy,
    /// Deadline elapsed.
    Timeout,
    /// Operation was cancelled by provider.
    Cancelled,
    /// Requested capability is unavailable.
    Unsupported,
    /// A blocking adapter was called from the worker thread that would have to service it.
    ///
    /// The adapter refuses to wait on itself; a caller on its own worker thread must not block.
    /// This category is never reported by a native asynchronous or synchronous provider.
    Reentrant,
    /// Parameter or result conversion/shape error.
    Decode,
    /// Connection or transport failure.
    Connection,
    /// A dispatched standalone write did not produce an authoritative result.
    WriteOutcomeUnknown,
    /// Commit was dispatched but its result is unknown.
    CommitOutcomeUnknown,
    /// Requested column or ordinal is absent.
    Missing,
    /// A cardinality-one convenience found no row.
    NoRows,
    /// A cardinality-one convenience found more than one row in the first result set.
    TooManyRows,
    /// Name identifies more than one column.
    Ambiguous,
    /// A SQL null was requested as a non-null value.
    Null,
    /// Exact numeric conversion exceeds the destination range.
    Overflow,
    /// Supplied value has an incompatible type.
    WrongType,
}
/// Sanitized database error with a stable category and no source payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DbError {
    kind: ErrorKind,
    reason: Option<InvalidArgumentReason>,
}
/// Static, safe explanation for invalid caller-supplied arguments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidArgumentReason {
    /// Parameter's value variant did not match its declared type.
    ParameterTypeMismatch,
    /// Floating-point value was NaN or infinite.
    NonFiniteFloat,
    /// Fetch row limit was zero.
    ZeroFetchRowLimit,
    /// Fetch byte limit was zero.
    ZeroByteLimit,
    /// Fetch result-set limit was zero.
    ZeroSetLimit,
}
impl DbError {
    /// Creates an error containing only its stable category.
    #[must_use]
    pub const fn new(kind: ErrorKind) -> Self {
        Self { kind, reason: None }
    }
    /// Creates an invalid-argument error with a static, non-sensitive reason.
    #[must_use]
    pub const fn invalid(reason: InvalidArgumentReason) -> Self {
        Self {
            kind: ErrorKind::InvalidArgument,
            reason: Some(reason),
        }
    }
    /// Returns the safe static invalid-argument reason, if present.
    #[must_use]
    pub const fn reason(self) -> Option<InvalidArgumentReason> {
        self.reason
    }
    /// Returns the stable error category.
    #[must_use]
    pub const fn kind(self) -> ErrorKind {
        self.kind
    }
}
impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason {
            Some(reason) => write!(f, "invalid database argument ({reason:?})"),
            None => write!(f, "database operation failed ({:?})", self.kind),
        }
    }
}
impl Error for DbError {}

/// A detached row of owned values in ordinal order with provider-supplied names.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    columns: Vec<(String, Value)>,
}
impl Row {
    /// Creates a row from owned name/value pairs; duplicate names remain representable but name lookup is ambiguous.
    ///
    /// # Errors
    /// Returns `InvalidArgument` with a static reason if a cell contains a non-finite float.
    pub fn new(columns: Vec<(String, Value)>) -> Result<Self, DbError> {
        for (_, value) in &columns {
            value.validate()?;
        }
        Ok(Self { columns })
    }
    /// Returns the number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }
    /// Returns whether the row has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
    /// Returns a cell by zero-based ordinal, distinguishing missing columns and null values.
    ///
    /// # Errors
    /// Returns `Missing`, `Null`, `WrongType`, or `Overflow` as appropriate.
    pub fn get_ordinal<T: FromValue>(&self, ordinal: usize) -> Result<T, DbError> {
        let (_, value) = self
            .columns
            .get(ordinal)
            .ok_or_else(|| DbError::new(ErrorKind::Missing))?;
        T::from_value(value)
    }
    /// Returns a cell by exact name; missing and duplicate names are decode errors.
    ///
    /// # Errors
    /// Returns `Missing`, `Ambiguous`, `Null`, `WrongType`, or `Overflow` as appropriate.
    pub fn get<T: FromValue>(&self, name: &str) -> Result<T, DbError> {
        let mut found = self.columns.iter().filter(|(column, _)| column == name);
        let (_, value) = found
            .next()
            .ok_or_else(|| DbError::new(ErrorKind::Missing))?;
        if found.next().is_some() {
            return Err(DbError::new(ErrorKind::Ambiguous));
        }
        T::from_value(value)
    }
    /// Reads a cell by exact name with the same exact typing rules as [`Row::get`].
    ///
    /// # Errors
    /// Returns `Missing`, `Ambiguous`, `Null`, `WrongType`, or `Overflow` as appropriate.
    pub fn read<T: FromValue>(&self, name: &str) -> Result<T, DbError> {
        self.get(name)
    }
    /// Reads a cell by zero-based ordinal with the same exact typing rules as
    /// [`Row::get_ordinal`].
    ///
    /// # Errors
    /// Returns `Missing`, `Null`, `WrongType`, or `Overflow` as appropriate.
    pub fn read_at<T: FromValue>(&self, ordinal: usize) -> Result<T, DbError> {
        self.get_ordinal(ordinal)
    }
    /// Returns a cloned cell by zero-based ordinal, without typed conversion.
    ///
    /// # Errors
    /// Returns `Missing` when the ordinal is absent.
    pub fn value_at(&self, ordinal: usize) -> Result<Value, DbError> {
        self.columns
            .get(ordinal)
            .map(|(_, value)| value.clone())
            .ok_or_else(|| DbError::new(ErrorKind::Missing))
    }
    /// Returns the row's portable byte cost: column-name UTF-8 bytes plus value payload bytes.
    ///
    /// Payload accounting is `Null = 0`, `Bool = 1`, `I64`/`F64 = 8`, and text/bytes length.
    /// Providers and buffering use the same rule so a limit means the same thing at every layer.
    ///
    /// # Errors
    /// Returns `Overflow` if the aggregate cannot be represented in `usize`.
    pub fn encoded_bytes(&self) -> Result<usize, DbError> {
        self.columns
            .iter()
            .try_fold(0usize, |total, (name, value)| {
                let payload = match value {
                    Value::Null(_) => 0,
                    Value::Bool(_) => 1,
                    Value::I64(_) | Value::F64(_) => 8,
                    Value::Text(value) => value.len(),
                    Value::Bytes(value) => value.len(),
                };
                total
                    .checked_add(name.len())
                    .and_then(|total| total.checked_add(payload))
                    .ok_or_else(|| DbError::new(ErrorKind::Overflow))
            })
    }
    /// Reads a non-null value by exact name.
    ///
    /// # Errors
    /// Returns `Missing`, `Ambiguous`, `Null`, `WrongType`, or `Overflow`.
    pub fn require<T: FromValue>(&self, name: &str) -> Result<T, DbError> {
        let mut found = self.columns.iter().filter(|(column, _)| column == name);
        let (_, value) = found
            .next()
            .ok_or_else(|| DbError::new(ErrorKind::Missing))?;
        if found.next().is_some() {
            return Err(DbError::new(ErrorKind::Ambiguous));
        }
        if let Value::Null(actual_type) = value {
            return Err(DbError::new(if *actual_type == T::DB_TYPE {
                ErrorKind::Null
            } else {
                ErrorKind::WrongType
            }));
        }
        T::from_value(value)
    }
    /// Reads a non-null value by ordinal.
    ///
    /// # Errors
    /// Returns `Missing`, `Null`, `WrongType`, or `Overflow`.
    pub fn require_ordinal<T: FromValue>(&self, ordinal: usize) -> Result<T, DbError> {
        let (_, value) = self
            .columns
            .get(ordinal)
            .ok_or_else(|| DbError::new(ErrorKind::Missing))?;
        if let Value::Null(actual_type) = value {
            return Err(DbError::new(if *actual_type == T::DB_TYPE {
                ErrorKind::Null
            } else {
                ErrorKind::WrongType
            }));
        }
        T::from_value(value)
    }
}
/// Exact typed conversion from a row value; `Option<T>` represents a matching typed SQL null.
pub trait FromValue: Sized {
    /// SQL type accepted by this decoder.
    const DB_TYPE: DbType;
    /// Converts without lossy coercion.
    ///
    /// # Errors
    /// For non-nullable targets, returns `Null` for a matching typed SQL null, `WrongType` for
    /// incompatible types/null types, or `Overflow`.
    fn from_value(value: &Value) -> Result<Self, DbError>;
}
macro_rules! exact_from {
    ($ty:ty, $variant:ident, $db:ident) => {
        impl FromValue for $ty {
            const DB_TYPE: DbType = DbType::$db;
            fn from_value(value: &Value) -> Result<Self, DbError> {
                match value {
                    Value::Null(DbType::$db) => Err(DbError::new(ErrorKind::Null)),
                    Value::Null(_) => Err(DbError::new(ErrorKind::WrongType)),
                    Value::$variant(value) => Ok(value.clone()),
                    _ => Err(DbError::new(ErrorKind::WrongType)),
                }
            }
        }
    };
}
impl<T: FromValue> FromValue for Option<T> {
    const DB_TYPE: DbType = T::DB_TYPE;
    fn from_value(value: &Value) -> Result<Self, DbError> {
        match value {
            Value::Null(db_type) if *db_type == T::DB_TYPE => Ok(None),
            Value::Null(_) => Err(DbError::new(ErrorKind::WrongType)),
            non_null => T::from_value(non_null).map(Some),
        }
    }
}
exact_from!(bool, Bool, Bool);
exact_from!(i64, I64, I64);
exact_from!(f64, F64, F64);
exact_from!(String, Text, Text);
exact_from!(Vec<u8>, Bytes, Bytes);
/// Exact conversion from an SQL integer to a 32-bit integer.
impl FromValue for i32 {
    const DB_TYPE: DbType = DbType::I64;
    fn from_value(value: &Value) -> Result<Self, DbError> {
        match value {
            Value::Null(DbType::I64) => Err(DbError::new(ErrorKind::Null)),
            Value::I64(value) => {
                i32::try_from(*value).map_err(|_| DbError::new(ErrorKind::Overflow))
            }
            _ => Err(DbError::new(ErrorKind::WrongType)),
        }
    }
}

/// A statement's bounded result sets, consumed row by row.
///
/// A reader is not proof of a read: statements such as `INSERT ... RETURNING` can write.
/// `next_row_async` yields rows from the current result set and returns `None` at the end of that set.
/// `has_next_set_async` advances: it discards unread rows in the current set and reports whether another
/// set exists. `finish_async` drains and discards every remaining row and set, surfacing any later
/// statement error. A provider with unread results attached must discard its connection if an
/// unfinished reader is dropped. A provider that materializes the bounded batch before returning
/// may already have made the connection reusable and has nothing outstanding to discard.
///
/// `RowLimit` is a bound for the whole batch. A provider that supports multiple result sets counts
/// rows, bytes, and sets across the batch, including empty sets; a single-set provider may report
/// no next set instead of inventing a boundary it cannot observe. Buffering wrappers enforce the
/// same bounds while preserving boundaries.
pub trait Reader: Send {
    /// Returns the next row in the current result set, or `None` at the set's end.
    ///
    /// # Errors
    /// Returns a sanitized provider error. After an error the reader is finished; further calls
    /// return `None` or the same error. A provider with unread results attached must discard its
    /// connection when the reader is dropped.
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>>;

    /// Advances to the next result set, discarding any unread rows in the current one.
    ///
    /// Returns `true` when a next set is current, or `false` at the end of the batch. A single-set
    /// provider returns `false`; it must not fabricate result boundaries.
    ///
    /// # Errors
    /// Returns a sanitized provider error while draining or advancing.
    fn has_next_set_async(&mut self) -> DbFuture<'_, bool>;

    /// Drains and discards every remaining row and result set.
    ///
    /// The reader is finished afterwards. A provider with unread results must give dropping the
    /// reader the same cleanup effect on the connection, but dropping cannot report a later
    /// statement error.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while draining.
    fn finish_async(&mut self) -> DbFuture<'_, ()>;

    /// Drains every result set and returns the owned rows, flattening set boundaries.
    ///
    /// This is a convenience for callers that want the whole bounded batch. It does not add a
    /// limit of its own; providers apply the `RowLimit` supplied to `fetch_async`. Use
    /// [`Reader::read_sets_async`] when boundaries matter.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_all_async(&mut self) -> DbFuture<'_, Vec<Row>> {
        Box::pin(async move {
            Ok(self
                .read_sets_async()
                .await?
                .into_iter()
                .flatten()
                .collect())
        })
    }

    /// Drains every result set and returns the rows grouped by set, including empty sets.
    ///
    /// This is a convenience for callers that want the whole bounded batch without losing
    /// boundaries. It does not add a limit of its own; providers apply the `RowLimit` supplied to
    /// `fetch_async`.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_sets_async(&mut self) -> DbFuture<'_, Vec<Vec<Row>>> {
        Box::pin(async move {
            let mut sets = Vec::new();
            loop {
                let mut set = Vec::new();
                while let Some(row) = self.next_row_async().await? {
                    set.push(row);
                }
                sets.push(set);
                if !self.has_next_set_async().await? {
                    break;
                }
            }
            self.finish_async().await?;
            Ok(sets)
        })
    }
}

/// A reader over an already-materialized batch, preserving result-set boundaries.
///
/// Providers and wrappers that materialize the bounded batch before returning use this. It has no
/// unread wire results, so `finish_async` and `has_next_set_async` only discard the in-memory rows. Empty sets
/// remain representable and countable.
#[derive(Debug)]
pub struct BufferedReader {
    sets: VecDeque<VecDeque<Row>>,
    done: bool,
}
impl BufferedReader {
    /// Wraps owned rows as one result set.
    #[must_use]
    pub fn new(rows: Vec<Row>) -> Self {
        Self::from_sets(vec![rows])
    }
    /// Wraps owned rows grouped by result set, including empty sets.
    #[must_use]
    pub fn from_sets(sets: Vec<Vec<Row>>) -> Self {
        Self {
            sets: sets.into_iter().map(VecDeque::from).collect(),
            done: false,
        }
    }
}
impl Reader for BufferedReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        let row = if self.done {
            None
        } else {
            self.sets
                .front_mut()
                .and_then(std::collections::VecDeque::pop_front)
        };
        Box::pin(async move { Ok(row) })
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        if self.done {
            return Box::pin(async { Ok(false) });
        }
        self.sets.pop_front();
        let more = !self.sets.is_empty();
        if !more {
            self.done = true;
        }
        Box::pin(async move { Ok(more) })
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.sets.clear();
        self.done = true;
        Box::pin(async { Ok(()) })
    }
}

fn row_cardinality_error(more_than_one: bool) -> DbError {
    DbError::new(if more_than_one {
        ErrorKind::TooManyRows
    } else {
        ErrorKind::NoRows
    })
}

/// A pool able to acquire exclusive sessions and report truthful capabilities.
pub trait Pool: Send + Sync {
    /// Returns capabilities for this provider/configuration.
    fn capabilities(&self) -> Capabilities;
    /// Returns defaults used for options inherited by this pool.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }
    /// Acquires one exclusive session; the caller must drop it to release it.
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>>;
    /// Executes one statement through a newly acquired session with per-query policy overrides.
    /// Overrides must reject an explicit fetch limit before dispatch.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_async<'a>(&'a self, sql: &'a str, mut options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            reject_unhandled_pool_options(&options)?;
            resolve_provider_timeout(&mut options, self.defaults().timeout)?;
            let mut session = self.acquire_async().await?;
            session.execute_async(sql, options).await
        })
    }
    /// Fetches one bounded batch through a newly acquired session with per-query overrides.
    ///
    /// The default implementation drains the batch and returns a reader over owned rows.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_async<'a>(
        &'a self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            reject_unhandled_pool_options(&options)?;
            let limit = options.resolved_limit(self.defaults());
            options.limit = LimitSetting::Set(limit);
            resolve_provider_timeout(&mut options, self.defaults().timeout)?;
            let mut session = self.acquire_async().await?;
            let mut reader = session.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(Box::new(BufferedReader::from_sets(sets)) as Box<dyn Reader + 'a>)
        })
    }
    /// Fetches and drains the whole bounded batch using default options.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        Box::pin(async move {
            let limit = options.resolved_limit(self.defaults());
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }
    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }
    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// Remaining rows and later result sets are drained before returning. A later error wins over
    /// the cardinality result.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(row_cardinality_error(false)),
                Some(_) if more => Err(row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }
    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }
}
/// An exclusively borrowed database session.
pub trait Session: Send {
    /// Defaults inherited by operations on this session.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }
    /// Executes one positional-parameter statement; parameters follow placeholder order, with no SQL rewriting.
    /// Providers must reject an explicit fetch limit before dispatch, e.g. via
    /// [`QueryOptions::prepare_execute_provider`]. An inherited limit is permitted.
    /// Providers reject placeholder/parameter count mismatches. SQLite uses `?`; PostgreSQL uses `$1`, `$2`.
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64>;
    /// Fetches one bounded batch as a reader.
    ///
    /// The reader may borrow this session until it is finished or dropped; a provider that
    /// materializes the batch may release the session earlier. The limit applies to the whole
    /// batch; providers that support multiple result sets count rows and bytes across sets.
    /// Parameters are positional and ordered; providers reject count mismatches and do not rewrite
    /// SQL. Fetching does not imply read-only SQL: `INSERT ... RETURNING` can write. Providers must
    /// report uncertain write results as `WriteOutcomeUnknown`, not retryable transport errors.
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>>;
    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        let limit = options.resolved_limit(self.defaults());
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }
    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }
    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// Remaining rows and later result sets are drained before returning. A later error wins over
    /// the cardinality result.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(row_cardinality_error(false)),
                Some(_) if more => Err(row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }
    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }
    /// Executes a multi-statement unparameterized script. Input must be trusted; scripts must not manage transactions.
    /// An interrupted operation requires provider cleanup before session reuse.
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()>;
    /// Begins an interactive transaction on this same session; unsupported requested options fail before dispatch.
    /// If dropped after begin dispatch, the provider must roll back or discard the connection before reuse.
    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>>;
}
/// A transaction that exclusively borrows its session until consumed or dropped.
///
/// Dropping without commit or rollback, or interrupting a begin/commit/rollback future, must not leave
/// a connection reusable while a transaction may remain open. The provider must complete rollback
/// or discard that connection before allowing reuse. Dropping a Rust value alone is not proof of
/// rollback. A dispatched commit with unconfirmed success is an unknown outcome, not a rollback.
pub trait Transaction<'session>: Send {
    /// Defaults inherited by operations on this transaction.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }
    /// Executes one statement with positional parameters in slice order, without SQL rewriting.
    /// Providers must reject an explicit fetch limit before dispatch, e.g. via
    /// [`QueryOptions::prepare_execute_provider`]. An inherited limit is permitted.
    /// Providers reject placeholder/parameter count mismatches.
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64>;
    /// Fetches one bounded batch as a reader.
    ///
    /// The reader may borrow this transaction until it is finished or dropped; a provider that
    /// materializes the batch may release it earlier. The limit applies to the whole batch;
    /// providers that support multiple result sets count rows and bytes across sets. Parameters are
    /// positional and ordered.
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>>;
    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        let limit = options.resolved_limit(self.defaults());
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }
    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }
    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// Remaining rows and later result sets are drained before returning. A later error wins over
    /// the cardinality result.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(row_cardinality_error(false)),
                Some(_) if more => Err(row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }
    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }
    /// Executes a trusted multi-statement script that must not manage transactions. Interrupted work
    /// requires provider cleanup before transaction reuse.
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()>;
    /// Commits and consumes this transaction. Before reuse, the provider must establish transaction
    /// closure and cleanup or discard the connection, including if this future is interrupted. If
    /// commit was dispatched but success is unknown, return `CommitOutcomeUnknown`; this does not
    /// imply rollback.
    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()>;
    /// Rolls back and consumes this transaction. The provider must finish rollback or discard the
    /// connection before reuse, including if this future is interrupted; it must never return a
    /// possibly active connection.
    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()>;
}
/// Optional provider extension for SQLite-style immediate transaction starts without leaking driver types.
pub trait ImmediateTransaction: Session {
    /// Begins an immediate transaction on the same session or returns `Unsupported` before beginning.
    fn begin_immediate_async<'a>(&'a mut self) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>>;
}

/// SQLite-specific pool capabilities shared by SQLite consumers without exposing a driver.
///
/// Implementations must apply `busy_timeout` to each acquired connection before returning the
/// immediate-transaction-capable session.
pub trait SqlitePool: Pool {
    /// Returns the configured SQLite busy timeout.
    fn busy_timeout(&self) -> Duration;

    /// Acquires a session capable of starting `BEGIN IMMEDIATE`.
    fn acquire_sqlite_async(&self) -> DbFuture<'_, Box<dyn ImmediateTransaction>>;
}

/// Policy and telemetry decorator for pool, session, transaction, and script operations.
///
/// Pool methods `execute_async`/`fetch_async` acquire a fresh session for each configured retry attempt.
/// Session, transaction, and trusted-script methods do not retry.
/// The total-budget timer is constructed once before acquisition and checked again before each
/// statement dispatch, including after every synchronous hook in nested policy session wrappers.
/// A pre-dispatch rejection remains `Cancelled`/`Timeout` and emits no statement events.
/// Already-dispatched success wins simultaneous cancellation/deadline.
/// Other interruptions during dispatch are `WriteOutcomeUnknown`, even for fetches; authoritative
/// provider unknown-outcome errors are never replaced by cancellation/deadline or retried.
/// Dropping an in-flight observed operation emits one `WriteOutcomeUnknown` event because dispatch
/// completion cannot be established.
/// Statement events cover execution only; acquisition and transaction begin/commit/rollback
/// boundaries are not separately observed.
pub struct PolicyPool {
    inner: Arc<dyn Pool>,
    policy: Arc<PoolPolicy>,
}
impl fmt::Debug for PolicyPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolicyPool")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}
impl PolicyPool {
    /// Wraps a provider and applies the supplied defaults to operations made through this wrapper.
    pub fn new<P: Pool + 'static>(pool: P, policy: PoolPolicy) -> Self {
        Self::from_shared(Arc::new(pool), policy)
    }

    /// Wraps a shared provider trait object with per-pool policy.
    #[must_use]
    pub fn from_shared(pool: Arc<dyn Pool>, mut policy: PoolPolicy) -> Self {
        if policy.default_timeout.is_none() {
            policy.default_timeout = pool.defaults().timeout;
        }
        Self {
            inner: pool,
            policy: Arc::new(policy),
        }
    }
}

impl Pool for PolicyPool {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn defaults(&self) -> PoolDefaults {
        policy_defaults(&self.policy)
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let inner = Arc::clone(&self.inner);
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let session = inner.acquire_async().await?;
            Ok(Box::new(PolicySession { session, policy }) as Box<dyn Session>)
        })
    }

    fn execute_async<'a>(&'a self, sql: &'a str, mut options: QueryOptions) -> DbFuture<'a, u64> {
        let pool = Arc::clone(&self.inner);
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            options.validate_execute()?;
            resolve_provider_timeout(&mut options, policy.default_timeout)?;
            let retry = resolve_retry(options.retry, policy.retry);
            if options.replay_safety == ReplaySafety::ExplicitlyReplaySafe
                && let RetryPolicy::Enabled {
                    policy: retry_policy,
                    retry_timeouts,
                } = retry
            {
                let Some(mut runtime) = options
                    .retry_runtime
                    .clone()
                    .or_else(|| policy.retry_runtime.clone())
                else {
                    return Err(DbError::new(ErrorKind::Unsupported));
                };
                let dispatch = RetryDispatch::for_operation(&mut runtime);
                let cancellation = Arc::clone(&dispatch.cancellation);
                let deadline = dispatch.deadline.clone();
                let attempt_state = Arc::clone(&dispatch.state);
                let mut options = options;
                options.retry_runtime = Some(runtime.clone());
                let retry_if = |error: &DbError| {
                    retry_allowed(*error, retry_timeouts, &cancellation, &deadline)
                };
                let outcome = execute_idempotent_retry(
                    retry_policy,
                    &cancellation,
                    deadline.clone(),
                    || {
                        let pool = Arc::clone(&pool);
                        let policy = Arc::clone(&policy);
                        let cancellation = Arc::clone(&cancellation);
                        let deadline = deadline.clone();
                        let attempt_state = Arc::clone(&attempt_state);
                        let options = options.clone();
                        async move {
                            attempt_state.reset();
                            deadline.check(&cancellation).await?;
                            let mut session = pool.acquire_async().await?;
                            let result = execute_observed(
                                session.as_mut(),
                                sql,
                                options,
                                Arc::clone(&policy),
                            )
                            .await;
                            drop(session);
                            result
                        }
                    },
                    retry_if,
                    || (runtime.random)(),
                    |delay| (runtime.sleep)(delay),
                )
                .await;
                drop(dispatch);
                return retry_result_with_interrupt(outcome, &attempt_state);
            }

            let mut session = pool.acquire_async().await?;
            let result = execute_observed(session.as_mut(), sql, options, policy).await;
            drop(session);
            result
        })
    }

    fn fetch_async<'a>(
        &'a self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let pool = Arc::clone(&self.inner);
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let limit = match options.limit {
                LimitSetting::Inherit => policy.default_limit,
                LimitSetting::Set(limit) => limit,
            };
            options.limit = LimitSetting::Set(limit);
            resolve_provider_timeout(&mut options, policy.default_timeout)?;
            let retry = resolve_retry(options.retry, policy.retry);
            if options.replay_safety == ReplaySafety::ExplicitlyReplaySafe
                && let RetryPolicy::Enabled {
                    policy: retry_policy,
                    retry_timeouts,
                } = retry
            {
                let Some(mut runtime) = options
                    .retry_runtime
                    .clone()
                    .or_else(|| policy.retry_runtime.clone())
                else {
                    return Err(DbError::new(ErrorKind::Unsupported));
                };
                let dispatch = RetryDispatch::for_operation(&mut runtime);
                let cancellation = Arc::clone(&dispatch.cancellation);
                let deadline = dispatch.deadline.clone();
                let attempt_state = Arc::clone(&dispatch.state);
                let mut options = options;
                options.retry_runtime = Some(runtime.clone());
                let retry_if = |error: &DbError| {
                    retry_allowed(*error, retry_timeouts, &cancellation, &deadline)
                };
                let outcome = execute_idempotent_retry(
                    retry_policy,
                    &cancellation,
                    deadline.clone(),
                    || {
                        let pool = Arc::clone(&pool);
                        let policy = Arc::clone(&policy);
                        let cancellation = Arc::clone(&cancellation);
                        let deadline = deadline.clone();
                        let attempt_state = Arc::clone(&attempt_state);
                        let options = options.clone();
                        async move {
                            attempt_state.reset();
                            deadline.check(&cancellation).await?;
                            let mut session = pool.acquire_async().await?;
                            let result = fetch_observed(
                                session.as_mut(),
                                sql,
                                limit,
                                options,
                                Arc::clone(&policy),
                            )
                            .await;
                            drop(session);
                            result
                        }
                    },
                    retry_if,
                    || (runtime.random)(),
                    |delay| (runtime.sleep)(delay),
                )
                .await;
                drop(dispatch);
                return retry_result_with_interrupt(outcome, &attempt_state)
                    .map(|reader| Box::new(reader) as Box<dyn Reader + 'a>);
            }

            let mut session = pool.acquire_async().await?;
            let result = fetch_observed(session.as_mut(), sql, limit, options, policy).await;
            drop(session);
            result.map(|reader| Box::new(reader) as Box<dyn Reader + 'a>)
        })
    }
}

fn resolve_retry(setting: RetrySetting, default: RetryPolicy) -> RetryPolicy {
    match setting {
        RetrySetting::Inherit => default,
        RetrySetting::Off => RetryPolicy::Disabled,
        RetrySetting::Use(policy) => policy,
    }
}

fn retry_allowed(
    error: DbError,
    retry_timeouts: bool,
    cancellation: &Cancellation,
    deadline: &RetryDeadline,
) -> bool {
    !cancellation.is_cancelled()
        && !deadline.is_expired()
        && (matches!(error.kind(), ErrorKind::Busy | ErrorKind::Connection)
            || (retry_timeouts && error.kind() == ErrorKind::Timeout))
}

fn retry_result<T>(outcome: RetryOutcome<T, DbError>) -> Result<T, DbError> {
    match outcome {
        RetryOutcome::Success { value, .. } => Ok(value),
        RetryOutcome::Failure { error, .. } => Err(error),
        RetryOutcome::Cancelled { .. } => Err(DbError::new(ErrorKind::Cancelled)),
        RetryOutcome::TimedOut { .. } => Err(DbError::new(ErrorKind::Timeout)),
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct RetryAttemptState {
    dispatch_in_flight: AtomicBool,
    last_error: Mutex<Option<ErrorKind>>,
    dispatch_rejection: Mutex<Option<DbError>>,
}

impl RetryAttemptState {
    fn reset(&self) {
        self.dispatch_in_flight.store(false, Ordering::Release);
        *mutex_lock(&self.last_error) = None;
        *mutex_lock(&self.dispatch_rejection) = None;
    }
}

// One pinned total-budget timer, shared by the resilience race and the SQL dispatch gate.
// Latch completion so neither consumer ever polls a completed timer again.
#[derive(Clone)]
struct RetryDeadline(Arc<Mutex<Option<RetryWait>>>);

impl RetryDeadline {
    fn new(timer: RetryWait) -> Self {
        Self(Arc::new(Mutex::new(Some(timer))))
    }

    fn is_expired(&self) -> bool {
        mutex_lock(&self.0).is_none()
    }

    async fn check(&self, cancellation: &Cancellation) -> Result<(), DbError> {
        if cancellation.is_cancelled() {
            return Err(DbError::new(ErrorKind::Cancelled));
        }
        let mut deadline = self.clone();
        let expired = poll_fn(|cx| Poll::Ready(Pin::new(&mut deadline).poll(cx).is_ready())).await;
        // Polling the caller's timer can itself make cancellation ready.
        if cancellation.is_cancelled() {
            Err(DbError::new(ErrorKind::Cancelled))
        } else if expired {
            Err(DbError::new(ErrorKind::Timeout))
        } else {
            Ok(())
        }
    }
}

impl Future for RetryDeadline {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut timer = mutex_lock(&self.0);
        if let Some(wait) = timer.as_mut() {
            if wait.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            *timer = None;
        }
        Poll::Ready(())
    }
}

struct RetryDispatch {
    deadline: RetryDeadline,
    cancellation: Arc<Cancellation>,
    state: Arc<RetryAttemptState>,
}

impl RetryDispatch {
    fn for_operation(runtime: &mut RetryRuntime) -> Arc<Self> {
        let dispatch = Arc::new(Self {
            // Construct the only timer before the first acquisition, not lazily inside a layer.
            deadline: RetryDeadline::new((runtime.timer)()),
            cancellation: Arc::clone(&runtime.cancellation),
            state: Arc::new(RetryAttemptState::default()),
        });
        runtime.dispatch = Some(Arc::downgrade(&dispatch));
        dispatch
    }

    async fn start(&self) -> Result<(), DbError> {
        // Acquisition may become ready in the engine's success probe. Check the actual timer,
        // not an expired flag that depends on the engine having polled its timer first.
        // A deeper policy layer is still preparing SQL, so no provider has been invoked yet.
        self.state
            .dispatch_in_flight
            .store(false, Ordering::Release);
        if let Err(error) = self.deadline.check(&self.cancellation).await {
            *mutex_lock(&self.state.dispatch_rejection) = Some(error);
            return Err(error);
        }
        self.state.dispatch_in_flight.store(true, Ordering::Release);
        Ok(())
    }

    fn rejection(&self) -> Option<DbError> {
        *mutex_lock(&self.state.dispatch_rejection)
    }

    fn finish(&self, result: Result<(), DbError>) {
        self.state
            .dispatch_in_flight
            .store(false, Ordering::Release);
        if let Err(error) = result {
            *mutex_lock(&self.state.last_error) = Some(error.kind());
        }
    }
}

fn retry_result_with_interrupt<T>(
    outcome: RetryOutcome<T, DbError>,
    state: &RetryAttemptState,
) -> Result<T, DbError> {
    match outcome {
        RetryOutcome::Cancelled { .. } => Err(interruption_error(state, ErrorKind::Cancelled)),
        RetryOutcome::TimedOut { .. } => Err(interruption_error(state, ErrorKind::Timeout)),
        other => retry_result(other),
    }
}

fn interruption_error(state: &RetryAttemptState, interruption: ErrorKind) -> DbError {
    // Fetch is not proof of a read: INSERT ... RETURNING can have changed the database.
    if state.dispatch_in_flight.load(Ordering::Acquire) {
        return DbError::new(ErrorKind::WriteOutcomeUnknown);
    }
    let last_error = *mutex_lock(&state.last_error);
    match last_error {
        Some(kind @ (ErrorKind::WriteOutcomeUnknown | ErrorKind::CommitOutcomeUnknown)) => {
            DbError::new(kind)
        }
        _ => DbError::new(interruption),
    }
}

/// Policy decorator that retains a pool's SQLite immediate-transaction capability.
///
/// It shares the observer, capture, timeout, and pool-operation retry implementation of
/// [`PolicyPool`]. Acquired SQLite sessions use the same observer helpers but intentionally do not
/// retry, matching ordinary policy sessions and transactions.
pub struct SqlitePolicyPool {
    inner: Arc<dyn SqlitePool>,
    policy_pool: PolicyPool,
}

impl fmt::Debug for SqlitePolicyPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlitePolicyPool")
            .field("policy", &self.policy_pool.policy)
            .finish_non_exhaustive()
    }
}

impl SqlitePolicyPool {
    /// Wraps a SQLite provider and preserves its immediate-transaction extension.
    pub fn new<P: SqlitePool + 'static>(pool: P, policy: PoolPolicy) -> Self {
        Self::from_shared(Arc::new(pool), policy)
    }

    /// Wraps a shared SQLite provider while retaining one underlying pool for both APIs.
    #[must_use]
    pub fn from_shared(inner: Arc<dyn SqlitePool>, policy: PoolPolicy) -> Self {
        let pool: Arc<dyn Pool> = inner.clone();
        Self {
            inner,
            policy_pool: PolicyPool::from_shared(pool, policy),
        }
    }
}

impl Pool for SqlitePolicyPool {
    fn capabilities(&self) -> Capabilities {
        self.policy_pool.capabilities()
    }

    fn defaults(&self) -> PoolDefaults {
        self.policy_pool.defaults()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        self.policy_pool.acquire_async()
    }

    fn execute_async<'a>(&'a self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        self.policy_pool.execute_async(sql, options)
    }

    fn fetch_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.policy_pool.fetch_async(sql, options)
    }
}

impl SqlitePool for SqlitePolicyPool {
    fn busy_timeout(&self) -> Duration {
        self.inner.busy_timeout()
    }

    fn acquire_sqlite_async(&self) -> DbFuture<'_, Box<dyn ImmediateTransaction>> {
        let inner = Arc::clone(&self.inner);
        let policy = Arc::clone(&self.policy_pool.policy);
        Box::pin(async move {
            let session = inner.acquire_sqlite_async().await?;
            Ok(Box::new(PolicySqliteSession { session, policy }) as Box<dyn ImmediateTransaction>)
        })
    }
}

struct PolicySession {
    session: Box<dyn Session>,
    policy: Arc<PoolPolicy>,
}
impl Session for PolicySession {
    fn defaults(&self) -> PoolDefaults {
        policy_defaults(&self.policy)
    }

    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            execute_observed(
                self.session.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            reject_session_retry(&options)?;
            let limit = options.resolved_limit(policy_defaults(&self.policy));
            options.limit = LimitSetting::Set(limit);
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            let reader = fetch_observed(
                self.session.as_mut(),
                sql,
                limit,
                options,
                Arc::clone(&self.policy),
            )
            .await?;
            Ok(Box::new(reader) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.validate_script()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            script_observed(
                self.session.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let session = &mut self.session;
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let transaction = session.begin_async(options).await?;
            Ok(Box::new(PolicyTransaction {
                transaction,
                policy,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

struct PolicySqliteSession {
    session: Box<dyn ImmediateTransaction>,
    policy: Arc<PoolPolicy>,
}

impl Session for PolicySqliteSession {
    fn defaults(&self) -> PoolDefaults {
        policy_defaults(&self.policy)
    }

    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            execute_observed(
                self.session.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            reject_session_retry(&options)?;
            let limit = options.resolved_limit(policy_defaults(&self.policy));
            options.limit = LimitSetting::Set(limit);
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            let reader = fetch_observed(
                self.session.as_mut(),
                sql,
                limit,
                options,
                Arc::clone(&self.policy),
            )
            .await?;
            Ok(Box::new(reader) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.validate_script()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            script_observed(
                self.session.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let session = &mut self.session;
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let transaction = session.begin_async(options).await?;
            Ok(Box::new(PolicyTransaction {
                transaction,
                policy,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

impl ImmediateTransaction for PolicySqliteSession {
    fn begin_immediate_async<'a>(&'a mut self) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let session = &mut self.session;
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let transaction = session.begin_immediate_async().await?;
            Ok(Box::new(PolicyTransaction {
                transaction,
                policy,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

struct PolicyTransaction<'session> {
    transaction: Box<dyn Transaction<'session> + 'session>,
    policy: Arc<PoolPolicy>,
}
impl<'session> Transaction<'session> for PolicyTransaction<'session> {
    fn defaults(&self) -> PoolDefaults {
        policy_defaults(&self.policy)
    }

    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            execute_observed(
                self.transaction.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            reject_session_retry(&options)?;
            let limit = options.resolved_limit(policy_defaults(&self.policy));
            options.limit = LimitSetting::Set(limit);
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            let reader = fetch_observed(
                self.transaction.as_mut(),
                sql,
                limit,
                options,
                Arc::clone(&self.policy),
            )
            .await?;
            Ok(Box::new(reader) as Box<dyn Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.validate_script()?;
            reject_session_retry(&options)?;
            resolve_provider_timeout(&mut options, self.policy.default_timeout)?;
            script_observed(
                self.transaction.as_mut(),
                sql,
                options,
                Arc::clone(&self.policy),
            )
            .await
        })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        let transaction = self.transaction;
        transaction.commit_async()
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        let transaction = self.transaction;
        transaction.rollback_async()
    }
}

fn resolve_setting(setting: QuerySetting, pool_default: bool) -> bool {
    match setting {
        QuerySetting::Inherit => pool_default,
        QuerySetting::Off => false,
        QuerySetting::On => true,
    }
}

fn captured_sql(sql: &str, policy: &PoolPolicy, options: &QueryOptions) -> Option<String> {
    if !resolve_setting(options.sql_capture, policy.sql_capture_enabled)
        || !resolve_setting(options.telemetry, policy.telemetry_enabled)
    {
        return None;
    }
    policy.observer.as_ref()?;
    let hook = options.sql_hook.as_ref().or(policy.sql_hook.as_ref());
    match hook {
        Some(hook) => {
            contain_callback_panic(catch_unwind(AssertUnwindSafe(|| hook(sql)))).flatten()
        }
        None => Some(sql.to_owned()),
    }
}

fn contain_callback_panic<T>(result: Result<T, Box<dyn std::any::Any + Send>>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(payload) => {
            // A panic payload may have a destructor that panics. Forget it so this callback
            // boundary cannot unwind while unwinding a post-operation observer/hook.
            std::mem::forget(payload);
            None
        }
    }
}

fn emit(
    operation: Operation,
    started: Instant,
    result: Result<(), DbError>,
    sql: Option<String>,
    policy: &PoolPolicy,
    options: &QueryOptions,
) {
    if !resolve_setting(options.telemetry, policy.telemetry_enabled) {
        return;
    }
    let Some(observer) = &policy.observer else {
        return;
    };
    let outcome = match result {
        Ok(()) => TelemetryOutcome::Success,
        Err(error) => TelemetryOutcome::Error(error.kind()),
    };
    let event = DbEvent {
        operation,
        duration: started.elapsed(),
        outcome,
        sql,
    };
    let _ = contain_callback_panic(catch_unwind(AssertUnwindSafe(|| observer(&event))));
}

struct ObservationGuard {
    operation: Operation,
    started: Instant,
    captured: Option<String>,
    policy: Arc<PoolPolicy>,
    options: QueryOptions,
    active: bool,
}

impl ObservationGuard {
    fn new(
        operation: Operation,
        started: Instant,
        captured: Option<String>,
        policy: Arc<PoolPolicy>,
        options: QueryOptions,
    ) -> Self {
        Self {
            operation,
            started,
            captured,
            policy,
            options,
            active: true,
        }
    }

    fn finish(&mut self, result: Result<(), DbError>) {
        if self.active {
            self.active = false;
            emit(
                self.operation,
                self.started,
                result,
                self.captured.take(),
                &self.policy,
                &self.options,
            );
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for ObservationGuard {
    fn drop(&mut self) {
        if self.active {
            self.finish(Err(DbError::new(ErrorKind::WriteOutcomeUnknown)));
        }
    }
}

trait StatementExecutor: Send {
    fn execute_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, u64>;
    fn fetch_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>>;
    fn execute_script<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, ()>;
}
impl StatementExecutor for dyn Session + '_ {
    fn execute_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        self.execute_async(sql, options)
    }
    fn fetch_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.fetch_async(sql, options)
    }
    fn execute_script<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, ()> {
        self.execute_trusted_script_async(sql, options)
    }
}
impl StatementExecutor for dyn ImmediateTransaction + '_ {
    fn execute_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        self.execute_async(sql, options)
    }
    fn fetch_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.fetch_async(sql, options)
    }
    fn execute_script<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, ()> {
        self.execute_trusted_script_async(sql, options)
    }
}
impl StatementExecutor for dyn Transaction<'_> + '_ {
    fn execute_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        self.execute_async(sql, options)
    }
    fn fetch_statement<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.fetch_async(sql, options)
    }
    fn execute_script<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, ()> {
        self.execute_trusted_script_async(sql, options)
    }
}

fn script_observed<'a, S: StatementExecutor + ?Sized>(
    session: &'a mut S,
    sql: &'a str,
    query_options: QueryOptions,
    policy: Arc<PoolPolicy>,
) -> DbFuture<'a, ()> {
    Box::pin(async move {
        let captured = captured_sql(sql, &policy, &query_options);
        let started = Instant::now();
        let mut observation = ObservationGuard::new(
            Operation::Execute,
            started,
            captured,
            Arc::clone(&policy),
            query_options.clone(),
        );
        let mut provider_options = query_options.clone();
        provider_options.policy_applied = true;
        let result = session.execute_script(sql, provider_options).await;
        let observer_result = match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(DbError::new(error.kind())),
        };
        observation.finish(observer_result);
        result
    })
}

fn operation_dispatch(options: &QueryOptions) -> Option<Arc<RetryDispatch>> {
    options.retry_runtime.as_ref()?.dispatch.as_ref()?.upgrade()
}

fn execute_observed<'a, S: StatementExecutor + ?Sized>(
    session: &'a mut S,
    sql: &'a str,
    options: QueryOptions,
    policy: Arc<PoolPolicy>,
) -> DbFuture<'a, u64> {
    Box::pin(async move {
        let dispatch = operation_dispatch(&options);
        let captured = captured_sql(sql, &policy, &options);
        if let Some(dispatch) = &dispatch {
            dispatch.start().await?;
        }
        let started = Instant::now();
        let mut observation = ObservationGuard::new(
            Operation::Execute,
            started,
            captured,
            Arc::clone(&policy),
            options.clone(),
        );
        let mut provider_options = options.clone();
        provider_options.policy_applied = true;
        let result = session.execute_statement(sql, provider_options).await;
        // An inner hook can stop this operation before reaching the provider. Preserve that
        // rejection through every outer wrapper: it is neither a write nor a statement event.
        if let Some(error) = dispatch.as_ref().and_then(|dispatch| dispatch.rejection()) {
            observation.disarm();
            return Err(error);
        }
        let result = result.map_err(normalize_write_outcome);
        if let Some(dispatch) = &dispatch {
            dispatch.finish(result.as_ref().map(|_| ()).map_err(|error| *error));
        }
        observation.finish(result.as_ref().map(|_| ()).map_err(|error| *error));
        result
    })
}

fn normalize_write_outcome(error: DbError) -> DbError {
    match error.kind() {
        ErrorKind::Connection | ErrorKind::Timeout | ErrorKind::Cancelled => {
            DbError::new(ErrorKind::WriteOutcomeUnknown)
        }
        _ => error,
    }
}

async fn collect_sets(reader: &mut dyn Reader, limit: RowLimit) -> Result<Vec<Vec<Row>>, DbError> {
    let mut sets = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    loop {
        let mut set = Vec::new();
        while let Some(row) = reader.next_row_async().await? {
            if rows == limit.rows() {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            let size = row.encoded_bytes()?;
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
            if bytes > limit.bytes() {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            rows = rows
                .checked_add(1)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
            set.push(row);
        }
        sets.push(set);
        if !reader.has_next_set_async().await? {
            break;
        }
        if sets.len() == limit.max_sets() {
            return Err(DbError::new(ErrorKind::Overflow));
        }
    }
    reader.finish_async().await?;
    Ok(sets)
}

fn fetch_observed<'a, S: StatementExecutor + ?Sized>(
    session: &'a mut S,
    sql: &'a str,
    limit: RowLimit,
    options: QueryOptions,
    policy: Arc<PoolPolicy>,
) -> DbFuture<'a, BufferedReader> {
    Box::pin(async move {
        let dispatch = operation_dispatch(&options);
        let captured = captured_sql(sql, &policy, &options);
        if let Some(dispatch) = &dispatch {
            dispatch.start().await?;
        }
        let started = Instant::now();
        let mut observation = ObservationGuard::new(
            Operation::Fetch,
            started,
            captured,
            Arc::clone(&policy),
            options.clone(),
        );
        let result = async {
            let mut provider_options = options.clone();
            provider_options.policy_applied = true;
            let mut reader = session.fetch_statement(sql, provider_options).await?;
            collect_sets(reader.as_mut(), limit).await
        }
        .await;
        if let Some(error) = dispatch.as_ref().and_then(|dispatch| dispatch.rejection()) {
            observation.disarm();
            return Err(error);
        }
        if let Some(dispatch) = &dispatch {
            dispatch.finish(result.as_ref().map(|_| ()).map_err(|error| *error));
        }
        observation.finish(result.as_ref().map(|_| ()).map_err(|error| *error));
        result.map(BufferedReader::from_sets)
    })
}
