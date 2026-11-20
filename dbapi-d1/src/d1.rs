//! The wasm-only Cloudflare D1 implementation of the local traits.

use std::{fmt, sync::Arc};

use neotales_dbapi::{
    Capabilities, DbError, ErrorKind, Parameter, PoolDefaults, Row, Value, local,
    paging::{PageSize, multi::PageBudget},
};
use worker::{D1Database, D1PreparedStatement, Env, js_sys::Uint8Array, wasm_bindgen::JsValue};

use crate::codec::{
    D1Changes, classify_result, decode_raw, raw_with_columns, validate_placeholders,
    validate_result_count,
};
use crate::paging::{D1PageContinuation, D1PageStream, D1ReadOnlyReplaySafe, Materialize};

/// The largest integer a JavaScript number represents exactly.
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// Experimental pool over one Cloudflare D1 binding, locally tested but not hosted-validated.
///
/// The DBAPI pool contract is disabled: acquisition and all trait operations fail before dispatch.
/// Its explicitly named uncertain write methods do not provide DBAPI session semantics.
#[derive(Clone)]
pub struct D1Pool {
    database: Arc<D1Database>,
    defaults: PoolDefaults,
}

impl fmt::Debug for D1Pool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("D1Pool")
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

impl D1Pool {
    /// Experimentally wraps an existing D1 binding.
    #[must_use]
    pub fn new(database: D1Database) -> Self {
        Self {
            database: Arc::new(database),
            defaults: PoolDefaults::default(),
        }
    }

    /// Experimentally reads a D1 binding from a Workers environment; binding behavior is unvalidated.
    ///
    /// # Errors
    /// Returns `Connection` when the environment has no matching binding or cannot cast it.
    pub fn from_env(env: &Env, binding: &str) -> Result<Self, DbError> {
        env.d1(binding)
            .map(Self::new)
            .map_err(|_| DbError::new(ErrorKind::Connection))
    }

    /// Dispatches a single write without DBAPI session isolation or cancellation guarantees.
    ///
    /// Dropping this future after dispatch or receiving `WriteOutcomeUnknown` leaves the outcome
    /// unknown. Other calls and clones may still use the binding, including while it is in flight.
    /// The caller must independently handle coordination, reconciliation, and safe retry.
    ///
    /// # Errors
    /// Rejects unsupported values or placeholder mismatches before dispatch. A rejected promise
    /// returns `WriteOutcomeUnknown`, never proof that the statement did not execute.
    #[must_use]
    pub fn execute_uncertain_async<'a>(
        &'a self,
        sql: &'a str,
        parameters: &'a [Parameter],
    ) -> local::DbFuture<'a, u64> {
        Box::pin(async move {
            validate_placeholders(sql, parameters.len())?;
            let statement = prepare(&self.database, sql, parameters)?;
            let result = statement
                .run()
                .await
                .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
            result_changes(&result)
        })
    }

    /// Experimentally executes several prepared statements as one uncertain D1 batch.
    ///
    /// Cloudflare documents a D1 batch as atomic. That is not an interactive transaction: there is
    /// no begin, rollback, or commit handle, and [`Capabilities::transactions`] stays `false`.
    /// Results are returned in statement order and carry each statement's reported `changes`
    /// count. This operation is deliberately separate from the local transaction traits.
    /// Dropping its future does not cancel the dispatched promise and does not poison the pool
    /// or any session. The caller must treat the outcome as unknown.
    ///
    /// # Errors
    /// Returns `WriteOutcomeUnknown` if the dispatched batch rejects without an authoritative
    /// result, `InvalidArgument` for a placeholder/parameter mismatch before dispatch, or a
    /// sanitized D1 result error.
    #[must_use]
    pub fn batch_uncertain_async(
        &self,
        statements: Vec<D1BatchStatement>,
    ) -> local::DbFuture<'_, Vec<u64>> {
        Box::pin(async move {
            let expected_results = statements.len();
            let mut prepared = Vec::with_capacity(statements.len());
            for statement in statements {
                prepared.push(prepare(
                    &self.database,
                    &statement.sql,
                    &statement.parameters,
                )?);
            }
            let results = self
                .database
                .batch(prepared)
                .await
                .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
            validate_result_count(expected_results, results.len())?;
            results.iter().map(result_changes).collect()
        })
    }

    /// Creates an experimental requery pager over this binding and one fixed statement.
    ///
    /// `safety` is the caller's explicit assertion that `sql` is read-only and replay-safe; the
    /// library does not verify it. `page_size` bounds the rows of one materialized page, and
    /// `budget` bounds the whole delivered sequence. Those are publication bounds applied after D1
    /// materializes a page, not allocation limits; see [`crate::paging`].
    ///
    /// The SQL is stored, never rewritten, and no request is made until a page is fetched or the
    /// returned pager's stream is polled.
    pub fn pager(
        &self,
        sql: impl Into<String>,
        safety: D1ReadOnlyReplaySafe,
        page_size: PageSize,
        budget: PageBudget,
    ) -> D1Pager {
        D1Pager {
            database: Arc::clone(&self.database),
            sql: sql.into(),
            safety,
            page_size,
            budget,
        }
    }
}

impl local::Pool for D1Pool {
    fn capabilities(&self) -> Capabilities {
        crate::codec::d1_capabilities()
    }

    fn defaults(&self) -> PoolDefaults {
        self.defaults
    }

    fn acquire_async(&self) -> local::DbFuture<'_, Box<dyn local::Session>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

/// An experimental fixed-statement D1 requery pager.
///
/// Build it with [`D1Pool::pager`]. There is exactly one SQL string and it is never rewritten. Each
/// request prepares that SQL with the caller's bindings and materializes at most one page.
///
/// This type does not implement any DBAPI trait and does not make D1's fail-closed DBAPI contract
/// succeed. See [`crate::paging`] for the assertion, materialization, bound, error, and cancellation
/// rules.
#[must_use = "a pager does nothing unless a page is fetched or its stream is polled"]
pub struct D1Pager {
    database: Arc<D1Database>,
    sql: String,
    safety: D1ReadOnlyReplaySafe,
    page_size: PageSize,
    budget: PageBudget,
}

impl fmt::Debug for D1Pager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("D1Pager")
            .field("read_only_replay_safe", &self.safety)
            .field("page_size", &self.page_size)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl D1Pager {
    /// Materializes one page with the supplied bindings, then publishes it only if it fits.
    ///
    /// This is the one-page operation: exactly one request, no continuation, no retry, no prefetch.
    /// The page is returned only after the row, byte, and page-count bounds have been checked against
    /// the materialized rows, so a page that exceeds them is an error and never a truncated result.
    ///
    /// # Errors
    /// Returns `InvalidArgument` or `Unsupported` before dispatch for a placeholder/binding mismatch
    /// or an unsupported parameter value. A dispatched request without an authoritative result is
    /// `WriteOutcomeUnknown`; it is never reported as a safe-to-retry transport error. A cell that
    /// cannot be represented is `Decode` or `Unsupported`, a page larger than `page_size` is
    /// `Decode`, and a page over an aggregate row or byte bound is `Overflow`.
    #[must_use]
    pub fn fetch_page_async<'a>(
        &'a self,
        parameters: &'a [Parameter],
    ) -> local::DbFuture<'a, Vec<Row>> {
        Box::pin(async move {
            let rows = fetch_rows(&self.database, &self.sql, parameters).await?;
            validate_single_page(&rows, self.page_size, self.budget)?;
            Ok(rows)
        })
    }

    /// Turns this pager into a bounded `!Send` page stream driven by `continuation`.
    ///
    /// The continuation is asked for the initial bindings on the first poll and then with each
    /// materialized page before that page is published; it is not asked only when the next page is
    /// requested. No request is dispatched here.
    pub fn pages<'a, C>(self, continuation: C) -> D1PageStream<'a, C>
    where
        C: D1PageContinuation + 'a,
    {
        D1PageStream::new(
            Box::new(D1Materializer {
                database: self.database,
                sql: self.sql,
            }),
            continuation,
            self.page_size,
            self.budget,
        )
    }
}

/// One request over the pager's fixed SQL.
struct D1Materializer {
    database: Arc<D1Database>,
    sql: String,
}

impl Materialize for D1Materializer {
    fn materialize_async(&self, parameters: Vec<Parameter>) -> local::DbFuture<'_, Vec<Row>> {
        Box::pin(async move { fetch_rows(&self.database, &self.sql, &parameters).await })
    }
}

async fn fetch_rows(
    database: &D1Database,
    sql: &str,
    parameters: &[Parameter],
) -> Result<Vec<Row>, DbError> {
    validate_placeholders(sql, parameters.len())?;
    let statement = prepare(database, sql, parameters)?;
    let (columns, cells) = raw_with_columns(&statement).await?;
    decode_raw(&columns, cells)
}

fn validate_single_page(
    rows: &[Row],
    page_size: PageSize,
    budget: PageBudget,
) -> Result<(), DbError> {
    if rows.len() > page_size.get() {
        return Err(DbError::new(ErrorKind::Decode));
    }
    if rows.len() > budget.rows() || encoded_rows(rows)? > budget.bytes() {
        return Err(DbError::new(ErrorKind::Overflow));
    }
    Ok(())
}

fn encoded_rows(rows: &[Row]) -> Result<usize, DbError> {
    rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))
    })
}

/// One experimental statement in a [`D1Pool::batch_uncertain_async`] call.
///
/// The SQL and parameter values are private and never appear in `Debug`, matching the sanitized
/// error rule used by the rest of the API.
#[derive(Clone)]
pub struct D1BatchStatement {
    sql: String,
    parameters: Vec<Parameter>,
}

impl fmt::Debug for D1BatchStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("D1BatchStatement")
            .field("parameter_count", &self.parameters.len())
            .finish_non_exhaustive()
    }
}

impl D1BatchStatement {
    /// Creates an experimental batch statement with positional parameters.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for a placeholder/parameter count mismatch, mixed placeholder
    /// forms, or a named placeholder.
    pub fn new(sql: impl Into<String>, parameters: Vec<Parameter>) -> Result<Self, DbError> {
        let sql = sql.into();
        validate_placeholders(&sql, parameters.len())?;
        Ok(Self { sql, parameters })
    }
}

fn prepare(
    database: &D1Database,
    sql: &str,
    parameters: &[Parameter],
) -> Result<D1PreparedStatement, DbError> {
    let values = parameters
        .iter()
        .map(to_js_value)
        .collect::<Result<Vec<_>, _>>()?;
    let statement = database.prepare(sql);
    if values.is_empty() {
        return Ok(statement);
    }
    statement
        .bind(&values)
        .map_err(|_| DbError::new(ErrorKind::InvalidArgument))
}

#[allow(clippy::cast_precision_loss)] // Range-checked to JavaScript's exact integer range first.
fn to_js_value(parameter: &Parameter) -> Result<JsValue, DbError> {
    match parameter.value() {
        Value::Null(_) => Ok(JsValue::NULL),
        Value::Bool(value) => Ok(JsValue::from_bool(*value)),
        Value::I64(value) => {
            if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(value) {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            Ok(JsValue::from_f64(*value as f64))
        }
        Value::F64(value) => {
            // JS numbers erase whether an integral value originated as a float. The binding
            // cannot guarantee its SQL storage class, so reject rather than imply type fidelity.
            if !value.is_finite() || value.fract() == 0.0 || value.abs() > MAX_SAFE_INTEGER as f64 {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            Ok(JsValue::from_f64(*value))
        }
        Value::Text(value) => Ok(JsValue::from_str(value)),
        Value::Bytes(value) => Ok(JsValue::from(Uint8Array::from(value.as_slice()))),
    }
}

fn result_changes(result: &worker::D1Result) -> Result<u64, DbError> {
    if !result.success() {
        let changes =
            classify_result(false, result.error().as_deref(), D1Changes::MissingMetadata)?;
        return u64::try_from(changes).map_err(|_| DbError::new(ErrorKind::Overflow));
    }
    let meta = result
        .meta()
        .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
    let changes = match meta {
        Some(meta) => meta
            .changes
            .map_or(D1Changes::MissingValue, D1Changes::Value),
        None => D1Changes::MissingMetadata,
    };
    let changes = classify_result(true, None, changes)?;
    u64::try_from(changes).map_err(|_| DbError::new(ErrorKind::Overflow))
}
