//! Typed operations on the same exclusive quiet connection. No SQL rewriting.
use futures_util::TryStreamExt as _;
use neotales_dbapi::{DbError, DbType, ErrorKind, Parameter, Row, RowLimit, Value};
use pg::types::{FromSql, Type};

use super::{Error, Lease, ToSql, TransactionState, operation_error, pg};
use std::{future::Future, time::Duration};

pub(super) fn error(kind: ErrorKind) -> DbError {
    DbError::new(kind)
}

pub(super) fn driver_error(cause: &pg::Error, dispatched: bool) -> DbError {
    if operation_error(cause) == Error::OutcomeUnknown {
        return error(if dispatched {
            ErrorKind::WriteOutcomeUnknown
        } else {
            ErrorKind::Connection
        });
    }
    error(match cause.code().map(pg::error::SqlState::code) {
        Some(code) if code.starts_with("23") => ErrorKind::Constraint,
        Some("22003") => ErrorKind::Overflow,
        Some("55P03" | "40P01" | "40001") => ErrorKind::Busy,
        Some("57014") => ErrorKind::Cancelled,
        _ => ErrorKind::Decode,
    })
}

fn parameter_type(parameter: &Parameter) -> Type {
    match parameter.db_type() {
        DbType::Bool => Type::BOOL,
        DbType::I64 => Type::INT8,
        DbType::F64 => Type::FLOAT8,
        DbType::Text => Type::TEXT,
        DbType::Bytes => Type::BYTEA,
    }
}

fn bind(parameters: &[Parameter]) -> Vec<Box<dyn ToSql + Sync + Send + '_>> {
    parameters
        .iter()
        .map(|parameter| -> Box<dyn ToSql + Sync + Send> {
            match parameter.value() {
                Value::Bool(value) => Box::new(*value),
                Value::I64(value) => Box::new(*value),
                Value::F64(value) => Box::new(*value),
                Value::Text(value) => Box::new(value.as_str()),
                Value::Bytes(value) => Box::new(value.as_slice()),
                Value::Null(DbType::Bool) => Box::new(Option::<bool>::None),
                Value::Null(DbType::I64) => Box::new(Option::<i64>::None),
                Value::Null(DbType::F64) => Box::new(Option::<f64>::None),
                Value::Null(DbType::Text) => Box::new(Option::<&str>::None),
                Value::Null(DbType::Bytes) => Box::new(Option::<&[u8]>::None),
            }
        })
        .collect()
}

// Recognize only leading SQL trivia; PostgreSQL remains authoritative for syntax
// and the maximum statement count. No token or placeholder rewriting is needed.
fn has_statement(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() || bytes[index] == b';' {
            index += 1;
        } else if bytes[index..].starts_with(b"--") {
            index += 2;
            while index < bytes.len() && !matches!(bytes[index], b'\n' | b'\r') {
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            index += 2;
            let mut depth = 1;
            while index < bytes.len() && depth != 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            if depth != 0 {
                // Let the server reject malformed comments, not reinterpret them.
                return true;
            }
        } else {
            return true;
        }
    }
    false
}

async fn prepare(
    client: &pg::Client,
    sql: &str,
    parameters: &[Parameter],
    transaction: bool,
) -> Result<pg::Statement, DbError> {
    if !transaction {
        return prepare_inner(client, sql, parameters).await;
    }
    control(
        client,
        "SAVEPOINT neotales_arity",
        pg::CommandTag::Savepoint,
        pg::TransactionStatus::Active,
    )
    .await?;
    let result = prepare_inner(client, sql, parameters).await;
    if result
        .as_ref()
        .is_err_and(|error| error.kind() == ErrorKind::Connection)
    {
        return result;
    }
    // A successful full Parse can coexist with an expected failed inference
    // probe. Always restore this internal savepoint before Execute. Session-level
    // prepared statements survive ROLLBACK TO; the caller's preceding writes do too.
    control(
        client,
        "ROLLBACK TO SAVEPOINT neotales_arity",
        pg::CommandTag::Rollback,
        pg::TransactionStatus::Active,
    )
    .await?;
    control(
        client,
        "RELEASE SAVEPOINT neotales_arity",
        pg::CommandTag::Release,
        pg::TransactionStatus::Active,
    )
    .await?;
    result
}

pub(super) async fn control(
    client: &pg::Client,
    sql: &str,
    tag: pg::CommandTag,
    status: pg::TransactionStatus,
) -> Result<(), DbError> {
    let completion = client
        .batch_complete(sql)
        .await
        .map_err(|_| error(ErrorKind::Connection))?;
    if completion.commands != 1 || completion.tag != tag || completion.status != status {
        return Err(error(ErrorKind::Connection));
    }
    Ok(())
}

async fn prepare_inner(
    client: &pg::Client,
    sql: &str,
    parameters: &[Parameter],
) -> Result<pg::Statement, DbError> {
    // PostgreSQL's protocol encodes the Bind parameter count as a u16.
    if parameters.len() > usize::from(u16::MAX) || !has_statement(sql) {
        return Err(error(ErrorKind::InvalidArgument));
    }
    let types: Vec<_> = parameters.iter().map(parameter_type).collect();
    let statement = client
        .prepare_typed(sql, &types)
        .await
        .map_err(|cause| driver_error(&cause, false))?;
    if statement.params() != types {
        return Err(error(ErrorKind::InvalidArgument));
    }
    if !types.is_empty() {
        // PostgreSQL accepts *unused* supplied parameter type declarations. A
        // simple params().len() check on the full typed preparation misses extra
        // bindings. Remove the last declaration in a second Parse (never Execute):
        // an unused declaration disappears from ParameterDescription, while an
        // actually referenced parameter remains or needs its type for inference.
        match client.prepare_typed(sql, &types[..types.len() - 1]).await {
            Ok(probe) if probe.params().len() != types.len() => {
                return Err(error(ErrorKind::InvalidArgument));
            }
            Ok(_) => {}
            Err(cause)
                if cause.as_db_error().is_some_and(|cause| {
                    cause.parsed_severity() == Some(pg::error::Severity::Error)
                        && matches!(cause.code().code(), "42P18" | "42725" | "42804" | "42P08")
                }) =>
            {
                // The full typed Parse succeeded. Removing a referenced type can
                // make it indeterminate, ambiguous, inconsistent, or leave a
                // polymorphic argument UNKNOWN (42804). Accept only these
                // inference failures with authoritative, nonfatal ERROR severity;
                // never swallow fatal, missing-severity, or transport failures.
            }
            Err(cause) => return Err(driver_error(&cause, false)),
        }
    }
    Ok(statement)
}

struct TypedCall<'a> {
    lease: &'a mut Lease,
    finished: bool,
}

impl TypedCall<'_> {
    fn client(&self) -> &pg::Client {
        &self
            .lease
            .connection
            .as_ref()
            .expect("checked lease owns a connection")
            .client
    }

    fn finish<T>(mut self, result: &Result<T, DbError>, dispatched: bool) {
        self.lease.finish_typed(result, dispatched);
        self.finished = true;
    }
}

impl Drop for TypedCall<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.lease.poisoned = true;
            self.lease.in_flight = false;
            self.lease.connection.take();
        }
    }
}

impl Lease {
    fn typed_call(&mut self) -> Result<TypedCall<'_>, DbError> {
        if self.transaction == TransactionState::Aborted {
            return Err(error(ErrorKind::Connection));
        }
        self.check().map_err(|_| error(ErrorKind::Connection))?;
        Ok(TypedCall {
            lease: self,
            finished: false,
        })
    }

    fn finish_typed<T>(&mut self, result: &Result<T, DbError>, dispatched: bool) {
        self.in_flight = false;
        if let Err(cause) = result {
            if self.transaction != TransactionState::Autocommit
                && !matches!(
                    cause.kind(),
                    ErrorKind::Connection | ErrorKind::WriteOutcomeUnknown | ErrorKind::Timeout
                )
            {
                // Preflight was restored to its savepoint. A dispatched,
                // authoritative server rejection instead leaves the transaction
                // aborted: only verified rollback or a failed commit may follow.
                if dispatched {
                    self.transaction = TransactionState::Aborted;
                }
                return;
            }
            self.poisoned = true;
            // Do not keep an interrupted result stream or a pending statement
            // alive. The worker still owns capacity until its socket is dropped.
            self.connection.take();
        }
    }

    /// Executes exactly one native PostgreSQL `$n` statement with typed dbapi
    /// parameters. Slice position is parameter number; repeated/reordered uses
    /// are not rewritten. Extra or missing bindings fail before Execute.
    /// Empty/comment-only input (including bare semicolons) is `InvalidArgument`.
    /// Returns the command's affected-row count only after `ReadyForQuery`.
    /// Result rows, if any, are discarded without collecting them.
    ///
    /// Uses server-side typed preparation (and an arity probe), not SQL text
    /// interpolation. Errors/interruption permanently poison this lease; no
    /// retry occurs. The server ceiling applies; wrap the future in a client
    /// deadline to bound transport stalls. Dropping it can leave a write unknown.
    /// Use `Lease::begin` for transactions; raw/script transaction control is not
    /// supported. Transaction arity probes are protected by an internal savepoint.
    ///
    /// # Errors
    /// Returns sanitized dbapi categories. `WriteOutcomeUnknown` means Execute
    /// may have run but lacks an authoritative outcome; reconcile before retrying.
    /// Preparation failures have not dispatched the user statement. A known server
    /// rejection is classified by SQLSTATE without retaining its message.
    pub async fn execute(&mut self, sql: &str, parameters: &[Parameter]) -> Result<u64, DbError> {
        self.execute_bounded(sql, parameters, None).await
    }

    pub(crate) async fn execute_bounded(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        budget: Option<Duration>,
    ) -> Result<u64, DbError> {
        let transaction = self.transaction != TransactionState::Autocommit;
        let call = self.typed_call()?;
        let client = call.client();
        let mut dispatched = false;
        let operation = async {
            let statement = prepare(client, sql, parameters, transaction).await?;
            let values = bind(parameters);
            let values = values
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync));
            dispatched = true;
            let stream = client
                .query_raw(&statement, values)
                .await
                .map_err(|cause| driver_error(&cause, true))?;
            tokio::pin!(stream);
            while stream
                .try_next()
                .await
                .map_err(|cause| driver_error(&cause, true))?
                .is_some()
            {}
            completed(
                stream.command_tag(),
                stream.transaction_status(),
                transaction,
            )?;
            stream
                .rows_affected()
                .ok_or_else(|| error(ErrorKind::WriteOutcomeUnknown))
        };
        let result = within(budget, operation)
            .await
            .unwrap_or_else(|| Err(expired(dispatched)));
        call.finish(&result, dispatched);
        result
    }

    /// Fetches one native `$n` statement into detached dbapi rows. Supports exact
    /// `BOOL`, `INT8`, finite `FLOAT8`, `TEXT`/`VARCHAR`, `BYTEA`, and matching typed
    /// nulls. Unsupported result types fail during preparation, before Execute.
    /// Duplicate names remain accessible by ordinal; name lookup is ambiguous.
    /// Empty/comment-only input is `InvalidArgument`; scripts are rejected.
    ///
    /// Pulls `query_raw` incrementally. Limits count aggregate rows and column-name
    /// UTF-8 bytes plus non-null binary payload bytes per row (bool 1, integer/float
    /// 8, text/bytes length). Checks occur before copying each cell. Allocation
    /// overhead, metadata, and driver wire buffers are not counted; this is not a
    /// bound on a single incoming wire message or total process memory.
    ///
    /// Success requires stream exhaustion through `ReadyForQuery`, never merely
    /// receipt of rows or `CommandComplete`. No partial rows escape on failure.
    /// Any SQL can write: exceeding a cap or failing to decode before completion
    /// returns `WriteOutcomeUnknown` and discards, rather than draining unbounded
    /// results or pretending the statement was read-only. Dropping the future also
    /// requires reconciliation and permanently prevents reuse. No retry occurs.
    /// Use `Lease::begin` for transaction ownership and safe arity-probe recovery.
    ///
    /// ```no_run
    /// use neotales_dbapi::{DbType, Parameter, RowLimit, Value};
    /// use neotales_dbapi_postgres::experimental::Lease;
    /// # async fn example(lease: &mut Lease) -> Result<(), neotales_dbapi::DbError> {
    /// let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
    /// let rows = lease.fetch("SELECT $1 AS number", &parameters,
    ///     RowLimit::new(1, 14)?).await?;
    /// assert_eq!(rows[0].get::<i64>("number")?, 7);
    /// // The owner must recycle only after success, or drop/discard after error.
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns sanitized dbapi categories. Arity/type/preparation errors occur
    /// before Execute. After dispatch, transport loss or local bounds/decode failure
    /// without authoritative completion is `WriteOutcomeUnknown`. The server
    /// ceiling applies; callers should also supply a client deadline.
    pub async fn fetch(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
    ) -> Result<Vec<Row>, DbError> {
        self.fetch_bounded(sql, parameters, limit, None).await
    }

    pub(crate) async fn fetch_bounded(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
        budget: Option<Duration>,
    ) -> Result<Vec<Row>, DbError> {
        let transaction = self.transaction != TransactionState::Autocommit;
        let call = self.typed_call()?;
        let client = call.client();
        let mut dispatched = false;
        let operation = async {
            let statement = prepare(client, sql, parameters, transaction).await?;
            if statement
                .columns()
                .iter()
                .any(|column| !supported(column.type_()))
            {
                return Err(error(ErrorKind::WrongType));
            }
            let values = bind(parameters);
            let values = values
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync));
            dispatched = true;
            let stream = client
                .query_raw(&statement, values)
                .await
                .map_err(|cause| driver_error(&cause, true))?;
            tokio::pin!(stream);
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            while let Some(row) = stream
                .try_next()
                .await
                .map_err(|cause| driver_error(&cause, true))?
            {
                if rows.len() == limit.rows() {
                    return Err(error(ErrorKind::WriteOutcomeUnknown));
                }
                rows.push(detach(&row, &mut bytes, limit.bytes())?);
            }
            completed(
                stream.command_tag(),
                stream.transaction_status(),
                transaction,
            )?;
            if stream.rows_affected().is_none() {
                return Err(error(ErrorKind::WriteOutcomeUnknown));
            }
            Ok(rows)
        };
        let result = within(budget, operation)
            .await
            .unwrap_or_else(|| Err(expired(dispatched)));
        call.finish(&result, dispatched);
        result
    }

    /// Executes a trusted, unparameterized SQL script on this lease. Unlike
    /// `execute`/`fetch`, this permits multiple statements. SQL is never rewritten;
    /// scripts must not override the configured ceiling. No automatic retry.
    /// Errors/interruption permanently poison the lease. Completion requires
    /// the driver's final `ReadyForQuery`; socket close is not rollback.
    ///
    /// # Errors
    /// Returns sanitized SQLSTATE categories, or `WriteOutcomeUnknown` if a
    /// dispatched script lacks an authoritative outcome. An error can follow
    /// earlier completed statements; do not infer that nothing took effect.
    pub async fn execute_trusted_script(&mut self, sql: &str) -> Result<(), DbError> {
        self.script_bounded(sql, None).await
    }

    pub(crate) async fn script_bounded(
        &mut self,
        sql: &str,
        budget: Option<Duration>,
    ) -> Result<(), DbError> {
        let transaction = self.transaction != TransactionState::Autocommit;
        let call = self.typed_call()?;
        let client = call.client();
        let operation = async {
            let stream = client
                .simple_query_raw(sql)
                .await
                .map_err(|cause| driver_error(&cause, true))?;
            tokio::pin!(stream);
            let mut acknowledged = false;
            loop {
                match stream.try_next().await {
                    Ok(Some(pg::SimpleQueryMessage::CommandComplete(_))) => acknowledged = true,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(cause) => {
                        // Earlier commands may have ended the original
                        // transaction before a later ErrorResponse arrived.
                        return Err(if stream.transaction_control() {
                            error(ErrorKind::WriteOutcomeUnknown)
                        } else {
                            driver_error(&cause, true)
                        });
                    }
                }
            }
            if !acknowledged || stream.transaction_control() {
                return Err(error(ErrorKind::WriteOutcomeUnknown));
            }
            completed(
                stream.command_tag(),
                stream.transaction_status(),
                transaction,
            )
        };
        let result = within(budget, operation)
            .await
            .unwrap_or_else(|| Err(expired(true)));
        call.finish(&result, true);
        result
    }
}

fn completed(
    tag: pg::CommandTag,
    status: Option<pg::TransactionStatus>,
    transaction: bool,
) -> Result<(), DbError> {
    let expected = if transaction {
        pg::TransactionStatus::Active
    } else {
        pg::TransactionStatus::Idle
    };
    if tag.is_transaction_control() || status != Some(expected) {
        return Err(error(ErrorKind::WriteOutcomeUnknown));
    }
    Ok(())
}

fn expired(dispatched: bool) -> DbError {
    error(if dispatched {
        ErrorKind::WriteOutcomeUnknown
    } else {
        ErrorKind::Timeout
    })
}

async fn within<T>(
    budget: Option<Duration>,
    operation: impl Future<Output = Result<T, DbError>>,
) -> Option<Result<T, DbError>> {
    match budget {
        Some(budget) => tokio::time::timeout(budget, operation).await.ok(),
        None => Some(operation.await),
    }
}
fn supported(kind: &Type) -> bool {
    matches!(
        *kind,
        Type::BOOL | Type::INT8 | Type::FLOAT8 | Type::TEXT | Type::VARCHAR | Type::BYTEA
    )
}

/// Borrow a cell's raw wire payload to check the budget before allocating.
struct WireBytes<'a>(&'a [u8]);

impl<'a> FromSql<'a> for WireBytes<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

fn detach(row: &pg::Row, bytes: &mut usize, limit: usize) -> Result<Row, DbError> {
    let unknown = || error(ErrorKind::WriteOutcomeUnknown);
    let mut cells = Vec::new();
    for (index, column) in row.columns().iter().enumerate() {
        let raw: Option<WireBytes<'_>> = row.try_get(index).map_err(|_| unknown())?;
        *bytes = bytes
            .checked_add(column.name().len())
            .and_then(|bytes| bytes.checked_add(raw.as_ref().map_or(0, |raw| raw.0.len())))
            .filter(|bytes| *bytes <= limit)
            .ok_or_else(unknown)?;
        let value = match *column.type_() {
            Type::BOOL => row
                .try_get::<_, Option<bool>>(index)
                .map(|value| value.map_or(Value::Null(DbType::Bool), Value::Bool)),
            Type::INT8 => row
                .try_get::<_, Option<i64>>(index)
                .map(|value| value.map_or(Value::Null(DbType::I64), Value::I64)),
            Type::FLOAT8 => row
                .try_get::<_, Option<f64>>(index)
                .map(|value| value.map_or(Value::Null(DbType::F64), Value::F64)),
            Type::TEXT | Type::VARCHAR => row.try_get::<_, Option<&str>>(index).map(|value| {
                value.map_or(Value::Null(DbType::Text), |value| {
                    Value::Text(value.to_owned())
                })
            }),
            Type::BYTEA => row.try_get::<_, Option<&[u8]>>(index).map(|value| {
                value.map_or(Value::Null(DbType::Bytes), |value| {
                    Value::Bytes(value.to_vec())
                })
            }),
            _ => return Err(unknown()),
        }
        .map_err(|_| unknown())?;
        value.validate().map_err(|_| unknown())?;
        cells.push((column.name().to_owned(), value));
    }
    Row::new(cells).map_err(|_| unknown())
}
