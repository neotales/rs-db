use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

use mssql_tds::connection::tds_client::{ResultSet, StatementResult, TdsClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqldatatypes::TdsDataType;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use mssql_tds::message::transaction_management::TransactionIsolationLevel;
use mssql_tds::query::metadata::ColumnMetadata;

use sqlx_core::Either;
use sqlx_core::connection::{Connection, LogSettings};
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::Transaction;

use crate::error::database_error_with_context;
use crate::statement::ParamInfo;
use crate::type_info::from_system_type_name;
use crate::value::sql_string_to_utf8;
use crate::{
    Mssql, MssqlArguments, MssqlColumn, MssqlConnectOptions, MssqlQueryResult, MssqlRow,
    MssqlStatement, MssqlTypeInfo, MssqlValue,
};

/// A live connection to SQL Server over TDS.
pub struct MssqlConnection {
    /// `mssql-tds` owns the transport, login state and result-set cursor. It is
    /// boxed because `ClientContext` makes it large.
    client: Box<TdsClient>,
    transaction_depth: usize,
    /// Set when a row stream was dropped before it finished. The result set is
    /// then half-read, and the connection has to drain it before anything else
    /// can be sent.
    needs_reset: bool,
    /// Set when a transaction was dropped without being committed. The rollback
    /// is issued lazily on next use, because `TransactionManager::start_rollback`
    /// is synchronous.
    pending_rollback: bool,
    /// A cancelled or failed I/O operation may have left the TDS state uncertain.
    poisoned: bool,
    /// Statement logging configuration, copied from the connect options.
    log_settings: LogSettings,
}

impl MssqlConnection {
    /// Connects to SQL Server using the given options.
    pub(crate) async fn connect_with(
        options: &MssqlConnectOptions,
    ) -> Result<Self, sqlx_core::Error> {
        let datasource = options.datasource();
        let context = options.client_context();

        // `mssql-tds`'s client-creation future is deeply nested. Awaiting it
        // unboxed splices that type into this opaque future, where it counts
        // against the recursion limit of every crate that connects. Coerce it
        // to a trait object so the concrete type is erased, not just moved to
        // the heap.
        let connect: Pin<
            Box<dyn Future<Output = mssql_tds::core::TdsResult<TdsClient>> + Send + '_>,
        > = Box::pin(TdsConnectionProvider.create_client(context, &datasource, None));
        let client = connect.await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                format!("failed to connect to SQL Server at {datasource}"),
            ))
        })?;

        Ok(Self {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: false,
            poisoned: false,
            log_settings: options.log_settings().clone(),
        })
    }

    /// The current transaction nesting depth.
    pub(crate) fn transaction_depth(&self) -> usize {
        self.transaction_depth
    }

    /// Records that an abandoned transaction still needs rolling back.
    pub(crate) fn queue_rollback(&mut self) {
        self.pending_rollback = true;
        self.transaction_depth = 0;
    }

    /// Issues any work left pending by an earlier dropped stream or transaction.
    pub(crate) async fn drain_pending(&mut self) -> Result<(), sqlx_core::Error> {
        if self.poisoned {
            return Err(sqlx_core::Error::Protocol(
                "SQL Server connection is unusable after interrupted or failed I/O".into(),
            ));
        }
        if self.needs_reset {
            let mut guard = IoGuard::new(&mut self.poisoned);
            self.client.close_query().await.map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "failed to close the previous result set",
                ))
            })?;
            guard.complete();
            self.needs_reset = false;
        }

        if self.pending_rollback {
            let mut guard = IoGuard::new(&mut self.poisoned);
            self.client
                .rollback_transaction(None, None)
                .await
                .map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to roll back the abandoned transaction",
                    ))
                })?;
            guard.complete();
            self.pending_rollback = false;
        }

        Ok(())
    }

    pub(crate) async fn begin_transaction(&mut self) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await?;

        let mut guard = IoGuard::new(&mut self.poisoned);
        if self.transaction_depth == 0 {
            self.client
                .begin_transaction(TransactionIsolationLevel::ReadCommitted, None)
                .await
        } else {
            // SQL Server has no real nested transactions. Nesting is emulated
            // with a savepoint so an inner rollback only undoes the inner work.
            self.client
                .save_transaction(format!("sqlx_sp_{}", self.transaction_depth))
                .await
        }
        .map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to begin a transaction",
            ))
        })?;
        guard.complete();

        self.transaction_depth += 1;
        Ok(())
    }

    pub(crate) async fn commit_transaction(&mut self) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await?;
        if self.transaction_depth == 0 {
            return Ok(());
        }

        // Committing an inner "transaction" is a no-op in SQL Server: only the
        // outermost commit ends the transaction.
        if self.transaction_depth == 1 {
            let mut guard = IoGuard::new(&mut self.poisoned);
            self.client
                .commit_transaction(None, None)
                .await
                .map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to commit the transaction",
                    ))
                })?;
            guard.complete();
        }

        self.transaction_depth -= 1;
        Ok(())
    }

    pub(crate) async fn rollback_transaction(&mut self) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await?;
        if self.transaction_depth == 0 {
            return Ok(());
        }

        let mut guard = IoGuard::new(&mut self.poisoned);
        if self.transaction_depth == 1 {
            self.client
                .rollback_transaction(None, None)
                .await
                .map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to roll back the transaction",
                    ))
                })?;
        } else {
            self.client
                .rollback_transaction(
                    Some(format!("sqlx_sp_{}", self.transaction_depth - 1)),
                    None,
                )
                .await
                .map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to roll back to the nested transaction savepoint",
                    ))
                })?;
        }

        guard.complete();
        self.transaction_depth -= 1;
        Ok(())
    }

    /// Describes a statement without running it.
    ///
    /// Uses `sp_describe_first_result_set`, which asks the server to parse the
    /// statement and report its result shape, plus
    /// `sp_describe_undeclared_parameters` to learn its parameter types.
    async fn describe_statement(
        &mut self,
        sql: &str,
    ) -> Result<DescribedStatement, sqlx_core::Error> {
        // Description is a metadata operation, not execution; never rewrite the supplied SQL.
        // Inference remains SQL Server's responsibility when native markers appear.
        let rewritten = sql.to_owned();
        let marker_count = usize::from(sql.contains("@P") || sql.contains("@p"));

        // `sp_describe_first_result_set` only understands `@P1`-style names and
        // requires each parameter to be declared. The types come from
        // `sp_describe_undeclared_parameters`, which infers them from context.
        let (declarations, parameter_types) = if marker_count > 0 {
            let inferred = self.infer_parameters(&rewritten).await?;
            (inferred.declarations, inferred.types)
        } else {
            // No parameters: an empty list is exact rather than merely a count.
            (String::new(), Some(Vec::new()))
        };

        let (metadata, rows) = if declarations.is_empty() {
            self.collect_rows(
                "EXEC sp_describe_first_result_set @tsql = @P1, @params = NULL, @options = 0",
                vec![nvarchar_parameter("@P1", &rewritten)],
            )
            .await?
        } else {
            self.collect_rows(
                "EXEC sp_describe_first_result_set @tsql = @P1, @params = @P2, @options = 0",
                vec![
                    nvarchar_parameter("@P1", &rewritten),
                    nvarchar_parameter("@P2", &declarations),
                ],
            )
            .await?
        };

        let index_of = |name: &str| {
            metadata
                .iter()
                .position(|column| column.column_name.eq_ignore_ascii_case(name))
        };

        let name_column = index_of("name");
        let nullable_column = index_of("is_nullable");
        let hidden_column = index_of("is_hidden");
        let type_name_column = index_of("system_type_name");
        let tds_type_column = index_of("tds_type_id");
        let length_column = index_of("tds_length");
        let precision_column = index_of("precision");
        let scale_column = index_of("scale");

        let mut columns = Vec::new();

        for values in &rows {
            if bool_at(values, hidden_column).unwrap_or(false) {
                continue;
            }

            let ordinal = columns.len();
            let name = text_at(values, name_column).unwrap_or_default();
            let type_name =
                text_at(values, type_name_column).unwrap_or_else(|| "unknown".to_owned());
            let tds_type = integer_at(values, tds_type_column).unwrap_or_default();
            let length = integer_at(values, length_column).unwrap_or_default();
            let raw_type = u8::try_from(tds_type).unwrap_or(0);
            let raw_precision =
                integer_at(values, precision_column).and_then(|value| u8::try_from(value).ok());
            let raw_scale =
                integer_at(values, scale_column).and_then(|value| u8::try_from(value).ok());

            // `sp_describe_first_result_set` reports the storage type in
            // `tds_type_id` — every nullable integer is `IntN`, for example —
            // and fills precision and scale for every numeric type. The
            // `system_type_name` is the logical type, and it is the same text
            // the parameter path parses, so prefer it: it normalizes to the
            // constructors, which is what the query macros match against.
            let info = from_system_type_name(&type_name)
                .unwrap_or_else(|| {
                    MssqlTypeInfo::new(
                        type_name,
                        raw_type,
                        u32::try_from(length).unwrap_or(0),
                        raw_precision,
                        raw_scale,
                    )
                })
                .with_precision_scale(raw_precision, raw_scale);

            columns.push(MssqlColumn::new(
                ordinal,
                name,
                info,
                Some(bool_at(values, nullable_column).unwrap_or(true)),
            ));
        }

        // Report the individual types only when they are known for every
        // parameter, because the macros use this list to check each bound
        // argument, and a wrong entry would reject a valid one. A count is
        // always safe: it drives the arity check.
        let parameters = match parameter_types {
            Some(types) if types.len() == marker_count => Some(Either::Left(types)),
            _ => Some(Either::Right(marker_count)),
        };

        Ok(DescribedStatement {
            columns,
            parameters,
        })
    }

    /// Runs a statement through `sp_executesql` and collects one result set.
    ///
    /// Returns the result set's column metadata alongside its rows, so callers
    /// can address columns by name.
    async fn collect_rows(
        &mut self,
        sql: &str,
        params: Vec<RpcParameter>,
    ) -> Result<(Vec<ColumnMetadata>, Vec<Vec<ColumnValues>>), sqlx_core::Error> {
        self.drain_pending().await?;

        let mut guard = IoGuard::new(&mut self.poisoned);
        let result = self
            .client
            .execute_sp_executesql(sql.to_owned(), params, ())
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "failed to describe the statement",
                ))
            })?;

        if !matches!(result, StatementResult::Rows) {
            let positioned = self.client.advance_to_rows().await.map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "failed to read statement metadata",
                ))
            })?;

            if !positioned {
                self.client.close_query().await.map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to close metadata result",
                    ))
                })?;
                guard.complete();
                return Ok((Vec::new(), Vec::new()));
            }
        }

        let metadata = self.client.get_metadata().clone();
        let mut rows = Vec::new();

        while let Some(values) = self.client.next_row().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to read statement metadata",
            ))
        })? {
            rows.push(values);
        }

        self.client.close_query().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to close the metadata result set",
            ))
        })?;

        guard.complete();
        Ok((metadata, rows))
    }

    /// Infers the type of every parameter in a rewritten statement.
    ///
    /// `sp_describe_first_result_set` requires every parameter to be declared,
    /// and `sp_describe_undeclared_parameters` lets SQL Server infer the types
    /// from how each parameter is used. The same rows also give the driver
    /// per-parameter type information, which is reported to sqlx whenever it is
    /// specific enough to be useful.
    async fn infer_parameters(
        &mut self,
        rewritten_sql: &str,
    ) -> Result<InferredParameters, sqlx_core::Error> {
        let (metadata, rows) = self
            .collect_rows(
                "EXEC sp_describe_undeclared_parameters @tsql = @P1",
                vec![nvarchar_parameter("@P1", rewritten_sql)],
            )
            .await?;

        let index_of = |name: &str| {
            metadata
                .iter()
                .position(|column| column.column_name.eq_ignore_ascii_case(name))
        };

        let ordinal_column = index_of("parameter_ordinal");
        let name_column = index_of("name");
        let type_column = index_of("suggested_system_type_name");

        let mut inferred = Vec::with_capacity(rows.len());

        for values in &rows {
            let ordinal = integer_at(values, ordinal_column).unwrap_or_default();
            let name = text_at(values, name_column).unwrap_or_else(|| format!("@P{ordinal}"));
            // A parameter whose type cannot be inferred is declared as text,
            // which SQL Server converts from for most target types.
            let type_name =
                text_at(values, type_column).unwrap_or_else(|| "nvarchar(4000)".to_owned());

            inferred.push((ordinal, name, type_name));
        }

        // The declaration list is positional, so do not depend on the order the
        // rows happen to arrive in.
        inferred.sort_by_key(|(ordinal, _, _)| *ordinal);

        let mut declarations = Vec::with_capacity(inferred.len());
        let mut types = Vec::with_capacity(inferred.len());
        let mut every_type_is_known = true;

        for (_, name, type_name) in &inferred {
            declarations.push(format!("{name} {type_name}"));

            // A type this driver cannot pin to one Rust type is only useful to
            // sqlx as part of the parameter count.
            match from_system_type_name(type_name) {
                Some(info) if info.has_exact_rust_mapping() => types.push(info),
                _ => every_type_is_known = false,
            }
        }

        Ok(InferredParameters {
            declarations: declarations.join(", "),
            types: every_type_is_known.then_some(types),
        })
    }

    /// Runs a statement that is not expected to return rows.
    async fn execute_simple(&mut self, sql: &'static str) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await?;

        let mut guard = IoGuard::new(&mut self.poisoned);
        self.client
            .execute(sql.to_owned(), ())
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(error, "query failed"))
            })?;

        self.client.close_query().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to close the result set",
            ))
        })?;
        guard.complete();
        Ok(())
    }
}

impl std::fmt::Debug for MssqlConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlConnection")
            .field("transaction_depth", &self.transaction_depth)
            .field("needs_reset", &self.needs_reset)
            .field("pending_rollback", &self.pending_rollback)
            .finish_non_exhaustive()
    }
}

/// The parameter types inferred for a statement.
struct InferredParameters {
    /// The `@P1 type, @P2 type` list `sp_describe_first_result_set` requires.
    declarations: String,
    /// One entry per parameter, or `None` when a type could not be pinned down.
    types: Option<Vec<MssqlTypeInfo>>,
}

/// A statement's shape, as reported by the server without running it.
struct DescribedStatement {
    columns: Vec<MssqlColumn>,
    parameters: ParamInfo,
}

/// Sets the recovery flag if the row stream was dropped before finishing.
struct ResetGuard<'a> {
    flag: &'a mut bool,
    armed: bool,
}

/// A TDS operation is only reusable after its future completes successfully.
struct IoGuard<'a>(&'a mut bool);

impl<'a> IoGuard<'a> {
    fn new(poisoned: &'a mut bool) -> Self {
        *poisoned = true;
        Self(poisoned)
    }

    fn complete(&mut self) {
        *self.0 = false;
    }
}

impl<'a> ResetGuard<'a> {
    fn new(flag: &'a mut bool) -> Self {
        Self { flag, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ResetGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.flag = true;
        }
    }
}

/// Validates native markers and builds RPC parameters without changing the SQL text.
fn build_request(
    sql: &str,
    arguments: Option<MssqlArguments>,
) -> Result<(String, Vec<RpcParameter>), sqlx_core::Error> {
    let arguments = arguments.unwrap_or_default();
    crate::validate_native_parameters(sql, arguments.len())
        .map_err(|message| sqlx_core::Error::Encode(message.into()))?;

    if arguments.is_empty() {
        return Ok((sql.to_owned(), Vec::new()));
    }

    let params = arguments
        .values()
        .iter()
        .enumerate()
        .map(|(index, value)| {
            RpcParameter::new(
                Some(format!("@P{}", index + 1)),
                StatusFlags::NONE,
                value.value().clone(),
            )
        })
        .collect();

    Ok((sql.to_owned(), params))
}

/// Target for statement logging.
const LOG_TARGET: &str = "sqlx_mssql_rs::query";

/// Logs a statement when the `log_statements` setting enables it.
fn log_statement(settings: &LogSettings, sql: &str) {
    let Some(level) = settings.statements_level.to_level() else {
        return;
    };

    log::log!(target: LOG_TARGET, level, "{sql}");
}

/// Logs a statement whose execution exceeded the configured duration.
fn log_slow_statement(settings: &LogSettings, elapsed: Duration, sql: &str) {
    let Some(level) = settings.slow_statements_level.to_level() else {
        return;
    };

    if elapsed < settings.slow_statements_duration {
        return;
    }

    log::log!(target: LOG_TARGET, level, "slow statement ({elapsed:?}): {sql}");
}

/// Builds an `nvarchar(max)` RPC parameter.
fn nvarchar_parameter(name: &str, value: &str) -> RpcParameter {
    RpcParameter::new(
        Some(name.to_owned()),
        StatusFlags::NONE,
        SqlType::NVarcharMax(Some(SqlString::from_utf8_string(value.to_owned()))),
    )
}

fn text_at(values: &[ColumnValues], index: Option<usize>) -> Option<String> {
    match index.and_then(|index| values.get(index))? {
        ColumnValues::String(value) => Some(sql_string_to_utf8(value)),
        _ => None,
    }
}

fn integer_at(values: &[ColumnValues], index: Option<usize>) -> Option<i64> {
    match index.and_then(|index| values.get(index))? {
        ColumnValues::TinyInt(value) => Some(i64::from(*value)),
        ColumnValues::SmallInt(value) => Some(i64::from(*value)),
        ColumnValues::Int(value) => Some(i64::from(*value)),
        ColumnValues::BigInt(value) => Some(*value),
        _ => None,
    }
}

fn bool_at(values: &[ColumnValues], index: Option<usize>) -> Option<bool> {
    match index.and_then(|index| values.get(index))? {
        ColumnValues::Bit(value) => Some(*value),
        _ => None,
    }
}

/// Statement helpers used by migration support.
///
/// Migrations need to run ad-hoc SQL and read scalar/row results, which the
/// `Executor` path cannot express.
#[cfg(feature = "migrate")]
impl MssqlConnection {
    /// Runs a statement and returns the rows of its first row-returning result set.
    pub(crate) async fn query_rows(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<MssqlValue>>, sqlx_core::Error> {
        self.drain_pending().await?;

        let mut guard = IoGuard::new(&mut self.poisoned);
        let result = self
            .client
            .execute(sql.to_owned(), ())
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(error, "query failed"))
            })?;

        // `execute` stops at the first statement boundary. Only advance when
        // that boundary was not already a row-returning result set, otherwise
        // the rows just handed to us would be skipped.
        if !matches!(result, StatementResult::Rows) {
            let positioned = self.client.advance_to_rows().await.map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "failed to read the result set",
                ))
            })?;

            if !positioned {
                self.client.close_query().await.map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(
                        error,
                        "failed to close result",
                    ))
                })?;
                guard.complete();
                return Ok(Vec::new());
            }
        }

        let metadata = self.client.get_metadata().clone();
        let mut rows = Vec::new();

        while let Some(values) = self.client.next_row().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(error, "failed to read a row"))
        })? {
            rows.push(
                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| match metadata.get(index) {
                        Some(column) => MssqlValue::from_column_value(value, column),
                        None => MssqlValue::null(MssqlTypeInfo::unknown()),
                    })
                    .collect(),
            );
        }

        self.client.close_query().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to close the result set",
            ))
        })?;

        guard.complete();
        Ok(rows)
    }

    /// Runs a statement, discarding any result sets.
    pub(crate) async fn exec_sql(&mut self, sql: &str) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await?;

        let mut guard = IoGuard::new(&mut self.poisoned);
        let mut result = self
            .client
            .execute(sql.to_owned(), ())
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(error, "query failed"))
            })?;

        while !matches!(result, StatementResult::End) {
            result = self.client.advance().await.map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "failed to advance past a result set",
                ))
            })?;
        }

        self.client.close_query().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to close the result set",
            ))
        })?;
        guard.complete();
        Ok(())
    }

    /// Runs a statement and returns its first column of its first row as `i64`.
    pub(crate) async fn scalar_i64(&mut self, sql: &str) -> Result<Option<i64>, sqlx_core::Error> {
        let rows = self.query_rows(sql).await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(MssqlValue::as_i64))
    }

    /// Reads `(version, checksum)` pairs from the migrations table.
    pub(crate) async fn list_migrations(
        &mut self,
        sql: &str,
    ) -> Result<Vec<(i64, Vec<u8>)>, sqlx_core::Error> {
        let rows = self.query_rows(sql).await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let version = row.first().and_then(MssqlValue::as_i64).unwrap_or_default();
                let checksum = row
                    .get(1)
                    .and_then(MssqlValue::as_bytes)
                    .map(|bytes| bytes.into_owned())
                    .unwrap_or_default();

                (version, checksum)
            })
            .collect())
    }

    /// Runs a statement and a follow-up bookkeeping statement, in a transaction
    /// unless the migration opted out of one.
    pub(crate) async fn run_migration_pair(
        &mut self,
        first_sql: &str,
        second_sql: &str,
        no_tx: bool,
    ) -> Result<Duration, sqlx_core::Error> {
        let started = Instant::now();

        if no_tx {
            self.exec_sql(first_sql).await?;
            self.exec_sql(second_sql).await?;
            return Ok(started.elapsed());
        }

        self.begin_transaction().await?;

        let outcome = async {
            self.exec_sql(first_sql).await?;
            self.exec_sql(second_sql).await
        }
        .await;

        match outcome {
            Ok(()) => {
                self.commit_transaction().await?;
            }
            Err(error) => {
                // The original failure is what matters; a failed rollback just
                // leaves the connection for `drain_pending` to recover.
                let _ = self.rollback_transaction().await;
                return Err(error);
            }
        }

        Ok(started.elapsed())
    }
}

impl Connection for MssqlConnection {
    type Database = Mssql;
    type Options = MssqlConnectOptions;
    async fn close(self) -> Result<(), sqlx_core::Error> {
        let MssqlConnection {
            client, poisoned, ..
        } = self;
        if poisoned {
            // No more protocol traffic on an uncertain stream.
            drop(client);
            return Ok(());
        }
        let mut client = client;

        // Coerced to a trait object for the same reason as `connect_with`: the
        // close chain is deep and the pool closes connections when it releases
        // them.
        let close: Pin<Box<dyn Future<Output = mssql_tds::core::TdsResult<()>> + Send + '_>> =
            Box::pin(client.close_connection());
        close.await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "failed to close the connection",
            ))
        })
    }

    async fn close_hard(self) -> Result<(), sqlx_core::Error> {
        // Dropping the client closes the transport without a graceful exchange.
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), sqlx_core::Error> {
        // `execute_simple` is deep and `Pool::acquire` pings on every checkout,
        // so this chain otherwise reaches more callers than any other. Coerce
        // to a trait object so the concrete future type is erased rather than
        // just moved to the heap.
        let ping: Pin<Box<dyn Future<Output = Result<(), sqlx_core::Error>> + Send + '_>> =
            Box::pin(self.execute_simple("SELECT 1"));
        ping.await
    }

    fn begin(
        &mut self,
    ) -> impl Future<Output = Result<Transaction<'_, Self::Database>, sqlx_core::Error>> + Send + '_
    {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> Result<(), sqlx_core::Error> {
        self.drain_pending().await
    }

    fn should_flush(&self) -> bool {
        self.poisoned || self.needs_reset || self.pending_rollback
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        // Statements are described on demand; nothing is cached here.
        0
    }

    async fn clear_cached_statements(&mut self) -> Result<(), sqlx_core::Error>
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        Ok(())
    }
}

/// A statement or row-set boundary in a SQL Server batch.
#[derive(Debug)]
pub enum MssqlResultEvent {
    /// A row set begins, including when it contains no rows.
    ResultSetStart(Arc<[MssqlColumn]>),
    /// A row within the current row set.
    Row(MssqlRow),
    /// The current row set ended.
    ResultSetEnd,
    /// A non-row statement, with its count when supplied by SQL Server.
    NonRow {
        /// Affected rows, or `None` if no count was supplied.
        rows_affected: Option<u64>,
    },
}

impl MssqlConnection {
    /// Calls `sys.sp_getapplock` and returns its raw SQL Server return value.
    ///
    /// The procedure is invoked by provider-owned SQL through `sp_executesql`
    /// and its `RETURN` value is selected as a fresh result row. The dependency's
    /// `ReturnStatus` is deliberately not used: `mssql-tds` does not clear that
    /// token value at the start of a command, so a missing token can reuse a
    /// previous success. A missing row, a non-`int` value, or a drain failure is
    /// an error, never a grant.
    ///
    /// `resource` is at most 255 UTF-16 code units, nonempty, and NUL-free;
    /// `timeout_ms` is positive. Both are sent as typed RPC parameters, not
    /// interpolated into SQL. Interpretation of the returned status is the
    /// caller's responsibility (`0`/`1` granted, `-1` timeout, `-2` cancelled,
    /// `-3` deadlock, `-999` parameter error).
    ///
    /// # Errors
    /// Returns a sanitized encode/connection error for an invalid request, a
    /// transport failure, or a missing/malformed fresh result.
    pub async fn acquire_application_lock(
        &mut self,
        resource: &str,
        timeout_ms: i32,
    ) -> Result<i32, sqlx_core::Error> {
        if resource.is_empty()
            || resource.encode_utf16().count() > 255
            || resource.as_bytes().contains(&0)
            || timeout_ms <= 0
        {
            return Err(sqlx_core::Error::Encode(
                "invalid application lock request".into(),
            ));
        }
        self.drain_pending().await?;
        let mut guard = IoGuard::new(&mut self.poisoned);
        let parameters = vec![
            RpcParameter::new(
                Some("@p1".to_owned()),
                StatusFlags::NONE,
                SqlType::NVarchar(Some(SqlString::from_utf8_string(resource.to_owned())), 255),
            ),
            RpcParameter::new(
                Some("@p2".to_owned()),
                StatusFlags::NONE,
                SqlType::Int(Some(timeout_ms)),
            ),
        ];
        let result = self
            .client
            .execute_sp_executesql(
                "DECLARE @rc int;                  EXEC @rc = sys.sp_getapplock @Resource = @p1,                  @LockMode = N'Exclusive', @LockOwner = N'Transaction',                  @LockTimeout = @p2;                  SELECT @rc AS result"
                    .to_owned(),
                parameters,
                (),
            )
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "application lock request failed",
                ))
            })?;
        if !matches!(result, StatementResult::Rows) {
            return Err(sqlx_core::Error::Encode(
                "application lock result rowset missing".into(),
            ));
        }
        let metadata = self.client.get_metadata().clone();
        if metadata.len() != 1 {
            return Err(sqlx_core::Error::Encode(
                "application lock result must have exactly one column".into(),
            ));
        }
        if !matches!(metadata[0].data_type, TdsDataType::Int4 | TdsDataType::IntN) {
            return Err(sqlx_core::Error::Encode(
                "application lock result column must be an integer".into(),
            ));
        }
        let row = self
            .client
            .next_row()
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "application lock result read failed",
                ))
            })?
            .ok_or_else(|| {
                sqlx_core::Error::Encode("application lock result row missing".into())
            })?;
        if row.len() != 1 {
            return Err(sqlx_core::Error::Encode(
                "application lock result row must have exactly one value".into(),
            ));
        }
        let status = match row.first() {
            Some(ColumnValues::Int(value)) => *value,
            _ => {
                return Err(sqlx_core::Error::Encode(
                    "application lock result has an unexpected value type".into(),
                ));
            }
        };
        if self
            .client
            .next_row()
            .await
            .map_err(|error| {
                sqlx_core::Error::from(database_error_with_context(
                    error,
                    "application lock result read failed",
                ))
            })?
            .is_some()
        {
            return Err(sqlx_core::Error::Encode(
                "application lock result must have exactly one row".into(),
            ));
        }
        match self.client.advance().await {
            Ok(StatementResult::End) => {}
            Ok(_) => {
                return Err(sqlx_core::Error::Encode(
                    "application lock result must have exactly one rowset".into(),
                ));
            }
            Err(error) => {
                return Err(sqlx_core::Error::from(database_error_with_context(
                    error,
                    "application lock result drain failed",
                )));
            }
        }
        self.client.close_query().await.map_err(|error| {
            sqlx_core::Error::from(database_error_with_context(
                error,
                "application lock result close failed",
            ))
        })?;
        guard.complete();
        Ok(status)
    }

    /// Execute a query as a stream that preserves row-set boundaries.
    ///
    /// Dropping between events schedules a drain before the next operation.
    /// Cancellation or failure during TDS I/O instead makes the connection
    /// unusable: subsequent operations return an error and SQLx's pool
    /// release ping discards it. `close` drops its transport without further
    /// protocol traffic. A stream that has not been polled submits nothing.
    ///
    /// ```no_run
    /// use futures_util::StreamExt;
    /// use sqlx_core::query::query;
    /// use sqlx_mssql_rs_core::{MssqlConnection, MssqlResultEvent};
    ///
    /// # async fn example(conn: &mut MssqlConnection) -> Result<(), sqlx_core::Error> {
    /// let mut events = conn.fetch_events(query("SELECT 1 AS value WHERE 1 = 0"));
    /// while let Some(event) = events.next().await {
    ///     match event? {
    ///         MssqlResultEvent::ResultSetStart(columns) => println!("{} columns", columns.len()),
    ///         MssqlResultEvent::Row(_row) => {}
    ///         MssqlResultEvent::ResultSetEnd => {}
    ///         MssqlResultEvent::NonRow { rows_affected: _ } => {}
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn fetch_events<'e, 'q, E>(
        &'e mut self,
        mut query: E,
    ) -> BoxStream<'e, Result<MssqlResultEvent, sqlx_core::Error>>
    where
        E: Execute<'q, Mssql>,
        'q: 'e,
        E: 'q,
    {
        // `Execute::sql` consumes the query, so arguments must be taken first.
        let arguments = query.take_arguments().map_err(sqlx_core::Error::Encode);
        let sql = query.sql();

        Box::pin(
            async_stream::try_stream! {
                let arguments = arguments?;
                self.drain_pending().await?;

                let MssqlConnection {
                    client,
                    needs_reset,
                    poisoned,
                    log_settings,
                    ..
                } = self;

                log_statement(log_settings, sql.as_str());

                let (rewritten_sql, params) = build_request(sql.as_str(), arguments)?;
                let started = Instant::now();

                let mut io_guard = IoGuard::new(poisoned);
                let mut result = if params.is_empty() {
                    client.execute(rewritten_sql, ()).await
                } else {
                    client.execute_sp_executesql(rewritten_sql, params, ()).await
                }
                .map_err(|error| {
                    sqlx_core::Error::from(database_error_with_context(error, "query failed"))
                })?;
                io_guard.complete();

                // From here the result set may be left half-read if the caller
                // drops the stream, so the guard records that on the way out.
                let mut guard = ResetGuard::new(needs_reset);

                loop {
                    match result {
                        StatementResult::Rows => {
                            let metadata = client.get_metadata().clone();

                            let columns: Arc<[MssqlColumn]> = metadata
                                .iter()
                                .enumerate()
                                .map(|(ordinal, column)| {
                                    MssqlColumn::new(
                                        ordinal,
                                        column.column_name.clone(),
                                        MssqlTypeInfo::from_column_metadata(column),
                                        Some(column.is_nullable()),
                                    )
                                })
                                .collect::<Vec<_>>()
                                .into();

                            yield MssqlResultEvent::ResultSetStart(columns.clone());

                            loop {
                                let mut io_guard = IoGuard::new(poisoned);
                                let values = client.next_row().await.map_err(|error| {
                                    sqlx_core::Error::from(database_error_with_context(
                                        error,
                                        "failed to read a row",
                                    ))
                                })?;
                                io_guard.complete();
                                let Some(values) = values else { break };
                                let row_values = values
                                    .iter()
                                    .enumerate()
                                    .map(|(index, value)| match metadata.get(index) {
                                        Some(column) => MssqlValue::from_column_value(value, column),
                                        None => MssqlValue::null(MssqlTypeInfo::unknown()),
                                    })
                                    .collect();

                                yield MssqlResultEvent::Row(MssqlRow::new_shared(
                                    columns.clone(),
                                    row_values,
                                ));
                            }

                            // A row-returning statement reports no affected-row
                            // count: TDS tags a SELECT's DONE count as SQLSELECT,
                            // which counts matched rather than changed rows.
                            yield MssqlResultEvent::ResultSetEnd;
                        }
                        StatementResult::NoRows { rows_affected } => {
                            yield MssqlResultEvent::NonRow { rows_affected };
                        }
                        StatementResult::End => break,
                    }

                    let mut io_guard = IoGuard::new(poisoned);
                    result = client.advance().await.map_err(|error| {
                        sqlx_core::Error::from(database_error_with_context(
                            error,
                            "failed to advance to the next result set",
                        ))
                    })?;
                    io_guard.complete();
                }

                log_slow_statement(log_settings, started.elapsed(), sql.as_str());
                guard.disarm();
            }
            .boxed(),
        )
    }
}

impl<'c> Executor<'c> for &'c mut MssqlConnection {
    type Database = Mssql;

    fn fetch_many<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<MssqlQueryResult, MssqlRow>, sqlx_core::Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        self.fetch_events(query)
            .filter_map(|event| async move {
                match event {
                    Ok(MssqlResultEvent::ResultSetStart(_)) => None,
                    Ok(MssqlResultEvent::Row(row)) => Some(Ok(Either::Right(row))),
                    Ok(MssqlResultEvent::ResultSetEnd) => {
                        Some(Ok(Either::Left(MssqlQueryResult::new(0))))
                    }
                    Ok(MssqlResultEvent::NonRow { rows_affected }) => Some(Ok(Either::Left(
                        MssqlQueryResult::new(rows_affected.unwrap_or(0)),
                    ))),
                    Err(error) => Some(Err(error)),
                }
            })
            .boxed()
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<MssqlRow>, sqlx_core::Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        Box::pin(async move {
            let mut stream = self.fetch_many(query);

            while let Some(item) = stream.next().await {
                if let Either::Right(row) = item? {
                    return Ok(Some(row));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &[MssqlTypeInfo],
    ) -> BoxFuture<'e, Result<MssqlStatement, sqlx_core::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let described = self.describe_statement(sql.as_str()).await?;

            Ok(MssqlStatement::new(
                sql,
                described.columns,
                described.parameters,
            ))
        })
    }

    #[cfg(feature = "offline")]
    fn describe<'e>(
        self,
        sql: SqlStr,
    ) -> BoxFuture<'e, Result<sqlx_core::describe::Describe<Self::Database>, sqlx_core::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let described = self.describe_statement(sql.as_str()).await?;
            let nullable = described
                .columns
                .iter()
                .map(MssqlColumn::nullable)
                .collect();

            Ok(sqlx_core::describe::Describe {
                columns: described.columns,
                parameters: described.parameters,
                nullable,
            })
        })
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use mssql_tds::datatypes::sqldatatypes::TdsDataType;
    use mssql_tds::test_client_support::{
        col_metadata, done_more_select_with_count, done_more_with_count, done_no_more,
        done_select_no_more, int_columns, return_status, sql_error, tds_client_from_int_rows,
        tds_client_from_int_rows_with_trailing_tokens, tds_client_from_tokens,
    };
    use sqlx_core::column::Column;

    #[tokio::test]
    async fn empty_sets_and_nonrow_counts_are_distinct() {
        let client = tds_client_from_tokens(vec![
            col_metadata(int_columns(1)),
            done_more_select_with_count(0),
            done_more_with_count(3),
            col_metadata(int_columns(2)),
            done_select_no_more(),
        ]);
        let mut connection = MssqlConnection {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: false,
            poisoned: false,
            log_settings: LogSettings::default(),
        };
        let mut events = connection.fetch_events(sqlx_core::query::query("test"));
        match events.next().await.unwrap().unwrap() {
            MssqlResultEvent::ResultSetStart(columns) => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name(), "c1");
            }
            event => panic!("unexpected event: {event:?}"),
        }
        assert!(matches!(
            events.next().await.unwrap().unwrap(),
            MssqlResultEvent::ResultSetEnd
        ));
        assert!(matches!(
            events.next().await.unwrap().unwrap(),
            MssqlResultEvent::NonRow {
                rows_affected: Some(3)
            }
        ));
        assert!(
            matches!(events.next().await.unwrap().unwrap(), MssqlResultEvent::ResultSetStart(columns) if columns.len() == 2)
        );
        assert!(matches!(
            events.next().await.unwrap().unwrap(),
            MssqlResultEvent::ResultSetEnd
        ));
        assert!(events.next().await.is_none());
        drop(events);
        assert!(!connection.needs_reset);
    }

    #[tokio::test]
    async fn dropped_at_start_marks_connection_for_reset() {
        let client =
            tds_client_from_tokens(vec![col_metadata(int_columns(1)), done_select_no_more()]);
        let mut connection = MssqlConnection {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: false,
            poisoned: false,
            log_settings: LogSettings::default(),
        };
        let mut events = connection.fetch_events(sqlx_core::query::query("test"));
        assert!(matches!(
            events.next().await.unwrap().unwrap(),
            MssqlResultEvent::ResultSetStart(_)
        ));
        drop(events);
        assert!(connection.needs_reset);
    }

    #[tokio::test]
    async fn app_lock_reads_a_fresh_result_row() {
        let client = tds_client_from_int_rows(vec![vec![0]]);
        let mut connection = MssqlConnection {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: false,
            poisoned: false,
            log_settings: LogSettings::default(),
        };
        assert_eq!(
            connection
                .acquire_application_lock("neotales-test-lock", 1)
                .await
                .unwrap(),
            0
        );
        assert!(!connection.poisoned);
    }

    #[tokio::test]
    async fn app_lock_rejects_extra_rows_columns_and_rowsets() {
        for client in [
            tds_client_from_int_rows(vec![vec![0], vec![1]]),
            tds_client_from_int_rows(vec![vec![0, 1]]),
            tds_client_from_int_rows_with_trailing_tokens(
                vec![vec![0]],
                vec![
                    done_more_select_with_count(1),
                    col_metadata(int_columns(1)),
                    done_select_no_more(),
                ],
            ),
        ] {
            let mut connection = MssqlConnection {
                client: Box::new(client),
                transaction_depth: 0,
                needs_reset: false,
                pending_rollback: false,
                poisoned: false,
                log_settings: LogSettings::default(),
            };
            let error = connection
                .acquire_application_lock("neotales-test-lock", 1)
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx_core::Error::Encode(_)), "{error:?}");
            assert!(connection.poisoned);
        }
    }

    #[tokio::test]
    async fn app_lock_rejects_wrong_type_and_missing_rows() {
        let mut columns = int_columns(1);
        columns[0].data_type = TdsDataType::Int8;
        for client in [
            tds_client_from_tokens(vec![col_metadata(columns), done_select_no_more()]),
            tds_client_from_tokens(vec![col_metadata(int_columns(1)), done_select_no_more()]),
            tds_client_from_tokens(vec![done_no_more()]),
        ] {
            let mut connection = MssqlConnection {
                client: Box::new(client),
                transaction_depth: 0,
                needs_reset: false,
                pending_rollback: false,
                poisoned: false,
                log_settings: LogSettings::default(),
            };
            let error = connection
                .acquire_application_lock("neotales-test-lock", 1)
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx_core::Error::Encode(_)), "{error:?}");
            assert!(connection.poisoned);
        }
    }

    #[tokio::test]
    async fn prior_success_status_cannot_cover_a_missing_fresh_result() {
        // A prior command's ReturnStatus 0 is replayed, but the current app-lock
        // command has no fresh result row. It must fail closed and poison the
        // connection rather than treating the stale status as a grant.
        let client = tds_client_from_tokens(vec![return_status(0), done_no_more()]);
        let mut connection = MssqlConnection {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: false,
            poisoned: false,
            log_settings: LogSettings::default(),
        };
        let error = connection
            .acquire_application_lock("neotales-test-lock", 1)
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx_core::Error::Encode(_)), "{error:?}");
        assert!(connection.poisoned);
    }

    #[tokio::test]
    async fn failed_pending_rollback_poisoned_and_not_cleared() {
        let client =
            tds_client_from_tokens(vec![sql_error(3903, 16, "rollback failed"), done_no_more()]);
        let mut connection = MssqlConnection {
            client: Box::new(client),
            transaction_depth: 0,
            needs_reset: false,
            pending_rollback: true,
            poisoned: false,
            log_settings: LogSettings::default(),
        };
        assert!(connection.drain_pending().await.is_err());
        assert!(connection.pending_rollback);
        assert!(connection.poisoned);
        assert!(connection.drain_pending().await.is_err());
    }
}
