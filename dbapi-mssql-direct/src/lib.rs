#![doc = include_str!("../README.md")]
//! Experimental direct TDS DBAPI provider, not suitable for production.
//!
//! Only sequential TCP connection on Linux is enabled. All dispatched failures
//! have unknown effects. The low-level `query` probe buffers without limits.

use std::{future::Future, net::SocketAddrV4, sync::Arc, time::Duration};

use mssql_tds::{
    connection::{
        client_context::ClientContext,
        tds_client::{ResultSet, StatementResult, TdsClient},
    },
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::EncryptionSetting,
    datatypes::column_values::ColumnValues,
    datatypes::sqldatatypes::TdsDataType,
    message::{
        parameters::rpc_parameters::RpcParameter, transaction_management::TransactionIsolationLevel,
    },
};
use neotales_dbapi::{DbType, Row, RowLimit, Value};

/// Native parameter mapping shared with DBAPI.
pub mod bind;
/// Transactional migration ledger over exclusive direct TDS connections.
pub mod migration;
/// Experimental DBAPI adapter over exclusive, non-recycled TDS connections.
pub mod provider;

#[cfg(test)]
mod live;

/// A sanitized error. An issued command may have changed server state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Quietness has not been validated on this target (currently Linux only).
    UnsafeTracing,
    /// Connection could not be established.
    Connect,
    /// A command was dispatched but its complete outcome is unknown.
    WriteOutcomeUnknown,
    /// The connection has been discarded.
    Discarded,
    /// No lease or connection could be acquired within the configured budget.
    AcquireTimeout,
    /// The pool configuration is invalid.
    InvalidConfiguration,
    /// Detached result limits were exceeded.
    Overflow,
    /// The server returned an unsupported or malformed value.
    Decode,
    /// A dispatched commit did not complete with a confirmed outcome.
    CommitOutcomeUnknown,
    /// The server declined the transaction-owned application lock.
    LockUnavailable,
}

/// Quietness is enforced in the vendored sources, not by a dispatcher.
/// Keep this boundary for the experimental driver's operation call sites.
async fn silent<T>(future: impl Future<Output = T>) -> T {
    future.await
}

/// An unpooled connection; interrupted operations permanently discard the client.
pub struct Probe {
    client: Option<TdsClient>,
}

impl std::fmt::Debug for Probe {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Probe")
            .field("connected", &self.client.is_some())
            .finish()
    }
}

/// A bounded connection owner. Connections are discarded, never recycled.
///
/// This intentionally sacrifices reuse until session-reset is proved.
/// Acquiring a lease and opening its connection share one deadline.
pub struct ExclusivePool {
    slots: Arc<tokio::sync::Semaphore>,
    context: ClientContext,
    address: SocketAddrV4,
    acquire_timeout: Duration,
    #[cfg(test)]
    insecure_test: bool,
}

impl std::fmt::Debug for ExclusivePool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExclusivePool")
            .field("address", &self.address)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("available_slots", &self.slots.available_permits())
            .finish_non_exhaustive()
    }
}

/// An exclusive connection whose capacity is released only after its socket is dropped.
pub struct ExclusiveLease {
    connection: Probe,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::fmt::Debug for ExclusiveLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExclusiveLease")
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

impl ExclusiveLease {
    /// Execute a query without exposing ownership of the underlying client.
    ///
    /// # Errors
    /// Returns `Discarded` if the lease has no client, or
    /// `WriteOutcomeUnknown` after a dispatched query cannot be fully read.
    pub async fn query(
        &mut self,
        sql: &str,
        parameters: Vec<RpcParameter>,
    ) -> Result<Vec<Vec<Vec<ColumnValues>>>, Error> {
        self.connection.query(sql, parameters).await
    }
}

impl ExclusivePool {
    /// Construct a bounded pool with a finite, positive acquisition deadline.
    ///
    /// # Errors
    /// Returns `InvalidConfiguration` for zero capacity, capacity beyond
    /// Tokio's semaphore limit, or a zero acquisition timeout.
    pub fn new(
        context: ClientContext,
        address: SocketAddrV4,
        capacity: usize,
        acquire_timeout: Duration,
    ) -> Result<Self, Error> {
        if capacity == 0
            || capacity > tokio::sync::Semaphore::MAX_PERMITS
            || acquire_timeout.is_zero()
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
            context,
            address,
            acquire_timeout,
            #[cfg(test)]
            insecure_test: false,
        })
    }

    /// Allow the disposable test fixture's self-signed certificate.
    #[cfg(test)]
    pub(crate) fn insecure_test(mut self) -> Self {
        self.insecure_test = true;
        self
    }

    /// Acquire an exclusive newly connected lease.
    ///
    /// # Errors
    /// Returns `AcquireTimeout`, `Connect`, `UnsafeTracing`, or `Discarded`
    /// when acquisition fails before a usable connection is returned.
    pub async fn acquire(&self) -> Result<ExclusiveLease, Error> {
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::PoolWait);
        tokio::time::timeout(self.acquire_timeout, async {
            let permit = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Discarded)?;
            #[cfg(test)]
            crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Reconnect);
            #[cfg(test)]
            let connection = if self.insecure_test {
                Probe::connect_insecure_test(self.context.clone(), self.address).await?
            } else {
                Probe::connect(self.context.clone(), self.address).await?
            };
            #[cfg(not(test))]
            let connection = Probe::connect(self.context.clone(), self.address).await?;
            #[cfg(test)]
            crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Connected);
            Ok(ExclusiveLease {
                connection,
                _permit: permit,
            })
        })
        .await
        .map_err(|_| Error::AcquireTimeout)?
    }
}

impl Probe {
    async fn script_batch(&mut self, sql: &str) -> Result<(), Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        let result = silent(async {
            let mut state = client
                .execute(sql.to_owned(), ())
                .await
                .map_err(|_| Error::WriteOutcomeUnknown)?;
            loop {
                match state {
                    StatementResult::Rows => return Err(Error::WriteOutcomeUnknown),
                    StatementResult::NoRows { .. } => {}
                    StatementResult::End => break,
                }
                state = client
                    .advance()
                    .await
                    .map_err(|_| Error::WriteOutcomeUnknown)?;
            }
            Ok(())
        })
        .await;
        if result.is_ok() {
            self.client = Some(client);
        }
        result
    }

    /// Exercise the provider's write response handling on a disposable fixture.
    #[cfg(test)]
    pub(crate) async fn fixture_write(
        &mut self,
        sql: &str,
        params: Vec<RpcParameter>,
    ) -> Result<u64, Error> {
        self.execute_one(sql, params).await
    }

    /// Run a disposable test fixture batch, discarding its connection on error.
    #[cfg(test)]
    pub(crate) async fn fixture_batch(&mut self, sql: &str) -> Result<(), Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        let result = silent(async {
            let mut state = client
                .execute(sql.to_owned(), ())
                .await
                .map_err(|_| Error::WriteOutcomeUnknown)?;
            loop {
                match state {
                    StatementResult::Rows => {
                        while client
                            .next_row()
                            .await
                            .map_err(|_| Error::WriteOutcomeUnknown)?
                            .is_some()
                        {}
                    }
                    StatementResult::NoRows { .. } => {}
                    StatementResult::End => break,
                }
                state = client
                    .advance()
                    .await
                    .map_err(|_| Error::WriteOutcomeUnknown)?;
            }
            Ok(())
        })
        .await;
        if result.is_ok() {
            self.client = Some(client);
        }
        result
    }

    async fn execute_one(&mut self, sql: &str, parameter: Vec<RpcParameter>) -> Result<u64, Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        let result = silent(async {
            let mut state = client
                .execute_sp_executesql(sql.to_owned(), parameter, ())
                .await
                .map_err(|_| Error::WriteOutcomeUnknown)?;
            let mut count = None;
            loop {
                match state {
                    StatementResult::NoRows {
                        rows_affected: Some(value),
                    } if count.is_none() => count = Some(value),
                    StatementResult::End => break,
                    _ => return Err(Error::WriteOutcomeUnknown),
                }
                state = client
                    .advance()
                    .await
                    .map_err(|_| Error::WriteOutcomeUnknown)?;
            }
            count.ok_or(Error::WriteOutcomeUnknown)
        })
        .await;
        match result {
            Ok(count) => {
                self.client = Some(client);
                Ok(count)
            }
            Err(error) => Err(error),
        }
    }

    /// Connect with required encryption and system certificate/hostname validation.
    ///
    /// `context` carries credentials; it is never formatted or returned.
    /// Linux only; other targets return `UnsafeTracing` before opening a socket.
    ///
    /// # Errors
    /// Returns `Connect` if the server rejects the connection, or
    /// `UnsafeTracing` on targets whose diagnostic behavior is not validated.
    pub async fn connect(mut context: ClientContext, source: SocketAddrV4) -> Result<Self, Error> {
        context.encryption_options.trust_server_certificate = false;
        Self::connect_inner(context, source).await
    }

    /// Connect to a disposable self-signed test server without identity validation.
    /// Linux only; other targets return `UnsafeTracing` before opening a socket.
    #[cfg(test)]
    pub(crate) async fn connect_insecure_test(
        mut context: ClientContext,
        source: SocketAddrV4,
    ) -> Result<Self, Error> {
        context.encryption_options.trust_server_certificate = true;
        context.encryption_options.server_certificate = None;
        Self::connect_inner(context, source).await
    }

    async fn connect_inner(
        mut context: ClientContext,
        source: SocketAddrV4,
    ) -> Result<Self, Error> {
        // Quiet source forks, never dispatcher/global logger suppression.
        if !driver_tracing_is_confidential() {
            return Err(Error::UnsafeTracing);
        }
        context.multi_subnet_failover = Some(false);
        context.connect_retry_count = 0;
        context.encryption_options.mode = EncryptionSetting::Required;
        let address = format!("tcp:{},{}", source.ip(), source.port());
        let client = silent(TdsConnectionProvider {}.create_client(context, &address, None))
            .await
            .map_err(|error| {
                #[cfg(test)]
                crate::live::record_connect_failure(&error);
                let _ = error;
                Error::Connect
            })?;
        Ok(Self {
            client: Some(client),
        })
    }

    /// Begin a read-committed transaction with a TDS transaction-management request.
    ///
    /// # Errors
    /// Returns `Discarded` if the client was already discarded, or
    /// `WriteOutcomeUnknown` if transaction dispatch is not confirmed.
    pub async fn begin(&mut self) -> Result<(), Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        if silent(client.begin_transaction(TransactionIsolationLevel::ReadCommitted, None))
            .await
            .is_err()
        {
            return Err(Error::WriteOutcomeUnknown);
        }
        self.client = Some(client);
        Ok(())
    }

    /// Roll back the transaction with a TDS transaction-management request.
    ///
    /// # Errors
    /// Returns `Discarded` if the client was already discarded, or
    /// `WriteOutcomeUnknown` if rollback dispatch is not confirmed.
    pub async fn rollback(&mut self) -> Result<(), Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        if silent(client.rollback_transaction(None, None))
            .await
            .is_err()
        {
            return Err(Error::WriteOutcomeUnknown);
        }
        self.client = Some(client);
        Ok(())
    }

    /// Commit the active transaction; an unconfirmed result is never retryable.
    ///
    /// # Errors
    /// Returns `Discarded` if the client was already discarded, or
    /// `CommitOutcomeUnknown` if commit dispatch is not confirmed.
    pub async fn commit(&mut self) -> Result<(), Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        if silent(client.commit_transaction(None, None)).await.is_err() {
            return Err(Error::CommitOutcomeUnknown);
        }
        self.client = Some(client);
        Ok(())
    }

    /// Materialize all result sets under DBAPI's aggregate detached-data limits.
    ///
    /// A single driver-decoded cell and the wire buffers may temporarily exceed
    /// the detached byte limit. The entire client is discarded on any failure.
    ///
    /// # Errors
    /// Returns `Discarded`, `WriteOutcomeUnknown`, `Decode`, or `Overflow`;
    /// any failure discards this connection.
    pub async fn fetch_bounded(
        &mut self,
        sql: &str,
        parameters: Vec<RpcParameter>,
        limit: RowLimit,
    ) -> Result<Vec<Vec<Row>>, Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        let result = silent(async {
            let mut state = client
                .execute_sp_executesql(sql.to_owned(), parameters, ())
                .await
                .map_err(|_| Error::WriteOutcomeUnknown)?;
            let mut sets = Vec::new();
            let mut row_count = 0usize;
            let mut bytes = 0usize;
            loop {
                match state {
                    StatementResult::Rows => {
                        if sets.len() >= limit.max_sets() {
                            return Err(Error::Overflow);
                        }
                        let metadata = client.get_metadata().clone();
                        let mut rows = Vec::new();
                        while let Some(values) = client
                            .next_row()
                            .await
                            .map_err(|_| Error::WriteOutcomeUnknown)?
                        {
                            row_count = row_count.checked_add(1).ok_or(Error::Overflow)?;
                            if row_count > limit.rows() || values.len() != metadata.len() {
                                return Err(if values.len() == metadata.len() {
                                    Error::Overflow
                                } else {
                                    Error::Decode
                                });
                            }
                            let mut columns = Vec::with_capacity(values.len());
                            for (value, column) in values.into_iter().zip(&metadata) {
                                let name_bytes = column.column_name.len();
                                let remaining =
                                    limit.bytes().checked_sub(bytes).ok_or(Error::Overflow)?;
                                if name_bytes > remaining {
                                    return Err(Error::Overflow);
                                }
                                bytes += name_bytes;
                                let value =
                                    decode_value(value, column.data_type, limit.bytes() - bytes)?;
                                let size = match &value {
                                    Value::Null(_) => 0,
                                    Value::Bool(_) => 1,
                                    Value::I64(_) | Value::F64(_) => 8,
                                    Value::Text(text) => text.len(),
                                    Value::Bytes(data) => data.len(),
                                };
                                bytes = bytes.checked_add(size).ok_or(Error::Overflow)?;
                                if bytes > limit.bytes() {
                                    return Err(Error::Overflow);
                                }
                                columns.push((column.column_name.clone(), value));
                            }
                            rows.push(Row::new(columns).map_err(|_| Error::Decode)?);
                        }
                        sets.push(rows);
                    }
                    StatementResult::NoRows { .. } => {}
                    StatementResult::End => break,
                }
                state = client
                    .advance()
                    .await
                    .map_err(|_| Error::WriteOutcomeUnknown)?;
            }
            Ok(sets)
        })
        .await;
        match result {
            Ok(sets) => {
                self.client = Some(client);
                Ok(sets)
            }
            Err(Error::Overflow | Error::Decode) => Err(Error::WriteOutcomeUnknown),
            Err(error) => Err(error),
        }
    }

    /// Execute original SQL with named RPC values and buffer each result set.
    ///
    /// A dropped or failed future drops the owned connection, rather than
    /// returning a potentially desynchronized connection to the caller.
    ///
    /// # Errors
    /// Returns `Discarded` if this probe has no client, or
    /// `WriteOutcomeUnknown` if dispatch or result draining fails.
    pub async fn query(
        &mut self,
        sql: &str,
        parameters: Vec<RpcParameter>,
    ) -> Result<Vec<Vec<Vec<ColumnValues>>>, Error> {
        let mut client = self.client.take().ok_or(Error::Discarded)?;
        let result = silent(async {
            let mut state = client
                .execute_sp_executesql(sql.to_owned(), parameters, ())
                .await?;
            let mut sets = Vec::new();
            loop {
                match state {
                    StatementResult::Rows => {
                        let mut rows = Vec::new();
                        while let Some(row) = client.next_row().await? {
                            rows.push(row);
                        }
                        sets.push(rows);
                    }
                    StatementResult::NoRows { .. } => {}
                    StatementResult::End => break,
                }
                state = client.advance().await?;
            }
            Ok::<_, mssql_tds::error::Error>(sets)
        })
        .await;
        match result {
            Ok(sets) => {
                self.client = Some(client);
                Ok(sets)
            }
            Err(_) => Err(Error::WriteOutcomeUnknown),
        }
    }
}

fn driver_tracing_is_confidential() -> bool {
    cfg!(target_os = "linux")
}

fn decode_value(
    value: ColumnValues,
    sql_type: TdsDataType,
    remaining: usize,
) -> Result<Value, Error> {
    let value = match value {
        ColumnValues::Null => Value::Null(match sql_type {
            TdsDataType::Bit | TdsDataType::BitN => DbType::Bool,
            TdsDataType::Int1
            | TdsDataType::Int2
            | TdsDataType::Int4
            | TdsDataType::Int8
            | TdsDataType::IntN => DbType::I64,
            TdsDataType::Flt4 | TdsDataType::Flt8 | TdsDataType::FltN => DbType::F64,
            TdsDataType::NVarChar
            | TdsDataType::NChar
            | TdsDataType::BigVarChar
            | TdsDataType::BigChar
            | TdsDataType::VarChar
            | TdsDataType::Char
            | TdsDataType::Text
            | TdsDataType::NText => DbType::Text,
            TdsDataType::BigVarBinary
            | TdsDataType::BigBinary
            | TdsDataType::VarBinary
            | TdsDataType::Binary
            | TdsDataType::Image => DbType::Bytes,
            _ => return Err(Error::Decode),
        }),
        ColumnValues::Bit(value) => Value::Bool(value),
        ColumnValues::TinyInt(value) => Value::I64(i64::from(value)),
        ColumnValues::SmallInt(value) => Value::I64(i64::from(value)),
        ColumnValues::Int(value) => Value::I64(i64::from(value)),
        ColumnValues::BigInt(value) => Value::I64(value),
        ColumnValues::Real(value) if value.is_finite() => Value::F64(f64::from(value)),
        ColumnValues::Float(value) if value.is_finite() => Value::F64(value),
        ColumnValues::Bytes(data) if data.len() <= remaining => Value::Bytes(data),
        ColumnValues::Bytes(_) => return Err(Error::Overflow),
        ColumnValues::String(text) => {
            let (raw, encoding) = text.into_parts();
            let encoding = encoding.resolved_encoding().ok_or(Error::Decode)?;
            let (decoded, errors) = encoding.decode_without_bom_handling(&raw);
            if errors {
                return Err(Error::Decode);
            }
            if decoded.len() > remaining {
                return Err(Error::Overflow);
            }
            Value::Text(decoded.into_owned())
        }
        _ => return Err(Error::Decode),
    };
    Ok(value)
}
