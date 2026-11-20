//! Exclusive, connection-affine transaction ownership, independent of `SQLx`.
use neotales_dbapi::{DbError, ErrorKind, IsolationLevel, Parameter, Row, RowLimit, TxOptions};

use super::{
    Lease, TransactionState, pg,
    typed::{control, driver_error, error},
};

/// Owns the exact lease that began this transaction. Drop discards the connection,
/// never starts detached rollback. Commit/rollback consume this owner and return
/// a lease only after verified transaction closure. No implicit retry.
///
/// Every operation has a finite client deadline equal to the profile ceiling,
/// including preflight and cleanup. The server ceiling remains per statement,
/// not a transaction lifetime or server-work admission limit. Caller cancellation
/// can leave writes unknown; interrupted transactions can never commit or reuse
/// their connection. A leaked owner can still retain idle locks indefinitely.
///
/// This is a separate experimental API, not a dbapi Session/Transaction adapter.
/// Scripts/statements must not manage transaction ownership/savepoints, change
/// transaction characteristics, or override the timeout. SQL is trusted, not a sandbox.
///
/// ```no_run
/// use neotales_dbapi::{DbType, Parameter, TxOptions, Value};
/// use neotales_dbapi_postgres::experimental::Lease;
/// # async fn example(lease: Lease) -> Result<(), Box<dyn std::error::Error>> {
/// let mut transaction = lease.begin(TxOptions::default()).await?;
/// let result = transaction.execute("INSERT INTO jobs(id) VALUES ($1)",
///     &[Parameter::new(DbType::I64, Value::I64(7))?]).await;
/// match result {
///     Ok(_) => transaction.commit().await?.recycle().await?,
///     Err(error) => {
///         // Rollback may fail after uncertainty; consuming it still discards.
///         if let Ok(lease) = transaction.rollback().await { lease.recycle().await?; }
///         return Err(error.into()); // Reconcile unknown writes; never auto-retry.
///     }
/// }
/// # Ok(()) }
/// ```
pub struct Transaction {
    lease: Lease,
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Transaction(..)")
    }
}

impl Lease {
    /// Consumes this lease to begin a transaction on its existing connection.
    /// Defaults are READ COMMITTED, READ WRITE. Serializable/repeatable-read/
    /// read-committed and explicit read-only/read-write are honored.
    /// READ UNCOMMITTED is rejected, rather than silently upgraded by PostgreSQL.
    /// Raw/script-created transactions are not adopted.
    ///
    /// Cancellation, failure, or a lost BEGIN response discards the consumed lease.
    /// The entire idle check and begin share the finite profile client budget.
    ///
    /// # Errors
    /// Returns `Unsupported` for unsupported isolation before dispatch, `Timeout`
    /// on deadline, or sanitized connection/server categories. No transaction
    /// owner is returned without verified BEGIN and Active `ReadyForQuery` status.
    pub async fn begin(mut self, options: TxOptions) -> Result<Transaction, DbError> {
        let isolation = match options.isolation {
            None | Some(IsolationLevel::ReadCommitted) => "READ COMMITTED",
            Some(IsolationLevel::RepeatableRead) => "REPEATABLE READ",
            Some(IsolationLevel::Serializable) => "SERIALIZABLE",
            Some(IsolationLevel::ReadUncommitted) => return Err(error(ErrorKind::Unsupported)),
        };
        let access = if options.read_only.unwrap_or(false) {
            "READ ONLY"
        } else {
            "READ WRITE"
        };
        let sql = format!("BEGIN ISOLATION LEVEL {isolation} {access}");
        let budget = self.pool.lifecycle_timeout();
        let client = self.check().map_err(|_| error(ErrorKind::Connection))?;
        let operation = async {
            control(
                client,
                "SELECT 1",
                pg::CommandTag::Other,
                pg::TransactionStatus::Idle,
            )
            .await?;
            control(
                client,
                &sql,
                pg::CommandTag::Begin,
                pg::TransactionStatus::Active,
            )
            .await
        };
        tokio::time::timeout(budget, operation)
            .await
            .map_err(|_| error(ErrorKind::Timeout))??;
        self.in_flight = false;
        self.transaction = TransactionState::Active;
        Ok(Transaction { lease: self })
    }
}

impl Transaction {
    /// Executes one typed statement on this transaction's connection, with the
    /// profile's finite client deadline. See `Lease::execute` for binding rules.
    /// Known preflight failures restore the internal savepoint; known server
    /// execution failures abort the transaction, allowing only commit/rollback.
    ///
    /// # Errors
    /// Sanitized dbapi categories; an unconfirmed dispatched operation is
    /// `WriteOutcomeUnknown`. Failure before Execute is not a write outcome.
    pub async fn execute(&mut self, sql: &str, parameters: &[Parameter]) -> Result<u64, DbError> {
        let budget = self.lease.pool.lifecycle_timeout();
        self.execute_with_budget(sql, parameters, budget).await
    }

    pub(crate) async fn execute_with_budget(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        budget: std::time::Duration,
    ) -> Result<u64, DbError> {
        self.lease
            .execute_bounded(sql, parameters, Some(budget))
            .await
    }

    /// Fetches detached, hard-bounded rows on this transaction's connection.
    /// The whole preflight/fetch shares the finite profile client deadline.
    /// See `Lease::fetch` for types, byte accounting, and ambiguity semantics.
    ///
    /// # Errors
    /// Sanitized dbapi categories. Bounds/decode interruption before authoritative
    /// completion is `WriteOutcomeUnknown` and discards, never truncated success.
    pub async fn fetch(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
    ) -> Result<Vec<Row>, DbError> {
        let budget = self.lease.pool.lifecycle_timeout();
        self.fetch_with_budget(sql, parameters, limit, budget).await
    }

    pub(crate) async fn fetch_with_budget(
        &mut self,
        sql: &str,
        parameters: &[Parameter],
        limit: RowLimit,
        budget: std::time::Duration,
    ) -> Result<Vec<Row>, DbError> {
        self.lease
            .fetch_bounded(sql, parameters, limit, Some(budget))
            .await
    }

    /// Executes a trusted script, without transaction/timeout control statements.
    /// No SQL-prefix inference is used: transaction-control completion tags or
    /// unexpected final transaction status invalidate and discard the connection.
    ///
    /// # Errors
    /// Sanitized server categories, or `WriteOutcomeUnknown` on unconfirmed work.
    pub async fn execute_trusted_script(&mut self, sql: &str) -> Result<(), DbError> {
        let budget = self.lease.pool.lifecycle_timeout();
        self.script_with_budget(sql, budget).await
    }

    pub(crate) async fn script_with_budget(
        &mut self,
        sql: &str,
        budget: std::time::Duration,
    ) -> Result<(), DbError> {
        self.lease.script_bounded(sql, Some(budget)).await
    }

    /// Commits and consumes the transaction; returns the lease only for a COMMIT
    /// tag followed by Idle `ReadyForQuery`. PostgreSQL's ROLLBACK response to COMMIT
    /// in an aborted transaction is `Cancelled`, never success.
    ///
    /// # Errors
    /// `CommitOutcomeUnknown` only after COMMIT dispatch lacks authoritative
    /// success/rejection; known server commit rejections retain their SQLSTATE
    /// category. Prior interrupted operations prohibit dispatching COMMIT.
    /// On every error or cancellation the connection is discarded.
    pub async fn commit(mut self) -> Result<Lease, DbError> {
        let budget = self.lease.pool.lifecycle_timeout();
        let client = self
            .lease
            .check()
            .map_err(|_| error(ErrorKind::Connection))?;
        let result = tokio::time::timeout(budget, client.batch_complete("COMMIT"))
            .await
            .map_err(|_| error(ErrorKind::CommitOutcomeUnknown))?
            .map_err(|cause| {
                let failure = driver_error(&cause, true);
                if failure.kind() == ErrorKind::WriteOutcomeUnknown {
                    error(ErrorKind::CommitOutcomeUnknown)
                } else {
                    failure
                }
            })?;
        if result.commands != 1 || result.status != pg::TransactionStatus::Idle {
            return Err(error(ErrorKind::CommitOutcomeUnknown));
        }
        if result.tag == pg::CommandTag::Rollback {
            return Err(error(ErrorKind::Cancelled));
        }
        if result.tag != pg::CommandTag::Commit
            || self.lease.transaction == TransactionState::Aborted
        {
            return Err(error(ErrorKind::CommitOutcomeUnknown));
        }
        self.lease.transaction = TransactionState::Autocommit;
        self.lease.in_flight = false;
        Ok(self.lease)
    }

    /// Rolls back and consumes this transaction. A lease is returned only after
    /// a ROLLBACK tag and Idle `ReadyForQuery`, including after a known server error.
    /// Interrupted/uncertain operations cannot be recovered by this method: they
    /// cause discard instead, without waiting for their server work.
    ///
    /// # Errors
    /// Returns `Connection` for invalid state/unconfirmed rollback, or `Timeout`
    /// on the finite profile deadline. Failure/cancellation always discards.
    pub async fn rollback(mut self) -> Result<Lease, DbError> {
        let budget = self.lease.pool.lifecycle_timeout();
        let client = self
            .lease
            .check()
            .map_err(|_| error(ErrorKind::Connection))?;
        tokio::time::timeout(
            budget,
            control(
                client,
                "ROLLBACK",
                pg::CommandTag::Rollback,
                pg::TransactionStatus::Idle,
            ),
        )
        .await
        .map_err(|_| error(ErrorKind::Timeout))??;
        self.lease.transaction = TransactionState::Autocommit;
        self.lease.in_flight = false;
        Ok(self.lease)
    }
}
