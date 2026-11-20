//! Narrow, experimental DBAPI provider. SQL is trusted; no connection is reused.

use std::sync::Arc;

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, DbType, ErrorKind, Parameter, Pool,
    PoolDefaults, QueryOptions, Reader, RowLimit, Session, Transaction, TxOptions, Value,
};

use crate::{Error, ExclusiveLease, ExclusivePool, bind};

const APPLICATION_LOCK_SQL: &str = "DECLARE @r int; EXEC @r = sys.sp_getapplock @Resource = @P1, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = @P2; SELECT @r AS lock_status";

fn db_error(error: Error) -> DbError {
    DbError::new(match error {
        Error::UnsafeTracing => ErrorKind::Unsupported,
        Error::Connect | Error::Discarded => ErrorKind::Connection,
        Error::AcquireTimeout => ErrorKind::Timeout,
        Error::InvalidConfiguration => ErrorKind::InvalidArgument,
        Error::WriteOutcomeUnknown => ErrorKind::WriteOutcomeUnknown,
        Error::CommitOutcomeUnknown => ErrorKind::CommitOutcomeUnknown,
        Error::Overflow => ErrorKind::Overflow,
        Error::Decode => ErrorKind::Decode,
        Error::LockUnavailable => ErrorKind::Busy,
    })
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

fn identifier(value: &str) -> bool {
    let value = value.strip_prefix('#').unwrap_or(value);
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_alphabetic()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        && ![
            "SET", "NOEXEC", "EXEC", "EXECUTE", "BEGIN", "COMMIT", "ROLLBACK", "GO",
        ]
        .iter()
        .any(|word| value.eq_ignore_ascii_case(word))
}

// Exact single-statement forms; no comments, quotes, expressions or trailing tokens.
fn single_write(sql: &str, count: usize) -> Result<(), DbError> {
    let parts: Vec<_> = sql.split_ascii_whitespace().collect();
    let eq = |i: usize, s: &str| parts.get(i).is_some_and(|p| p.eq_ignore_ascii_case(s));
    let valid = (count == 1
        && parts.len() == 5
        && eq(0, "INSERT")
        && eq(1, "INTO")
        && identifier(parts[2])
        && eq(3, "VALUES")
        && eq(4, "(@P1)"))
        || (count == 2
            && parts.len() == 10
            && eq(0, "UPDATE")
            && identifier(parts[1])
            && eq(2, "SET")
            && identifier(parts[3])
            && eq(4, "=")
            && eq(5, "@P1")
            && eq(6, "WHERE")
            && identifier(parts[7])
            && eq(8, "=")
            && eq(9, "@P2"))
        || (count == 1
            && parts.len() == 7
            && eq(0, "DELETE")
            && eq(1, "FROM")
            && identifier(parts[2])
            && eq(3, "WHERE")
            && identifier(parts[4])
            && eq(5, "=")
            && eq(6, "@P1"));
    if valid { Ok(()) } else { Err(unsupported()) }
}

async fn insert(
    lease: &mut ExclusiveLease,
    sql: &str,
    mut options: QueryOptions,
) -> Result<u64, DbError> {
    options.prepare_execute_provider(PoolDefaults::default())?;
    single_write(sql, options.parameters.len())?;
    let params = bind::bind(&options.parameters);
    let future = lease.connection.execute_one(sql, params);
    match options.provider_execute_options().timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), future)
            .await
            .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?
            .map_err(db_error),
        None => future.await.map_err(db_error),
    }
}

/// An experimental DBAPI pool. Every acquired connection is discarded on release.
///
/// Supports bounded fetch, read-committed transactions, and the narrow INSERT,
/// UPDATE and DELETE forms described in the crate README. Extra trigger results
/// or missing counts produce unknown outcome even if the write executed.
/// SQL is trusted; preflight is not a general SQL security boundary.
#[derive(Clone)]
pub struct DirectPool(Arc<ExclusivePool>);

impl std::fmt::Debug for DirectPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("DirectPool").field(&self.0).finish()
    }
}

impl DirectPool {
    /// Wrap a configured exclusive connection owner.
    #[must_use]
    pub fn new(pool: ExclusivePool) -> Self {
        Self(Arc::new(pool))
    }

    /// Begin a transaction and acquire an exclusive transaction-owned application lock.
    ///
    /// `lock_timeout_ms` is only the server-side lock-wait budget (zero is
    /// nonblocking, maximum `i32::MAX`). Pool acquisition has its own configured
    /// deadline; BEGIN and network I/O have no total deadline here. Callers
    /// must supply and enforce any total deadline by cancelling the future.
    /// A timeout or deadlock result maps to Busy, server cancellation to
    /// `Cancelled` and invalid lock arguments to `InvalidArgument`; transport or
    /// unconfirmed response is `WriteOutcomeUnknown`. Failure or interruption
    /// discards the connection and its transaction. The returned handle can
    /// execute the provider's narrow trusted-script subset on that transaction.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid lock input, `Timeout` or `Busy`
    /// for acquisition/lock-wait failure, `Cancelled` for server cancellation,
    /// and `WriteOutcomeUnknown` when the dispatched lock result is uncertain.
    pub async fn begin_locked(
        &self,
        resource: &str,
        lock_timeout_ms: u32,
    ) -> Result<LockedSession, DbError> {
        if resource.is_empty()
            || resource.contains('\0')
            || resource.encode_utf16().count() > 255
            || lock_timeout_ms > i32::MAX as u32
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        let mut lease = self.0.acquire().await.map_err(db_error)?;
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Begin);
        lease.connection.begin().await.map_err(db_error)?;
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Begun);
        #[cfg(test)]
        crate::live::ledger::report_waiter_spid(&mut lease.connection).await;
        let params = [
            Parameter::new(DbType::Text, Value::Text(resource.to_owned()))?,
            Parameter::new(DbType::I64, Value::I64(i64::from(lock_timeout_ms)))?,
        ];
        #[cfg(test)]
        crate::live::lock_event(crate::live::LockEvent::Request);
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::LockRequest);
        let sets = lease
            .connection
            .fetch_bounded(
                APPLICATION_LOCK_SQL,
                bind::bind(&params),
                RowLimit::new(1, 64)?,
            )
            .await
            .map_err(db_error)?;
        if sets.len() != 1 || sets[0].len() != 1 {
            return Err(DbError::new(ErrorKind::WriteOutcomeUnknown));
        }
        let status = sets[0][0]
            .get::<i64>("lock_status")
            .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
        #[cfg(test)]
        crate::live::lock_event(crate::live::LockEvent::Status(status));
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::LockStatus(status));
        match status {
            0 | 1 => {}
            -1 | -3 => return Err(db_error(Error::LockUnavailable)),
            -2 => return Err(DbError::new(ErrorKind::Cancelled)),
            -999 => return Err(DbError::new(ErrorKind::InvalidArgument)),
            _ => return Err(DbError::new(ErrorKind::WriteOutcomeUnknown)),
        }
        Ok(LockedSession { lease })
    }
}

/// Exclusive transaction holding an application lock until completion or drop.
pub struct LockedSession {
    lease: ExclusiveLease,
}

impl std::fmt::Debug for LockedSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LockedSession")
            .field("lease", &self.lease)
            .finish()
    }
}

impl LockedSession {
    // Crate-private provider SQL, exclusively for the ledger. Never exposed as
    // a general escape hatch through the public trusted-script interface.
    pub(crate) async fn fetch_all_async(
        &mut self,
        sql: &str,
        mut options: QueryOptions,
    ) -> Result<Vec<neotales_dbapi::Row>, DbError> {
        let limit = options.prepare_provider(PoolDefaults::default())?;
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Query);
        let future =
            self.lease
                .connection
                .fetch_bounded(sql, bind::bind(&options.parameters), limit);
        let sets = match options.provider_execute_options().timeout_ms {
            Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), future)
                .await
                .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?,
            None => future.await,
        }
        .map_err(db_error)?;
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::QueryComplete);
        Ok(sets.into_iter().flatten().collect())
    }

    pub(crate) async fn ledger_ddl(&mut self, sql: &str) -> Result<(), DbError> {
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::LedgerDdl);
        let result = self
            .lease
            .connection
            .script_batch(sql)
            .await
            .map_err(db_error);
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::LedgerDdlComplete(
            result.is_ok(),
        ));
        result
    }

    /// Require SQL Server major version 16 on this locked transaction.
    ///
    /// This is only a compatibility gate, not ledger or migration validation.
    ///
    /// # Errors
    /// Returns `Unsupported` for other server versions and
    /// `WriteOutcomeUnknown` if the version query cannot be confirmed.
    pub async fn verify_server_version(&mut self) -> Result<(), DbError> {
        const SQL: &str =
            "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int) AS major_version";
        let sets = self
            .lease
            .connection
            .fetch_bounded(SQL, Vec::new(), RowLimit::new(1, 64)?)
            .await
            .map_err(db_error)?;
        if sets.len() != 1 || sets[0].len() != 1 {
            return Err(DbError::new(ErrorKind::WriteOutcomeUnknown));
        }
        let version = sets[0][0]
            .get::<i64>("major_version")
            .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
        if version != 16 {
            return Err(unsupported());
        }
        Ok(())
    }

    /// Preflight a complete script plan before sending any batch on this transaction.
    ///
    /// This low-level primitive does not validate history, checksums, bindings,
    /// dirty state, or ledger shape and is not a migration backend. An error or
    /// cancellation after dispatch discards the connection.
    ///
    /// # Errors
    /// Returns `Unsupported` for invalid scripts and `WriteOutcomeUnknown`
    /// when a dispatched batch does not complete with a confirmed result.
    pub async fn execute_script_plan(
        &mut self,
        scripts: &[&str],
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        options.prepare_script_provider(PoolDefaults::default())?;
        let mut all = Vec::new();
        for sql in scripts {
            all.extend(script_batches(sql)?);
        }
        let work = async {
            for batch in all {
                self.lease
                    .connection
                    .script_batch(&batch)
                    .await
                    .map_err(db_error)?;
            }
            Ok(())
        };
        match options.provider_execute_options().timeout_ms {
            Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), work)
                .await
                .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?,
            None => work.await,
        }
    }

    /// Run a preflighted trusted script on the locked transaction's connection.
    ///
    /// The lock and transaction remain owned by this handle. An interrupted or
    /// failed dispatched batch discards the connection.
    ///
    /// # Errors
    /// Returns `Unsupported` for scripts outside the narrow grammar and
    /// `WriteOutcomeUnknown` for uncertain dispatched batches.
    pub async fn execute_trusted_script_async(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<(), DbError> {
        script(&mut self.lease, sql, options).await
    }

    /// Commit; an unconfirmed commit is reported as unknown.
    ///
    /// # Errors
    /// Returns `CommitOutcomeUnknown` unless the server confirms the commit.
    pub async fn commit(mut self) -> Result<(), DbError> {
        #[cfg(test)]
        crate::live::before_commit_dispatch();
        #[cfg(test)]
        crate::live::lock_event(crate::live::LockEvent::CommitStart);
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::Commit);
        let result = self.lease.connection.commit().await.map_err(db_error);
        #[cfg(test)]
        crate::live::lock_event(crate::live::LockEvent::CommitEnd(result.is_ok()));
        #[cfg(test)]
        crate::live::reconciliation_phase(crate::live::ReconciliationPhase::CommitComplete(
            result.is_ok(),
        ));
        result
    }
    /// Roll back and release the lock.
    ///
    /// # Errors
    /// Returns `WriteOutcomeUnknown` unless the server confirms rollback.
    pub async fn rollback(mut self) -> Result<(), DbError> {
        self.lease.connection.rollback().await.map_err(db_error)
    }
}

impl Pool for DirectPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            trusted_scripts: true,
            ..Capabilities::default()
        }
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move {
            let lease = self.0.acquire().await.map_err(db_error)?;
            Ok(Box::new(DirectSession { lease }) as Box<dyn Session>)
        })
    }
}

struct DirectSession {
    lease: ExclusiveLease,
}

/// Conservative preflight, not a general T-SQL parser. It rejects rather than
/// guessing about constructs whose session effects or placeholders are unclear.
fn preflight(sql: &str, count: usize) -> Result<(), DbError> {
    let bytes = sql.as_bytes();
    let mut seen = vec![false; count];
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'@' => {
                if bytes
                    .get(i..i + 6)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"@@SPID"))
                    && !bytes
                        .get(i + 6)
                        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                {
                    i += 6;
                    continue;
                }
                if bytes.get(i + 1) != Some(&b'P') {
                    return Err(unsupported());
                }
                i += 2;
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if start == i
                    || bytes[start] == b'0'
                    || bytes
                        .get(i)
                        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                {
                    return Err(unsupported());
                }
                let index = sql[start..i].parse::<usize>().map_err(|_| unsupported())?;
                if index == 0 || index > count {
                    return Err(DbError::new(ErrorKind::InvalidArgument));
                }
                seen[index - 1] = true;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                if [
                    "BEGIN", "COMMIT", "ROLLBACK", "SAVE", "SET", "USE", "EXEC", "EXECUTE",
                    "DECLARE", "GO",
                ]
                .iter()
                .any(|v| word.eq_ignore_ascii_case(v))
                {
                    return Err(unsupported());
                }
            }
            c if c.is_ascii_digit() || c.is_ascii_whitespace() || b",;().=*<>+".contains(&c) => {
                i += 1;
            }
            _ => return Err(unsupported()),
        }
    }
    if seen.iter().any(|seen| !seen) {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    Ok(())
}

async fn fetch<'a>(
    lease: &mut ExclusiveLease,
    sql: &str,
    mut options: QueryOptions,
) -> Result<Box<dyn Reader + 'a>, DbError> {
    let limit = options.prepare_provider(PoolDefaults::default())?;
    preflight(sql, options.parameters.len())?;
    let parameters = bind::bind(&options.parameters);
    let future = lease.connection.fetch_bounded(sql, parameters, limit);
    let result = match options.provider_execute_options().timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), future)
            .await
            .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?,
        None => future.await,
    }
    .map_err(db_error)?;
    Ok(Box::new(BufferedReader::from_sets(result)))
}

// This intentionally accepts no quoting or comments. GO is therefore only
// interpreted after rejecting syntax that could hide it inside a lexical construct.
pub(crate) fn script_batches(sql: &str) -> Result<Vec<String>, DbError> {
    if !sql.is_ascii()
        || sql.bytes().any(|c| {
            !(c.is_ascii_alphanumeric() || c.is_ascii_whitespace() || b"_#(),".contains(&c))
        })
    {
        return Err(unsupported());
    }
    let mut batches = Vec::new();
    let mut current = String::new();
    for line in sql.split_inclusive(['\r', '\n']) {
        if line.trim().eq_ignore_ascii_case("GO") {
            if !current.trim().is_empty() {
                batches.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(line);
        }
    }
    if !current.trim().is_empty() {
        batches.push(current);
    }
    for batch in &batches {
        let p: Vec<_> = batch.split_ascii_whitespace().collect();
        let eq = |i: usize, s: &str| p.get(i).is_some_and(|v| v.eq_ignore_ascii_case(s));
        let create = p.len() == 7
            && eq(0, "CREATE")
            && eq(1, "TABLE")
            && identifier(p[2])
            && eq(3, "(")
            && identifier(p[4])
            && eq(5, "BIGINT")
            && eq(6, ")");
        let drop_table = p.len() == 3 && eq(0, "DROP") && eq(1, "TABLE") && identifier(p[2]);
        let insert = p.len() == 5
            && eq(0, "INSERT")
            && eq(1, "INTO")
            && identifier(p[2])
            && eq(3, "VALUES")
            && p[4]
                .strip_prefix('(')
                .and_then(|v| v.strip_suffix(')'))
                .is_some_and(|v| !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit()));
        if !(create || drop_table || insert) {
            return Err(unsupported());
        }
    }
    Ok(batches)
}

async fn script(
    lease: &mut ExclusiveLease,
    sql: &str,
    mut options: QueryOptions,
) -> Result<(), DbError> {
    options.prepare_script_provider(PoolDefaults::default())?;
    let batches = script_batches(sql)?;
    let work = async {
        for batch in batches {
            lease
                .connection
                .script_batch(&batch)
                .await
                .map_err(db_error)?;
        }
        Ok(())
    };
    match options.provider_execute_options().timeout_ms {
        Some(ms) => tokio::time::timeout(std::time::Duration::from_millis(ms), work)
            .await
            .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?,
        None => work.await,
    }
}

impl Session for DirectSession {
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(insert(&mut self.lease, sql, options))
    }
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(fetch(&mut self.lease, sql, options))
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(script(&mut self.lease, sql, options))
    }
    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            if options.isolation.is_some() || options.read_only.is_some() {
                return Err(unsupported());
            }
            self.lease.connection.begin().await.map_err(db_error)?;
            Ok(Box::new(DirectTransaction {
                lease: &mut self.lease,
                finished: false,
            }) as Box<dyn Transaction<'a>>)
        })
    }
}

struct DirectTransaction<'a> {
    lease: &'a mut ExclusiveLease,
    finished: bool,
}

impl Drop for DirectTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.lease.connection.client.take();
        }
    }
}

impl<'session> Transaction<'session> for DirectTransaction<'session> {
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(insert(self.lease, sql, options))
    }
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(fetch(self.lease, sql, options))
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(script(self.lease, sql, options))
    }
    fn commit_async(mut self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            self.lease.connection.commit().await.map_err(db_error)?;
            self.finished = true;
            Ok(())
        })
    }
    fn rollback_async(mut self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            self.lease.connection.rollback().await.map_err(db_error)?;
            self.finished = true;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripts_are_preflighted_whole_before_dispatch() {
        for ending in ["\n", "\r", "\r\n"] {
            let sql =
                format!("CREATE TABLE x ( n BIGINT ){ending}GO{ending}INSERT INTO x VALUES (1)");
            assert_eq!(script_batches(&sql).unwrap().len(), 2);
        }
        assert_eq!(
            script_batches("CREATE TABLE x ( n BIGINT )\nGO\nINSERT INTO x VALUES (1)")
                .unwrap()
                .len(),
            2
        );
        for sql in [
            "CREATE TABLE x ( n BIGINT )\nGO\nSET NOEXEC ON",
            "CREATE TABLE x ( n BIGINT )\nGO 2\nDROP TABLE x",
            "CREATE TABLE x ( n BIGINT )\nGO\nEXEC hidden",
            "CREATE TABLE x ( n BIGINT )\nGO\nCOMMIT",
            "CREATE TABLE x ( n BIGINT )\nGO\nSELECT 'GO'",
        ] {
            assert!(script_batches(sql).is_err());
        }
    }

    #[test]
    fn write_grammar_rejects_control_and_trailing_batches() {
        assert!(single_write("UPDATE tab SET col = @P1 WHERE id = @P2", 2).is_ok());
        assert!(single_write("DELETE FROM tab WHERE id = @P1", 1).is_ok());
        assert!(single_write("INSERT INTO tab VALUES (@P1)", 1).is_ok());
        assert!(single_write("INSERT INTO tab VALUES (@P1)", 0).is_err());
        assert!(single_write("INSERT INTO tab VALUES (@P1)", 2).is_err());
        assert!(single_write("INSERT INTO tab VALUES (@P1); SELECT 1", 1).is_err());
        for sql in [
            "UPDATE tab SET NOEXEC = @P1 WHERE id = @P2",
            "UPDATE tab SET col = @P1 WHERE id = @P2; SET NOEXEC ON",
            "UPDATE tab SET col = @P1 WHERE id = @P2 -- comment",
            "DELETE FROM tab WHERE id = @P1;DELETE FROM tab",
            "DELETE FROM tab WHERE id = @P1 OR 1 = 1",
            "DELETE FROM tab WHERE id = @P1 EXEC x",
        ] {
            assert!(single_write(sql, if sql.starts_with("UPDATE") { 2 } else { 1 }).is_err());
        }
    }

    #[test]
    fn conservative_preflight_requires_every_parameter_and_rejects_opaque_sql() {
        assert!(preflight("SELECT @P1, @P1; SELECT @P2", 2).is_ok());
        assert!(preflight("SELECT @P1", 2).is_err());
        assert!(preflight("SELECT @P2", 1).is_err());
        assert!(preflight("SELECT '@P1'", 1).is_err());
        assert!(preflight("SELECT 1-- comment", 0).is_err());
        assert!(preflight("SELECT 1; COMMIT", 0).is_err());
        assert!(preflight("SELECT @P1extra", 1).is_err());
    }
}
