#![doc = include_str!("../README.md")]
use neotales_dbapi::{
    DbError, DbType, ImmediateTransaction, Parameter, QueryOptions, Row, RowLimit, Transaction,
    Value,
};
use neotales_dbapi_libsql::LibsqlPoolProvider;
use neotales_migration::{
    Applied, Checksum, FlattenManifest, HistoryState, Migrations, PersistedHistory,
};

/// Backend configuration. The history table must be a simple lowercase identifier.
///
/// Use trusted SQL without transaction control or external side effects. This backend talks to a
/// remote libSQL/Turso server through `neotales-dbapi-libsql`. Each operation acquires one
/// exclusive session and begins `BEGIN IMMEDIATE`. Before commit dispatch, cleanup discards the
/// session and attempts rollback on a best-effort basis. If a future is cancelled after commit
/// dispatch, or when the dispatch boundary is uncertain, the commit may have succeeded: reconcile
/// exactly as for an unknown commit result. A commit whose result cannot be established is reported
/// as [`Error::Database`] when the future returns, not as success. Coordinate all migration runners
/// exclusively from before dispatch through complete history inspection, metadata validation,
/// managed-object verification, any retry decision and completion of any retry. If a retry is
/// interrupted or its outcome is unknown, retain coordination through further reconciliation
/// and any subsequent retry. The backend does not lock that whole interval. Compare complete
/// expected pre- and post-operation states, and investigate any
/// mismatch. A committed `down(count = 1)` followed by a blind retry removes two migrations.
/// A server-side busy response
/// is not classified separately by the remote driver.
///
/// Existing ledger tables are adopted by name without ownership or full structural validation.
/// Before every use the caller must ensure both ledger tables are trusted backend-owned objects
/// with the expected schema, constraints and no hostile triggers. A compatible-looking untrusted
/// table is not safe to adopt. See the README for the exact trust and recovery limitations.
#[derive(Clone, Debug)]
pub struct LibsqlBackend {
    table: String,
}

/// Sanitized backend failure; never includes SQL or database values.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The ledger table name is invalid.
    #[error("invalid backend configuration")]
    Configuration,
    /// A managed object already exists without a matching registration or shadows it.
    #[error("managed application object conflicts with migration")]
    UntrackedObjects,
    /// Persisted migration history is inconsistent or does not match definitions.
    #[error("migration history is corrupt or does not match definitions")]
    History,
    /// A database operation failed; a commit may have an unknown outcome.
    #[error("database operation failed")]
    Database,
    /// Migration planning or validation rejected the requested operation.
    #[error("core migration operation rejected history or plan")]
    Core,
}

struct AssertSqlSafe<T>(T);

impl<T: AsRef<str>> From<AssertSqlSafe<T>> for String {
    fn from(sql: AssertSqlSafe<T>) -> Self {
        sql.0.as_ref().to_owned()
    }
}

trait QueryValue {
    fn parameter(self) -> Result<Parameter, DbError>;
}

impl QueryValue for &str {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(DbType::Text, Value::Text(self.to_owned()))
    }
}

impl QueryValue for &String {
    fn parameter(self) -> Result<Parameter, DbError> {
        self.as_str().parameter()
    }
}

impl QueryValue for String {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(DbType::Text, Value::Text(self))
    }
}

impl QueryValue for i64 {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(DbType::I64, Value::I64(self))
    }
}

impl QueryValue for Option<i64> {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(
            DbType::I64,
            self.map_or(Value::Null(DbType::I64), Value::I64),
        )
    }
}

impl QueryValue for Option<String> {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(
            DbType::Text,
            self.map_or(Value::Null(DbType::Text), Value::Text),
        )
    }
}

impl QueryValue for Option<&str> {
    fn parameter(self) -> Result<Parameter, DbError> {
        Parameter::new(
            DbType::Text,
            self.map_or_else(
                || Value::Null(DbType::Text),
                |value| Value::Text(value.to_owned()),
            ),
        )
    }
}

struct Query {
    sql: String,
    parameters: Vec<Parameter>,
    invalid: bool,
}

fn query(sql: impl Into<String>) -> Query {
    Query {
        sql: sql.into(),
        parameters: Vec::new(),
        invalid: false,
    }
}

impl Query {
    fn bind(mut self, value: impl QueryValue) -> Self {
        match value.parameter() {
            Ok(parameter) => self.parameters.push(parameter),
            Err(_) => self.invalid = true,
        }
        self
    }

    fn parameters(&self) -> Result<&[Parameter], Error> {
        if self.invalid {
            Err(Error::Database)
        } else {
            Ok(&self.parameters)
        }
    }

    async fn execute(self, transaction: &mut (dyn Transaction<'_> + '_)) -> Result<u64, Error> {
        let parameters = self.parameters()?;
        transaction
            .execute_async(&self.sql, QueryOptions::new().bind_all(parameters))
            .await
            .map_err(|_| Error::Database)
    }

    async fn fetch_optional(
        self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<Option<Row>, Error> {
        let parameters = self.parameters()?;
        let limit = RowLimit::new(1, usize::MAX).map_err(|_| Error::Database)?;
        transaction
            .fetch_all_async(
                &self.sql,
                QueryOptions::new().bind_all(parameters).limit(limit),
            )
            .await
            .map(|mut rows| rows.pop())
            .map_err(|_| Error::Database)
    }

    async fn fetch_all(
        self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<Vec<Row>, Error> {
        let parameters = self.parameters()?;
        let limit = RowLimit::new(usize::MAX, usize::MAX).map_err(|_| Error::Database)?;
        transaction
            .fetch_all_async(
                &self.sql,
                QueryOptions::new().bind_all(parameters).limit(limit),
            )
            .await
            .map_err(|_| Error::Database)
    }
}

async fn execute_script(
    transaction: &mut (dyn Transaction<'_> + '_),
    sql: &str,
) -> Result<(), Error> {
    transaction
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .map_err(|_| Error::Database)
}

async fn finish<'session>(
    transaction: Box<dyn Transaction<'session> + 'session>,
    result: Result<(), Error>,
) -> Result<(), Error> {
    match result {
        Ok(()) => transaction
            .commit_async()
            .await
            .map_err(|_| Error::Database),
        Err(error) => {
            let _ = transaction.rollback_async().await;
            Err(error)
        }
    }
}

impl LibsqlBackend {
    /// Creates a backend with the given main-database history table.
    /// Existing tables at the derived names must satisfy the trusted ownership, schema and trigger
    /// preconditions on [`Self`]; construction does not inspect or authenticate them.
    ///
    /// # Errors
    /// Returns [`Error::Configuration`] when the table name is not a supported identifier.
    pub fn new(table: impl Into<String>) -> Result<Self, Error> {
        let table = table.into();
        if table.is_empty()
            || table.len() > 63
            || !table.is_ascii()
            || !table
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || !table.as_bytes()[0].is_ascii_lowercase() && table.as_bytes()[0] != b'_'
        {
            return Err(Error::Configuration);
        }
        Ok(Self { table })
    }

    /// Reads and validates complete persisted history and summary metadata for a module.
    ///
    /// `None` means no module is registered and no rows for it exist. Partial ledgers, orphaned
    /// rows, inconsistent summary metadata, mismatched definitions and binding drift fail closed.
    /// This is a point-in-time inspection, not a retry precondition: coordinate all migration
    /// runners exclusively from before dispatch through inspection and completion of any retry.
    /// If a retry is interrupted or its outcome is unknown, retain coordination through further
    /// reconciliation and any subsequent retry. Existing ledger tables must satisfy the trusted
    /// ownership/schema/trigger preconditions on [`Self`].
    ///
    /// # Errors
    /// Returns [`Error::History`] for inconsistent or mismatched history,
    /// [`Error::UntrackedObjects`] for a temporary ledger-name collision, or [`Error::Database`]
    /// for an unsuccessful database operation or transaction cleanup.
    pub async fn inspect(
        &self,
        provider: &LibsqlPoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
    ) -> Result<Option<PersistedHistory>, Error> {
        let mut session = provider
            .acquire_libsql_session()
            .await
            .map_err(|_| Error::Database)?;
        let mut tx = session
            .begin_immediate_async()
            .await
            .map_err(|_| Error::Database)?;
        let result = async {
            let mut present = [false; 2];
            let meta_name = format!("{}_modules", self.table);
            self.reject_temp(&mut *tx, &[&self.table, &meta_name])
                .await?;
            for (index, name) in [self.table.as_str(), meta_name.as_str()]
                .into_iter()
                .enumerate()
            {
                let object =
                    query("SELECT type FROM main.sqlite_master WHERE name=? COLLATE NOCASE")
                        .bind(name)
                        .fetch_optional(&mut *tx)
                        .await?;
                if let Some(row) = object {
                    let kind: String = row.get("type").map_err(|_| Error::History)?;
                    if kind != "table" {
                        return Err(Error::History);
                    }
                    present[index] = true;
                }
            }
            match present {
                [false, false] => Ok(None),
                [true, true] => {
                    let registered = query(AssertSqlSafe(format!(
                        "SELECT 1 FROM {} WHERE module=?",
                        self.meta()
                    )))
                    .bind(module)
                    .fetch_optional(&mut *tx)
                    .await?
                    .is_some();
                    if !registered {
                        let orphaned = query(AssertSqlSafe(format!(
                            "SELECT 1 FROM {} WHERE module=? LIMIT 1",
                            self.quoted()
                        )))
                        .bind(module)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_some();
                        return if orphaned {
                            Err(Error::History)
                        } else {
                            Ok(None)
                        };
                    }
                    let state = self.read(&mut *tx, module, migrations, fingerprint).await?;
                    Ok(Some(state.persisted()))
                }
                _ => Err(Error::History),
            }
        }
        .await;
        let rollback = tx.rollback_async().await.map_err(|_| Error::Database);
        match (result, rollback) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(history), Ok(())) => Ok(history),
        }
    }

    /// Applies all pending migrations atomically after checking untracked objects.
    /// Fresh modules, including those using a baseline, must use this method.
    ///
    /// Cancellation after commit dispatch, or at an uncertain dispatch boundary, may mean the
    /// transaction already committed. For cancellation or an unknown commit, coordinate all
    /// migration runners exclusively from before dispatch through [`Self::inspect`], verification
    /// of managed objects and side effects, any retry decision and completion of any retry.
    /// If a retry is interrupted or its outcome is unknown, retain coordination through further
    /// reconciliation and any subsequent retry. Compare the complete history
    /// and internally validated metadata with the expected pre- and post-operation states, not
    /// only the head. Consider retrying only if the complete pre-operation state matches and
    /// object verification supports it; investigate any other or ambiguous state rather than
    /// retrying.
    /// `managed_objects` must name all managed objects that indicate an existing
    /// schema; an empty list is valid only when there truly are none. Temporary
    /// objects with these names are rejected even for registered modules.
    ///
    /// # Errors
    /// Returns a sanitized error on invalid history, conflicting objects, planning,
    /// database, or commit failure. Reconcile the ledger after an uncertain commit.
    /// Existing ledger tables must satisfy the ownership/schema/trigger preconditions on [`Self`].
    pub async fn up(
        &self,
        provider: &LibsqlPoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_libsql_session()
            .await
            .map_err(|_| Error::Database)?;
        let mut tx = session
            .begin_immediate_async()
            .await
            .map_err(|_| Error::Database)?;
        let result = self
            .up_inner(&mut *tx, module, migrations, fingerprint, managed_objects)
            .await;
        finish(tx, result).await
    }

    async fn up_inner(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        module: &str,
        ms: &Migrations,
        fp: &str,
        objects: &[&str],
    ) -> Result<(), Error> {
        self.reject_temp(c, objects).await?;
        let table = self.quoted();
        let exists =
            query("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=? COLLATE NOCASE")
                .bind(&self.table)
                .fetch_optional(&mut *c)
                .await
                .map_err(|_| Error::Database)?
                .is_some();
        if !exists {
            query(AssertSqlSafe(format!("CREATE TABLE {table} (module TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, fingerprint TEXT NOT NULL, barrier TEXT, barrier_checksum TEXT, PRIMARY KEY(module,version))"))).execute(&mut *c).await.map_err(|_| Error::Database)?;
            query(AssertSqlSafe(format!("CREATE TABLE {} (module TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, barrier TEXT, barrier_checksum TEXT, applied_count INTEGER NOT NULL, head_version TEXT, head_checksum TEXT)", self.meta()))).execute(&mut *c).await.map_err(|_| Error::Database)?;
        }
        let module_exists = exists
            && query(AssertSqlSafe(format!(
                "SELECT 1 FROM {} WHERE module=?",
                self.meta()
            )))
            .bind(module)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| Error::Database)?
            .is_some();
        let state = if module_exists {
            self.read(c, module, ms, fp).await?
        } else {
            if query(AssertSqlSafe(format!(
                "SELECT 1 FROM {table} WHERE module=? LIMIT 1"
            )))
            .bind(module)
            .fetch_optional(&mut *c)
            .await
            .map_err(|_| Error::Database)?
            .is_some()
            {
                return Err(Error::History);
            }
            self.reject_untracked(c, objects).await?;
            ms.validate_state(Vec::new(), fp.to_owned(), None)
                .map_err(|_| Error::Core)?
        };
        let plans = ms.plan_up(&state, fp).map_err(|_| Error::Core)?;
        if plans.iter().any(|p| p.sql.contains('\0')) {
            return Err(Error::Core);
        }
        for p in plans {
            execute_script(c, &p.sql).await?;
        }
        let next = ms.plan_up(&state, fp).map_err(|_| Error::Core)?;
        let applied = state.applied().to_vec();
        // Persist complete validated history after execution. The definitions provide the checksums.
        let count = applied.len() + next.len();
        let all = ms.as_slice();
        for m in all.iter().take(count).skip(applied.len()) {
            query(AssertSqlSafe(format!("INSERT INTO {table} (module,version,checksum,fingerprint,barrier,barrier_checksum) VALUES (?,?,?,?,NULL,NULL)")))
                .bind(module).bind(i64::try_from(m.version()).map_err(|_| Error::Core)?).bind(m.checksum()).bind(fp)
                .execute(&mut *c).await.map_err(|_| Error::Database)?;
        }
        let persisted_rows: Vec<_> = ms
            .as_slice()
            .iter()
            .take(count)
            .map(|m| Applied {
                version: m.version(),
                checksum: m.checksum().to_owned(),
            })
            .collect();
        let barrier = persisted_rows
            .first()
            .filter(|first| {
                ms.validate_state(
                    persisted_rows.clone(),
                    fp.to_owned(),
                    Some((*first).clone()),
                )
                .is_ok()
            })
            .cloned();
        query(AssertSqlSafe(format!(
            "UPDATE {table} SET barrier=?,barrier_checksum=? WHERE module=?"
        )))
        .bind(barrier.as_ref().map(|a| a.version.to_string()))
        .bind(barrier.as_ref().map(|a| a.checksum.as_str()))
        .bind(module)
        .execute(&mut *c)
        .await
        .map_err(|_| Error::Database)?;
        self.write_meta(c, module, fp, barrier.as_ref(), &persisted_rows)
            .await?;
        Ok(())
    }

    /// Executes a rollback after validating every requested down script before SQL effects.
    /// `managed_objects` must name every managed object referenced by the migration;
    /// matching temporary objects are rejected to prevent shadowing main objects.
    ///
    /// Cancellation after commit dispatch, or at an uncertain dispatch boundary, may mean the
    /// transaction already committed. For cancellation or an unknown commit, coordinate all
    /// migration runners exclusively from before dispatch through [`Self::inspect`], verification
    /// of managed objects and side effects, any retry decision and completion of any retry.
    /// If a retry is interrupted or its outcome is unknown, retain coordination through further
    /// reconciliation and any subsequent retry. Compare complete history and
    /// internally validated metadata with the expected pre- and post-operation states, not only
    /// the head. Consider retrying only if the complete pre-operation state matches and object
    /// verification supports it; investigate any other or ambiguous state. A committed `down` is not idempotent:
    /// blindly repeating `down(count = 1)` after a committed first call removes two migrations.
    /// Existing ledger tables must satisfy the ownership/schema/trigger preconditions on [`Self`].
    ///
    /// # Errors
    /// Returns a sanitized error on invalid history, conflicting objects, planning,
    /// database, or commit failure. Reconcile the ledger after an uncertain commit.
    pub async fn down(
        &self,
        provider: &LibsqlPoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_libsql_session()
            .await
            .map_err(|_| Error::Database)?;
        let mut tx = session
            .begin_immediate_async()
            .await
            .map_err(|_| Error::Database)?;
        let result = async {
            self.reject_temp(&mut *tx, managed_objects).await?;
            let state = self.read(&mut *tx, module, migrations, fingerprint).await?;
            let plans = migrations
                .plan_down(&state, fingerprint, count)
                .map_err(|_| Error::Core)?;
            let remaining_count = state
                .applied()
                .len()
                .checked_sub(plans.len())
                .ok_or(Error::History)?;
            if plans.iter().any(|p| p.sql.contains('\0')) {
                return Err(Error::Core);
            }
            for p in &plans {
                execute_script(&mut *tx, &p.sql).await?;
            }
            let table = self.quoted();
            for p in plans {
                query(AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE module=? AND version=?"
                )))
                .bind(module)
                .bind(i64::try_from(p.version).map_err(|_| Error::Core)?)
                .execute(&mut *tx)
                .await
                .map_err(|_| Error::Database)?;
            }
            self.write_meta(
                &mut *tx,
                module,
                fingerprint,
                state.barrier(),
                &state.applied()[..remaining_count],
            )
            .await?;
            Ok(())
        }
        .await;
        finish(tx, result).await
    }

    /// Replaces exact legacy history with a baseline marker. Fresh modules must
    /// first execute their baseline through [`Self::up`]. `managed_objects` must
    /// name every managed object; temporary name collisions are always rejected.
    ///
    /// Cancellation after commit dispatch, or at an uncertain dispatch boundary, may mean the
    /// transaction already committed. For cancellation or an unknown commit, coordinate all
    /// migration runners exclusively from before dispatch through complete history and metadata
    /// inspection, managed-object verification, any retry decision and completion of any retry.
    /// If a retry is interrupted or its outcome is unknown, retain coordination through further
    /// reconciliation and any subsequent retry.
    /// Existing ledger tables must satisfy the ownership/schema/trigger preconditions on [`Self`].
    ///
    /// # Errors
    /// Returns a sanitized error on invalid history, conflicting objects, planning,
    /// database, or commit failure. Reconcile the ledger after an uncertain commit.
    pub async fn flatten(
        &self,
        provider: &LibsqlPoolProvider,
        module: &str,
        manifest: &FlattenManifest,
        fingerprint: Checksum,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_libsql_session()
            .await
            .map_err(|_| Error::Database)?;
        let mut tx = session
            .begin_immediate_async()
            .await
            .map_err(|_| Error::Database)?;
        let result = async {
            self.reject_temp(&mut *tx, managed_objects).await?;
            let table = self.quoted();
            let exists = query(
                "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=? COLLATE NOCASE",
            )
            .bind(&self.table)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|_| Error::Database)?
            .is_some();
            let registered = exists
                && query(AssertSqlSafe(format!(
                    "SELECT 1 FROM {} WHERE module=?",
                    self.meta()
                )))
                .bind(module)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| Error::Database)?
                .is_some();
            if !registered {
                if exists
                    && query(AssertSqlSafe(format!(
                        "SELECT 1 FROM {table} WHERE module=? LIMIT 1"
                    )))
                    .bind(module)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|_| Error::Database)?
                    .is_some()
                {
                    return Err(Error::History);
                }
                self.reject_untracked(&mut *tx, managed_objects).await?;
                return Err(Error::Core);
            }
            let history = self.read_persisted(&mut *tx, module).await?;
            let assessment = manifest
                .assess(Some(&history), fingerprint.clone())
                .map_err(|_| Error::Core)?;
            query(AssertSqlSafe(format!("DELETE FROM {table} WHERE module=?")))
                .bind(module)
                .execute(&mut *tx)
                .await
                .map_err(|_| Error::Database)?;
            let baseline = assessment.state.applied().first().ok_or(Error::Core)?;
            query(AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?,?,?,?,?)"
            )))
            .bind(module)
            .bind(i64::try_from(baseline.version).map_err(|_| Error::Core)?)
            .bind(&baseline.checksum)
            .bind(&fingerprint)
            .bind(baseline.version.to_string())
            .bind(&baseline.checksum)
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Database)?;
            self.write_meta(
                &mut *tx,
                module,
                &fingerprint,
                Some(baseline),
                std::slice::from_ref(baseline),
            )
            .await?;
            Ok(())
        }
        .await;
        finish(tx, result).await
    }
    fn quoted(&self) -> String {
        format!("main.\"{}\"", self.table)
    }
    fn meta(&self) -> String {
        format!("main.\"{}_modules\"", self.table)
    }
    async fn write_meta(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        barrier: Option<&Applied>,
        applied: &[Applied],
    ) -> Result<(), Error> {
        let version = |a: Option<&Applied>| a.map(|a| a.version.to_string());
        let head = applied.last();
        query(AssertSqlSafe(format!("INSERT INTO {} (module,fingerprint,barrier,barrier_checksum,applied_count,head_version,head_checksum) VALUES (?,?,?,?,?,?,?) ON CONFLICT(module) DO UPDATE SET fingerprint=excluded.fingerprint,barrier=excluded.barrier,barrier_checksum=excluded.barrier_checksum,applied_count=excluded.applied_count,head_version=excluded.head_version,head_checksum=excluded.head_checksum", self.meta())))
            .bind(module).bind(fingerprint).bind(version(barrier)).bind(barrier.map(|a| a.checksum.as_str()))
            .bind(i64::try_from(applied.len()).map_err(|_| Error::Core)?).bind(version(head)).bind(head.map(|a| a.checksum.as_str()))
            .execute(c).await.map_err(|_| Error::Database)?;
        Ok(())
    }
    async fn reject_untracked(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        objects: &[&str],
    ) -> Result<(), Error> {
        for object in objects {
            if query("SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE name=? COLLATE NOCASE AND type IN ('table','view','trigger','index')) AS found")
                .bind(*object)
                .fetch_optional(&mut *c)
                .await
                .map_err(|_| Error::Database)?
                .ok_or(Error::Database)?
                .get::<i64>("found")
                .map_err(|_| Error::Database)?
                == 1
            {
                return Err(Error::UntrackedObjects);
            }
        }
        Ok(())
    }
    async fn reject_temp(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        objects: &[&str],
    ) -> Result<(), Error> {
        for object in objects {
            if query("SELECT EXISTS(SELECT 1 FROM temp.sqlite_master WHERE name=? COLLATE NOCASE AND type IN ('table','view','trigger','index')) AS found")
                .bind(*object)
                .fetch_optional(&mut *c)
                .await
                .map_err(|_| Error::Database)?
                .ok_or(Error::Database)?
                .get::<i64>("found")
                .map_err(|_| Error::Database)?
                == 1
            {
                return Err(Error::UntrackedObjects);
            }
        }
        Ok(())
    }
    async fn read(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        module: &str,
        ms: &Migrations,
        fp: &str,
    ) -> Result<HistoryState, Error> {
        let history = self.read_persisted(c, module).await?;
        if history.binding_fingerprint != fp {
            return Err(Error::History);
        }
        ms.validate_persisted(&history).map_err(|_| Error::History)
    }
    async fn read_persisted(
        &self,
        c: &mut (dyn Transaction<'_> + '_),
        module: &str,
    ) -> Result<PersistedHistory, Error> {
        let table = self.quoted();
        let meta = self.meta();
        let metadata = query(AssertSqlSafe(format!(
            "SELECT fingerprint,barrier,barrier_checksum,applied_count,head_version,head_checksum FROM {meta} WHERE module=?"
        )))
        .bind(module)
        .fetch_optional(&mut *c)
        .await
        .map_err(|_| Error::Database)?
        .ok_or(Error::History)?;
        let binding_fingerprint: String =
            metadata.get("fingerprint").map_err(|_| Error::History)?;
        let barrier_version: Option<String> =
            metadata.get("barrier").map_err(|_| Error::History)?;
        let barrier_checksum: Option<String> = metadata
            .get("barrier_checksum")
            .map_err(|_| Error::History)?;
        let expected_count: i64 = metadata.get("applied_count").map_err(|_| Error::History)?;
        let head_version: Option<String> =
            metadata.get("head_version").map_err(|_| Error::History)?;
        let head_checksum: Option<String> =
            metadata.get("head_checksum").map_err(|_| Error::History)?;
        if barrier_version.is_some() != barrier_checksum.is_some() {
            return Err(Error::History);
        }
        let barrier = match (barrier_version, barrier_checksum) {
            (Some(version), Some(checksum)) => Some(Applied {
                version: version.parse().map_err(|_| Error::History)?,
                checksum,
            }),
            _ => None,
        };
        let rows=query(AssertSqlSafe(format!("SELECT version,checksum,fingerprint,barrier,barrier_checksum FROM {table} WHERE module=? ORDER BY version"))).bind(module).fetch_all(c).await.map_err(|_| Error::Database)?;
        let mut applied = Vec::new();
        for row in rows {
            let version: i64 = row.get("version").map_err(|_| Error::History)?;
            let a = Applied {
                version: u64::try_from(version).map_err(|_| Error::History)?,
                checksum: row.get("checksum").map_err(|_| Error::History)?,
            };
            applied.push(a);
            let f: String = row.get("fingerprint").map_err(|_| Error::History)?;
            if binding_fingerprint != f {
                return Err(Error::History);
            }
            let b: Option<String> = row.get("barrier").map_err(|_| Error::History)?;
            let bc: Option<String> = row.get("barrier_checksum").map_err(|_| Error::History)?;
            if b.is_some() != bc.is_some()
                || b.and_then(|v| v.parse::<u64>().ok()) != barrier.as_ref().map(|a| a.version)
                || bc.as_deref() != barrier.as_ref().map(|a| a.checksum.as_str())
            {
                return Err(Error::History);
            }
        }
        if usize::try_from(expected_count).ok() != Some(applied.len())
            || head_version != applied.last().map(|a| a.version.to_string())
            || head_checksum.as_deref() != applied.last().map(|a| a.checksum.as_str())
        {
            return Err(Error::History);
        }
        Ok(PersistedHistory {
            applied,
            binding_fingerprint,
            barrier,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, LibsqlBackend, finish};
    use neotales_dbapi::{DbError, DbFuture, ErrorKind, QueryOptions, Reader, Transaction};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct FakeTransaction {
        commits: Arc<AtomicUsize>,
        rollbacks: Arc<AtomicUsize>,
        fail_commit: bool,
    }

    impl<'session> Transaction<'session> for FakeTransaction {
        fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
            panic!("unexpected execute")
        }
        fn fetch_async<'a>(
            &'a mut self,
            _: &'a str,
            _: QueryOptions,
        ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
            Box::pin(async { Err(DbError::new(ErrorKind::Connection)) })
        }
        fn execute_trusted_script_async<'a>(
            &'a mut self,
            _: &'a str,
            _: QueryOptions,
        ) -> DbFuture<'a, ()> {
            panic!("unexpected script")
        }
        fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.fail_commit {
                    Err(DbError::new(ErrorKind::CommitOutcomeUnknown))
                } else {
                    Ok(())
                }
            })
        }
        fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn unknown_commit_is_not_reported_as_success_or_retried() {
        let commits = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let tx = FakeTransaction {
            commits: commits.clone(),
            rollbacks: rollbacks.clone(),
            fail_commit: true,
        };
        assert!(matches!(
            finish(Box::new(tx), Ok(())).await,
            Err(Error::Database)
        ));
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(rollbacks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn precommit_error_rolls_back_and_preserves_error() {
        let commits = Arc::new(AtomicUsize::new(0));
        let rollbacks = Arc::new(AtomicUsize::new(0));
        let tx = FakeTransaction {
            commits: commits.clone(),
            rollbacks: rollbacks.clone(),
            fail_commit: false,
        };
        assert!(matches!(
            finish(Box::new(tx), Err(Error::History)).await,
            Err(Error::History)
        ));
        assert_eq!(commits.load(Ordering::SeqCst), 0);
        assert_eq!(rollbacks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn metadata_transport_failure_is_database_not_history() {
        let mut tx = FakeTransaction {
            commits: Arc::new(AtomicUsize::new(0)),
            rollbacks: Arc::new(AtomicUsize::new(0)),
            fail_commit: false,
        };
        let backend = LibsqlBackend::new("migration_log").unwrap();
        assert!(matches!(
            backend.read_persisted(&mut tx, "module").await,
            Err(Error::Database)
        ));
    }
}
