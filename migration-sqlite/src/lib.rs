#![doc = include_str!("../README.md")]
#![allow(missing_docs)]
#![allow(clippy::missing_errors_doc)]
use neotales_dbapi::{
    DbError, DbType, Parameter, QueryOptions, Row, RowLimit, SqlitePool, Transaction, Value,
};
#[cfg(feature = "sqlx-compat")]
use neotales_dbapi_sqlite::SqlitePoolProvider;
use neotales_migration::{
    Applied, Checksum, FlattenManifest, HistoryState, Migrations, PersistedHistory,
};

/// Backend configuration. The history table must be a simple lowercase identifier.
///
/// Use trusted SQL without transaction control or external side effects. Cancellation
/// interrupts SQLite virtual-machine execution and discards the connection, but cannot
/// interrupt a blocked user-defined function, VFS call, or SQLite busy handler.
/// Prompt cancellation is not guaranteed. `busy_timeout_ms` bounds SQLite lock waits.
#[derive(Clone, Debug)]
pub struct SqliteBackend {
    table: String,
    busy_timeout_ms: u64,
}

/// Sanitized backend failure; never includes SQL or database values.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid backend configuration")]
    Configuration,
    #[error("managed application object conflicts with migration")]
    UntrackedObjects,
    #[error("migration history is corrupt or does not match definitions")]
    History,
    #[error("database operation failed")]
    Database,
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

impl SqliteBackend {
    /// Creates a backend with the given main-database history table and busy timeout.
    pub fn new(table: impl Into<String>, busy_timeout_ms: u64) -> Result<Self, Error> {
        let table = table.into();
        if table.is_empty()
            || table.len() > 63
            || !table.is_ascii()
            || !table
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || !table.as_bytes()[0].is_ascii_lowercase() && table.as_bytes()[0] != b'_'
            || busy_timeout_ms == 0
            || busy_timeout_ms > i32::MAX as u64
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            table,
            busy_timeout_ms,
        })
    }

    /// SQLx-provider compatibility overload. See [`Self::up_with_pool`] for the complete contract.
    #[cfg(feature = "sqlx-compat")]
    pub async fn up(
        &self,
        provider: &SqlitePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        self.up_with_pool(provider, module, migrations, fingerprint, managed_objects)
            .await
    }

    /// Applies pending migrations atomically through any driver-neutral SQLite pool.
    ///
    /// The operation acquires one exclusive SQLite session, applies this backend's
    /// `busy_timeout_ms`, and begins `BEGIN IMMEDIATE` on that same session. Existing managed
    /// objects indicate a prior schema and are rejected for an unregistered module; pass every
    /// application table, index, view, and trigger that indicates an initialized schema.
    /// Temporary objects matching any listed managed name are rejected for every module because
    /// they can shadow `main`. An empty list is safe only when the application truly has no
    /// managed objects. Fresh modules, including those with a baseline migration, must use this
    /// method to execute their baseline; do not call `flatten_with_pool` for a fresh module.
    ///
    /// Migration SQL must be trusted and transactional: do not use transaction-control commands,
    /// `ATTACH`/`DETACH`, or external/non-transactional side effects. The migration scripts run
    /// inside the immediate transaction. If an inner operation returns an error before commit,
    /// the backend attempts rollback; a rollback error does not replace the original error. On
    /// success the backend attempts commit. A commit error, cancellation, or other interruption
    /// after dispatch can leave the final transaction outcome unknown (the commit may have
    /// succeeded). Therefore [`Error::Database`] is sanitized and is not proof that changes were
    /// rolled back. Re-open and inspect migration history before deciding whether to retry.
    /// The provider restores its configured busy timeout on the next acquisition after this
    /// method's per-operation override.
    ///
    /// ```no_run
    /// use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};
    /// use neotales_migration::{Migration, Migrations};
    /// use neotales_migration_sqlite::SqliteBackend;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = SqlitePoolProvider::connect(SqliteConfig::new("app.sqlite")).await?;
    /// let migrations = Migrations::new(vec![Migration::new(
    ///     1,
    ///     "create_items",
    ///     "CREATE TABLE items (id INTEGER PRIMARY KEY)",
    ///     None,
    /// )?])?;
    /// SqliteBackend::new("migration_log", 5_000)?
    ///     .up_with_pool(&pool, "items", &migrations, "app-v1", &["items"])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns a sanitized database, history, core-planning, or untracked-object error. No provider
    /// SQL, values, or error text is included.
    pub async fn up_with_pool(
        &self,
        provider: &dyn SqlitePool,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_sqlite_async()
            .await
            .map_err(|_| Error::Database)?;
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms);
        session
            .execute_async(&busy_timeout, QueryOptions::new())
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
            query(AssertSqlSafe(format!("CREATE TABLE {table} (module TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, fingerprint TEXT NOT NULL, barrier INTEGER, barrier_checksum TEXT, PRIMARY KEY(module,version))"))).execute(&mut *c).await.map_err(|_| Error::Database)?;
            query(AssertSqlSafe(format!("CREATE TABLE {} (module TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, barrier INTEGER, barrier_checksum TEXT, applied_count INTEGER NOT NULL, head_version INTEGER, head_checksum TEXT)", self.meta()))).execute(&mut *c).await.map_err(|_| Error::Database)?;
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
        .bind(barrier.as_ref().and_then(|a| i64::try_from(a.version).ok()))
        .bind(barrier.as_ref().map(|a| a.checksum.as_str()))
        .bind(module)
        .execute(&mut *c)
        .await
        .map_err(|_| Error::Database)?;
        self.write_meta(c, module, fp, barrier.as_ref(), &persisted_rows)
            .await?;
        Ok(())
    }

    /// SQLx-provider compatibility overload. See [`Self::down_with_pool`] for the complete contract.
    #[cfg(feature = "sqlx-compat")]
    pub async fn down(
        &self,
        provider: &SqlitePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        self.down_with_pool(
            provider,
            module,
            migrations,
            fingerprint,
            count,
            managed_objects,
        )
        .await
    }

    /// Rolls back verified migrations through any driver-neutral SQLite pool.
    ///
    /// This uses one `BEGIN IMMEDIATE` transaction and applies `busy_timeout_ms` before beginning.
    /// `managed_objects` must list every managed object referenced by the migration; matching
    /// temporary objects are rejected to prevent shadowing `main`. Every requested down script is
    /// validated and planned before any script or history row is changed. Scripts must be trusted
    /// transactional SQL without transaction control, `ATTACH`/`DETACH`, or external side effects.
    /// If an inner operation returns an error before commit, rollback is attempted and the
    /// original error is returned even if rollback also fails. On success commit is attempted. A
    /// commit error, cancellation, or another interruption after dispatch can leave the final
    /// outcome unknown (the commit may have succeeded); [`Error::Database`] is not proof of
    /// rollback. Re-open and inspect migration history before deciding whether to retry. Provider
    /// details are sanitized. The next acquisition restores the provider's configured busy
    /// timeout after the operation override.
    ///
    /// # Errors
    /// Returns a sanitized database, history, core-planning, or untracked-object error.
    pub async fn down_with_pool(
        &self,
        provider: &dyn SqlitePool,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_sqlite_async()
            .await
            .map_err(|_| Error::Database)?;
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms);
        session
            .execute_async(&busy_timeout, QueryOptions::new())
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

    /// SQLx-provider compatibility overload. See [`Self::flatten_with_pool`] for the complete contract.
    #[cfg(feature = "sqlx-compat")]
    pub async fn flatten(
        &self,
        provider: &SqlitePoolProvider,
        module: &str,
        manifest: &FlattenManifest,
        fingerprint: Checksum,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        self.flatten_with_pool(provider, module, manifest, fingerprint, managed_objects)
            .await
    }

    /// Replaces verified migration history with a baseline marker through a driver-neutral pool.
    ///
    /// Flattening applies `busy_timeout_ms` and runs atomically under `BEGIN IMMEDIATE`. The
    /// manifest must exactly describe the complete persisted history being replaced; partial,
    /// orphaned, or mismatched history is rejected. Flatten does not execute the baseline SQL.
    /// Fresh modules must apply their baseline with [`Self::up_with_pool`] first. `managed_objects`
    /// must list every managed table, index, view, and trigger. Matching temporary objects are
    /// always rejected because they can shadow `main`; an empty list is safe only when the
    /// application has no managed objects. This API assumes trusted transactional SQL elsewhere
    /// in the migration history; transaction-control commands, `ATTACH`/`DETACH`, and external
    /// effects are not supported. Before commit, inner errors trigger a rollback attempt; rollback
    /// failure does not replace the original error. A commit error or cancellation after dispatch
    /// may have an unknown final outcome (the commit may have succeeded), so [`Error::Database`]
    /// does not prove rollback. Re-open and inspect migration history before retrying. Provider
    /// details are sanitized. The provider restores its configured busy timeout on the next
    /// acquisition after this override.
    ///
    /// # Errors
    /// Returns a sanitized database, history, core-planning, or untracked-object error.
    pub async fn flatten_with_pool(
        &self,
        provider: &dyn SqlitePool,
        module: &str,
        manifest: &FlattenManifest,
        fingerprint: Checksum,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        let mut session = provider
            .acquire_sqlite_async()
            .await
            .map_err(|_| Error::Database)?;
        let busy_timeout = format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms);
        session
            .execute_async(&busy_timeout, QueryOptions::new())
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
            .bind(i64::try_from(baseline.version).map_err(|_| Error::Core)?)
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
        let version = |a: Option<&Applied>| {
            a.map(|a| i64::try_from(a.version).map_err(|_| Error::Core))
                .transpose()
        };
        let head = applied.last();
        query(AssertSqlSafe(format!("INSERT INTO {} (module,fingerprint,barrier,barrier_checksum,applied_count,head_version,head_checksum) VALUES (?,?,?,?,?,?,?) ON CONFLICT(module) DO UPDATE SET fingerprint=excluded.fingerprint,barrier=excluded.barrier,barrier_checksum=excluded.barrier_checksum,applied_count=excluded.applied_count,head_version=excluded.head_version,head_checksum=excluded.head_checksum", self.meta())))
            .bind(module).bind(fingerprint).bind(version(barrier)?).bind(barrier.map(|a| a.checksum.as_str()))
            .bind(i64::try_from(applied.len()).map_err(|_| Error::Core)?).bind(version(head)?).bind(head.map(|a| a.checksum.as_str()))
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
        .map_err(|_| Error::History)?
        .ok_or(Error::History)?;
        let binding_fingerprint: String =
            metadata.get("fingerprint").map_err(|_| Error::History)?;
        let barrier_version: Option<i64> = metadata.get("barrier").map_err(|_| Error::History)?;
        let barrier_checksum: Option<String> = metadata
            .get("barrier_checksum")
            .map_err(|_| Error::History)?;
        let expected_count: i64 = metadata.get("applied_count").map_err(|_| Error::History)?;
        let head_version: Option<i64> = metadata.get("head_version").map_err(|_| Error::History)?;
        let head_checksum: Option<String> =
            metadata.get("head_checksum").map_err(|_| Error::History)?;
        if barrier_version.is_some() != barrier_checksum.is_some() {
            return Err(Error::History);
        }
        let barrier = match (barrier_version, barrier_checksum) {
            (Some(version), Some(checksum)) => Some(Applied {
                version: u64::try_from(version).map_err(|_| Error::History)?,
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
            let b: Option<i64> = row.get("barrier").map_err(|_| Error::History)?;
            let bc: Option<String> = row.get("barrier_checksum").map_err(|_| Error::History)?;
            if b.is_some() != bc.is_some()
                || b.and_then(|v| u64::try_from(v).ok()) != barrier.as_ref().map(|a| a.version)
                || bc.as_deref() != barrier.as_ref().map(|a| a.checksum.as_str())
            {
                return Err(Error::History);
            }
        }
        if usize::try_from(expected_count).ok() != Some(applied.len())
            || head_version != applied.last().and_then(|a| i64::try_from(a.version).ok())
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
