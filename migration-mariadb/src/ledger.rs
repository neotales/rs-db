use neotales_dbapi::{DbType, Parameter, QueryOptions, RowLimit, Session, Value};
use neotales_migration::{Applied, Migration, Migrations, PersistedHistory};

use crate::{DirtyDirection, DirtyStage, DirtyState, Error, HistoryReason, LedgerState};

const HISTORY_MARKER: &str = "neotales-migration-mariadb/ledger/v1/history";
const META_MARKER: &str = "neotales-migration-mariadb/ledger/v1/meta";
const HISTORY_CHECK: &str = "`version` > 0";
const META_CHECKS: [&str; 4] = [
    "`barrier_version` is null = (`barrier_checksum` is null)",
    "`dirty_version` is null = (`dirty_checksum` is null)",
    "`dirty_version` is null = (`dirty_direction` is null)",
    "`dirty_version` is null = (`dirty_stage` is null)",
];

struct Column {
    name: &'static str,
    data_type: &'static str,
    sql_type: &'static str,
    nullable: &'static str,
    collation: &'static str,
    max_length: i64,
}

const fn column(
    name: &'static str,
    data_type: &'static str,
    sql_type: &'static str,
    nullable: &'static str,
    collation: &'static str,
    max_length: i64,
) -> Column {
    Column {
        name,
        data_type,
        sql_type,
        nullable,
        collation,
        max_length,
    }
}

const HISTORY_COLUMNS: &[Column] = &[
    column(
        "module",
        "varchar",
        "varchar(255)",
        "NO",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column("version", "bigint", "bigint(20)", "NO", "", 0),
    column(
        "checksum",
        "varchar",
        "varchar(255)",
        "NO",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column(
        "fingerprint",
        "varchar",
        "varchar(255)",
        "NO",
        "utf8mb4_0900_as_cs",
        255,
    ),
];

const META_COLUMNS: &[Column] = &[
    column(
        "module",
        "varchar",
        "varchar(255)",
        "NO",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column(
        "fingerprint",
        "varchar",
        "varchar(255)",
        "NO",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column("barrier_version", "bigint", "bigint(20)", "YES", "", 0),
    column(
        "barrier_checksum",
        "varchar",
        "varchar(255)",
        "YES",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column("dirty_version", "bigint", "bigint(20)", "YES", "", 0),
    column(
        "dirty_checksum",
        "varchar",
        "varchar(255)",
        "YES",
        "utf8mb4_0900_as_cs",
        255,
    ),
    column(
        "dirty_direction",
        "varchar",
        "varchar(8)",
        "YES",
        "utf8mb4_0900_as_cs",
        8,
    ),
    column(
        "dirty_stage",
        "varchar",
        "varchar(16)",
        "YES",
        "utf8mb4_0900_as_cs",
        16,
    ),
];

pub(crate) struct Ledger {
    database: String,
    table: String,
    meta: String,
    history_sql: String,
    meta_sql: String,
}

impl Ledger {
    pub(crate) fn new(database: &str, table: &str) -> Self {
        let meta = format!("{table}_meta");
        let history_sql = qualified(database, table);
        let meta_sql = qualified(database, &meta);
        Self {
            database: database.to_owned(),
            table: table.to_owned(),
            meta,
            history_sql,
            meta_sql,
        }
    }

    pub(crate) async fn bootstrap(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let history = self.table_info(session, &self.table).await?;
        let meta = self.table_info(session, &self.meta).await?;
        match (history, meta) {
            (None, None) => {
                self.create(session).await?;
                let history = self
                    .table_info(session, &self.table)
                    .await?
                    .ok_or(Error::History(HistoryReason::PartialLedger))?;
                let meta = self
                    .table_info(session, &self.meta)
                    .await?
                    .ok_or(Error::History(HistoryReason::PartialLedger))?;
                Self::verify_table_info(&history, HISTORY_MARKER)?;
                Self::verify_table_info(&meta, META_MARKER)?;
                self.verify_columns(session).await?;
                self.verify_primary_keys(session).await?;
                self.verify_constraints(session).await
            }
            (Some(history), Some(meta)) => {
                Self::verify_table_info(&history, HISTORY_MARKER)?;
                Self::verify_table_info(&meta, META_MARKER)?;
                self.verify_columns(session).await?;
                self.verify_primary_keys(session).await?;
                self.verify_constraints(session).await
            }
            _ => Err(Error::History(HistoryReason::PartialLedger)),
        }
    }

    pub(crate) async fn verify_present(
        &self,
        session: &mut (dyn Session + '_),
    ) -> Result<bool, Error> {
        let history = self.table_info(session, &self.table).await?;
        let meta = self.table_info(session, &self.meta).await?;
        match (history, meta) {
            (None, None) => Ok(false),
            (Some(history), Some(meta)) => {
                Self::verify_table_info(&history, HISTORY_MARKER)?;
                Self::verify_table_info(&meta, META_MARKER)?;
                self.verify_columns(session).await?;
                self.verify_primary_keys(session).await?;
                self.verify_constraints(session).await?;
                Ok(true)
            }
            _ => Err(Error::History(HistoryReason::PartialLedger)),
        }
    }

    async fn table_info(
        &self,
        session: &mut (dyn Session + '_),
        table: &str,
    ) -> Result<Option<(String, String)>, Error> {
        let rows = session
            .fetch_all_async(
                "SELECT CONVERT(table_name USING utf8mb4) AS table_name, \
                        CONVERT(table_comment USING utf8mb4) AS table_comment, \
                        CONVERT(COALESCE(engine,'') USING utf8mb4) AS engine \
                 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                QueryOptions::new()
                    .bind_all(&[text(&self.database), text(table)])
                    .limit(limit(1, 4096)),
            )
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let comment = row
            .get::<String>("table_comment")
            .map_err(Error::Database)?;
        let engine = row.get::<String>("engine").map_err(Error::Database)?;
        Ok(Some((comment, engine)))
    }

    fn verify_table_info(info: &(String, String), marker: &str) -> Result<(), Error> {
        if info.0 != marker || !info.1.eq_ignore_ascii_case("InnoDB") {
            return Err(Error::History(HistoryReason::UnownedLedger));
        }
        Ok(())
    }

    async fn create(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let history = &self.history_sql;
        let meta = &self.meta_sql;
        let ddl = format!(
            "CREATE TABLE {history} (\
               module VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,\
               version BIGINT NOT NULL,\
               checksum VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,\
               fingerprint VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,\
               PRIMARY KEY (module, version),\
               CHECK (version > 0)\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs COMMENT='{HISTORY_MARKER}';\
             CREATE TABLE {meta} (\
               module VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL PRIMARY KEY,\
               fingerprint VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,\
               barrier_version BIGINT NULL,\
               barrier_checksum VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NULL,\
               dirty_version BIGINT NULL,\
               dirty_checksum VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NULL,\
               dirty_direction VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NULL,\
               dirty_stage VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NULL,\
               CHECK ((barrier_version IS NULL) = (barrier_checksum IS NULL)),\
               CHECK ((dirty_version IS NULL) = (dirty_checksum IS NULL)),\
               CHECK ((dirty_version IS NULL) = (dirty_direction IS NULL)),\
               CHECK ((dirty_version IS NULL) = (dirty_stage IS NULL))\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs COMMENT='{META_MARKER}'"
        );
        session
            .execute_trusted_script_async(&ddl, QueryOptions::new())
            .await
            .map_err(Error::Database)
    }

    async fn verify_columns(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let rows = session
            .fetch_all_async(
                "SELECT CONVERT(table_name USING utf8mb4) AS table_name, \
                        CONVERT(column_name USING utf8mb4) AS column_name, \
                        CONVERT(data_type USING utf8mb4) AS data_type, \
                        CONVERT(column_type USING utf8mb4) AS column_type, \
                        CONVERT(is_nullable USING utf8mb4) AS is_nullable, \
                        CONVERT(COALESCE(collation_name,'') USING utf8mb4) AS collation_name, \
                        CAST(COALESCE(character_maximum_length,0) AS SIGNED) AS max_length, \
                        CASE WHEN column_default IS NULL OR (is_nullable = 'YES' AND column_default = 'NULL') THEN 1 ELSE 0 END AS no_default, \
                        CONVERT(extra USING utf8mb4) AS extra, \
                        CONVERT(COALESCE(generation_expression,'') USING utf8mb4)                             AS generation_expression \
                 FROM information_schema.columns \
                 WHERE table_schema = ? AND table_name IN (?, ?) \
                 ORDER BY table_name, ordinal_position",
                QueryOptions::new().bind_all(&[text(&self.database), text(&self.table), text(&self.meta)]).limit(limit(64, 32_768)),
            )
            .await
            .map_err(Error::Database)?;
        let mut history = Vec::new();
        let mut meta = Vec::new();
        for row in &rows {
            let table: String = row.get("table_name").map_err(Error::Database)?;
            if !row
                .get::<String>("extra")
                .map_err(Error::Database)?
                .is_empty()
                || !row
                    .get::<String>("generation_expression")
                    .map_err(Error::Database)?
                    .is_empty()
                || row.get::<i64>("no_default").map_err(Error::Database)? != 1
            {
                return Err(Error::History(HistoryReason::IncompatibleLedger));
            }
            let column = (
                row.get::<String>("column_name").map_err(Error::Database)?,
                row.get::<String>("data_type").map_err(Error::Database)?,
                row.get::<String>("column_type").map_err(Error::Database)?,
                row.get::<String>("is_nullable").map_err(Error::Database)?,
                row.get::<String>("collation_name")
                    .map_err(Error::Database)?,
                row.get::<i64>("max_length").map_err(Error::Database)?,
            );
            if table == self.table {
                history.push(column);
            } else if table == self.meta {
                meta.push(column);
            } else {
                return Err(Error::History(HistoryReason::IncompatibleLedger));
            }
        }
        verify_shape(&history, HISTORY_COLUMNS)?;
        verify_shape(&meta, META_COLUMNS)?;
        Ok(())
    }

    async fn verify_primary_keys(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let rows = session
            .fetch_all_async(
                "SELECT CONVERT(table_name USING utf8mb4) AS table_name, \
                        CONVERT(column_name USING utf8mb4) AS column_name \
                 FROM information_schema.statistics \
                 WHERE table_schema = ? AND table_name IN (?, ?) AND index_name = 'PRIMARY' \
                 ORDER BY table_name, seq_in_index",
                QueryOptions::new()
                    .bind_all(&[text(&self.database), text(&self.table), text(&self.meta)])
                    .limit(limit(16, 4096)),
            )
            .await
            .map_err(Error::Database)?;
        let mut history = Vec::new();
        let mut meta = Vec::new();
        for row in &rows {
            let table: String = row.get("table_name").map_err(Error::Database)?;
            let column: String = row.get("column_name").map_err(Error::Database)?;
            if table == self.table {
                history.push(column);
            } else if table == self.meta {
                meta.push(column);
            } else {
                return Err(Error::History(HistoryReason::IncompatibleLedger));
            }
        }
        if history != ["module", "version"] || meta != ["module"] {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        Ok(())
    }

    async fn verify_constraints(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let rows = session
            .fetch_all_async(
                "SELECT CONVERT(tc.table_name USING utf8mb4) AS table_name, \
                        CONVERT(tc.constraint_type USING utf8mb4) AS constraint_type, \
                        CONVERT(COALESCE(cc.check_clause,'') USING utf8mb4) AS check_clause \
                 FROM information_schema.table_constraints tc \
                 LEFT JOIN information_schema.check_constraints cc \
                   ON cc.constraint_schema = tc.constraint_schema \
                  AND cc.table_name = tc.table_name \
                  AND cc.constraint_name = tc.constraint_name \
                 WHERE tc.table_schema = ? AND tc.table_name IN (?, ?)",
                QueryOptions::new()
                    .bind_all(&[text(&self.database), text(&self.table), text(&self.meta)])
                    .limit(limit(32, 8192)),
            )
            .await
            .map_err(Error::Database)?;
        let mut history_primary = 0;
        let mut history_checks = Vec::new();
        let mut meta_primary = 0;
        let mut meta_checks = Vec::new();
        for row in &rows {
            let table: String = row.get("table_name").map_err(Error::Database)?;
            let kind: String = row.get("constraint_type").map_err(Error::Database)?;
            let clause: String = row.get("check_clause").map_err(Error::Database)?;
            match (table.as_str(), kind.as_str()) {
                (table, "PRIMARY KEY") if table == self.table => history_primary += 1,
                (table, "CHECK") if table == self.table => history_checks.push(clause),
                (table, "PRIMARY KEY") if table == self.meta => meta_primary += 1,
                (table, "CHECK") if table == self.meta => meta_checks.push(clause),
                (_, "FOREIGN KEY" | "UNIQUE") => {
                    return Err(Error::History(HistoryReason::IncompatibleLedger));
                }
                _ => return Err(Error::History(HistoryReason::IncompatibleLedger)),
            }
        }
        let mut expected_meta = META_CHECKS.to_vec();
        expected_meta.sort_unstable();
        meta_checks.sort_unstable();
        if history_primary != 1
            || history_checks.as_slice() != [HISTORY_CHECK]
            || meta_primary != 1
            || !meta_checks.iter().map(String::as_str).eq(expected_meta)
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        Ok(())
    }

    pub(crate) async fn read_module(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        expected_fingerprint: Option<&str>,
    ) -> Result<Option<LedgerState>, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT fingerprint, barrier_version, barrier_checksum, dirty_version, \
                            dirty_checksum, dirty_direction, dirty_stage \
                     FROM {} WHERE module = ?",
                    self.meta_sql
                ),
                QueryOptions::new().bind(text(module)).limit(limit(1, 4096)),
            )
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            if self.module_has_history(session, module).await? {
                return Err(Error::History(HistoryReason::InconsistentRows));
            }
            return Ok(None);
        };
        let fingerprint: String = row.get("fingerprint").map_err(Error::Database)?;
        if expected_fingerprint.is_some_and(|expected| expected != fingerprint) {
            return Err(Error::History(HistoryReason::BindingMismatch));
        }
        let barrier_version: Option<i64> = row.get("barrier_version").map_err(Error::Database)?;
        let barrier_checksum: Option<String> =
            row.get("barrier_checksum").map_err(Error::Database)?;
        if barrier_version.is_some() != barrier_checksum.is_some() {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        let barrier = match (barrier_version, barrier_checksum) {
            (Some(version), Some(checksum)) => Some(Applied {
                version: positive(version)?,
                checksum,
            }),
            _ => None,
        };
        let dirty_version: Option<i64> = row.get("dirty_version").map_err(Error::Database)?;
        let dirty_checksum: Option<String> = row.get("dirty_checksum").map_err(Error::Database)?;
        let dirty_direction: Option<String> =
            row.get("dirty_direction").map_err(Error::Database)?;
        let dirty_stage: Option<String> = row.get("dirty_stage").map_err(Error::Database)?;
        let dirty_fields = [
            dirty_version.is_some(),
            dirty_checksum.is_some(),
            dirty_direction.is_some(),
            dirty_stage.is_some(),
        ];
        if dirty_fields.iter().any(|present| *present)
            && !dirty_fields.iter().all(|present| *present)
        {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        let dirty = match (dirty_version, dirty_checksum, dirty_direction, dirty_stage) {
            (Some(version), Some(checksum), Some(direction), Some(stage)) => Some(DirtyState {
                direction: DirtyDirection::parse(&direction)
                    .ok_or(Error::History(HistoryReason::InconsistentRows))?,
                stage: DirtyStage::parse(&stage)
                    .ok_or(Error::History(HistoryReason::InconsistentRows))?,
                version: positive(version)?,
                checksum,
            }),
            _ => None,
        };
        let applied = self.read_applied(session, module, &fingerprint).await?;
        if let Some(barrier) = &barrier
            && applied.first() != Some(barrier)
        {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        Ok(Some(LedgerState {
            persisted: PersistedHistory {
                applied,
                binding_fingerprint: fingerprint,
                barrier,
            },
            dirty,
        }))
    }

    async fn read_applied(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        fingerprint: &str,
    ) -> Result<Vec<Applied>, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT version AS version, checksum AS checksum, fingerprint AS fingerprint \
                     FROM {} WHERE module = ? ORDER BY version",
                    self.history_sql
                ),
                QueryOptions::new()
                    .bind(text(module))
                    .limit(limit(65_536, 16_777_216)),
            )
            .await
            .map_err(Error::Database)?;
        let mut applied = Vec::with_capacity(rows.len());
        for row in &rows {
            let version: i64 = row.get("version").map_err(Error::Database)?;
            let checksum: String = row.get("checksum").map_err(Error::Database)?;
            let row_fingerprint: String = row.get("fingerprint").map_err(Error::Database)?;
            if row_fingerprint != fingerprint {
                return Err(Error::History(HistoryReason::InconsistentRows));
            }
            applied.push(Applied {
                version: positive(version)?,
                checksum,
            });
        }
        Ok(applied)
    }

    async fn module_has_history(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
    ) -> Result<bool, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT version FROM {} WHERE module = ? LIMIT 1",
                    self.history_sql
                ),
                QueryOptions::new().bind(text(module)).limit(limit(1, 1024)),
            )
            .await
            .map_err(Error::Database)?;
        Ok(!rows.is_empty())
    }

    pub(crate) async fn reject_untracked(
        &self,
        session: &mut (dyn Session + '_),
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        for object in managed_objects {
            let rows = session
                .fetch_all_async(
                    "SELECT CONVERT(table_name USING utf8mb4) AS table_name \
                     FROM information_schema.tables \
                     WHERE table_schema = ? AND table_name = ? LIMIT 1",
                    QueryOptions::new()
                        .bind_all(&[text(&self.database), text(object)])
                        .limit(limit(1, 1024)),
                )
                .await
                .map_err(Error::Database)?;
            if !rows.is_empty() {
                return Err(Error::UntrackedObjects);
            }
        }
        Ok(())
    }

    pub(crate) async fn register(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        fingerprint: &str,
    ) -> Result<(), Error> {
        if self.module_has_history(session, module).await? {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        let affected = session
            .execute_async(
                &format!(
                    "INSERT INTO {} (module, fingerprint) VALUES (?, ?)",
                    self.meta_sql
                ),
                QueryOptions::new().bind_all(&[text(module), text(fingerprint)]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn mark_dirty(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        direction: DirtyDirection,
        version: u64,
        checksum: &str,
    ) -> Result<(), Error> {
        let affected = session
            .execute_async(
                &format!(
                    "UPDATE {} SET dirty_version=?, dirty_checksum=?, dirty_direction=?, \
                     dirty_stage='started' WHERE module=? AND dirty_version IS NULL",
                    self.meta_sql
                ),
                QueryOptions::new().bind_all(&[
                    integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                    text(checksum),
                    text(direction.as_str()),
                    text(module),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn mark_stage(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        direction: DirtyDirection,
        version: u64,
        stage: DirtyStage,
    ) -> Result<(), Error> {
        let affected = session
            .execute_async(
                &format!(
                    "UPDATE {} SET dirty_stage=? WHERE module=? AND dirty_version=? \
                     AND dirty_direction=?",
                    self.meta_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(stage.as_str()),
                    text(module),
                    integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                    text(direction.as_str()),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn clear_dirty(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        direction: DirtyDirection,
        version: u64,
    ) -> Result<(), Error> {
        let affected = session
            .execute_async(
                &format!(
                    "UPDATE {} SET dirty_version=NULL, dirty_checksum=NULL, \
                     dirty_direction=NULL, dirty_stage=NULL \
                     WHERE module=? AND dirty_version=? AND dirty_direction=?",
                    self.meta_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                    text(direction.as_str()),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn append(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        fingerprint: &str,
        migration: &Migration,
    ) -> Result<(), Error> {
        let affected = session
            .execute_async(
                &format!(
                    "INSERT INTO {} (module, version, checksum, fingerprint) VALUES (?,?,?,?)",
                    self.history_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    integer(
                        i64::try_from(migration.version())
                            .map_err(|_| Error::InvalidDefinitions)?,
                    ),
                    text(migration.checksum()),
                    text(fingerprint),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn remove(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        version: u64,
    ) -> Result<(), Error> {
        let affected = session
            .execute_async(
                &format!(
                    "DELETE FROM {} WHERE module=? AND version=?",
                    self.history_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one(affected)
    }

    pub(crate) async fn ensure_barrier(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        fingerprint: &str,
        migrations: &Migrations,
        applied: &[Applied],
    ) -> Result<(), Error> {
        let expected = applied.first().cloned().filter(|first| {
            migrations
                .validate_state(
                    applied.to_vec(),
                    fingerprint.to_owned(),
                    Some(first.clone()),
                )
                .is_ok()
        });
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT barrier_version AS barrier_version, \
                            barrier_checksum AS barrier_checksum \
                     FROM {} WHERE module=?",
                    self.meta_sql
                ),
                QueryOptions::new().bind(text(module)).limit(limit(1, 2048)),
            )
            .await
            .map_err(Error::Database)?;
        let row = rows
            .first()
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        let current = match (
            row.get::<Option<i64>>("barrier_version")
                .map_err(Error::Database)?,
            row.get::<Option<String>>("barrier_checksum")
                .map_err(Error::Database)?,
        ) {
            (Some(version), Some(checksum)) => Some(Applied {
                version: positive(version)?,
                checksum,
            }),
            (None, None) => None,
            _ => return Err(Error::History(HistoryReason::InconsistentRows)),
        };
        match (expected, current) {
            (Some(expected), None) => {
                let affected = session
                    .execute_async(
                        &format!(
                            "UPDATE {} SET barrier_version=?, barrier_checksum=? \
                             WHERE module=? AND barrier_version IS NULL",
                            self.meta_sql
                        ),
                        QueryOptions::new().bind_all(&[
                            integer(
                                i64::try_from(expected.version)
                                    .map_err(|_| Error::InvalidDefinitions)?,
                            ),
                            text(&expected.checksum),
                            text(module),
                        ]),
                    )
                    .await
                    .map_err(Error::Database)?;
                require_one(affected)
            }
            (Some(expected), Some(current)) if expected == current => Ok(()),
            (None, None) => Ok(()),
            _ => Err(Error::History(HistoryReason::InconsistentRows)),
        }
    }

    pub(crate) fn snapshot(module: &str, state: &LedgerState) -> crate::ModuleState {
        let head = state.persisted.applied.last().and_then(|applied| {
            i64::try_from(applied.version)
                .ok()
                .map(|version| (version, applied.checksum.clone()))
        });
        crate::ModuleState {
            module: module.to_owned(),
            fingerprint: state.persisted.binding_fingerprint.clone(),
            count: i64::try_from(state.persisted.applied.len()).unwrap_or(i64::MAX),
            head,
            dirty: state.dirty.clone(),
        }
    }
}

fn verify_shape(
    actual: &[(String, String, String, String, String, i64)],
    expected: &[Column],
) -> Result<(), Error> {
    if actual.len() != expected.len() {
        return Err(Error::History(HistoryReason::IncompatibleLedger));
    }
    for (actual, expected) in actual.iter().zip(expected) {
        if actual.0 != expected.name
            || actual.1 != expected.data_type
            || actual.2 != expected.sql_type
            || actual.3 != expected.nullable
            || actual.4 != expected.collation
            || actual.5 != expected.max_length
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
    }
    Ok(())
}

fn positive(version: i64) -> Result<u64, Error> {
    if version <= 0 {
        return Err(Error::History(HistoryReason::InconsistentRows));
    }
    u64::try_from(version).map_err(|_| Error::History(HistoryReason::InconsistentRows))
}

fn require_one(affected: u64) -> Result<(), Error> {
    if affected == 1 {
        Ok(())
    } else {
        Err(Error::History(HistoryReason::InconsistentRows))
    }
}

fn qualified(database: &str, relation: &str) -> String {
    format!("`{database}`.`{relation}`")
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("matching text type")
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("matching integer type")
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("positive static limits")
}
