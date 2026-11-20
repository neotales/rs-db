use std::fmt::Write as _;

use neotales_dbapi::{DbType, Parameter, QueryOptions, RowLimit, Session, Value};
use neotales_dbapi_clickhouse::ClickHouseSession;
use neotales_migration::{Applied, Migration, Migrations, PersistedHistory};

use crate::{
    DirtyDirection, DirtyStage, DirtyState, Error, HistoryReason, LedgerState, ModuleState,
    optional_text, required_text,
};

const HISTORY_MARKER: &str = "neotales-migration-clickhouse/ledger/v1/history";
const META_MARKER: &str = "neotales-migration-clickhouse/ledger/v1/meta";
const HISTORY_ENGINE_FULL: &str = "ReplacingMergeTree(revision, is_deleted) ORDER BY (module, version) SETTINGS index_granularity = 8192";
const META_ENGINE_FULL: &str =
    "ReplacingMergeTree(revision) ORDER BY module SETTINGS index_granularity = 8192";

struct Column {
    name: &'static str,
    data_type: &'static str,
    primary: bool,
}

const fn column(name: &'static str, data_type: &'static str, primary: bool) -> Column {
    Column {
        name,
        data_type,
        primary,
    }
}

const HISTORY_COLUMNS: &[Column] = &[
    column("module", "String", true),
    column("version", "UInt64", true),
    column("checksum", "String", false),
    column("revision", "UInt64", false),
    column("is_deleted", "UInt8", false),
];

const META_COLUMNS: &[Column] = &[
    column("module", "String", true),
    column("fingerprint", "String", false),
    column("barrier_version", "Nullable(UInt64)", false),
    column("barrier_checksum", "Nullable(String)", false),
    column("dirty_version", "Nullable(UInt64)", false),
    column("dirty_checksum", "Nullable(String)", false),
    column("dirty_direction", "Nullable(String)", false),
    column("dirty_stage", "Nullable(String)", false),
    column("revision", "UInt64", false),
];

struct TableInfo {
    engine: String,
    engine_full: String,
    sorting_key: String,
    partition_key: String,
    comment: String,
    primary_key: String,
}

struct ModuleMeta {
    fingerprint: String,
    barrier: Option<Applied>,
    dirty: Option<DirtyState>,
}

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

    pub(crate) async fn bootstrap(&self, session: &mut ClickHouseSession) -> Result<(), Error> {
        let history = self.table_info(session, &self.table).await?;
        let meta = self.table_info(session, &self.meta).await?;
        match (history, meta) {
            (None, None) => {
                self.create(session).await?;
                if !self.verify_present(session).await? {
                    return Err(Error::History(HistoryReason::PartialLedger));
                }
                Ok(())
            }
            (Some(history), Some(meta)) => {
                Self::verify_table_info(
                    &history,
                    HISTORY_MARKER,
                    "module, version",
                    HISTORY_ENGINE_FULL,
                )?;
                Self::verify_table_info(&meta, META_MARKER, "module", META_ENGINE_FULL)?;
                self.verify_columns(session, &self.table, HISTORY_COLUMNS)
                    .await?;
                self.verify_columns(session, &self.meta, META_COLUMNS).await
            }
            _ => Err(Error::History(HistoryReason::PartialLedger)),
        }
    }

    pub(crate) async fn verify_present(
        &self,
        session: &mut ClickHouseSession,
    ) -> Result<bool, Error> {
        let history = self.table_info(session, &self.table).await?;
        let meta = self.table_info(session, &self.meta).await?;
        match (history, meta) {
            (None, None) => Ok(false),
            (Some(history), Some(meta)) => {
                Self::verify_table_info(
                    &history,
                    HISTORY_MARKER,
                    "module, version",
                    HISTORY_ENGINE_FULL,
                )?;
                Self::verify_table_info(&meta, META_MARKER, "module", META_ENGINE_FULL)?;
                self.verify_columns(session, &self.table, HISTORY_COLUMNS)
                    .await?;
                self.verify_columns(session, &self.meta, META_COLUMNS)
                    .await?;
                Ok(true)
            }
            _ => Err(Error::History(HistoryReason::PartialLedger)),
        }
    }

    async fn table_info(
        &self,
        session: &mut ClickHouseSession,
        table: &str,
    ) -> Result<Option<TableInfo>, Error> {
        let rows = session
            .fetch_all_async(
                "SELECT engine, engine_full, sorting_key, partition_key, comment, primary_key FROM system.tables \
                 WHERE database = {p1:String} AND name = {p2:String}",
                QueryOptions::new()
                    .bind_all(&[text(&self.database), text(table)])
                    .limit(limit(1, 8192)),
            )
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        Ok(Some(TableInfo {
            engine: required_text(row, "engine")?,
            engine_full: required_text(row, "engine_full")?,
            sorting_key: required_text(row, "sorting_key")?,
            partition_key: required_text(row, "partition_key")?,
            comment: required_text(row, "comment")?,
            primary_key: required_text(row, "primary_key")?,
        }))
    }

    fn verify_table_info(
        info: &TableInfo,
        marker: &str,
        primary_key: &str,
        engine_full: &str,
    ) -> Result<(), Error> {
        if info.comment != marker || info.engine != "ReplacingMergeTree" {
            return Err(Error::History(HistoryReason::UnownedLedger));
        }
        if info.primary_key != primary_key
            || info.sorting_key != primary_key
            || !info.partition_key.is_empty()
            || info.engine_full != engine_full
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        Ok(())
    }

    async fn verify_columns(
        &self,
        session: &mut ClickHouseSession,
        table: &str,
        expected: &[Column],
    ) -> Result<(), Error> {
        let rows = session
            .fetch_all_async(
                "SELECT name, type, position, default_kind, toInt64(is_in_primary_key) AS is_in_primary_key \
                 FROM system.columns WHERE database = {p1:String} AND table = {p2:String} \
                 ORDER BY position",
                QueryOptions::new()
                    .bind_all(&[text(&self.database), text(table)])
                    .limit(limit(expected.len() + 1, 65_536)),
            )
            .await
            .map_err(Error::Database)?;
        if rows.len() != expected.len() {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        for (index, (row, column)) in rows.iter().zip(expected.iter()).enumerate() {
            let name = required_text(row, "name")?;
            let data_type = required_text(row, "type")?;
            let position = row.get::<i64>("position").map_err(Error::Database)?;
            let default_kind = required_text(row, "default_kind")?;
            let primary = row
                .get::<i64>("is_in_primary_key")
                .map_err(Error::Database)?;
            if name != column.name
                || data_type != column.data_type
                || position != i64::try_from(index + 1).map_err(|_| Error::Configuration)?
                || !default_kind.is_empty()
                || primary != i64::from(column.primary)
            {
                return Err(Error::History(HistoryReason::IncompatibleLedger));
            }
        }
        Ok(())
    }

    async fn create(&self, session: &mut ClickHouseSession) -> Result<(), Error> {
        let history = &self.history_sql;
        let meta = &self.meta_sql;
        let history_ddl = format!(
            "CREATE TABLE {history} (\
               module String,\
               version UInt64,\
               checksum String,\
               revision UInt64,\
               is_deleted UInt8\
             ) ENGINE=ReplacingMergeTree(revision, is_deleted)\
               ORDER BY (module, version) COMMENT '{HISTORY_MARKER}'"
        );
        let meta_ddl = format!(
            "CREATE TABLE {meta} (\
               module String,\
               fingerprint String,\
               barrier_version Nullable(UInt64),\
               barrier_checksum Nullable(String),\
               dirty_version Nullable(UInt64),\
               dirty_checksum Nullable(String),\
               dirty_direction Nullable(String),\
               dirty_stage Nullable(String),\
               revision UInt64\
             ) ENGINE=ReplacingMergeTree(revision)\
               ORDER BY module COMMENT '{META_MARKER}'"
        );
        session
            .execute_no_count(&history_ddl, QueryOptions::new())
            .await
            .map_err(Error::Database)?;
        session
            .execute_no_count(&meta_ddl, QueryOptions::new())
            .await
            .map_err(Error::Database)
    }

    pub(crate) async fn read_module(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        expected_fingerprint: Option<&str>,
    ) -> Result<Option<LedgerState>, Error> {
        let Some(meta) = self.read_meta(session, module).await? else {
            // FINAL hides deletion tombstones. Without metadata even a hidden physical row
            // is orphaned history, not a new module that can safely be registered.
            let rows = session
                .fetch_all_async(
                    &format!(
                        "SELECT toInt64(1) AS present FROM {} \
                         WHERE module = {{p1:String}} LIMIT 1",
                        self.history_sql
                    ),
                    QueryOptions::new().bind(text(module)).limit(limit(1, 4096)),
                )
                .await
                .map_err(Error::Database)?;
            if !rows.is_empty() {
                return Err(Error::History(HistoryReason::InconsistentRows));
            }
            return Ok(None);
        };
        if let Some(expected) = expected_fingerprint
            && meta.fingerprint != expected
        {
            return Err(Error::History(HistoryReason::BindingMismatch));
        }
        let applied = self.read_history(session, module).await?;
        Ok(Some(LedgerState {
            persisted: PersistedHistory {
                applied,
                binding_fingerprint: meta.fingerprint,
                barrier: meta.barrier,
            },
            dirty: meta.dirty,
        }))
    }

    async fn read_meta(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
    ) -> Result<Option<ModuleMeta>, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT fingerprint, barrier_version, barrier_checksum, \
                            dirty_version, dirty_checksum, dirty_direction, dirty_stage \
                     FROM {} FINAL WHERE module = {{p1:String}}",
                    self.meta_sql
                ),
                QueryOptions::new()
                    .bind(text(module))
                    .limit(limit(1, 65_536)),
            )
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let fingerprint = required_text(row, "fingerprint")?;
        let barrier_version = row
            .get::<Option<i64>>("barrier_version")
            .map_err(Error::Database)?;
        let barrier_checksum = optional_text(row, "barrier_checksum")?;
        let barrier = match (barrier_version, barrier_checksum) {
            (Some(version), Some(checksum)) => Some(Applied {
                version: positive(version)?,
                checksum,
            }),
            (None, None) => None,
            _ => return Err(Error::History(HistoryReason::InconsistentRows)),
        };
        let dirty_version = row
            .get::<Option<i64>>("dirty_version")
            .map_err(Error::Database)?;
        let dirty_checksum = optional_text(row, "dirty_checksum")?;
        let dirty_direction = optional_text(row, "dirty_direction")?;
        let dirty_stage = optional_text(row, "dirty_stage")?;
        let dirty = match (dirty_version, dirty_checksum, dirty_direction, dirty_stage) {
            (Some(version), Some(checksum), Some(direction), Some(stage)) => Some(DirtyState {
                direction: DirtyDirection::parse(&direction)
                    .ok_or(Error::History(HistoryReason::InconsistentRows))?,
                stage: DirtyStage::parse(&stage)
                    .ok_or(Error::History(HistoryReason::InconsistentRows))?,
                version: positive(version)?,
                checksum,
            }),
            (None, None, None, None) => None,
            _ => return Err(Error::History(HistoryReason::InconsistentRows)),
        };
        Ok(Some(ModuleMeta {
            fingerprint,
            barrier,
            dirty,
        }))
    }

    async fn read_history(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
    ) -> Result<Vec<Applied>, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT version, checksum, toInt64(is_deleted) AS is_deleted FROM {} FINAL \
                     WHERE module = {{p1:String}} ORDER BY version",
                    self.history_sql
                ),
                QueryOptions::new()
                    .bind(text(module))
                    .limit(limit(1_000_000, 16 * 1024 * 1024)),
            )
            .await
            .map_err(Error::Database)?;
        let mut applied = Vec::new();
        for row in rows {
            let version = positive(row.get::<i64>("version").map_err(Error::Database)?)?;
            let deleted = row.get::<i64>("is_deleted").map_err(Error::Database)?;
            match deleted {
                0 => {
                    if applied
                        .last()
                        .is_some_and(|previous: &Applied| previous.version >= version)
                    {
                        return Err(Error::History(HistoryReason::InconsistentRows));
                    }
                    applied.push(Applied {
                        version,
                        checksum: required_text(&row, "checksum")?,
                    });
                }
                1 => {}
                _ => return Err(Error::History(HistoryReason::InconsistentRows)),
            }
        }
        Ok(applied)
    }

    pub(crate) async fn register(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        fingerprint: &str,
    ) -> Result<(), Error> {
        let revision = self.next_revision(session, module).await?;
        self.write_meta(
            session,
            module,
            &ModuleMeta {
                fingerprint: fingerprint.to_owned(),
                barrier: None,
                dirty: None,
            },
            revision,
        )
        .await
    }

    pub(crate) async fn reject_untracked(
        &self,
        session: &mut ClickHouseSession,
        managed_objects: &[&str],
    ) -> Result<(), Error> {
        if managed_objects.is_empty() {
            return Ok(());
        }
        let mut sql = String::from(
            "SELECT name FROM system.tables WHERE database = {p1:String} AND name IN (",
        );
        let mut parameters = vec![text(&self.database)];
        for (index, object) in managed_objects.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            write!(&mut sql, "{{p{}:String}}", index + 2).expect("write to String");
            parameters.push(text(object));
        }
        sql.push(')');
        let rows = session
            .fetch_all_async(
                &sql,
                QueryOptions::new()
                    .bind_all(&parameters)
                    .limit(limit(managed_objects.len().max(1), 65_536)),
            )
            .await
            .map_err(Error::Database)?;
        if rows.is_empty() {
            Ok(())
        } else {
            Err(Error::UntrackedObjects)
        }
    }

    pub(crate) async fn mark_dirty(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        fingerprint: &str,
        direction: DirtyDirection,
        version: u64,
        checksum: &str,
    ) -> Result<(), Error> {
        let mut meta = self
            .read_meta(session, module)
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        meta.fingerprint = fingerprint.to_owned();
        meta.dirty = Some(DirtyState {
            direction,
            stage: DirtyStage::Started,
            version,
            checksum: checksum.to_owned(),
        });
        let revision = self.next_revision(session, module).await?;
        self.write_meta(session, module, &meta, revision).await
    }

    pub(crate) async fn mark_stage(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        fingerprint: &str,
        direction: DirtyDirection,
        version: u64,
        stage: DirtyStage,
    ) -> Result<(), Error> {
        let mut meta = self
            .read_meta(session, module)
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        let Some(dirty) = meta.dirty.as_mut() else {
            return Err(Error::History(HistoryReason::InconsistentRows));
        };
        if dirty.direction != direction || dirty.version != version {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        dirty.stage = stage;
        meta.fingerprint = fingerprint.to_owned();
        let revision = self.next_revision(session, module).await?;
        self.write_meta(session, module, &meta, revision).await
    }

    pub(crate) async fn clear_dirty(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        fingerprint: &str,
        direction: DirtyDirection,
        version: u64,
    ) -> Result<(), Error> {
        let mut meta = self
            .read_meta(session, module)
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        let Some(dirty) = meta.dirty.as_ref() else {
            return Err(Error::History(HistoryReason::InconsistentRows));
        };
        if dirty.direction != direction || dirty.version != version {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        meta.dirty = None;
        meta.fingerprint = fingerprint.to_owned();
        let revision = self.next_revision(session, module).await?;
        self.write_meta(session, module, &meta, revision).await
    }

    pub(crate) async fn append(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        _fingerprint: &str,
        migration: &Migration,
    ) -> Result<(), Error> {
        let revision = self.next_revision(session, module).await?;
        self.insert_history(
            session,
            module,
            migration.version(),
            migration.checksum(),
            revision,
            false,
        )
        .await
    }

    pub(crate) async fn remove(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        _fingerprint: &str,
        version: u64,
    ) -> Result<(), Error> {
        let revision = self.next_revision(session, module).await?;
        self.insert_history(session, module, version, "", revision, true)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_history(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        version: u64,
        checksum: &str,
        revision: u64,
        deleted: bool,
    ) -> Result<(), Error> {
        let version = i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?;
        let revision = i64::try_from(revision).map_err(|_| Error::InvalidDefinitions)?;
        let deleted = i64::from(deleted);
        session
            .execute_no_count(
                &format!(
                    "INSERT INTO {} (module, version, checksum, revision, is_deleted) \
                     VALUES ({{p1:String}}, {{p2:UInt64}}, {{p3:String}}, {{p4:UInt64}}, {{p5:UInt8}})",
                    self.history_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    integer(version),
                    text(checksum),
                    integer(revision),
                    integer(deleted),
                ]),
            )
            .await
            .map_err(Error::Database)
    }

    pub(crate) async fn ensure_barrier(
        &self,
        session: &mut ClickHouseSession,
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
        let mut meta = self
            .read_meta(session, module)
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        match (&expected, &meta.barrier) {
            (Some(expected), Some(current)) if expected == current => Ok(()),
            (None, None) => Ok(()),
            (Some(_), None) => {
                meta.barrier = expected;
                meta.fingerprint = fingerprint.to_owned();
                let revision = self.next_revision(session, module).await?;
                self.write_meta(session, module, &meta, revision).await
            }
            _ => Err(Error::History(HistoryReason::InconsistentRows)),
        }
    }

    async fn next_revision(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
    ) -> Result<u64, Error> {
        let meta = self.max_revision(session, &self.meta_sql, module).await?;
        let history = self
            .max_revision(session, &self.history_sql, module)
            .await?;
        let current = meta.max(history);
        current.checked_add(1).ok_or(Error::InvalidDefinitions)
    }

    async fn max_revision(
        &self,
        session: &mut ClickHouseSession,
        table: &str,
        module: &str,
    ) -> Result<u64, Error> {
        let rows = session
            .fetch_all_async(
                &format!(
                    "SELECT ifNull(max(revision), 0) AS revision FROM {table} \
                     WHERE module = {{p1:String}}"
                ),
                QueryOptions::new().bind(text(module)).limit(limit(1, 4096)),
            )
            .await
            .map_err(Error::Database)?;
        let value = rows
            .first()
            .ok_or(Error::History(HistoryReason::InconsistentRows))?
            .get::<i64>("revision")
            .map_err(Error::Database)?;
        u64::try_from(value).map_err(|_| Error::History(HistoryReason::InconsistentRows))
    }

    async fn write_meta(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        meta: &ModuleMeta,
        revision: u64,
    ) -> Result<(), Error> {
        let revision = i64::try_from(revision).map_err(|_| Error::InvalidDefinitions)?;
        let (barrier_version, barrier_checksum) = match &meta.barrier {
            Some(barrier) => (
                Value::I64(i64::try_from(barrier.version).map_err(|_| Error::InvalidDefinitions)?),
                Value::Text(barrier.checksum.clone()),
            ),
            None => (Value::Null(DbType::I64), Value::Null(DbType::Text)),
        };
        let (dirty_version, dirty_checksum, dirty_direction, dirty_stage) = match &meta.dirty {
            Some(dirty) => (
                Value::I64(i64::try_from(dirty.version).map_err(|_| Error::InvalidDefinitions)?),
                Value::Text(dirty.checksum.clone()),
                Value::Text(dirty.direction.as_str().to_owned()),
                Value::Text(dirty.stage.as_str().to_owned()),
            ),
            None => (
                Value::Null(DbType::I64),
                Value::Null(DbType::Text),
                Value::Null(DbType::Text),
                Value::Null(DbType::Text),
            ),
        };
        let parameters = [
            text(module),
            text(&meta.fingerprint),
            Parameter::new(DbType::I64, barrier_version).map_err(Error::Database)?,
            Parameter::new(DbType::Text, barrier_checksum).map_err(Error::Database)?,
            Parameter::new(DbType::I64, dirty_version).map_err(Error::Database)?,
            Parameter::new(DbType::Text, dirty_checksum).map_err(Error::Database)?,
            Parameter::new(DbType::Text, dirty_direction).map_err(Error::Database)?,
            Parameter::new(DbType::Text, dirty_stage).map_err(Error::Database)?,
            integer(revision),
        ];
        session
            .execute_no_count(
                &format!(
                    "INSERT INTO {} (module, fingerprint, barrier_version, barrier_checksum, \
                     dirty_version, dirty_checksum, dirty_direction, dirty_stage, revision) \
                     VALUES ({{p1:String}}, {{p2:String}}, {{p3:Nullable(UInt64)}}, \
                     {{p4:Nullable(String)}}, {{p5:Nullable(UInt64)}}, {{p6:Nullable(String)}}, \
                     {{p7:Nullable(String)}}, {{p8:Nullable(String)}}, {{p9:UInt64}})",
                    self.meta_sql
                ),
                QueryOptions::new().bind_all(&parameters),
            )
            .await
            .map_err(Error::Database)
    }

    pub(crate) fn snapshot(module: &str, state: &LedgerState) -> ModuleState {
        let head = state.persisted.applied.last().and_then(|applied| {
            i64::try_from(applied.version)
                .ok()
                .map(|version| (version, applied.checksum.clone()))
        });
        ModuleState {
            module: module.to_owned(),
            fingerprint: state.persisted.binding_fingerprint.clone(),
            count: i64::try_from(state.persisted.applied.len()).unwrap_or(i64::MAX),
            head,
            dirty: state.dirty.clone(),
        }
    }
}

fn positive(value: i64) -> Result<u64, Error> {
    if value > 0 {
        u64::try_from(value).map_err(|_| Error::History(HistoryReason::InconsistentRows))
    } else {
        Err(Error::History(HistoryReason::InconsistentRows))
    }
}

fn qualified(database: &str, table: &str) -> String {
    format!("`{database}`.`{table}`")
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
