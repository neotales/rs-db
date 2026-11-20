use crate::{
    Error, HistoryReason, ModuleState, integer, limit, nullable_integer, nullable_text, qualified,
    text,
};
use neotales_dbapi::{Parameter, QueryOptions, Transaction};
use neotales_migration::{Applied, Migration};
use sha2::{Digest, Sha256};

// Explicit ownership/format declarations. They are integrity markers, not
// authentication. Never add them to an existing table to make verification pass.
const HISTORY_MARKER: &str = "neotales-migration-mssql/ledger/v1/history";
const MODULES_MARKER: &str = "neotales-migration-mssql/ledger/v1/modules";
const COLLATION: &str = "Latin1_General_100_BIN2";

const HISTORY_COLUMNS: &str = "module:nvarchar:510:0:0:notnull:Latin1_General_100_BIN2:plain|version:bigint:8:19:0:notnull:-:plain|checksum:nvarchar:510:0:0:notnull:Latin1_General_100_BIN2:plain|fingerprint:nvarchar:510:0:0:notnull:Latin1_General_100_BIN2:plain|ledger_format:nvarchar:128:0:0:notnull:Latin1_General_100_BIN2:plain";
const MODULES_COLUMNS: &str = "module:nvarchar:510:0:0:notnull:Latin1_General_100_BIN2:plain|fingerprint:nvarchar:510:0:0:notnull:Latin1_General_100_BIN2:plain|applied_count:bigint:8:19:0:notnull:-:plain|head_version:bigint:8:19:0:null:-:plain|head_checksum:nvarchar:510:0:0:null:Latin1_General_100_BIN2:plain|ledger_format:nvarchar:128:0:0:notnull:Latin1_General_100_BIN2:plain";

pub(super) struct Ledger<'a> {
    schema: &'a str,
    table: &'a str,
    modules: String,
    names: String,
    history_sql: String,
    modules_sql: String,
}

struct TableShape {
    columns: String,
    checks: String,
    indexes: String,
    index_columns: String,
}

impl<'a> Ledger<'a> {
    pub(super) fn new(schema: &'a str, table: &'a str) -> Self {
        let modules = format!("{table}_modules");
        let names = constraint_prefix(schema, table);
        Self {
            schema,
            table,
            history_sql: qualified(schema, table),
            modules_sql: qualified(schema, &modules),
            modules,
            names,
        }
    }

    pub(super) async fn bootstrap(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        let history = object_id(transaction, self.schema, self.table).await?;
        let modules = object_id(transaction, self.schema, &self.modules).await?;
        match (history, modules) {
            (None, None) => self.create(transaction).await?,
            (Some(_), Some(_)) => {}
            _ => return Err(Error::History(HistoryReason::PartialLedger)),
        }
        if !is_user_table(transaction, self.schema, self.table).await?
            || !is_user_table(transaction, self.schema, &self.modules).await?
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        // Hold exclusive table locks until commit: non-cooperating DML/DDL cannot
        // race verification or ledger writes. The application lock already
        // serializes cooperating callers.
        transaction
            .fetch_all_async(
                &format!(
                    "SELECT COUNT_BIG(*) AS locked FROM {} WITH (TABLOCKX, HOLDLOCK);\
                     SELECT COUNT_BIG(*) AS locked FROM {} WITH (TABLOCKX, HOLDLOCK)",
                    self.history_sql, self.modules_sql
                ),
                QueryOptions::new().limit(
                    neotales_dbapi::RowLimit::with_max_sets(2, 128, 2).expect("static limits"),
                ),
            )
            .await
            .map_err(Error::Database)?;
        self.verify_tables(transaction).await
    }

    pub(super) async fn verify_tables(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        self.verify_table(transaction, &self.history_sql, &self.history_shape())
            .await?;
        self.verify_table(transaction, &self.modules_sql, &self.modules_shape())
            .await
    }

    fn history_shape(&self) -> TableShape {
        let names = &self.names;
        TableShape {
            columns: HISTORY_COLUMNS.to_owned(),
            checks: [
                format!("{names}_h_fmt:5:enabled:trusted:([ledger_format]=N'{HISTORY_MARKER}')"),
                format!("{names}_h_ver:2:enabled:trusted:([version]>(0))"),
            ]
            .join("|"),
            indexes: format!("{names}_h_pk:1:U:P:E:N:0"),
            index_columns: "module:1:0|version:2:0".to_owned(),
        }
    }

    fn modules_shape(&self) -> TableShape {
        let names = &self.names;
        TableShape {
            columns: MODULES_COLUMNS.to_owned(),
            checks: [
                format!("{names}_m_cnt:3:enabled:trusted:([applied_count]>=(0))"),
                format!(
                    "{names}_m_empty:0:enabled:trusted:([applied_count]=(0) AND [head_version] IS NULL AND [head_checksum] IS NULL OR [applied_count]>(0) AND [head_version] IS NOT NULL)"
                ),
                format!("{names}_m_fmt:6:enabled:trusted:([ledger_format]=N'{MODULES_MARKER}')"),
                format!(
                    "{names}_m_head:0:enabled:trusted:([head_version] IS NULL AND [head_checksum] IS NULL OR [head_version] IS NOT NULL AND [head_checksum] IS NOT NULL)"
                ),
            ]
            .join("|"),
            indexes: format!("{names}_m_pk:1:U:P:E:N:0"),
            index_columns: "module:1:0".to_owned(),
        }
    }

    async fn create(&self, transaction: &mut (dyn Transaction<'_> + '_)) -> Result<(), Error> {
        let names = &self.names;
        let ddl = format!(
            r"
CREATE TABLE {history} (
    [module] nvarchar(255) COLLATE {COLLATION} NOT NULL,
    [version] bigint NOT NULL,
    [checksum] nvarchar(255) COLLATE {COLLATION} NOT NULL,
    [fingerprint] nvarchar(255) COLLATE {COLLATION} NOT NULL,
    [ledger_format] nvarchar(64) COLLATE {COLLATION} NOT NULL,
    CONSTRAINT [{names}_h_pk] PRIMARY KEY CLUSTERED ([module], [version]),
    CONSTRAINT [{names}_h_ver] CHECK ([version] > 0),
    CONSTRAINT [{names}_h_fmt] CHECK ([ledger_format] = N'{HISTORY_MARKER}')
);
CREATE TABLE {modules} (
    [module] nvarchar(255) COLLATE {COLLATION} NOT NULL,
    [fingerprint] nvarchar(255) COLLATE {COLLATION} NOT NULL,
    [applied_count] bigint NOT NULL,
    [head_version] bigint NULL,
    [head_checksum] nvarchar(255) COLLATE {COLLATION} NULL,
    [ledger_format] nvarchar(64) COLLATE {COLLATION} NOT NULL,
    CONSTRAINT [{names}_m_pk] PRIMARY KEY CLUSTERED ([module]),
    CONSTRAINT [{names}_m_cnt] CHECK ([applied_count] >= 0),
    CONSTRAINT [{names}_m_head] CHECK (([head_version] IS NULL AND [head_checksum] IS NULL) OR ([head_version] IS NOT NULL AND [head_checksum] IS NOT NULL)),
    CONSTRAINT [{names}_m_empty] CHECK (([applied_count] = 0 AND [head_version] IS NULL AND [head_checksum] IS NULL) OR ([applied_count] > 0 AND [head_version] IS NOT NULL)),
    CONSTRAINT [{names}_m_fmt] CHECK ([ledger_format] = N'{MODULES_MARKER}')
)",
            history = self.history_sql,
            modules = self.modules_sql,
        );
        transaction
            .execute_trusted_script_async(&ddl, QueryOptions::new())
            .await
            .map_err(Error::Database)
    }

    async fn verify_table(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        relation: &str,
        expected: &TableShape,
    ) -> Result<(), Error> {
        let sql = format!(
            r"
WITH target AS (SELECT object_id FROM sys.tables WHERE object_id = OBJECT_ID(N'{relation}'))
SELECT
  (SELECT COUNT(*) FROM target) AS present,
  (SELECT COUNT(*) FROM sys.tables t JOIN target x ON x.object_id = t.object_id
     WHERE t.is_memory_optimized = 0 AND t.temporal_type = 0
       AND t.is_filetable = 0) AS kind_ok,
  (SELECT COUNT(*) FROM sys.foreign_keys f JOIN target x ON x.object_id = f.parent_object_id) AS foreign_keys,
  (SELECT COUNT(*) FROM sys.triggers tr JOIN target x ON x.object_id = tr.parent_id) AS triggers,
  (SELECT COUNT(*) FROM sys.security_predicates sp JOIN target x ON x.object_id = sp.target_object_id) AS policies,
  (SELECT COUNT(*) FROM sys.indexes i JOIN target x ON x.object_id = i.object_id
     WHERE i.index_id <> 1) AS extra_indexes,
  (SELECT COUNT(*) FROM sys.default_constraints dc JOIN target x ON x.object_id = dc.parent_object_id) AS default_constraints,
  (SELECT COUNT(*) FROM sys.columns c JOIN target x ON x.object_id = c.object_id
     WHERE c.default_object_id <> 0) AS default_columns,
  (SELECT COUNT(*) FROM sys.columns c JOIN target x ON x.object_id = c.object_id
     WHERE c.is_identity = 1 OR c.is_sparse = 1 OR c.is_masked = 1
       OR c.generated_always_type <> 0 OR c.encryption_type IS NOT NULL
       OR c.is_computed = 1 OR c.is_hidden = 1) AS special_columns,
  (SELECT STRING_AGG(CONVERT(nvarchar(max), CONCAT(c.name, N':', TYPE_NAME(c.user_type_id), N':',
        CONVERT(nvarchar(11), c.max_length), N':', CONVERT(nvarchar(11), c.precision), N':',
        CONVERT(nvarchar(11), c.scale), N':',
        CASE WHEN c.is_nullable = 1 THEN N'null' ELSE N'notnull' END, N':',
        COALESCE(c.collation_name, N'-'), N':',
        CASE WHEN c.is_computed = 1 THEN N'computed' ELSE N'plain' END)), N'|')
      WITHIN GROUP (ORDER BY c.column_id)
     FROM sys.columns c JOIN target x ON x.object_id = c.object_id) AS columns,
  (SELECT STRING_AGG(CONVERT(nvarchar(max), CONCAT(cc.name, N':',
        CONVERT(nvarchar(11), cc.parent_column_id), N':',
        CASE WHEN cc.is_disabled = 1 THEN N'disabled' ELSE N'enabled' END, N':',
        CASE WHEN cc.is_not_trusted = 1 THEN N'untrusted' ELSE N'trusted' END, N':',
        cc.definition)), N'|') WITHIN GROUP (ORDER BY cc.name)
     FROM sys.check_constraints cc JOIN target x ON x.object_id = cc.parent_object_id) AS checks,
  (SELECT STRING_AGG(CONVERT(nvarchar(max), CONCAT(i.name, N':',
        CONVERT(nvarchar(11), i.type), N':',
        CASE WHEN i.is_unique = 1 THEN N'U' ELSE N'N' END, N':',
        CASE WHEN i.is_primary_key = 1 THEN N'P' ELSE N'N' END, N':',
        CASE WHEN i.is_disabled = 1 THEN N'D' ELSE N'E' END, N':',
        CASE WHEN i.is_hypothetical = 1 THEN N'H' ELSE N'N' END, N':',
        CONVERT(nvarchar(11), i.fill_factor))), N'|') WITHIN GROUP (ORDER BY i.index_id)
     FROM sys.indexes i JOIN target x ON x.object_id = i.object_id) AS indexes,
  (SELECT STRING_AGG(CONVERT(nvarchar(max), CONCAT(c.name, N':',
        CONVERT(nvarchar(11), ic.key_ordinal), N':',
        CONVERT(nvarchar(11), ic.is_included_column))), N'|')
      WITHIN GROUP (ORDER BY ic.index_id, ic.key_ordinal, ic.index_column_id)
     FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id JOIN target x ON x.object_id = ic.object_id) AS index_columns
"
        );
        let rows = transaction
            .fetch_all_async(&sql, QueryOptions::new().limit(limit(1, 8192)))
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Err(Error::History(HistoryReason::PartialLedger));
        };
        let present = row.get::<i64>("present").map_err(Error::Database)?;
        if present != 1 {
            return Err(Error::History(HistoryReason::PartialLedger));
        }
        if row.get::<i64>("kind_ok").map_err(Error::Database)? != 1
            || row.get::<i64>("foreign_keys").map_err(Error::Database)? != 0
            || row.get::<i64>("triggers").map_err(Error::Database)? != 0
            || row.get::<i64>("policies").map_err(Error::Database)? != 0
            || row.get::<i64>("extra_indexes").map_err(Error::Database)? != 0
            || row
                .get::<i64>("default_constraints")
                .map_err(Error::Database)?
                != 0
            || row.get::<i64>("default_columns").map_err(Error::Database)? != 0
            || row.get::<i64>("special_columns").map_err(Error::Database)? != 0
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        let matches = |name: &str, expected: &str| {
            row.get::<Option<String>>(name)
                .map_err(Error::Database)
                .map(|value| value.as_deref() == Some(expected))
        };
        if !matches("columns", &expected.columns)?
            || !matches("checks", &expected.checks)?
            || !matches("indexes", &expected.indexes)?
            || !matches("index_columns", &expected.index_columns)?
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        Ok(())
    }

    pub(super) async fn verify_history(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        let sql = format!(
            r"
SELECT CAST(CASE WHEN NOT EXISTS (
    SELECT 1 FROM {history} h LEFT JOIN {modules} m ON m.module = h.module
    WHERE m.module IS NULL OR h.version <= 0
      OR h.fingerprint <> m.fingerprint
      OR h.ledger_format <> N'{HISTORY_MARKER}'
      OR DATALENGTH(h.checksum) NOT BETWEEN 2 AND 510
      OR CHARINDEX(NCHAR(0), h.checksum) > 0
      OR DATALENGTH(RTRIM(h.checksum)) <> DATALENGTH(h.checksum)
      OR DATALENGTH(h.fingerprint) NOT BETWEEN 2 AND 510
      OR CHARINDEX(NCHAR(0), h.fingerprint) > 0
      OR DATALENGTH(RTRIM(h.fingerprint)) <> DATALENGTH(h.fingerprint)
) AND NOT EXISTS (
    SELECT 1 FROM {modules} m
    LEFT JOIN (SELECT module, COUNT_BIG(*) AS n, MAX(version) AS head
        FROM {history} GROUP BY module) a ON a.module = m.module
    LEFT JOIN {history} h ON h.module = m.module AND h.version = a.head
    WHERE m.ledger_format <> N'{MODULES_MARKER}'
      OR DATALENGTH(m.module) NOT BETWEEN 2 AND 510
      OR CHARINDEX(NCHAR(0), m.module) > 0
      OR DATALENGTH(RTRIM(m.module)) <> DATALENGTH(m.module)
      OR DATALENGTH(m.fingerprint) NOT BETWEEN 2 AND 510
      OR CHARINDEX(NCHAR(0), m.fingerprint) > 0
      OR DATALENGTH(RTRIM(m.fingerprint)) <> DATALENGTH(m.fingerprint)
      OR (m.head_checksum IS NOT NULL AND (
          DATALENGTH(m.head_checksum) NOT BETWEEN 2 AND 510
          OR CHARINDEX(NCHAR(0), m.head_checksum) > 0
          OR DATALENGTH(RTRIM(m.head_checksum)) <> DATALENGTH(m.head_checksum)))
      OR m.applied_count <> COALESCE(a.n, 0)
      OR (m.head_version IS NULL AND a.head IS NOT NULL)
      OR (m.head_version IS NOT NULL AND (a.head IS NULL OR m.head_version <> a.head))
      OR (m.head_checksum IS NULL AND h.checksum IS NOT NULL)
      OR (m.head_checksum IS NOT NULL AND (h.checksum IS NULL OR m.head_checksum <> h.checksum))
) THEN 1 ELSE 0 END AS bit) AS valid
",
            history = self.history_sql,
            modules = self.modules_sql,
        );
        let rows = transaction
            .fetch_all_async(&sql, QueryOptions::new().limit(limit(1, 64)))
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Err(Error::History(HistoryReason::InconsistentRows));
        };
        if !row.get::<bool>("valid").map_err(Error::Database)? {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        Ok(())
    }

    pub(super) async fn register(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        if let Some(state) = self.module_state(transaction, module, fingerprint).await? {
            return Ok(state);
        }
        self.reject_untracked(transaction, managed_objects).await?;
        self.insert_module(transaction, module, fingerprint, 0, None, None)
            .await?;
        Ok(ModuleState {
            module: module.to_owned(),
            fingerprint: fingerprint.to_owned(),
            count: 0,
            head: None,
        })
    }

    pub(super) async fn module_state(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
    ) -> Result<Option<ModuleState>, Error> {
        let rows = transaction
            .fetch_all_async(
                &format!(
                    "SELECT fingerprint, applied_count, head_version, head_checksum FROM {} WHERE module = @P1",
                    self.modules_sql
                ),
                QueryOptions::new()
                    .bind(text(module))
                    .limit(limit(1, 4096)),
            )
            .await
            .map_err(Error::Database)?;
        if let Some(row) = rows.first() {
            if row.get::<String>("fingerprint").map_err(Error::Database)? != fingerprint {
                return Err(Error::History(HistoryReason::BindingMismatch));
            }
            return Ok(Some(ModuleState {
                module: module.to_owned(),
                fingerprint: fingerprint.to_owned(),
                count: row.get("applied_count").map_err(Error::Database)?,
                head: row
                    .get::<Option<i64>>("head_version")
                    .map_err(Error::Database)?
                    .zip(row.get("head_checksum").map_err(Error::Database)?),
            }));
        }
        Ok(None)
    }

    pub(super) async fn reject_untracked(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        managed_objects: &[(&str, &str)],
    ) -> Result<(), Error> {
        for (schema, object) in managed_objects {
            let rows = transaction
                .fetch_all_async(
                    &format!("SELECT OBJECT_ID(N'{}') AS id", qualified(schema, object)),
                    QueryOptions::new().limit(limit(1, 64)),
                )
                .await
                .map_err(Error::Database)?;
            let present = rows
                .first()
                .ok_or_else(|| {
                    Error::Database(neotales_dbapi::DbError::new(
                        neotales_dbapi::ErrorKind::Decode,
                    ))
                })?
                .get::<Option<i64>>("id")
                .map_err(Error::Database)?;
            if present.is_some() {
                return Err(Error::UntrackedObjects);
            }
        }
        Ok(())
    }

    pub(super) async fn read_applied(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        count: i64,
        definition_count: usize,
    ) -> Result<Vec<Applied>, Error> {
        let count =
            usize::try_from(count).map_err(|_| Error::History(HistoryReason::InconsistentRows))?;
        if count > definition_count {
            return Err(Error::Migration(neotales_migration::Error::UnknownApplied));
        }
        let bytes = count
            .max(1)
            .checked_mul(512)
            .ok_or(Error::InvalidDefinitions)?;
        let rows = transaction
            .fetch_all_async(
                &format!(
                    "SELECT version, checksum FROM {} WHERE module = @P1 ORDER BY version",
                    self.history_sql
                ),
                QueryOptions::new()
                    .bind(text(module))
                    .limit(limit(count.max(1), bytes)),
            )
            .await
            .map_err(Error::Database)?;
        rows.iter()
            .map(|row| {
                Ok(Applied {
                    version: u64::try_from(row.get::<i64>("version").map_err(Error::Database)?)
                        .map_err(|_| Error::History(HistoryReason::InconsistentRows))?,
                    checksum: row.get("checksum").map_err(Error::Database)?,
                })
            })
            .collect()
    }

    pub(super) async fn append(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        migration: &Migration,
    ) -> Result<(), Error> {
        require_one_row(
            affected_rows(
                transaction,
                &format!(
                    "INSERT INTO {} (module, version, checksum, fingerprint, ledger_format) VALUES (@P1,@P2,@P3,@P4,@P5)",
                    self.history_sql
                ),
                &[
                    text(module),
                    integer(
                        i64::try_from(migration.version())
                            .map_err(|_| Error::InvalidDefinitions)?,
                    ),
                    text(migration.checksum()),
                    text(fingerprint),
                    text(HISTORY_MARKER),
                ],
            )
            .await?,
        )
    }

    pub(super) async fn remove(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        version: u64,
    ) -> Result<(), Error> {
        require_one_row(
            affected_rows(
                transaction,
                &format!(
                    "DELETE FROM {} WHERE module = @P1 AND version = @P2",
                    self.history_sql
                ),
                &[
                    text(module),
                    integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                ],
            )
            .await?,
        )
    }

    pub(super) async fn write_head(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        applied: &[Applied],
    ) -> Result<(), Error> {
        require_one_row(
            affected_rows(
                transaction,
                &format!("DELETE FROM {} WHERE module = @P1", self.modules_sql),
                &[text(module)],
            )
            .await?,
        )?;
        let head = applied.last();
        self.insert_module(
            transaction,
            module,
            fingerprint,
            i64::try_from(applied.len()).map_err(|_| Error::InvalidDefinitions)?,
            head.map(|row| i64::try_from(row.version).map_err(|_| Error::InvalidDefinitions))
                .transpose()?,
            head.map(|row| row.checksum.as_str()),
        )
        .await
    }

    async fn insert_module(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        count: i64,
        head_version: Option<i64>,
        head_checksum: Option<&str>,
    ) -> Result<(), Error> {
        require_one_row(
            affected_rows(
                transaction,
                &format!(
                    "INSERT INTO {} (module, fingerprint, applied_count, head_version, head_checksum, ledger_format) VALUES (@P1,@P2,@P3,@P4,@P5,@P6)",
                    self.modules_sql
                ),
                &[
                    text(module),
                    text(fingerprint),
                    integer(count),
                    nullable_integer(head_version),
                    nullable_text(head_checksum),
                    text(MODULES_MARKER),
                ],
            )
            .await?,
        )
    }
}

async fn affected_rows(
    transaction: &mut (dyn Transaction<'_> + '_),
    sql: &str,
    parameters: &[Parameter],
) -> Result<i64, Error> {
    let rows = transaction
        .fetch_all_async(
            &format!("{sql}; SELECT CAST(@@ROWCOUNT AS bigint) AS affected"),
            QueryOptions::new()
                .bind_all(parameters)
                .limit(limit(1, 256)),
        )
        .await
        .map_err(Error::Database)?;
    rows.first()
        .ok_or_else(|| {
            Error::Database(neotales_dbapi::DbError::new(
                neotales_dbapi::ErrorKind::Decode,
            ))
        })?
        .get::<i64>("affected")
        .map_err(Error::Database)
}

fn require_one_row(affected: i64) -> Result<(), Error> {
    if affected != 1 {
        return Err(Error::History(HistoryReason::InconsistentRows));
    }
    Ok(())
}

async fn object_id(
    transaction: &mut (dyn Transaction<'_> + '_),
    schema: &str,
    object: &str,
) -> Result<Option<i64>, Error> {
    let rows = transaction
        .fetch_all_async(
            &format!("SELECT OBJECT_ID(N'{}') AS id", qualified(schema, object)),
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    rows.first()
        .ok_or_else(|| {
            Error::Database(neotales_dbapi::DbError::new(
                neotales_dbapi::ErrorKind::Decode,
            ))
        })?
        .get::<Option<i64>>("id")
        .map_err(Error::Database)
}

async fn is_user_table(
    transaction: &mut (dyn Transaction<'_> + '_),
    schema: &str,
    object: &str,
) -> Result<bool, Error> {
    let rows = transaction
        .fetch_all_async(
            &format!(
                "SELECT OBJECT_ID(N'{}', N'U') AS id",
                qualified(schema, object)
            ),
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    Ok(rows
        .first()
        .ok_or_else(|| {
            Error::Database(neotales_dbapi::DbError::new(
                neotales_dbapi::ErrorKind::Decode,
            ))
        })?
        .get::<Option<i64>>("id")
        .map_err(Error::Database)?
        .is_some())
}

fn constraint_prefix(schema: &str, table: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"neotales-migration-mssql/constraints/v1\0");
    digest.update((schema.len() as u64).to_be_bytes());
    digest.update(schema.as_bytes());
    digest.update((table.len() as u64).to_be_bytes());
    digest.update(table.as_bytes());
    let hash = digest.finalize();
    let mut prefix = String::from("nmt_");
    for byte in &hash[..8] {
        use std::fmt::Write as _;
        write!(&mut prefix, "{byte:02x}").expect("write to String");
    }
    prefix
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_names_stay_within_sql_server_limits() {
        let schema = "s".repeat(128);
        let table = "t".repeat(120);
        let ledger = Ledger::new(&schema, &table);
        assert_eq!(ledger.names.len(), 20);
        assert!(format!("{}_m_empty", ledger.names).len() <= 128);
        assert_eq!(ledger.modules.len(), 128);
    }
}
