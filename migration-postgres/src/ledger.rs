use crate::{Error, HistoryReason, ModuleState, limit, qualified, text};
use neotales_dbapi::{QueryOptions, Transaction};
use neotales_migration::{Applied, Migration};

// The marker is an explicit format/ownership declaration, not an authentication
// mechanism. Never add it to an existing relation to make verification succeed.
const HISTORY_MARKER: &str = "neotales-migration-postgres/ledger/v1/history";
const MODULES_MARKER: &str = "neotales-migration-postgres/ledger/v1/modules";
const HISTORY_COLUMNS: &str =
    "module:text:true|version:int8:true|checksum:text:true|fingerprint:text:true";
const MODULES_COLUMNS: &str = "module:text:true|fingerprint:text:true|applied_count:int8:true|head_version:int8:false|head_checksum:text:false";
const HISTORY_CONSTRAINTS: &str = "CHECK ((version > 0))|PRIMARY KEY (module, version)";
const MODULES_CONSTRAINTS: &str = "CHECK (((applied_count = 0) = (head_version IS NULL)))|CHECK (((head_version IS NULL) = (head_checksum IS NULL)))|CHECK ((applied_count >= 0))|PRIMARY KEY (module)";

pub(super) struct Ledger<'a> {
    schema: &'a str,
    table: &'a str,
    modules: String,
    history_sql: String,
    modules_sql: String,
}

impl<'a> Ledger<'a> {
    pub(super) fn new(schema: &'a str, table: &'a str) -> Self {
        let modules = format!("{table}_modules");
        Self {
            schema,
            table,
            history_sql: qualified(schema, table),
            modules_sql: qualified(schema, &modules),
            modules,
        }
    }

    pub(super) async fn bootstrap(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        let history = relation_kind(transaction, self.schema, self.table).await?;
        let modules = relation_kind(transaction, self.schema, &self.modules).await?;
        match (history.as_deref(), modules.as_deref()) {
            (None, None) => self.create(transaction).await?,
            (Some("r"), Some("r")) => {}
            (None, Some(_)) | (Some(_), None) => {
                return Err(Error::History(HistoryReason::PartialLedger));
            }
            _ => return Err(Error::History(HistoryReason::IncompatibleLedger)),
        }
        // Hold table locks until commit: ordinary DML/DDL cannot race verification.
        // SHARE ROW EXCLUSIVE also excludes another (non-advisory) ledger writer.
        transaction
            .execute_trusted_script_async(
                &format!(
                    "LOCK TABLE {}, {} IN SHARE ROW EXCLUSIVE MODE",
                    self.history_sql, self.modules_sql
                ),
                QueryOptions::new(),
            )
            .await
            .map_err(Error::Database)?;
        self.verify_tables(transaction).await
    }

    pub(super) async fn verify_tables(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        self.verify_table(
            transaction,
            self.table,
            HISTORY_MARKER,
            HISTORY_COLUMNS,
            HISTORY_CONSTRAINTS,
        )
        .await?;
        self.verify_table(
            transaction,
            &self.modules,
            MODULES_MARKER,
            MODULES_COLUMNS,
            MODULES_CONSTRAINTS,
        )
        .await
    }

    async fn create(&self, transaction: &mut (dyn Transaction<'_> + '_)) -> Result<(), Error> {
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\";
             CREATE TABLE {} (
                module text COLLATE pg_catalog.\"C\" NOT NULL, version bigint NOT NULL CHECK (version > 0),
                checksum text COLLATE pg_catalog.\"C\" NOT NULL, fingerprint text COLLATE pg_catalog.\"C\" NOT NULL, PRIMARY KEY (module, version));
             CREATE TABLE {} (
                module text COLLATE pg_catalog.\"C\" PRIMARY KEY, fingerprint text COLLATE pg_catalog.\"C\" NOT NULL,
                applied_count bigint NOT NULL CHECK (applied_count >= 0),
                head_version bigint, head_checksum text COLLATE pg_catalog.\"C\",
                CHECK ((head_version IS NULL) = (head_checksum IS NULL)),
                CHECK ((applied_count = 0) = (head_version IS NULL)));
             COMMENT ON TABLE {} IS '{HISTORY_MARKER}';
             COMMENT ON TABLE {} IS '{MODULES_MARKER}'",
            self.schema, self.history_sql, self.modules_sql, self.history_sql, self.modules_sql
        );
        transaction
            .execute_trusted_script_async(&ddl, QueryOptions::new())
            .await
            .map_err(Error::Database)
    }

    async fn verify_table(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        table: &str,
        marker: &str,
        columns: &str,
        constraints: &str,
    ) -> Result<(), Error> {
        let rows = transaction
            .fetch_all_async(
                include_str!("verify_table.sql"),
                QueryOptions::new()
                    .bind_all(&[
                        text(self.schema),
                        text(table),
                        text(marker),
                        text(columns),
                        text(constraints),
                    ])
                    .limit(limit(1, 64)),
            )
            .await
            .map_err(Error::Database)?;
        let Some(row) = rows.first() else {
            return Err(Error::History(HistoryReason::PartialLedger));
        };
        if !row.get::<bool>("marked").map_err(Error::Database)? {
            return Err(Error::History(HistoryReason::UnownedLedger));
        }
        if !row.get::<bool>("owned").map_err(Error::Database)?
            || !row.get::<bool>("valid").map_err(Error::Database)?
        {
            return Err(Error::History(HistoryReason::IncompatibleLedger));
        }
        Ok(())
    }

    pub(super) async fn verify_history(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
    ) -> Result<(), Error> {
        // Aggregate on the server: detached results stay bounded even for a large
        // ledger. This verifies all modules, including orphan rows for other modules.
        let sql = format!(
            "SELECT NOT EXISTS (
                SELECT 1 FROM {} h LEFT JOIN {} m ON m.module = h.module
                WHERE m.module IS NULL OR h.version <= 0
                   OR pg_catalog.octet_length(h.checksum) NOT BETWEEN 1 AND 255
                   OR h.fingerprint <> m.fingerprint
             ) AND NOT EXISTS (
                SELECT 1 FROM {} m
                LEFT JOIN (SELECT module, count(*) AS n, max(version) AS head FROM {} GROUP BY module) a
                    ON a.module = m.module
                LEFT JOIN {} h ON h.module = m.module AND h.version = a.head
                WHERE pg_catalog.octet_length(m.module) NOT BETWEEN 1 AND 255
                   OR pg_catalog.octet_length(m.fingerprint) NOT BETWEEN 1 AND 255
                   OR (m.head_checksum IS NOT NULL
                       AND pg_catalog.octet_length(m.head_checksum) NOT BETWEEN 1 AND 255)
                   OR m.applied_count <> COALESCE(a.n, 0)
                   OR m.head_version IS DISTINCT FROM a.head
                   OR m.head_checksum IS DISTINCT FROM h.checksum
             ) AS valid",
            self.history_sql, self.modules_sql, self.modules_sql, self.history_sql, self.history_sql
        );
        let rows = transaction
            .fetch_all_async(&sql, QueryOptions::new().limit(limit(1, 64)))
            .await
            .map_err(Error::Database)?;
        if !rows[0].get::<bool>("valid").map_err(Error::Database)? {
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
        transaction
            .execute_async(
                &format!(
                    "INSERT INTO {} (module, fingerprint, applied_count) VALUES ($1, $2, 0)",
                    self.modules_sql
                ),
                QueryOptions::new().bind_all(&[text(module), text(fingerprint)]),
            )
            .await
            .map_err(Error::Database)?;
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
                    "SELECT fingerprint, applied_count, head_version, head_checksum
                     FROM {} WHERE module = $1",
                    self.modules_sql
                ),
                QueryOptions::new().bind(text(module)).limit(limit(1, 4096)),
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
        for (schema, name) in managed_objects {
            if relation_kind(transaction, schema, name).await?.is_some() {
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
        // Checksums are already bounded to 255 bytes by whole-ledger verification.
        // Each detached row (names + version + checksum) fits in 512 bytes.
        let bytes = count
            .max(1)
            .checked_mul(512)
            .ok_or(Error::InvalidDefinitions)?;
        let rows = transaction
            .fetch_all_async(
                &format!(
                    "SELECT version, checksum FROM {} WHERE module=$1 ORDER BY version",
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
        transaction
            .execute_async(
                &format!(
                    "INSERT INTO {} (module, version, checksum, fingerprint) VALUES ($1,$2,$3,$4)",
                    self.history_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    crate::integer(
                        i64::try_from(migration.version())
                            .map_err(|_| Error::InvalidDefinitions)?,
                    ),
                    text(migration.checksum()),
                    text(fingerprint),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        Ok(())
    }

    pub(super) async fn remove(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        version: u64,
    ) -> Result<(), Error> {
        let affected = transaction
            .execute_async(
                &format!(
                    "DELETE FROM {} WHERE module=$1 AND version=$2",
                    self.history_sql
                ),
                QueryOptions::new().bind_all(&[
                    text(module),
                    crate::integer(i64::try_from(version).map_err(|_| Error::InvalidDefinitions)?),
                ]),
            )
            .await
            .map_err(Error::Database)?;
        require_one_row(affected)
    }

    pub(super) async fn write_head(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        applied: &[Applied],
    ) -> Result<(), Error> {
        use neotales_dbapi::{DbType, Parameter, Value};
        let head = applied.last();
        let version = head
            .map(|row| i64::try_from(row.version))
            .transpose()
            .map_err(|_| Error::InvalidDefinitions)?;
        let head_version = Parameter::new(
            DbType::I64,
            version.map_or(Value::Null(DbType::I64), Value::I64),
        )
        .map_err(Error::Database)?;
        let head_checksum = Parameter::new(
            DbType::Text,
            head.map_or(Value::Null(DbType::Text), |row| {
                Value::Text(row.checksum.clone())
            }),
        )
        .map_err(Error::Database)?;
        let affected = transaction.execute_async(
            &format!("UPDATE {} SET applied_count=$2, head_version=$3, head_checksum=$4 WHERE module=$1", self.modules_sql),
            QueryOptions::new().bind_all(&[text(module), crate::integer(i64::try_from(applied.len()).map_err(|_| Error::InvalidDefinitions)?),
                head_version, head_checksum]),
        ).await.map_err(Error::Database)?;
        require_one_row(affected)
    }
}

fn require_one_row(affected: u64) -> Result<(), Error> {
    if affected != 1 {
        return Err(Error::History(HistoryReason::InconsistentRows));
    }
    Ok(())
}

async fn relation_kind(
    transaction: &mut (dyn Transaction<'_> + '_),
    schema: &str,
    relation: &str,
) -> Result<Option<String>, Error> {
    let rows = transaction
        .fetch_all_async(
            "SELECT c.relkind::text AS kind FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2",
            QueryOptions::new()
                .bind_all(&[text(schema), text(relation)])
                .limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    rows.first()
        .map(|row| row.get("kind").map_err(Error::Database))
        .transpose()
}
