//! SQL Server 16 ledger. Internal catalog SQL never passes through the public
//! restricted-script API. Application migrations still obey that API's grammar.

mod execution;
mod ledger;

use crate::provider::{DirectPool as MssqlPoolProvider, LockedSession};
use execution::Operation;
use neotales_dbapi::{DbError, DbType, ErrorKind, Parameter, QueryOptions, RowLimit, Value};
use neotales_migration::Migrations;
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Failure from SQL Server module-history management. Driver text is never retained.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An identifier, identity, fingerprint, or deadline is invalid.
    #[error("invalid backend configuration")]
    Configuration,
    /// A definition has a zero/out-of-range version or contains NUL SQL bytes.
    #[error("migration definitions contain an unsupported version or NUL SQL")]
    InvalidDefinitions,
    /// The core rejected definition drift or the complete up/down plan.
    #[error("migration planning or history validation failed: {0}")]
    Migration(#[source] neotales_migration::Error),
    /// First registration would adopt an existing declared application object.
    #[error("a declared managed object already exists for an unregistered module")]
    UntrackedObjects,
    /// Ledger verification failed with a stable, sanitized reason.
    #[error("migration history verification failed: {0}")]
    History(HistoryReason),
    /// Only SQL Server major version 16 is supported by this ledger verifier.
    #[error("unsupported SQL Server major version; this backend requires version 16")]
    UnsupportedVersion,
    /// The current database does not confirm a commit-durable setting.
    #[error("database delayed-durability metadata is missing or invalid")]
    DurabilityMetadata,
    /// The current database is configured to acknowledge commits before durable flush.
    #[error("migration ledger requires delayed durability to be disabled or allowed")]
    ForcedDelayedDurability,
    /// A database operation failed. A dispatched batch may have applied earlier statements, and
    /// an unconfirmed commit may have succeeded; the provider error distinguishes the category.
    #[error("SQL Server operation failed or its outcome is unknown: {0}")]
    Database(#[source] DbError),
    /// Transaction or transaction-owned application-lock setup failed before ledger work.
    #[error("database transaction or application-lock setup failed: {0}")]
    Begin(#[source] DbError),
    /// The transaction-owned application lock was not granted within its deadline.
    #[error("timed out waiting for the ledger application lock")]
    LockTimeout,
}

/// Stable ledger-verification reasons. No identity, fingerprint, SQL, or row data is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HistoryReason {
    /// The requested binding differs from the registered module binding.
    #[error("binding fingerprint does not match the registered module")]
    BindingMismatch,
    /// Exactly one of the two ledger tables exists.
    #[error("the ledger table pair is incomplete")]
    PartialLedger,
    /// A relation kind, table shape, collation, constraint, marker, or index is unsupported.
    #[error("ledger structure or ownership marker is incompatible")]
    IncompatibleLedger,
    /// Persisted rows are invalid, orphaned, oversized, or disagree with metadata.
    #[error("persisted history rows and metadata are inconsistent")]
    InconsistentRows,
    /// Rollback was requested for a module that has never been registered.
    #[error("the module is not registered for rollback")]
    UnregisteredModule,
}

/// Validated SQL Server schema and history-table configuration.
///
/// Executes trusted up/down migrations in one transaction with their ledger. The
/// schema must already exist; this backend never creates one. A post-dispatch
/// batch failure may invalidate the provider session; rollback is then attempted
/// without server acknowledgement and the connection is discarded.
#[derive(Clone, Debug)]
pub struct SqlServerBackend {
    schema: String,
    table: String,
    lock_timeout: Duration,
    lock_resource: String,
}

/// Validated snapshot of one registered module's persisted ledger head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleState {
    module: String,
    fingerprint: String,
    count: i64,
    head: Option<(i64, String)>,
}

impl ModuleState {
    /// Returns the registered module identity.
    #[must_use]
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Returns the validated binding fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Returns the applied-history row count.
    #[must_use]
    pub const fn applied_count(&self) -> i64 {
        self.count
    }

    /// Returns the validated latest version and checksum, if history is non-empty.
    #[must_use]
    pub fn head(&self) -> Option<(i64, &str)> {
        self.head
            .as_ref()
            .map(|(version, checksum)| (*version, checksum.as_str()))
    }
}

impl SqlServerBackend {
    /// Constructs a backend for `schema.table` and its `_modules` companion.
    ///
    /// Schema and table names must match `[a-z_][a-z_0-9]*`; the schema is at most
    /// 128 bytes and cannot be `sys` or `information_schema`, and the table is at
    /// most 120 bytes to leave room for `_modules`. Names are separate identifiers,
    /// not dotted paths. The schema must already exist.
    ///
    /// `lock_timeout` must be a positive integral number of milliseconds no greater
    /// than `i32::MAX`. It bounds the server-side application-lock wait, not the
    /// whole operation. Enforce a total deadline by cancelling the future.
    ///
    /// # Errors
    /// Returns [`Error::Configuration`] for invalid names or timeout.
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
        lock_timeout: Duration,
    ) -> Result<Self, Error> {
        let schema = schema.into();
        let table = table.into();
        let millis = lock_timeout.as_millis();
        if !valid_identifier(&schema, 128)
            || schema == "sys"
            || schema == "information_schema"
            || !valid_identifier(&table, 120)
            || millis == 0
            || millis > i32::MAX as u128
            || !lock_timeout.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::Configuration);
        }
        let lock_resource = lock_resource(&schema, &table);
        Ok(Self {
            schema,
            table,
            lock_timeout,
            lock_resource,
        })
    }

    /// Returns the database-scoped application-lock resource for this ledger.
    ///
    /// Cooperating writers must acquire this resource with `sp_getapplock` and
    /// `@LockOwner = 'Transaction'` before inspecting the ledger, and hold it until
    /// transaction closure. Session-owned locks are not equivalent and can outlive
    /// transaction closure. The resource is derived from the schema and table with
    /// SHA-256; hash collisions remain possible. This diagnostic method does not
    /// acquire a lock.
    #[must_use]
    pub fn lock_resource(&self) -> &str {
        &self.lock_resource
    }

    /// Initializes or validates one module under the ledger application lock in
    /// one transaction.
    ///
    /// `module` and `fingerprint` are opaque nonempty UTF-8 strings of at most 255
    /// bytes without NUL or a trailing space. `managed_objects` lists separate `(schema, object)`
    /// identifiers whose pre-existence forbids first registration. Supply every
    /// application object that indicates prior initialization; an empty list cannot
    /// detect an untracked application. No existing objects are adopted.
    ///
    /// Repeating with the same identity/fingerprint validates the entire ledger and
    /// returns a detached snapshot. The ownership markers and exact catalog shape are
    /// required on existing ledger tables. This detects accidental corruption, not a
    /// coordinated rewrite by a database owner. No migration definitions are checked.
    /// SQL Server major version 16 and the current database's delayed durability
    /// setting are checked before ledger inspection or DDL. `FORCED` is rejected
    /// because SQL Server may acknowledge a commit before its log records are
    /// hardened. Missing, unreadable, or unrecognized setting metadata fails closed.
    /// This is a point-in-time check only: it does not serialize against an
    /// administrator changing the database setting. Administrators must not change
    /// delayed durability to `FORCED` while migration operations rely on durable
    /// commit acknowledgement. [`Self::up`] and [`Self::down`] inherit this
    /// precondition and check.
    ///
    /// Cancellation is expressed by dropping the future. Transaction and connection
    /// ownership delegates rollback or discard to the provider; drop is not proof of
    /// prompt lock release. Cancellation during commit has an unknown outcome. Repeat
    /// the call to reconcile.
    ///
    /// # Errors
    /// Returns configuration, lock-timeout, ownership/history, untracked-object, or
    /// sanitized database errors. Explicit failures before commit attempt rollback
    /// without masking the original error. A post-dispatch batch error can invalidate
    /// the provider session, so the rollback attempt can fail before server
    /// acknowledgement; the connection is then discarded, and prompt server-side
    /// rollback and lock release are not guaranteed. A dispatched-batch error is
    /// `WriteOutcomeUnknown` (earlier statements are not confirmed committed), not a
    /// rollback-success claim. A commit error is a separate unknown-commit outcome:
    /// repeat the call to reconcile.
    pub async fn initialize_module(
        &self,
        provider: &MssqlPoolProvider,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        Box::pin(self.run(
            provider,
            module,
            fingerprint,
            managed_objects,
            Operation::Initialize,
        ))
        .await
    }

    /// Applies every pending core migration in order, in one transaction with its ledger.
    ///
    /// Uses the same locking, ownership, binding, managed-object, and SQL Server 16
    /// contracts as [`Self::initialize_module`]. Checksums must match an exact applied
    /// prefix of `migrations` before any script executes. Versions must be in
    /// `1..=i64::MAX`; all definition SQL must be NUL-free.
    /// The delayed-durability check is point-in-time and does not prevent an
    /// administrator from concurrently changing the database setting to `FORCED`.
    ///
    /// Before the first effect, every pending up script is split at client `GO`
    /// boundaries and every batch is validated. All batches of one script then execute
    /// on the same transaction, in order. A `GO` boundary is not a transaction boundary.
    /// SQL is **trusted** and must satisfy the provider's restricted trusted-script
    /// contract: one whitespace-tokenized CREATE TABLE with one BIGINT column,
    /// DROP TABLE, or INSERT of a nonnegative integer per GO-separated batch.
    /// Strings, comments, quoted identifiers, UPDATE, transaction/session control
    /// and executable SQL are rejected rather than weakened here.
    ///
    /// Since SQL Server DDL is ordinarily transactional, scripts and ledger writes
    /// commit together. A failing later batch or script is a transaction failure. The
    /// backend attempts rollback, but a post-dispatch provider error invalidates the
    /// session, so the attempt can fail before server acknowledgement and the
    /// connection is discarded. Earlier statements are not confirmed committed; the
    /// returned dispatched-batch error is `WriteOutcomeUnknown`. Prompt server-side
    /// rollback and lock release are not guaranteed. Non-transactional effects are
    /// outside this contract.
    ///
    /// Repeating with identical definitions/binding is safe: applied scripts are not
    /// rerun. A detached snapshot is returned only after confirmed commit.
    ///
    /// # Errors
    /// Returns the initialization errors, [`Error::InvalidDefinitions`],
    /// or a sanitized core/`Unsupported` database error. Before commit, errors trigger a
    /// rollback attempt; post-dispatch errors can invalidate the session, so rollback is
    /// not confirmed and the connection is discarded. Commit failure is a separate
    /// unknown outcome and is not proof of rollback; reconcile by repeating `up` with
    /// identical definitions and binding.
    pub async fn up(
        &self,
        provider: &MssqlPoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        Box::pin(self.run(
            provider,
            module,
            fingerprint,
            managed_objects,
            Operation::Up(migrations),
        ))
        .await
    }

    /// Reverts exactly `count` applied migrations, newest first, in one transaction.
    ///
    /// All [`Self::up`] trust, locking, definition, batch, and binding contracts apply.
    /// The module must already be registered. The complete rollback is preflighted: an
    /// excessive count, missing down SQL, baseline barrier, or unsplittable batch
    /// rejects the whole request before any script.
    /// The delayed-durability check is point-in-time and does not prevent an
    /// administrator from concurrently changing the database setting to `FORCED`.
    ///
    /// Unlike `up`, this is **not idempotent**: repeating can revert another batch.
    /// Never blindly retry after an uncertain commit or cancellation. First reconcile
    /// count/head with [`Self::initialize_module`] against the expected old/new states.
    ///
    /// # Errors
    /// Returns the `up` errors, sanitized `MissingDown`/`Baseline` core reasons, or
    /// [`HistoryReason::UnregisteredModule`]. Pre-commit errors trigger a rollback
    /// attempt; a post-dispatch error can invalidate the session, so rollback and
    /// prompt lock release are unconfirmed and the connection is discarded. A commit
    /// error is a separate unknown outcome.
    pub async fn down(
        &self,
        provider: &MssqlPoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        Box::pin(self.run(
            provider,
            module,
            fingerprint,
            managed_objects,
            Operation::Down(migrations, count),
        ))
        .await
    }

    async fn run(
        &self,
        provider: &MssqlPoolProvider,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        validate_inputs(module, fingerprint, managed_objects)?;
        operation.validate()?;
        let lock_timeout_ms =
            u32::try_from(self.lock_timeout.as_millis()).map_err(|_| Error::Configuration)?;
        let mut transaction = provider
            .begin_locked(&self.lock_resource, lock_timeout_ms)
            .await
            .map_err(|error| {
                if error.kind() == ErrorKind::Busy {
                    Error::LockTimeout
                } else {
                    Error::Begin(error)
                }
            })?;
        #[cfg(test)]
        crate::live::ledger::hold_first_writer(&mut transaction).await;
        let result = self
            .run_locked(
                &mut transaction,
                module,
                fingerprint,
                managed_objects,
                operation,
            )
            .await;
        match result {
            Ok(state) => {
                transaction.commit().await.map_err(Error::Database)?;
                Ok(state)
            }
            Err(error) => {
                // Rollback is attempted, not confirmed. A post-dispatch provider error can
                // invalidate the session; rollback_async may then return before server-side
                // rollback. Dropping the transaction and session closes that connection. SQL
                // Server may roll back and release locks on disconnect, but no acknowledgement
                // is obtained. The original batch error is returned, not a rollback success.
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn run_locked(
        &self,
        transaction: &mut LockedSession,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        ensure_supported_version(transaction).await?;
        ensure_durable_commits(transaction).await?;
        let ledger = ledger::Ledger::new(&self.schema, &self.table);
        ledger.bootstrap(transaction).await?;
        ledger.verify_history(transaction).await?;
        operation
            .apply(transaction, &ledger, module, fingerprint, managed_objects)
            .await
    }
}

async fn ensure_supported_version(transaction: &mut LockedSession) -> Result<(), Error> {
    let rows = transaction
        .fetch_all_async(
            "SELECT CAST(SERVERPROPERTY(N'ProductMajorVersion') AS int) AS version",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    let version = rows
        .first()
        .ok_or_else(|| Error::Database(DbError::new(ErrorKind::Decode)))?
        .get::<i64>("version")
        .map_err(Error::Database)?;
    if version != 16 {
        return Err(Error::UnsupportedVersion);
    }
    Ok(())
}

async fn ensure_durable_commits(transaction: &mut LockedSession) -> Result<(), Error> {
    // Query the current database rather than server-wide configuration: the
    // setting is database-scoped and governs whether an acknowledged commit is
    // guaranteed to have hardened its log records.
    let rows = transaction
        .fetch_all_async(
            "SELECT delayed_durability_desc AS durability FROM sys.databases WHERE database_id = DB_ID()",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    let mode = rows
        .first()
        .ok_or(Error::DurabilityMetadata)?
        .get::<String>("durability")
        .map_err(|_| Error::DurabilityMetadata)?;
    match mode.as_str() {
        "FORCED" => Err(Error::ForcedDelayedDurability),
        "DISABLED" | "ALLOWED" => Ok(()),
        _ => Err(Error::DurabilityMetadata),
    }
}

fn validate_inputs(
    module: &str,
    fingerprint: &str,
    managed_objects: &[(&str, &str)],
) -> Result<(), Error> {
    if !valid_identity(module)
        || !valid_identity(fingerprint)
        || managed_objects.iter().any(|(schema, object)| {
            !valid_identifier(schema, 128) || !valid_identifier(object, 128)
        })
    {
        return Err(Error::Configuration);
    }
    Ok(())
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.contains('\0') && !value.ends_with(' ')
}

fn lock_resource(schema: &str, table: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"neotales-migration-mssql/lock/v1\0");
    digest.update((schema.len() as u64).to_be_bytes());
    digest.update(schema.as_bytes());
    digest.update((table.len() as u64).to_be_bytes());
    digest.update(table.as_bytes());
    let hash = digest.finalize();
    let mut resource = String::with_capacity(36 + hash.len() * 2);
    resource.push_str("neotales-migration-mssql/lock/v1/");
    for byte in hash {
        use std::fmt::Write as _;
        write!(&mut resource, "{byte:02x}").expect("write to String");
    }
    resource
}

pub(crate) fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("matching text type")
}

pub(crate) fn nullable_text(value: Option<&str>) -> Parameter {
    Parameter::new(
        DbType::Text,
        value.map_or(Value::Null(DbType::Text), |value| {
            Value::Text(value.to_owned())
        }),
    )
    .expect("matching text type")
}

pub(crate) fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("matching integer type")
}

pub(crate) fn nullable_integer(value: Option<i64>) -> Parameter {
    Parameter::new(
        DbType::I64,
        value.map_or(Value::Null(DbType::I64), Value::I64),
    )
    .expect("matching integer type")
}

pub(crate) fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("positive static limits")
}

pub(crate) fn valid_identifier(identifier: &str, maximum: usize) -> bool {
    !identifier.is_empty()
        && identifier.len() <= maximum
        && identifier.is_ascii()
        && identifier.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_lowercase() || (index > 0 && byte.is_ascii_digit())
        })
}

pub(crate) fn qualified(schema: &str, relation: &str) -> String {
    format!("[{schema}].[{relation}]")
}

#[cfg(test)]
mod tests;
