#![doc = include_str!("../README.md")]

mod execution;
mod flatten;
mod ledger;

use execution::Operation;
use neotales_dbapi::{
    DbError, DbType, ErrorKind, IsolationLevel, Parameter, Pool, QueryOptions, RowLimit,
    Transaction, TxOptions, Value,
};
use neotales_migration::{FlattenManifest, Migrations};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Failure from PostgreSQL module-history management. Driver text is never retained.
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
    /// First registration would adopt an existing declared application relation.
    #[error("a declared managed object already exists for an unregistered module")]
    UntrackedObjects,
    /// Ledger verification failed with a stable, sanitized reason.
    #[error("migration history verification failed: {0}")]
    History(HistoryReason),
    /// Only PostgreSQL major version 17 is supported by this ledger format verifier.
    #[error("unsupported PostgreSQL server version; this backend requires major version 17")]
    UnsupportedVersion,
    /// A database operation failed; an unconfirmed commit may have succeeded.
    #[error("PostgreSQL operation failed or its outcome is unknown: {0}")]
    Database(#[source] DbError),
    /// Exclusive session acquisition failed before any ledger work.
    #[error("database session acquisition failed: {0}")]
    Acquire(#[source] DbError),
    /// Transaction setup failed before any ledger work.
    #[error("database transaction setup failed: {0}")]
    Begin(#[source] DbError),
    /// The advisory-lock query failed.
    #[error("advisory lock acquisition failed: {0}")]
    Lock(#[source] DbError),
    /// The whole advisory-lock acquisition budget elapsed.
    #[error("timed out waiting for the schema advisory lock")]
    LockTimeout,
}

/// Stable ledger-verification reasons. No identity, fingerprint, SQL, or row data is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HistoryReason {
    /// The requested binding differs from the registered module binding.
    #[error("binding fingerprint does not match the registered module")]
    BindingMismatch,
    /// Exactly one of the two ledger relations exists.
    #[error("the ledger relation pair is incomplete")]
    PartialLedger,
    /// An existing ledger table has no ownership/format marker.
    #[error("an existing ledger table has no ownership marker")]
    UnownedLedger,
    /// A relation kind, table shape, collation, or nonempty format marker is unsupported.
    #[error("ledger structure or format marker is incompatible")]
    IncompatibleLedger,
    /// Persisted rows are invalid, orphaned, oversized, or disagree with metadata.
    #[error("persisted history rows and metadata are inconsistent")]
    InconsistentRows,
    /// Rollback or flatten was requested for a module that has never been registered.
    #[error("the module is not registered for rollback or flatten")]
    UnregisteredModule,
}

/// Validated PostgreSQL schema and history-table configuration.
///
/// Executes trusted up/down migrations and exact ledger-only flattening atomically.
#[derive(Clone, Debug)]
pub struct PostgresBackend {
    schema: String,
    table: String,
    lock_timeout: Duration,
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

impl PostgresBackend {
    /// Constructs a backend. Identifiers must match `[a-z_][a-z_0-9]*`.
    ///
    /// Schema identifiers have at most 63 bytes and cannot start with reserved
    /// `pg_`; table identifiers have at most 55 bytes,
    /// leaving room for `_modules`. Names are separate identifiers, not dotted paths.
    /// `lock_timeout` must be an integral number of milliseconds in `1..=i32::MAX`.
    /// It bounds advisory-lock acquisition, including round trips, not the whole
    /// operation. The pool must supply finite acquisition and statement deadlines.
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
        if !valid_identifier(&schema)
            || schema.starts_with("pg_")
            || !valid_identifier(&table)
            || !valid_identifier(&format!("{table}_modules"))
            || millis == 0
            || millis > i32::MAX as u128
            || !lock_timeout.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            schema,
            table,
            lock_timeout,
        })
    }

    /// Returns the stable schema-wide transaction advisory-lock key.
    ///
    /// All cooperating ledger writers must acquire this key *before* schema/catalog
    /// inspection, and hold it until transaction closure. Distinct schemas normally
    /// have distinct keys (64-bit hash collisions are possible). Modules and ledger
    /// tables within one schema deliberately serialize, including first bootstrap.
    /// This diagnostic/interoperability method does not acquire a lock.
    /// Use `pg_advisory_xact_lock` or `pg_try_advisory_xact_lock`; the session-level
    /// `pg_advisory_lock` is not equivalent and can outlive transaction closure.
    #[must_use]
    pub fn advisory_lock_key(&self) -> i64 {
        let mut digest = Sha256::new();
        digest.update(b"neotales-migration-postgres/schema-lock/v1\0");
        digest.update((self.schema.len() as u64).to_be_bytes());
        digest.update(self.schema.as_bytes());
        let hash = digest.finalize();
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&hash[..8]);
        i64::from_be_bytes(prefix)
    }

    /// Atomically initializes or validates one module under a schema-wide exclusive
    /// transaction advisory lock, acquired before catalog inspection or DDL.
    ///
    /// `module` and `fingerprint` are opaque nonempty UTF-8 strings of at most 255
    /// bytes without NUL. `managed_objects` lists separate `(schema, relation)`
    /// identifiers (reserved `pg_` schemas are rejected) whose pre-existence
    /// forbids first registration. Supply every
    /// application relation that indicates prior initialization; an empty list
    /// cannot detect an untracked application schema. No existing objects are adopted.
    ///
    /// Repeating with the same identity/fingerprint validates the entire ledger
    /// and returns a detached snapshot. Ownership markers and exact v1 structure
    /// are required on existing ledger tables. This detects accidental corruption,
    /// not coordinated malicious rewrites; no migration definitions are checked.
    /// PostgreSQL major version 17 is checked after acquiring the advisory lock,
    /// before ledger inspection or DDL. Other versions fail with
    /// [`Error::UnsupportedVersion`]. Schema owners must be trusted. Identity,
    /// fingerprint and checksum comparisons use verified `pg_catalog."C"` collation;
    /// persisted checksums must have `1..=255` UTF-8 bytes.
    ///
    /// The caller cancels by dropping this future. Before commit, the provider must
    /// roll back or discard the session; drop alone is not confirmed rollback or
    /// prompt server lock release. Cancellation during commit has an unknown outcome.
    /// Overlapping calls are supported but serialize within the same schema.
    ///
    /// # Errors
    /// Returns configuration, lock/deadline, ownership/history, untracked-object,
    /// or sanitized database errors. Explicit failures before commit attempt a full
    /// rollback without masking the original error. A commit error may have an
    /// unknown outcome: repeat the call to reconcile, never assume rollback.
    pub async fn initialize_module(
        &self,
        pool: &dyn Pool,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        self.run(
            pool,
            module,
            fingerprint,
            managed_objects,
            Operation::Initialize,
        )
        .await
    }

    /// Applies every pending core migration in order, atomically with its ledger.
    ///
    /// Uses the same locking, ownership, binding, managed-object and PostgreSQL 17
    /// contracts as [`Self::initialize_module`]. Checksums must match an exact
    /// applied prefix of `migrations` before any script executes. Versions must be
    /// in `1..=i64::MAX`; all definition SQL must be NUL-free.
    /// When the applied count is zero, declared managed relations must still be
    /// absent even if this module was registered earlier.
    ///
    /// SQL is **trusted**, must schema-qualify application relations, and must not
    /// manage transactions/savepoints, locks, timeout/search-path settings, or the
    /// ledger itself. No nontransactional external/sequence effects are allowed.
    /// This is not an SQL sandbox. Caller-owned SQL files can be loaded/bound into
    /// core [`Migrations`] before calling; no file I/O occurs during the transaction.
    ///
    /// Repeating with identical definitions/binding is safe: applied scripts are
    /// not rerun. Returns a detached count/head snapshot only after confirmed commit.
    /// Dropping the future delegates rollback/discard to the provider; cancellation
    /// during commit has an unknown outcome. No implicit retries are performed.
    ///
    /// # Errors
    /// Returns the initialization errors, [`Error::InvalidDefinitions`], or a
    /// sanitized core [`Error::Migration`]. Before commit, errors roll back the
    /// entire batch, including bootstrap. Commit failure is not proof of rollback;
    /// reconcile by repeating `up` with identical definitions and binding.
    pub async fn up(
        &self,
        pool: &dyn Pool,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        self.run(
            pool,
            module,
            fingerprint,
            managed_objects,
            Operation::Up(migrations),
        )
        .await
    }

    /// Reverts exactly `count` applied migrations, newest first, in one transaction.
    ///
    /// All [`Self::up`] trust, locking, definition and binding contracts apply.
    /// The module must already be registered. The complete rollback is preflighted:
    /// an excessive count, missing down SQL, or baseline barrier rejects the whole
    /// batch before any script. `count == 0` still verifies history and definitions.
    /// Baseline identity is pinned by the core's checksum; removing the baseline
    /// marker changes that checksum and is rejected, not adopted.
    ///
    /// Unlike `up`, this is **not idempotent**: repeating can revert another batch.
    /// Never blindly retry after an uncertain commit/cancellation. First reconcile
    /// count/head under [`Self::initialize_module`] with the expected old/new states.
    /// No implicit retries are performed; the snapshot is returned only on confirmed commit.
    ///
    /// # Errors
    /// Returns the `up` errors, including sanitized `MissingDown`/`Baseline` core
    /// reasons, or [`HistoryReason::UnregisteredModule`]. Pre-commit errors roll
    /// back the whole batch. A commit error may have an unknown outcome.
    pub async fn down(
        &self,
        pool: &dyn Pool,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        self.run(
            pool,
            module,
            fingerprint,
            managed_objects,
            Operation::Down(migrations, count),
        )
        .await
    }

    /// Atomically replaces a complete exact legacy ledger with a marked baseline.
    ///
    /// Uses the same schema-wide transaction advisory lock, PostgreSQL 17 gate,
    /// exact ledger ownership/structure checks and binding contract as [`Self::up`].
    /// `manifest.legacy()` must match **every** applied version/checksum, with
    /// identical binding. Only this module's history and count/head are replaced;
    /// no application DDL/DML or baseline/down SQL is executed.
    ///
    /// Fresh, unregistered and registered-empty modules are rejected. Apply fresh
    /// baseline SQL with `up` using `manifest.baseline().clone()` instead; flatten
    /// never falsely records an unexecuted fresh baseline. Declared managed objects
    /// on untracked/empty state reject adoption. Partial/drifted/unknown history and
    /// repeated flatten (including a concurrent loser) fail without replacement.
    /// Keep the marked baseline definition: its checksum pins the rollback barrier.
    ///
    /// This is **not an idempotent success API**. After cancellation or an unknown
    /// commit outcome, inspect count/head under [`Self::initialize_module`] and
    /// compare to the expected legacy/baseline states before taking further action.
    /// No automatic retry is performed. Provider drop/unknown-commit rules apply.
    ///
    /// # Errors
    /// Returns initialization/definition errors, [`HistoryReason::UnregisteredModule`],
    /// or sanitized core mismatch errors (including `FlattenDrift` for incomplete
    /// or empty legacy history). Pre-commit failure rolls back all ledger changes;
    /// commit failure is not proof of rollback. Returns a snapshot only on confirmed commit.
    pub async fn flatten(
        &self,
        pool: &dyn Pool,
        module: &str,
        manifest: &FlattenManifest,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        self.run(
            pool,
            module,
            fingerprint,
            managed_objects,
            Operation::Flatten(manifest),
        )
        .await
    }

    async fn run(
        &self,
        pool: &dyn Pool,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        validate_inputs(module, fingerprint, managed_objects)?;
        operation.validate()?;
        let mut session = pool.acquire_async().await.map_err(Error::Acquire)?;
        let mut transaction = session
            .begin_async(TxOptions {
                isolation: Some(IsolationLevel::ReadCommitted),
                read_only: Some(false),
            })
            .await
            .map_err(Error::Begin)?;
        let result = self
            .run_locked(
                &mut *transaction,
                module,
                fingerprint,
                managed_objects,
                operation,
            )
            .await;
        match result {
            Ok(state) => {
                transaction.commit_async().await.map_err(Error::Database)?;
                Ok(state)
            }
            Err(error) => {
                let _ = transaction.rollback_async().await;
                Err(error)
            }
        }
    }

    async fn run_locked(
        &self,
        transaction: &mut (dyn Transaction<'_> + '_),
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        tokio::time::timeout(
            self.lock_timeout,
            acquire_lock(transaction, self.advisory_lock_key()),
        )
        .await
        .map_err(|_| Error::LockTimeout)??;
        ensure_supported_version(transaction).await?;
        transaction
            .execute_trusted_script_async("SET LOCAL search_path = pg_catalog", QueryOptions::new())
            .await
            .map_err(Error::Database)?;
        let ledger = ledger::Ledger::new(&self.schema, &self.table);
        ledger.bootstrap(transaction).await?;
        ledger.verify_history(transaction).await?;
        operation
            .apply(transaction, &ledger, module, fingerprint, managed_objects)
            .await
    }
}

async fn ensure_supported_version(
    transaction: &mut (dyn Transaction<'_> + '_),
) -> Result<(), Error> {
    let rows = transaction
        .fetch_all_async(
            "SELECT pg_catalog.current_setting('server_version_num')::bigint AS version",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .map_err(Error::Database)?;
    let version = rows
        .first()
        .ok_or_else(|| Error::Database(DbError::new(ErrorKind::Decode)))?
        .get::<i64>("version")
        .map_err(Error::Database)?;
    if version / 10_000 != 17 {
        return Err(Error::UnsupportedVersion);
    }
    Ok(())
}

fn validate_inputs(
    module: &str,
    fingerprint: &str,
    managed_objects: &[(&str, &str)],
) -> Result<(), Error> {
    if [module, fingerprint]
        .iter()
        .any(|value| value.is_empty() || value.len() > 255 || value.contains('\0'))
        || managed_objects.iter().any(|(schema, name)| {
            !valid_identifier(schema) || schema.starts_with("pg_") || !valid_identifier(name)
        })
    {
        return Err(Error::Configuration);
    }
    Ok(())
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

async fn acquire_lock(transaction: &mut (dyn Transaction<'_> + '_), key: i64) -> Result<(), Error> {
    let key_parameter = integer(key);
    loop {
        let rows = transaction
            .fetch_all_async(
                "SELECT pg_catalog.pg_try_advisory_xact_lock($1) AS locked",
                QueryOptions::new()
                    .bind(key_parameter.clone())
                    .limit(limit(1, 64)),
            )
            .await
            .map_err(Error::Lock)?;
        if rows
            .first()
            .ok_or_else(|| Error::Lock(DbError::new(ErrorKind::Decode)))?
            .get::<bool>("locked")
            .map_err(Error::Lock)?
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn valid_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.len() <= 63
        && identifier.is_ascii()
        && identifier.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_lowercase() || (index > 0 && byte.is_ascii_digit())
        })
}

fn qualified(schema: &str, relation: &str) -> String {
    format!("\"{schema}\".\"{relation}\"")
}

#[cfg(test)]
mod tests;
