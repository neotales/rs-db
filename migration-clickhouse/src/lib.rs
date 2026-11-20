#![doc = include_str!("../README.md")]
#![allow(clippy::doc_markdown)]

mod ledger;

#[cfg(test)]
extern crate self as neotales_migration_clickhouse;
#[cfg(test)]
#[path = "../tests/support/live.rs"]
mod live;

#[cfg(test)]
#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh --test-harness"]
#[cfg(feature = "experimental-native-migrations")]
async fn clickhouse_migration_contract() {
    live::clickhouse_migration_contract().await;
}

use ledger::Ledger;
use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, QueryOptions, Row, RowLimit, Session, Value,
};
use neotales_dbapi_clickhouse::{ClickHousePoolProvider, ClickHouseSession};
use neotales_migration::{
    Applied, Error as CoreError, FlattenManifest, HistoryState, Migrations, PersistedHistory,
};
use std::time::Duration;

/// A dirty migration record that blocks further automated schema changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirtyState {
    direction: DirtyDirection,
    stage: DirtyStage,
    version: u64,
    checksum: String,
}

impl DirtyState {
    /// Returns whether the interrupted operation was an apply or a revert.
    #[must_use]
    pub const fn direction(&self) -> DirtyDirection {
        self.direction
    }

    /// Returns the last recorded stage of the interrupted operation.
    #[must_use]
    pub const fn stage(&self) -> DirtyStage {
        self.stage
    }

    /// Returns the migration version that was being processed.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the checksum of the definition that was being processed.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

impl std::fmt::Display for DirtyState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "direction={} stage={} version={}",
            self.direction.as_str(),
            self.stage.as_str(),
            self.version
        )
    }
}

/// Direction of an interrupted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirtyDirection {
    /// Applying a migration.
    Up,
    /// Reverting a migration.
    Down,
}

impl DirtyDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "up" => Some(Self::Up),
            "down" => Some(Self::Down),
            _ => None,
        }
    }
}

/// Last persisted stage of an interrupted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirtyStage {
    /// The dirty marker was written; script completion is unknown.
    Started,
    /// The script returned success; history synchronization may or may not have completed.
    /// Inspect persisted history before reconciliation; this stage alone cannot establish it.
    ScriptDone,
    /// The script returned an error; it may still have had partial effects.
    ScriptFailed,
}

impl DirtyStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::ScriptDone => "script_done",
            Self::ScriptFailed => "script_failed",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "started" => Some(Self::Started),
            "script_done" => Some(Self::ScriptDone),
            "script_failed" => Some(Self::ScriptFailed),
            _ => None,
        }
    }
}

/// Operator assertion used to clear a dirty ledger after manual inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    /// The intended schema effect is present. For `up`, record the migration; for `down`,
    /// remove the reverted migration from the ledger.
    Applied,
    /// The intended schema effect is absent. For `down`, the original schema effect remains:
    /// if its history row is missing, append a replacement before clearing the marker.
    /// For `up`, reject an already-recorded history row; otherwise clear the marker without
    /// adding history. Existing down history is left unchanged.
    NotApplied,
}

/// Detached snapshot of one registered module's persisted state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleState {
    module: String,
    fingerprint: String,
    count: i64,
    head: Option<(i64, String)>,
    dirty: Option<DirtyState>,
}

impl ModuleState {
    /// Returns the module identity.
    #[must_use]
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Returns the registered binding fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Returns the number of applied migrations.
    #[must_use]
    pub const fn applied_count(&self) -> i64 {
        self.count
    }

    /// Returns the latest applied version and checksum.
    #[must_use]
    pub fn head(&self) -> Option<(i64, &str)> {
        self.head
            .as_ref()
            .map(|(version, checksum)| (*version, checksum.as_str()))
    }

    /// Returns the dirty marker, if the ledger requires reconciliation.
    #[must_use]
    pub fn dirty(&self) -> Option<&DirtyState> {
        self.dirty.as_ref()
    }
}

/// Stable, sanitized history-verification reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HistoryReason {
    /// The requested binding differs from the registered module binding.
    #[error("binding fingerprint does not match the registered module")]
    BindingMismatch,
    /// Exactly one ledger table exists.
    #[error("the ledger table pair is incomplete")]
    PartialLedger,
    /// An existing ledger table has no ownership marker or has the wrong engine.
    #[error("an existing ledger table has no ownership marker")]
    UnownedLedger,
    /// A ledger table shape, column type, or primary key is unsupported.
    #[error("ledger structure is incompatible")]
    IncompatibleLedger,
    /// Persisted rows and metadata disagree, or a write affected an unexpected row count.
    #[error("persisted history rows and metadata are inconsistent")]
    InconsistentRows,
    /// The operation requires a registered module.
    #[error("the module is not registered")]
    UnregisteredModule,
    /// Reconciliation was requested without a dirty record.
    #[error("the module has no dirty migration to reconcile")]
    NotDirty,
}

/// Failure from ClickHouse module-history management. Driver text and SQL are never retained.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An identifier, identity, or lock timeout is invalid.
    #[error("invalid backend configuration")]
    Configuration,
    /// A definition has a zero/out-of-range version or contains NUL SQL.
    #[error("migration definitions contain an unsupported version or NUL SQL")]
    InvalidDefinitions,
    /// The core rejected definition drift or a plan.
    #[error("migration planning or history validation failed: {0}")]
    Migration(#[source] CoreError),
    /// First registration would adopt an existing declared application relation.
    #[error("a declared managed object already exists for an unregistered module")]
    UntrackedObjects,
    /// Ledger verification failed with a stable, sanitized reason.
    #[error("migration history verification failed: {0}")]
    History(#[source] HistoryReason),
    /// A dirty marker blocks automated schema changes.
    #[error("migration ledger is dirty: {0}")]
    Dirty(DirtyState),
    /// A script failed after the dirty marker was persisted.
    #[error("migration script failed; ledger is dirty: {0}")]
    Failed(DirtyState, #[source] DbError),
    /// A script failed and the stage write also failed. Both sanitized outcomes are retained;
    /// the lock is kept if either outcome is uncertain.
    #[error("migration script failed; stage recording also failed ({stage}): {dirty}")]
    FailedStage {
        /// Dirty marker state derived from the last confirmed write.
        dirty: DirtyState,
        /// Sanitized script failure.
        #[source]
        script: DbError,
        /// Sanitized stage-write failure, including its own classification.
        stage: Box<Error>,
    },
    /// Reserved for future contention-aware locking; current lock attempts do not retry.
    #[error("timed out waiting for the migration lock")]
    LockTimeout,
    /// Flatten would replace history without atomicity.
    #[error("flatten is unsupported because ClickHouse is not transactional")]
    UnsupportedFlatten,
    /// The target database is not an Atomic database, so the lock DDL is not atomic.
    #[error("the target database must use the Atomic engine")]
    UnsupportedDatabase,
    /// Exclusive session acquisition failed.
    #[error("database session acquisition failed: {0}")]
    Acquire(#[source] DbError),
    /// Acquiring the advisory lock failed.
    #[error("database lock operation failed: {0}")]
    Lock(#[source] DbError),
    /// Lock creation may have succeeded or may still be running. Recovery requires an
    /// operator-managed exclusive window: quiesce all writers, positively establish the owner
    /// has stopped, verify outstanding server work stopped, and inspect the ledger before
    /// removing the lock. The lock has no owner identity or fencing; an empty `system.processes`
    /// result alone does not prove that its owner stopped.
    #[error("migration lock creation outcome is unknown: {0}")]
    LockOutcomeUnknown(#[source] DbError),
    /// Releasing the advisory lock failed.
    #[error("database lock release failed: {0}")]
    Release(#[source] DbError),
    /// Both the operation and the subsequent lock release failed.
    #[error("operation failed ({operation}); lock release also failed ({release})")]
    OperationAndRelease {
        /// The original operation failure.
        #[source]
        operation: Box<Error>,
        /// The lock release failure, including any uncertain outcome.
        release: Box<Error>,
    },
    /// A ledger or migration operation failed; its outcome may be unknown.
    #[error("database operation failed: {0}")]
    Database(#[source] DbError),
}

pub(crate) struct LedgerState {
    pub(crate) persisted: PersistedHistory,
    pub(crate) dirty: Option<DirtyState>,
}

/// Validated ClickHouse ledger configuration.
///
/// The backend uses the current database's qualified tables and a table-based mutex. It does not
/// use a transaction around migration scripts because ClickHouse has no interactive transaction.
/// A cancelled operation can leave the lock table behind. Before manual removal, quiesce all
/// cooperating writers, positively establish that the owner has stopped, verify outstanding
/// server work has stopped, and inspect the ledger read-only. Keep writers stopped through
/// reconciliation; an empty `system.processes` result does not prove the owner has stopped.
/// The lock has no owner identity or fencing and is atomic only in an Atomic database.
///
/// Every method takes the concrete [`ClickHousePoolProvider`] rather than `&dyn Pool`. The native
/// provider's migration write path is disabled by default. The
/// `experimental-native-migrations` Cargo feature explicitly enables it, including in normal
/// dependency builds. Cargo features are additive and can be enabled by another dependency.
/// Without it, registration, migration, state inspection, and reconciliation return
/// [`Error::Database`] with [`ErrorKind::Unsupported`] after local validation and before acquiring
/// a session. The feature is experimental, tested only on ClickHouse 24.8. SQL and managed
/// object declarations must be trusted and complete. All writers must share one coordination
/// domain for both the ledger and effects, or use a correctly scoped external lock. There is no
/// cluster-wide fencing, transactional coupling of effects and ledger, or power-loss guarantee.
/// Recovery requires exclusive manual intervention as described above.
/// [`Self::flatten`] separately always returns [`Error::UnsupportedFlatten`]. The concrete type
/// prevents a generic pool from claiming session-level lock cleanup it cannot perform.
#[derive(Clone, Debug)]
pub struct ClickHouseBackend {
    database: String,
    table: String,
    lock_timeout: Duration,
}

impl ClickHouseBackend {
    /// Constructs a backend for `database.table`.
    ///
    /// Identifiers must match `[a-z_][a-z0-9_]*`, fit ClickHouse's 255-byte limit, and `table`
    /// must leave room for the `_meta` and `_lock` suffixes. The database cannot be a ClickHouse
    /// system schema. `lock_timeout` must be positive and bounds one create attempt.
    ///
    /// # Errors
    /// Returns [`Error::Configuration`] for invalid identifiers or timeout.
    pub fn new(
        database: impl Into<String>,
        table: impl Into<String>,
        lock_timeout: Duration,
    ) -> Result<Self, Error> {
        let database = database.into();
        let table = table.into();
        if !valid_identifier(&database)
            || !valid_identifier(&table)
            || database.len() > 250
            || table.len() > 250
            || database == "system"
            || database == "information_schema"
            || lock_timeout.as_millis() == 0
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            database,
            table,
            lock_timeout,
        })
    }

    /// Returns the fully qualified lock table for inspection and controlled recovery.
    ///
    /// Never drop it merely because no query appears in `system.processes`. Follow the
    /// exclusive-window procedure on [`ClickHouseBackend`] first.
    #[must_use]
    pub fn lock_table(&self) -> String {
        format!("{}.{}", self.database, self.lock_table_name())
    }

    fn lock_table_name(&self) -> String {
        format!("{}_lock", self.table)
    }

    /// Registers an empty module after rejecting declared pre-existing objects.
    ///
    /// Without the `experimental-native-migrations` feature, returns [`Error::Database`] with
    /// [`ErrorKind::Unsupported`] before acquiring a session, after local validation.
    /// See [`ClickHouseBackend`] for the experimental safety preconditions.
    ///
    /// # Errors
    /// Returns configuration/history/untracked-object/lock/database errors. A dirty
    /// ledger is returned as [`Error::Dirty`]; use [`Self::state`] and [`Self::reconcile`].
    pub async fn initialize_module(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
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

    /// Applies pending migrations one script at a time, persisting a dirty marker before each
    /// script and its ledger row after it.
    ///
    /// ClickHouse has no transactional DDL or DML. A script that fails after some statements may
    /// leave partial schema effects; the dirty marker blocks all further automated changes until
    /// an operator reconciles. Applied scripts are never silently retried.
    ///
    /// Without the `experimental-native-migrations` feature, returns [`Error::Database`] with
    /// [`ErrorKind::Unsupported`] before acquiring a session, after local validation.
    /// See [`ClickHouseBackend`] for the experimental safety preconditions.
    ///
    /// # Errors
    /// Returns configuration/history/untracked-object/lock/database errors,
    /// [`Error::Dirty`] when a marker already exists, or [`Error::Failed`] when a script fails.
    pub async fn up(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        managed_objects: &[&str],
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

    /// Reverts exactly `count` applied migrations, newest first, one at a time.
    ///
    /// All down scripts and the baseline barrier are preflighted before the first dirty marker.
    /// This is **not** idempotent: never blindly repeat after an uncertain failure.
    ///
    /// Without the `experimental-native-migrations` feature, returns [`Error::Database`] with
    /// [`ErrorKind::Unsupported`] before acquiring a session, after local validation.
    /// See [`ClickHouseBackend`] for the experimental safety preconditions.
    ///
    /// # Errors
    /// Returns unregistered/history/planning/lock/database errors, [`Error::Dirty`], or
    /// [`Error::Failed`].
    pub async fn down(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        managed_objects: &[&str],
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

    /// Reads one module's detached state under the same table lock.
    ///
    /// Returns `None` for an unregistered module. A dirty marker is returned in the snapshot
    /// instead of failing, because this is the recovery inspection API.
    ///
    /// Without the `experimental-native-migrations` feature, returns [`Error::Database`] with
    /// [`ErrorKind::Unsupported`] before acquiring a session, after module validation.
    /// See [`ClickHouseBackend`] for the experimental safety preconditions.
    ///
    /// # Errors
    /// Returns configuration/history/lock/database errors.
    pub async fn state(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
    ) -> Result<Option<ModuleState>, Error> {
        validate_module(module)?;
        if !native_migrations_enabled() {
            return Err(Error::Database(DbError::new(ErrorKind::Unsupported)));
        }
        let mut session = pool.acquire_clickhouse().await.map_err(Error::Acquire)?;
        self.require_atomic_database(&mut session).await?;
        self.acquire_lock(&mut session).await?;
        let result = self.state_locked(&mut session, module).await;
        let release = self.release_lock(&mut session).await;
        combine_release(result, release)
    }

    async fn state_locked(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
    ) -> Result<Option<ModuleState>, Error> {
        let ledger = Ledger::new(&self.database, &self.table);
        if !ledger.verify_present(session).await? {
            return Ok(None);
        }
        Ok(ledger
            .read_module(session, module, None)
            .await?
            .map(|state| Ledger::snapshot(module, &state)))
    }

    /// Clears a dirty marker after an operator has inspected the schema.
    ///
    /// `Applied` records an apply or removes a reverted down migration. `NotApplied` for `down`
    /// restores a missing history row by appending a replacement, because the original schema
    /// effect remains; existing history is unchanged. `NotApplied` for `up` rejects an
    /// already-recorded history row, otherwise clearing the marker without adding history.
    /// The caller is asserting the schema effect; the provider cannot verify arbitrary DDL.
    ///
    /// Without the `experimental-native-migrations` feature, returns [`Error::Database`] with
    /// [`ErrorKind::Unsupported`] before acquiring a session, after local validation.
    /// See [`ClickHouseBackend`] for the experimental safety preconditions.
    ///
    /// # Errors
    /// Returns unregistered/not-dirty/history/mismatch/lock/database errors.
    pub async fn reconcile(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        outcome: ReconcileOutcome,
    ) -> Result<ModuleState, Error> {
        self.run(
            pool,
            module,
            fingerprint,
            &[],
            Operation::Reconcile(migrations, outcome),
        )
        .await
    }

    /// Flatten is rejected because replacing history plus a baseline cannot be made atomic when
    /// ClickHouse DDL and inserts are not transactional.
    ///
    /// # Errors
    /// Always returns [`Error::UnsupportedFlatten`].
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)] // Parity with the other backends.
    pub async fn flatten(
        &self,
        _pool: &ClickHousePoolProvider,
        _module: &str,
        _manifest: &FlattenManifest,
        _fingerprint: &str,
        _managed_objects: &[&str],
    ) -> Result<ModuleState, Error> {
        Err(Error::UnsupportedFlatten)
    }

    async fn run(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        validate_common(module, fingerprint, managed_objects)?;
        operation.validate()?;
        if !native_migrations_enabled() {
            return Err(Error::Database(DbError::new(ErrorKind::Unsupported)));
        }
        let mut session = pool.acquire_clickhouse().await.map_err(Error::Acquire)?;
        self.require_atomic_database(&mut session).await?;
        self.acquire_lock(&mut session).await?;
        let result = self
            .run_locked(
                &mut session,
                module,
                fingerprint,
                managed_objects,
                operation,
            )
            .await;
        // An uncertain script may still be running on the server. Releasing the lock here would
        // allow a second writer to overlap it. Keep the lock table; an operator confirms the
        // owner and server work stopped in an exclusive window before manual removal.
        let retain_lock = result.as_ref().err().is_some_and(retains_lock);
        let release = if retain_lock {
            None
        } else {
            Some(self.release_lock(&mut session).await)
        };
        match release {
            Some(release) => combine_release(result, release),
            None => result,
        }
    }

    async fn run_locked(
        &self,
        session: &mut ClickHouseSession,
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        let ledger = Ledger::new(&self.database, &self.table);
        ledger.bootstrap(session).await?;
        match operation {
            Operation::Initialize => {
                let state = if let Some(state) = ledger
                    .read_module(session, module, Some(fingerprint))
                    .await?
                {
                    state
                } else {
                    ledger.reject_untracked(session, managed_objects).await?;
                    ledger.register(session, module, fingerprint).await?;
                    ledger
                        .read_module(session, module, Some(fingerprint))
                        .await?
                        .ok_or(Error::History(HistoryReason::InconsistentRows))?
                };
                if let Some(dirty) = state.dirty {
                    return Err(Error::Dirty(dirty));
                }
                Ok(Ledger::snapshot(module, &state))
            }
            Operation::Up(migrations) => {
                self.up_locked(
                    session,
                    &ledger,
                    module,
                    fingerprint,
                    managed_objects,
                    migrations,
                )
                .await
            }
            Operation::Down(migrations, count) => {
                self.down_locked(session, &ledger, module, fingerprint, migrations, count)
                    .await
            }
            Operation::Reconcile(migrations, outcome) => {
                self.reconcile_locked(session, &ledger, module, fingerprint, migrations, outcome)
                    .await
            }
        }
    }

    #[allow(clippy::too_many_lines)] // One ordered nontransactional apply path must stay auditable.
    async fn up_locked(
        &self,
        session: &mut ClickHouseSession,
        ledger: &Ledger,
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
        migrations: &Migrations,
    ) -> Result<ModuleState, Error> {
        let state = if let Some(state) = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
        {
            state
        } else {
            ledger.reject_untracked(session, managed_objects).await?;
            ledger.register(session, module, fingerprint).await?;
            ledger
                .read_module(session, module, Some(fingerprint))
                .await?
                .ok_or(Error::History(HistoryReason::InconsistentRows))?
        };
        if let Some(dirty) = state.dirty.clone() {
            return Err(Error::Dirty(dirty));
        }
        if state.persisted.applied.is_empty() {
            ledger.reject_untracked(session, managed_objects).await?;
        }
        let history = validated_history(migrations, &state.persisted)?;
        let plans = migrations
            .plan_up(&history, fingerprint)
            .map_err(Error::Migration)?;
        let mut applied = state.persisted.applied.clone();
        for plan in plans {
            let migration = definition(migrations, plan.version)?;
            let direction = DirtyDirection::Up;
            let dirty = DirtyState {
                direction,
                stage: DirtyStage::Started,
                version: plan.version,
                checksum: migration.checksum().to_owned(),
            };
            ledger
                .mark_dirty(
                    session,
                    module,
                    fingerprint,
                    direction,
                    plan.version,
                    migration.checksum(),
                )
                .await?;
            if let Err(script_error) = session
                .execute_no_count(&plan.sql, QueryOptions::new())
                .await
            {
                return match ledger
                    .mark_stage(
                        session,
                        module,
                        fingerprint,
                        direction,
                        plan.version,
                        DirtyStage::ScriptFailed,
                    )
                    .await
                {
                    Ok(()) => Err(Error::Failed(
                        DirtyState {
                            stage: DirtyStage::ScriptFailed,
                            ..dirty
                        },
                        script_error,
                    )),
                    Err(stage_error) => Err(Error::FailedStage {
                        dirty: DirtyState {
                            stage: DirtyStage::Started,
                            ..dirty
                        },
                        script: script_error,
                        stage: Box::new(stage_error),
                    }),
                };
            }
            ledger
                .mark_stage(
                    session,
                    module,
                    fingerprint,
                    direction,
                    plan.version,
                    DirtyStage::ScriptDone,
                )
                .await?;
            ledger
                .append(session, module, fingerprint, migration)
                .await?;
            applied.push(Applied {
                version: migration.version(),
                checksum: migration.checksum().to_owned(),
            });
            ledger
                .ensure_barrier(session, module, fingerprint, migrations, &applied)
                .await?;
            ledger
                .clear_dirty(session, module, fingerprint, direction, plan.version)
                .await?;
        }
        let final_state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &final_state.persisted)?;
        Ok(Ledger::snapshot(module, &final_state))
    }

    async fn down_locked(
        &self,
        session: &mut ClickHouseSession,
        ledger: &Ledger,
        module: &str,
        fingerprint: &str,
        migrations: &Migrations,
        count: usize,
    ) -> Result<ModuleState, Error> {
        let state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::UnregisteredModule))?;
        if let Some(dirty) = state.dirty.clone() {
            return Err(Error::Dirty(dirty));
        }
        let history = validated_history(migrations, &state.persisted)?;
        let plans = migrations
            .plan_down(&history, fingerprint, count)
            .map_err(Error::Migration)?;
        for plan in plans {
            let migration = definition(migrations, plan.version)?;
            let direction = DirtyDirection::Down;
            let dirty = DirtyState {
                direction,
                stage: DirtyStage::Started,
                version: plan.version,
                checksum: migration.checksum().to_owned(),
            };
            ledger
                .mark_dirty(
                    session,
                    module,
                    fingerprint,
                    direction,
                    plan.version,
                    migration.checksum(),
                )
                .await?;
            if let Err(script_error) = session
                .execute_no_count(&plan.sql, QueryOptions::new())
                .await
            {
                return match ledger
                    .mark_stage(
                        session,
                        module,
                        fingerprint,
                        direction,
                        plan.version,
                        DirtyStage::ScriptFailed,
                    )
                    .await
                {
                    Ok(()) => Err(Error::Failed(
                        DirtyState {
                            stage: DirtyStage::ScriptFailed,
                            ..dirty
                        },
                        script_error,
                    )),
                    Err(stage_error) => Err(Error::FailedStage {
                        dirty: DirtyState {
                            stage: DirtyStage::Started,
                            ..dirty
                        },
                        script: script_error,
                        stage: Box::new(stage_error),
                    }),
                };
            }
            ledger
                .mark_stage(
                    session,
                    module,
                    fingerprint,
                    direction,
                    plan.version,
                    DirtyStage::ScriptDone,
                )
                .await?;
            ledger
                .remove(session, module, fingerprint, plan.version)
                .await?;
            ledger
                .clear_dirty(session, module, fingerprint, direction, plan.version)
                .await?;
        }
        let final_state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &final_state.persisted)?;
        Ok(Ledger::snapshot(module, &final_state))
    }

    async fn reconcile_locked(
        &self,
        session: &mut ClickHouseSession,
        ledger: &Ledger,
        module: &str,
        fingerprint: &str,
        migrations: &Migrations,
        outcome: ReconcileOutcome,
    ) -> Result<ModuleState, Error> {
        let state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::UnregisteredModule))?;
        let Some(dirty) = state.dirty.clone() else {
            return Err(Error::History(HistoryReason::NotDirty));
        };
        let history = reconciliation_history(migrations, &state.persisted, &dirty, fingerprint)?;
        let migration = definition(migrations, dirty.version)?;
        if migration.checksum() != dirty.checksum {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        let position = migrations
            .as_slice()
            .iter()
            .position(|candidate| candidate.version() == dirty.version)
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        let present = history
            .applied()
            .iter()
            .any(|applied| applied.version == dirty.version);
        let last = history.applied().last().map(|applied| applied.version);
        if present {
            if last != Some(dirty.version) {
                return Err(Error::History(HistoryReason::InconsistentRows));
            }
        } else if position != history.applied().len() {
            return Err(Error::History(HistoryReason::InconsistentRows));
        }
        match (dirty.direction, outcome, present) {
            (DirtyDirection::Up, ReconcileOutcome::Applied, false) => {
                ledger
                    .append(session, module, fingerprint, migration)
                    .await?;
                let mut applied = history.applied().to_vec();
                applied.push(Applied {
                    version: migration.version(),
                    checksum: migration.checksum().to_owned(),
                });
                ledger
                    .ensure_barrier(session, module, fingerprint, migrations, &applied)
                    .await?;
            }
            (DirtyDirection::Up, ReconcileOutcome::Applied, true) => {
                ledger
                    .ensure_barrier(session, module, fingerprint, migrations, history.applied())
                    .await?;
            }
            (DirtyDirection::Up, ReconcileOutcome::NotApplied, true) => {
                return Err(Error::History(HistoryReason::InconsistentRows));
            }
            (DirtyDirection::Up, ReconcileOutcome::NotApplied, false)
            | (DirtyDirection::Down, ReconcileOutcome::NotApplied, true)
            | (DirtyDirection::Down, ReconcileOutcome::Applied, false) => {}
            (DirtyDirection::Down, ReconcileOutcome::NotApplied, false) => {
                ledger
                    .append(session, module, fingerprint, migration)
                    .await?;
            }
            (DirtyDirection::Down, ReconcileOutcome::Applied, true) => {
                ledger
                    .remove(session, module, fingerprint, dirty.version)
                    .await?;
            }
        }
        let pending = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &pending.persisted)?;
        ledger
            .clear_dirty(session, module, fingerprint, dirty.direction, dirty.version)
            .await?;
        let final_state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &final_state.persisted)?;
        Ok(Ledger::snapshot(module, &final_state))
    }

    async fn require_atomic_database(&self, session: &mut (dyn Session + '_)) -> Result<(), Error> {
        let rows = session
            .fetch_all_async(
                "SELECT engine FROM system.databases WHERE name = {p1:String}",
                QueryOptions::new()
                    .bind(text(&self.database))
                    .limit(limit(1, 1024)),
            )
            .await
            .map_err(Error::Database)?;
        let engine = required_text(rows.first().ok_or(Error::UnsupportedDatabase)?, "engine")?;
        if engine == "Atomic" {
            Ok(())
        } else {
            Err(Error::UnsupportedDatabase)
        }
    }

    async fn acquire_lock(&self, session: &mut ClickHouseSession) -> Result<(), Error> {
        let sql = format!(
            "CREATE TABLE {} (owner String) ENGINE=Memory",
            qualified(&self.database, &self.lock_table_name())
        );
        match session
            .execute_no_count(&sql, QueryOptions::new().timeout(self.lock_timeout))
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if uncertain_kind(error.kind()) => Err(Error::LockOutcomeUnknown(error)),
            Err(error) => Err(Error::Lock(error)),
        }
    }

    async fn release_lock(&self, session: &mut ClickHouseSession) -> Result<(), Error> {
        session
            .execute_no_count(
                &format!(
                    "DROP TABLE {}",
                    qualified(&self.database, &self.lock_table_name())
                ),
                QueryOptions::new(),
            )
            .await
            .map_err(Error::Release)
    }
}

const fn native_migrations_enabled() -> bool {
    cfg!(feature = "experimental-native-migrations")
}

fn combine_release<T>(result: Result<T, Error>, release: Result<(), Error>) -> Result<T, Error> {
    match (result, release) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(_), Err(release)) => Err(release),
        (Err(operation), Err(release)) => Err(Error::OperationAndRelease {
            operation: Box::new(operation),
            release: Box::new(release),
        }),
    }
}

fn uncertain_kind(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::WriteOutcomeUnknown
            | ErrorKind::CommitOutcomeUnknown
            | ErrorKind::Timeout
            | ErrorKind::Cancelled
    )
}

fn retains_lock(error: &Error) -> bool {
    match error {
        Error::Failed(_, source) | Error::Database(source) => uncertain_kind(source.kind()),
        Error::FailedStage { script, stage, .. } => {
            uncertain_kind(script.kind()) || retains_lock(stage)
        }
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum Operation<'a> {
    Initialize,
    Up(&'a Migrations),
    Down(&'a Migrations, usize),
    Reconcile(&'a Migrations, ReconcileOutcome),
}

impl Operation<'_> {
    fn validate(self) -> Result<(), Error> {
        match self {
            Self::Initialize => Ok(()),
            Self::Up(migrations) | Self::Down(migrations, _) | Self::Reconcile(migrations, _) => {
                validate_migrations(migrations)
            }
        }
    }
}

fn validate_module(module: &str) -> Result<(), Error> {
    if module.is_empty() || module.len() > 255 || module.as_bytes().contains(&0) {
        Err(Error::Configuration)
    } else {
        Ok(())
    }
}

fn validate_common(module: &str, fingerprint: &str, managed_objects: &[&str]) -> Result<(), Error> {
    validate_module(module)?;
    if fingerprint.is_empty()
        || fingerprint.len() > 255
        || fingerprint.as_bytes().contains(&0)
        || managed_objects
            .iter()
            .any(|object| !valid_identifier(object))
    {
        return Err(Error::Configuration);
    }
    Ok(())
}

fn validate_migrations(migrations: &Migrations) -> Result<(), Error> {
    for migration in migrations.as_slice() {
        if migration.version() == 0
            || migration.version() > i64::MAX as u64
            || migration.up().as_bytes().contains(&0)
            || migration
                .down()
                .is_some_and(|sql| sql.as_bytes().contains(&0))
        {
            return Err(Error::InvalidDefinitions);
        }
        // This backend has no native multi-statement script runner yet. Reject the
        // entire plan, including down scripts, before acquiring a session or lock.
        neotales_dbapi_clickhouse::validate_no_count_statement(migration.up(), &[])
            .map_err(|_| Error::InvalidDefinitions)?;
        if let Some(down) = migration.down() {
            neotales_dbapi_clickhouse::validate_no_count_statement(down, &[])
                .map_err(|_| Error::InvalidDefinitions)?;
        }
    }
    Ok(())
}

fn validated_history(
    migrations: &Migrations,
    persisted: &PersistedHistory,
) -> Result<HistoryState, Error> {
    migrations
        .validate_persisted(persisted)
        .map_err(|error| match error {
            CoreError::BindingDrift => Error::History(HistoryReason::BindingMismatch),
            _ => Error::History(HistoryReason::InconsistentRows),
        })
}

// Only reconciliation may bridge the interval after a baseline history insert and before
// its barrier write. A previous Applied reconciliation can have inserted that row while the
// marker was Started or ScriptFailed, so all three known stages are possible. The temporary
// barrier is used solely for validation and is persisted only by the Applied branch.
fn reconciliation_history(
    migrations: &Migrations,
    persisted: &PersistedHistory,
    dirty: &DirtyState,
    fingerprint: &str,
) -> Result<HistoryState, Error> {
    if persisted.binding_fingerprint != fingerprint {
        return Err(Error::History(HistoryReason::BindingMismatch));
    }
    match validated_history(migrations, persisted) {
        Ok(history) => Ok(history),
        Err(error) => {
            if dirty.direction != DirtyDirection::Up
                || persisted.barrier.is_some()
                || persisted.applied.len() != 1
                || persisted.applied[0].version != dirty.version
                || persisted.applied[0].checksum != dirty.checksum
            {
                return Err(error);
            }
            let mut candidate = persisted.clone();
            candidate.barrier = candidate.applied.first().cloned();
            // The core validator proves the row matches the first baseline definition,
            // including version/checksum, and rejects non-baselines and all other drift.
            validated_history(migrations, &candidate).map_err(|_| error)
        }
    }
}

fn definition(
    migrations: &Migrations,
    version: u64,
) -> Result<&neotales_migration::Migration, Error> {
    migrations
        .as_slice()
        .iter()
        .find(|migration| migration.version() == version)
        .ok_or(Error::History(HistoryReason::InconsistentRows))
}

fn valid_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.len() <= 250
        && identifier.is_ascii()
        && identifier.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_lowercase() || (index > 0 && byte.is_ascii_digit())
        })
}

fn qualified(database: &str, table: &str) -> String {
    format!("`{database}`.`{table}`")
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("matching text type")
}

// ClickHouse String is arbitrary bytes. Ledger metadata is required to be UTF-8;
// reject malformed values rather than normalizing or replacing invalid bytes.
fn optional_text(row: &Row, name: &str) -> Result<Option<String>, Error> {
    row.get::<Option<Vec<u8>>>(name)
        .map_err(Error::Database)?
        .map(|bytes| {
            String::from_utf8(bytes)
                .map_err(|_| Error::Database(DbError::new(ErrorKind::WrongType)))
        })
        .transpose()
}

fn required_text(row: &Row, name: &str) -> Result<String, Error> {
    let bytes = row.get::<Vec<u8>>(name).map_err(Error::Database)?;
    String::from_utf8(bytes).map_err(|_| Error::Database(DbError::new(ErrorKind::WrongType)))
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("positive static limits")
}

#[cfg(test)]
mod tests {
    use super::{
        ClickHouseBackend, Error, combine_release, optional_text, required_text, retains_lock,
        valid_identifier,
    };
    use neotales_dbapi::{DbError, DbType, ErrorKind, Row, Value};
    use std::time::Duration;

    #[test]
    fn only_exact_dirty_baseline_gap_is_admitted_for_reconciliation() {
        use super::{
            DirtyDirection, DirtyStage, DirtyState, HistoryReason, reconciliation_history,
            validated_history,
        };
        use neotales_migration::{Applied, Migration, Migrations, PersistedHistory};
        let baseline = Migration::new(1, "base", "CREATE TABLE t (n Int64) ENGINE=Memory", None)
            .unwrap()
            .into_baseline();
        let plain =
            Migration::new(1, "base", "CREATE TABLE t (n Int64) ENGINE=Memory", None).unwrap();
        let second =
            Migration::new(2, "later", "CREATE TABLE u (n Int64) ENGINE=Memory", None).unwrap();
        let definitions = Migrations::new(vec![baseline.clone(), second.clone()]).unwrap();
        let persisted = PersistedHistory {
            applied: vec![Applied {
                version: 1,
                checksum: baseline.checksum().to_owned(),
            }],
            binding_fingerprint: "binding".into(),
            barrier: None,
        };
        let dirty = DirtyState {
            direction: DirtyDirection::Up,
            stage: DirtyStage::ScriptDone,
            version: 1,
            checksum: baseline.checksum().to_owned(),
        };
        assert!(
            validated_history(&definitions, &persisted).is_err(),
            "normal validation must remain strict"
        );
        assert!(reconciliation_history(&definitions, &persisted, &dirty, "binding").is_ok());
        assert!(matches!(
            reconciliation_history(&definitions, &persisted, &dirty, "other"),
            Err(Error::History(HistoryReason::BindingMismatch))
        ));
        let mut bad = dirty.clone();
        bad.stage = DirtyStage::Started;
        assert!(reconciliation_history(&definitions, &persisted, &bad, "binding").is_ok());
        bad.stage = DirtyStage::ScriptFailed;
        assert!(reconciliation_history(&definitions, &persisted, &bad, "binding").is_ok());
        bad = dirty.clone();
        bad.direction = DirtyDirection::Down;
        assert!(reconciliation_history(&definitions, &persisted, &bad, "binding").is_err());
        bad = dirty.clone();
        bad.checksum = "forged".into();
        assert!(reconciliation_history(&definitions, &persisted, &bad, "binding").is_err());
        bad = dirty.clone();
        bad.version = 2;
        assert!(reconciliation_history(&definitions, &persisted, &bad, "binding").is_err());
        let mut altered = persisted.clone();
        altered.applied[0].checksum = "forged".into();
        assert!(reconciliation_history(&definitions, &altered, &dirty, "binding").is_err());
        altered = persisted.clone();
        altered.applied.push(Applied {
            version: 2,
            checksum: second.checksum().to_owned(),
        });
        assert!(reconciliation_history(&definitions, &altered, &dirty, "binding").is_err());
        altered = persisted.clone();
        altered.barrier = Some(Applied {
            version: 2,
            checksum: "forged".into(),
        });
        assert!(reconciliation_history(&definitions, &altered, &dirty, "binding").is_err());
        let ordinary = Migrations::new(vec![plain.clone()]).unwrap();
        let mut nonbaseline = persisted.clone();
        nonbaseline.applied[0].checksum = plain.checksum().into();
        let mut ordinary_dirty = dirty.clone();
        ordinary_dirty.checksum = plain.checksum().into();
        assert!(
            reconciliation_history(&ordinary, &nonbaseline, &ordinary_dirty, "binding")
                .unwrap()
                .barrier()
                .is_none()
        );
    }

    #[test]
    fn ledger_text_requires_exact_utf8_bytes_and_preserves_nulls() {
        let row = Row::new(vec![
            ("valid".into(), Value::Bytes("héllo\0".as_bytes().to_vec())),
            ("invalid".into(), Value::Bytes(vec![0xff])),
            ("none".into(), Value::Null(DbType::Bytes)),
            ("wrong".into(), Value::Text("text".into())),
        ])
        .unwrap();
        assert_eq!(required_text(&row, "valid").unwrap(), "héllo\0");
        assert_eq!(
            optional_text(&row, "valid").unwrap().as_deref(),
            Some("héllo\0")
        );
        assert_eq!(optional_text(&row, "none").unwrap(), None);
        for name in ["invalid", "wrong"] {
            assert!(
                matches!(required_text(&row, name), Err(Error::Database(error)) if error.kind() == ErrorKind::WrongType)
            );
            assert!(
                matches!(optional_text(&row, name), Err(Error::Database(error)) if error.kind() == ErrorKind::WrongType)
            );
        }
        assert!(
            matches!(required_text(&row, "none"), Err(Error::Database(error)) if error.kind() == ErrorKind::Null)
        );
    }

    #[test]
    fn operation_and_release_failures_are_both_preserved() {
        let result: Result<(), Error> = combine_release(
            Err(Error::Database(DbError::new(ErrorKind::InvalidArgument))),
            Err(Error::Release(DbError::new(ErrorKind::WriteOutcomeUnknown))),
        );
        match result {
            Err(Error::OperationAndRelease { operation, release }) => {
                assert!(
                    matches!(*operation, Error::Database(ref e) if e.kind() == ErrorKind::InvalidArgument)
                );
                assert!(
                    matches!(*release, Error::Release(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown)
                );
            }
            other => panic!("expected both errors, got {other:?}"),
        }
    }

    #[test]
    fn unit_harness_does_not_bypass_feature_opt_in() {
        // Private configuration branch: a downstream test cannot detect a cfg(test) bypass.
        assert_eq!(
            super::native_migrations_enabled(),
            cfg!(feature = "experimental-native-migrations")
        );
    }

    #[test]
    fn configuration_rejects_invalid_identifiers_and_timeouts() {
        for (database, table) in [
            ("", "ledger"),
            ("neotales_test", ""),
            ("Neotales", "ledger"),
            ("neotales_test", "Ledger"),
            ("neotales_test", "ledger-meta"),
            ("system", "ledger"),
            ("information_schema", "ledger"),
        ] {
            assert!(
                ClickHouseBackend::new(database, table, Duration::from_secs(1)).is_err(),
                "{database}.{table}"
            );
        }
        assert!(ClickHouseBackend::new("neotales_test", "ledger", Duration::ZERO).is_err());
    }

    #[test]
    fn migration_preflight_rejects_every_unsupported_up_or_down_before_work() {
        use neotales_migration::{Migration, Migrations};
        let cases = [
            ("CREATE TABLE a (n Int64) ENGINE=Memory; DROP TABLE a", None),
            (
                "INSERT INTO a SELECT $$'$$ SETTINGS async_insert=1 -- '",
                None,
            ),
            (
                "CREATE TABLE a (n Int64) ENGINE=Memory",
                Some("DROP TABLE a; DROP TABLE b"),
            ),
            ("CREATE TABLE a (n Int64) ENGINE=Memory", Some("SELECT 1")),
            ("INSERT INTO a SELECT {p1:Int64}", None),
        ];
        for (up, down) in cases {
            let migrations = Migrations::new(vec![
                Migration::new(1, "test", up, down.map(str::to_owned)).unwrap(),
            ])
            .unwrap();
            assert!(matches!(
                super::Operation::Up(&migrations).validate(),
                Err(Error::InvalidDefinitions)
            ));
        }
        let valid = Migrations::new(vec![
            Migration::new(
                1,
                "test",
                "CREATE TABLE a (n Int64) ENGINE=Memory",
                Some("DROP TABLE a".into()),
            )
            .unwrap(),
        ])
        .unwrap();
        assert!(super::Operation::Up(&valid).validate().is_ok());
    }

    #[test]
    fn uncertain_errors_retain_the_lock() {
        assert!(retains_lock(&Error::Database(DbError::new(
            ErrorKind::WriteOutcomeUnknown
        ))));
        assert!(retains_lock(&Error::Database(DbError::new(
            ErrorKind::Cancelled
        ))));
        assert!(!retains_lock(&Error::Database(DbError::new(
            ErrorKind::InvalidArgument
        ))));
        assert!(retains_lock(&Error::FailedStage {
            dirty: crate::DirtyState {
                direction: crate::DirtyDirection::Up,
                stage: crate::DirtyStage::Started,
                version: 1,
                checksum: String::new(),
            },
            script: DbError::new(ErrorKind::InvalidArgument),
            stage: Box::new(Error::Database(DbError::new(
                ErrorKind::WriteOutcomeUnknown
            ))),
        }));
        assert!(!retains_lock(&Error::FailedStage {
            dirty: crate::DirtyState {
                direction: crate::DirtyDirection::Up,
                stage: crate::DirtyStage::Started,
                version: 1,
                checksum: String::new(),
            },
            script: DbError::new(ErrorKind::InvalidArgument),
            stage: Box::new(Error::Database(DbError::new(ErrorKind::InvalidArgument))),
        }));
    }

    #[test]
    fn lock_table_is_qualified_and_distinct() {
        let backend =
            ClickHouseBackend::new("neotales_test", "ledger", Duration::from_secs(1)).unwrap();
        assert_eq!(backend.lock_table(), "neotales_test.ledger_lock");
        assert!(valid_identifier("ledger_2"));
        assert!(!valid_identifier("2ledger"));
    }
    #[test]
    fn module_and_fingerprint_limits_are_bytes_and_reject_nul() {
        use super::{Error, validate_common};

        assert!(validate_common(&"a".repeat(255), &"b".repeat(255), &[]).is_ok());
        for (module, fingerprint) in [
            (String::new(), "ok".to_owned()),
            ("a".repeat(256), "ok".to_owned()),
            ("é".repeat(128), "ok".to_owned()),
            ("bad\0module".to_owned(), "ok".to_owned()),
            ("ok".to_owned(), String::new()),
            ("ok".to_owned(), "b".repeat(256)),
            ("ok".to_owned(), "bad\0fingerprint".to_owned()),
        ] {
            assert!(matches!(
                validate_common(&module, &fingerprint, &[]),
                Err(Error::Configuration)
            ));
        }
    }
}
