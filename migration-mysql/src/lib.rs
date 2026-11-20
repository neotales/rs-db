#![doc = include_str!("../README.md")]

mod ledger;

use ledger::Ledger;
use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, QueryOptions, RowLimit, Session, Value,
};
use neotales_dbapi_mysql::{MySqlPoolProvider, MySqlSession};
use neotales_migration::{
    Applied, Error as CoreError, FlattenManifest, HistoryState, Migrations, PersistedHistory,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// A dirty migration record that blocks further automated schema changes.
///
/// It is persisted before a script runs. A failure, an interruption, or a ledger
/// write failure leaves it in place; only [`MySqlBackend::reconcile`] clears it.
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
    /// The script returned success; the ledger row was not yet synchronized.
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
    /// The intended schema effect is absent. Leave the ledger row unchanged and clear the marker.
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
    /// An existing ledger table has no ownership marker or is not `InnoDB`.
    #[error("an existing ledger table has no ownership marker")]
    UnownedLedger,
    /// A ledger table shape, column type, collation, or ordinal is unsupported.
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

/// Failure from MySQL module-history management. Driver text and SQL are never retained.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An identifier, identity, or lock timeout is invalid, or the selected database differs.
    #[error("invalid backend configuration or selected database mismatch")]
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
    /// The advisory lock could not be acquired before the timeout.
    #[error("timed out waiting for the migration lock")]
    LockTimeout,
    /// Flatten would replace history without atomicity.
    #[error("flatten is unsupported because MySQL DDL is not transactional")]
    UnsupportedFlatten,
    /// Exclusive session acquisition failed.
    #[error("database session acquisition failed: {0}")]
    Acquire(#[source] DbError),
    /// Acquiring the advisory lock failed.
    #[error("database lock operation failed: {0}")]
    Lock(#[source] DbError),
    /// Releasing the advisory lock failed.
    #[error("database lock release failed: {0}")]
    Release(#[source] DbError),
    /// A ledger or migration operation failed; its outcome may be unknown.
    #[error("database operation failed: {0}")]
    Database(#[source] DbError),
}

pub(crate) struct LedgerState {
    pub(crate) persisted: PersistedHistory,
    pub(crate) dirty: Option<DirtyState>,
}

/// Validated MySQL ledger configuration.
///
/// The backend uses the current database's qualified tables and a server-wide
/// `GET_LOCK` name derived from `database.table`. It does not use a transaction
/// around migration scripts because MySQL implicitly commits most DDL.
///
/// Every method takes the concrete [`MySqlPoolProvider`] rather than `&dyn Pool`.
/// Session-level `GET_LOCK` safety on cancellation depends on that provider
/// discarding the physical connection when a future is dropped. A generic pool
/// could return a session that leaks its server lock.
#[derive(Clone, Debug)]
pub struct MySqlBackend {
    database: String,
    table: String,
    lock_timeout: Duration,
}

impl MySqlBackend {
    /// Constructs a backend for `database.table`.
    ///
    /// Identifiers must match `[a-z_][a-z0-9_]*`, fit MySQL's 64-byte limit, and
    /// `table` must leave room for the `_meta` suffix. The database cannot be one
    /// of MySQL's system schemas. `lock_timeout` must be a positive whole number
    /// of seconds because `GET_LOCK` takes seconds.
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
        let seconds = lock_timeout.as_secs();
        if !valid_identifier(&database)
            || !valid_identifier(&table)
            || table.len() > 59
            || matches!(
                database.as_str(),
                "mysql" | "information_schema" | "performance_schema" | "sys"
            )
            || seconds == 0
            || seconds > i32::MAX as u64
            || lock_timeout.subsec_nanos() != 0
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            database,
            table,
            lock_timeout,
        })
    }

    /// Returns the stable advisory-lock name used by cooperating writers.
    #[must_use]
    pub fn lock_name(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"neotales-migration-mysql/lock/v1\0");
        digest.update((self.database.len() as u64).to_be_bytes());
        digest.update(self.database.as_bytes());
        digest.update((self.table.len() as u64).to_be_bytes());
        digest.update(self.table.as_bytes());
        let hash = digest.finalize();
        let mut name = String::from("neotales-mm:");
        for byte in &hash[..24] {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("write to String");
        }
        name
    }

    /// Registers an empty module after rejecting declared pre-existing objects.
    ///
    /// # Errors
    /// Returns configuration/history/untracked-object/lock/database errors. A dirty
    /// ledger is returned as [`Error::Dirty`]; use [`Self::state`] and
    /// [`Self::reconcile`] for recovery.
    pub async fn initialize_module(
        &self,
        pool: &MySqlPoolProvider,
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

    /// Applies pending migrations one script at a time, persisting a dirty marker
    /// before each script and its ledger row after it.
    ///
    /// MySQL DDL implicitly commits. A script that fails after some statements may
    /// leave partial schema effects; the dirty marker blocks all further automated
    /// changes until an operator reconciles. Applied scripts are never silently
    /// retried. Repeating after a clean success is safe and does not rerun them.
    ///
    /// # Errors
    /// Returns configuration/history/untracked-object/lock/database errors,
    /// [`Error::Dirty`] when a marker already exists, or [`Error::Failed`] when a
    /// script fails. Inspect [`Self::state`] and call [`Self::reconcile`].
    pub async fn up(
        &self,
        pool: &MySqlPoolProvider,
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
    /// All down scripts and the baseline barrier are preflighted before the first
    /// dirty marker. This is **not** idempotent: never blindly repeat after an
    /// uncertain failure. Reconcile by inspecting the schema and using
    /// [`Self::reconcile`].
    ///
    /// # Errors
    /// Returns unregistered/history/planning/lock/database errors, [`Error::Dirty`],
    /// or [`Error::Failed`].
    pub async fn down(
        &self,
        pool: &MySqlPoolProvider,
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

    /// Reads one module's detached state under the same advisory lock.
    ///
    /// Returns `None` for an unregistered module. A dirty marker is returned in the
    /// snapshot instead of failing, because this is the recovery inspection API.
    ///
    /// # Errors
    /// Returns configuration/history/lock/database errors.
    pub async fn state(
        &self,
        pool: &MySqlPoolProvider,
        module: &str,
    ) -> Result<Option<ModuleState>, Error> {
        validate_module(module)?;
        let mut session = pool.acquire_mysql_session().await.map_err(Error::Acquire)?;
        let mut guard = DiscardOnDrop::new(&mut session);
        let lock = self.lock_name();
        acquire_lock(guard.session_mut(), &lock, self.lock_timeout).await?;
        let result = self.state_locked(guard.session_mut(), module).await;
        let release = release_lock(guard.session_mut(), &lock).await;
        match result {
            Ok(state) => {
                release?;
                guard.disarm();
                Ok(state)
            }
            Err(error) => {
                if release.is_ok() {
                    guard.disarm();
                }
                Err(error)
            }
        }
    }

    async fn state_locked(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
    ) -> Result<Option<ModuleState>, Error> {
        ensure_database(session, &self.database).await?;
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
    /// `Applied` records an apply or removes a reverted down migration. `NotApplied`
    /// leaves history unchanged. The caller is asserting the schema effect; the
    /// provider cannot verify arbitrary DDL.
    ///
    /// # Errors
    /// Returns unregistered/not-dirty/history/mismatch/lock/database errors.
    pub async fn reconcile(
        &self,
        pool: &MySqlPoolProvider,
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

    /// Flatten is rejected because replacing history plus a baseline cannot be made
    /// atomic when MySQL DDL implicitly commits.
    ///
    /// # Errors
    /// Always returns [`Error::UnsupportedFlatten`].
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)] // Kept async for parity with the other backends.
    pub async fn flatten(
        &self,
        _pool: &MySqlPoolProvider,
        _module: &str,
        _manifest: &FlattenManifest,
        _fingerprint: &str,
        _managed_objects: &[&str],
    ) -> Result<ModuleState, Error> {
        Err(Error::UnsupportedFlatten)
    }

    async fn run(
        &self,
        pool: &MySqlPoolProvider,
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        validate_common(module, fingerprint, managed_objects)?;
        operation.validate()?;
        let mut session = pool.acquire_mysql_session().await.map_err(Error::Acquire)?;
        let mut guard = DiscardOnDrop::new(&mut session);
        let lock = self.lock_name();
        acquire_lock(guard.session_mut(), &lock, self.lock_timeout).await?;
        let result = self
            .run_locked(
                guard.session_mut(),
                module,
                fingerprint,
                managed_objects,
                operation,
            )
            .await;
        let release = release_lock(guard.session_mut(), &lock).await;
        match result {
            Ok(state) => {
                release?;
                guard.disarm();
                Ok(state)
            }
            Err(error) => {
                if release.is_ok() {
                    guard.disarm();
                }
                Err(error)
            }
        }
    }

    async fn run_locked(
        &self,
        session: &mut (dyn Session + '_),
        module: &str,
        fingerprint: &str,
        managed_objects: &[&str],
        operation: Operation<'_>,
    ) -> Result<ModuleState, Error> {
        ensure_database(session, &self.database).await?;
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

    async fn up_locked(
        &self,
        session: &mut (dyn Session + '_),
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
                    direction,
                    plan.version,
                    migration.checksum(),
                )
                .await?;
            if let Err(error) = session
                .execute_trusted_script_async(&plan.sql, QueryOptions::new())
                .await
            {
                let failed_stage = if ledger
                    .mark_stage(
                        session,
                        module,
                        direction,
                        plan.version,
                        DirtyStage::ScriptFailed,
                    )
                    .await
                    .is_ok()
                {
                    DirtyStage::ScriptFailed
                } else {
                    DirtyStage::Started
                };
                return Err(Error::Failed(
                    DirtyState {
                        stage: failed_stage,
                        ..dirty
                    },
                    error,
                ));
            }
            ledger
                .mark_stage(
                    session,
                    module,
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
                .clear_dirty(session, module, direction, plan.version)
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
        session: &mut (dyn Session + '_),
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
                    direction,
                    plan.version,
                    migration.checksum(),
                )
                .await?;
            if let Err(error) = session
                .execute_trusted_script_async(&plan.sql, QueryOptions::new())
                .await
            {
                let failed_stage = if ledger
                    .mark_stage(
                        session,
                        module,
                        direction,
                        plan.version,
                        DirtyStage::ScriptFailed,
                    )
                    .await
                    .is_ok()
                {
                    DirtyStage::ScriptFailed
                } else {
                    DirtyStage::Started
                };
                return Err(Error::Failed(
                    DirtyState {
                        stage: failed_stage,
                        ..dirty
                    },
                    error,
                ));
            }
            ledger
                .mark_stage(
                    session,
                    module,
                    direction,
                    plan.version,
                    DirtyStage::ScriptDone,
                )
                .await?;
            ledger.remove(session, module, plan.version).await?;
            ledger
                .clear_dirty(session, module, direction, plan.version)
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
        session: &mut (dyn Session + '_),
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
        let history = validated_history(migrations, &state.persisted)?;
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
        // The dirty version must be the history head when present, and exactly the next
        // migration when absent. This applies to every outcome; a marker that does not
        // describe the persisted history is drift and is never cleared.
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
                // A crash can occur after the down script and history delete but before the
                // dirty marker is cleared. If the operator says the down did not take effect,
                // restore the history row instead of clearing the marker over a missing row.
                ledger
                    .append(session, module, fingerprint, migration)
                    .await?;
            }
            (DirtyDirection::Down, ReconcileOutcome::Applied, true) => {
                ledger.remove(session, module, dirty.version).await?;
            }
        }
        let pending = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &pending.persisted)?;
        ledger
            .clear_dirty(session, module, dirty.direction, dirty.version)
            .await?;
        let final_state = ledger
            .read_module(session, module, Some(fingerprint))
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        validated_history(migrations, &final_state.persisted)?;
        Ok(Ledger::snapshot(module, &final_state))
    }
}

/// Discards the physical connection on every cancellation path after lock acquisition, including
/// cancellation between `GET_LOCK` success and the next operation.
struct DiscardOnDrop<'a> {
    session: &'a mut MySqlSession,
    armed: bool,
}

impl<'a> DiscardOnDrop<'a> {
    fn new(session: &'a mut MySqlSession) -> Self {
        Self {
            session,
            armed: true,
        }
    }

    fn session_mut(&mut self) -> &mut (dyn Session + '_) {
        self.session
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for DiscardOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.session.discard();
        }
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
        && identifier.len() <= 64
        && identifier.is_ascii()
        && identifier.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_lowercase() || (index > 0 && byte.is_ascii_digit())
        })
}

// Check the selected schema on the same locked physical session before any ledger access.
async fn ensure_database(session: &mut (dyn Session + '_), database: &str) -> Result<(), Error> {
    let rows = session
        .fetch_all_async(
            "SELECT DATABASE() AS selected_database",
            QueryOptions::new().limit(limit(1, 1024)),
        )
        .await
        .map_err(Error::Database)?;
    let row = rows
        .first()
        .ok_or_else(|| Error::Database(DbError::new(ErrorKind::Decode)))?;
    let selected: Option<String> = row.get_ordinal(0).map_err(Error::Database)?;
    if selected.as_deref() == Some(database) {
        Ok(())
    } else {
        Err(Error::Configuration)
    }
}

async fn acquire_lock(
    session: &mut (dyn Session + '_),
    name: &str,
    timeout: Duration,
) -> Result<(), Error> {
    let seconds = i64::try_from(timeout.as_secs()).map_err(|_| Error::Configuration)?;
    let rows = session
        .fetch_all_async(
            "SELECT GET_LOCK(?, ?) AS acquired",
            QueryOptions::new()
                .bind_all(&[text(name), integer(seconds)])
                .limit(limit(1, 1024)),
        )
        .await
        .map_err(Error::Lock)?;
    let row = rows
        .first()
        .ok_or_else(|| Error::Lock(DbError::new(ErrorKind::Decode)))?;
    // Read the only projected column by position. A proxy such as Vitess `vtgate` may rewrite
    // the `AS acquired` label for a function expression. The projection has exactly one column,
    // so its ordinal is unambiguous.
    match row.get_ordinal::<Option<i64>>(0).map_err(Error::Lock)? {
        Some(1) => Ok(()),
        Some(0) => Err(Error::LockTimeout),
        _ => Err(Error::Lock(DbError::new(ErrorKind::Decode))),
    }
}

async fn release_lock(session: &mut (dyn Session + '_), name: &str) -> Result<(), Error> {
    let rows = session
        .fetch_all_async(
            "SELECT RELEASE_LOCK(?) AS released",
            QueryOptions::new().bind(text(name)).limit(limit(1, 1024)),
        )
        .await
        .map_err(Error::Release)?;
    let row = rows
        .first()
        .ok_or_else(|| Error::Release(DbError::new(ErrorKind::Decode)))?;
    // Positional for the same reason as `acquire_lock`: the single projected column is stable
    // even when a proxy rewrites the `AS released` label.
    match row.get_ordinal::<Option<i64>>(0).map_err(Error::Release)? {
        Some(1) => Ok(()),
        _ => Err(Error::Release(DbError::new(ErrorKind::Decode))),
    }
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

#[cfg(test)]
mod tests {
    use super::{MySqlBackend, valid_identifier};
    use std::time::Duration;

    #[test]
    fn configuration_rejects_invalid_identifiers_and_timeouts() {
        for (database, table) in [
            ("", "ledger"),
            ("neotales_test", ""),
            ("Neotales", "ledger"),
            ("neotales_test", "Ledger"),
            ("neotales_test", "ledger-meta"),
            ("mysql", "ledger"),
            ("information_schema", "ledger"),
            ("neotales_test", "a"),
        ] {
            let result = MySqlBackend::new(database, table, Duration::from_secs(1));
            if database == "neotales_test" && table == "a" {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err(), "{database}.{table}");
            }
        }
        assert!(MySqlBackend::new("neotales_test", "ledger", Duration::ZERO).is_err());
        assert!(MySqlBackend::new("neotales_test", "ledger", Duration::from_millis(1500)).is_err());
        assert!(
            MySqlBackend::new("neotales_test", "a".repeat(60), Duration::from_secs(1)).is_err()
        );
    }

    #[test]
    fn lock_name_is_stable_bounded_and_distinct() {
        let first = MySqlBackend::new("neotales_test", "ledger", Duration::from_secs(1)).unwrap();
        let second = MySqlBackend::new("neotales_test", "ledger", Duration::from_secs(1)).unwrap();
        let other = MySqlBackend::new("neotales_test", "other", Duration::from_secs(1)).unwrap();
        assert_eq!(first.lock_name(), second.lock_name());
        assert_ne!(first.lock_name(), other.lock_name());
        assert!(first.lock_name().len() <= 64);
        assert!(first.lock_name().starts_with("neotales-mm:"));
    }

    #[test]
    fn identifiers_are_conservative() {
        assert!(valid_identifier("a"));
        assert!(valid_identifier("_a1"));
        assert!(!valid_identifier("1a"));
        assert!(!valid_identifier("A"));
        assert!(!valid_identifier("a-b"));
        assert!(!valid_identifier(&"a".repeat(65)));
    }
}
