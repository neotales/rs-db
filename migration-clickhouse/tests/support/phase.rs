//! Fixed-schema fixture diagnostics. Never retain arguments, SQL, rows, panic payloads,
//! driver messages, or Error display/debug output. Results are forwarded unchanged.
use neotales_dbapi::{DbError, ErrorKind, QueryOptions, Row, Session as _};
use neotales_dbapi_clickhouse::{ClickHousePoolProvider, ClickHouseSession};
use neotales_migration::{FlattenManifest, Migrations};
use neotales_migration_clickhouse::{
    ClickHouseBackend as NativeBackend, Error, ModuleState, ReconcileOutcome,
};
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Case {
    Unscoped,
    Bootstrap,
    Disabled,
    SqlGuard,
    OrphanHistory,
    LockCreateLoss,
    LockReleaseLoss,
    Ordered,
    DirtyRecovery,
    LockContention,
    ConcurrentWriters,
    NonAtomic,
    ObservedWrites,
    ScriptTimeout,
    ScriptCancellation,
    StageWriteLoss,
    BaselineHistoryLoss,
    BaselineEarlyRecovery,
    DownHistoryRestore,
    Untracked,
    Unowned,
    PartialLedger,
    SchemaDrift,
    EngineDrift,
    Flatten,
    BetweenQueries,
    ClearBoundary,
    RestartDirty,
    RestartHistory,
    RestartDown,
    RestartScript,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    ContainerStart,
    PortIsolation,
    BridgeAddress,
    ContainerRestart,
    Reconnect,
    Readiness,
    BackendConfiguration,
    Initialize,
    Up,
    Down,
    State,
    Reconcile,
    Flatten,
    Acquire,
    FixtureFetch,
    FixtureExecute,
    ScalarDecode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    Unlocalized,
    Validation,
    Acquisition,
    LockCreate,
    LockRelease,
    Script,
    ScriptAndStage,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Class {
    Configuration,
    InvalidDefinitions,
    Migration,
    UntrackedObjects,
    History,
    Dirty,
    Failed,
    FailedStage,
    LockTimeout,
    UnsupportedFlatten,
    UnsupportedDatabase,
    Acquire,
    Lock,
    LockOutcomeUnknown,
    Release,
    OperationAndRelease,
    Database,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Primary,
    Operation,
    Release,
    StageWrite,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Begin,
    Ok,
    DbError(ErrorKind),
    Error(Class, Option<ErrorKind>, Role),
    InterruptedOrPanicked,
    ErrorTreeTruncated,
}
#[derive(Clone, Copy, Debug)]
struct Event {
    sequence: u64,
    phase: Phase,
    boundary: Boundary,
    elapsed_us: u128,
    status: Status,
}
#[derive(Default)]
struct Buffer {
    sequence: u64,
    dropped: usize,
    events: VecDeque<Event>,
}
#[derive(Clone)]
struct Context {
    case: Case,
    started: Instant,
    buffer: Arc<Mutex<Buffer>>,
}
tokio::task_local! { static CONTEXT: Context; }
#[cfg(test)]
tokio::task_local! { static STOP_BEFORE_CONTAINER: bool; }
#[cfg(test)]
struct EntryBoundaryReached;

impl Context {
    fn new(case: Case) -> Self {
        Self {
            case,
            started: Instant::now(),
            buffer: Arc::new(Mutex::new(Buffer::default())),
        }
    }
    fn current() -> Self {
        CONTEXT
            .try_with(Clone::clone)
            .unwrap_or_else(|_| Self::new(Case::Unscoped))
    }
    fn record(&self, sequence: u64, phase: Phase, boundary: Boundary, status: Status) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffer.events.len() == 128 {
            buffer.events.pop_front();
            buffer.dropped += 1;
        }
        buffer.events.push_back(Event {
            sequence,
            phase,
            boundary,
            elapsed_us: self.started.elapsed().as_micros(),
            status,
        });
    }
    fn begin(&self, phase: Phase) -> Operation {
        let sequence = {
            let mut buffer = self
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer.sequence += 1;
            buffer.sequence
        };
        self.record(sequence, phase, Boundary::Unlocalized, Status::Begin);
        #[cfg(test)]
        if phase == Phase::ContainerStart
            && STOP_BEFORE_CONTAINER
                .try_with(|stop| *stop)
                .unwrap_or(false)
        {
            std::panic::panic_any(EntryBoundaryReached);
        }
        Operation {
            context: self.clone(),
            sequence,
            phase,
            completed: false,
        }
    }
}

pub(super) struct Operation {
    context: Context,
    sequence: u64,
    phase: Phase,
    completed: bool,
}
impl Operation {
    pub(super) fn ok(mut self) {
        self.context
            .record(self.sequence, self.phase, Boundary::Unlocalized, Status::Ok);
        self.completed = true;
    }
    fn db<T>(mut self, result: &Result<T, DbError>) {
        self.context.record(
            self.sequence,
            self.phase,
            Boundary::Unlocalized,
            result
                .as_ref()
                .map_or_else(|error| Status::DbError(error.kind()), |_| Status::Ok),
        );
        self.completed = true;
    }
    fn backend<T>(mut self, result: &Result<T, Error>) {
        match result {
            Ok(_) => {
                self.context
                    .record(self.sequence, self.phase, Boundary::Unlocalized, Status::Ok);
            }
            Err(error) => self.error(error, Role::Primary, 0),
        }
        self.completed = true;
    }
    fn error(&self, error: &Error, role: Role, depth: usize) {
        if depth == 8 {
            self.context.record(
                self.sequence,
                self.phase,
                Boundary::Unlocalized,
                Status::ErrorTreeTruncated,
            );
            return;
        }
        let (class, boundary, kind) = match error {
            Error::Configuration => (Class::Configuration, Boundary::Validation, None),
            Error::InvalidDefinitions => (Class::InvalidDefinitions, Boundary::Validation, None),
            Error::Migration(_) => (Class::Migration, Boundary::Validation, None),
            Error::UntrackedObjects => (Class::UntrackedObjects, Boundary::Validation, None),
            Error::History(_) => (Class::History, Boundary::Validation, None),
            Error::Dirty(_) => (Class::Dirty, Boundary::Validation, None),
            Error::Failed(_, source) => (Class::Failed, Boundary::Script, Some(source.kind())),
            Error::FailedStage { script, .. } => (
                Class::FailedStage,
                Boundary::ScriptAndStage,
                Some(script.kind()),
            ),
            Error::LockTimeout => (Class::LockTimeout, Boundary::LockCreate, None),
            Error::UnsupportedFlatten => (Class::UnsupportedFlatten, Boundary::Validation, None),
            Error::UnsupportedDatabase => (Class::UnsupportedDatabase, Boundary::Validation, None),
            Error::Acquire(source) => (Class::Acquire, Boundary::Acquisition, Some(source.kind())),
            Error::Lock(source) => (Class::Lock, Boundary::LockCreate, Some(source.kind())),
            Error::LockOutcomeUnknown(source) => (
                Class::LockOutcomeUnknown,
                Boundary::LockCreate,
                Some(source.kind()),
            ),
            Error::Release(source) => (Class::Release, Boundary::LockRelease, Some(source.kind())),
            Error::OperationAndRelease { .. } => {
                (Class::OperationAndRelease, Boundary::Unlocalized, None)
            }
            Error::Database(source) => {
                (Class::Database, Boundary::Unlocalized, Some(source.kind()))
            }
        };
        self.context.record(
            self.sequence,
            self.phase,
            boundary,
            Status::Error(class, kind, role),
        );
        match error {
            Error::FailedStage { stage, .. } => self.error(stage, Role::StageWrite, depth + 1),
            Error::OperationAndRelease { operation, release } => {
                self.error(operation, Role::Operation, depth + 1);
                self.error(release, Role::Release, depth + 1);
            }
            _ => {}
        }
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if !self.completed {
            self.context.record(
                self.sequence,
                self.phase,
                Boundary::Unlocalized,
                Status::InterruptedOrPanicked,
            );
        }
    }
}

pub(super) fn begin(phase: Phase) -> Operation {
    Context::current().begin(phase)
}

struct CaseGuard {
    context: Context,
    completed: bool,
}
impl Drop for CaseGuard {
    fn drop(&mut self) {
        let status = if self.completed {
            "complete"
        } else {
            "interrupted-or-panicked"
        };
        eprintln!(
            "fixture-case case={:?} status={status} elapsed-us={}",
            self.context.case,
            self.context.started.elapsed().as_micros()
        );
        if !self.completed {
            let (dropped, events) = {
                let buffer = self
                    .context
                    .buffer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    buffer.dropped,
                    buffer.events.iter().copied().collect::<Vec<_>>(),
                )
            };
            eprintln!(
                "fixture-trace case={:?} retained={} dropped={}",
                self.context.case,
                events.len(),
                dropped
            );
            for event in &events {
                eprintln!(
                    "fixture-phase case={:?} sequence={} phase={:?} boundary={:?} elapsed-us={} status={:?}",
                    self.context.case,
                    event.sequence,
                    event.phase,
                    event.boundary,
                    event.elapsed_us,
                    event.status
                );
            }
        }
    }
}

pub(super) fn case<T>(case: Case, work: impl Future<Output = T>) -> impl Future<Output = T> {
    // Box BEFORE constructing/polling the async scope. Boxing only the outer case
    // future still copied the full enabled-contract state through nested scopes,
    // overflowing the default test-thread stack on the external Rust 1.100 compiler.
    let work = Box::pin(work);
    async move {
        let context = Context::new(case);
        eprintln!("fixture-case case={case:?} status=begin");
        CONTEXT
            .scope(context.clone(), async move {
                let mut guard = CaseGuard {
                    context,
                    completed: false,
                };
                let result = work.await;
                guard.completed = true;
                result
            })
            .await
    }
}

pub(super) fn spawn<F>(work: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(CONTEXT.scope(Context::current(), work))
}

pub(super) fn db_result<T>(
    phase: Phase,
    work: impl FnOnce() -> Result<T, DbError>,
) -> Result<T, DbError> {
    let operation = begin(phase);
    let result = work();
    operation.db(&result);
    result
}

pub(super) async fn readiness(
    config: neotales_dbapi_clickhouse::ClickHouseConfig,
) -> Result<ClickHousePoolProvider, DbError> {
    let operation = begin(Phase::Readiness);
    let result = ClickHousePoolProvider::connect(config).await;
    operation.db(&result);
    result
}

pub(super) async fn acquire(provider: &ClickHousePoolProvider) -> Result<ObservedSession, DbError> {
    let context = Context::current();
    let operation = context.begin(Phase::Acquire);
    let result = provider.acquire_clickhouse().await;
    operation.db(&result);
    result.map(|inner| ObservedSession { inner, context })
}

pub(super) struct ObservedSession {
    inner: ClickHouseSession,
    context: Context,
}
impl ObservedSession {
    pub(super) async fn execute_async(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<u64, DbError> {
        let operation = self.context.begin(Phase::FixtureExecute);
        let result = self.inner.execute_async(sql, options).await;
        operation.db(&result);
        result
    }
    pub(super) async fn execute_no_count(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<(), DbError> {
        let operation = self.context.begin(Phase::FixtureExecute);
        let result = self.inner.execute_no_count(sql, options).await;
        operation.db(&result);
        result
    }
    pub(super) async fn fetch_all_async(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Vec<Row>, DbError> {
        let operation = self.context.begin(Phase::FixtureFetch);
        let result = self.inner.fetch_all_async(sql, options).await;
        operation.db(&result);
        result
    }
}

#[derive(Clone)]
pub(super) struct Backend {
    inner: NativeBackend,
    context: Context,
}
impl Backend {
    pub(super) fn new(
        database: impl Into<String>,
        table: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let context = Context::current();
        let operation = context.begin(Phase::BackendConfiguration);
        let result = NativeBackend::new(database, table, timeout);
        operation.backend(&result);
        result.map(|inner| Self { inner, context })
    }
    pub(super) fn lock_table(&self) -> String {
        self.inner.lock_table()
    }
    async fn observe<T>(
        &self,
        phase: Phase,
        work: impl Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        let operation = self.context.begin(phase);
        let result = work.await;
        operation.backend(&result);
        result
    }
    pub(super) async fn initialize_module(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        fingerprint: &str,
        objects: &[&str],
    ) -> Result<ModuleState, Error> {
        self.observe(
            Phase::Initialize,
            self.inner
                .initialize_module(pool, module, fingerprint, objects),
        )
        .await
    }
    pub(super) async fn up(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        objects: &[&str],
    ) -> Result<ModuleState, Error> {
        self.observe(
            Phase::Up,
            self.inner
                .up(pool, module, migrations, fingerprint, objects),
        )
        .await
    }
    pub(super) async fn down(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        count: usize,
        objects: &[&str],
    ) -> Result<ModuleState, Error> {
        self.observe(
            Phase::Down,
            self.inner
                .down(pool, module, migrations, fingerprint, count, objects),
        )
        .await
    }
    pub(super) async fn state(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
    ) -> Result<Option<ModuleState>, Error> {
        self.observe(Phase::State, self.inner.state(pool, module))
            .await
    }
    pub(super) async fn reconcile(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        migrations: &Migrations,
        fingerprint: &str,
        outcome: ReconcileOutcome,
    ) -> Result<ModuleState, Error> {
        self.observe(
            Phase::Reconcile,
            self.inner
                .reconcile(pool, module, migrations, fingerprint, outcome),
        )
        .await
    }
    pub(super) async fn flatten(
        &self,
        pool: &ClickHousePoolProvider,
        module: &str,
        manifest: &FlattenManifest,
        fingerprint: &str,
        objects: &[&str],
    ) -> Result<ModuleState, Error> {
        self.observe(
            Phase::Flatten,
            self.inner
                .flatten(pool, module, manifest, fingerprint, objects),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Private entry/scoping lifecycle: exercise the real full enabled future up to
    // its first effect, without Docker, SQL execution, or any fixture being started.
    #[tokio::test]
    async fn enabled_entry_reaches_fixture_boundary_without_stack_overflow() {
        let task = tokio::spawn(
            STOP_BEFORE_CONTAINER.scope(true, super::super::clickhouse_migration_contract()),
        );
        let error = task
            .await
            .expect_err("entry must stop before starting a container");
        assert!(error.is_panic());
        assert!(error.into_panic().is::<EntryBoundaryReached>());
    }

    // Private allocation boundary: a large captured state must not be copied into
    // the returned async scope; this checks the regression without polling or Docker.
    #[test]
    fn case_boxes_work_before_constructing_scope_future() {
        let state = [0_u8; 8192];
        let work = async move {
            std::hint::black_box(state);
        };
        let scoped = case(Case::Bootstrap, work);
        assert!(std::mem::size_of_val(&scoped) < 1024);
    }

    // Private error-tree and serialization branches must preserve both sanitized causes.
    #[test]
    fn fixed_schema_retains_nested_kinds_without_backend_arguments() {
        let context = Context::new(Case::RestartHistory);
        let result: Result<(), Error> = Err(Error::OperationAndRelease {
            operation: Box::new(Error::LockOutcomeUnknown(DbError::new(
                ErrorKind::WriteOutcomeUnknown,
            ))),
            release: Box::new(Error::Release(DbError::new(ErrorKind::Timeout))),
        });
        context.begin(Phase::State).backend(&result);
        assert!(matches!(result, Err(Error::OperationAndRelease { .. })));
        let buffer = context.buffer.lock().unwrap();
        assert!(
            buffer
                .events
                .iter()
                .any(|event| event.boundary == Boundary::LockCreate
                    && event.status
                        == Status::Error(
                            Class::LockOutcomeUnknown,
                            Some(ErrorKind::WriteOutcomeUnknown),
                            Role::Operation
                        ))
        );
        assert!(
            buffer
                .events
                .iter()
                .any(|event| event.boundary == Boundary::LockRelease
                    && event.status
                        == Status::Error(Class::Release, Some(ErrorKind::Timeout), Role::Release))
        );
        drop(buffer);
        let backend = Backend::new(
            "synthetic_private_database",
            "synthetic_private_table",
            Duration::from_secs(2),
        )
        .unwrap();
        let serialized = format!("{:?}", backend.context.buffer.lock().unwrap().events);
        assert!(!serialized.contains("synthetic_private"));
    }

    // Private lifecycle branch: a spawned writer and panic unwinding must not erase context.
    #[tokio::test]
    async fn spawned_writer_retains_case_and_typed_failure_before_panic() {
        let context = Context::new(Case::RestartDown);
        let retained = context.clone();
        let task = tokio::spawn(CONTEXT.scope(context, async {
            let child = spawn(async {
                let context = Context::current();
                assert_eq!(context.case, Case::RestartDown);
                let result: Result<(), Error> = Err(Error::LockOutcomeUnknown(DbError::new(
                    ErrorKind::WriteOutcomeUnknown,
                )));
                context.begin(Phase::State).backend(&result);
                let _unfinished = context.begin(Phase::FixtureFetch);
                panic!("synthetic fixed panic");
            });
            assert!(child.await.unwrap_err().is_panic());
        }));
        task.await.unwrap();
        let buffer = retained.buffer.lock().unwrap();
        assert!(buffer.events.iter().any(|event| matches!(
            event.status,
            Status::Error(
                Class::LockOutcomeUnknown,
                Some(ErrorKind::WriteOutcomeUnknown),
                Role::Primary
            )
        )));
        assert!(
            buffer
                .events
                .iter()
                .any(|event| event.phase == Phase::FixtureFetch
                    && event.status == Status::InterruptedOrPanicked)
        );
    }

    // Private case-guard unwind branch: exercise the same scope and forwarding path
    // used by the ignored exact contract, without needing a database failure.
    #[tokio::test]
    async fn case_scope_retains_unexpected_lock_failure_during_unwind() {
        let (sent, received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(case(Case::RestartHistory, async move {
            assert!(sent.send(Context::current()).is_ok());
            let backend =
                Backend::new("default", "synthetic_private_table", Duration::from_secs(2)).unwrap();
            let result: Result<(), Error> = backend
                .observe(Phase::State, async {
                    Err(Error::LockOutcomeUnknown(DbError::new(
                        ErrorKind::WriteOutcomeUnknown,
                    )))
                })
                .await;
            assert!(result.is_ok(), "synthetic unexpected typed failure");
        }));
        assert!(task.await.unwrap_err().is_panic());
        let context = received.await.unwrap();
        assert_eq!(context.case, Case::RestartHistory);
        let buffer = context.buffer.lock().unwrap();
        assert_eq!(
            buffer.events.back().unwrap().status,
            Status::Error(
                Class::LockOutcomeUnknown,
                Some(ErrorKind::WriteOutcomeUnknown),
                Role::Primary,
            )
        );
        assert_eq!(buffer.events.back().unwrap().boundary, Boundary::LockCreate);
    }

    // Private bounded-buffer branch: overflow cannot grow diagnostics without limit.
    #[test]
    fn trace_ring_retains_latest_failure_with_bounded_storage() {
        let context = Context::new(Case::OrphanHistory);
        for _ in 0..200 {
            context.begin(Phase::State).ok();
        }
        context
            .begin(Phase::State)
            .backend::<()>(&Err(Error::Database(DbError::new(ErrorKind::Decode))));
        let buffer = context.buffer.lock().unwrap();
        assert_eq!(buffer.events.len(), 128);
        assert!(buffer.dropped > 0);
        assert_eq!(
            buffer.events.back().unwrap().status,
            Status::Error(Class::Database, Some(ErrorKind::Decode), Role::Primary)
        );
    }
}
