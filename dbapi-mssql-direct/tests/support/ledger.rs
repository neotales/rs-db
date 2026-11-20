use std::time::Duration;

use crate::{
    Probe,
    migration::{Error, HistoryReason, SqlServerBackend},
    provider::DirectPool,
};
use neotales_dbapi::ErrorKind;
use neotales_migration::{Migration, Migrations};

struct FirstWriterGate {
    acquired: tokio::sync::oneshot::Sender<i64>,
    release: tokio::sync::oneshot::Receiver<()>,
    trace: Trace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    FirstStarted,
    FirstLocked,
    FirstIdentityQuery,
    FirstIdentitySent,
    FirstReleased(bool),
    FirstCompleted(bool),
    FirstAcquireFailed(bool),
    SecondStarted,
    SecondIdentityQuery,
    SecondIdentityResult(Result<(), EvidenceError>),
    SecondIdentitySent(bool),
    SecondIdentityReceived,
    ObservationStarted,
    ObservationFinished(Result<(), EvidenceError>),
    ReleaseSent(bool),
    SecondCompleted(bool),
    FirstCompletionDeadline,
    SecondCompletionDeadline,
}

#[derive(Clone)]
struct Trace {
    started: std::time::Instant,
    events: std::sync::Arc<std::sync::Mutex<Vec<(Duration, Phase)>>>,
}

impl Trace {
    fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
            events: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn record(&self, phase: Phase) {
        let mut events = self.events.lock().unwrap();
        if events.len() < 64 {
            events.push((self.started.elapsed(), phase));
        }
    }
}

struct TraceOnDrop(Trace);

impl Drop for TraceOnDrop {
    fn drop(&mut self) {
        eprintln!(
            "serialized migration sanitized phases: {:?}",
            self.0.events.lock().unwrap()
        );
    }
}

#[derive(Default)]
struct AbortWriters(Vec<tokio::task::AbortHandle>);

impl Drop for AbortWriters {
    fn drop(&mut self) {
        for writer in &self.0 {
            writer.abort();
        }
    }
}

tokio::task_local! {
    static FIRST_WRITER_GATE: std::cell::RefCell<Option<FirstWriterGate>>;
    static SECOND_WRITER_ID: std::cell::RefCell<Option<(tokio::sync::oneshot::Sender<Result<i64, EvidenceError>>, Trace)>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvidenceError {
    WaiterIdentityQueryFailed,
    WaiterIdentityInvalid,
    WaiterIdentityDeadline,
    WaiterIdentityChannelClosed,
    ObserverQueryFailedOrPermission,
    ObserverResultInvalid,
    ObserverDeadline,
}

pub(crate) async fn report_waiter_spid(probe: &mut Probe) {
    let Ok(Some((sender, trace))) = SECOND_WRITER_ID.try_with(|sender| sender.borrow_mut().take())
    else {
        return;
    };
    trace.record(Phase::SecondIdentityQuery);
    let result = probe
        .fetch_bounded(
            "SELECT CAST(@@SPID AS int) AS session_id",
            Vec::new(),
            neotales_dbapi::RowLimit::new(1, 64).unwrap(),
        )
        .await
        .map_err(|_| EvidenceError::WaiterIdentityQueryFailed)
        .and_then(|sets| {
            sets.first()
                .and_then(|rows| rows.first())
                .and_then(|row| row.get::<i64>("session_id").ok())
                .ok_or(EvidenceError::WaiterIdentityInvalid)
        });
    trace.record(Phase::SecondIdentityResult(
        result.as_ref().map(|_| ()).map_err(|error| *error),
    ));
    trace.record(Phase::SecondIdentitySent(sender.send(result).is_ok()));
}

fn wait_evidence_sql(holder: i64, waiter: i64) -> String {
    format!(
        "SELECT COUNT(*) FROM sys.dm_exec_requests AS r \
         JOIN sys.dm_tran_locks AS w ON w.request_session_id = r.session_id \
         JOIN sys.dm_tran_locks AS h ON h.request_session_id = r.blocking_session_id \
          AND h.resource_database_id = w.resource_database_id \
          AND h.resource_type = w.resource_type \
          AND h.resource_description = w.resource_description \
         WHERE r.session_id = {waiter} AND w.request_session_id = {waiter} \
          AND r.blocking_session_id = {holder} AND r.wait_type LIKE N'LCK_M_%' \
          AND w.resource_database_id = DB_ID() AND w.resource_type = N'APPLICATION' \
          AND w.request_status = N'WAIT' AND h.request_status = N'GRANT' \
          AND h.request_owner_type = N'TRANSACTION'"
    )
}

#[test]
fn wait_evidence_filters_both_writer_sessions() {
    let sql = wait_evidence_sql(41, 73);
    assert!(sql.contains("r.session_id = 73 AND w.request_session_id = 73"));
    assert!(sql.contains("r.blocking_session_id = 41"));
}

async fn wait_for_release(gate: FirstWriterGate, spid: i64) {
    if gate.acquired.send(spid).is_ok() {
        gate.trace.record(Phase::FirstIdentitySent);
        let released = gate.release.await.is_ok();
        gate.trace.record(Phase::FirstReleased(released));
    }
}

/// Test-only gate: signal only after the migration owns its application lock.
pub(crate) async fn hold_first_writer(transaction: &mut crate::provider::LockedSession) {
    let Ok(Some(gate)) = FIRST_WRITER_GATE.try_with(|gate| gate.borrow_mut().take()) else {
        return;
    };
    gate.trace.record(Phase::FirstLocked);
    gate.trace.record(Phase::FirstIdentityQuery);
    let rows = transaction
        .fetch_all_async(
            "SELECT CAST(@@SPID AS int) AS session_id",
            neotales_dbapi::QueryOptions::new()
                .limit(neotales_dbapi::RowLimit::new(1, 64).unwrap()),
        )
        .await;
    if let Ok(rows) = rows
        && let Some(spid) = rows
            .first()
            .and_then(|row| row.get::<i64>("session_id").ok())
    {
        wait_for_release(gate, spid).await;
    }
}

#[tokio::test]
async fn first_writer_gate_waits_for_explicit_release() {
    let (acquired, acquired_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let mut task = tokio::spawn(wait_for_release(
        FirstWriterGate {
            acquired,
            release: release_rx,
            trace: Trace::new(),
        },
        7,
    ));
    assert_eq!(acquired_rx.await.unwrap(), 7);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut task)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn phase_trace_survives_cancellation() {
    let trace = Trace::new();
    let in_task = trace.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _guard = TraceOnDrop(in_task.clone());
        in_task.record(Phase::FirstStarted);
        in_task.record(Phase::ObservationFinished(Err(
            EvidenceError::ObserverDeadline,
        )));
        let _ = ready.send(());
        std::future::pending::<()>().await;
    });
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let events = trace.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].1, Phase::FirstStarted);
    assert_eq!(
        events[1].1,
        Phase::ObservationFinished(Err(EvidenceError::ObserverDeadline))
    );
    assert!(events[0].0 <= events[1].0);
}

#[allow(
    clippy::too_many_lines,
    reason = "one bounded lifecycle with cancellation-safe diagnostics"
)]
async fn deterministic_migration_serialization(pool: &DirectPool, observer: &mut Probe) {
    let trace = Trace::new();
    let _trace_on_drop = TraceOnDrop(trace.clone());
    let backend = backend("direct_serial_history");
    let definitions = definitions("direct_serial");
    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let first_pool = pool.clone();
    let first_backend = backend.clone();
    let first_defs = definitions.clone();
    let first_trace = trace.clone();
    trace.record(Phase::FirstStarted);
    let mut first = tokio::spawn(FIRST_WRITER_GATE.scope(
        std::cell::RefCell::new(Some(FirstWriterGate {
            acquired: acquired_tx,
            release: release_rx,
            trace: trace.clone(),
        })),
        async move {
            let result = first_backend
                .up(&first_pool, "serial", &first_defs, "binding", &[])
                .await;
            first_trace.record(Phase::FirstCompleted(result.is_ok()));
            result
        },
    ));
    let mut cleanup = AbortWriters(vec![first.abort_handle()]);
    let acquired = tokio::time::timeout(Duration::from_secs(10), acquired_rx).await;
    let Ok(Ok(spid)) = acquired else {
        trace.record(Phase::FirstAcquireFailed(acquired.is_err()));
        first.abort();
        let _ = first.await;
        panic!("first migration did not confirm its application lock");
    };
    let second_pool = pool.clone();
    let second_backend = backend.clone();
    let second_defs = definitions.clone();
    let second_trace = trace.clone();
    let (waiter_tx, waiter_rx) = tokio::sync::oneshot::channel();
    trace.record(Phase::SecondStarted);
    let mut second = tokio::spawn(SECOND_WRITER_ID.scope(
        std::cell::RefCell::new(Some((waiter_tx, trace.clone()))),
        async move {
            let result = second_backend
                .up(&second_pool, "serial", &second_defs, "binding", &[])
                .await;
            second_trace.record(Phase::SecondCompleted(result.is_ok()));
            result
        },
    ));
    cleanup.0.push(second.abort_handle());
    let wait_evidence = async {
        let waiter = tokio::time::timeout(Duration::from_secs(3), waiter_rx)
            .await
            .map_err(|_| EvidenceError::WaiterIdentityDeadline)?
            .map_err(|_| EvidenceError::WaiterIdentityChannelClosed)??;
        trace.record(Phase::SecondIdentityReceived);
        let sql = wait_evidence_sql(spid, waiter);
        trace.record(Phase::ObservationStarted);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let rows = observer
                    .query(&sql, vec![])
                    .await
                    .map_err(|_| EvidenceError::ObserverQueryFailedOrPermission)?;
                let value = match rows
                    .first()
                    .and_then(|set| set.first())
                    .and_then(|row| row.first())
                {
                    Some(mssql_tds::datatypes::column_values::ColumnValues::Int(value)) => *value,
                    _ => return Err(EvidenceError::ObserverResultInvalid),
                };
                if value > 0 {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| EvidenceError::ObserverDeadline)?
    }
    .await;
    trace.record(Phase::ObservationFinished(wait_evidence));
    let released = release_tx.send(()).is_ok();
    trace.record(Phase::ReleaseSent(released));
    let (a, b) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(30), &mut first),
        tokio::time::timeout(Duration::from_secs(30), &mut second)
    );
    if a.is_err() {
        trace.record(Phase::FirstCompletionDeadline);
        first.abort();
        let _ = first.await;
    }
    if b.is_err() {
        trace.record(Phase::SecondCompletionDeadline);
        second.abort();
        let _ = second.await;
    }
    let (Ok(a), Ok(b)) = (a, b) else {
        panic!("serialized migrations exceeded completion deadline");
    };
    assert!(
        wait_evidence.is_ok(),
        "SQL Server did not confirm the second application-lock wait: {wait_evidence:?}"
    );
    assert!(released, "first writer stopped before release");
    assert_eq!(a.unwrap().unwrap(), b.unwrap().unwrap());
    assert_eq!(
        count(observer, "SELECT COUNT(*) FROM direct_serial").await,
        1
    );
}

fn backend(table: &str) -> SqlServerBackend {
    SqlServerBackend::new("dbo", table, Duration::from_secs(5)).unwrap()
}

fn definitions(table: &str) -> Migrations {
    Migrations::new(vec![
        Migration::new(
            1,
            "create",
            format!("CREATE TABLE {table} ( n BIGINT )"),
            Some(format!("DROP TABLE {table}")),
        )
        .unwrap(),
        Migration::new(2, "append", format!("INSERT INTO {table} VALUES (7)"), None).unwrap(),
    ])
    .unwrap()
}

async fn count(probe: &mut Probe, sql: &str) -> i32 {
    super::scalar(&probe.query(sql, vec![]).await.unwrap())
}

// These assertions form one migration-ledger acceptance sequence against a
// single database; keeping its state transitions together aids review.
#[allow(
    clippy::too_many_lines,
    reason = "ordered migration acceptance scenario"
)]
pub(crate) async fn run(pool: &DirectPool, observer: &mut Probe, blocker: &mut Probe) {
    let ledger = backend("direct_history");
    let migrations = definitions("direct_managed");
    let managed = [("dbo", "direct_managed")];
    let state = Box::pin(ledger.up(pool, "module", &migrations, "binding", &managed))
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2);
    assert_eq!(state.head().unwrap().0, 2);
    assert_eq!(
        Box::pin(ledger.up(pool, "module", &migrations, "binding", &managed))
            .await
            .unwrap(),
        state
    );
    assert_eq!(
        count(observer, "SELECT COUNT(*) FROM direct_managed").await,
        1
    );
    assert!(matches!(
        Box::pin(ledger.initialize_module(pool, "module", "changed", &managed)).await,
        Err(Error::History(HistoryReason::BindingMismatch))
    ));
    let drift = Migrations::new(vec![
        Migration::new(
            1,
            "changed",
            "CREATE TABLE direct_managed ( n BIGINT )",
            Some("DROP TABLE direct_managed".into()),
        )
        .unwrap(),
        migrations.as_slice()[1].clone(),
    ])
    .unwrap();
    assert!(matches!(
        Box::pin(ledger.up(pool, "module", &drift, "binding", &managed)).await,
        Err(Error::Migration(neotales_migration::Error::Modified))
    ));
    // A missing down anywhere rejects the entire rollback, before any script.
    assert!(matches!(
        Box::pin(ledger.down(pool, "module", &migrations, "binding", 2, &managed)).await,
        Err(Error::Migration(neotales_migration::Error::MissingDown))
    ));
    assert_eq!(
        count(observer, "SELECT COUNT(*) FROM direct_managed").await,
        1
    );

    let opaque = "module'); DROP TABLE direct_managed; -- 🦀";
    let fingerprint = "binding'🦀";
    let registered = Box::pin(ledger.initialize_module(pool, opaque, fingerprint, &[]))
        .await
        .unwrap();
    assert_eq!(registered.module(), opaque);
    assert_eq!(registered.fingerprint(), fingerprint);
    assert_eq!(
        Box::pin(ledger.initialize_module(pool, opaque, fingerprint, &[]))
            .await
            .unwrap(),
        registered
    );

    let reversible = backend("direct_reverse_history");
    let reverse_defs = Migrations::new(vec![
        Migration::new(
            1,
            "create",
            "CREATE TABLE direct_reverse ( n BIGINT )",
            Some("DROP TABLE direct_reverse".into()),
        )
        .unwrap(),
    ])
    .unwrap();
    Box::pin(reversible.up(
        pool,
        "reverse",
        &reverse_defs,
        "binding",
        &[("dbo", "direct_reverse")],
    ))
    .await
    .unwrap();
    assert_eq!(
        Box::pin(reversible.down(pool, "reverse", &reverse_defs, "binding", 1, &[]))
            .await
            .unwrap()
            .applied_count(),
        0
    );
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_reverse'"
        )
        .await,
        0
    );
    Box::pin(reversible.up(
        pool,
        "reverse",
        &reverse_defs,
        "binding",
        &[("dbo", "direct_reverse")],
    ))
    .await
    .unwrap();

    let baseline = backend("direct_baseline_history");
    let base_defs = Migrations::new(vec![
        Migration::new(
            1,
            "baseline",
            "CREATE TABLE direct_base ( n BIGINT )",
            Some("DROP TABLE direct_base".into()),
        )
        .unwrap()
        .into_baseline(),
    ])
    .unwrap();
    Box::pin(baseline.up(pool, "baseline", &base_defs, "binding", &[]))
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(baseline.down(pool, "baseline", &base_defs, "binding", 1, &[])).await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_base'"
        )
        .await,
        1
    );

    let untracked = backend("direct_untracked_history");
    assert!(matches!(
        Box::pin(untracked.up(pool, "untracked", &migrations, "binding", &managed)).await,
        Err(Error::UntrackedObjects)
    ));
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_untracked_history'"
        )
        .await,
        0
    );

    // Preflight the complete pending plan, not only the first migration.
    let preflight = backend("direct_preflight_history");
    let invalid = Migrations::new(vec![
        Migration::new(
            1,
            "create",
            "CREATE TABLE direct_preflight ( n BIGINT )",
            None,
        )
        .unwrap(),
        Migration::new(2, "invalid", "SET NOEXEC ON", None).unwrap(),
    ])
    .unwrap();
    assert!(
        matches!(Box::pin(preflight.up(pool, "invalid", &invalid, "binding", &[])).await,
        Err(Error::Database(ref e)) if e.kind() == ErrorKind::Unsupported)
    );
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_preflight'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_preflight_history'"
        )
        .await,
        0
    );

    // Both concurrent writers serialize on the same transaction-owned lock.
    let concurrent = backend("direct_concurrent_history");
    let concurrent_defs = definitions("direct_concurrent");
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let start = std::time::Instant::now();
    let diagnostic = |writer| super::LockDiagnostic {
        writer,
        start,
        events: events.clone(),
    };
    let (a, b) = tokio::join!(
        super::LOCK_DIAGNOSTIC.scope(
            diagnostic(1),
            Box::pin(concurrent.up(pool, "concurrent", &concurrent_defs, "binding", &[]))
        ),
        super::LOCK_DIAGNOSTIC.scope(
            diagnostic(2),
            Box::pin(concurrent.up(pool, "concurrent", &concurrent_defs, "binding", &[]))
        )
    );
    eprintln!(
        "concurrent migration sanitized lock events: {:?}",
        events.lock().unwrap()
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        count(observer, "SELECT COUNT(*) FROM direct_concurrent").await,
        1
    );
    deterministic_migration_serialization(pool, observer).await;
    let held = pool
        .begin_locked(concurrent.lock_resource(), 0)
        .await
        .unwrap();
    let impatient = SqlServerBackend::new(
        "dbo",
        "direct_concurrent_history",
        Duration::from_millis(200),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(impatient.initialize_module(pool, "concurrent", "binding", &[])).await,
        Err(Error::LockTimeout)
    ));
    held.rollback().await.unwrap();

    // A dispatched later batch error is unknown, never reported as rolled back.
    let recovery = backend("direct_recovery_history");
    let broken = Migrations::new(vec![
        Migration::new(1, "broken",
            "CREATE TABLE direct_recovery ( n BIGINT )\nGO\nINSERT INTO direct_recovery VALUES (99999999999999999999999999999999999999999999)",
            Some("DROP TABLE direct_recovery".into())).unwrap(),
    ]).unwrap();
    assert!(
        matches!(Box::pin(recovery.up(pool, "recovery", &broken, "binding", &[])).await,
        Err(Error::Database(ref e)) if e.kind() == ErrorKind::WriteOutcomeUnknown)
    );
    // Reconciliation obtains the same lock, and observes the disconnected
    // transaction's rollback before returning an empty state.
    assert_eq!(
        Box::pin(recovery.initialize_module(pool, "recovery", "binding", &[]))
            .await
            .unwrap()
            .applied_count(),
        0
    );
    assert_eq!(
        count(
            observer,
            "SELECT COUNT(*) FROM sys.tables WHERE name = 'direct_recovery'"
        )
        .await,
        0
    );
    let fixed = definitions("direct_recovery");
    Box::pin(recovery.up(pool, "recovery", &fixed, "binding", &[]))
        .await
        .unwrap();

    // Cancellation only after SQL Server confirms both the transaction-owned
    // application lock and a request blocked on this ledger's exclusive table lock.
    blocker.begin().await.unwrap();
    let blocker_spid = count(blocker, "SELECT CAST(@@SPID AS int)").await;
    blocker
        .query(
            "SELECT COUNT(*) FROM direct_recovery_history WITH (TABLOCKX,HOLDLOCK)",
            vec![],
        )
        .await
        .unwrap();
    let cancel_pool = pool.clone();
    let cancel_backend = recovery.clone();
    let cancelled = tokio::spawn(async move {
        Box::pin(cancel_backend.initialize_module(&cancel_pool, "recovery", "binding", &[])).await
    });
    let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let migration_spid = loop {
        let sql = format!(
            "SELECT COALESCE(MIN(session_id), 0) FROM sys.dm_exec_requests \
             WHERE blocking_session_id = {blocker_spid} AND wait_type LIKE N'LCK_M_%'"
        );
        let waiting_spid = count(observer, &sql).await;
        let app_locks = if waiting_spid == 0 {
            0
        } else {
            count(
                observer,
                &format!(
                    "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_session_id = {waiting_spid} AND resource_database_id = DB_ID() AND resource_type = N'APPLICATION' AND request_mode = N'X' AND request_status = N'GRANT'"
                ),
            )
            .await
        };
        // SQL Server renders application resource names in DMV diagnostics as
        // a shortened namespace prefix plus a server-generated resource hash.
        let matching_app_lock = if waiting_spid == 0 {
            0
        } else {
            count(
                observer,
                &format!(
                    "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_session_id = {waiting_spid} AND resource_database_id = DB_ID() AND resource_type = N'APPLICATION' AND request_mode = N'X' AND request_status = N'GRANT' AND CHARINDEX(N'neotales-migration-mssql/lock/v1', resource_description) = 4"
                ),
            )
            .await
        };
        let table_lock = count(
            observer,
            &format!(
                "SELECT COUNT(*) FROM sys.dm_tran_locks WHERE request_session_id = {blocker_spid} AND resource_database_id = DB_ID() AND resource_type = N'OBJECT' AND resource_associated_entity_id = OBJECT_ID(N'dbo.direct_recovery_history') AND request_mode = N'X' AND request_status = N'GRANT'"
            ),
        )
        .await;
        if waiting_spid != 0 && app_locks == 1 && matching_app_lock == 1 && table_lock != 0 {
            break waiting_spid;
        }
        assert!(
            tokio::time::Instant::now() < wait_deadline,
            "SQL Server did not confirm migration app lock + ledger wait (waiting_spid={waiting_spid}, app_locks={app_locks}, matching_app_lock={matching_app_lock}, table_lock={table_lock})"
        );
        tokio::task::yield_now().await;
    };
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    let close_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = count(
            observer,
            &format!(
                "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {migration_spid}"
            ),
        )
        .await;
        if remaining == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < close_deadline,
            "cancelled migration's physical SQL Server session must disappear"
        );
        tokio::task::yield_now().await;
    }
    let reacquired = pool
        .begin_locked(recovery.lock_resource(), 0)
        .await
        .expect("cancelled migration must release its application lock");
    reacquired.rollback().await.unwrap();
    blocker.rollback().await.unwrap();
    assert_eq!(
        Box::pin(recovery.up(pool, "recovery", &fixed, "binding", &[]))
            .await
            .unwrap()
            .applied_count(),
        2
    );

    // Existing tables are not adopted; partial pairs and metadata drift fail.
    observer
        .query("CREATE TABLE direct_foreign_history (n bigint)", vec![])
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(backend("direct_foreign_history").initialize_module(pool, "x", "binding", &[]))
            .await,
        Err(Error::History(HistoryReason::PartialLedger))
    ));
    observer
        .query(
            "ALTER TABLE direct_history ADD unexpected bigint NULL",
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(ledger.initialize_module(pool, "module", "binding", &[])).await,
        Err(Error::History(HistoryReason::IncompatibleLedger))
    ));
    observer
        .query("ALTER TABLE direct_history DROP COLUMN unexpected", vec![])
        .await
        .unwrap();
    observer
        .query(
            "UPDATE direct_history_modules SET applied_count = 999 WHERE module = N'module'",
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(ledger.initialize_module(pool, "module", "binding", &[])).await,
        Err(Error::History(HistoryReason::InconsistentRows))
    ));
}
