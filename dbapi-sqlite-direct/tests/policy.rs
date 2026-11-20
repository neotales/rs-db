//! `PolicyPool` telemetry, SQL capture, and retry behavior over the direct SQLite provider.

use std::{
    future::{pending, ready},
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi::{
    Cancellation, DbEvent, DbType, Delay, ErrorKind, Operation, Parameter, PolicyPool, Pool,
    PoolPolicy, QueryOptions, RetryPolicy, RetryRuntime, RowLimit, SqlitePolicyPool, SqlitePool,
    TelemetryOutcome, TxOptions, Value,
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

fn limit() -> RowLimit {
    RowLimit::new(16, 16_384).expect("valid limit")
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("valid parameter")
}

async fn provider(path: &std::path::Path, busy_ms: u64) -> SqlitePoolProvider {
    SqlitePoolProvider::connect(
        SqliteConfig::new(path)
            .busy_timeout(Duration::from_millis(busy_ms))
            .max_connections(2),
    )
    .await
    .expect("provider")
}

fn inspector(path: &std::path::Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(path).expect("inspector");
    connection
        .busy_timeout(Duration::from_millis(500))
        .expect("busy timeout");
    connection
}

fn events_sink(events: Arc<Mutex<Vec<DbEvent>>>) -> Arc<dyn Fn(&DbEvent) + Send + Sync> {
    Arc::new(move |event| events.lock().unwrap().push(event.clone()))
}

fn retry_runtime(waits: Arc<Mutex<Vec<Duration>>>) -> RetryRuntime {
    RetryRuntime::new(
        Arc::new(Cancellation::new()),
        || Box::pin(pending()),
        move |delay| {
            waits.lock().unwrap().push(delay);
            Box::pin(ready(()))
        },
        || 0,
    )
}

fn retry_policy(attempts: u32) -> RetryPolicy {
    RetryPolicy::new(attempts, Delay::Fixed(Duration::ZERO), false).expect("valid retry policy")
}

#[tokio::test]
async fn telemetry_captures_only_opted_in_sql_and_never_values_or_rows() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("telemetry.sqlite"), 25).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let sentinel = "bound-value-must-not-appear";
    let rows = db
        .fetch_all_async(
            "SELECT ? AS value /* sql-comment-visible-only-if-captured */",
            QueryOptions::new().bind(text(sentinel)).limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("value").unwrap(), sentinel);

    let captured = events.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].operation, Operation::Fetch);
    assert_eq!(captured[0].outcome, TelemetryOutcome::Success);
    assert_eq!(
        captured[0].sql.as_deref(),
        Some("SELECT ? AS value /* sql-comment-visible-only-if-captured */")
    );
    let rendered = format!("{captured:?}");
    assert!(!rendered.contains(sentinel));
    assert!(rendered.contains("sql-comment-visible-only-if-captured"));
}

#[tokio::test]
async fn sql_capture_off_never_emits_sql_or_values() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("no-capture.sqlite"), 25).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: false,
            observer: Some(events_sink(Arc::clone(&events))),
            ..PoolPolicy::default()
        },
    );
    let sentinel = "secret-comment-and-value";
    db.fetch_all_async(
        &format!("SELECT ? AS value /* {sentinel} */"),
        QueryOptions::new().bind(text(sentinel)).limit(limit()),
    )
    .await
    .unwrap();
    let captured = events.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].sql, None);
    let rendered = format!("{captured:?}");
    assert!(!rendered.contains(sentinel));
}

#[tokio::test]
async fn disabled_retry_runs_once_and_missing_retry_runtime_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retry-disabled.sqlite");
    let inspect = inspector(&path);
    inspect
        .execute_batch("CREATE TABLE data (value INTEGER)")
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(&path, 10).await,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    inspect.execute_batch("BEGIN IMMEDIATE").unwrap();

    // The disabled policy deliberately does not retry, even for a transient Busy result.
    let error = db
        .execute_async(
            "INSERT INTO data(value) VALUES (1)",
            QueryOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(events.lock().unwrap().len(), 1);
    inspect.execute_batch("ROLLBACK").unwrap();

    let needs_runtime = PolicyPool::new(
        provider(&path, 10).await,
        PoolPolicy {
            retry: retry_policy(2),
            ..PoolPolicy::default()
        },
    );
    let error = needs_runtime
        .execute_async(
            "INSERT INTO data(value) VALUES (3)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let count: i64 = inspect
        .query_row("SELECT count(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn unknown_write_outcomes_are_never_retried() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unknown.sqlite");
    let provider = provider(&path, 10).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let waits = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(Arc::clone(&waits))),
            ..PoolPolicy::default()
        },
    );

    // A statement that cannot be prepared is conservative for a possible write: its outcome is
    // unknown, so the policy must not replay it even with an explicit replay-safe assertion.
    let error = db
        .execute_async(
            "INSERT INTO missing_table VALUES (1)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(events.lock().unwrap().len(), 1);
    assert!(waits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn session_and_transaction_operations_never_retry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("no-session-retry.sqlite");
    let inspect = inspector(&path);
    inspect
        .execute_batch("CREATE TABLE data (value INTEGER)")
        .unwrap();
    let waits = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(&path, 5).await,
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(Arc::clone(&waits))),
            ..PoolPolicy::default()
        },
    );
    inspect.execute_batch("BEGIN IMMEDIATE").unwrap();

    let mut session = db.acquire_async().await.unwrap();
    let error = session
        .execute_async(
            "INSERT INTO data(value) VALUES (1)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    drop(session);
    assert!(waits.lock().unwrap().is_empty());

    // A deferred transaction that upgrades to a write lock follows the same no-retry rule.
    let mut session = db.acquire_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let error = transaction
        .execute_async(
            "INSERT INTO data(value) VALUES (2)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert!(waits.lock().unwrap().is_empty());
    drop(transaction);
    drop(session);
    inspect.execute_batch("ROLLBACK").unwrap();
}

#[tokio::test]
async fn sqlite_policy_session_and_immediate_begin_do_not_retry_busy_but_pool_does() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sqlite-session-busy.sqlite");
    let inspect = inspector(&path);
    inspect
        .execute_batch("CREATE TABLE data (value INTEGER)")
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let pool = Arc::new(SqlitePolicyPool::new(
        provider(&path, 2).await,
        PoolPolicy {
            retry: retry_policy(20),
            retry_runtime: Some(retry_runtime(Arc::clone(&sleeps))),
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            ..PoolPolicy::default()
        },
    ));

    inspect.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_sqlite_session_busy_once(&pool, &events, &sleeps).await;
    assert_sqlite_transaction_busy_once(&pool, &events, &sleeps).await;

    let mut session = pool.acquire_sqlite_async().await.unwrap();
    let Err(error) = session.begin_immediate_async().await else {
        panic!("external writer lock must reject BEGIN IMMEDIATE");
    };
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert!(
        sleeps.lock().unwrap().is_empty(),
        "a failed immediate begin does not enter the pool retry loop"
    );
    drop(session);

    let pool_for_write = Arc::clone(&pool);
    let write = tokio::spawn(async move {
        pool_for_write
            .execute_async(
                "INSERT INTO data(value) VALUES (17)",
                QueryOptions::new().replay_safe(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let busy_attempts = events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
                .count();
            if busy_attempts >= 3 && !sleeps.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pool operation should retry the transient Busy result");
    inspect.execute_batch("ROLLBACK").unwrap();
    assert_eq!(write.await.unwrap().unwrap(), 1);

    let observed = events.lock().unwrap().clone();
    assert!(
        observed
            .iter()
            .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
            .count()
            >= 3
    );
    assert!(
        observed
            .iter()
            .any(|event| event.operation == Operation::Execute
                && event.outcome == TelemetryOutcome::Success),
        "the positive-control pool execution eventually succeeds"
    );
    assert!(!sleeps.lock().unwrap().is_empty());
    inspect
        .execute_batch("CREATE TABLE check_after_retry (value INTEGER)")
        .unwrap();
}

async fn assert_sqlite_session_busy_once(
    pool: &SqlitePolicyPool,
    events: &Arc<Mutex<Vec<DbEvent>>>,
    sleeps: &Arc<Mutex<Vec<Duration>>>,
) {
    let mut session = pool.acquire_sqlite_async().await.unwrap();
    let error = session
        .execute_async(
            "INSERT INTO data(value) VALUES (1)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
            .count(),
        1,
        "the acquired SQLite session dispatches the replay-safe Busy statement once"
    );
    assert!(sleeps.lock().unwrap().is_empty());
}

async fn assert_sqlite_transaction_busy_once(
    pool: &SqlitePolicyPool,
    events: &Arc<Mutex<Vec<DbEvent>>>,
    sleeps: &Arc<Mutex<Vec<Duration>>>,
) {
    let mut session = pool.acquire_sqlite_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let before = events.lock().unwrap().len();
    let error = transaction
        .execute_async(
            "INSERT INTO data(value) VALUES (2)",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    let observed = events.lock().unwrap().clone();
    assert_eq!(
        observed.len(),
        before + 1,
        "the replay-safe transaction statement emits exactly one dispatch event"
    );
    assert_eq!(
        observed
            .iter()
            .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
            .count(),
        2,
        "the deferred transaction statement is dispatched only once"
    );
    assert!(sleeps.lock().unwrap().is_empty());
}

#[tokio::test]
async fn busy_is_retried_only_with_explicit_replay_safety_and_a_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("busy-retry.sqlite");
    let inspect = inspector(&path);
    inspect
        .execute_batch("CREATE TABLE retry_data (value INTEGER)")
        .unwrap();
    inspect.execute_batch("BEGIN IMMEDIATE").unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(&path, 2).await,
        PoolPolicy {
            retry: retry_policy(30),
            retry_runtime: Some(RetryRuntime::new(
                Arc::new(Cancellation::new()),
                || Box::pin(tokio::time::sleep(Duration::from_secs(5))),
                |_| Box::pin(tokio::time::sleep(Duration::from_millis(5))),
                || 0,
            )),
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async(
            "INSERT INTO retry_data(value) VALUES (17)",
            QueryOptions::new().replay_safe(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let busy = events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
                .count();
            if busy >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the held SQLite lock should produce two Busy attempts");
    inspect.execute_batch("ROLLBACK").unwrap();
    assert_eq!(task.await.unwrap().unwrap(), 1);

    let stored: i64 = inspect
        .query_row("SELECT count(*) FROM retry_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(stored, 1);
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
            .count()
            >= 2
    );
}

#[tokio::test]
async fn replay_safe_is_required_even_when_the_pool_retry_default_is_enabled() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("no-assertion.sqlite");
    let inspect = inspector(&path);
    inspect
        .execute_batch("CREATE TABLE data (value INTEGER)")
        .unwrap();
    inspect.execute_batch("BEGIN IMMEDIATE").unwrap();
    let waits = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(&path, 5).await,
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(Arc::clone(&waits))),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async("INSERT INTO data(value) VALUES (1)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert!(waits.lock().unwrap().is_empty());
    inspect.execute_batch("ROLLBACK").unwrap();
}

#[tokio::test]
async fn policy_pool_futures_remain_send() {
    fn assert_send<T: Send>(_: T) {}
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("send.sqlite"), 10).await;
    let db = PolicyPool::new(provider, PoolPolicy::default());
    assert_send(db.acquire_async());
    assert_send(db.execute_async("SELECT 1", QueryOptions::new()));
    assert_send(db.fetch_all_async("SELECT 1", QueryOptions::new().limit(limit())));
}
