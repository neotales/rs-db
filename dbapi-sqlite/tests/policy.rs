//! Policy decorator integration coverage using SQLite file-backed pools.

use std::{
    future::{Future, pending, poll_fn, ready},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbEvent, DbFuture, DbType, ErrorKind, Operation,
    Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, QuerySetting, Reader, RetryPolicy,
    RetryRuntime, RetrySetting, Row, RowLimit, Session, TelemetryOutcome, Transaction, TxOptions,
    Value,
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use tempfile::NamedTempFile;

async fn provider(path: &std::path::Path, timeout: Duration) -> SqlitePoolProvider {
    SqlitePoolProvider::connect(
        SqliteConfig::new(path)
            .busy_timeout(timeout)
            .max_connections(2),
    )
    .await
    .unwrap()
}

async fn inspector(path: &std::path::Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true),
        )
        .await
        .unwrap()
}

fn text_parameter(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).unwrap()
}

fn limit() -> RowLimit {
    RowLimit::new(10, 4096).unwrap()
}

fn assert_send<T: Future + Send>(_: T) {}

struct PanicDropPayload;
impl Drop for PanicDropPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor sentinel");
    }
}

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // Independent fake-provider race controls.
struct FakeStats {
    acquisitions: usize,
    execute_calls: usize,
    fetch_calls: usize,
    session_drops: usize,
    fail_execute: usize,
    fail_fetch: usize,
    execute_error: Option<ErrorKind>,
    fetch_error: Option<ErrorKind>,
    pending_execute: bool,
    execute_ready: bool,
    execute_waker: Option<Waker>,
    cancel_on_execute: Option<Arc<neotales_resilience::Cancellation>>,
    deadline_on_execute: Option<Arc<std::sync::atomic::AtomicBool>>,
    pending_acquire: bool,
    acquire_ready: bool,
    acquire_waker: Option<Waker>,
}

#[derive(Clone)]
struct FakePool {
    stats: Arc<Mutex<FakeStats>>,
    acquire_drops: Arc<AtomicUsize>,
}

struct FakeSession {
    stats: Arc<Mutex<FakeStats>>,
}

struct FakeTransaction {
    stats: Arc<Mutex<FakeStats>>,
}

impl Drop for FakeSession {
    fn drop(&mut self) {
        self.stats.lock().unwrap().session_drops += 1;
    }
}

struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Pool for FakePool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let stats = Arc::clone(&self.stats);
        let drops = Arc::clone(&self.acquire_drops);
        Box::pin(async move {
            stats.lock().unwrap().acquisitions += 1;
            if stats.lock().unwrap().pending_acquire {
                let _drop = DropCounter(drops);
                poll_fn(|cx| {
                    let mut stats_guard = stats.lock().unwrap();
                    if stats_guard.acquire_ready {
                        Poll::Ready(())
                    } else {
                        stats_guard.acquire_waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
            }
            Ok(Box::new(FakeSession { stats }) as Box<dyn Session>)
        })
    }
}

impl Session for FakeSession {
    fn execute_async<'a>(&'a mut self, _sql: &'a str, _options: QueryOptions) -> DbFuture<'a, u64> {
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let (pending_execute, call_number, failure, cancellation, deadline) = {
                let mut stats_guard = stats.lock().unwrap();
                stats_guard.execute_calls += 1;
                (
                    stats_guard.pending_execute,
                    stats_guard.execute_calls,
                    stats_guard.fail_execute,
                    stats_guard.cancel_on_execute.clone(),
                    stats_guard.deadline_on_execute.clone(),
                )
            };
            if pending_execute {
                poll_fn(|cx| {
                    let mut stats_guard = stats.lock().unwrap();
                    if stats_guard.execute_ready {
                        Poll::Ready(())
                    } else {
                        stats_guard.execute_waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
            }
            if let Some(cancellation) = cancellation {
                cancellation.cancel();
            }
            if let Some(deadline) = deadline {
                deadline.store(true, Ordering::SeqCst);
            }
            let stats_guard = stats.lock().unwrap();
            if call_number <= failure {
                Err(DbError::new(
                    stats_guard.execute_error.unwrap_or(ErrorKind::Busy),
                ))
            } else {
                Ok(1)
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _sql: &'a str,
        _options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let mut stats = stats.lock().unwrap();
            stats.fetch_calls += 1;
            if stats.fetch_calls <= stats.fail_fetch {
                Err(DbError::new(stats.fetch_error.unwrap_or(ErrorKind::Busy)))
            } else {
                Ok(
                    Box::new(BufferedReader::new(vec![Row::new(vec![]).unwrap()]))
                        as Box<dyn Reader>,
                )
            }
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _sql: &'a str,
        _options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn begin_async<'a>(
        &'a mut self,
        _options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let stats = Arc::clone(&self.stats);
        Box::pin(
            async move { Ok(Box::new(FakeTransaction { stats }) as Box<dyn Transaction<'a> + 'a>) },
        )
    }
}

impl<'session> Transaction<'session> for FakeTransaction {
    fn execute_async<'a>(&'a mut self, _sql: &'a str, _options: QueryOptions) -> DbFuture<'a, u64> {
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let mut stats = stats.lock().unwrap();
            stats.execute_calls += 1;
            if stats.execute_calls <= stats.fail_execute {
                Err(DbError::new(stats.execute_error.unwrap_or(ErrorKind::Busy)))
            } else {
                Ok(1)
            }
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _sql: &'a str,
        _options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async {
            Ok(Box::new(BufferedReader::new(vec![Row::new(vec![]).unwrap()])) as Box<dyn Reader>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _sql: &'a str,
        _options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::CommitOutcomeUnknown)) })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn retry_policy(attempts: u32) -> RetryPolicy {
    RetryPolicy::new(
        attempts,
        neotales_dbapi::Delay::Fixed(Duration::ZERO),
        false,
    )
    .unwrap()
}

fn retry_runtime(
    cancellation: Arc<neotales_resilience::Cancellation>,
    waits: Arc<Mutex<Vec<Duration>>>,
) -> RetryRuntime {
    RetryRuntime::new(
        cancellation,
        || Box::pin(pending()),
        move |delay| {
            waits.lock().unwrap().push(delay);
            Box::pin(ready(()))
        },
        || 0,
    )
}

fn events_sink(events: Arc<Mutex<Vec<DbEvent>>>) -> Arc<dyn Fn(&DbEvent) + Send + Sync> {
    Arc::new(move |event| events.lock().unwrap().push(event.clone()))
}

#[tokio::test]
async fn convenience_operations_capture_only_opted_in_sql_and_never_values_or_rows() {
    let file = NamedTempFile::new().unwrap();
    let provider = provider(file.path(), Duration::from_millis(25)).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let policy = PoolPolicy {
        telemetry_enabled: true,
        sql_capture_enabled: true,
        observer: Some(events_sink(Arc::clone(&events))),
        retry: RetryPolicy::Disabled,
        ..PoolPolicy::default()
    };
    let db = PolicyPool::new(provider, policy);
    let sentinel = "bound-value-must-not-appear";
    let rows = db
        .fetch_all_async(
            "SELECT ? AS value /* sql-comment-visible-only-if-captured */",
            QueryOptions::new()
                .bind(text_parameter(sentinel))
                .limit(limit()),
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
async fn operation_precedence_hooks_and_telemetry_master_off_are_independent() {
    let file = NamedTempFile::new().unwrap();
    let db_events = Arc::new(Mutex::new(Vec::new()));
    let hook_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let pool_hook_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let per_query_hook_calls = Arc::clone(&hook_calls);
    let observed_pool_hook_calls = Arc::clone(&pool_hook_calls);
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: false,
            sql_capture_enabled: false,
            sql_hook: Some(Arc::new(move |sql| {
                observed_pool_hook_calls
                    .lock()
                    .unwrap()
                    .push(sql.to_owned());
                Some("pool replacement".to_owned())
            })),
            observer: Some(events_sink(Arc::clone(&db_events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );

    // Operation On overrides both disabled pool defaults; its hook replaces the pool hook.
    db.execute_async(
        "SELECT 11",
        QueryOptions::new()
            .telemetry(QuerySetting::On)
            .sql_capture(QuerySetting::On)
            .sql_hook(Arc::new(move |sql| {
                per_query_hook_calls.lock().unwrap().push(sql.to_owned());
                Some("query replacement".to_owned())
            })),
    )
    .await
    .unwrap();
    assert_eq!(hook_calls.lock().unwrap().as_slice(), &["SELECT 11"]);
    assert!(pool_hook_calls.lock().unwrap().is_empty());
    assert_eq!(
        db_events.lock().unwrap()[0].sql.as_deref(),
        Some("query replacement")
    );

    // SQL Off emits ordinary telemetry but neither invokes the hook nor captures SQL.
    db.fetch_all_async(
        "SELECT 12",
        QueryOptions::new()
            .telemetry(QuerySetting::On)
            .sql_capture(QuerySetting::Off)
            .limit(limit()),
    )
    .await
    .unwrap();
    assert_eq!(db_events.lock().unwrap().len(), 2);
    assert_eq!(db_events.lock().unwrap()[1].sql, None);
    assert!(pool_hook_calls.lock().unwrap().is_empty());

    // A per-query hook can suppress SQL without suppressing the telemetry event.
    db.fetch_all_async(
        "SELECT 121",
        QueryOptions::new()
            .telemetry(QuerySetting::On)
            .sql_capture(QuerySetting::On)
            .sql_hook(Arc::new(|_| None))
            .limit(limit()),
    )
    .await
    .unwrap();
    assert_eq!(db_events.lock().unwrap().len(), 3);
    assert_eq!(db_events.lock().unwrap()[2].sql, None);
    assert!(pool_hook_calls.lock().unwrap().is_empty());

    // Telemetry Off is the operation master switch, even when SQL capture requests On.
    db.execute_async(
        "SELECT 13",
        QueryOptions::new()
            .telemetry(QuerySetting::Off)
            .sql_capture(QuerySetting::On),
    )
    .await
    .unwrap();
    assert_eq!(db_events.lock().unwrap().len(), 3);
    assert!(pool_hook_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn trait_object_query_options_cover_pool_transaction_and_trusted_script() {
    let file = NamedTempFile::new().unwrap();
    let inspect = inspector(file.path()).await;
    sqlx::query("CREATE TABLE secrets (value TEXT)")
        .execute(&inspect)
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(|sql| Some(format!("pool-hook:{sql}")))),
            observer: Some(events_sink(Arc::clone(&events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let pool: &dyn Pool = &db;

    pool.execute_async(
        "INSERT INTO secrets VALUES ('pool-secret-literal')",
        QueryOptions::new().sql_capture(QuerySetting::Off),
    )
    .await
    .unwrap();
    assert_eq!(events.lock().unwrap()[0].sql, None);

    let mut session = pool.acquire_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            "INSERT INTO secrets VALUES ('transaction-secret-literal')",
            QueryOptions::new().sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    assert_eq!(events.lock().unwrap()[1].sql, None);

    session
        .execute_trusted_script_async(
            "INSERT INTO secrets VALUES ('script-secret-literal');",
            QueryOptions::new().sql_hook(Arc::new(|_| Some("script replacement".to_owned()))),
        )
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap()[2].sql.as_deref(),
        Some("script replacement")
    );
    let rendered = format!("{:?}", events.lock().unwrap());
    for secret in [
        "pool-secret-literal",
        "transaction-secret-literal",
        "script-secret-literal",
    ] {
        assert!(!rendered.contains(secret));
    }
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM secrets")
        .fetch_one(&inspect)
        .await
        .unwrap();
    assert_eq!(stored, 3);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep one end-to-end nested-policy leakage scenario atomic.
async fn nested_wrappers_preserve_off_and_replacement_for_every_statement_surface() {
    let file = NamedTempFile::new().unwrap();
    let inspect = inspector(file.path()).await;
    sqlx::query("CREATE TABLE secrets (value TEXT)")
        .execute(&inspect)
        .await
        .unwrap();
    let inner_events = Arc::new(Mutex::new(Vec::new()));
    let outer_events = Arc::new(Mutex::new(Vec::new()));
    let inner_hook_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let outer_hook_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let replacement_calls = Arc::new(Mutex::new(Vec::<String>::new()));

    let inner_hook_log = Arc::clone(&inner_hook_calls);
    let inner = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(move |sql| {
                inner_hook_log.lock().unwrap().push(sql.to_owned());
                Some("inner hook output".to_owned())
            })),
            observer: Some(events_sink(Arc::clone(&inner_events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let outer_hook_log = Arc::clone(&outer_hook_calls);
    let outer = PolicyPool::new(
        inner,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(move |sql| {
                outer_hook_log.lock().unwrap().push(sql.to_owned());
                Some("outer hook output".to_owned())
            })),
            observer: Some(events_sink(Arc::clone(&outer_events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let pool: &dyn Pool = &outer;
    let secret_literals = [
        "pool_sql_secret",
        "session_sql_secret",
        "transaction_sql_secret",
        "script_sql_secret",
        "replacement_sql_secret",
    ];

    pool.execute_async(
        "INSERT INTO secrets VALUES ('pool_sql_secret')",
        QueryOptions::new().sql_capture(QuerySetting::Off),
    )
    .await
    .unwrap();
    let bound_secret = "bound_parameter_and_result_secret";
    let result = pool
        .fetch_all_async(
            "SELECT ? AS value /* fetch_sql_secret */",
            QueryOptions::new()
                .bind(text_parameter(bound_secret))
                .limit(limit())
                .sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    assert_eq!(result[0].get::<String>("value").unwrap(), bound_secret);

    let mut session = pool.acquire_async().await.unwrap();
    session
        .execute_async(
            "INSERT INTO secrets VALUES ('session_sql_secret')",
            QueryOptions::new().sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    let session_fetch = session
        .fetch_all_async(
            "SELECT ? AS value /* session_fetch_sql_secret */",
            QueryOptions::new()
                .bind(text_parameter(bound_secret))
                .limit(limit())
                .sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    assert_eq!(
        session_fetch[0].get::<String>("value").unwrap(),
        bound_secret
    );

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            "INSERT INTO secrets VALUES ('transaction_sql_secret')",
            QueryOptions::new().sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    let transaction_fetch = transaction
        .fetch_all_async(
            "SELECT ? AS value /* transaction_fetch_sql_secret */",
            QueryOptions::new()
                .bind(text_parameter(bound_secret))
                .limit(limit())
                .sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    assert_eq!(
        transaction_fetch[0].get::<String>("value").unwrap(),
        bound_secret
    );
    transaction
        .execute_trusted_script_async(
            "INSERT INTO secrets VALUES ('transaction_script_sql_secret');",
            QueryOptions::new().sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();

    // Explicit SQL Off must reach both wrappers and prevent every hook from running.
    assert!(inner_hook_calls.lock().unwrap().is_empty());
    assert!(outer_hook_calls.lock().unwrap().is_empty());
    assert_eq!(inner_events.lock().unwrap().len(), 7);
    assert_eq!(outer_events.lock().unwrap().len(), 7);
    for event in inner_events
        .lock()
        .unwrap()
        .iter()
        .chain(outer_events.lock().unwrap().iter())
    {
        assert_eq!(event.sql, None);
    }

    let replacement_log = Arc::clone(&replacement_calls);
    session
        .execute_trusted_script_async(
            "INSERT INTO secrets VALUES ('script_sql_secret');",
            QueryOptions::new().sql_hook(Arc::new(move |sql| {
                replacement_log.lock().unwrap().push(sql.to_owned());
                Some("approved replacement".to_owned())
            })),
        )
        .await
        .unwrap();

    assert_eq!(replacement_calls.lock().unwrap().len(), 2);
    assert!(inner_hook_calls.lock().unwrap().is_empty());
    assert!(outer_hook_calls.lock().unwrap().is_empty());
    let inner_event_log = inner_events.lock().unwrap().clone();
    let outer_event_log = outer_events.lock().unwrap().clone();
    assert_eq!(inner_event_log.len(), 8);
    assert_eq!(outer_event_log.len(), 8);
    assert_eq!(
        inner_event_log[7].sql.as_deref(),
        Some("approved replacement")
    );
    assert_eq!(
        outer_event_log[7].sql.as_deref(),
        Some("approved replacement")
    );
    let all_events = format!("{inner_event_log:?} {outer_event_log:?}");
    for secret in secret_literals.into_iter().chain([
        bound_secret,
        "fetch_sql_secret",
        "session_fetch_sql_secret",
        "transaction_fetch_sql_secret",
        "transaction_script_sql_secret",
    ]) {
        assert!(!all_events.contains(secret));
    }

    // Telemetry Off is a stronger master switch than SQL Full and suppresses hooks at every layer.
    let off_hook_calls = Arc::new(Mutex::new(0));
    let off_calls = Arc::clone(&off_hook_calls);
    pool.execute_async(
        "INSERT INTO secrets VALUES ('replacement_sql_secret')",
        QueryOptions::new()
            .telemetry(QuerySetting::Off)
            .sql_capture(QuerySetting::On)
            .sql_hook(Arc::new(move |_| {
                *off_calls.lock().unwrap() += 1;
                Some("must not run".to_owned())
            })),
    )
    .await
    .unwrap();
    assert_eq!(*off_hook_calls.lock().unwrap(), 0);
    assert_eq!(inner_events.lock().unwrap().len(), 8);
    assert_eq!(outer_events.lock().unwrap().len(), 8);
    assert_eq!(replacement_calls.lock().unwrap().len(), 2);
    assert!(inner_hook_calls.lock().unwrap().is_empty());
    assert!(outer_hook_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn hook_and_observer_panics_never_change_database_results() {
    let file = NamedTempFile::new().unwrap();
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(|_| std::panic::panic_any(PanicDropPayload))),
            observer: Some(Arc::new(|_| std::panic::panic_any(PanicDropPayload))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let rows = db
        .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit()))
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
}

#[tokio::test]
async fn missing_observer_means_no_emission_or_hook_invocation() {
    let file = NamedTempFile::new().unwrap();
    let hook_calls = Arc::new(Mutex::new(0));
    let calls = Arc::clone(&hook_calls);
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(move |_| {
                *calls.lock().unwrap() += 1;
                Some("captured".to_owned())
            })),
            ..PoolPolicy::default()
        },
    );
    assert_eq!(
        db.execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap(),
        0
    );
    assert_eq!(*hook_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn acquired_sessions_and_transactions_are_observed_but_never_retried() {
    let file = NamedTempFile::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    let mut session = db.acquire_async().await.unwrap();
    let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    tx.execute_async("CREATE TABLE data (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    tx.commit_async().await.unwrap();
    assert_eq!(events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn policy_pool_convenience_and_acquire_futures_remain_send() {
    let file = NamedTempFile::new().unwrap();
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy::default(),
    );
    assert_send(db.execute_async("SELECT 1", QueryOptions::new()));
    assert_send(db.fetch_all_async("SELECT 1", QueryOptions::new().limit(limit())));
    assert_send(db.acquire_async());
}

#[tokio::test]
async fn disabled_retry_runs_once_and_missing_retry_runtime_fails_closed() {
    let file = NamedTempFile::new().unwrap();
    let inspect = inspector(file.path()).await;
    sqlx::query("CREATE TABLE data (value INTEGER)")
        .execute(&inspect)
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(10)).await,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(events_sink(Arc::clone(&events))),
            retry: RetryPolicy::Disabled,
            ..PoolPolicy::default()
        },
    );
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&inspect)
        .await
        .unwrap();

    // The current policy deliberately does not retry, even for a transient Busy result.
    let error = db
        .execute_async(
            "INSERT INTO data(value) VALUES (1)",
            QueryOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), neotales_dbapi::ErrorKind::Busy);
    assert_eq!(events.lock().unwrap().len(), 1);
    sqlx::query("ROLLBACK").execute(&inspect).await.unwrap();

    let needs_runtime = PolicyPool::new(
        provider(file.path(), Duration::from_millis(10)).await,
        PoolPolicy {
            retry: RetryPolicy::new(2, neotales_dbapi::Delay::Fixed(Duration::ZERO), false)
                .unwrap(),
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
    assert_eq!(error.kind(), neotales_dbapi::ErrorKind::Unsupported);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM data")
        .fetch_one(&inspect)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One integration scenario asserts retry defaults and overrides.
async fn replay_safe_pool_retries_use_fresh_sessions_and_per_call_overrides() {
    let stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 2,
        fail_fetch: 1,
        ..FakeStats::default()
    }));
    let waits = Arc::new(Mutex::new(Vec::new()));
    let cleanup_before_backoff = Arc::new(Mutex::new(Vec::new()));
    let stats_for_sleep = Arc::clone(&stats);
    let cleanup_for_sleep = Arc::clone(&cleanup_before_backoff);
    let waits_for_sleep = Arc::clone(&waits);
    let runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        || Box::pin(pending()),
        move |delay| {
            waits_for_sleep.lock().unwrap().push(delay);
            cleanup_for_sleep
                .lock()
                .unwrap()
                .push(stats_for_sleep.lock().unwrap().session_drops);
            Box::pin(ready(()))
        },
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );

    // No replay-safe assertion means the configured default does not retry.
    let error = db
        .execute_async("UPDATE records SET value = 1", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(stats.lock().unwrap().execute_calls, 1);

    let result = db
        .execute_async(
            "write explicitly asserted safe",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap();
    assert_eq!(result, 1);
    let rows = db
        .fetch_all_async(
            "SELECT caller-asserted-safe",
            QueryOptions::new().replay_safe().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    {
        let current = stats.lock().unwrap();
        assert_eq!(current.execute_calls, 3);
        assert_eq!(current.fetch_calls, 2);
        assert_eq!(current.acquisitions, 5);
        assert_eq!(current.session_drops, 5);
    }
    assert_eq!(waits.lock().unwrap().len(), 2);
    assert_eq!(cleanup_before_backoff.lock().unwrap().as_slice(), &[2, 4]);

    // Explicit Off prevents a retry despite the pool default and replay-safe assertion.
    let off_stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 5,
        ..FakeStats::default()
    }));
    let off_db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&off_stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(retry_runtime(
                Arc::new(neotales_resilience::Cancellation::new()),
                Arc::new(Mutex::new(Vec::new())),
            )),
            ..PoolPolicy::default()
        },
    );
    let error = off_db
        .execute_async(
            "safe but disabled",
            QueryOptions::new().retry(RetrySetting::Off).replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(off_stats.lock().unwrap().execute_calls, 1);

    // Per-call Use replaces a disabled pool default.
    let use_stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 1,
        ..FakeStats::default()
    }));
    let use_db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&use_stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry_runtime: Some(retry_runtime(
                Arc::new(neotales_resilience::Cancellation::new()),
                Arc::new(Mutex::new(Vec::new())),
            )),
            ..PoolPolicy::default()
        },
    );
    use_db
        .execute_async(
            "safe with per-call retry",
            QueryOptions::new()
                .retry(RetrySetting::Use(retry_policy(2)))
                .replay_safe(),
        )
        .await
        .unwrap();
    assert_eq!(use_stats.lock().unwrap().execute_calls, 2);
}

#[tokio::test]
async fn cancellation_drops_pending_acquisition_and_backoff_without_another_attempt() {
    let acquire_drops = Arc::new(AtomicUsize::new(0));
    let acquire_stats = Arc::new(Mutex::new(FakeStats {
        pending_acquire: true,
        ..FakeStats::default()
    }));
    let cancellation = Arc::new(neotales_resilience::Cancellation::new());
    let db = PolicyPool::new(
        FakePool {
            stats: acquire_stats,
            acquire_drops: Arc::clone(&acquire_drops),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(retry_runtime(
                Arc::clone(&cancellation),
                Arc::new(Mutex::new(Vec::new())),
            )),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async(
            "safe while acquire is pending",
            QueryOptions::new().replay_safe(),
        )
        .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(acquire_drops.load(Ordering::SeqCst), 1);

    let stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 1,
        ..FakeStats::default()
    }));
    let cancellation = Arc::new(neotales_resilience::Cancellation::new());
    let sleeper_drops = Arc::new(AtomicUsize::new(0));
    let sleeper_drop = Arc::clone(&sleeper_drops);
    let runtime = RetryRuntime::new(
        Arc::clone(&cancellation),
        || Box::pin(pending()),
        move |_| {
            let drop_guard = DropCounter(Arc::clone(&sleeper_drop));
            Box::pin(async move {
                let _drop = drop_guard;
                pending::<()>().await;
            })
        },
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async(
            "safe while backoff is pending",
            QueryOptions::new().replay_safe(),
        )
        .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(stats.lock().unwrap().execute_calls, 1);
    assert_eq!(sleeper_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One paired cancellation/deadline acquisition-race scenario.
async fn cancellation_or_deadline_at_acquisition_completion_never_dispatches_sql() {
    let stats = Arc::new(Mutex::new(FakeStats {
        pending_acquire: true,
        ..FakeStats::default()
    }));
    let cancellation = Arc::new(neotales_resilience::Cancellation::new());
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(retry_runtime(
                Arc::clone(&cancellation),
                Arc::new(Mutex::new(Vec::new())),
            )),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async(
            "must not dispatch after cancellation",
            QueryOptions::new().replay_safe(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if stats.lock().unwrap().acquire_waker.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("acquisition should become pending");
    let acquire_waker = {
        let mut stats_guard = stats.lock().unwrap();
        stats_guard.acquire_ready = true;
        stats_guard.acquire_waker.take().unwrap()
    };
    cancellation.cancel();
    acquire_waker.wake();
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        ErrorKind::Cancelled
    );
    {
        let stats_guard = stats.lock().unwrap();
        assert_eq!(stats_guard.execute_calls, 0);
        assert_eq!(stats_guard.acquisitions, 1);
    }

    // A ready total-budget timer can win at the same pending-acquisition boundary.
    let stats = Arc::new(Mutex::new(FakeStats {
        pending_acquire: true,
        ..FakeStats::default()
    }));
    let timer_stats = Arc::clone(&stats);
    let timeout_runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        move || {
            let timer_stats = Arc::clone(&timer_stats);
            Box::pin(poll_fn(move |cx| {
                let waker = {
                    let mut stats_guard = timer_stats.lock().unwrap();
                    stats_guard.acquire_ready = true;
                    stats_guard.acquire_waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake_by_ref();
                }
                let _ = cx;
                Poll::Ready(())
            }))
        },
        |_| Box::pin(ready(())),
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(timeout_runtime),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async(
            "must not dispatch after deadline",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(stats.lock().unwrap().execute_calls, 0);
}

#[tokio::test]
async fn total_budget_timer_interrupts_pending_acquisition_and_backoff() {
    let acquire_drops = Arc::new(AtomicUsize::new(0));
    let acquire_stats = Arc::new(Mutex::new(FakeStats {
        pending_acquire: true,
        ..FakeStats::default()
    }));
    let runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        || Box::pin(tokio::time::sleep(Duration::from_millis(10))),
        |_| Box::pin(ready(())),
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: acquire_stats,
            acquire_drops: Arc::clone(&acquire_drops),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async(
            "safe during pending acquisition",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(acquire_drops.load(Ordering::SeqCst), 1);

    let stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 1,
        ..FakeStats::default()
    }));
    let sleeper_drops = Arc::new(AtomicUsize::new(0));
    let sleeper_drop = Arc::clone(&sleeper_drops);
    let runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        || Box::pin(tokio::time::sleep(Duration::from_millis(10))),
        move |_| {
            let drop_guard = DropCounter(Arc::clone(&sleeper_drop));
            Box::pin(async move {
                let _drop = drop_guard;
                pending::<()>().await;
            })
        },
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async(
            "safe during pending backoff",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(stats.lock().unwrap().execute_calls, 1);
    assert_eq!(stats.lock().unwrap().session_drops, 1);
    assert_eq!(sleeper_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn total_budget_deadline_during_dispatched_write_is_unknown() {
    let stats = Arc::new(Mutex::new(FakeStats {
        pending_execute: true,
        ..FakeStats::default()
    }));
    let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let timer_expired = Arc::clone(&expired);
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(3),
            retry_runtime: Some(RetryRuntime::new(
                Arc::new(neotales_resilience::Cancellation::new()),
                move || {
                    let expired = Arc::clone(&timer_expired);
                    Box::pin(poll_fn(move |_| {
                        if expired.load(Ordering::SeqCst) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }))
                },
                |_| panic!("an unknown write must not be retried"),
                || 0,
            )),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async("synthetic write", QueryOptions::new().replay_safe())
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if stats.lock().unwrap().execute_waker.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the provider write should be pending");
    expired.store(true, Ordering::SeqCst);
    stats.lock().unwrap().execute_waker.take().unwrap().wake();
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    let stats = stats.lock().unwrap();
    assert_eq!(stats.execute_calls, 1);
    assert_eq!(stats.acquisitions, 1);
}

#[tokio::test]
async fn sqlite_busy_retries_after_lock_release_using_fresh_sessions() {
    let file = NamedTempFile::new().unwrap();
    let inspect = inspector(file.path()).await;
    sqlx::query("CREATE TABLE retry_data (value INTEGER)")
        .execute(&inspect)
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&inspect)
        .await
        .unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(2)).await,
        PoolPolicy {
            retry: retry_policy(10),
            retry_runtime: Some(RetryRuntime::new(
                Arc::new(neotales_resilience::Cancellation::new()),
                || Box::pin(tokio::time::sleep(Duration::from_secs(2))),
                |_| Box::pin(tokio::time::sleep(Duration::from_millis(10))),
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
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.outcome == TelemetryOutcome::Error(ErrorKind::Busy))
                .count()
                >= 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the held SQLite lock should produce two Busy attempts");
    sqlx::query("ROLLBACK").execute(&inspect).await.unwrap();
    assert_eq!(task.await.unwrap().unwrap(), 1);
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| { event.outcome == TelemetryOutcome::Error(ErrorKind::Busy) })
            .count()
            >= 2
    );
    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM retry_data")
        .fetch_one(&inspect)
        .await
        .unwrap();
    assert_eq!(stored, 1);
}

#[tokio::test]
async fn unknown_outcomes_and_transaction_statements_never_retry() {
    let unknown_stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 5,
        execute_error: Some(ErrorKind::CommitOutcomeUnknown),
        ..FakeStats::default()
    }));
    let waits = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&unknown_stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(
                Arc::new(neotales_resilience::Cancellation::new()),
                Arc::clone(&waits),
            )),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async(
            "caller asserted replay safety but result is unknown",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CommitOutcomeUnknown);
    assert_eq!(unknown_stats.lock().unwrap().execute_calls, 1);
    assert!(waits.lock().unwrap().is_empty());

    let tx_stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 5,
        ..FakeStats::default()
    }));
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&tx_stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(
                Arc::new(neotales_resilience::Cancellation::new()),
                Arc::new(Mutex::new(Vec::new())),
            )),
            ..PoolPolicy::default()
        },
    );
    let mut session = db.acquire_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let error = transaction
        .execute_async("transaction statement", QueryOptions::new().replay_safe())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(tx_stats.lock().unwrap().execute_calls, 1);
    let error = transaction.commit_async().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CommitOutcomeUnknown);
    assert_eq!(tx_stats.lock().unwrap().execute_calls, 1);
}

#[tokio::test]
async fn sqlite_returning_execution_error_is_conservatively_unknown_and_never_retried() {
    let file = NamedTempFile::new().unwrap();
    let inspect = inspector(file.path()).await;
    sqlx::query("CREATE TABLE returning_data (value INTEGER)")
        .execute(&inspect)
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let waits = Arc::new(Mutex::new(Vec::new()));
    let db = PolicyPool::new(
        provider(file.path(), Duration::from_millis(25)).await,
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(retry_runtime(
                Arc::new(neotales_resilience::Cancellation::new()),
                waits.clone(),
            )),
            telemetry_enabled: true,
            observer: Some(events_sink(events.clone())),
            ..PoolPolicy::default()
        },
    );
    // abs(i64::MIN) raises a SQLite execution error while evaluating RETURNING.
    // The provider does not prove whether a fetch is read-only or establish the write outcome.
    let error = db
        .fetch_all_async(
            "INSERT INTO returning_data VALUES (1) RETURNING abs(-9223372036854775808)",
            QueryOptions::new().replay_safe().limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert!(waits.lock().unwrap().is_empty());
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].outcome,
        TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
    );
    assert!(events[0].sql.is_none());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep the real SQLite hook/dispatch matrix in one scenario.
async fn nested_inner_hook_interruptions_never_dispatch_returning_writes() {
    for fetch in [false, true] {
        for cancel in [false, true] {
            for layers in [2, 3] {
                let file = NamedTempFile::new().unwrap();
                let inspect = inspector(file.path()).await;
                sqlx::query("CREATE TABLE guarded_returning (value TEXT)")
                    .execute(&inspect)
                    .await
                    .unwrap();
                let cancellation = Arc::new(neotales_resilience::Cancellation::new());
                let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let factories = Arc::new(AtomicUsize::new(0));
                let timer_count = factories.clone();
                let deadline = expired.clone();
                let runtime = RetryRuntime::new(
                    cancellation.clone(),
                    move || {
                        timer_count.fetch_add(1, Ordering::SeqCst);
                        let deadline = deadline.clone();
                        Box::pin(poll_fn(move |_| {
                            if deadline.load(Ordering::SeqCst) {
                                Poll::Ready(())
                            } else {
                                Poll::Pending
                            }
                        }))
                    },
                    |_| panic!("a rejected dispatch must not back off"),
                    || 0,
                );
                let events = Arc::new(Mutex::new(Vec::new()));
                let hook_calls = Arc::new(AtomicUsize::new(0));
                let mut pool: Arc<dyn Pool> =
                    Arc::new(provider(file.path(), Duration::from_millis(25)).await);
                for layer in 0..layers {
                    let token = cancellation.clone();
                    let deadline = expired.clone();
                    let calls = hook_calls.clone();
                    pool = Arc::new(PolicyPool::from_shared(
                        pool,
                        PoolPolicy {
                            telemetry_enabled: true,
                            sql_capture_enabled: true,
                            sql_hook: (layer == 0).then(|| {
                                Arc::new(move |_: &str| {
                                    calls.fetch_add(1, Ordering::SeqCst);
                                    if cancel {
                                        token.cancel();
                                    } else {
                                        deadline.store(true, Ordering::SeqCst);
                                    }
                                    Some("safe replacement".to_owned())
                                }) as neotales_dbapi::SqlCaptureHook
                            }),
                            observer: Some(events_sink(events.clone())),
                            retry: retry_policy(3),
                            retry_runtime: Some(runtime.clone()),
                            ..PoolPolicy::default()
                        },
                    ));
                }
                let sql =
                    "INSERT INTO guarded_returning VALUES ('private literal') RETURNING value";
                let options = QueryOptions::new().replay_safe();
                let error = if fetch {
                    pool.fetch_all_async(sql, options.limit(limit()))
                        .await
                        .unwrap_err()
                } else {
                    pool.execute_async(sql, options).await.unwrap_err()
                };
                assert_eq!(
                    error.kind(),
                    if cancel {
                        ErrorKind::Cancelled
                    } else {
                        ErrorKind::Timeout
                    }
                );
                assert_eq!(factories.load(Ordering::SeqCst), 1);
                assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
                assert!(events.lock().unwrap().is_empty(), "no SQL was dispatched");
                let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM guarded_returning")
                    .fetch_one(&inspect)
                    .await
                    .unwrap();
                assert_eq!(rows, 0);
            }
        }
    }
}

#[tokio::test]
async fn interrupted_final_attempt_preserves_unknown_write_and_commit_outcomes() {
    for unknown_kind in [
        ErrorKind::WriteOutcomeUnknown,
        ErrorKind::CommitOutcomeUnknown,
    ] {
        let stats = Arc::new(Mutex::new(FakeStats {
            fail_execute: 1,
            execute_error: Some(unknown_kind),
            ..FakeStats::default()
        }));
        let cancellation = Arc::new(neotales_resilience::Cancellation::new());
        stats.lock().unwrap().cancel_on_execute = Some(Arc::clone(&cancellation));
        let db = PolicyPool::new(
            FakePool {
                stats: Arc::clone(&stats),
                acquire_drops: Arc::new(AtomicUsize::new(0)),
            },
            PoolPolicy {
                retry: retry_policy(1),
                retry_runtime: Some(retry_runtime(
                    Arc::clone(&cancellation),
                    Arc::new(Mutex::new(Vec::new())),
                )),
                ..PoolPolicy::default()
            },
        );
        let error = db
            .execute_async(
                "explicitly replay-safe but provider reports uncertainty",
                QueryOptions::new().replay_safe(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), unknown_kind);
        assert_eq!(stats.lock().unwrap().execute_calls, 1);
        assert_eq!(stats.lock().unwrap().acquisitions, 1);
    }

    let stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 1,
        execute_error: Some(ErrorKind::CommitOutcomeUnknown),
        ..FakeStats::default()
    }));
    let timer_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    stats.lock().unwrap().deadline_on_execute = Some(Arc::clone(&timer_ready));
    let timer_flag = Arc::clone(&timer_ready);
    let runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        move || {
            let timer_flag = Arc::clone(&timer_flag);
            Box::pin(poll_fn(move |_| {
                if timer_flag.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }))
        },
        |_| Box::pin(ready(())),
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(1),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );
    let error = db
        .execute_async(
            "deadline races unknown commit outcome",
            QueryOptions::new().replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CommitOutcomeUnknown);
    assert_eq!(stats.lock().unwrap().execute_calls, 1);
}

#[tokio::test]
async fn timeout_errors_are_retried_only_when_policy_opts_in() {
    for (retry_timeouts, expected_calls) in [(false, 1), (true, 2)] {
        let stats = Arc::new(Mutex::new(FakeStats {
            fail_fetch: 1,
            fetch_error: Some(ErrorKind::Timeout),
            ..FakeStats::default()
        }));
        let db = PolicyPool::new(
            FakePool {
                stats: Arc::clone(&stats),
                acquire_drops: Arc::new(AtomicUsize::new(0)),
            },
            PoolPolicy {
                retry: RetryPolicy::new(
                    2,
                    neotales_dbapi::Delay::Fixed(Duration::ZERO),
                    retry_timeouts,
                )
                .unwrap(),
                retry_runtime: Some(retry_runtime(
                    Arc::new(neotales_resilience::Cancellation::new()),
                    Arc::new(Mutex::new(Vec::new())),
                )),
                ..PoolPolicy::default()
            },
        );
        let result = db
            .fetch_all_async(
                "caller asserted replay safety",
                QueryOptions::new().replay_safe().limit(limit()),
            )
            .await;
        if retry_timeouts {
            assert_eq!(result.unwrap().len(), 1);
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Timeout);
        }
        assert_eq!(stats.lock().unwrap().fetch_calls, expected_calls);
    }
}

#[tokio::test]
async fn dropping_retrying_pool_future_stops_before_another_attempt() {
    let stats = Arc::new(Mutex::new(FakeStats {
        fail_execute: 10,
        ..FakeStats::default()
    }));
    let backoff_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::clone(&backoff_started);
    let sleeper_drops = Arc::new(AtomicUsize::new(0));
    let sleeper_drop = Arc::clone(&sleeper_drops);
    let runtime = RetryRuntime::new(
        Arc::new(neotales_resilience::Cancellation::new()),
        || Box::pin(pending()),
        move |_| {
            started.store(true, Ordering::SeqCst);
            let drop_guard = DropCounter(Arc::clone(&sleeper_drop));
            Box::pin(async move {
                let _drop = drop_guard;
                pending::<()>().await;
            })
        },
        || 0,
    );
    let db = PolicyPool::new(
        FakePool {
            stats: Arc::clone(&stats),
            acquire_drops: Arc::new(AtomicUsize::new(0)),
        },
        PoolPolicy {
            retry: retry_policy(5),
            retry_runtime: Some(runtime),
            ..PoolPolicy::default()
        },
    );
    let task = tokio::spawn(async move {
        db.execute_async(
            "caller drops while in backoff",
            QueryOptions::new().replay_safe(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backoff_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retry should reach backoff");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(stats.lock().unwrap().execute_calls, 1);
    assert_eq!(sleeper_drops.load(Ordering::SeqCst), 1);
}
