//! File-backed integration tests for transactional SQLite migration behavior.
use neotales_dbapi::{
    DbEvent, ErrorKind, PoolPolicy, QueryOptions, RowLimit, SqlitePolicyPool,
    SqlitePool as DbSqlitePool, TelemetryOutcome,
    sync::{Pool as SyncPool, Session as SyncSession},
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
use neotales_dbapi_sqlite_direct::{
    SqliteConfig as DirectConfig, SqlitePoolProvider as DirectProvider,
};
use neotales_migration::{FlattenManifest, Migration, Migrations};
use neotales_migration_sqlite::{Error, SqliteBackend};
use sqlx::{Connection, Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};
use std::{str::FromStr, time::Duration};

async fn make_pool(max: u32) -> (tempfile::TempDir, SqlitePool, SqlitePoolProvider) {
    let dir = tempfile::tempdir().unwrap();
    let database_path = dir.path().join("db.sqlite");
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(database_path.clone())
            .busy_timeout(Duration::from_millis(100))
            .max_connections(max),
    )
    .await
    .unwrap();
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(max)
        .connect_with(options)
        .await
        .unwrap();
    (dir, pool, provider)
}
fn m(v: u64, n: &str, table: &str) -> Migration {
    Migration::new(
        v,
        n,
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
        Some(format!("DROP TABLE {table}")),
    )
    .unwrap()
}
fn definitions(items: Vec<Migration>) -> Migrations {
    Migrations::new(items).unwrap()
}

fn observed_pool<P: DbSqlitePool + 'static>(
    provider: P,
) -> (
    SqlitePolicyPool,
    std::sync::Arc<std::sync::Mutex<Vec<DbEvent>>>,
) {
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = std::sync::Arc::clone(&events);
    let pool = SqlitePolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(std::sync::Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    (pool, events)
}

async fn wait_for_script_pending(
    operation: &mut std::pin::Pin<Box<impl std::future::Future<Output = Result<(), Error>>>>,
    script_polled: &std::sync::atomic::AtomicBool,
) {
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| {
            // The hook runs immediately before polling the provider script. Only stop after that
            // poll returned Pending, not after an elapsed guess about acquisition/DDL speed.
            match operation.as_mut().poll(cx) {
                std::task::Poll::Ready(result) => panic!("slow migration completed: {result:?}"),
                std::task::Poll::Pending
                    if script_polled.load(std::sync::atomic::Ordering::SeqCst) =>
                {
                    std::task::Poll::Ready(())
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }),
    )
    .await
    .expect("migration did not dispatch its script");
}

#[tokio::test]
async fn direct_provider_runs_up_repeat_down_and_flatten() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct-migrations.sqlite");
    let provider = DirectProvider::connect(DirectConfig::new(&path).max_connections(1))
        .await
        .unwrap();
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<DbEvent>::new()));
    let observed = std::sync::Arc::clone(&events);
    let policy_pool = SqlitePolicyPool::new(
        provider.clone(),
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(std::sync::Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let migrations = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&policy_pool, "direct", &migrations, "fp", &["one"])
        .await
        .unwrap();
    let mut session = provider.acquire_sync_sqlite().unwrap();
    let row = neotales_dbapi::sync::Session::fetch_one(
        &mut session,
        "PRAGMA busy_timeout",
        QueryOptions::new().limit(RowLimit::new(1, 128).unwrap()),
    )
    .unwrap();
    assert_eq!(row.get::<i64>("timeout").unwrap(), 5_000);
    drop(session);

    let mut session = SyncPool::acquire(&provider).unwrap();
    SyncSession::execute(
        session.as_mut(),
        "PRAGMA busy_timeout = 100",
        QueryOptions::new(),
    )
    .unwrap();
    drop(session);
    let mut session = SyncPool::acquire(&provider).unwrap();
    let row = SyncSession::fetch_one(
        session.as_mut(),
        "PRAGMA busy_timeout",
        QueryOptions::new().limit(RowLimit::new(1, 128).unwrap()),
    )
    .unwrap();
    assert_eq!(row.get::<i64>("timeout").unwrap(), 5_000);
    drop(session);
    backend
        .up_with_pool(&policy_pool, "direct", &migrations, "fp", &["one"])
        .await
        .unwrap();
    backend
        .down_with_pool(&policy_pool, "direct", &migrations, "fp", 1, &["one"])
        .await
        .unwrap();

    let old = vec![m(1, "old", "old")];
    let baseline = m(10, "baseline", "replacement").into_baseline();
    let manifest = FlattenManifest::new(old.clone(), baseline).unwrap();
    let old_set = definitions(old);
    backend
        .up_with_pool(&policy_pool, "flatten-direct", &old_set, "fp", &["old"])
        .await
        .unwrap();
    backend
        .flatten_with_pool(
            &policy_pool,
            "flatten-direct",
            &manifest,
            "fp".into(),
            &["old"],
        )
        .await
        .unwrap();
    let events = events.lock().unwrap();
    assert_ne!(events.len(), 0);
    assert!(events.iter().all(|event| event.sql.is_none()));
    assert!(
        events
            .iter()
            .any(|event| event.outcome == TelemetryOutcome::Success)
    );
}

#[tokio::test]
async fn direct_provider_migration_failure_rolls_back_and_drift_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let provider = DirectProvider::connect(
        DirectConfig::new(dir.path().join("direct-failure.sqlite")).max_connections(1),
    )
    .await
    .unwrap();
    let backend = SqliteBackend::new("migration_log", 100).unwrap();

    let failing = Migration::new(
        1,
        "partial",
        "CREATE TABLE rollback_marker (id INTEGER); INSERT INTO missing_table VALUES (1)",
        None,
    )
    .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(
                &provider,
                "failed-module",
                &definitions(vec![failing]),
                "fp",
                &["rollback_marker"],
            )
            .await,
        Err(Error::Database)
    ));
    assert_eq!(
        direct_scalar(&provider, "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='rollback_marker'").await,
        0,
        "the earlier DDL in the failed migration is rolled back"
    );
    assert_eq!(
        direct_scalar(&provider, "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='migration_log'").await,
        0,
        "failed first migration leaves no history table"
    );

    backend
        .up_with_pool(
            &provider,
            "drift-module",
            &definitions(vec![m(1, "original", "original_table")]),
            "fp",
            &["original_table"],
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(
                &provider,
                "drift-module",
                &definitions(vec![m(1, "changed", "changed_table")]),
                "fp",
                &["original_table", "changed_table"],
            )
            .await,
        Err(Error::History)
    ));
    assert_eq!(
        direct_scalar(&provider, "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='original_table'").await,
        1
    );
    assert_eq!(
        direct_scalar(&provider, "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='changed_table'").await,
        0
    );
}

#[tokio::test]
async fn direct_provider_rejects_untracked_main_objects_and_temp_shadows() {
    let dir = tempfile::tempdir().unwrap();
    let provider = DirectProvider::connect(
        DirectConfig::new(dir.path().join("direct-untracked.sqlite")).max_connections(1),
    )
    .await
    .unwrap();
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    {
        let mut session = provider.acquire_sqlite().await.unwrap();
        session
            .execute_async(
                "CREATE TABLE untracked_main (value INTEGER)",
                QueryOptions::new(),
            )
            .await
            .unwrap();
        session
            .execute_async(
                "INSERT INTO untracked_main VALUES (41)",
                QueryOptions::new(),
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        backend
            .up_with_pool(
                &provider,
                "untracked-module",
                &definitions(vec![m(1, "target", "target_table")]),
                "fp",
                &["untracked_main"],
            )
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert_eq!(
        direct_scalar(
            &provider,
            "SELECT COUNT(*) AS value FROM untracked_main WHERE value=41"
        )
        .await,
        1,
        "untracked main data remains intact"
    );
    assert_eq!(
        direct_scalar(
            &provider,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='target_table'"
        )
        .await,
        0
    );

    {
        let mut session = provider.acquire_sqlite().await.unwrap();
        session
            .execute_async(
                "CREATE TEMP TABLE shadowed_object (value INTEGER)",
                QueryOptions::new(),
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        backend
            .up_with_pool(
                &provider,
                "shadow-module",
                &definitions(vec![m(1, "shadow-target", "shadow_target")]),
                "fp",
                &["shadowed_object"],
            )
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert_eq!(
        direct_scalar(
            &provider,
            "SELECT COUNT(*) AS value FROM temp.shadowed_object"
        )
        .await,
        0,
        "the temporary shadow remains present after rejection"
    );
    assert_eq!(
        direct_scalar(&provider, "SELECT COUNT(*) AS value FROM sqlite_master WHERE type='table' AND name='shadow_target'").await,
        0
    );
}

async fn direct_scalar(provider: &DirectProvider, sql: &str) -> i64 {
    let mut session = provider.acquire_sqlite().await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 128).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("value").unwrap()
}

#[tokio::test]
#[cfg(feature = "sqlx-compat")]
async fn sqlx_compatibility_overloads_cover_up_down_and_flatten() {
    let (_dir, _inspect, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("compat_migration_log", 100).unwrap();
    let first = definitions(vec![m(1, "first", "compat_first")]);
    backend
        .up(&provider, "compat-up-down", &first, "fp", &["compat_first"])
        .await
        .unwrap();
    backend
        .down(
            &provider,
            "compat-up-down",
            &first,
            "fp",
            1,
            &["compat_first"],
        )
        .await
        .unwrap();

    let old = vec![m(1, "old", "compat_old")];
    let migrations = definitions(old.clone());
    backend
        .up(
            &provider,
            "compat-flatten",
            &migrations,
            "fp",
            &["compat_old"],
        )
        .await
        .unwrap();
    let manifest =
        FlattenManifest::new(old, m(10, "baseline", "compat_baseline").into_baseline()).unwrap();
    backend
        .flatten(
            &provider,
            "compat-flatten",
            &manifest,
            "fp".into(),
            &["compat_old"],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlx_provider_restores_configured_timeout_after_migration_override() {
    let dir = tempfile::tempdir().unwrap();
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(dir.path().join("timeout-reset.sqlite"))
            .busy_timeout(Duration::from_secs(5))
            .max_connections(1),
    )
    .await
    .unwrap();
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let migrations = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "timeout", &migrations, "fp", &["one"])
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "PRAGMA busy_timeout",
            QueryOptions::new().limit(RowLimit::new(1, 128).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("timeout").unwrap(), 5_000);
}

#[tokio::test]
async fn migration_policy_observes_write_failure_without_sql_capture() {
    let dir = tempfile::tempdir().unwrap();
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(dir.path().join("migration-timeout.sqlite"))
            .busy_timeout(Duration::from_secs(1))
            .max_connections(1),
    )
    .await
    .unwrap();
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<DbEvent>::new()));
    let observed = std::sync::Arc::clone(&events);
    let policy_pool = SqlitePolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(std::sync::Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let backend = SqliteBackend::new("migration_log", 1_000).unwrap();
    let slow = Migration::new(
        1,
        "slow",
        "CREATE TABLE partial_result (value INTEGER); INSERT INTO table_that_does_not_exist VALUES (1);",
        None,
    )
    .unwrap();
    let result = backend
        .up_with_pool(&policy_pool, "timeout", &definitions(vec![slow]), "fp", &[])
        .await;
    assert!(matches!(result, Err(Error::Database)));
    let events = events.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| { event.outcome == TelemetryOutcome::Error(ErrorKind::Connection) })
    );
    assert!(events.iter().all(|event| event.sql.is_none()));
}

#[tokio::test]
async fn migration_policy_observes_cancellation_as_unknown_after_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let provider = DirectProvider::connect(
        DirectConfig::new(dir.path().join("migration-cancel.sqlite")).max_connections(1),
    )
    .await
    .unwrap();
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<DbEvent>::new()));
    let observed = std::sync::Arc::clone(&events);
    let script_polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let script_started = std::sync::Arc::clone(&script_polled);
    let sql = "CREATE TABLE slow_result AS WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n";
    // Saturate the sole session to reproduce cancellation before dispatch deterministically.
    let held = provider.acquire_sqlite_async().await.unwrap();
    let policy_pool = SqlitePolicyPool::new(
        provider.clone(),
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(std::sync::Arc::new(move |statement| {
                if statement == sql {
                    script_started.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                // Use the hook as a stage marker without retaining even fixture SQL.
                None
            })),
            observer: Some(std::sync::Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let backend = SqliteBackend::new("migration_log", 1_000).unwrap();
    let slow = Migration::new(1, "slow", sql, None).unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(20),
        backend.up_with_pool(&policy_pool, "cancel", &definitions(vec![slow]), "fp", &[]),
    )
    .await;
    assert!(result.is_err(), "acquisition should remain blocked");
    assert!(
        events.lock().unwrap().is_empty(),
        "no statement was dispatched"
    );
    drop(held);

    let migrations = definitions(vec![Migration::new(1, "slow", sql, None).unwrap()]);
    let mut operation =
        Box::pin(backend.up_with_pool(&policy_pool, "cancel", &migrations, "fp", &[]));
    wait_for_script_pending(&mut operation, &script_polled).await;
    drop(operation);
    {
        let events = events.lock().unwrap();
        assert!(events.iter().any(|event| {
            event.outcome == TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
        }));
        assert!(events.iter().all(|event| event.sql.is_none()));
    }
    drop(policy_pool);
    tokio::time::timeout(Duration::from_secs(10), async {
        // The cancelled worker retains its slot until it stops. A replacement must have a
        // fresh cancellation latch, and closing the old connection must roll back its DDL.
        let mut next = provider.acquire_sqlite_async().await.unwrap();
        let row = next.fetch_one_async(
            "SELECT count(*) AS value FROM sqlite_master WHERE name IN ('slow_result','migration_log','migration_log_modules')",
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        ).await.unwrap();
        assert_eq!(row.get::<i64>("value").unwrap(), 0);
        next.execute_async("CREATE TABLE after_cancellation(value INTEGER)", QueryOptions::new())
            .await.unwrap();
    }).await.expect("cancelled worker did not release its slot for a clean replacement");
    provider.close();
    tokio::time::timeout(Duration::from_secs(10), provider.wait_for_cleanup())
        .await
        .expect("cancelled script worker did not stop")
        .unwrap();
}

async fn provider_script(provider: &SqlitePoolProvider, sql: &str) {
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .unwrap();
}

async fn provider_i64(provider: &SqlitePoolProvider, sql: &str) -> i64 {
    let mut session = provider.acquire_sqlite().await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("value").unwrap()
}

async fn provider_text(provider: &SqlitePoolProvider, sql: &str) -> String {
    let mut session = provider.acquire_sqlite().await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("value").unwrap()
}

#[tokio::test]
async fn fresh_repeat_up_down_and_fingerprint_validation() {
    let (_dir, pool, provider) = make_pool(2).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![m(1, "one", "one"), m(2, "two", "two")]);
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["one", "two"])
        .await
        .unwrap();
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["one", "two"])
        .await
        .unwrap();
    assert_eq!(
        sqlx::query(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('one','two')"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>(0),
        2
    );
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &ms, "changed", &[])
            .await,
        Err(Error::History)
    ));
    backend
        .down_with_pool(&provider, "module", &ms, "fp", 1, &["one", "two"])
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='two'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<i64, _>(0),
        0
    );
}

#[tokio::test]
async fn down_preflight_and_sql_failure_have_no_partial_effects() {
    let (_dir, pool, provider) = make_pool(2).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![
        m(1, "one", "one"),
        Migration::new(2, "no-down", "CREATE TABLE two (id INTEGER)", None).unwrap(),
    ]);
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["one", "two"])
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down_with_pool(&provider, "module", &ms, "fp", 2, &["one", "two"])
            .await,
        Err(Error::Core)
    ));
    assert_eq!(
        sqlx::query(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('one','two')"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>(0),
        2
    );
    let bad = definitions(vec![
        m(1, "good", "good"),
        Migration::new(2, "bad", "INVALID SQL", Some("SELECT 1".into())).unwrap(),
    ]);
    let (_dir2, failed_pool, failed_provider) = make_pool(1).await;
    assert!(matches!(
        backend
            .up_with_pool(&failed_provider, "failed", &bad, "fp", &[])
            .await,
        Err(Error::Database)
    ));
    assert_eq!(sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('good','migration_log')").fetch_one(&failed_pool).await.unwrap().get::<i64,_>(0), 0);
}

#[tokio::test]
async fn untracked_schema_and_identifier_injection_are_rejected_intact() {
    let (_dir, pool, provider) = make_pool(1).await;
    sqlx::query("CREATE TABLE application_data (value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO application_data VALUES ('preserve')")
        .execute(&pool)
        .await
        .unwrap();
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![m(1, "one", "one")]);
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &ms, "fp", &["application_data"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    let preserved: String = sqlx::query("SELECT value FROM application_data")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(preserved, "preserve");
    assert!(SqliteBackend::new("log; DROP TABLE application_data", 100).is_err());
    let manifest = FlattenManifest::new(
        vec![m(1, "old", "old_table")],
        m(10, "base", "new_table").into_baseline(),
    )
    .unwrap();
    assert!(matches!(
        backend
            .flatten_with_pool(
                &provider,
                "flatten",
                &manifest,
                "fp".into(),
                &["application_data"]
            )
            .await,
        Err(Error::UntrackedObjects)
    ));
}

#[tokio::test]
async fn persisted_checksum_drift_is_rejected() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["one"])
        .await
        .unwrap();
    sqlx::query("UPDATE migration_log SET checksum='tampered' WHERE module='module'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &ms, "fp", &[])
            .await,
        Err(Error::History)
    ));
}

#[tokio::test]
async fn flatten_fresh_complete_and_partial_history() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let old = vec![m(1, "old", "old")];
    let baseline = m(10, "baseline", "replacement").into_baseline();
    let manifest = FlattenManifest::new(old.clone(), baseline.clone()).unwrap();
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "fresh", &manifest, "fp".into(), &[])
            .await,
        Err(Error::Core)
    ));
    let baseline_set = definitions(vec![baseline.clone(), m(11, "after", "after")]);
    let old_migrations = definitions(old.clone());
    backend
        .up_with_pool(&provider, "complete", &old_migrations, "fp", &["old"])
        .await
        .unwrap();
    backend
        .flatten_with_pool(&provider, "complete", &manifest, "fp".into(), &["old"])
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='replacement'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<i64, _>(0),
        0
    );
    backend
        .up_with_pool(
            &provider,
            "fresh",
            &definitions(vec![baseline.clone()]),
            "fp",
            &["replacement"],
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down_with_pool(
                &provider,
                "fresh",
                &baseline_set,
                "fp",
                1,
                &["replacement", "after"]
            )
            .await,
        Err(Error::Core)
    ));
    let partial = vec![m(1, "old", "partial"), m(2, "later", "later")];
    let partial_manifest =
        FlattenManifest::new(partial.clone(), m(10, "base", "new").into_baseline()).unwrap();
    let partial_migrations = definitions(vec![partial[0].clone()]);
    backend
        .up_with_pool(
            &provider,
            "partial",
            &partial_migrations,
            "fp",
            &["partial"],
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .flatten_with_pool(
                &provider,
                "partial",
                &partial_manifest,
                "fp".into(),
                &["partial", "later"]
            )
            .await,
        Err(Error::Core)
    ));
}

#[tokio::test]
async fn concurrent_same_module_and_lock_timeout() {
    let (_dir, pool, provider) = make_pool(3).await;
    let (provider, events) = observed_pool(provider);
    let provider = std::sync::Arc::new(provider);
    // Successful contention needs the normal lock budget; 80 ms is only the deliberately
    // held-lock failure case below, not a bound on another transaction's fsync/scheduling.
    let backend = SqliteBackend::new("migration_log", 5_000).unwrap();
    let ms = definitions(vec![m(1, "one", "one")]);
    let (a, b) = tokio::join!(
        backend.up_with_pool(provider.as_ref(), "module", &ms, "fp", &["one"]),
        backend.up_with_pool(provider.as_ref(), "module", &ms, "fp", &["one"])
    );
    assert!(
        a.is_ok() && b.is_ok(),
        "concurrent migration outcomes: {a:?}, {b:?}; stages: {:?}",
        events.lock().unwrap()
    );
    let mut held = pool.acquire().await.unwrap();
    let tx = held.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let task = {
        let backend = SqliteBackend::new("migration_log", 80).unwrap();
        let provider = provider.clone();
        let ms = ms.clone();
        tokio::spawn(async move {
            backend
                .up_with_pool(provider.as_ref(), "other", &ms, "fp", &["one"])
                .await
        })
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn cancelled_operation_rolls_back_and_pool_connection_is_reusable() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 5000).unwrap();
    let slow = Migration::new(
        1,
        "slow",
        "CREATE TABLE one (id INTEGER); WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<500000) INSERT INTO one SELECT x FROM c",
        Some("DROP TABLE one".into()),
    ).unwrap();
    let ms = definitions(vec![slow]);
    let task = {
        let backend = backend.clone();
        let provider = provider.clone();
        let ms = ms.clone();
        tokio::spawn(async move {
            backend
                .up_with_pool(&provider, "module", &ms, "fp", &["one"])
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(25)).await;
    task.abort();
    let _ = task.await;
    let mut reused = pool.acquire().await.unwrap();
    let _ = sqlx::query("SELECT 1")
        .fetch_one(&mut *reused)
        .await
        .unwrap();
    drop(reused);
    assert_eq!(sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('one','migration_log')")
        .fetch_one(&pool).await.unwrap().get::<i64,_>(0), 0, "cancelled transaction must roll back DDL and history");
    let fast = definitions(vec![m(1, "fast", "one")]);
    // A second operation proves the returned pooled connection was not left in a transaction.
    backend
        .up_with_pool(&provider, "module", &fast, "fp", &["one"])
        .await
        .unwrap();
    assert_eq!(
        sqlx::query("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='one'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<i64, _>(0),
        1
    );
}

#[tokio::test]
async fn unregistered_module_rejects_existing_objects_case_insensitively() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    backend
        .up_with_pool(
            &provider,
            "first",
            &definitions(vec![m(1, "one", "one")]),
            "fp",
            &[],
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE Existing (value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let ms = definitions(vec![m(1, "two", "two")]);
    assert!(matches!(
        backend
            .up_with_pool(&provider, "second", &ms, "fp", &["existing"])
            .await,
        Err(Error::UntrackedObjects)
    ));
}

#[tokio::test]
async fn cancellation_interrupt_reproduction() {
    let (dir, pool, provider) = make_pool(1).await;
    let options = SqliteConnectOptions::from_str(&format!(
        "sqlite://{}",
        dir.path().join("db.sqlite").display()
    ))
    .unwrap()
    .busy_timeout(Duration::from_millis(20));
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let backend = SqliteBackend::new("migration_log", 5_000).unwrap();
    // An unbounded INSERT made the synchronization wait consume unbounded disk under load.
    // This aggregate is just as nonterminating, but has constant storage.
    let sql = "CREATE TABLE slow AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT sum(x) FROM c";
    let script_polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let script_started = std::sync::Arc::clone(&script_polled);
    let observed = SqlitePolicyPool::new(
        provider.clone(),
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            // An observer enables the hook; the hook always suppresses SQL capture.
            observer: Some(std::sync::Arc::new(|_| {})),
            sql_hook: Some(std::sync::Arc::new(move |statement| {
                if statement == sql {
                    script_started.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                None
            })),
            ..PoolPolicy::default()
        },
    );
    let ms = definitions(vec![Migration::new(1, "slow", sql, None).unwrap()]);
    let mut operation = Box::pin(backend.up_with_pool(&observed, "slow", &ms, "fp", &[]));
    wait_for_script_pending(&mut operation, &script_polled).await;
    // The migration is now past BEGIN IMMEDIATE: the inspector cannot steal its lock first.
    let error = sqlx::query("BEGIN IMMEDIATE")
        .execute(&writer)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("5"),
        "expected SQLITE_BUSY while migration owns writer lock"
    );
    drop(operation);
    tokio::time::timeout(Duration::from_secs(10), async {
        sqlx::query("CREATE TABLE writer_after_cancel(x)")
            .execute(&writer)
            .await
            .unwrap();
        let fast = definitions(vec![m(1, "fast", "fast")]);
        SqliteBackend::new("migration_log", 5_000)
            .unwrap()
            .up_with_pool(&provider, "fast", &fast, "fp", &[])
            .await
            .unwrap();
    })
    .await
    .expect("cancelled migration failed to release writer or pool");
    let slow_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name='slow'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(slow_count, 0);
}

#[tokio::test]
async fn implicit_rollback_discards_poisoned_connection() {
    let (_dir, _pool, provider) = make_pool(1).await;
    provider_script(&provider, "CREATE TEMP TABLE connection_marker(x)").await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let bad = definitions(vec![Migration::new(
        1,
        "rollback",
        "CREATE TABLE unique_values(x UNIQUE); INSERT INTO unique_values VALUES (1); INSERT OR ROLLBACK INTO unique_values VALUES (1)",
        None,
    )
    .unwrap()]);
    assert!(matches!(
        backend
            .up_with_pool(&provider, "bad", &bad, "fp", &[])
            .await,
        Err(Error::Database)
    ));
    let marker_count = provider_i64(
        &provider,
        "SELECT count(*) AS value FROM temp.sqlite_master WHERE name='connection_marker'",
    )
    .await;
    assert_eq!(marker_count, 0, "poisoned connection was reused");
    let good = definitions(vec![m(1, "good", "good")]);
    for _ in 0..3 {
        tokio::time::timeout(
            Duration::from_secs(2),
            backend.up_with_pool(&provider, "good", &good, "fp", &[]),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

#[tokio::test]
async fn nul_sql_is_rejected_before_any_script_executes() {
    const CHILD: &str = "NEOTALES_SQLITE_NUL_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "nul_sql_is_rejected_before_any_script_executes",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "NUL subprocess failed: {status}");
                return;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("NUL subprocess timed out");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let scripts = definitions(vec![
        m(1, "first", "first"),
        Migration::new(2, "nul", "SELECT 1;\0SELECT 2", None).unwrap(),
    ]);
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            backend.up_with_pool(&provider, "nul", &scripts, "fp", &[])
        )
        .await
        .unwrap(),
        Err(Error::Core)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name='first'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn orphan_rows_are_rejected_before_new_module_execution() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "first", &ms, "fp", &[])
        .await
        .unwrap();
    sqlx::query("INSERT INTO migration_log SELECT 'orphan', version, checksum, fingerprint, barrier, barrier_checksum FROM migration_log WHERE module='first'")
        .execute(&pool).await.unwrap();
    let other = definitions(vec![m(1, "two", "two")]);
    assert!(matches!(
        backend
            .up_with_pool(&provider, "orphan", &other, "fp", &[])
            .await,
        Err(Error::History)
    ));
    assert_eq!(
        sqlx::query("SELECT count(*) FROM sqlite_master WHERE name='two'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get::<i64, _>(0),
        0
    );
}

#[tokio::test]
async fn deleted_tail_is_detected_before_reexecution() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![
        Migration::new(
            1,
            "setup",
            "CREATE TABLE events(value TEXT)",
            Some("DROP TABLE events".into()),
        )
        .unwrap(),
        Migration::new(
            2,
            "append",
            "INSERT INTO events VALUES ('once')",
            Some("DELETE FROM events".into()),
        )
        .unwrap(),
    ]);
    backend
        .up_with_pool(&provider, "events", &ms, "fp", &["events"])
        .await
        .unwrap();
    sqlx::query("DELETE FROM migration_log WHERE module='events' AND version=2")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "events", &ms, "fp", &["events"])
            .await,
        Err(Error::History)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn flatten_rejects_orphans_and_untracked_objects_with_shared_ledger() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let old = m(1, "old", "old");
    let manifest =
        FlattenManifest::new(vec![old.clone()], m(10, "base", "base").into_baseline()).unwrap();
    backend
        .up_with_pool(&provider, "first", &definitions(vec![old]), "fp", &["old"])
        .await
        .unwrap();
    sqlx::query("INSERT INTO migration_log SELECT 'orphan',version,checksum,fingerprint,barrier,barrier_checksum FROM migration_log WHERE module='first'").execute(&pool).await.unwrap();
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "orphan", &manifest, "fp".into(), &[])
            .await,
        Err(Error::History)
    ));
    sqlx::query("CREATE TABLE Existing(value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO Existing VALUES ('preserve')")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "new", &manifest, "fp".into(), &["existing"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "new", &manifest, "fp".into(), &[])
            .await,
        Err(Error::Core)
    ));
    let value: String = sqlx::query_scalar("SELECT value FROM Existing")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(value, "preserve");
}

#[tokio::test]
async fn empty_history_retains_binding_after_down_and_detects_deleted_only_row() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let empty = definitions(vec![]);
    backend
        .up_with_pool(&provider, "empty", &empty, "first", &[])
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "empty", &empty, "different", &[])
            .await,
        Err(Error::History)
    ));
    let ms = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "empty", &ms, "first", &["one"])
        .await
        .unwrap();
    backend
        .down_with_pool(&provider, "empty", &ms, "first", 1, &["one"])
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "empty", &empty, "different", &[])
            .await,
        Err(Error::History)
    ));
    backend
        .up_with_pool(&provider, "empty", &ms, "first", &[])
        .await
        .unwrap();
    sqlx::query("DELETE FROM migration_log WHERE module='empty'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "empty", &ms, "first", &[])
            .await,
        Err(Error::History)
    ));
}

#[tokio::test]
async fn concurrent_distinct_modules_initialize_shared_history() {
    let (_dir, pool, provider) = make_pool(4).await;
    let (provider, events) = observed_pool(provider);
    let backend = SqliteBackend::new("migration_log", 5_000).unwrap();
    let a = definitions(vec![m(1, "a", "a")]);
    let b = definitions(vec![m(1, "b", "b")]);
    let (left, right) = tokio::join!(
        backend.up_with_pool(&provider, "a", &a, "a-fp", &["a"]),
        backend.up_with_pool(&provider, "b", &b, "b-fp", &["b"])
    );
    assert!(
        left.is_ok() && right.is_ok(),
        "concurrent migration outcomes: {left:?}, {right:?}; stages: {:?}",
        events.lock().unwrap()
    );
    let modules: i64 =
        sqlx::query_scalar("SELECT count(*) FROM migration_log_modules WHERE applied_count=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(modules, 2);
    backend
        .up_with_pool(&provider, "a", &a, "a-fp", &["a"])
        .await
        .unwrap();
    backend
        .up_with_pool(&provider, "b", &b, "b-fp", &["b"])
        .await
        .unwrap();
}

#[tokio::test]
async fn temp_ledger_shadows_do_not_capture_main_history() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let empty = definitions(vec![]);
    backend
        .up_with_pool(&provider, "init", &empty, "fp", &[])
        .await
        .unwrap();
    provider_script(&provider, "CREATE TEMP TABLE Migration_Log (module TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, fingerprint TEXT NOT NULL, barrier INTEGER, barrier_checksum TEXT, PRIMARY KEY(module,version)); CREATE TEMP TABLE Migration_Log_Modules (module TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, barrier INTEGER, barrier_checksum TEXT, applied_count INTEGER NOT NULL, head_version INTEGER, head_checksum TEXT)").await;
    let ms = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "app", &ms, "fp", &["one"])
        .await
        .unwrap();
    let main: i64 =
        sqlx::query_scalar("SELECT count(*) FROM main.migration_log WHERE module='app'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let temp = provider_i64(
        &provider,
        "SELECT count(*) AS value FROM temp.migration_log WHERE module='app'",
    )
    .await;
    assert_eq!((main, temp), (1, 0));
    let manifest = FlattenManifest::new(
        vec![m(1, "one", "one")],
        m(10, "base", "base").into_baseline(),
    )
    .unwrap();
    backend
        .flatten_with_pool(&provider, "app", &manifest, "fp".into(), &["one"])
        .await
        .unwrap();
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM main.migration_log WHERE module='app'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(version, 10);
    let temp_rows = provider_i64(
        &provider,
        "SELECT count(*) AS value FROM temp.migration_log",
    )
    .await;
    assert_eq!(temp_rows, 0);
}

#[tokio::test]
async fn temp_managed_objects_are_rejected_and_preserved() {
    let (_dir, _pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    provider_script(
        &provider,
        "CREATE TEMP TABLE App(value TEXT); INSERT INTO temp.App VALUES ('keep')",
    )
    .await;
    let old = m(1, "old", "old");
    let manifest = FlattenManifest::new(vec![old], m(10, "base", "base").into_baseline()).unwrap();
    assert!(matches!(
        backend
            .up_with_pool(
                &provider,
                "x",
                &definitions(vec![m(1, "app", "app")]),
                "fp",
                &["aPP"]
            )
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "x", &manifest, "fp".into(), &["APP"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    let value = provider_text(&provider, "SELECT value AS value FROM temp.app").await;
    assert_eq!(value, "keep");
}

#[tokio::test]
async fn same_named_temp_table_and_trigger_are_rejected_without_discarding_connection() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let migrations = definitions(vec![m(1, "app", "app")]);
    backend
        .up_with_pool(&provider, "module", &migrations, "fp", &["app"])
        .await
        .unwrap();
    sqlx::query("INSERT INTO main.app VALUES (42)")
        .execute(&pool)
        .await
        .unwrap();

    provider_script(
        &provider,
        "CREATE TEMP TABLE app(value TEXT);
         INSERT INTO temp.app VALUES ('preserve');
         CREATE TEMP TABLE temp_effect(value TEXT);
         CREATE TEMP TRIGGER app AFTER INSERT ON app
         BEGIN INSERT INTO temp_effect VALUES (NEW.value); END;",
    )
    .await;

    let manifest = FlattenManifest::new(
        vec![m(1, "app", "app")],
        m(10, "base", "base").into_baseline(),
    )
    .unwrap();
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &migrations, "fp", &["app"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert!(matches!(
        backend
            .down_with_pool(&provider, "module", &migrations, "fp", 1, &["app"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "module", &manifest, "fp".into(), &["app"])
            .await,
        Err(Error::UntrackedObjects)
    ));

    assert_eq!(
        provider_i64(
            &provider,
            "SELECT count(*) AS value FROM temp.sqlite_master WHERE name='app'",
        )
        .await,
        2,
        "TEMP table and same-named trigger must both remain"
    );
    assert_eq!(
        provider_text(&provider, "SELECT value AS value FROM temp.app").await,
        "preserve"
    );
    provider_script(
        &provider,
        "INSERT INTO temp.app VALUES ('trigger still works')",
    )
    .await;
    assert_eq!(
        provider_text(&provider, "SELECT value AS value FROM temp_effect").await,
        "trigger still works"
    );
    let main_version: i64 =
        sqlx::query_scalar("SELECT version FROM main.migration_log WHERE module='module'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let main_app_value: i64 = sqlx::query_scalar("SELECT id FROM main.app")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((main_version, main_app_value), (1, 42));
}

#[tokio::test]
async fn same_named_main_table_and_trigger_are_rejected_as_untracked() {
    let (_dir, pool, provider) = make_pool(1).await;
    sqlx::raw_sql(
        "CREATE TABLE app(value TEXT);
         CREATE TRIGGER app AFTER INSERT ON app BEGIN SELECT 1; END;",
    )
    .execute(&pool)
    .await
    .unwrap();

    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let migrations = definitions(vec![m(1, "app", "app")]);
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &migrations, "fp", &["app"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert_eq!(
        provider_i64(&provider, "SELECT 1 AS value").await,
        1,
        "main collision detection must leave the provider connection usable"
    );
    let objects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM main.sqlite_master WHERE name='app' AND type IN ('table','trigger')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(objects, 2);
}

#[tokio::test]
async fn registered_modules_reject_temp_managed_objects() {
    let (_dir, pool, provider) = make_pool(1).await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let old = m(1, "old", "old");
    let ms = definitions(vec![old.clone()]);
    let manifest = FlattenManifest::new(vec![old], m(10, "base", "base").into_baseline()).unwrap();
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["old"])
        .await
        .unwrap();
    provider_script(
        &provider,
        "CREATE TEMP TABLE OLD(value TEXT); INSERT INTO temp.old VALUES ('keep')",
    )
    .await;
    assert!(matches!(
        backend
            .up_with_pool(&provider, "module", &ms, "fp", &["old"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    assert!(matches!(
        backend
            .flatten_with_pool(&provider, "module", &manifest, "fp".into(), &["OlD"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    let value = provider_text(&provider, "SELECT value AS value FROM temp.old").await;
    assert_eq!(value, "keep");
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM main.migration_log WHERE module='module'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn fresh_main_ledger_is_created_despite_temp_shadow() {
    let (_dir, pool, provider) = make_pool(1).await;
    provider_script(&provider, "CREATE TEMP TABLE Migration_Log(value TEXT); INSERT INTO temp.Migration_Log VALUES ('keep'); CREATE TEMP TABLE Migration_Log_Modules(value TEXT); INSERT INTO temp.Migration_Log_Modules VALUES ('keep_meta')").await;
    let backend = SqliteBackend::new("migration_log", 100).unwrap();
    let ms = definitions(vec![m(1, "one", "one")]);
    backend
        .up_with_pool(&provider, "module", &ms, "fp", &["one"])
        .await
        .unwrap();
    let main_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM main.migration_log WHERE module='module'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(main_count, 1);
    let temp_value =
        provider_text(&provider, "SELECT value AS value FROM temp.migration_log").await;
    let temp_meta = provider_text(
        &provider,
        "SELECT value AS value FROM temp.migration_log_modules",
    )
    .await;
    assert_eq!(
        (temp_value.as_str(), temp_meta.as_str()),
        ("keep", "keep_meta")
    );
}

#[tokio::test]
async fn down_rejects_temp_shadow_case_variants_and_preserves_all_data() {
    for spelling in ["app", "App", "APP"] {
        let (_dir, pool, provider) = make_pool(1).await;
        let backend = SqliteBackend::new("migration_log", 100).unwrap();
        let ms = definitions(vec![m(1, "app", "app")]);
        backend
            .up_with_pool(&provider, "module", &ms, "fp", &["app"])
            .await
            .unwrap();
        sqlx::query("INSERT INTO main.app VALUES (42)")
            .execute(&pool)
            .await
            .unwrap();
        let create = match spelling {
            "app" => "CREATE TEMP TABLE app(value TEXT)",
            "App" => "CREATE TEMP TABLE App(value TEXT)",
            _ => "CREATE TEMP TABLE APP(value TEXT)",
        };
        provider_script(
            &provider,
            &format!("{create}; INSERT INTO temp.app VALUES ('preserve')"),
        )
        .await;
        assert!(
            matches!(
                backend
                    .down_with_pool(&provider, "module", &ms, "fp", 1, &["app"])
                    .await,
                Err(Error::UntrackedObjects)
            ),
            "{spelling}"
        );
        let value = provider_text(&provider, "SELECT value AS value FROM temp.app").await;
        assert_eq!(value, "preserve");
        let main_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM main.sqlite_master WHERE type='table' AND name='app'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(main_count, 1);
        let main_value: i64 = sqlx::query_scalar("SELECT id FROM main.app")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(main_value, 42);
        let ledger_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM main.migration_log WHERE module='module' AND version=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(ledger_count, 1);
        let metadata: (i64, Option<i64>) = sqlx::query_as(
            "SELECT applied_count,head_version FROM main.migration_log_modules WHERE module='module'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(metadata, (1, Some(1)));
    }
}
