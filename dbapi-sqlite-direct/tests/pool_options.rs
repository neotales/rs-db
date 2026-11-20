//! Pool bounds, reuse, health, cleanup, and unsupported-option behavior.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi::{
    DbType, Delay, ErrorKind, Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, QuerySetting,
    RetryPolicy, RetrySetting, RowLimit, TxOptions, Value,
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn provider(path: &std::path::Path, max_connections: u32) -> SqlitePoolProvider {
    SqlitePoolProvider::connect_sync(
        SqliteConfig::new(path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(max_connections),
    )
    .expect("provider")
}

#[tokio::test]
async fn inherited_timeout_is_disabled_but_explicit_timeout_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let pool = SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("test.sqlite")))
        .await
        .unwrap();

    pool.execute_async("CREATE TABLE sample (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    let parameter = Parameter::new(DbType::I64, Value::I64(42)).unwrap();
    pool.execute_async(
        "INSERT INTO sample VALUES (?)",
        QueryOptions::new().bind(parameter),
    )
    .await
    .unwrap();
    let rows = pool
        .fetch_all_async(
            "SELECT value FROM sample",
            QueryOptions::new().limit(limit(1, 1024)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value_at(0).unwrap(), Value::I64(42));

    let error = pool
        .execute_async(
            "INSERT INTO sample VALUES (43)",
            QueryOptions::new().timeout(Duration::from_millis(10)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let rows = pool
        .fetch_all_async("SELECT value FROM sample", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "unsupported timeout must fail before dispatch"
    );
}

#[tokio::test]
async fn policy_pool_respects_sqlite_timeout_capability() {
    let directory = tempfile::tempdir().unwrap();
    let provider =
        SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("test.sqlite")))
            .await
            .unwrap();
    let pool = PolicyPool::new(provider, PoolPolicy::default());
    pool.execute_async("CREATE TABLE sample (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    let error = pool
        .execute_async(
            "INSERT INTO sample VALUES (1)",
            QueryOptions::new().timeout(Duration::from_millis(10)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let rows = pool
        .fetch_all_async("SELECT value FROM sample", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn session_owns_parameters_and_rejects_unhandled_policy_options() {
    let directory = tempfile::tempdir().unwrap();
    let pool = SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("test.sqlite")))
        .await
        .unwrap();
    let mut session = pool.acquire_async().await.unwrap();
    let parameter = Parameter::new(DbType::I64, Value::I64(42)).unwrap();
    let rows = session
        .fetch_all_async("SELECT ?", QueryOptions::new().bind(parameter))
        .await
        .unwrap();
    assert_eq!(rows[0].value_at(0).unwrap(), Value::I64(42));
    let error = session
        .execute_async("SELECT 1", QueryOptions::new().telemetry(QuerySetting::On))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    // A pre-dispatch rejection leaves the session usable.
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let rows = transaction
        .fetch_all_async(
            "SELECT ?",
            QueryOptions::new().bind(Parameter::new(DbType::I64, Value::I64(7)).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].value_at(0).unwrap(), Value::I64(7));
    let retry = RetryPolicy::new(2, Delay::Fixed(Duration::ZERO), false).unwrap();
    let error = transaction
        .execute_async(
            "SELECT 1",
            QueryOptions::new().retry(RetrySetting::Use(retry)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    transaction.rollback_async().await.unwrap();
}

#[tokio::test]
async fn scripts_reject_inapplicable_or_unavailable_options_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let pool = SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("test.sqlite")))
        .await
        .unwrap();
    let mut session = pool.acquire_async().await.unwrap();
    let script = "CREATE TABLE must_not_exist (value INTEGER)";
    let bound = QueryOptions::new().bind(Parameter::new(DbType::I64, Value::I64(1)).unwrap());
    assert_eq!(
        session
            .execute_trusted_script_async(script, bound)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        session
            .execute_trusted_script_async(
                script,
                QueryOptions::new().limit(RowLimit::new(1, 64).unwrap())
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        session
            .execute_trusted_script_async(script, QueryOptions::new().telemetry(QuerySetting::On))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    session
        .execute_trusted_script_async(script, QueryOptions::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn explicit_fetch_limit_on_execute_fails_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let pool =
        SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("limit.sqlite")))
            .await
            .unwrap();
    let result = pool
        .execute_async(
            "CREATE TABLE must_not_exist (value INTEGER)",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidArgument);
    let rows = pool
        .fetch_all_async(
            "SELECT name FROM sqlite_master WHERE name = 'must_not_exist'",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn policy_session_and_transaction_inherit_and_override_limits_and_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let provider =
        SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("policy.sqlite")))
            .await
            .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let pool = PolicyPool::new(
        provider,
        PoolPolicy {
            default_limit: RowLimit::new(1, 1024).unwrap(),
            telemetry_enabled: true,
            observer: Some(Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let sql = "SELECT 1 UNION ALL SELECT 2";
    let mut session = pool.acquire_async().await.unwrap();
    assert_eq!(
        session
            .fetch_all_async(sql, QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Overflow
    );
    // Overflow discards SQLite's connection, so acquire a new one before the override.
    drop(session);
    let mut session = pool.acquire_async().await.unwrap();
    let options = QueryOptions::new()
        .limit(RowLimit::new(2, 1024).unwrap())
        .telemetry(QuerySetting::Off);
    assert_eq!(
        session.fetch_all_async(sql, options).await.unwrap().len(),
        2
    );
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        transaction
            .fetch_all_async(
                sql,
                QueryOptions::new().limit(RowLimit::new(2, 1024).unwrap())
            )
            .await
            .unwrap()
            .len(),
        2
    );
    transaction.rollback_async().await.unwrap();
    assert_eq!(
        events.lock().unwrap().len(),
        2,
        "the explicit telemetry-off call emitted no event"
    );
}

#[tokio::test]
async fn pool_saturation_blocks_a_second_acquisition_until_release() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("saturated.sqlite"), 1);
    assert_eq!(provider.max_connections(), 1);
    assert_eq!(provider.unused_connection_slots(), 0);

    let mut first = provider.acquire_async().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), provider.acquire_async())
            .await
            .is_err(),
        "the second acquisition must wait for the only slot"
    );
    first
        .execute_async(
            "CREATE TABLE saturation (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(first);

    let mut second = tokio::time::timeout(Duration::from_secs(2), provider.acquire_async())
        .await
        .expect("released capacity")
        .expect("second acquisition");
    let count = second
        .fetch_scalar_async(
            "SELECT count(*) FROM saturation",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(count, Value::I64(0));
    drop(second);

    provider.close();
    provider.wait_for_cleanup().await.unwrap();
    assert_eq!(provider.unused_connection_slots(), 1);
}

#[tokio::test]
async fn idle_ttl_replaces_an_idle_physical_connection() {
    let directory = tempfile::tempdir().unwrap();
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(directory.path().join("ttl.sqlite"))
            .max_connections(1)
            .idle_ttl(Duration::from_millis(20)),
    )
    .await
    .unwrap();

    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TEMP TABLE marker (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(session);

    tokio::time::sleep(Duration::from_millis(60)).await;
    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT name FROM sqlite_temp_master WHERE name = 'marker'",
            QueryOptions::new().limit(limit(10, 256)),
        )
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "an expired worker must be replaced by a fresh connection"
    );
    drop(session);
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn in_memory_database_is_not_expired_by_idle_ttl() {
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(":memory:")
            .max_connections(1)
            .idle_ttl(Duration::from_millis(1)),
    )
    .await
    .unwrap();
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE memory_data (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async("INSERT INTO memory_data VALUES (7)", QueryOptions::new())
        .await
        .unwrap();
    drop(session);

    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut session = provider.acquire_async().await.unwrap();
    let count = session
        .fetch_scalar_async(
            "SELECT count(*) FROM memory_data",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(
        count,
        Value::I64(1),
        "the only in-memory connection must not be recycled by the idle window"
    );
}

#[tokio::test]
async fn all_file_uris_are_rejected_before_open() {
    for name in [
        "",
        "file://",
        "file://localhost",
        "file://localhost?cache=private",
        "file:///tmp/example.sqlite",
        "file://remote/example.sqlite",
        "file:ordinary.sqlite",
        "file:ordinary.sqlite?mode=rwc",
        "file::memory:",
        "file::memory:?cache=shared",
        "file:named?mode=memory",
        "file:named?vfs=memdb",
        "file:%3Amemory%3A",
        "file:review-only?mode=mem%6fry",
        "file:review-only?%6dode=memory",
        "file::memory:#ignored",
        "file:review?mode=memory#ignored",
        "file:",
        "file:?cache=private",
        "file:ordinary%20file.sqlite",
    ] {
        for (max_connections, idle_ttl) in
            [(1, Duration::from_secs(1)), (2, Duration::from_millis(1))]
        {
            let config = SqliteConfig::new(name)
                .max_connections(max_connections)
                .idle_ttl(idle_ttl);
            let error = SqlitePoolProvider::connect(config)
                .await
                .expect_err("file URI forms are rejected before opening");
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{name}");

            let config = SqliteConfig::new(name)
                .max_connections(max_connections)
                .idle_ttl(idle_ttl);
            let error = SqlitePoolProvider::connect_sync(config)
                .expect_err("sync connect must reject file URI forms");
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{name}");
        }
    }
}

#[tokio::test]
async fn plain_paths_with_uri_characters_remain_file_backed() {
    let directory = tempfile::tempdir().unwrap();
    for name in [
        "hash#percent%.sqlite",
        #[cfg(unix)]
        "file:literal.sqlite",
        #[cfg(unix)]
        "question?mark#percent%.sqlite",
    ] {
        let path = directory.path().join(name);
        let provider = SqlitePoolProvider::connect(SqliteConfig::new(&path))
            .await
            .unwrap();
        provider.close();
        provider.wait_for_cleanup().await.unwrap();
        assert!(path.is_file(), "{name}");
        let provider = SqlitePoolProvider::connect_sync(SqliteConfig::new(&path)).unwrap();
        provider.close();
        provider.wait_for_cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn a_failed_worker_is_not_returned_to_the_idle_pool() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("dirty.sqlite"), 1);
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TEMP TABLE marker (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async(
            "CREATE TABLE durable_marker (value INTEGER UNIQUE)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async("INSERT INTO durable_marker VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    // A known constraint error still discards the worker rather than reusing it.
    let error = session
        .execute_async("INSERT INTO durable_marker VALUES (1)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT name FROM sqlite_temp_master WHERE name = 'marker'",
            QueryOptions::new().limit(limit(10, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "the failed worker must not be reused");
    drop(session);
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn close_rejects_acquisition_and_waits_for_active_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("cleanup.sqlite"), 2);
    let session = provider.acquire_async().await.unwrap();
    // `connect` already opened one idle worker, so a two-connection pool has one free slot.
    assert_eq!(provider.unused_connection_slots(), 1);

    provider.close();
    let Err(error) = provider.acquire_async().await else {
        panic!("a closed pool must not acquire");
    };
    assert_eq!(error.kind(), ErrorKind::Connection);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), provider.wait_for_cleanup())
            .await
            .is_err(),
        "cleanup must wait for the active session"
    );
    drop(session);
    tokio::time::timeout(Duration::from_secs(2), provider.wait_for_cleanup())
        .await
        .expect("cleanup completes")
        .expect("cleanup succeeds");
    assert_eq!(provider.unused_connection_slots(), 2);
}

#[test]
fn synchronous_acquire_and_release_stress_does_not_deadlock_or_leak_slots() {
    const THREADS: u32 = 8;
    const ITERATIONS: u32 = 25;
    let directory = tempfile::tempdir().unwrap();
    let provider = SqlitePoolProvider::connect_sync(
        SqliteConfig::new(directory.path().join("stress.sqlite"))
            .max_connections(4)
            .busy_timeout(Duration::from_secs(10)),
    )
    .unwrap();
    // This stresses pool admission and reuse, not SQLite's non-FIFO writer lock. Repeated
    // autocommit inserts can starve a competing writer past even a correctly restored busy
    // timeout. Concurrent writes and finite lock budgets have separate integration coverage.
    std::thread::scope(|scope| {
        for thread in 0..THREADS {
            let provider = &provider;
            scope.spawn(move || {
                for iteration in 0..ITERATIONS {
                    let mut session = provider.acquire_sync_sqlite().unwrap();
                    let value = neotales_dbapi::sync::Session::fetch_scalar(
                        &mut session,
                        "SELECT ? + ?",
                        QueryOptions::new()
                            .bind(
                                Parameter::new(DbType::I64, Value::I64(i64::from(thread))).unwrap(),
                            )
                            .bind(
                                Parameter::new(DbType::I64, Value::I64(i64::from(iteration)))
                                    .unwrap(),
                            )
                            .limit(limit(1, 64)),
                    )
                    .unwrap();
                    assert_eq!(value, Value::I64(i64::from(thread + iteration)));
                }
            });
        }
    });

    provider.close();
}

#[test]
fn connect_rejects_invalid_pool_configuration() {
    let config = SqliteConfig::new("test.sqlite").max_connections(0);
    assert_eq!(
        SqlitePoolProvider::connect_sync(config)
            .expect_err("zero connections")
            .kind(),
        ErrorKind::InvalidArgument
    );
    let config = SqliteConfig::new("test.sqlite").busy_timeout(Duration::ZERO);
    assert_eq!(
        SqlitePoolProvider::connect_sync(config)
            .expect_err("zero busy timeout")
            .kind(),
        ErrorKind::InvalidArgument
    );
}
