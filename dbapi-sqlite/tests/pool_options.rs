//! Pool options must not promise a statement timeout SQLite cannot enforce.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi::{
    DbType, Delay, ErrorKind, Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, RetryPolicy,
    RetrySetting, RowLimit, TxOptions, Value,
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

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
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
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
        .execute_async(
            "SELECT 1",
            QueryOptions::new().telemetry(neotales_dbapi::QuerySetting::On),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
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
            .execute_trusted_script_async(
                script,
                QueryOptions::new().telemetry(neotales_dbapi::QuerySetting::On)
            )
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
    // Overflow invalidates SQLite's session, so acquire a new one before the override.
    drop(session);
    let mut session = pool.acquire_async().await.unwrap();
    let opts = QueryOptions::new()
        .limit(RowLimit::new(2, 1024).unwrap())
        .telemetry(neotales_dbapi::QuerySetting::Off);
    assert_eq!(session.fetch_all_async(sql, opts).await.unwrap().len(), 2);
    let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        tx.fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(2, 1024).unwrap())
        )
        .await
        .unwrap()
        .len(),
        2
    );
    tx.rollback_async().await.unwrap();
    assert_eq!(
        events.lock().unwrap().len(),
        2,
        "the explicit telemetry-off call emitted no event"
    );
}

#[tokio::test]
async fn policy_session_and_transaction_inherit_timeout_and_honor_explicit_disable() {
    let directory = tempfile::tempdir().unwrap();
    let provider =
        SqlitePoolProvider::connect(SqliteConfig::new(directory.path().join("timeout.sqlite")))
            .await
            .unwrap();
    let pool = PolicyPool::new(
        provider,
        PoolPolicy {
            default_timeout: Some(Duration::from_millis(50)),
            ..PoolPolicy::default()
        },
    );
    let mut session = pool.acquire_async().await.unwrap();
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    session
        .execute_async("SELECT 1", QueryOptions::new().no_timeout())
        .await
        .unwrap();
    let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        tx.execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    tx.execute_async("SELECT 1", QueryOptions::new().no_timeout())
        .await
        .unwrap();
    tx.rollback_async().await.unwrap();
}
