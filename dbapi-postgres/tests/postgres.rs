//! Public dyn Pool/Session/Transaction contract on disposable PostgreSQL.
use neotales_dbapi::{
    DbType, ErrorKind, IsolationLevel, Parameter, Pool, QueryOptions, RowLimit, Session, TxOptions,
    Value,
};
use neotales_dbapi_postgres::{Config, PoolOptions, PostgresPool, Profile};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[path = "support/public_proxy.rs"]
mod proxy;

fn options() -> QueryOptions {
    QueryOptions::new()
}
fn limit() -> RowLimit {
    RowLimit::new(10, 1000).unwrap()
}
fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).unwrap()
}
fn policy(max: usize, ceiling: Duration, ttl: Duration) -> PoolOptions {
    PoolOptions::new(
        max,
        Profile::normal(ceiling, Duration::from_millis(100)).unwrap(),
        ttl,
    )
    .unwrap()
}
fn at(port: u16, max: usize, ceiling: Duration, ttl: Duration) -> PostgresPool {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    PostgresPool::loopback_plaintext_for_tests(config, policy(max, ceiling, ttl)).unwrap()
}
fn pool(max: usize) -> PostgresPool {
    at(55433, max, Duration::from_secs(5), Duration::from_secs(60))
}
async fn pid(session: &mut dyn Session) -> i64 {
    session
        .fetch_all_async(
            "SELECT pg_backend_pid()::bigint AS pid",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap()[0]
        .get::<i64>("pid")
        .unwrap()
}
async fn count(session: &mut dyn Session, table: &str) -> i64 {
    session
        .fetch_all_async(
            &format!("SELECT count(*) AS n FROM {table}"),
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap()[0]
        .get::<i64>("n")
        .unwrap()
}
async fn unused(pool: &PostgresPool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.unused_connection_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("discarded socket owner retained capacity");
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One dyn Session ties detached types to reset and boundary checks.
async fn public_types_nulls_bounds_and_native_positions() {
    let pool = pool(2);
    let erased: &dyn Pool = &pool;
    let mut session = erased.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async("-- empty script is a completed no-op", options())
        .await
        .unwrap();
    let values = [
        Parameter::new(DbType::Bool, Value::Bool(true)).unwrap(),
        integer(i64::MIN),
        Parameter::new(DbType::F64, Value::F64(1.25)).unwrap(),
        Parameter::new(DbType::Text, Value::Text("héllo ; $1 ?".into())).unwrap(),
        Parameter::new(DbType::Bytes, Value::Bytes(vec![0, 1, 255])).unwrap(),
    ];
    let rows = session
        .fetch_all_async(
            "SELECT $1 AS b,$2 AS i,$3 AS f,$4 AS t,$5 AS x",
            QueryOptions::new().bind_all(&values).limit(limit()),
        )
        .await
        .unwrap();
    assert!(rows[0].get::<bool>("b").unwrap());
    assert_eq!(rows[0].get::<i64>("i").unwrap(), i64::MIN);
    assert_eq!(
        rows[0].get::<f64>("f").unwrap().to_bits(),
        1.25_f64.to_bits()
    );
    assert_eq!(rows[0].get::<String>("t").unwrap(), "héllo ; $1 ?");
    assert_eq!(rows[0].get::<Vec<u8>>("x").unwrap(), vec![0, 1, 255]);
    let parameters: Vec<_> = [
        DbType::Bool,
        DbType::I64,
        DbType::F64,
        DbType::Text,
        DbType::Bytes,
    ]
    .into_iter()
    .map(|kind| Parameter::new(kind, Value::Null(kind)).unwrap())
    .collect();
    let nulls = session
        .fetch_all_async(
            "SELECT $1 AS b,$2 AS i,$3 AS f,$4 AS t,$5 AS x",
            QueryOptions::new()
                .bind_all(&parameters)
                .limit(RowLimit::new(1, 5).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(nulls[0].get::<Option<bool>>("b").unwrap(), None);
    assert_eq!(nulls[0].get::<Option<i64>>("i").unwrap(), None);
    assert_eq!(nulls[0].get::<Option<f64>>("f").unwrap(), None);
    assert_eq!(nulls[0].get::<Option<String>>("t").unwrap(), None);
    assert_eq!(nulls[0].get::<Option<Vec<u8>>>("x").unwrap(), None);
    let order = session
        .fetch_all_async(
            "SELECT $2 AS dup,$1 AS dup,$1 AS again",
            QueryOptions::new()
                .bind_all(&[integer(7), integer(9)])
                .limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(
        order[0].get::<i64>("dup").unwrap_err().kind(),
        ErrorKind::Ambiguous
    );
    assert_eq!(order[0].get_ordinal::<i64>(0).unwrap(), 9);
    assert_eq!(order[0].get::<i64>("again").unwrap(), 7);
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT '界'::text AS \"é\" FROM generate_series(1,2)",
                QueryOptions::new().limit(RowLimit::new(2, 10).unwrap())
            )
            .await
            .unwrap()
            .len(),
        2
    );
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
    assert_eq!(rows[0].get::<i64>("i").unwrap(), i64::MIN);
    for (sql, bound) in [
        (
            "SELECT '界'::text AS \"é\" FROM generate_series(1,2)",
            RowLimit::new(2, 9).unwrap(),
        ),
        (
            "SELECT 1::bigint AS n FROM generate_series(1,2)",
            RowLimit::new(1, 100).unwrap(),
        ),
        ("SELECT 'NaN'::float8", RowLimit::new(1, 100).unwrap()),
    ] {
        let mut session = erased.acquire_async().await.unwrap();
        assert_eq!(
            session
                .fetch_all_async(sql, QueryOptions::new().limit(bound))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }
    pool.close();
}

#[tokio::test]
async fn public_session_drop_resets_and_reuses_same_connection() {
    let pool = pool(1);
    let mut session = (&pool as &dyn Pool).acquire_async().await.unwrap();
    let original = pid(&mut *session).await;
    session.execute_trusted_script_async(
        "CREATE TEMP TABLE public_reset(v bigint); SET application_name='public_reset'; SELECT pg_advisory_lock(75823391)",
        options(),
    ).await.unwrap();
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
    let mut session = pool.acquire_async().await.unwrap();
    assert_eq!(pid(&mut *session).await, original);
    let clean = session.fetch_all_async(
        "SELECT to_regclass('pg_temp.public_reset') IS NULL AS temp, current_setting('application_name') <> 'public_reset' AS setting, NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory') AS locks",
        QueryOptions::new().limit(limit()),
    ).await.unwrap();
    for name in ["temp", "setting", "locks"] {
        assert!(clean[0].get::<bool>(name).unwrap());
    }
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One borrowed Session proves transaction affinity and restoration.
async fn public_transactions_preserve_affinity_options_arity_and_outcomes() {
    let pool = pool(2);
    assert!(
        pool.capabilities().transactions
            && pool.capabilities().isolation_level
            && pool.capabilities().read_only_transactions
            && pool.capabilities().trusted_scripts
    );
    let mut session = (&pool as &dyn Pool).acquire_async().await.unwrap();
    let mut observer = pool.acquire_async().await.unwrap();
    let original = pid(&mut *session).await;
    session
        .execute_async(
            "CREATE TEMP TABLE public_tx(v bigint UNIQUE)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    {
        let mut tx = session
            .begin_async(TxOptions {
                isolation: Some(IsolationLevel::Serializable),
                read_only: Some(false),
            })
            .await
            .unwrap();
        let rows = tx.fetch_all_async("SELECT pg_backend_pid()::bigint AS pid,current_setting('transaction_isolation') AS isolation",
            QueryOptions::new().limit(limit())).await.unwrap();
        assert_eq!(rows[0].get::<i64>("pid").unwrap(), original);
        assert_eq!(rows[0].get::<String>("isolation").unwrap(), "serializable");
        tx.execute_async(
            "INSERT INTO public_tx VALUES ($1)",
            QueryOptions::new().bind(integer(1)),
        )
        .await
        .unwrap();
        tx.execute_async(
            "SELECT pg_advisory_xact_lock($1)",
            QueryOptions::new().bind(integer(75_823_393)),
        )
        .await
        .unwrap();
        assert!(
            !observer
                .fetch_all_async(
                    "SELECT pg_try_advisory_xact_lock($1) AS free",
                    QueryOptions::new().bind(integer(75_823_393)).limit(limit())
                )
                .await
                .unwrap()[0]
                .get::<bool>("free")
                .unwrap()
        );
        assert_eq!(
            tx.execute_async(
                "INSERT INTO public_tx VALUES (99)",
                QueryOptions::new().timeout(Duration::ZERO)
            )
            .await
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            tx.execute_async(
                "INSERT INTO public_tx VALUES ($1)",
                QueryOptions::new().bind_all(&[integer(2), integer(3)])
            )
            .await
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            tx.fetch_all_async(
                "SELECT to_json($1)::text AS v",
                QueryOptions::new().bind(integer(7)).limit(limit())
            )
            .await
            .unwrap()[0]
                .get::<String>("v")
                .unwrap(),
            "7"
        );
        tx.commit_async().await.unwrap();
    }
    assert_eq!(pid(&mut *session).await, original);
    assert!(
        observer
            .fetch_all_async(
                "SELECT pg_try_advisory_xact_lock($1) AS free",
                QueryOptions::new().bind(integer(75_823_393)).limit(limit())
            )
            .await
            .unwrap()[0]
            .get::<bool>("free")
            .unwrap()
    );
    assert_eq!(count(&mut *session, "public_tx").await, 1);
    {
        let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
        tx.execute_async("INSERT INTO public_tx VALUES (2)", QueryOptions::new())
            .await
            .unwrap();
        tx.rollback_async().await.unwrap();
    }
    assert_eq!(count(&mut *session, "public_tx").await, 1);
    {
        let mut tx = session
            .begin_async(TxOptions {
                isolation: None,
                read_only: Some(true),
            })
            .await
            .unwrap();
        assert!(
            tx.fetch_all_async(
                "SELECT current_setting('transaction_read_only')='on' AS readonly",
                QueryOptions::new().limit(limit())
            )
            .await
            .unwrap()[0]
                .get::<bool>("readonly")
                .unwrap()
        );
        tx.rollback_async().await.unwrap();
    }
    assert_eq!(
        session
            .begin_async(TxOptions {
                isolation: Some(IsolationLevel::ReadUncommitted),
                read_only: None
            })
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(pid(&mut *session).await, original);
    drop(session);
    drop(observer);
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn public_aborted_and_deferred_commit_errors_are_not_swallowed() {
    let pool = pool(1);
    for deferred in [false, true] {
        let mut session = pool.acquire_async().await.unwrap();
        let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
        if deferred {
            tx.execute_trusted_script_async("CREATE TEMP TABLE public_deferred(v bigint UNIQUE DEFERRABLE INITIALLY DEFERRED); INSERT INTO public_deferred VALUES(1),(1)", options()).await.unwrap();
        } else {
            tx.execute_trusted_script_async("CREATE TEMP TABLE public_abort(v bigint UNIQUE); INSERT INTO public_abort VALUES(1)", options()).await.unwrap();
            assert_eq!(
                tx.execute_async("INSERT INTO public_abort VALUES(1)", QueryOptions::new())
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Constraint
            );
        }
        let error = tx.commit_async().await.unwrap_err();
        assert_eq!(
            error.kind(),
            if deferred {
                ErrorKind::Constraint
            } else {
                ErrorKind::Cancelled
            }
        );
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(
            session
                .fetch_all_async("SELECT 1::bigint", QueryOptions::new().limit(limit()))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Connection
        );
        drop(session);
        unused(&pool).await;
    }
    pool.close();
}

#[tokio::test]
async fn public_dropped_or_interrupted_transaction_invalidates_session_without_retry() {
    let pool = pool(1);
    for interrupt in [false, true] {
        let mut session = pool.acquire_async().await.unwrap();
        let original = pid(&mut *session).await;
        {
            let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
            tx.execute_async(
                "SELECT pg_advisory_xact_lock(75823392)",
                QueryOptions::new(),
            )
            .await
            .unwrap();
            if interrupt {
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(30),
                        tx.execute_async("SELECT pg_sleep(1)", QueryOptions::new())
                    )
                    .await
                    .is_err()
                );
                assert_eq!(
                    tx.rollback_async().await.unwrap_err().kind(),
                    ErrorKind::Connection
                );
            }
        }
        assert_eq!(
            session
                .execute_async("SELECT 1", QueryOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Connection
        );
        drop(session);
        unused(&pool).await;
        let mut replacement = pool.acquire_async().await.unwrap();
        assert_ne!(pid(&mut *replacement).await, original);
        drop(replacement);
        pool.wait_for_cleanup().await.unwrap();
    }
    pool.close();
}

#[tokio::test]
async fn public_timeout_policy_ttl_close_and_invalid_arguments() {
    let pool = at(
        55433,
        1,
        Duration::from_millis(300),
        Duration::from_millis(30),
    );
    let mut session = pool.acquire_async().await.unwrap();
    let original = pid(&mut *session).await;
    for timeout_ms in [0, 301, u64::MAX] {
        assert_eq!(
            session
                .execute_async(
                    "SELECT 1",
                    QueryOptions::new().timeout(Duration::from_millis(timeout_ms))
                )
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
    assert_eq!(pid(&mut *session).await, original);
    assert_eq!(
        pool.acquire_async().await.err().unwrap().kind(),
        ErrorKind::Timeout
    );
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
    tokio::time::sleep(Duration::from_millis(70)).await;
    let mut session = pool.acquire_async().await.unwrap();
    assert_ne!(pid(&mut *session).await, original);
    assert_eq!(
        session
            .execute_async(
                "SELECT pg_sleep(1)",
                QueryOptions::new().timeout(Duration::from_millis(30))
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    drop(session);
    unused(&pool).await;
    pool.close();
    assert_eq!(
        pool.acquire_async().await.err().unwrap().kind(),
        ErrorKind::Connection
    );
    assert!(
        PoolOptions::new(
            0,
            Profile::normal(Duration::from_secs(1), Duration::from_secs(1)).unwrap(),
            Duration::from_secs(1)
        )
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_five_concurrent_public_clients_share_three_clean_connections() {
    let pool = Arc::new(pool(3));
    let first_three = Arc::new(tokio::sync::Barrier::new(3));
    let mut jobs = Vec::new();
    for index in 0..25 {
        let pool = pool.clone();
        let first_three = first_three.clone();
        jobs.push(tokio::spawn(async move {
            let erased: &dyn Pool = &*pool;
            let mut session = erased.acquire_async().await.unwrap();
            let original = pid(&mut *session).await;
            if index < 3 {
                first_three.wait().await;
            }
            let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
            tx.execute_trusted_script_async(
                "CREATE TEMP TABLE public_parallel(v bigint)",
                options(),
            )
            .await
            .unwrap();
            tx.execute_async(
                "INSERT INTO public_parallel VALUES($1)",
                QueryOptions::new().bind(integer(index)),
            )
            .await
            .unwrap();
            assert_eq!(
                tx.fetch_all_async(
                    "SELECT v FROM public_parallel",
                    QueryOptions::new().limit(limit())
                )
                .await
                .unwrap()[0]
                    .get::<i64>("v")
                    .unwrap(),
                index
            );
            tx.commit_async().await.unwrap();
            assert_eq!(pid(&mut *session).await, original);
            drop(session);
            original
        }));
    }
    let mut ids = std::collections::HashSet::new();
    for job in jobs {
        ids.insert(
            tokio::time::timeout(Duration::from_secs(10), job)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(ids.len(), 3);
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct Logger(Mutex<Vec<String>>);
impl log::Log for Logger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }
    fn flush(&self) {}
}
static LOGS: Logger = Logger(Mutex::new(Vec::new()));

#[tokio::test]
async fn public_sql_bind_notice_error_and_cleanup_never_log_or_change_callers_logging() {
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let writer = capture.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();
    let pool = pool(1);
    let mut session = (&pool as &dyn Pool).acquire_async().await.unwrap();
    session
        .fetch_all_async(
            "SELECT $1 AS value /* public_sql_canary */",
            QueryOptions::new()
                .bind(
                    Parameter::new(DbType::Text, Value::Text("public_bind_canary".into())).unwrap(),
                )
                .limit(limit()),
        )
        .await
        .unwrap();
    let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    tx.execute_trusted_script_async(
        "DO $$ BEGIN RAISE WARNING 'public_notice_canary'; END $$",
        options(),
    )
    .await
    .unwrap();
    let error = tx
        .execute_trusted_script_async(
            "DO $$ BEGIN RAISE EXCEPTION 'public_error_canary'; END $$",
            options(),
        )
        .await
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("public_error_canary"));
    tx.rollback_async().await.unwrap();
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
    tracing::info!("public_caller_trace_sentinel");
    log::info!("public_caller_log_sentinel");
    let trace = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let logs = LOGS.0.lock().unwrap().join("\n");
    assert!(trace.contains("public_caller_trace_sentinel"));
    assert!(logs.contains("public_caller_log_sentinel"));
    for canary in [
        "public_sql_canary",
        "public_bind_canary",
        "public_notice_canary",
        "public_error_canary",
        "public_sqlstate_canary",
    ] {
        assert!(!trace.contains(canary));
        assert!(!logs.contains(canary));
    }
    pool.close();
}

#[tokio::test]
async fn public_trait_withheld_commit_is_unknown_not_swallowed() {
    proxy::commit().await;
}

#[tokio::test]
async fn public_cleanup_failure_is_reported_and_discards() {
    proxy::cleanup().await;
}

#[tokio::test]
async fn public_cancelled_begin_and_rollback_never_restore_a_session() {
    proxy::cancelled(proxy::Stage::Begin).await;
    proxy::cancelled(proxy::Stage::Rollback).await;
}

#[test]
fn public_drop_without_runtime_discards_and_reports_cleanup_failure() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = pool(1);
    let session = runtime.block_on(pool.acquire_async()).unwrap();
    drop(session); // Runtime exists, but this thread is not entered into it.
    runtime.block_on(async {
        assert_eq!(
            pool.wait_for_cleanup().await.unwrap_err().kind(),
            ErrorKind::Connection
        );
        unused(&pool).await;
        pool.close();
    });
}

#[tokio::test]
async fn public_close_wakes_background_cleanup_and_rejects_new_acquisitions() {
    proxy::close_cleanup().await;
}

#[tokio::test]
async fn public_requires_command_completion_even_when_ready_is_received() {
    proxy::missing_completion().await;
}

#[test]
fn public_policy_and_plaintext_configuration_are_validated_before_io() {
    let profile = Profile::normal(Duration::from_secs(1), Duration::from_millis(100)).unwrap();
    for ttl in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
        assert_eq!(
            PoolOptions::new(1, profile, ttl).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }
    for max in [0, usize::MAX] {
        assert_eq!(
            PoolOptions::new(max, profile, Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
    for host in ["localhost", "192.0.2.1", ""] {
        let mut config = Config::new();
        config.host(host);
        assert_eq!(
            PostgresPool::loopback_plaintext_for_tests(
                config,
                policy(1, Duration::from_secs(1), Duration::from_secs(1)),
            )
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidArgument
        );
    }
    // Lazy construction and close need no runtime or live server.
    pool(1).close();
}

#[tokio::test]
async fn public_reset_requires_command_completion_before_reuse() {
    proxy::missing_reset().await;
}

#[tokio::test]
async fn public_sqlstate_categories_and_unknown_write_are_preserved() {
    let pool = pool(1);
    for (code, expected) in [
        ("23505", ErrorKind::Constraint),
        ("22003", ErrorKind::Overflow),
        ("40001", ErrorKind::Busy),
        ("57014", ErrorKind::Cancelled),
        ("22012", ErrorKind::Decode),
        ("40003", ErrorKind::WriteOutcomeUnknown),
    ] {
        let mut session = (&pool as &dyn Pool).acquire_async().await.unwrap();
        let sql = format!(
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='{code}', MESSAGE='public_sqlstate_canary'; END $$"
        );
        let error = session
            .execute_trusted_script_async(&sql, options())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(std::error::Error::source(&error).is_none());
        assert!(!format!("{error:?} {error}").contains("public_sqlstate_canary"));
        drop(session);
    }
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn public_transaction_statement_fetch_and_script_honor_shorter_client_budgets() {
    let pool = pool(1);
    for operation in 0..3 {
        let mut session = pool.acquire_async().await.unwrap();
        let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
        let timeout = QueryOptions::new().timeout(Duration::from_millis(100));
        let error = match operation {
            0 => tx
                .execute_async(
                    "SELECT pg_sleep(1)",
                    QueryOptions::new().timeout(Duration::from_millis(100)),
                )
                .await
                .unwrap_err(),
            1 => tx
                .fetch_all_async(
                    "SELECT 1::bigint AS n FROM pg_sleep(1)",
                    QueryOptions::new()
                        .timeout(Duration::from_millis(100))
                        .limit(limit()),
                )
                .await
                .unwrap_err(),
            _ => tx
                .execute_trusted_script_async("SELECT pg_sleep(1)", timeout)
                .await
                .unwrap_err(),
        };
        assert!(matches!(
            error.kind(),
            ErrorKind::Timeout | ErrorKind::WriteOutcomeUnknown
        ));
        assert_eq!(
            tx.rollback_async().await.unwrap_err().kind(),
            ErrorKind::Connection
        );
        assert_eq!(
            session
                .execute_async("SELECT 1", QueryOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Connection
        );
        drop(session);
        unused(&pool).await;
    }
    pool.close();
}
