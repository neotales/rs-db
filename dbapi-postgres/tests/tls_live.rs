//! Provider-level verified TLS regression.
//!
//! Requires the disposable TLS-enabled PostgreSQL and DER roots created by
//! `data/dbapi-postgres/tls/tests/live.sh`. The test is ignored so a plaintext
//! live server on 55433 is not mistaken for this TLS fixture; the harness runs
//! it with `--ignored`.

use neotales_dbapi::{
    DbType, ErrorKind as DbErrorKind, Parameter, Pool as DbPool, QueryOptions, RowLimit, TxOptions,
    Value,
};
use neotales_dbapi_postgres::{PoolOptions, PostgresPool, experimental as experimental_pool};
use neotales_dbapi_postgres_tls::{VerifiedTls, connect};
use neotales_tokio_postgres::{Config, config::SslMode};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncReadExt as _;
use tracing::instrument::WithSubscriber as _;

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

fn config(host: &str, port: u16) -> Config {
    let mut config = Config::new();
    config.host(host).hostaddr(std::net::Ipv4Addr::LOCALHOST.into())
        .port(port).user("postgres").dbname("postgres")
        // Even an explicitly disabled mode must be overridden by the wrapper.
        .ssl_mode(SslMode::Disable);
    config
}

#[tokio::test]
#[ignore = "requires the TLS fixture started by data/dbapi-postgres/tls/tests/live.sh"]
#[allow(clippy::too_many_lines)] // One capture covers pool, transaction and cleanup paths.
async fn experimental_and_public_pool_over_tls() {
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    // Include spawned production workers, not only this task-local context.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    async {
        let root = std::fs::read(
            std::env::var("DBAPI_TLS_TEST_CA_DER").expect("set DBAPI_TLS_TEST_CA_DER"),
        )
        .unwrap();
        let other = std::fs::read(
            std::env::var("DBAPI_TLS_TEST_OTHER_CA_DER").expect("set DBAPI_TLS_TEST_OTHER_CA_DER"),
        )
        .unwrap();
        let tls = VerifiedTls::new(&[&root]).unwrap();
        let good = config("localhost", 55433);

        // Exercise the same experimental pool with its verified TLS transport.
        let bad_pool = experimental_pool::Pool::new(
            good.clone(),
            VerifiedTls::new(&[&other]).unwrap(),
            1,
            experimental_pool::Profile::normal(Duration::from_secs(5), Duration::from_secs(1))
                .unwrap(),
        ).unwrap();
        assert!(matches!(
            bad_pool.acquire().await,
            Err(experimental_pool::Error::Connection)
        ));
        bad_pool.close();
        assert_eq!(bad_pool.free_slots(), 1);
        let pool = experimental_pool::Pool::new(
            good.clone(),
            tls.clone(),
            1,
            experimental_pool::Profile::normal(Duration::from_secs(5), Duration::from_secs(1))
                .unwrap(),
        ).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        let row = lease
            .query(
                "SELECT pg_backend_pid(), ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
                &[],
            )
            .await
            .unwrap();
        let pool_pid = row.int(0).unwrap();
        assert!(row.boolean(1).unwrap());
        let rows = lease
            .fetch(
                "SELECT $1 AS value /* tls_pool_sql_canary */",
                &[Parameter::new(DbType::Text, Value::Text("tls_pool_value_canary".into())).unwrap()],
                RowLimit::new(1, 100).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rows[0].get::<String>("value").unwrap(), "tls_pool_value_canary");
        assert_eq!(lease.execute("SELECT $1", &[
            Parameter::new(DbType::I64, Value::I64(7)).unwrap(),
        ]).await.unwrap(), 1);
        let mut tx = lease.begin(TxOptions::default()).await.unwrap();
        let tx_rows = tx.fetch(
            "SELECT pg_backend_pid()::bigint AS pid, ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
            &[], RowLimit::new(1, 100).unwrap(),
        ).await.unwrap();
        assert_eq!(tx_rows[0].get::<i64>("pid").unwrap(), i64::from(pool_pid));
        assert!(tx_rows[0].get::<bool>("ssl").unwrap());
        let tx_rows = tx.fetch("SELECT $1 AS v /* tls_tx_sql_canary */", &[
            Parameter::new(DbType::Text, Value::Text("tls_tx_bind_canary".into())).unwrap(),
        ], RowLimit::new(1, 100).unwrap()).await.unwrap();
        assert_eq!(tx_rows[0].get::<String>("v").unwrap(), "tls_tx_bind_canary");
        tx.execute_trusted_script("DO $$ BEGIN RAISE WARNING 'tls_tx_notice_canary'; END $$")
            .await.unwrap();
        lease = tx.commit().await.unwrap();
        let mut tx = lease.begin(TxOptions::default()).await.unwrap();
        let error = tx.execute_trusted_script(
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='tls_tx_error_canary'; END $$",
        ).await.unwrap_err();
        assert_eq!(error.kind(), DbErrorKind::Constraint);
        assert!(std::error::Error::source(&error).is_none());
        lease = tx.rollback().await.unwrap();
        lease.recycle().await.unwrap();
        let mut lease = pool.acquire().await.unwrap();
        assert_eq!(
            lease
                .query("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .int(0).unwrap(),
            pool_pid
        );
        let (observer, observer_connection, _) = connect(&good, tls.clone()).await.unwrap();
        let observer_worker = tokio::spawn(observer_connection.with_current_subscriber());
        assert_eq!(
            lease.batch_deadline(
                "SELECT pg_sleep(3)",
                Duration::from_secs(3),
                Duration::from_millis(100),
            ).await,
            Err(experimental_pool::Error::OutcomeUnknown),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let row = observer.query_one(
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')",
                    &[&pool_pid],
                ).await.unwrap();
                if row.get::<_, bool>(0) { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("TLS cancel did not stop server work before statement ceiling");
        assert_eq!(lease.recycle().await, Err(experimental_pool::Error::Poisoned));
        let mut lease = pool.acquire().await.unwrap();
        assert_ne!(lease.query("SELECT pg_backend_pid()", &[]).await.unwrap().int(0).unwrap(), pool_pid);
        lease.batch("SELECT pg_sleep(0.2)").await.unwrap();
        lease.recycle().await.unwrap();
        drop(observer);
        observer_worker.await.unwrap().unwrap();
        pool.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.free_slots() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("TLS pool worker did not terminate");

        // Abort a pool acquisition while the TLS negotiation is stalled.
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            ready_tx.send(()).unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        let stalled = experimental_pool::Pool::new(
            config("localhost", port),
            tls.clone(),
            1,
            experimental_pool::Profile::normal(Duration::from_secs(5), Duration::from_secs(1))
                .unwrap(),
        ).unwrap();
        let pending_pool = stalled.clone();
        let acquire = tokio::spawn(async move { pending_pool.acquire().await });
        tokio::time::timeout(Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();
        acquire.abort();
        assert!(matches!(acquire.await, Err(err) if err.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        stalled.close();
        assert_eq!(stalled.free_slots(), 1);

        // Exercise the canonical public trait adapter, not just the transport.
        let policy = PoolOptions::new(2,
            experimental_pool::Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
            Duration::from_secs(60),
        ).unwrap();
        for (cfg, trust) in [
            (good.clone(), VerifiedTls::new(&[&other]).unwrap()),
            (config("wrong.invalid", 55433), tls.clone()),
        ] {
            let rejected = PostgresPool::new(cfg, trust, policy).unwrap();
            assert_eq!(rejected.acquire_async().await.err().unwrap().kind(), DbErrorKind::Connection);
            rejected.close();
        }
        let mut binding_required = config("localhost", 55433);
        binding_required.user(
            std::env::var("DBAPI_TLS_TEST_SCRAM_USER").expect("set DBAPI_TLS_TEST_SCRAM_USER"),
        );
        binding_required.password(
            std::env::var("DBAPI_TLS_TEST_SCRAM_PASSWORD")
                .expect("set DBAPI_TLS_TEST_SCRAM_PASSWORD"),
        );
        binding_required.channel_binding(
            neotales_tokio_postgres::config::ChannelBinding::Require,
        );
        // The verified transport accepts `require`; only the plaintext test
        // transport rejects it. Acquisition proves SCRAM-SHA-256-PLUS through
        // the canonical public trait adapter.
        let binding_pool = PostgresPool::new(binding_required, tls.clone(), policy).unwrap();
        let mut binding_session = binding_pool.acquire_async().await.unwrap();
        let binding_rows = binding_session.fetch_all_async(
                "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
                QueryOptions::new().limit(RowLimit::new(1, 100).unwrap()),
            )
            .await
            .unwrap();
        assert!(binding_rows[0].get::<bool>("ssl").unwrap());
        drop(binding_session);
        binding_pool.wait_for_cleanup().await.unwrap();
        binding_pool.close();

        let public = PostgresPool::new(good.clone(), tls.clone(), policy).unwrap();
        let erased: &dyn DbPool = &public;
        let mut session = erased.acquire_async().await.unwrap();
        let rows = session.fetch_all_async(
            "SELECT pg_backend_pid()::bigint AS pid,ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
            QueryOptions::new().limit(RowLimit::new(1, 100).unwrap()),
        ).await.unwrap();
        let public_pid = rows[0].get::<i64>("pid").unwrap();
        assert!(rows[0].get::<bool>("ssl").unwrap());
        let mut tx = session.begin_async(TxOptions { isolation: None, read_only: Some(true) }).await.unwrap();
        let rows = tx.fetch_all_async("SELECT $1 AS v /* public_tls_sql_canary */", QueryOptions::new().bind_all(&[
            Parameter::new(DbType::Text, Value::Text("public_tls_bind_canary".into())).unwrap(),
        ]).limit(RowLimit::new(1, 100).unwrap())).await.unwrap();
        assert_eq!(rows[0].get::<String>("v").unwrap(), "public_tls_bind_canary");
        tx.execute_trusted_script_async(
            "DO $$ BEGIN RAISE WARNING 'public_tls_notice_canary'; END $$", QueryOptions::new(),
        ).await.unwrap();
        tx.commit_async().await.unwrap();
        drop(session);
        public.wait_for_cleanup().await.unwrap();
        let mut reused = erased.acquire_async().await.unwrap();
        assert_eq!(reused.fetch_all_async("SELECT pg_backend_pid()::bigint AS pid",
            QueryOptions::new().limit(RowLimit::new(1, 100).unwrap())).await.unwrap()[0].get::<i64>("pid").unwrap(), public_pid);
        drop(reused);
        public.wait_for_cleanup().await.unwrap();
        public.close();
        tracing::info!("tls_pool_caller_tracing_sentinel");
        log::info!("tls_pool_caller_log_sentinel");
    }
    .await;

    let tracing = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let logs = LOGS.0.lock().unwrap().join("\n");
    assert!(tracing.contains("tls_pool_caller_tracing_sentinel"));
    assert!(logs.contains("tls_pool_caller_log_sentinel"));
    assert!(!tracing.contains("experimental pool driver started"));
    assert!(!logs.contains("experimental pool driver started"));
    for canary in [
        "tls_pool_sql_canary",
        "tls_pool_value_canary",
        "tls_tx_sql_canary",
        "tls_tx_bind_canary",
        "tls_tx_notice_canary",
        "tls_tx_error_canary",
        "public_tls_sql_canary",
        "public_tls_bind_canary",
        "public_tls_notice_canary",
    ] {
        assert!(!tracing.contains(canary), "tracing disclosed {canary}");
        assert!(!logs.contains(canary), "log disclosed {canary}");
    }
}
