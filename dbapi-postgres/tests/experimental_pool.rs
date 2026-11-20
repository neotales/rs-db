//! Exercises the production experimental pool, not the `SQLx` dbapi provider.
use neotales_dbapi_postgres::experimental as pool;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_tokio_postgres::Config;
use pool::{Error, Profile};
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

fn make_pool(config: Config, max: usize, profile: Profile) -> pool::Pool {
    pool::Pool::loopback_plaintext_for_tests(config, max, profile).unwrap()
}

fn config() -> Config {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(55433)
        .user("postgres")
        .dbname("postgres");
    config
}

fn profile() -> Profile {
    Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap()
}

async fn pid(lease: &mut pool::Lease) -> i32 {
    lease
        .query("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .int(0)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // One capture spans concurrent tasks and their lifecycle.
async fn capacity_reset_cancellation_and_logging() {
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    // Production workers do not propagate task-local diagnostic context.
    // Capture globally in this one-test binary so their emissions cannot escape.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    async {
        let pool = make_pool(config(), 1, profile());
        let mut lease = pool.acquire().await.unwrap();
        let initial_timeout = lease.query(
            "SELECT setting::int4 FROM pg_catalog.pg_settings WHERE name='statement_timeout'",
            &[],
        ).await.unwrap();
        assert_eq!(initial_timeout.int(0).unwrap(), 5000);
        assert_eq!(initial_timeout.int(1), Err(Error::Operation));
        assert_eq!(initial_timeout.text(0), Err(Error::Operation));
        assert!(matches!(Profile::normal(Duration::ZERO, Duration::from_secs(1)), Err(Error::InvalidPolicy)));
        assert!(matches!(Profile::normal(Duration::from_nanos(1), Duration::from_secs(1)), Err(Error::InvalidPolicy)));
        assert!(matches!(Profile::migration(profile(), Duration::from_secs(5), Duration::from_secs(1)), Err(Error::InvalidPolicy)));
        assert_eq!(
            lease.batch_deadline("CREATE TEMP TABLE must_not_dispatch(v int)", Duration::from_secs(6), Duration::from_secs(1)).await,
            Err(Error::InvalidPolicy),
        );
        assert!(lease.query("SELECT to_regclass('pg_temp.must_not_dispatch') IS NULL", &[]).await.unwrap().boolean(0).unwrap());
        lease.batch_deadline(
            "SELECT pg_sleep(0.06); SELECT pg_sleep(0.06)",
            Duration::from_millis(100),
            Duration::from_secs(2),
        ).await.unwrap();
        assert_eq!(lease.query(
            "SELECT setting::int4 FROM pg_catalog.pg_settings WHERE name='statement_timeout'",
            &[],
        ).await.unwrap().int(0).unwrap(), 5000);
        let original = pid(&mut lease).await;
        assert!(tokio::time::timeout(Duration::from_millis(30), pool.acquire()).await.is_err());
        let pending_pool = pool.clone();
        let waiting = tokio::spawn(async move { pending_pool.acquire().await });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(matches!(waiting.await, Err(err) if err.is_cancelled()));
        lease.batch("CREATE TEMP TABLE pool_fixture(v bigint); SET application_name='pool_fixture'; SELECT pg_advisory_lock(75823371::bigint); BEGIN; INSERT INTO pool_fixture VALUES (1);").await.unwrap();
        let row = lease.query("SELECT $1::text /* pool_sql_canary */", &[&"pool_value_canary"]).await.unwrap();
        assert_eq!(row.text(0).unwrap(), "pool_value_canary");
        lease.batch("DO $$ BEGIN RAISE WARNING 'pool_notice_canary'; END $$").await.unwrap();
        lease.batch("CREATE TYPE pg_temp.pool_enum_a AS ENUM ('a'); CREATE TYPE pg_temp.pool_composite_a AS (v bigint)").await.unwrap();
        lease.prepare("SELECT NULL::pg_temp.pool_enum_a").await.unwrap();
        lease.prepare("SELECT NULL::pg_temp.pool_composite_a").await.unwrap();
        let waiter_pool = pool.clone();
        let waiter = tokio::spawn(async move { waiter_pool.acquire().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        lease.recycle().await.unwrap();
        let mut lease = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await.unwrap().unwrap().unwrap();
        assert_eq!(pid(&mut lease).await, original);
        assert_eq!(lease.query(
            "SELECT setting::int4 FROM pg_catalog.pg_settings WHERE name='statement_timeout'",
            &[],
        ).await.unwrap().int(0).unwrap(), 5000);
        lease.batch("CREATE TYPE pg_temp.pool_enum_b AS ENUM ('b'); CREATE TYPE pg_temp.pool_composite_b AS (v bigint)").await.unwrap();
        lease.prepare("SELECT NULL::pg_temp.pool_enum_b").await.unwrap();
        lease.prepare("SELECT NULL::pg_temp.pool_composite_b").await.unwrap();
        let clean = lease.query("SELECT to_regclass('pg_temp.pool_fixture') IS NULL, current_setting('application_name') <> 'pool_fixture', NOT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory')", &[]).await.unwrap();
        assert!(clean.boolean(0).unwrap());
        assert!(clean.boolean(1).unwrap());
        assert!(clean.boolean(2).unwrap());

        // PostgreSQL may finish an already-running command before observing the
        // socket close, even though the client-side driver has terminated.
        assert!(tokio::time::timeout(Duration::from_millis(40), lease.batch("SELECT pg_sleep(2)")).await.is_err());
        assert_eq!(lease.recycle().await, Err(Error::Poisoned));
        let mut lease = tokio::time::timeout(Duration::from_secs(5), pool.acquire()).await.unwrap().unwrap();
        let after_cancel = pid(&mut lease).await;
        assert_ne!(after_cancel, original);
        let overlapping = lease.query(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'active')",
            &[&original],
        ).await.unwrap();
        assert!(overlapping.boolean(0).unwrap(), "server work must still be active when replacement opens");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = lease.query(
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)",
                    &[&original],
                ).await.unwrap();
                if row.boolean(0).unwrap() { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("interrupted backend did not close");

        assert_ne!(after_cancel, original);
        lease.recycle().await.unwrap();
        pool.close();
        assert!(matches!(pool.acquire().await, Err(Error::Closed)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.free_slots() != 1 { tokio::task::yield_now().await; }
        }).await.expect("worker did not terminate");
        let active_pool = make_pool(config(), 1, profile());
        let lease = active_pool.acquire().await.unwrap();
        let waiter_pool = active_pool.clone();
        let waiter = tokio::spawn(async move { waiter_pool.acquire().await });
        tokio::task::yield_now().await;
        active_pool.close();
        assert!(matches!(waiter.await.unwrap(), Err(Error::Closed)));
        drop(lease);
        tokio::time::timeout(Duration::from_secs(5), async {
            while active_pool.free_slots() != 1 { tokio::task::yield_now().await; }
        }).await.expect("active worker did not terminate");
        let mut remote = config();
        remote.host("192.0.2.1");
        assert!(matches!(pool::Pool::loopback_plaintext_for_tests(remote, 1, profile()), Err(Error::InvalidPolicy)));
        assert!(matches!(pool::Pool::loopback_plaintext_for_tests(config(), 0, profile()), Err(Error::InvalidPolicy)));
        assert!(matches!(pool::Pool::loopback_plaintext_for_tests(config(), usize::MAX, profile()), Err(Error::InvalidPolicy)));
        let migration = make_pool(
            config(), 1,
            Profile::migration(profile(), Duration::from_secs(8), Duration::from_secs(1)).unwrap(),
        );
        let mut migration_lease = migration.acquire().await.unwrap();
        assert_eq!(migration_lease.query(
            "SELECT setting::int4 FROM pg_catalog.pg_settings WHERE name='statement_timeout'",
            &[],
        ).await.unwrap().int(0).unwrap(), 8000);
        migration_lease.recycle().await.unwrap();
        migration.close();

        let deadline_pool = make_pool(config(), 1, profile());
        let mut short = deadline_pool.acquire().await.unwrap();
        assert_eq!(
            short.batch_deadline("SELECT pg_sleep(1)", Duration::from_millis(80), Duration::from_secs(2)).await,
            Err(Error::ServerCancelled),
        );
        assert_eq!(short.recycle().await, Err(Error::Poisoned));
        let mut timed = tokio::time::timeout(Duration::from_secs(5), deadline_pool.acquire()).await.unwrap().unwrap();
        let cancelled_pid = pid(&mut timed).await;
        assert_eq!(
            timed.batch_deadline("SELECT pg_sleep(3)", Duration::from_secs(3), Duration::from_millis(70)).await,
            Err(Error::OutcomeUnknown),
        );
        assert_eq!(timed.recycle().await, Err(Error::Poisoned));
        let mut fresh = tokio::time::timeout(Duration::from_secs(5), deadline_pool.acquire()).await.unwrap().unwrap();
        let fresh_pid = pid(&mut fresh).await;
        assert_ne!(fresh_pid, cancelled_pid);
        fresh.recycle().await.unwrap();
        let mut reused = deadline_pool.acquire().await.unwrap();
        assert_eq!(pid(&mut reused).await, fresh_pid);
        reused.batch("SELECT pg_sleep(0.2)").await.unwrap();
        reused.recycle().await.unwrap();
        deadline_pool.close();
        let bounded_after_drop = make_pool(
            config(),
            1,
            Profile::normal(Duration::from_millis(250), Duration::from_secs(1)).unwrap(),
        );
        let mut interrupted = bounded_after_drop.acquire().await.unwrap();
        let interrupted_pid = pid(&mut interrupted).await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                interrupted.batch("SELECT pg_sleep(10)"),
            )
            .await
            .is_err()
        );
        drop(interrupted);
        let mut observer = tokio::time::timeout(
            Duration::from_secs(5), bounded_after_drop.acquire(),
        ).await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let row = observer.query(
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')",
                    &[&interrupted_pid],
                ).await.unwrap();
                if row.boolean(0).unwrap() { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("statement timeout did not bound abandoned server work");
        observer.recycle().await.unwrap();
        bounded_after_drop.close();
        let concurrent = make_pool(config(), 2, profile());
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = concurrent.clone();
            tasks.push(tokio::spawn(async move {
                let mut lease = pool.acquire().await.unwrap();
                let row = lease.query(
                    "SELECT pg_backend_pid() WHERE $1::text = 'concurrent_value_canary' /* concurrent_sql_canary */",
                    &[&"concurrent_value_canary"],
                ).await.unwrap();
                let pid = row.int(0).unwrap();
                lease.batch("SELECT pg_sleep(0.02)").await.unwrap();
                lease.recycle().await.unwrap();
                pid
            }.with_current_subscriber()));
        }
        let mut pids = std::collections::HashSet::new();
        for task in tasks {
            pids.insert(tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap());
        }
        assert!(!pids.is_empty() && pids.len() <= 2);
        concurrent.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            while concurrent.free_slots() != 2 { tokio::task::yield_now().await; }
        }).await.expect("concurrent workers did not terminate");
        let mut error_lease = pool::Pool::loopback_plaintext_for_tests(config(), 1, profile()).unwrap().acquire().await.unwrap();
        assert_eq!(error_lease.batch("DO $$ BEGIN RAISE EXCEPTION 'pool_server_error_canary'; END $$").await, Err(Error::Operation));
        assert_eq!(error_lease.recycle().await, Err(Error::Poisoned));
        for sql in [
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40003', MESSAGE='pool_unknown_error_canary'; END $$",
            "SELECT pg_terminate_backend(pg_backend_pid())",
        ] {
            let unknown_pool = make_pool(config(), 1, profile());
            let mut unknown = unknown_pool.acquire().await.unwrap();
            assert_eq!(unknown.batch(sql).await, Err(Error::OutcomeUnknown));
            assert_eq!(unknown.recycle().await, Err(Error::Poisoned));
            unknown_pool.close();
        }
        localized_fatal_is_unknown(true).await;
        localized_fatal_is_unknown(false).await;
        tracing::info!("pool_caller_tracing_sentinel");
        log::info!("pool_caller_log_sentinel");
    }.await;
    let trace = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let logs = LOGS.0.lock().unwrap().join("\n");
    assert!(trace.contains("pool_caller_tracing_sentinel"));
    assert!(logs.contains("pool_caller_log_sentinel"));
    assert!(!trace.contains("experimental pool driver started"));
    assert!(!logs.contains("experimental pool driver started"));
    for secret in [
        "pool_sql_canary",
        "pool_value_canary",
        "pool_notice_canary",
        "pool_server_error_canary",
        "pool_unknown_error_canary",
        "ERREUR FATALE",
        "localized_fatal_server_message_canary",
        "concurrent_sql_canary",
        "concurrent_value_canary",
    ] {
        assert!(!trace.contains(secret));
        assert!(!logs.contains(secret));
    }
}

/// Mutate real PostgreSQL fatal errors rather than depending on installed
/// server locales. Keep the non-08 SQLSTATE, translate S, and optionally omit V.
fn localize_fatal(body: &[u8], retain_nonlocalized: bool) -> Vec<u8> {
    let mut rewritten = Vec::new();
    let mut required = [false; 4];
    for field in body
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
    {
        match field[0] {
            b'S' => {
                assert_eq!(&field[1..], b"FATAL");
                rewritten.extend_from_slice(b"SERREUR FATALE\0");
                required[0] = true;
            }
            b'V' => {
                assert_eq!(&field[1..], b"FATAL");
                if retain_nonlocalized {
                    rewritten.extend_from_slice(b"VFATAL\0");
                }
                required[1] = true;
            }
            b'C' => {
                assert_eq!(&field[1..], b"57P01");
                rewritten.extend_from_slice(field);
                rewritten.push(0);
                required[2] = true;
            }
            b'M' => {
                rewritten.extend_from_slice(b"Mlocalized_fatal_server_message_canary\0");
                required[3] = true;
            }
            _ => {
                rewritten.extend_from_slice(field);
                rewritten.push(0);
            }
        }
    }
    assert!(required.into_iter().all(|present| present));
    rewritten.push(0);
    rewritten
}

async fn localized_fatal_is_unknown(retain_nonlocalized: bool) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (incoming, _) = listener.accept().await.unwrap();
        let upstream = tokio::net::TcpStream::connect(("127.0.0.1", 55433))
            .await
            .unwrap();
        let (mut client_read, mut client_write) = incoming.into_split();
        let (mut server_read, mut server_write) = upstream.into_split();
        let outbound = async {
            let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
        };
        let inbound = async {
            let mut rewritten = false;
            loop {
                let tag = match server_read.read_u8().await {
                    Ok(tag) => tag,
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => panic!("unexpected proxy read failure ({:?})", error.kind()),
                };
                let len = server_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; usize::try_from(len - 4).unwrap()];
                server_read.read_exact(&mut body).await.unwrap();
                if tag == b'E' {
                    assert!(!rewritten);
                    body = localize_fatal(&body, retain_nonlocalized);
                    rewritten = true;
                }
                client_write.write_u8(tag).await.unwrap();
                client_write
                    .write_u32(u32::try_from(body.len() + 4).unwrap())
                    .await
                    .unwrap();
                client_write.write_all(&body).await.unwrap();
            }
            client_write.shutdown().await.unwrap();
            assert!(rewritten, "live fatal ErrorResponse was not observed");
        };
        tokio::join!(outbound, inbound);
    });
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    let pool = make_pool(config, 1, profile());
    let mut lease = pool.acquire().await.unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        lease.batch("SELECT pg_terminate_backend(pg_backend_pid())"),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error, Error::OutcomeUnknown);
    assert!(std::error::Error::source(&error).is_none());
    let safe = format!("{error:?} {error}");
    assert!(!safe.contains("localized_fatal_server_message_canary"));
    assert!(!safe.contains("ERREUR FATALE"));
    assert_eq!(lease.recycle().await, Err(Error::Poisoned));
    pool.close();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.free_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("localized fatal leaked a connection permit");
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .unwrap()
        .unwrap();
}
