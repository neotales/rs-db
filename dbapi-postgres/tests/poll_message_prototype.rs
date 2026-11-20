//! Isolated driver experiment, not used by the public provider.
//!
//! This deliberately uses `NoTls` only against a disposable local server. It is not a
//! production connection implementation: it has no pool, TLS, or transaction policy.
//! At debug level, tokio-postgres logs query text and bound parameter values. This
//! experiment addresses notice messages only, not that separate disclosure risk.

use std::{
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::task::JoinHandle;
use tokio_postgres::{AsyncMessage, Client, NoTls};
use tracing::instrument::WithSubscriber as _;

/// Owns both ends of a connection. Dropping it aborts the driver and closes the socket.
struct Prototype {
    client: Client,
    driver: JoinHandle<()>,
    notices: Arc<AtomicUsize>,
}

impl Prototype {
    async fn connect(url: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, mut connection) = tokio_postgres::connect(url, NoTls).await?;
        let notices = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&notices);
        let driver = tokio::spawn(
            async move {
                tracing::trace!("prototype driver started");
                log::trace!("prototype driver started");
                loop {
                    match poll_fn(|cx| connection.poll_message(cx)).await {
                        // Never format or log server-provided notice contents.
                        Some(Ok(AsyncMessage::Notice(_))) => {
                            observed.fetch_add(1, Ordering::Relaxed);
                        }
                        Some(Ok(_)) => {}
                        // Errors may also contain sensitive information.
                        Some(Err(_)) | None => break,
                    }
                }
            }
            .with_current_subscriber(),
        );
        Ok(Self {
            client,
            driver,
            notices,
        })
    }
}

impl Drop for Prototype {
    fn drop(&mut self) {
        self.driver.abort();
    }
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

struct LogCapture(Mutex<Vec<String>>);

impl log::Log for LogCapture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }

    fn flush(&self) {}
}

static LOGS: LogCapture = LogCapture(Mutex::new(Vec::new()));

#[tokio::test]
async fn consumes_notices_and_closes_connection() {
    // This integration-test binary has one test. This logger is test-only and is not
    // installed by the prototype or provider.
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();

    async {
        let address = std::net::Ipv4Addr::LOCALHOST;
        let url = format!("postgres://postgres@{address}:55433/postgres");
        let observer = Prototype::connect(&url)
            .await
            .expect("test database required");
        let connection = Prototype::connect(&url).await.unwrap();
        let pid: i32 = connection
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        // Build the canary on the server: the driver logs both SQL and parameters.
        connection
            .client
            .batch_execute(
                "CREATE FUNCTION pg_temp.emit_notice() RETURNS void LANGUAGE plpgsql AS $$ \
             DECLARE v text := 'fixture_' || 'notice_secret'; \
             BEGIN RAISE NOTICE '%', v; RAISE WARNING '%', v; END $$",
            )
            .await
            .unwrap();
        connection
            .client
            .query_one("SELECT pg_temp.emit_notice()", &[])
            .await
            .unwrap();
        let answer: i32 = connection
            .client
            .query_one("SELECT 42::int4", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(answer, 42);
        assert_eq!(connection.notices.load(Ordering::Relaxed), 2);
        // Demonstrate the distinct debug-parameter disclosure before any provider switch.
        connection
            .client
            .query_one("SELECT $1::text", &[&"fixture_parameter_canary"])
            .await
            .unwrap();
        drop(connection);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let exists: bool = observer
                    .client
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)",
                        &[&pid],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if !exists {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("connection did not close");
        drop(observer);
        // Aborted tasks are scheduled before inspecting their output.
        tokio::task::yield_now().await;
    }
    .with_subscriber(subscriber)
    .await;

    let tracing_output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let log_output = LOGS.0.lock().unwrap().join("\n");
    assert!(tracing_output.contains("prototype driver started"));
    assert!(log_output.contains("prototype driver started"));
    assert!(log_output.contains("fixture_parameter_canary"));
    assert!(
        !tracing_output.contains("fixture_notice_secret"),
        "{tracing_output}"
    );
    assert!(
        !log_output.contains("fixture_notice_secret"),
        "{log_output}"
    );
}
