//! Tests the isolated fork; it is not wired to the public provider.

use std::{
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use futures_util::{SinkExt as _, TryStreamExt as _};
use neotales_tokio_postgres::{self as driver, AsyncMessage, Client, NoTls};
use tokio::task::JoinHandle;
use tracing::instrument::WithSubscriber as _;

/// Owns both ends of a connection. Dropping it aborts the driver and closes the socket.
struct Prototype {
    client: Client,
    driver: JoinHandle<()>,
    notices: Arc<AtomicUsize>,
}

impl Prototype {
    async fn connect(url: &str) -> Result<Self, driver::Error> {
        let (client, mut connection) = driver::connect(url, NoTls).await?;
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
#[allow(clippy::too_many_lines)] // One scoped capture observes both drivers and the full lifecycle.
async fn fork_does_not_log_sensitive_data_or_change_stock_logging() {
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
        let bad_url = format!("postgres://fork_auth_canary@{address}:55433/postgres");
        let Err(startup_error) = Prototype::connect(&bad_url).await else {
            panic!("nonexistent role unexpectedly connected");
        };
        assert!(
            startup_error
                .as_db_error()
                .unwrap()
                .message()
                .contains("fork_auth_canary")
        );
        let bad_db_url = format!("postgres://postgres@{address}:55433/fork_startup_canary");
        let Err(startup_error) = Prototype::connect(&bad_db_url).await else {
            panic!("nonexistent database unexpectedly connected");
        };
        assert!(
            startup_error
                .as_db_error()
                .unwrap()
                .message()
                .contains("fork_startup_canary")
        );
        let connection = Prototype::connect(&url).await.unwrap();
        let pid: i32 = connection
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        connection
            .client
            .batch_execute(
                "CREATE FUNCTION pg_temp.emit_notice() RETURNS void LANGUAGE plpgsql AS $$ \
             DECLARE v text := 'fork_notice_canary'; \
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
        connection
            .client
            .query_one(
                "SELECT $1::text AS fork_sql_canary",
                &[&"fork_parameter_canary"],
            )
            .await
            .unwrap();
        connection
            .client
            .batch_execute("CREATE TEMP TABLE fork_copy_table (value text)")
            .await
            .unwrap();
        let statement = connection
            .client
            .prepare_typed(
                "INSERT INTO fork_copy_table VALUES ($1::text) /* fork_execute_sql_canary */",
                &[driver::types::Type::TEXT],
            )
            .await
            .unwrap();
        assert_eq!(
            connection
                .client
                .execute(&statement, &[&"fork_execute_value_canary"])
                .await
                .unwrap(),
            1
        );
        let statement = connection
            .client
            .prepare_typed(
                "SELECT $1::text AS fork_prepare_sql_canary",
                &[driver::types::Type::TEXT],
            )
            .await
            .unwrap();
        let result = connection
            .client
            .query_one(&statement, &[&"fork_prepare_value_canary"])
            .await
            .unwrap();
        assert_eq!(result.get::<_, &str>(0), "fork_prepare_value_canary");
        let rows = connection
            .client
            .query_typed(
                "SELECT $1::text AS fork_typed_sql_canary",
                &[(&"fork_typed_value_canary", driver::types::Type::TEXT)],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].get::<_, &str>(0), "fork_typed_value_canary");
        let copy = connection
            .client
            .copy_in::<_, Bytes>("COPY fork_copy_table FROM STDIN /* fork_copy_in_sql_canary */")
            .await
            .unwrap();
        tokio::pin!(copy);
        copy.as_mut()
            .send(Bytes::from_static(b"fork_copy_value_canary\n"))
            .await
            .unwrap();
        assert_eq!(copy.as_mut().finish().await.unwrap(), 1);
        let copy_out = connection
            .client
            .copy_out("COPY fork_copy_table TO STDOUT /* fork_copy_out_sql_canary */")
            .await
            .unwrap();
        tokio::pin!(copy_out);
        let mut copied = Vec::new();
        while let Some(chunk) = copy_out.try_next().await.unwrap() {
            copied.extend_from_slice(&chunk);
        }
        assert!(
            String::from_utf8(copied)
                .unwrap()
                .contains("fork_copy_value_canary")
        );
        connection
            .client
            .simple_query("SELECT 'fork_simple_canary'")
            .await
            .unwrap();
        connection
            .client
            .batch_execute("DO $$ BEGIN RAISE EXCEPTION 'fork_error_canary'; END $$")
            .await
            .unwrap_err();
        // The Future implementation must not log notices either.
        let (future_client, future_connection) = driver::connect(&url, NoTls).await.unwrap();
        let future_task = tokio::spawn(future_connection.with_current_subscriber());
        future_client
            .batch_execute("DO $$ BEGIN RAISE WARNING 'fork_future_canary'; END $$")
            .await
            .unwrap();
        drop(future_client);
        future_task.await.unwrap().unwrap();

        // Ordinary upstream users retain their own logging behavior and type identity.
        let (stock_client, stock_connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let stock_task = tokio::spawn(stock_connection.with_current_subscriber());
        stock_client
            .query_one("SELECT $1::text", &[&"stock_parameter_sentinel"])
            .await
            .unwrap();
        drop(stock_client);
        stock_task.await.unwrap().unwrap();
        tracing::info!("caller_tracing_sentinel");
        log::info!("caller_log_sentinel");
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
    assert!(tracing_output.contains("caller_tracing_sentinel"));
    assert!(log_output.contains("caller_log_sentinel"));
    assert!(log_output.contains("stock_parameter_sentinel"));
    for canary in [
        "fork_sql_canary",
        "fork_parameter_canary",
        "fork_simple_canary",
        "fork_notice_canary",
        "fork_error_canary",
        "fork_future_canary",
        "fork_auth_canary",
        "fork_startup_canary",
        "fork_execute_sql_canary",
        "fork_execute_value_canary",
        "fork_prepare_sql_canary",
        "fork_prepare_value_canary",
        "fork_typed_sql_canary",
        "fork_typed_value_canary",
        "fork_copy_in_sql_canary",
        "fork_copy_out_sql_canary",
        "fork_copy_value_canary",
    ] {
        assert!(
            !tracing_output.contains(canary),
            "tracing disclosed {canary}"
        );
        assert!(!log_output.contains(canary), "log disclosed {canary}");
    }
}
