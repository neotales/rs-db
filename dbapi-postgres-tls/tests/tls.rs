//! Requires the disposable TLS-enabled PostgreSQL and public DER roots described in README.md.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi_postgres_tls::{Error, ErrorKind, VerifiedTls, connect};
use neotales_tokio_postgres::{Config, config::SslMode};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
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

async fn connect_error(config: &Config, tls: VerifiedTls) -> Error {
    match connect(config, tls).await {
        Ok(_) => panic!("connection unexpectedly succeeded"),
        Err(err) => err,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One capture covers startup, cancellation and lifecycle.
async fn verified_tls_and_logging() {
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
        assert_eq!(
            VerifiedTls::new(&[]).unwrap_err().kind(),
            ErrorKind::InvalidRoots
        );
        let invalid = VerifiedTls::new(&[b"invalid_cert_canary"]).unwrap_err();
        assert_eq!(invalid.kind(), ErrorKind::InvalidRoots);
        assert!(!format!("{invalid:?} {invalid}").contains("invalid_cert_canary"));
        assert!(std::error::Error::source(&invalid).is_none());
        let good = config("localhost", 55433);
        let mut direct = good.clone();
        direct.ssl_negotiation(neotales_tokio_postgres::config::SslNegotiation::Direct);
        assert_eq!(
            connect_error(&direct, tls.clone()).await.kind(),
            ErrorKind::Unsupported
        );
        let mut binding = good.clone();
        binding.channel_binding(neotales_tokio_postgres::config::ChannelBinding::Require);
        // The fixture authenticates `postgres` with trust, so no SASL exchange
        // can carry the required binding. The handshake itself is supported.
        assert_eq!(
            connect_error(&binding, tls.clone()).await.kind(),
            ErrorKind::Connection
        );
        let rejected = connect_error(&good, VerifiedTls::new(&[&other]).unwrap()).await;
        assert_eq!(rejected.kind(), ErrorKind::TlsRejected);
        assert!(std::error::Error::source(&rejected).is_none());
        assert!(!format!("{rejected:?} {rejected}").contains("tls_fixture_cert_canary"));
        assert_eq!(
            connect_error(&config("wrong.invalid", 55433), tls.clone())
                .await
                .kind(),
            ErrorKind::TlsRejected
        );
        let closed = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let closed_port = closed.local_addr().unwrap().port();
        drop(closed);
        assert_eq!(
            connect_error(&config("localhost", closed_port), tls.clone())
                .await
                .kind(),
            ErrorKind::Io
        );
        let mut invalid_user = good.clone();
        invalid_user.user("tls_server_error_canary");
        let server_error = connect_error(&invalid_user, tls.clone()).await;
        assert_eq!(server_error.kind(), ErrorKind::Connection);
        assert!(std::error::Error::source(&server_error).is_none());
        assert!(!format!("{server_error:?} {server_error}").contains("tls_server_error_canary"));
        let (client, connection, cancellation) = connect(&good, tls.clone())
            .await
            .expect("trusted TLS connection");
        let worker = tokio::spawn(connection.with_current_subscriber());
        let row = client
            .query_one(
                "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid() /* tls_sql_canary */",
                &[],
            )
            .await
            .unwrap();
        assert!(row.get::<_, bool>(0));
        let row = client
            .query_one("SELECT $1::text", &[&"tls_parameter_canary"])
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), "tls_parameter_canary");
        let (query, cancel_result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(client.simple_query("SELECT pg_sleep(30)"), async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                cancellation.cancel().await
            })
        })
        .await
        .expect("cancellation did not finish");
        cancel_result.expect("TLS cancellation transport");
        assert_eq!(query.unwrap_err().code().unwrap().code(), "57014");
        drop(client);
        worker.await.unwrap().unwrap();

        // A proxy forwards the initial TLS connection but refuses TLS on the
        // second (cancellation) connection. It must receive no cancel payload.
        let proxy = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = proxy.local_addr().unwrap().port();
        let proxy_task = tokio::spawn(async move {
            let (mut incoming, _) = proxy.accept().await.unwrap();
            let mut upstream =
                tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, 55433))
                    .await
                    .unwrap();
            let forwarding = tokio::spawn(async move {
                tokio::io::copy_bidirectional(&mut incoming, &mut upstream)
                    .await
                    .unwrap();
            });
            let (mut cancellation_socket, _) = proxy.accept().await.unwrap();
            let mut request = [0u8; 8];
            cancellation_socket.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            cancellation_socket.write_all(b"N").await.unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(cancellation_socket.read(&mut byte).await.unwrap(), 0);
            forwarding.await.unwrap();
        });
        let (proxy_client, proxy_connection, proxy_cancel) =
            connect(&config("localhost", port), tls.clone())
                .await
                .unwrap();
        let proxy_worker = tokio::spawn(proxy_connection.with_current_subscriber());
        let error = proxy_cancel.cancel().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TlsRejected);
        assert!(std::error::Error::source(&error).is_none());
        drop(proxy_client);
        proxy_worker.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), proxy_task)
            .await
            .unwrap()
            .unwrap();

        // A plaintext-only endpoint refuses PostgreSQL's TLS request; no startup
        // message or authentication data may follow.
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
            socket.write_all(b"N").await.unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        assert_eq!(
            connect_error(&config("localhost", port), tls.clone())
                .await
                .kind(),
            ErrorKind::TlsRejected
        );
        server.await.unwrap();
        // Cancel setup while waiting for the TLS response and verify socket closure.
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
        let task = tokio::spawn(async move { connect(&config("localhost", port), tls).await });
        tokio::time::timeout(Duration::from_secs(5), ready_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(err) if err.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        tracing::info!("tls_caller_tracing_sentinel");
        log::info!("tls_caller_log_sentinel");
    }
    .await;

    let tracing = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let logs = LOGS.0.lock().unwrap().join("\n");
    assert!(tracing.contains("tls_caller_tracing_sentinel"));
    assert!(logs.contains("tls_caller_log_sentinel"));
    for canary in [
        "tls_sql_canary",
        "tls_parameter_canary",
        "invalid_cert_canary",
        "tls_fixture_cert_canary",
        "tls_server_error_canary",
        // DER debug output commonly prints certificate content as hexadecimal.
        "746c735f666978747572655f636572745f63616e617279",
    ] {
        assert!(!tracing.contains(canary), "tracing disclosed {canary}");
        assert!(!logs.contains(canary), "log disclosed {canary}");
    }
}
