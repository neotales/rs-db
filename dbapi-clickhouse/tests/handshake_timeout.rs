//! Connection timeouts must cover the native hello/init exchange, not only TCP and TLS.

use std::time::{Duration, Instant};

use neotales_dbapi::ErrorKind;
use neotales_dbapi_clickhouse::{ClickHouseConfig, ClickHousePoolProvider};
#[cfg(unix)]
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
#[cfg(unix)]
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio_rustls::TlsAcceptor;
#[cfg(unix)]
use tokio_rustls::rustls::{self, pki_types::PrivateKeyDer};

const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
const TEST_GUARD: Duration = Duration::from_secs(3);
const MAX_OBSERVED: Duration = Duration::from_secs(1);

#[tokio::test]
async fn plaintext_silent_peer_is_bounded_by_connect_timeout() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (eof, eof_seen) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1024];
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => {
                    let _ = eof.send(());
                    break;
                }
                Ok(_) => {}
            }
        }
    });

    let config = ClickHouseConfig::plaintext_loopback("127.0.0.1", port, "default", "default")
        .unwrap()
        .connect_timeout(CONNECT_TIMEOUT);
    let started = Instant::now();
    let result = tokio::time::timeout(TEST_GUARD, ClickHousePoolProvider::connect(config))
        .await
        .expect("connect must not hang on a silent native peer");
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Connection);
    assert!(started.elapsed() < MAX_OBSERVED);
    tokio::time::timeout(Duration::from_secs(1), eof_seen)
        .await
        .expect("a timed-out native handshake must release the socket")
        .expect("peer EOF signal");
    let _ = tokio::time::timeout(Duration::from_secs(1), peer).await;
}

#[tokio::test]
async fn non_reading_peer_blocks_hello_write_but_times_out_and_closes() {
    const HELLO_PASSWORD_BYTES: usize = 8 * 1024 * 1024;
    // Copying and zeroizing the oversized hello are synchronous; this is a
    // liveness budget, not an assertion about scheduler or destructor latency.
    const LIVENESS_GUARD: Duration = Duration::from_secs(10);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted, peer_accepted) = tokio::sync::oneshot::channel();
    let (read_now, start_reading) = tokio::sync::oneshot::channel();
    let (eof, eof_seen) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = accepted.send(());
        let _ = start_reading.await;
        let mut buffer = [0u8; 8192];
        let mut received = 0usize;
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => {
                    let _ = eof.send(received);
                    break;
                }
                Ok(count) => received += count,
            }
        }
    });

    let config = ClickHouseConfig::plaintext_loopback("127.0.0.1", port, "default", "default")
        .unwrap()
        .password("x".repeat(HELLO_PASSWORD_BYTES))
        .connect_timeout(CONNECT_TIMEOUT);
    let connect = tokio::spawn(ClickHousePoolProvider::connect(config));
    tokio::time::timeout(LIVENESS_GUARD, peer_accepted)
        .await
        .expect("peer must accept the connection")
        .expect("peer accept signal");
    let result = tokio::time::timeout(LIVENESS_GUARD, connect)
        .await
        .expect("connect must not hang on a blocked native hello write")
        .expect("connect task");
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Connection);

    let _ = read_now.send(());
    let received = tokio::time::timeout(LIVENESS_GUARD, eof_seen)
        .await
        .expect("a timed-out native hello write must release the socket")
        .expect("peer EOF signal");
    assert!(
        (1..HELLO_PASSWORD_BYTES).contains(&received),
        "expected a partial hello, received {received} bytes"
    );
    tokio::time::timeout(LIVENESS_GUARD, peer)
        .await
        .expect("peer must finish")
        .expect("peer task");
}

#[cfg(unix)]
#[tokio::test]
async fn tls_silent_peer_is_bounded_after_handshake() {
    let (ca_pem, server) = tls_server();
    let directory = tempfile::tempdir().unwrap();
    let ca_path = directory.path().join("ca.pem");
    std::fs::write(&ca_path, ca_pem).unwrap();

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let (eof, eof_seen) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(socket).await.unwrap();
        let mut buffer = [0u8; 1024];
        loop {
            match tls.read(&mut buffer).await {
                Ok(0) | Err(_) => {
                    let _ = eof.send(());
                    break;
                }
                Ok(_) => {}
            }
        }
    });

    let config = ClickHouseConfig::new("127.0.0.1", port, "default", "default")
        .ca_file(&ca_path)
        .tls_server_name("clickhouse.test")
        .connect_timeout(CONNECT_TIMEOUT);
    let started = Instant::now();
    let result = tokio::time::timeout(TEST_GUARD, ClickHousePoolProvider::connect(config))
        .await
        .expect("connect must not hang after a silent TLS handshake");
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Connection);
    assert!(started.elapsed() < MAX_OBSERVED);
    tokio::time::timeout(Duration::from_secs(1), eof_seen)
        .await
        .expect("a timed-out native handshake must release the TLS stream")
        .expect("peer EOF signal");
    let _ = tokio::time::timeout(Duration::from_secs(1), peer).await;
}

#[cfg(unix)]
fn tls_server() -> (String, rustls::ServerConfig) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["clickhouse.test".to_string()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.signed_by(&server_key, &issuer).unwrap();
    let crypto = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let server = rustls::ServerConfig::builder_with_provider(crypto)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
        )
        .unwrap();
    (ca_pem, server)
}

#[cfg(not(unix))]
#[tokio::test]
async fn tls_roots_are_unsupported_before_network_connection() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let directory = tempfile::tempdir().unwrap();
    let config = ClickHouseConfig::new("127.0.0.1", port, "default", "default")
        .ca_file(directory.path().join("missing.pem"))
        .tls_server_name("clickhouse.test")
        .connect_timeout(TEST_GUARD);
    let result = tokio::time::timeout(TEST_GUARD, ClickHousePoolProvider::connect(config))
        .await
        .expect("unsupported roots must return promptly");
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "unsupported TLS must not connect to the peer"
    );
}
