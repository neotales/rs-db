//! `ClickHouse` migration integration coverage against a testcontainers-managed server.
//!
//! Requires Docker. `tests/live.sh` selects this ignored test. The container
//! publishes no ports and uses a random nonempty password from the OS CSPRNG.
//!
//! Both binaries compile the fixture helpers, but each selects only the test
//! appropriate to its library build.
#![allow(dead_code)]

#[path = "diagnostic.rs"]
mod diagnostic;
#[path = "phase.rs"]
mod phase;

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    time::Duration,
};

use neotales_dbapi::{DbEvent, ErrorKind, QueryOptions, QuerySetting, RowLimit, TelemetryOutcome};
use neotales_dbapi_clickhouse::{ClickHouseConfig, ClickHousePoolProvider, ClickHouseTelemetry};
use neotales_migration::{FlattenManifest, Migration, Migrations};
use neotales_migration_clickhouse::{
    DirtyStage, DirtyState, Error, HistoryReason, ReconcileOutcome,
};
use phase::{Backend as ClickHouseBackend, Case, Phase};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};

const IMAGE: &str = "clickhouse/clickhouse-server";
const TAG: &str =
    "24.8-alpine@sha256:b002e56ed5c16e224c312527f6fcba7e77216fec5d7a88a7828f59efc614feb5";
const FIXTURE_OWNER: &str = "plab-clickhouse-phase-b8ba17e";

fn disposable_password() -> String {
    use std::fmt::Write as _;

    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes).expect("OS cryptographic random");
    let mut password = String::with_capacity(3 + bytes.len() * 2);
    password.push_str("Nt-");
    for byte in bytes {
        write!(&mut password, "{byte:02x}").expect("write to String");
    }
    password
}

async fn start_clickhouse(password: &str) -> ContainerAsync<GenericImage> {
    let operation = phase::begin(Phase::ContainerStart);
    let container = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::seconds(2))
        .with_label("io.neotales.fixture-owner", FIXTURE_OWNER)
        .with_env_var("CLICKHOUSE_PASSWORD", password)
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(std::collections::HashMap::new());
        })
        .start()
        .await
        .unwrap_or_else(|_| panic!("fixture container startup failed; details withheld"));
    operation.ok();
    container
}

fn assert_no_published_ports(container: &ContainerAsync<GenericImage>) {
    let operation = phase::begin(Phase::PortIsolation);
    let inspect = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.HostConfig.PublishAllPorts}}|{{json .HostConfig.PortBindings}}",
            container.id(),
        ])
        .output()
        .expect("docker inspect");
    assert!(inspect.status.success());
    assert_eq!(
        String::from_utf8_lossy(&inspect.stdout).trim(),
        "false|{}",
        "container ports must not be published"
    );
    for port in ["8123", "9000"] {
        let mapped = Command::new("docker")
            .args(["port", container.id(), port])
            .output()
            .expect("docker port");
        assert!(mapped.stdout.is_empty(), "{port} must have no host mapping");
    }
    operation.ok();
}

fn bridge_address(ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, 9000)
}

async fn bridge_ip(container: &ContainerAsync<GenericImage>) -> IpAddr {
    let operation = phase::begin(Phase::BridgeAddress);
    let ip = container
        .get_bridge_ip_address()
        .await
        .expect("bridge address");
    operation.ok();
    ip
}

async fn loopback_proxy(target: SocketAddr) -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("proxy listener");
    let port = listener.local_addr().expect("proxy address").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                // Preserve the native client's low-latency transport on both proxy legs.
                // Nagle plus delayed ACKs otherwise amplifies multi-query backend latency.
                inbound.set_nodelay(true).unwrap();
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    outbound.set_nodelay(true).unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    port
}

async fn monitored_proxy(
    target: SocketAddr,
) -> (u16, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                break;
            };
            observed.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                client.set_nodelay(true).unwrap();
                if let Ok(mut server) = tokio::net::TcpStream::connect(target).await {
                    server.set_nodelay(true).unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                }
            });
        }
    });
    (port, count)
}

async fn connect(port: u16, password: &str) -> ClickHousePoolProvider {
    connect_with_timeout(port, password, Duration::from_secs(30)).await
}

async fn connect_with_timeout(
    port: u16,
    password: &str,
    request_timeout: Duration,
) -> ClickHousePoolProvider {
    for _ in 0..30 {
        let config = ClickHouseConfig::plaintext_loopback("127.0.0.1", port, "default", "default")
            .expect("loopback configuration")
            .password(password)
            .request_timeout(request_timeout);
        match phase::readiness(config).await {
            Ok(provider) => return provider,
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    panic!("disposable ClickHouse did not accept connections");
}

// A server process restart of the same container. The container's writable layer is preserved;
// its Memory table data is not. This is not a host crash or power-loss test.
fn restart_clickhouse(container: &ContainerAsync<GenericImage>) {
    let operation = phase::begin(Phase::ContainerRestart);
    let output = Command::new("docker")
        .args(["restart", "--timeout", "2", container.id()])
        .output()
        .expect("docker restart");
    assert!(
        output.status.success(),
        "fixture container restart failed; details withheld"
    );
    operation.ok();
}

// Reopen the loopback-only path to the current bridge address. Docker may reassign the address
// across a restart, so the old proxy target is not reused.
async fn reconnect(
    container: &ContainerAsync<GenericImage>,
    password: &str,
) -> ClickHousePoolProvider {
    let operation = phase::begin(Phase::Reconnect);
    let ip = bridge_ip(container).await;
    let port = loopback_proxy(bridge_address(ip)).await;
    let provider = connect(port, password).await;
    operation.ok();
    provider
}

async fn held_response_proxy(
    target: SocketAddr,
    marker: &'static [u8],
) -> (
    u16,
    tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (detected, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            let detected = detected.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                inbound.set_nodelay(true).unwrap();
                outbound.set_nodelay(true).unwrap();
                let (mut client_read, mut client_write) = inbound.split();
                let (mut server_read, mut server_write) = outbound.split();
                let mut seen = Vec::new();
                let mut from_client = [0; 8192];
                let mut from_server = [0; 8192];
                loop {
                    tokio::select! {
                        result = client_read.read(&mut from_client) => {
                            let Ok(n) = result else { break };
                            if n == 0 { break; }
                            seen.extend_from_slice(&from_client[..n]);
                            if server_write.write_all(&from_client[..n]).await.is_err() { break; }
                            if seen.windows(marker.len()).any(|window| window == marker) {
                                let (close, mut wait) = tokio::sync::oneshot::channel();
                                let _ = detected.send(close);
                                // The request may span several writes (for example an INSERT
                                // whose data block follows the query text). Keep forwarding the
                                // client so the server can execute it, but discard the response so
                                // the caller observes the interrupted outcome.
                                loop {
                                    tokio::select! {
                                        _ = &mut wait => break,
                                        result = client_read.read(&mut from_client) => {
                                            let Ok(n) = result else { break };
                                            if n == 0 { break; }
                                            if server_write.write_all(&from_client[..n]).await.is_err() { break; }
                                        }
                                        result = server_read.read(&mut from_server) => {
                                            let Ok(n) = result else { break };
                                            if n == 0 { break; }
                                        }
                                    }
                                }
                                break;
                            }
                            let keep = marker.len().saturating_sub(1);
                            if seen.len() > keep { seen.drain(..seen.len() - keep); }
                        }
                        result = server_read.read(&mut from_server) => {
                            let Ok(n) = result else { break };
                            if n == 0 || client_write.write_all(&from_server[..n]).await.is_err() { break; }
                        }
                    }
                }
            });
        }
    });
    (port, receiver)
}

// Hold the response to the Nth request containing `marker` on one client connection. The server
// commits the write, then this proxy withholds its response until the test releases it. Later
// requests on the same connection are not reached while the response is held.
async fn held_nth_response_proxy(
    target: SocketAddr,
    marker: &'static [u8],
    occurrence: usize,
) -> (
    u16,
    tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (detected, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            let detected = detected.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                inbound.set_nodelay(true).unwrap();
                outbound.set_nodelay(true).unwrap();
                let (mut client_read, mut client_write) = inbound.split();
                let (mut server_read, mut server_write) = outbound.split();
                let mut seen = Vec::new();
                let mut matches = 0usize;
                let mut from_client = [0; 8192];
                let mut from_server = [0; 8192];
                loop {
                    tokio::select! {
                        result = client_read.read(&mut from_client) => {
                            let Ok(n) = result else { break };
                            if n == 0 { break; }
                            seen.extend_from_slice(&from_client[..n]);
                            if server_write.write_all(&from_client[..n]).await.is_err() { break; }
                            if seen.windows(marker.len()).any(|window| window == marker) {
                                matches += 1;
                                if matches == occurrence {
                                    let (close, mut wait) = tokio::sync::oneshot::channel();
                                    let _ = detected.send(close);
                                    // Forward any remaining client writes so the server can
                                    // commit, but discard the response.
                                    loop {
                                        tokio::select! {
                                            _ = &mut wait => break,
                                            result = client_read.read(&mut from_client) => {
                                                let Ok(n) = result else { break };
                                                if n == 0 { break; }
                                                if server_write.write_all(&from_client[..n]).await.is_err() { break; }
                                            }
                                            result = server_read.read(&mut from_server) => {
                                                let Ok(n) = result else { break };
                                                if n == 0 { break; }
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                            let keep = marker.len().saturating_sub(1);
                            if seen.len() > keep { seen.drain(..seen.len() - keep); }
                        }
                        result = server_read.read(&mut from_server) => {
                            let Ok(n) = result else { break };
                            if n == 0 || client_write.write_all(&from_server[..n]).await.is_err() { break; }
                        }
                    }
                }
            });
        }
    });
    (port, receiver)
}

// Pause the server response to one identified request without closing either stream.
// This holds the winner inside its write until the contenders have finished.
async fn gated_response_proxy(
    target: SocketAddr,
    marker: &'static [u8],
) -> (
    u16,
    tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (detected, receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            let detected = detected.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                inbound.set_nodelay(true).unwrap();
                outbound.set_nodelay(true).unwrap();
                let (mut cr, mut cw) = inbound.split();
                let (mut sr, mut sw) = outbound.split();
                let mut seen = Vec::new();
                let mut client = [0; 8192];
                let mut server = [0; 8192];
                loop {
                    tokio::select! {
                        result = cr.read(&mut client) => {
                            let Ok(n) = result else { break };
                            if n == 0 { break; }
                            seen.extend_from_slice(&client[..n]);
                            if sw.write_all(&client[..n]).await.is_err() { break; }
                            if seen.windows(marker.len()).any(|window| window == marker) {
                                let (resume, mut wait) = tokio::sync::oneshot::channel();
                                if detected.send(resume).is_err() { break; }
                                // A native request includes a terminating data block after
                                // its query packet. Gate only the response: pausing both
                                // directions here depends on TCP coalescing and can prevent
                                // the server from ever executing the identified request.
                                let resumed = loop {
                                    tokio::select! {
                                        result = &mut wait => break result.is_ok(),
                                        result = cr.read(&mut client) => {
                                            let Ok(n) = result else { break false };
                                            if n == 0 { break false; }
                                            if sw.write_all(&client[..n]).await.is_err() { break false; }
                                        }
                                    }
                                };
                                if !resumed { break; }
                                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                                break;
                            }
                            let keep = marker.len().saturating_sub(1);
                            if seen.len() > keep { seen.drain(..seen.len() - keep); }
                        }
                        result = sr.read(&mut server) => {
                            let Ok(n) = result else { break };
                            if n == 0 || cw.write_all(&server[..n]).await.is_err() { break; }
                        }
                    }
                }
            });
        }
    });
    (port, receiver)
}

fn migration(version: u64, name: &str, up: &str, down: Option<&str>) -> Migration {
    Migration::new(version, name, up, down.map(str::to_owned)).unwrap()
}

#[tokio::test]
async fn response_gate_forwards_a_fragmented_request_before_resume() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let (port, mut detected) =
        gated_response_proxy(listener.local_addr().unwrap(), b"fixture-marker").await;
    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    client.write_all(b"fixture-marker").await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    let mut marker = [0; 14];
    server.read_exact(&mut marker).await.unwrap();
    let resume = detected.recv().await.unwrap();
    // The native query and its terminating data block may arrive in separate writes.
    client.write_all(b"tail").await.unwrap();
    let mut tail = [0; 4];
    tokio::time::timeout(Duration::from_secs(1), server.read_exact(&mut tail))
        .await
        .expect("response gate must not hold back request bytes")
        .unwrap();
    assert_eq!(&tail, b"tail");
    server.write_all(b"reply").await.unwrap();
    let mut reply = [0; 5];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), client.read_exact(&mut reply))
            .await
            .is_err()
    );
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, b"reply");
}

fn backend_for(table: &str, timeout: Duration) -> ClickHouseBackend {
    ClickHouseBackend::new("default", table, timeout).unwrap()
}

// Private fixture branching: prove a held/faulted socket cannot gate later
// connections or leave its synthetic server worker alive after client teardown.
#[tokio::test]
async fn response_faults_are_connection_local() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for mode in 0..3 {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let target = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut workers = tokio::task::JoinSet::new();
                for _ in 0..3 {
                    let (mut peer, _) = listener.accept().await.unwrap();
                    workers.spawn(async move {
                        let mut request = [0; 5];
                        peer.read_exact(&mut request).await.unwrap();
                        peer.write_all(b"reply").await.unwrap();
                        let mut end = [0];
                        match peer.read(&mut end).await {
                            Ok(0) => {}
                            Err(error)
                                if request == *b"fault"
                                    && error.kind() == std::io::ErrorKind::ConnectionReset => {}
                            result => panic!("worker did not close: {result:?}"),
                        }
                    });
                }
                while let Some(result) = workers.join_next().await {
                    result.unwrap();
                }
            });
            let (port, mut detected) = match mode {
                0 => held_response_proxy(target, b"fault").await,
                1 => held_nth_response_proxy(target, b"fault", 1).await,
                _ => gated_response_proxy(target, b"fault").await,
            };
            let mut fault = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            fault.write_all(b"fault").await.unwrap();
            let release = detected.recv().await.unwrap();
            let mut reply = [0; 5];
            assert!(
                tokio::time::timeout(Duration::from_millis(50), fault.read_exact(&mut reply))
                    .await
                    .is_err()
            );
            for after_close in [false, true] {
                if after_close {
                    // Closing the faulted peer, not resuming a successful response, must
                    // tear down that worker without changing subsequent connections.
                    fault.shutdown().await.unwrap();
                }
                let mut clean = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .unwrap();
                clean.write_all(b"clean").await.unwrap();
                clean.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"reply");
            }
            drop(fault);
            drop(release);
            server.await.unwrap();
        })
        .await
        .expect("fault must neither block independent sockets nor leak a server worker");
    }
}

async fn run_sql(provider: &ClickHousePoolProvider, sql: &str) {
    let mut session = phase::acquire(provider).await.unwrap();
    // Fixture-only cleanup statements contain no quoted semicolons; production migration
    // preflight never uses this split and rejects multi-statement scripts.
    for statement in sql
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        session
            .fetch_all_async(
                statement,
                QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
            )
            .await
            .unwrap();
    }
}

async fn scalar(provider: &ClickHousePoolProvider, sql: &str) -> i64 {
    let mut session = phase::acquire(provider).await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(neotales_dbapi::RowLimit::new(1, 4096).unwrap()),
        )
        .await
        .unwrap();
    phase::db_result(Phase::ScalarDecode, || rows[0].get::<i64>("value")).unwrap()
}

#[allow(clippy::too_many_lines)] // One isolated fixture checks preflight and cancellation.
pub(super) async fn the_backend_fails_closed_on_the_fetch_only_provider() {
    Box::pin(phase::case(Case::Disabled, disabled_contract())).await;
}

#[allow(clippy::too_many_lines)]
async fn disabled_contract() {
    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    assert_no_published_ports(&container);
    let ip = bridge_ip(&container).await;
    let (port, accepted) = monitored_proxy(bridge_address(ip)).await;
    let provider = connect(port, &password).await;
    let baseline = accepted.load(std::sync::atomic::Ordering::SeqCst);
    let backend = backend_for("unavailable", Duration::from_secs(1));

    let invalid = Migrations::new(vec![migration(
        1,
        "bad-down",
        "CREATE TABLE unavailable_app (n Int64) ENGINE=Memory",
        Some("DROP TABLE unavailable_app; DROP TABLE unavailable_other"),
    )])
    .unwrap();
    assert!(matches!(
        backend
            .up(
                &provider,
                "unavailable",
                &invalid,
                "binding",
                &["unavailable_app"]
            )
            .await,
        Err(Error::InvalidDefinitions)
    ));

    let result = backend
        .initialize_module(&provider, "unavailable", "fetch-only", &[])
        .await;
    // The default build rejects the operation before acquiring a session or creating a lock.
    match result {
        Err(Error::Database(error)) => assert_eq!(error.kind(), ErrorKind::Unsupported),
        other => panic!("expected the unavailable backend to fail, got {other:?}"),
    }
    assert!(
        matches!(backend.state(&provider, "unavailable").await, Err(Error::Database(error)) if error.kind() == ErrorKind::Unsupported)
    );
    let valid = Migrations::new(vec![migration(
        1,
        "disabled",
        "CREATE TABLE unavailable_app (n Int64) ENGINE=Memory",
        Some("DROP TABLE unavailable_app"),
    )])
    .unwrap();
    for result in [
        backend
            .up(&provider, "unavailable", &valid, "binding", &[])
            .await,
        backend
            .down(&provider, "unavailable", &valid, "binding", 1, &[])
            .await,
        backend
            .reconcile(
                &provider,
                "unavailable",
                &valid,
                "binding",
                ReconcileOutcome::NotApplied,
            )
            .await,
    ] {
        assert!(
            matches!(result, Err(Error::Database(error)) if error.kind() == ErrorKind::Unsupported)
        );
    }
    let cancelled_provider = provider.clone();
    let cancelled_backend = backend.clone();
    let cancelled = phase::spawn(async move {
        cancelled_backend
            .initialize_module(&cancelled_provider, "cancelled", "binding", &[])
            .await
    });
    cancelled.abort();
    let _ = cancelled.await;
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        baseline,
        "disabled and cancelled operations must not open a session"
    );

    let mut session = phase::acquire(&provider).await.unwrap();
    assert_eq!(
        session
            .execute_async(
                "CREATE TABLE unavailable_lock (owner String) ENGINE=Memory",
                QueryOptions::new(),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported,
        "execute must fail before dispatch"
    );
    let rows = session
        .fetch_all_async(
            "SELECT toInt64(count()) AS n FROM system.tables \
             WHERE database = currentDatabase() AND name LIKE 'unavailable%'",
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        0,
        "the unavailable backend created a table"
    );
    assert!(
        matches!(backend.state(&provider, "unavailable").await, Err(Error::Database(error)) if error.kind() == ErrorKind::Unsupported)
    );
    let (fault_port, mut detected) =
        held_response_proxy(bridge_address(ip), b"unavailable_fault_lock").await;
    let fault_provider = connect(fault_port, &password).await;
    let fault_backend = backend_for("unavailable_fault", Duration::from_secs(5));
    let attempt = phase::spawn(async move {
        fault_backend
            .initialize_module(&fault_provider, "unavailable", "binding", &[])
            .await
    });
    let result = attempt.await.unwrap();
    assert!(
        matches!(result, Err(Error::Database(error)) if error.kind() == ErrorKind::Unsupported)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), detected.recv())
            .await
            .is_err(),
        "disabled path must never send lock CREATE"
    );
    assert_eq!(scalar(&provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='unavailable_fault_lock'").await, 0);
}

async fn lock_create_response_loss(provider: &ClickHousePoolProvider, ip: IpAddr, password: &str) {
    let backend = backend_for("unavailable", Duration::from_secs(1));
    let mut session = phase::acquire(provider).await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE unavailable_lock (owner String) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_no_count(
            "INSERT INTO unavailable_lock SELECT 'sentinel'",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let contention = backend
        .initialize_module(provider, "unavailable", "binding", &[])
        .await;
    assert!(
        matches!(contention, Err(Error::LockOutcomeUnknown(error)) if error.kind() == ErrorKind::WriteOutcomeUnknown)
    );
    let rows = session
        .fetch_all_async(
            "SELECT toInt64(count()) AS n FROM unavailable_lock WHERE owner = 'sentinel'",
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        1,
        "contender must not remove an existing lock"
    );
    session
        .execute_no_count("DROP TABLE unavailable_lock", QueryOptions::new())
        .await
        .unwrap();

    let (fault_port, mut detected) =
        held_response_proxy(bridge_address(ip), b"unavailable_fault_lock").await;
    let fault_provider = connect(fault_port, password).await;
    let fault_backend = backend_for("unavailable_fault", Duration::from_secs(5));
    let attempt = phase::spawn(async move {
        fault_backend
            .initialize_module(&fault_provider, "unavailable", "binding", &[])
            .await
    });
    let close = tokio::time::timeout(Duration::from_secs(5), detected.recv())
        .await
        .unwrap()
        .expect("lock CREATE must reach proxy");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='unavailable_fault_lock'").await;
            if count == 1 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("server must create lock before response is lost");
    close.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(Error::LockOutcomeUnknown(error)) if error.kind() == ErrorKind::WriteOutcomeUnknown)
    );
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='unavailable_fault_lock'").await, 1);
    session
        .execute_no_count("DROP TABLE unavailable_fault_lock", QueryOptions::new())
        .await
        .unwrap();
}

#[allow(clippy::too_many_lines)]
pub(super) async fn clickhouse_migration_contract() {
    Box::pin(phase::case(Case::Bootstrap, enabled_contract())).await;
}

#[allow(clippy::too_many_lines)]
async fn enabled_contract() {
    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    assert_no_published_ports(&container);
    let ip = bridge_ip(&container).await;
    let target = bridge_address(ip);
    let port = loopback_proxy(target).await;
    let provider = connect(port, &password).await;

    macro_rules! stage {
        ($case:ident, $work:expr) => {{ phase::case(Case::$case, $work).await }};
    }
    stage!(SqlGuard, enabled_sql_guard(target, &password));
    stage!(OrphanHistory, orphan_history_is_rejected(&provider));
    stage!(
        LockCreateLoss,
        lock_create_response_loss(&provider, ip, &password)
    );
    stage!(
        LockReleaseLoss,
        lock_release_response_loss(&provider, target, &password)
    );
    stage!(Ordered, ordered_apply_and_down(&provider));
    stage!(
        DirtyRecovery,
        dirty_marker_blocks_until_reconciled(&provider)
    );
    stage!(LockContention, lock_timeout_and_release(&provider));
    stage!(
        ConcurrentWriters,
        concurrent_writers_serialize(&provider, target, &password)
    );
    stage!(NonAtomic, non_atomic_database_is_rejected(&provider));
    stage!(
        ObservedWrites,
        observed_writes_and_cancellation(&provider, target, &password)
    );
    stage!(
        ScriptTimeout,
        uncertain_script_retains_lock(&provider, port, &password)
    );
    stage!(ScriptCancellation, cancelled_script_retains_lock(&provider));
    stage!(
        StageWriteLoss,
        stage_write_uncertainty_retains_lock(&provider, target, &password)
    );
    stage!(
        BaselineHistoryLoss,
        baseline_history_response_loss_recovers_barrier(&provider, target, &password)
    );
    stage!(
        BaselineEarlyRecovery,
        baseline_reconciliation_response_loss_recovers_early_stages(&provider, target, &password)
    );
    stage!(
        DownHistoryRestore,
        down_history_removal_is_restored(&provider)
    );
    stage!(Untracked, untracked_objects_are_rejected(&provider));
    stage!(Unowned, unowned_lookalikes_are_rejected(&provider));
    stage!(PartialLedger, partial_ledgers_are_rejected(&provider));
    stage!(SchemaDrift, schema_drift_is_rejected(&provider));
    stage!(
        EngineDrift,
        engine_and_key_drift_is_rejected_before_migration(&provider)
    );
    stage!(Flatten, flatten_is_unsupported(&provider));
    stage!(
        BetweenQueries,
        competing_writers_cannot_steal_a_lock_held_between_queries(&provider, target, &password)
    );
    stage!(
        ClearBoundary,
        clear_boundary_response_loss_is_not_dirty(&provider, target, &password)
    );
    // Restart cases last: each consumes the live provider and returns one connected to the
    // restarted server.
    let provider = stage!(
        RestartDirty,
        server_restart_at_dirty_marker(provider, &container, &password)
    );
    let provider = stage!(
        RestartHistory,
        server_restart_after_history_before_barrier(provider, &container, &password)
    );
    let provider = stage!(
        RestartDown,
        server_restart_after_down_tombstone(provider, &container, &password)
    );
    let provider = stage!(
        RestartScript,
        server_restart_mid_script(provider, &container, &password)
    );
    drop(provider);
}

#[tokio::test]
#[ignore = "bounded create/drop diagnostic, Docker required"]
async fn memory_lock_lifecycle_without_fault_injection() {
    Box::pin(phase::case(Case::Diagnostic, diagnostic::run())).await;
}

const GUARD_CREATE: &str = "CREATE TABLE nt_guard_a (n Int64) ENGINE=Memory";
const GUARD_MULTI_CREATE: &str = "CREATE TABLE nt_guard_a (n Int64) ENGINE=Memory; CREATE TABLE nt_guard_b (n Int64) ENGINE=Memory";
const GUARD_MULTI_DROP: &str = "DROP TABLE nt_guard_a; DROP TABLE nt_guard_b";
const GUARD_UNBOUND_INSERT: &str = "INSERT INTO nt_guard_a SELECT {p1:Int64}";

#[test]
fn preflight_rejects_multiple_up_statements() {
    use neotales_dbapi_clickhouse::validate_no_count_statement;
    for sql in GUARD_MULTI_CREATE.split("; ") {
        validate_no_count_statement(sql, &[]).unwrap();
    }
    assert_eq!(
        validate_no_count_statement(GUARD_MULTI_CREATE, &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn preflight_rejects_multiple_down_statements() {
    use neotales_dbapi_clickhouse::validate_no_count_statement;
    validate_no_count_statement(GUARD_CREATE, &[]).unwrap();
    for sql in GUARD_MULTI_DROP.split("; ") {
        validate_no_count_statement(sql, &[]).unwrap();
    }
    assert_eq!(
        validate_no_count_statement(GUARD_MULTI_DROP, &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn preflight_rejects_unbound_insert_parameter() {
    use neotales_dbapi::{DbType, Parameter, Value};
    use neotales_dbapi_clickhouse::validate_no_count_statement;
    let bound = Parameter::new(DbType::I64, Value::I64(1)).unwrap();
    validate_no_count_statement(GUARD_UNBOUND_INSERT, &[bound]).unwrap();
    assert_eq!(
        validate_no_count_statement(GUARD_UNBOUND_INSERT, &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

async fn enabled_sql_guard(target: SocketAddr, password: &str) {
    use std::sync::atomic::Ordering;

    let (port, accepted) = monitored_proxy(target).await;
    let provider = connect(port, password).await;
    let baseline = accepted.load(Ordering::SeqCst);
    let backend = backend_for("nt_sql_guard", Duration::from_secs(2));
    for (case, up, down) in [
        ("multiple up statements", GUARD_MULTI_CREATE, None),
        (
            "multiple down statements",
            GUARD_CREATE,
            Some(GUARD_MULTI_DROP),
        ),
        ("unbound insert parameter", GUARD_UNBOUND_INSERT, None),
    ] {
        let definitions = Migrations::new(vec![migration(1, "guard", up, down)]).unwrap();
        assert!(
            matches!(
                backend
                    .up(&provider, "guard", &definitions, "binding", &[])
                    .await,
                Err(Error::InvalidDefinitions)
            ),
            "{case} must be rejected as invalid definitions"
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            baseline,
            "{case} must not acquire a session"
        );
    }
}

// Orphaned history must never be adopted, even when FINAL hides a retained tombstone.
async fn orphan_history_is_rejected(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_revision_probe", Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_revision_probe; DROP TABLE IF EXISTS nt_revision_probe_meta; \
         DROP TABLE IF EXISTS nt_revision_probe_lock",
    )
    .await;
    backend
        .initialize_module(provider, "seed", "binding", &[])
        .await
        .unwrap();
    let mut session = phase::acquire(provider).await.unwrap();
    let definition = migration(
        1,
        "orphan",
        "CREATE TABLE nt_orphan_object (n Int64) ENGINE=Memory",
        None,
    );
    let checksum = definition.checksum().to_owned();
    let definitions = Migrations::new(vec![definition]).unwrap();
    for (module, deleted) in [("orphan_live", 0), ("orphan_tombstone", 1)] {
        session
            .execute_no_count(
                &format!(
                    "INSERT INTO nt_revision_probe (module, version, checksum, revision, is_deleted) \
                     VALUES ('{module}', 1, '{checksum}', 100, {deleted})"
                ),
                QueryOptions::new(),
            )
            .await
            .unwrap();
        assert_eq!(scalar(provider, &format!(
            "SELECT toInt64(count()) AS value FROM nt_revision_probe WHERE module='{module}'"
        )).await, 1, "physical orphan must be present");
        if deleted == 1 {
            assert_eq!(scalar(provider, &format!(
                "SELECT toInt64(count()) AS value FROM nt_revision_probe FINAL WHERE module='{module}'"
            )).await, 0, "FINAL must hide the tombstone");
        }
        for result in [
            backend
                .up(provider, module, &definitions, "binding", &[])
                .await
                .map(|_| ()),
            backend
                .initialize_module(provider, module, "binding", &[])
                .await
                .map(|_| ()),
            backend.state(provider, module).await.map(|_| ()),
        ] {
            assert!(
                matches!(result, Err(Error::History(HistoryReason::InconsistentRows))),
                "orphan {module} must be rejected: {result:?}"
            );
        }
        assert_eq!(scalar(provider, &format!(
            "SELECT toInt64(count()) AS value FROM nt_revision_probe_meta WHERE module='{module}'"
        )).await, 0, "registration must not write metadata");
        assert_eq!(scalar(provider, &format!(
            "SELECT toInt64(count()) AS value FROM nt_revision_probe WHERE module='{module}'"
        )).await, 1, "the orphaned history must remain untouched");
    }
    assert_eq!(scalar(provider,
        "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_orphan_object'"
    ).await, 0, "migration script must not run");
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_revision_probe; DROP TABLE IF EXISTS nt_revision_probe_meta; \
         DROP TABLE IF EXISTS nt_revision_probe_lock",
    )
    .await;
}

#[allow(clippy::too_many_lines)]
async fn observed_writes_and_cancellation(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    use std::sync::{Arc, Mutex};
    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let sink = Arc::clone(&events);
    let observed = provider.clone().with_telemetry(ClickHouseTelemetry {
        telemetry_enabled: true,
        sql_capture_enabled: true,
        sql_hook: Some(Arc::new(|sql| {
            Some(
                if sql.contains("nt_observe_ledger_lock") {
                    "lock"
                } else if sql.contains("nt_observe_ledger") {
                    "ledger"
                } else if sql.contains("nt_observe_app") {
                    "script"
                } else {
                    "other"
                }
                .to_owned(),
            )
        })),
        observer: Some(Arc::new(move |event| {
            sink.lock().unwrap().push(event.clone());
        })),
    });
    let definitions = Migrations::new(vec![migration(
        1,
        "observed",
        "CREATE TABLE nt_observe_app (id Int64) ENGINE=Memory",
        None,
    )])
    .unwrap();
    backend_for("nt_observe_ledger", Duration::from_secs(2))
        .up(
            &observed,
            "app",
            &definitions,
            "binding",
            &["nt_observe_app"],
        )
        .await
        .unwrap();
    {
        let events = events.lock().unwrap();
        for category in ["lock", "ledger", "script"] {
            assert!(
                events
                    .iter()
                    .any(|event| event.sql.as_deref() == Some(category)
                        && event.outcome == TelemetryOutcome::Success),
                "missing {category}"
            );
        }
        assert!(
            events
                .iter()
                .all(|event| event.operation == neotales_dbapi::Operation::Execute)
        );
    }
    let mut session = phase::acquire(&observed).await.unwrap();
    let failure = session
        .execute_no_count(
            "INVALID secret",
            QueryOptions::new().sql_capture(QuerySetting::Off),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        failure.kind(),
        ErrorKind::InvalidArgument | ErrorKind::Unsupported
    ));
    let last = events.lock().unwrap().last().unwrap().clone();
    assert_eq!(last.sql, None);
    assert_eq!(last.outcome, TelemetryOutcome::Error(failure.kind()));
    let retry =
        neotales_dbapi::RetryPolicy::new(2, neotales_dbapi::Delay::Fixed(Duration::ZERO), false)
            .unwrap();
    let error = session
        .execute_no_count(
            "CREATE TABLE nt_observe_retry (id Int64) ENGINE=Memory",
            QueryOptions::new()
                .retry(neotales_dbapi::RetrySetting::Use(retry))
                .replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(
        scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_observe_retry'").await,
        0,
        "explicit retry must be rejected before dispatch"
    );

    let (port, mut detected) =
        gated_response_proxy(target, b"CREATE TABLE nt_observe_cancel ").await;
    let sink = Arc::clone(&events);
    let events_before_cancellation = events.lock().unwrap().len();
    let cancel_provider = connect(port, password)
        .await
        .with_telemetry(ClickHouseTelemetry {
            telemetry_enabled: true,
            observer: Some(Arc::new(move |event| {
                sink.lock().unwrap().push(event.clone());
            })),
            ..ClickHouseTelemetry::default()
        });
    let task = phase::spawn(async move {
        let mut session = phase::acquire(&cancel_provider).await.unwrap();
        session
            .execute_no_count(
                "CREATE TABLE nt_observe_cancel (id Int64) ENGINE=Memory",
                QueryOptions::new(),
            )
            .await
    });
    let gate = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("write must reach proxy");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_observe_cancel'").await == 1 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("server must finish CREATE while its response is held");
    assert_eq!(
        events.lock().unwrap().len(),
        events_before_cancellation,
        "no success event may precede EndOfStream"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(gate);
    let last = events.lock().unwrap().last().unwrap().clone();
    assert_eq!(last.sql, None);
    assert_eq!(
        last.outcome,
        TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
    );
    run_sql(provider, "DROP TABLE IF EXISTS nt_observe_cancel; DROP TABLE nt_observe_app; DROP TABLE nt_observe_ledger; DROP TABLE nt_observe_ledger_meta").await;
}

async fn lock_release_response_loss(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let (port, mut detected) =
        held_response_proxy(target, b"DROP TABLE `default`.`nt_release_fault_lock`").await;
    let fault_provider = connect(port, password).await;
    let mut attempt = phase::spawn(async move {
        backend_for("nt_release_fault", Duration::from_secs(5))
            .state(&fault_provider, "app")
            .await
    });
    let close = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut attempt => {
                panic!("release fixture: backend exited before DROP detection: {result:?}");
            },
            detection = detected.recv() => detection,
        }
    })
    .await
    .unwrap()
    .expect("lock DROP must reach proxy");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_release_fault_lock'").await == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server must drop lock before response is lost");
    close.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(Error::Release(error)) if error.kind() == ErrorKind::WriteOutcomeUnknown),
        "lost release response must not be reported as success"
    );
}

#[allow(clippy::too_many_lines)]
async fn baseline_reconciliation_response_loss_recovers_early_stages(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    for (ledger, object, stage, marker) in [
        (
            "nt_base_started",
            "nt_base_started_app",
            "started",
            b"INSERT INTO `default`.`nt_base_started` (module".as_slice(),
        ),
        (
            "nt_base_failed",
            "nt_base_failed_app",
            "script_failed",
            b"INSERT INTO `default`.`nt_base_failed` (module".as_slice(),
        ),
    ] {
        let backend = backend_for(ledger, Duration::from_secs(5));
        let baseline = migration(
            1,
            "baseline",
            &format!("CREATE TABLE {object} (n Int64) ENGINE=Memory"),
            Some(&format!("DROP TABLE {object}")),
        )
        .into_baseline();
        let checksum = baseline.checksum().to_owned();
        let migrations = Migrations::new(vec![baseline]).unwrap();
        backend
            .initialize_module(provider, "app", "binding", &[])
            .await
            .unwrap();
        run_sql(
            provider,
            &format!("CREATE TABLE {object} (n Int64) ENGINE=Memory"),
        )
        .await;
        seed_dirty_baseline(provider, ledger, stage, &checksum).await;
        let (port, mut detected) = held_response_proxy(target, marker).await;
        let faulty = connect(port, password).await;
        let writer = {
            let backend = backend.clone();
            let migrations = migrations.clone();
            phase::spawn(async move {
                backend
                    .reconcile(
                        &faulty,
                        "app",
                        &migrations,
                        "binding",
                        ReconcileOutcome::Applied,
                    )
                    .await
            })
        };
        let close = tokio::time::timeout(Duration::from_secs(5), detected.recv())
            .await
            .unwrap()
            .expect("reconcile history insert dispatched");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if scalar(provider, &format!("SELECT toInt64(count()) AS value FROM {ledger} FINAL WHERE module='app' AND version=1")).await == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("history persisted before response loss");
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM {ledger}_meta FINAL WHERE module='app' AND dirty_stage='{stage}' AND barrier_version IS NULL")).await, 1);
        close.send(()).unwrap();
        assert!(
            matches!(writer.await.unwrap(), Err(Error::Database(error)) if error.kind() == ErrorKind::WriteOutcomeUnknown)
        );
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM system.processes WHERE query LIKE '%INSERT INTO `default`.`{ledger}`%' AND query NOT LIKE '%system.processes%'")).await, 0);
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='{ledger}_lock'")).await, 1);
        run_sql(provider, &format!("DROP TABLE {ledger}_lock")).await;
        seed_dirty_baseline(provider, ledger, stage, "forged").await;
        assert!(matches!(
            backend
                .reconcile(
                    provider,
                    "app",
                    &migrations,
                    "binding",
                    ReconcileOutcome::Applied
                )
                .await,
            Err(Error::History(HistoryReason::InconsistentRows))
        ));
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM {ledger}_meta FINAL WHERE module='app' AND dirty_checksum='forged' AND barrier_version IS NULL")).await, 1);
        seed_dirty_baseline(provider, ledger, stage, &checksum).await;
        assert!(matches!(
            backend
                .reconcile(
                    provider,
                    "app",
                    &migrations,
                    "binding",
                    ReconcileOutcome::NotApplied
                )
                .await,
            Err(Error::History(HistoryReason::InconsistentRows))
        ));
        let state = backend
            .reconcile(
                provider,
                "app",
                &migrations,
                "binding",
                ReconcileOutcome::Applied,
            )
            .await
            .unwrap();
        assert_eq!(state.applied_count(), 1);
        assert!(state.dirty().is_none());
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM {ledger}_meta FINAL WHERE module='app' AND barrier_version=1 AND dirty_version IS NULL")).await, 1);
        run_sql(
            provider,
            &format!("DROP TABLE {object}; DROP TABLE {ledger}; DROP TABLE {ledger}_meta"),
        )
        .await;
    }
}

async fn seed_dirty_baseline(
    provider: &ClickHousePoolProvider,
    ledger: &str,
    stage: &str,
    checksum: &str,
) {
    // Generated table names, enum stages and test-only checksums are controlled fixtures.
    run_sql(provider, &format!("INSERT INTO {ledger}_meta (module, fingerprint, barrier_version, barrier_checksum, dirty_version, dirty_checksum, dirty_direction, dirty_stage, revision) SELECT 'app', 'binding', CAST(NULL AS Nullable(UInt64)), CAST(NULL AS Nullable(String)), toUInt64(1), '{checksum}', 'up', '{stage}', max(revision)+1 FROM {ledger}_meta")).await;
}

#[allow(clippy::too_many_lines)]
async fn baseline_history_response_loss_recovers_barrier(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let backend = backend_for("nt_baseline_fault", Duration::from_secs(5));
    let baseline = migration(
        1,
        "baseline",
        "CREATE TABLE nt_baseline_app (n Int64) ENGINE=Memory",
        Some("DROP TABLE nt_baseline_app"),
    )
    .into_baseline();
    let migrations = Migrations::new(vec![baseline]).unwrap();
    backend
        .initialize_module(provider, "app", "binding", &[])
        .await
        .unwrap();
    let (port, mut detected) =
        held_response_proxy(target, b"INSERT INTO `default`.`nt_baseline_fault` (module").await;
    let faulty = connect(port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .up(&faulty, "app", &migrations, "binding", &["nt_baseline_app"])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(5), detected.recv())
        .await
        .unwrap()
        .expect("history insert dispatched");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if scalar(provider, "SELECT toInt64(count()) AS value FROM nt_baseline_fault FINAL WHERE module='app' AND version=1").await == 1 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("history row persisted before response loss");
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM nt_baseline_fault_meta FINAL WHERE module='app' AND dirty_stage='script_done' AND barrier_version IS NULL").await, 1);
    close.send(()).unwrap();
    let result = writer.await.unwrap().unwrap_err();
    assert!(
        matches!(result, Error::Database(error) if error.kind() == ErrorKind::WriteOutcomeUnknown)
    );
    // The only cooperating writer is joined above. Verify its server-side work has stopped.
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM system.processes WHERE query LIKE '%INSERT INTO `default`.`nt_baseline_fault`%' AND query NOT LIKE '%system.processes%'").await, 0);
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_baseline_fault_lock'").await, 1);
    run_sql(provider, "DROP TABLE nt_baseline_fault_lock").await;
    assert!(matches!(
        backend
            .up(
                provider,
                "app",
                &migrations,
                "binding",
                &["nt_baseline_app"]
            )
            .await,
        Err(Error::Dirty(_))
    ));
    assert!(matches!(
        backend
            .down(provider, "app", &migrations, "binding", 1, &[])
            .await,
        Err(Error::Dirty(_))
    ));
    let not_applied = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        not_applied,
        Error::History(HistoryReason::InconsistentRows)
    ));
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM nt_baseline_fault_meta FINAL WHERE module='app' AND dirty_stage='script_done' AND barrier_version IS NULL").await, 1);
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding",
            ReconcileOutcome::Applied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert!(state.dirty().is_none());
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM nt_baseline_fault_meta FINAL WHERE module='app' AND barrier_version=1 AND dirty_version IS NULL").await, 1);
    run_sql(provider, "DROP TABLE nt_baseline_app; DROP TABLE nt_baseline_fault; DROP TABLE nt_baseline_fault_meta").await;
}

async fn ordered_apply_and_down(provider: &ClickHousePoolProvider) {
    let table = "nt_ordered";
    let backend = backend_for(table, Duration::from_secs(5));
    assert!(backend.state(provider, "app").await.unwrap().is_none());
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() \
             AND name IN ('nt_ordered', 'nt_ordered_meta')",
        )
        .await,
        0,
        "state must not create ledger tables"
    );

    let migrations = Migrations::new(vec![
        migration(
            1,
            "create-users",
            "CREATE TABLE nt_users (id Int64) ENGINE=MergeTree ORDER BY id",
            Some("DROP TABLE nt_users"),
        ),
        migration(
            2,
            "create-orders",
            "CREATE TABLE nt_orders (id Int64) ENGINE=MergeTree ORDER BY id",
            Some("DROP TABLE nt_orders"),
        ),
    ])
    .unwrap();

    let state = backend
        .initialize_module(provider, "app", "binding-v1", &["nt_users", "nt_orders"])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);

    let state = backend
        .up(
            provider,
            "app",
            &migrations,
            "binding-v1",
            &["nt_users", "nt_orders"],
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2);
    assert_eq!(state.head().unwrap().0, 2);
    assert_eq!(scalar(provider, "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() AND name IN ('nt_users','nt_orders')").await, 2);

    let state = backend
        .up(
            provider,
            "app",
            &migrations,
            "binding-v1",
            &["nt_users", "nt_orders"],
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2, "repeat up is a no-op");

    let state = backend
        .down(provider, "app", &migrations, "binding-v1", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_orders'"
        )
        .await,
        0
    );

    let state = backend
        .down(provider, "app", &migrations, "binding-v1", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(backend.state(provider, "app").await.unwrap().is_some());
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_users; DROP TABLE IF EXISTS nt_orders",
    )
    .await;

    // Remove the ledger so the next test starts from a clean namespace.
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_ordered; DROP TABLE IF EXISTS nt_ordered_meta",
    )
    .await;
}

async fn dirty_marker_blocks_until_reconciled(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_dirty", Duration::from_secs(5));
    run_sql(provider, "DROP TABLE IF EXISTS nt_dirty_a").await;
    let migrations = Migrations::new(vec![
        migration(
            1,
            "create",
            "CREATE TABLE nt_dirty_a (id Int64) ENGINE=MergeTree ORDER BY id",
            Some("DROP TABLE nt_dirty_a"),
        ),
        migration(
            2,
            "partial",
            "INSERT INTO nt_dirty_a SELECT toInt64(throwIf(number = 10, 'stop')) FROM numbers(20)",
            Some("DROP TABLE nt_dirty_a"),
        ),
    ])
    .unwrap();

    backend
        .initialize_module(provider, "app", "binding-v1", &["nt_dirty_a"])
        .await
        .unwrap();

    let error = backend
        .up(provider, "app", &migrations, "binding-v1", &["nt_dirty_a"])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::FailedStage { .. }), "{error:?}");
    assert_eq!(scalar(provider, "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_dirty_lock'").await, 1);
    // No failed query may still be running before the operator removes the retained lock.
    assert_eq!(scalar(provider, "SELECT count() AS value FROM system.processes WHERE query LIKE '%INSERT INTO nt_dirty_a%' AND query NOT LIKE '%system.processes%'").await, 0);
    run_sql(provider, "DROP TABLE nt_dirty_lock").await;
    assert_eq!(
        scalar(provider, "SELECT count() AS value FROM nt_dirty_a").await,
        0,
        "operator must verify the failed insert had no effects before NotApplied"
    );

    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert!(
        state.dirty().is_some(),
        "a failed script must leave a marker"
    );
    assert!(
        backend
            .up(provider, "app", &migrations, "binding-v1", &["nt_dirty_a"])
            .await
            .unwrap_err()
            .to_string()
            .contains("dirty")
    );

    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding-v1",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert!(state.dirty().is_none());
    assert_eq!(
        scalar(provider, "SELECT count() AS value FROM nt_dirty_a").await,
        0
    );

    let state = backend
        .down(provider, "app", &migrations, "binding-v1", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_dirty_a; DROP TABLE IF EXISTS nt_dirty; DROP TABLE IF EXISTS nt_dirty_meta",
    )
    .await;
}

async fn lock_timeout_and_release(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_lock", Duration::from_millis(200));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_lock; DROP TABLE IF EXISTS nt_lock_meta; DROP TABLE IF EXISTS nt_lock_lock",
    )
    .await;
    run_sql(
        provider,
        "CREATE TABLE nt_lock_lock (owner String) ENGINE=Memory",
    )
    .await;
    run_sql(provider, "INSERT INTO nt_lock_lock VALUES ('sentinel')").await;
    let error = backend.state(provider, "app").await.unwrap_err();
    assert!(matches!(error, Error::LockOutcomeUnknown(_)), "{error:?}");
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM nt_lock_lock WHERE owner='sentinel'"
        )
        .await,
        1,
        "contender must not modify lock ownership"
    );
    run_sql(provider, "DROP TABLE nt_lock_lock").await;
    assert!(backend.state(provider, "app").await.unwrap().is_none());
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_lock_lock'"
        )
        .await,
        0
    );
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_lock; DROP TABLE IF EXISTS nt_lock_meta",
    )
    .await;
}

async fn concurrent_writers_serialize(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let backend = backend_for("nt_concurrent_ledger", Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_concurrent; DROP TABLE IF EXISTS nt_concurrent_ledger; \
         DROP TABLE IF EXISTS nt_concurrent_ledger_meta; DROP TABLE IF EXISTS nt_concurrent_ledger_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "slow",
        "CREATE TABLE nt_concurrent (id Int64) ENGINE=Memory",
        Some("DROP TABLE nt_concurrent"),
    )])
    .unwrap();
    let first_backend = backend.clone();
    let (port, mut detected) = gated_response_proxy(target, b"CREATE TABLE nt_concurrent ").await;
    let first_provider = connect(port, password).await;
    let first = phase::spawn(async move {
        first_backend
            .up(
                &first_provider,
                "app",
                &migrations,
                "binding-v1",
                &["nt_concurrent"],
            )
            .await
    });
    let resume = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .expect("winner must reach its script")
        .expect("proxy must remain alive");
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_concurrent_ledger_lock'",
        )
        .await,
        1,
        "winner must hold the lock before launching competing writers"
    );
    let competing = Migrations::new(vec![migration(
        1,
        "competing",
        "CREATE TABLE nt_competing (id Int64) ENGINE=Memory",
        Some("DROP TABLE nt_competing"),
    )])
    .unwrap();
    let error = backend
        .up(provider, "app", &competing, "binding-v1", &["nt_competing"])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LockOutcomeUnknown(_)), "{error:?}");
    let error = backend
        .down(provider, "app", &competing, "binding-v1", 1, &[])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::LockOutcomeUnknown(_)), "{error:?}");
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_competing'",
        )
        .await,
        0,
        "the losing writer must not execute its script"
    );
    resume.send(()).expect("winner response gate remains open");
    let state = first.await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_concurrent_ledger_lock'"
        )
        .await,
        0,
        "the first writer must release its lock"
    );
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_concurrent; DROP TABLE IF EXISTS nt_concurrent_ledger; \
         DROP TABLE IF EXISTS nt_concurrent_ledger_meta",
    )
    .await;
}

async fn non_atomic_database_is_rejected(provider: &ClickHousePoolProvider) {
    run_sql(provider, "CREATE DATABASE nt_non_atomic ENGINE=Memory").await;
    let backend =
        ClickHouseBackend::new("nt_non_atomic", "ledger", Duration::from_secs(2)).unwrap();
    assert!(matches!(
        backend.state(provider, "app").await,
        Err(Error::UnsupportedDatabase)
    ));
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables WHERE database='nt_non_atomic'"
        )
        .await,
        0,
        "unsupported database must not acquire a lock or create ledger tables"
    );
    run_sql(provider, "DROP DATABASE nt_non_atomic").await;
}

async fn uncertain_script_retains_lock(
    provider: &ClickHousePoolProvider,
    port: u16,
    password: &str,
) {
    let backend = backend_for("nt_uncertain_ledger", Duration::from_secs(2));
    let fast = connect_with_timeout(port, password, Duration::from_millis(200)).await;
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_uncertain_a; DROP TABLE IF EXISTS nt_uncertain_ledger; \
         DROP TABLE IF EXISTS nt_uncertain_ledger_meta; DROP TABLE IF EXISTS nt_uncertain_ledger_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "slow",
        "CREATE TABLE nt_uncertain_a ENGINE=Memory AS SELECT toInt64(sleepEachRow(0.02)) AS id FROM numbers(100)",
        Some("DROP TABLE nt_uncertain_a"),
    )])
    .unwrap();
    backend
        .initialize_module(&fast, "app", "binding-v1", &["nt_uncertain_a"])
        .await
        .unwrap();
    let error = backend
        .up(&fast, "app", &migrations, "binding-v1", &["nt_uncertain_a"])
        .await
        .unwrap_err();
    let failed_unknown = matches!(
        &error,
        Error::FailedStage { script, .. } if script.kind() == ErrorKind::WriteOutcomeUnknown
    );
    assert!(failed_unknown, "{error:?}");
    assert_eq!(
        scalar(
            provider,
            "SELECT count() AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_uncertain_ledger_lock'",
        )
        .await,
        1,
        "an uncertain script must retain its lock"
    );
    assert!(
        matches!(
            backend.state(provider, "app").await.unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "a second reader must not acquire the retained lock"
    );
    assert!(
        matches!(
            backend
                .reconcile(
                    provider,
                    "app",
                    &migrations,
                    "binding-v1",
                    ReconcileOutcome::Applied,
                )
                .await
                .unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "concurrent reconciliation must not acquire the retained lock"
    );

    let mut stopped = false;
    for _ in 0..100 {
        let running = scalar(
            provider,
            "SELECT count() AS value FROM system.processes \
             WHERE query LIKE '%sleepEachRow%' AND query NOT LIKE '%system.processes%'",
        )
        .await;
        if running == 0 {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(stopped, "the timed-out query must stop before lock removal");
    run_sql(provider, "DROP TABLE IF EXISTS nt_uncertain_a").await;
    run_sql(provider, "DROP TABLE nt_uncertain_ledger_lock").await;
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding-v1",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_none());
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_uncertain_a; DROP TABLE IF EXISTS nt_uncertain_ledger; \
         DROP TABLE IF EXISTS nt_uncertain_ledger_meta",
    )
    .await;
}

#[allow(clippy::too_many_lines)]
async fn cancelled_script_retains_lock(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_cancel_ledger", Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_cancel_a; DROP TABLE IF EXISTS nt_cancel_ledger; \
         DROP TABLE IF EXISTS nt_cancel_ledger_meta; DROP TABLE IF EXISTS nt_cancel_ledger_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "slow",
        "CREATE TABLE nt_cancel_a ENGINE=Memory AS SELECT toInt64(sleepEachRow(0.02)) AS nt_cancel_marker FROM numbers(100)",
        Some("DROP TABLE nt_cancel_a"),
    )])
    .unwrap();
    backend
        .initialize_module(provider, "app", "binding-v1", &["nt_cancel_a"])
        .await
        .unwrap();
    let task_backend = backend.clone();
    let task_provider = provider.clone();
    let task_migrations = migrations.clone();
    let task = phase::spawn(async move {
        task_backend
            .up(
                &task_provider,
                "app",
                &task_migrations,
                "binding-v1",
                &["nt_cancel_a"],
            )
            .await
    });
    let mut running = false;
    for _ in 0..100 {
        let count = scalar(
            provider,
            "SELECT count() AS value FROM system.processes \
             WHERE query LIKE '%nt_cancel_marker%' \
               AND query NOT LIKE '%system.processes%' AND query NOT LIKE '%KILL QUERY%'",
        )
        .await;
        if count > 0 {
            running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(running, "the migration query must be observable");
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(
        matches!(
            backend.state(provider, "app").await.unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "a killed script must retain its lock"
    );
    let mut stopped = false;
    for _ in 0..100 {
        let count = scalar(
            provider,
            "SELECT count() AS value FROM system.processes \
             WHERE query LIKE '%nt_cancel_marker%' \
               AND query NOT LIKE '%system.processes%' AND query NOT LIKE '%KILL QUERY%'",
        )
        .await;
        if count == 0 {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(stopped, "the killed query must stop before lock removal");
    run_sql(provider, "DROP TABLE IF EXISTS nt_cancel_a").await;
    run_sql(provider, "DROP TABLE nt_cancel_ledger_lock").await;
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding-v1",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_none());
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_cancel_a; DROP TABLE IF EXISTS nt_cancel_ledger; \
         DROP TABLE IF EXISTS nt_cancel_ledger_meta",
    )
    .await;
}

async fn stage_write_uncertainty_retains_lock(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let backend = backend_for("nt_stage_fault_ledger", Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_stage_fault_ledger; \
         DROP TABLE IF EXISTS nt_stage_fault_ledger_meta; \
         DROP TABLE IF EXISTS nt_stage_fault_ledger_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "create",
        "CREATE TABLE nt_stage_fault_a (n Int64) ENGINE=Memory",
        Some("DROP TABLE nt_stage_fault_a"),
    )])
    .unwrap();
    backend
        .initialize_module(provider, "app", "binding-v1", &[])
        .await
        .unwrap();
    let (fault_port, mut detected) = held_response_proxy(
        target,
        b"INSERT INTO `default`.`nt_stage_fault_ledger_meta`",
    )
    .await;
    let faulty = connect(fault_port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .up(&faulty, "app", &migrations, "binding-v1", &[])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(5), detected.recv())
        .await
        .unwrap()
        .expect("dirty marker dispatch");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if scalar(provider, "SELECT toInt64(count()) AS value FROM nt_stage_fault_ledger_meta FINAL WHERE module='app' AND dirty_version IS NOT NULL").await == 1 { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("dirty marker must be committed before cutting response");
    close.send(()).unwrap();
    let error = writer.await.unwrap().unwrap_err();
    assert!(
        matches!(&error, Error::Database(db) if db.kind() == ErrorKind::WriteOutcomeUnknown),
        "{error:?}"
    );
    assert_eq!(scalar(provider, "SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='nt_stage_fault_a'").await, 0);
    assert!(
        matches!(
            backend.state(provider, "app").await.unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "an uncertain stage write must retain the lock"
    );
    let mut stopped = false;
    for _ in 0..100 {
        let running = scalar(
            provider,
            "SELECT count() AS value FROM system.processes \
             WHERE query LIKE '%nt_stage_fault_ledger_meta%' \
               AND query NOT LIKE '%system.processes%'",
        )
        .await;
        if running == 0 {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        stopped,
        "the uncertain stage write must stop before lock removal"
    );
    run_sql(provider, "DROP TABLE nt_stage_fault_ledger_lock").await;
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding-v1",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_none());
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_stage_fault_ledger; \
         DROP TABLE IF EXISTS nt_stage_fault_ledger_meta",
    )
    .await;
}

async fn down_history_removal_is_restored(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_restore", Duration::from_secs(5));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_restore_a; DROP TABLE IF EXISTS nt_restore; \
         DROP TABLE IF EXISTS nt_restore_meta",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "restore",
        "CREATE TABLE nt_restore_a (id Int64) ENGINE=MergeTree ORDER BY id",
        Some("DROP TABLE nt_restore_a"),
    )])
    .unwrap();
    let checksum = migrations.as_slice()[0].checksum().to_owned();
    backend
        .initialize_module(provider, "app", "binding-v1", &["nt_restore_a"])
        .await
        .unwrap();
    backend
        .up(
            provider,
            "app",
            &migrations,
            "binding-v1",
            &["nt_restore_a"],
        )
        .await
        .unwrap();

    // Simulate a crash after the down script and history tombstone but before clearing the
    // dirty marker. The operator reports that the down did not take effect.
    run_sql(provider, "DROP TABLE nt_restore_a").await;
    run_sql(
        provider,
        "INSERT INTO nt_restore (module, version, checksum, revision, is_deleted) \
         VALUES ('app', 1, '', 1000, 1)",
    )
    .await;
    run_sql(
        provider,
        &format!(
            "INSERT INTO nt_restore_meta (module, fingerprint, barrier_version, barrier_checksum, \
             dirty_version, dirty_checksum, dirty_direction, dirty_stage, revision) \
             VALUES ('app', 'binding-v1', NULL, NULL, 1, '{checksum}', 'down', 'started', 999)"
        ),
    )
    .await;

    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_some());
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "binding-v1",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert!(state.dirty().is_none());
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_restore_a; DROP TABLE IF EXISTS nt_restore; \
         DROP TABLE IF EXISTS nt_restore_meta",
    )
    .await;
}

async fn untracked_objects_are_rejected(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_untracked", Duration::from_secs(5));
    run_sql(provider, "DROP TABLE IF EXISTS nt_untracked_a").await;
    run_sql(
        provider,
        "CREATE TABLE nt_untracked_a (id Int64) ENGINE=MergeTree ORDER BY id",
    )
    .await;
    let error = backend
        .initialize_module(provider, "app", "binding-v1", &["nt_untracked_a"])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::UntrackedObjects), "{error:?}");
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_untracked_a; DROP TABLE IF EXISTS nt_untracked; DROP TABLE IF EXISTS nt_untracked_meta",
    )
    .await;
}

async fn unowned_lookalikes_are_rejected(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_unowned", Duration::from_secs(5));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_unowned; DROP TABLE IF EXISTS nt_unowned_meta",
    )
    .await;
    run_sql(
        provider,
        "CREATE TABLE nt_unowned (x UInt8) ENGINE=MergeTree ORDER BY x; \
         CREATE TABLE nt_unowned_meta (x UInt8) ENGINE=MergeTree ORDER BY x",
    )
    .await;
    let error = backend.state(provider, "app").await.unwrap_err();
    assert!(
        matches!(error, Error::History(HistoryReason::UnownedLedger)),
        "{error:?}"
    );
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_unowned; DROP TABLE IF EXISTS nt_unowned_meta",
    )
    .await;
}

async fn partial_ledgers_are_rejected(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_partial", Duration::from_secs(5));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_partial; DROP TABLE IF EXISTS nt_partial_meta",
    )
    .await;
    run_sql(
        provider,
        "CREATE TABLE nt_partial (x UInt8) ENGINE=MergeTree ORDER BY x",
    )
    .await;
    let error = backend.state(provider, "app").await.unwrap_err();
    assert!(
        matches!(error, Error::History(HistoryReason::PartialLedger)),
        "{error:?}"
    );
    run_sql(provider, "DROP TABLE nt_partial").await;
}

async fn schema_drift_is_rejected(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_drift", Duration::from_secs(5));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_drift; DROP TABLE IF EXISTS nt_drift_meta",
    )
    .await;
    backend
        .initialize_module(provider, "app", "binding-v1", &[])
        .await
        .unwrap();
    run_sql(provider, "ALTER TABLE nt_drift ADD COLUMN extra String").await;
    let error = backend.state(provider, "app").await.unwrap_err();
    assert!(
        matches!(error, Error::History(HistoryReason::IncompatibleLedger)),
        "{error:?}"
    );
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_drift; DROP TABLE IF EXISTS nt_drift_meta",
    )
    .await;
}

async fn engine_and_key_drift_is_rejected_before_migration(provider: &ClickHousePoolProvider) {
    for (index, engine) in [
        "MergeTree ORDER BY (module, version)",
        "ReplacingMergeTree(is_deleted) ORDER BY (module, version)",
        "ReplacingMergeTree(revision, is_deleted) PRIMARY KEY (module, version) ORDER BY (module, version, revision)",
        "ReplacingMergeTree(revision, is_deleted) PARTITION BY module ORDER BY (module, version)",
        "ReplacingMergeTree(revision) ORDER BY (module, version)",
    ].iter().enumerate() {
        let history = format!("nt_catalog_{index}");
        let meta = format!("{history}_meta");
        let object = format!("nt_catalog_object_{index}");
        let backend = backend_for(&history, Duration::from_secs(5));
        backend.initialize_module(provider, "app", "binding", &[]).await.unwrap();
        let revision = scalar(provider, &format!("SELECT toInt64(max(revision)) AS value FROM {meta} WHERE module='app'")).await;
        run_sql(provider, &format!("DROP TABLE {history}")).await;
        run_sql(provider, &format!("CREATE TABLE {history} (module String, version UInt64, checksum String, revision UInt64, is_deleted UInt8) ENGINE={engine} COMMENT 'neotales-migration-clickhouse/ledger/v1/history'")).await;
        let migrations = Migrations::new(vec![migration(1, "create", &format!("CREATE TABLE {object} (n Int64) ENGINE=Memory"), Some(&format!("DROP TABLE {object}")))]).unwrap();
        let error = backend.up(provider, "app", &migrations, "binding", &[&object]).await.unwrap_err();
        assert!(matches!(error, Error::History(HistoryReason::IncompatibleLedger | HistoryReason::UnownedLedger)), "{engine}: {error:?}");
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='{object}'")).await, 0);
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM {meta} FINAL WHERE module='app' AND dirty_version IS NOT NULL")).await, 0);
        assert_eq!(scalar(provider, &format!("SELECT toInt64(max(revision)) AS value FROM {meta} WHERE module='app'")).await, revision, "drift rejection must not write metadata");
        run_sql(provider, &format!("DROP TABLE {history}; DROP TABLE {meta}")).await;
    }
    for (index, engine) in [
        "ReplacingMergeTree() ORDER BY module",
        "ReplacingMergeTree(revision) PARTITION BY module ORDER BY module",
    ]
    .iter()
    .enumerate()
    {
        let history = format!("nt_meta_catalog_{index}");
        let meta = format!("{history}_meta");
        let object = format!("nt_meta_object_{index}");
        let backend = backend_for(&history, Duration::from_secs(5));
        backend
            .initialize_module(provider, "app", "binding", &[])
            .await
            .unwrap();
        run_sql(provider, &format!("DROP TABLE {meta}")).await;
        run_sql(provider, &format!("CREATE TABLE {meta} (module String, fingerprint String, barrier_version Nullable(UInt64), barrier_checksum Nullable(String), dirty_version Nullable(UInt64), dirty_checksum Nullable(String), dirty_direction Nullable(String), dirty_stage Nullable(String), revision UInt64) ENGINE={engine} COMMENT 'neotales-migration-clickhouse/ledger/v1/meta'")).await;
        let migrations = Migrations::new(vec![migration(
            1,
            "create",
            &format!("CREATE TABLE {object} (n Int64) ENGINE=Memory"),
            Some(&format!("DROP TABLE {object}")),
        )])
        .unwrap();
        let error = backend
            .up(provider, "app", &migrations, "binding", &[&object])
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::History(HistoryReason::IncompatibleLedger)),
            "{engine}: {error:?}"
        );
        assert_eq!(scalar(provider, &format!("SELECT toInt64(count()) AS value FROM system.tables WHERE database=currentDatabase() AND name='{object}'")).await, 0);
        assert_eq!(
            scalar(
                provider,
                &format!("SELECT toInt64(count()) AS value FROM {history} FINAL")
            )
            .await,
            0
        );
        run_sql(
            provider,
            &format!("DROP TABLE {history}; DROP TABLE {meta}"),
        )
        .await;
    }
}

async fn flatten_is_unsupported(provider: &ClickHousePoolProvider) {
    let backend = backend_for("nt_flatten", Duration::from_secs(5));
    let old = vec![migration(
        1,
        "old",
        "CREATE TABLE nt_flatten_old (id Int64) ENGINE=MergeTree ORDER BY id",
        Some("DROP TABLE nt_flatten_old"),
    )];
    let baseline = migration(
        2,
        "baseline",
        "CREATE TABLE nt_flatten_new (id Int64) ENGINE=MergeTree ORDER BY id",
        Some("DROP TABLE nt_flatten_new"),
    )
    .into_baseline();
    let manifest = FlattenManifest::new(old, baseline).unwrap();
    let error = backend
        .flatten(provider, "app", &manifest, "binding-v1", &[])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::UnsupportedFlatten), "{error:?}");
}

// The owner has completed its lock query and is waiting between queries. A competing writer must
// not steal or delete the lock, and an empty `system.processes` result is not evidence that the
// owner is gone. Only the owner resumes and releases the lock.
async fn competing_writers_cannot_steal_a_lock_held_between_queries(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let table = "nt_between";
    let object = "nt_between_a";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_between_a; DROP TABLE IF EXISTS nt_between; \
         DROP TABLE IF EXISTS nt_between_meta; DROP TABLE IF EXISTS nt_between_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "between",
        "CREATE TABLE nt_between_a (n Int64) ENGINE=MergeTree ORDER BY n",
        Some("DROP TABLE nt_between_a"),
    )])
    .unwrap();
    let (port, mut detected) =
        gated_response_proxy(target, b"CREATE TABLE `default`.`nt_between_lock`").await;
    let gated = connect(port, password).await;
    let owner_backend = backend.clone();
    let owner_migrations = migrations.clone();
    let owner = phase::spawn(async move {
        owner_backend
            .up(&gated, "app", &owner_migrations, "binding", &[object])
            .await
    });
    let resume = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("the owner lock CREATE must reach the gate");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count = scalar(
                provider,
                "SELECT toInt64(count()) AS value FROM system.tables \
                 WHERE database=currentDatabase() AND name='nt_between_lock'",
            )
            .await;
            if count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the owner lock must exist while its response is withheld");
    // The owner's lock query has finished on the server. The empty result does not prove that
    // the owner stopped: it may send its next query at any time, and it still owns this lock.
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.processes \
             WHERE query LIKE '%nt_between_lock%' AND query NOT LIKE '%system.processes%'",
        )
        .await,
        0,
        "the owner must be between queries, not running one"
    );
    // No ledger exists yet because the owner is still inside acquisition.
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name IN ('nt_between', 'nt_between_meta')",
        )
        .await,
        0,
        "the lock is acquired before the ledger is bootstrapped"
    );
    assert_competing_writers_fail_closed(provider, &backend).await;
    // An operator does not remove the lock while the owner may still be alive. Release the gate.
    resume
        .send(())
        .expect("owner response gate must remain open");
    let state = owner.await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_between_lock'",
        )
        .await,
        0,
        "the owner must release its own lock"
    );
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_between_a; DROP TABLE IF EXISTS nt_between; \
         DROP TABLE IF EXISTS nt_between_meta",
    )
    .await;
}

async fn assert_competing_writers_fail_closed(
    provider: &ClickHousePoolProvider,
    backend: &ClickHouseBackend,
) {
    let competitor = Migrations::new(vec![migration(
        1,
        "competitor",
        "CREATE TABLE nt_between_competitor (n Int64) ENGINE=Memory",
        Some("DROP TABLE nt_between_competitor"),
    )])
    .unwrap();
    for (label, error) in [
        (
            "up",
            backend
                .up(
                    provider,
                    "app",
                    &competitor,
                    "binding",
                    &["nt_between_competitor"],
                )
                .await
                .unwrap_err(),
        ),
        (
            "down",
            backend
                .down(provider, "app", &competitor, "binding", 1, &[])
                .await
                .unwrap_err(),
        ),
        ("state", backend.state(provider, "app").await.unwrap_err()),
        (
            "reconcile",
            backend
                .reconcile(
                    provider,
                    "app",
                    &competitor,
                    "binding",
                    ReconcileOutcome::Applied,
                )
                .await
                .unwrap_err(),
        ),
    ] {
        assert!(
            matches!(error, Error::LockOutcomeUnknown(_)),
            "{label} must fail closed on the held lock: {error:?}"
        );
    }
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_between_competitor'",
        )
        .await,
        0,
        "a losing writer must not execute its script"
    );
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_between_lock'",
        )
        .await,
        1,
        "contention must never remove the owner's lock"
    );
}

// The clear write commits, but the client never receives its response. The server state is clean;
// the client must not report a dirty marker, and the retained lock must be removed only after an
// operator verifies that the writer stopped.
async fn clear_boundary_response_loss_is_not_dirty(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
) {
    let table = "nt_clear_boundary";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_clear_boundary_a; DROP TABLE IF EXISTS nt_clear_boundary_b; \
         DROP TABLE IF EXISTS nt_clear_boundary; DROP TABLE IF EXISTS nt_clear_boundary_meta; \
         DROP TABLE IF EXISTS nt_clear_boundary_lock",
    )
    .await;
    let first = Migrations::new(vec![clear_boundary_first_migration()]).unwrap();
    let migrations = Migrations::new(vec![
        clear_boundary_first_migration(),
        migration(
            2,
            "second",
            "CREATE TABLE nt_clear_boundary_b (n Int64) ENGINE=MergeTree ORDER BY n",
            Some("DROP TABLE nt_clear_boundary_b"),
        ),
    ])
    .unwrap();
    backend
        .initialize_module(provider, "app", "binding", &[])
        .await
        .unwrap();
    backend
        .up(provider, "app", &first, "binding", &[])
        .await
        .unwrap();
    // In this `up`, the third meta insert is clear_dirty: mark_dirty, mark_stage, clear_dirty.
    let (port, mut detected) = held_nth_response_proxy(
        target,
        b"INSERT INTO `default`.`nt_clear_boundary_meta` (module",
        3,
    )
    .await;
    let faulty = connect(port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .up(&faulty, "app", &migrations, "binding", &[])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("clear_dirty dispatch");
    wait_for_clear_and_history(provider).await;
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_clear_boundary_lock'",
        )
        .await,
        1,
        "an uncertain clear response must retain the lock"
    );
    close.send(()).unwrap();
    let error = writer.await.unwrap().unwrap_err();
    assert!(
        matches!(error, Error::Database(ref source) if source.kind() == ErrorKind::WriteOutcomeUnknown),
        "{error:?}"
    );
    assert_eq!(
        scalar(
            provider,
            "SELECT toInt64(count()) AS value FROM system.processes \
             WHERE query LIKE '%nt_clear_boundary_meta%' \
               AND query NOT LIKE '%system.processes%'",
        )
        .await,
        0,
        "the uncertain clear write must stop before lock removal"
    );
    run_sql(provider, "DROP TABLE nt_clear_boundary_lock").await;
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 2);
    assert!(state.dirty().is_none(), "the committed clear is not dirty");
    let state = backend
        .up(provider, "app", &migrations, "binding", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2, "repeat up must be a no-op");
    run_sql(
        provider,
        "DROP TABLE IF EXISTS nt_clear_boundary_a; DROP TABLE IF EXISTS nt_clear_boundary_b; \
         DROP TABLE IF EXISTS nt_clear_boundary; DROP TABLE IF EXISTS nt_clear_boundary_meta",
    )
    .await;
}

fn clear_boundary_first_migration() -> Migration {
    migration(
        1,
        "first",
        "CREATE TABLE nt_clear_boundary_a (n Int64) ENGINE=MergeTree ORDER BY n",
        Some("DROP TABLE nt_clear_boundary_a"),
    )
}

async fn wait_for_clear_and_history(provider: &ClickHousePoolProvider) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let clean = scalar(
                provider,
                "SELECT toInt64(count()) AS value FROM nt_clear_boundary_meta FINAL \
                 WHERE module='app' AND dirty_version IS NULL",
            )
            .await;
            let recorded = scalar(
                provider,
                "SELECT toInt64(count()) AS value FROM nt_clear_boundary FINAL \
                 WHERE module='app' AND version=2 AND is_deleted=0",
            )
            .await;
            if clean == 1 && recorded == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the clear write must commit while its response is held");
}

// A real server process restart while the dirty marker response is withheld. The ledger rows are
// in the container's writable layer and survive; the Memory lock table definition survives with
// its data lost, so it still blocks until an operator removes it.
#[allow(clippy::too_many_lines)]
async fn server_restart_at_dirty_marker(
    provider: ClickHousePoolProvider,
    container: &ContainerAsync<GenericImage>,
    password: &str,
) -> ClickHousePoolProvider {
    let table = "nt_restart_dirty";
    let object = "nt_restart_dirty_a";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_dirty_a; DROP TABLE IF EXISTS nt_restart_dirty; \
         DROP TABLE IF EXISTS nt_restart_dirty_meta; DROP TABLE IF EXISTS nt_restart_dirty_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "restart",
        "CREATE TABLE nt_restart_dirty_a (n Int64) ENGINE=MergeTree ORDER BY n",
        Some("DROP TABLE nt_restart_dirty_a"),
    )])
    .unwrap();
    backend
        .initialize_module(&provider, "app", "binding", &[])
        .await
        .unwrap();
    let target = bridge_address(bridge_ip(container).await);
    let (port, mut detected) = held_response_proxy(
        target,
        b"INSERT INTO `default`.`nt_restart_dirty_meta` (module",
    )
    .await;
    let faulty = connect(port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .up(&faulty, "app", &migrations, "binding", &[object])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("dirty marker dispatch");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let marker = scalar(
                &provider,
                "SELECT toInt64(count()) AS value FROM nt_restart_dirty_meta FINAL \
                 WHERE module='app' AND dirty_version=1 AND dirty_stage='started'",
            )
            .await;
            if marker == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dirty marker must be persisted before the restart");
    restart_clickhouse(container);
    drop(close);
    let outcome = tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("the writer must return after the server restart")
        .expect("writer task");
    assert!(
        matches!(outcome, Err(Error::Database(ref source)) if source.kind() == ErrorKind::WriteOutcomeUnknown),
        "{outcome:?}"
    );
    let provider = reconnect(container, password).await;
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_dirty'",
        )
        .await,
        1,
        "the ledger table must survive a server process restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_dirty_meta FINAL \
             WHERE module='app' AND dirty_version=1 AND dirty_stage='started'",
        )
        .await,
        1,
        "the dirty marker must survive a server process restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_dirty FINAL WHERE module='app'",
        )
        .await,
        0,
        "history must not record the interrupted script"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_dirty_a'",
        )
        .await,
        0,
        "the script must not have started"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_dirty_lock'",
        )
        .await,
        1,
        "the Atomic database keeps the Memory lock table definition across a restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_dirty_lock",
        )
        .await,
        0,
        "Memory table data does not survive a restart; the lock carries no owner identity"
    );
    assert!(
        matches!(
            backend.state(&provider, "app").await.unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "the retained lock must still block after the restart"
    );
    assert!(
        matches!(
            backend
                .up(&provider, "app", &migrations, "binding", &[object])
                .await
                .unwrap_err(),
            Error::LockOutcomeUnknown(_)
        ),
        "a restarted server must not bypass a retained lock"
    );
    // The writer is joined and the server was restarted, so its server work is gone. The fixture
    // checks no matching query remains; the README still requires an operator to establish this.
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.processes \
             WHERE query LIKE '%nt_restart_dirty_meta%' \
               AND query NOT LIKE '%system.processes%'",
        )
        .await,
        0
    );
    run_sql(&provider, "DROP TABLE nt_restart_dirty_lock").await;
    let state = backend.state(&provider, "app").await.unwrap().unwrap();
    assert_eq!(
        state.dirty().map(DirtyState::stage),
        Some(DirtyStage::Started)
    );
    assert_eq!(state.applied_count(), 0);
    let state = backend
        .reconcile(
            &provider,
            "app",
            &migrations,
            "binding",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_none());
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_dirty_a; DROP TABLE IF EXISTS nt_restart_dirty; \
         DROP TABLE IF EXISTS nt_restart_dirty_meta",
    )
    .await;
    provider
}

// A real restart after the baseline history row but before its barrier write. The row and the
// script_done marker survive; recovery must restore the barrier rather than discard the baseline.
#[allow(clippy::too_many_lines)]
async fn server_restart_after_history_before_barrier(
    provider: ClickHousePoolProvider,
    container: &ContainerAsync<GenericImage>,
    password: &str,
) -> ClickHousePoolProvider {
    let table = "nt_restart_history";
    let object = "nt_restart_history_app";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_history_app; DROP TABLE IF EXISTS nt_restart_history; \
         DROP TABLE IF EXISTS nt_restart_history_meta; DROP TABLE IF EXISTS nt_restart_history_lock",
    )
    .await;
    let baseline = migration(
        1,
        "baseline",
        "CREATE TABLE nt_restart_history_app (n Int64) ENGINE=MergeTree ORDER BY n",
        Some("DROP TABLE nt_restart_history_app"),
    )
    .into_baseline();
    let migrations = Migrations::new(vec![baseline]).unwrap();
    backend
        .initialize_module(&provider, "app", "binding", &[])
        .await
        .unwrap();
    let target = bridge_address(bridge_ip(container).await);
    let (port, mut detected) = held_response_proxy(
        target,
        b"INSERT INTO `default`.`nt_restart_history` (module",
    )
    .await;
    let faulty = connect(port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .up(&faulty, "app", &migrations, "binding", &[object])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("history insert dispatch");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let recorded = scalar(
                &provider,
                "SELECT toInt64(count()) AS value FROM nt_restart_history FINAL \
                 WHERE module='app' AND version=1 AND is_deleted=0",
            )
            .await;
            if recorded == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("history row must be persisted before the restart");
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_history_meta FINAL \
             WHERE module='app' AND dirty_stage='script_done' AND barrier_version IS NULL",
        )
        .await,
        1,
        "the barrier must not exist before the history response is released"
    );
    restart_clickhouse(container);
    drop(close);
    let outcome = tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("the writer must return after the server restart")
        .expect("writer task");
    assert!(
        matches!(outcome, Err(Error::Database(ref source)) if source.kind() == ErrorKind::WriteOutcomeUnknown),
        "{outcome:?}"
    );
    let provider = reconnect(container, password).await;
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_history FINAL \
             WHERE module='app' AND version=1 AND is_deleted=0",
        )
        .await,
        1,
        "the history row must survive a server process restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_history_meta FINAL \
             WHERE module='app' AND dirty_stage='script_done' AND barrier_version IS NULL",
        )
        .await,
        1,
        "the missing barrier must survive the restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_history_lock'",
        )
        .await,
        1
    );
    assert!(matches!(
        backend.state(&provider, "app").await.unwrap_err(),
        Error::LockOutcomeUnknown(_)
    ));
    run_sql(&provider, "DROP TABLE nt_restart_history_lock").await;
    let state = backend.state(&provider, "app").await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        state.dirty().map(DirtyState::stage),
        Some(DirtyStage::ScriptDone)
    );
    assert!(
        matches!(
            backend
                .reconcile(
                    &provider,
                    "app",
                    &migrations,
                    "binding",
                    ReconcileOutcome::NotApplied
                )
                .await,
            Err(Error::History(HistoryReason::InconsistentRows))
        ),
        "NotApplied must not discard a recorded baseline"
    );
    let state = backend
        .reconcile(
            &provider,
            "app",
            &migrations,
            "binding",
            ReconcileOutcome::Applied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert!(state.dirty().is_none());
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_history_meta FINAL \
             WHERE module='app' AND barrier_version=1 AND dirty_version IS NULL",
        )
        .await,
        1,
        "Applied must restore the barrier after the restart"
    );
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_history_app; DROP TABLE IF EXISTS nt_restart_history; \
         DROP TABLE IF EXISTS nt_restart_history_meta",
    )
    .await;
    provider
}

// A real restart after the down script and history tombstone, but before clear_dirty. The
// tombstone survives; recovery must keep the reverted history absent and clear the marker.
#[allow(clippy::too_many_lines)]
async fn server_restart_after_down_tombstone(
    provider: ClickHousePoolProvider,
    container: &ContainerAsync<GenericImage>,
    password: &str,
) -> ClickHousePoolProvider {
    let table = "nt_restart_down";
    let object = "nt_restart_down_a";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_down_a; DROP TABLE IF EXISTS nt_restart_down; \
         DROP TABLE IF EXISTS nt_restart_down_meta; DROP TABLE IF EXISTS nt_restart_down_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "down",
        "CREATE TABLE nt_restart_down_a (n Int64) ENGINE=MergeTree ORDER BY n",
        Some("DROP TABLE nt_restart_down_a"),
    )])
    .unwrap();
    backend
        .initialize_module(&provider, "app", "binding", &[])
        .await
        .unwrap();
    backend
        .up(&provider, "app", &migrations, "binding", &[object])
        .await
        .unwrap();
    let target = bridge_address(bridge_ip(container).await);
    let (port, mut detected) =
        held_response_proxy(target, b"INSERT INTO `default`.`nt_restart_down` (module").await;
    let faulty = connect(port, password).await;
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        phase::spawn(async move {
            backend
                .down(&faulty, "app", &migrations, "binding", 1, &[])
                .await
        })
    };
    let close = tokio::time::timeout(Duration::from_secs(10), detected.recv())
        .await
        .unwrap()
        .expect("down history tombstone dispatch");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let tombstone = scalar(
                &provider,
                "SELECT toInt64(countIf(is_deleted=1)) AS value FROM nt_restart_down \
                 WHERE module='app' AND version=1",
            )
            .await;
            if tombstone == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("down history tombstone must be persisted before the restart");
    let tombstone_revision = scalar(
        &provider,
        "SELECT toInt64(maxIf(revision, is_deleted=1)) AS value FROM nt_restart_down \
         WHERE module='app' AND version=1",
    )
    .await;
    assert!(tombstone_revision > 0);
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(max(revision)) AS value FROM nt_restart_down WHERE module='app'"
        )
        .await,
        tombstone_revision,
        "the latest physical history revision must be the deletion"
    );
    assert_eq!(
        scalar(&provider, "SELECT toInt64(count()) AS value FROM nt_restart_down FINAL WHERE module='app' AND version=1").await,
        0,
        "FINAL hides the latest deletion even before restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_down_meta FINAL \
             WHERE module='app' AND dirty_direction='down' AND dirty_stage='script_done'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_down_a'",
        )
        .await,
        0,
        "the down script must have taken effect before the restart"
    );
    restart_clickhouse(container);
    drop(close);
    let outcome = tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("the writer must return after the server restart")
        .expect("writer task");
    assert!(
        matches!(outcome, Err(Error::Database(ref source)) if source.kind() == ErrorKind::WriteOutcomeUnknown),
        "{outcome:?}"
    );
    let provider = reconnect(container, password).await;
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_down FINAL \
             WHERE module='app' AND version=1",
        )
        .await,
        0,
        "FINAL must continue to hide the deleted logical row after restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(maxIf(revision, is_deleted=1)) AS value FROM nt_restart_down \
             WHERE module='app' AND version=1",
        )
        .await,
        tombstone_revision,
        "the physical tombstone and revision survive the process restart"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(max(revision)) AS value FROM nt_restart_down WHERE module='app'"
        )
        .await,
        tombstone_revision,
        "the non-FINAL maximum remains available to the revision allocator"
    );
    run_sql(&provider, "OPTIMIZE TABLE nt_restart_down FINAL").await;
    assert_eq!(
        scalar(&provider, "SELECT toInt64(maxIf(revision, is_deleted=1)) AS value FROM nt_restart_down WHERE module='app'").await,
        tombstone_revision,
        "a normal FINAL merge retains the deletion revision"
    );
    assert_eq!(
        scalar(&provider, "SELECT toInt64(count()) AS value FROM nt_restart_down FINAL WHERE module='app' AND version=1").await,
        0,
        "merging does not resurrect the deleted row"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_down_lock'",
        )
        .await,
        1
    );
    assert!(matches!(
        backend.state(&provider, "app").await.unwrap_err(),
        Error::LockOutcomeUnknown(_)
    ));
    run_sql(&provider, "DROP TABLE nt_restart_down_lock").await;
    let state = backend.state(&provider, "app").await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 0);
    assert_eq!(
        state.dirty().map(DirtyState::stage),
        Some(DirtyStage::ScriptDone)
    );
    assert!(matches!(
        backend
            .up(&provider, "app", &migrations, "binding", &[object])
            .await,
        Err(Error::Dirty(_))
    ));
    assert!(matches!(
        backend
            .down(&provider, "app", &migrations, "binding", 1, &[])
            .await,
        Err(Error::Dirty(_))
    ));
    let state = backend
        .reconcile(
            &provider,
            "app",
            &migrations,
            "binding",
            ReconcileOutcome::Applied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(state.dirty().is_none());
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_down FINAL \
             WHERE module='app' AND version=1 AND is_deleted=0",
        )
        .await,
        0,
        "the reverted row must stay absent after recovery"
    );
    let state = backend
        .up(&provider, "app", &migrations, "binding", &[object])
        .await
        .unwrap();
    assert_eq!(
        state.applied_count(),
        1,
        "the module is usable after recovery"
    );
    assert!(
        scalar(
            &provider,
            "SELECT toInt64(max(revision)) AS value FROM nt_restart_down WHERE module='app'"
        )
        .await
            > tombstone_revision,
        "reapplication must use a revision newer than the retained tombstone"
    );
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_down_a; DROP TABLE IF EXISTS nt_restart_down; \
         DROP TABLE IF EXISTS nt_restart_down_meta",
    )
    .await;
    provider
}

// A real restart while the script is running. The dirty marker is still `started`, no history row
// exists, and the script's effect may be absent or partial. The fixture does not assert either
// reconciliation outcome, because the provider cannot verify arbitrary DDL.
#[allow(clippy::too_many_lines)]
async fn server_restart_mid_script(
    provider: ClickHousePoolProvider,
    container: &ContainerAsync<GenericImage>,
    password: &str,
) -> ClickHousePoolProvider {
    let table = "nt_restart_script";
    let object = "nt_restart_script_a";
    let backend = backend_for(table, Duration::from_secs(2));
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_script_a; DROP TABLE IF EXISTS nt_restart_script; \
         DROP TABLE IF EXISTS nt_restart_script_meta; DROP TABLE IF EXISTS nt_restart_script_lock",
    )
    .await;
    let migrations = Migrations::new(vec![migration(
        1,
        "slow",
        "CREATE TABLE nt_restart_script_a ENGINE=MergeTree ORDER BY id \
         AS SELECT toInt64(number) AS id FROM numbers(100000) WHERE sleepEachRow(0.01) = 0",
        Some("DROP TABLE nt_restart_script_a"),
    )])
    .unwrap();
    backend
        .initialize_module(&provider, "app", "binding", &[])
        .await
        .unwrap();
    let writer = {
        let backend = backend.clone();
        let migrations = migrations.clone();
        let writer_provider = provider.clone();
        phase::spawn(async move {
            backend
                .up(&writer_provider, "app", &migrations, "binding", &[object])
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let table_exists = scalar(
                &provider,
                "SELECT toInt64(count()) AS value FROM system.tables \
                 WHERE database=currentDatabase() AND name='nt_restart_script_a'",
            )
            .await;
            let running = scalar(
                &provider,
                "SELECT toInt64(count()) AS value FROM system.processes \
                 WHERE query LIKE '%nt_restart_script_a%' \
                   AND query NOT LIKE '%system.processes%'",
            )
            .await;
            if table_exists == 1 && running > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the slow script must be observable before the restart");
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_script_meta FINAL \
             WHERE module='app' AND dirty_stage='started' AND barrier_version IS NULL",
        )
        .await,
        1,
        "the marker must be started before the script runs"
    );
    restart_clickhouse(container);
    let outcome = tokio::time::timeout(Duration::from_secs(15), writer)
        .await
        .expect("the writer must return after the server restart")
        .expect("writer task");
    match outcome {
        Err(Error::FailedStage { script, .. }) => {
            assert_eq!(script.kind(), ErrorKind::WriteOutcomeUnknown);
        }
        other => panic!("expected the interrupted script to be unknown, got {other:?}"),
    }
    let provider = reconnect(container, password).await;
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_script_meta FINAL \
             WHERE module='app' AND dirty_stage='started'",
        )
        .await,
        1,
        "the started marker must survive a restart during the script"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM nt_restart_script FINAL WHERE module='app'",
        )
        .await,
        0,
        "history must not record an uncompleted script"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_script_a'",
        )
        .await,
        1,
        "the CREATE TABLE portion was committed before the restart"
    );
    let rows = scalar(
        &provider,
        "SELECT toInt64(count()) AS value FROM nt_restart_script_a",
    )
    .await;
    assert!(
        rows < 100_000,
        "the interrupted script must not have completed: {rows} rows"
    );
    assert_eq!(
        scalar(
            &provider,
            "SELECT toInt64(count()) AS value FROM system.tables \
             WHERE database=currentDatabase() AND name='nt_restart_script_lock'",
        )
        .await,
        1
    );
    assert!(matches!(
        backend.state(&provider, "app").await.unwrap_err(),
        Error::LockOutcomeUnknown(_)
    ));
    run_sql(&provider, "DROP TABLE nt_restart_script_lock").await;
    let state = backend.state(&provider, "app").await.unwrap().unwrap();
    assert_eq!(state.applied_count(), 0);
    assert_eq!(
        state.dirty().map(DirtyState::stage),
        Some(DirtyStage::Started)
    );
    assert!(matches!(
        backend
            .up(&provider, "app", &migrations, "binding", &[object])
            .await,
        Err(Error::Dirty(_))
    ));
    // The schema effect is partial: recovery must be an explicit operator decision. The fixture
    // only removes its objects; it does not claim that either outcome is correct.
    run_sql(
        &provider,
        "DROP TABLE IF EXISTS nt_restart_script_a; DROP TABLE IF EXISTS nt_restart_script; \
         DROP TABLE IF EXISTS nt_restart_script_meta",
    )
    .await;
    provider
}
