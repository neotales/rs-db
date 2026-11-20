//! Remote libSQL/Turso migration contract against a disposable Docker `sqld`.
//!
//! Requires Docker. The suite is `#[ignore]`d so a plain `cargo test` stays fixture-free;
//! `tests/live.sh` runs it explicitly. The container publishes no host port and is reached
//! through a loopback-only proxy, matching the provider's cleartext-loopback exception.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use neotales_dbapi::{QueryOptions, RowLimit, Session};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};
use neotales_migration::{FlattenManifest, Migration, Migrations};
use neotales_migration_libsql::{Error, LibsqlBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
const IMAGE: &str = "ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb276d9d3604e4a48ac4a999a2e267814732d57d7e94c04ba71482333a67";

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

fn start_sqld() -> Container {
    let output = Command::new("docker")
        .args([
            "run",
            "--pull",
            "never",
            "--detach",
            "--rm",
            "--network",
            "bridge",
            "--env",
            "SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080",
            IMAGE,
        ])
        .output()
        .expect("docker run");
    assert!(
        output.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let container = Container(String::from_utf8(output.stdout).unwrap().trim().to_owned());
    assert_ne!(container.0.len(), 0);
    let inspect = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.HostConfig.PublishAllPorts}}|{{json .HostConfig.PortBindings}}",
            &container.0,
        ])
        .output()
        .expect("docker inspect");
    assert!(inspect.status.success(), "docker inspect failed");
    let bindings = String::from_utf8(inspect.stdout).unwrap();
    assert!(
        matches!(bindings.trim(), "false|null" | "false|{}"),
        "container published ports"
    );
    container
}

async fn loopback_proxy(target: SocketAddr) -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("loopback proxy listener");
    let port = listener.local_addr().expect("proxy address").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    port
}

// Drop the response to one dispatched COMMIT. The ledger, not the transport
// failure, is the authority on whether the server committed.
struct CommitFault {
    port: u16,
    armed: Arc<AtomicBool>,
    tripped: Arc<AtomicBool>,
    response_seen: Arc<Notify>,
    release: Arc<Notify>,
}

async fn commit_loss_proxy(target: SocketAddr, hold_response: bool) -> CommitFault {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let armed = Arc::new(AtomicBool::new(false));
    let tripped = Arc::new(AtomicBool::new(false));
    let response_seen = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    tokio::spawn({
        let armed = armed.clone();
        let tripped = tripped.clone();
        let response_seen = response_seen.clone();
        let release = release.clone();
        async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    break;
                };
                let armed = armed.clone();
                let tripped = tripped.clone();
                let response_seen = response_seen.clone();
                let release = release.clone();
                tokio::spawn(async move {
                    let Ok(outbound) = tokio::net::TcpStream::connect(target).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = inbound.into_split();
                    let (mut server_read, mut server_write) = outbound.into_split();
                    let triggered = Arc::new(AtomicBool::new(false));
                    let trigger_writer = triggered.clone();
                    let requests = async move {
                        let mut buf = [0; 8192];
                        let mut tail = Vec::new();
                        while let Ok(n) = client_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            tail.extend_from_slice(&buf[..n]);
                            if tail.windows(6).any(|part| part == b"COMMIT")
                                && armed.swap(false, Ordering::SeqCst)
                            {
                                trigger_writer.store(true, Ordering::SeqCst);
                                tripped.store(true, Ordering::SeqCst);
                            }
                            if tail.len() > 5 {
                                tail.drain(..tail.len() - 5);
                            }
                            if server_write.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        let _ = server_write.shutdown().await;
                    };
                    let responses = async move {
                        let mut buf = [0; 8192];
                        while let Ok(n) = server_read.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if triggered.load(Ordering::SeqCst) {
                                response_seen.notify_one();
                                if hold_response {
                                    release.notified().await;
                                }
                                let _ = client_write.shutdown().await;
                                break;
                            }
                            if client_write.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    };
                    tokio::join!(requests, responses);
                });
            }
        }
    });
    CommitFault {
        port,
        armed,
        tripped,
        response_seen,
        release,
    }
}

fn bridge_address(container: &Container) -> SocketAddr {
    let output = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{with index .NetworkSettings.Networks \"bridge\"}}{{.IPAddress}}{{end}}",
            &container.0,
        ])
        .output()
        .expect("docker inspect");
    assert!(output.status.success(), "docker inspect failed");
    let ip: IpAddr = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .expect("container bridge address");
    SocketAddr::new(ip, 8080)
}

async fn connect(port: u16) -> LibsqlPoolProvider {
    let mut last = neotales_dbapi::ErrorKind::Connection;
    for _ in 0..30 {
        match LibsqlPoolProvider::connect(
            LibsqlConfig::plaintext_loopback(format!("http://127.0.0.1:{port}"), "")
                .expect("loopback configuration")
                .max_connections(4)
                .acquire_timeout(Duration::from_secs(5)),
        )
        .await
        {
            Ok(provider) => return provider,
            Err(error) => {
                last = error.kind();
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("disposable sqld did not accept connections: {last:?}");
}

fn m(version: u64, name: &str, table: &str) -> Migration {
    Migration::new(
        version,
        name,
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
        Some(format!("DROP TABLE {table}")),
    )
    .unwrap()
}

fn definitions(items: Vec<Migration>) -> Migrations {
    Migrations::new(items).unwrap()
}

async fn script(provider: &LibsqlPoolProvider, sql: &str) {
    let mut session = provider.acquire_libsql_session().await.unwrap();
    session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .unwrap();
}

async fn value_i64(provider: &LibsqlPoolProvider, sql: &str) -> i64 {
    let mut session = provider.acquire_libsql_session().await.unwrap();
    session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap()[0]
        .get("value")
        .unwrap()
}

async fn table_count(provider: &LibsqlPoolProvider, names: &str) -> i64 {
    value_i64(
        provider,
        &format!(
            "SELECT count(*) AS value FROM sqlite_master WHERE type='table' AND name IN ({names})"
        ),
    )
    .await
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn lost_commit_response_requires_head_reconciliation() {
    let container = start_sqld();
    let address = bridge_address(&container);
    let normal_port = loopback_proxy(address).await;
    let provider = connect(normal_port).await;
    let backend = LibsqlBackend::new("reconcile_log").unwrap();
    let migrations = definitions(vec![
        m(1, "first", "reconcile_first"),
        m(2, "second", "reconcile_second"),
    ]);
    let objects = ["reconcile_first", "reconcile_second"];
    backend
        .up(&provider, "reconcile", &migrations, "fp", &objects)
        .await
        .unwrap();
    let before = backend
        .inspect(&provider, "reconcile", &migrations, "fp")
        .await
        .unwrap()
        .unwrap();
    let mut expected_after = before.clone();
    expected_after.applied.pop();

    let fault = commit_loss_proxy(address, false).await;
    let fault_provider = connect(fault.port).await;
    fault.armed.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        backend.down(&fault_provider, "reconcile", &migrations, "fp", 1, &objects),
    )
    .await
    .expect("fault-injected down timed out");
    assert!(matches!(result, Err(Error::Database)));
    assert!(
        fault.tripped.load(Ordering::SeqCst),
        "the commit response fault did not fire"
    );
    assert_eq!(
        backend
            .inspect(&provider, "reconcile", &migrations, "fp")
            .await
            .unwrap(),
        Some(expected_after)
    );
    assert!(matches!(
        backend
            .inspect(&provider, "reconcile", &migrations, "wrong-fingerprint")
            .await,
        Err(Error::History)
    ));

    // The response was lost, but this particular commit reached the server.
    // Confirm both ledger head/checksum and managed objects before any retry.
    let expected_checksum = migrations.as_slice()[0].checksum();
    assert_eq!(
        value_i64(&provider, &format!(
            "SELECT count(*) AS value FROM main.reconcile_log_modules WHERE module='reconcile' AND head_version='1' AND head_checksum='{expected_checksum}'"
        )).await,
        1
    );
    assert_eq!(
        value_i64(&provider, &format!(
            "SELECT count(*) AS value FROM main.reconcile_log WHERE module='reconcile' AND version=1 AND checksum='{expected_checksum}'"
        )).await,
        1
    );
    assert_eq!(
        value_i64(&provider, "SELECT count(*) AS value FROM main.reconcile_log WHERE module='reconcile' AND version=2").await,
        0
    );
    assert_eq!(
        table_count(&provider, "'reconcile_first','reconcile_second'").await,
        1
    );
    assert_eq!(table_count(&provider, "'reconcile_first'").await, 1);
    script(
        &provider,
        "UPDATE main.reconcile_log_modules SET applied_count=999 WHERE module='reconcile'",
    )
    .await;
    assert!(matches!(
        backend
            .inspect(&provider, "reconcile", &migrations, "fp")
            .await,
        Err(Error::History)
    ));
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn cancelled_commit_requires_full_history_reconciliation() {
    let container = start_sqld();
    let address = bridge_address(&container);
    let provider = connect(loopback_proxy(address).await).await;
    let backend = LibsqlBackend::new("cancel_log").unwrap();
    let migrations = definitions(vec![
        m(1, "first", "cancel_first"),
        m(2, "second", "cancel_second"),
    ]);
    let objects = ["cancel_first", "cancel_second"];
    assert!(
        backend
            .inspect(&provider, "cancel", &migrations, "fp")
            .await
            .unwrap()
            .is_none()
    );
    backend
        .up(&provider, "cancel", &migrations, "fp", &objects)
        .await
        .unwrap();
    let before = backend
        .inspect(&provider, "cancel", &migrations, "fp")
        .await
        .unwrap()
        .unwrap();
    let mut expected_after = before.clone();
    expected_after.applied.pop();

    let fault = commit_loss_proxy(address, true).await;
    let fault_provider = connect(fault.port).await;
    fault.armed.store(true, Ordering::SeqCst);
    let operation = tokio::spawn({
        let backend = backend.clone();
        let migrations = migrations.clone();
        async move {
            backend
                .down(&fault_provider, "cancel", &migrations, "fp", 1, &objects)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(15), fault.response_seen.notified())
        .await
        .expect("commit response not reached");
    assert!(fault.tripped.load(Ordering::SeqCst));
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    fault.release.notify_one();

    let actual = backend
        .inspect(&provider, "cancel", &migrations, "fp")
        .await
        .unwrap();
    assert_eq!(actual, Some(expected_after));
    assert_ne!(actual, Some(before));
    assert_eq!(
        table_count(&provider, "'cancel_first','cancel_second'").await,
        1
    );
    assert_eq!(table_count(&provider, "'cancel_first'").await, 1);
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn existing_ledger_triggers_are_not_an_ownership_check() {
    let container = start_sqld();
    let provider = connect(loopback_proxy(bridge_address(&container)).await).await;
    let backend = LibsqlBackend::new("trusted_log").unwrap();
    let first = definitions(vec![m(1, "first", "owned_first")]);
    backend
        .up(&provider, "first", &first, "fp", &["owned_first"])
        .await
        .unwrap();
    script(&provider, "CREATE TABLE trigger_effect (value INTEGER); CREATE TRIGGER ledger_side_effect AFTER INSERT ON trusted_log BEGIN INSERT INTO trigger_effect VALUES (1); END;").await;
    let second = definitions(vec![m(1, "second", "owned_second")]);
    backend
        .up(&provider, "second", &second, "fp", &["owned_second"])
        .await
        .unwrap();
    assert_eq!(
        value_i64(&provider, "SELECT count(*) AS value FROM trigger_effect").await,
        1
    );
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)]
async fn sqld_migration_contract() {
    let container = start_sqld();
    let port = loopback_proxy(bridge_address(&container)).await;
    let provider = connect(port).await;
    let backend = LibsqlBackend::new("migration_log").unwrap();
    script(&provider, "CREATE TABLE partial_ledger (module TEXT)").await;
    let partial = LibsqlBackend::new("partial_ledger").unwrap();
    let empty = definitions(vec![]);
    assert!(matches!(
        partial.inspect(&provider, "partial", &empty, "fp").await,
        Err(Error::History)
    ));

    // Fresh, repeatable up; binding drift; down.
    let ms = definitions(vec![m(1, "one", "one"), m(2, "two", "two")]);
    backend
        .up(&provider, "main", &ms, "fp", &["one", "two"])
        .await
        .unwrap();
    backend
        .up(&provider, "main", &ms, "fp", &["one", "two"])
        .await
        .unwrap();
    assert_eq!(table_count(&provider, "'one','two'").await, 2);
    assert!(matches!(
        backend.up(&provider, "main", &ms, "changed", &[]).await,
        Err(Error::History)
    ));
    backend
        .down(&provider, "main", &ms, "fp", 1, &["one", "two"])
        .await
        .unwrap();
    assert_eq!(table_count(&provider, "'two'").await, 0);

    // Regression: a committed `down(count = 1)` is not idempotent. Repeating it
    // after an unknown commit removes the next migration as well. The recovery
    // rule requires comparing the persisted head with the expected pre-down head
    // before another down is issued.
    let repeat = definitions(vec![
        m(1, "repeat_one", "repeat_one"),
        m(2, "repeat_two", "repeat_two"),
    ]);
    backend
        .up(
            &provider,
            "repeat",
            &repeat,
            "fp",
            &["repeat_one", "repeat_two"],
        )
        .await
        .unwrap();
    backend
        .down(
            &provider,
            "repeat",
            &repeat,
            "fp",
            1,
            &["repeat_one", "repeat_two"],
        )
        .await
        .unwrap();
    assert_eq!(table_count(&provider, "'repeat_two'").await, 0);
    assert_eq!(table_count(&provider, "'repeat_one'").await, 1);
    backend
        .down(
            &provider,
            "repeat",
            &repeat,
            "fp",
            1,
            &["repeat_one", "repeat_two"],
        )
        .await
        .unwrap();
    assert_eq!(
        table_count(&provider, "'repeat_one','repeat_two'").await,
        0,
        "a blind retry of down(count = 1) must be shown to undo two migrations"
    );

    // A fresh module must not adopt pre-existing objects.
    script(
        &provider,
        "CREATE TABLE application_data (value TEXT); INSERT INTO application_data VALUES ('preserve');",
    )
    .await;
    let one = definitions(vec![m(1, "one", "managed_one")]);
    assert!(matches!(
        backend
            .up(&provider, "untracked", &one, "fp", &["application_data"])
            .await,
        Err(Error::UntrackedObjects)
    ));
    let preserved = {
        let mut session = provider.acquire_libsql_session().await.unwrap();
        session
            .fetch_all_async(
                "SELECT value FROM application_data",
                QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
            )
            .await
            .unwrap()[0]
            .get::<String>("value")
            .unwrap()
    };
    assert_eq!(preserved, "preserve");
    assert!(LibsqlBackend::new("log; DROP TABLE application_data").is_err());

    // Checksum drift is detected.
    backend
        .up(&provider, "drift", &one, "fp", &["managed_one"])
        .await
        .unwrap();
    script(
        &provider,
        "UPDATE migration_log SET checksum='tampered' WHERE module='drift'",
    )
    .await;
    assert!(matches!(
        backend.up(&provider, "drift", &one, "fp", &[]).await,
        Err(Error::History)
    ));

    // A matching head alone is insufficient: an earlier row may have drifted.
    let full = definitions(vec![
        m(1, "full_one", "full_one"),
        m(2, "full_two", "full_two"),
    ]);
    backend
        .up(&provider, "full", &full, "fp", &["full_one", "full_two"])
        .await
        .unwrap();
    script(
        &provider,
        "UPDATE migration_log SET checksum='tampered' WHERE module='full' AND version=1",
    )
    .await;
    assert_eq!(value_i64(&provider, "SELECT count(*) AS value FROM migration_log_modules WHERE module='full' AND head_version='2'").await, 1);
    assert!(matches!(
        backend.inspect(&provider, "full", &full, "fp").await,
        Err(Error::History)
    ));

    // down plans every script before executing, and a failed script rolls back.
    let no_down = definitions(vec![
        m(1, "one", "down_one"),
        Migration::new(2, "no-down", "CREATE TABLE down_two (id INTEGER)", None).unwrap(),
    ]);
    backend
        .up(
            &provider,
            "no_down",
            &no_down,
            "fp",
            &["down_one", "down_two"],
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down(
                &provider,
                "no_down",
                &no_down,
                "fp",
                2,
                &["down_one", "down_two"]
            )
            .await,
        Err(Error::Core)
    ));
    assert_eq!(table_count(&provider, "'down_one','down_two'").await, 2);

    let bad = definitions(vec![
        m(1, "good", "failed_good"),
        Migration::new(2, "bad", "INVALID SQL", Some("SELECT 1".into())).unwrap(),
    ]);
    assert!(matches!(
        backend.up(&provider, "failed", &bad, "fp", &[]).await,
        Err(Error::Database)
    ));
    assert_eq!(table_count(&provider, "'failed_good'").await, 0);

    // Flatten replaces exact complete history with the baseline marker and does not execute it.
    let old = vec![m(1, "old", "old")];
    let baseline = m(10, "baseline", "replacement").into_baseline();
    let manifest = FlattenManifest::new(old.clone(), baseline.clone()).unwrap();
    assert!(matches!(
        backend
            .flatten(&provider, "fresh", &manifest, "fp".into(), &[])
            .await,
        Err(Error::Core)
    ));
    backend
        .up(
            &provider,
            "complete",
            &definitions(old.clone()),
            "fp",
            &["old"],
        )
        .await
        .unwrap();
    backend
        .flatten(&provider, "complete", &manifest, "fp".into(), &["old"])
        .await
        .unwrap();
    assert_eq!(table_count(&provider, "'replacement'").await, 0);
    backend
        .up(
            &provider,
            "fresh",
            &definitions(vec![baseline.clone()]),
            "fp",
            &["replacement"],
        )
        .await
        .unwrap();
    assert_eq!(table_count(&provider, "'replacement'").await, 1);
    let baseline_set = definitions(vec![baseline, m(11, "after", "after")]);
    assert!(matches!(
        backend
            .down(
                &provider,
                "fresh",
                &baseline_set,
                "fp",
                1,
                &["replacement", "after"]
            )
            .await,
        Err(Error::Core)
    ));

    let partial = vec![m(1, "old", "partial"), m(2, "later", "later")];
    let partial_manifest =
        FlattenManifest::new(partial.clone(), m(10, "base", "new").into_baseline()).unwrap();
    backend
        .up(
            &provider,
            "partial",
            &definitions(vec![partial[0].clone()]),
            "fp",
            &["partial"],
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .flatten(
                &provider,
                "partial",
                &partial_manifest,
                "fp".into(),
                &["partial", "later"]
            )
            .await,
        Err(Error::Core)
    ));
}
