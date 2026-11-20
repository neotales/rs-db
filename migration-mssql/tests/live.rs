//! Explicit live lane: starts one digest-pinned, portless SQL Server 2022
//! testcontainer with a CSPRNG password and connects through a loopback proxy.
//! `tests/live.sh` selects this ignored test.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    time::Duration,
};

use neotales_dbapi::{ErrorKind, Pool, QueryOptions, Row, RowLimit};
use neotales_dbapi_mssql::{MssqlConfig, MssqlPoolProvider, MssqlTls};
use neotales_migration::{Migration, Migrations};
use neotales_migration_mssql::{Error, HistoryReason, SqlServerBackend};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

const IMAGE: &str = "mcr.microsoft.com/mssql/server";
const TAG: &str =
    "2022-latest@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f";

fn disposable_password() -> String {
    use std::fmt::Write as _;

    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes).expect("OS cryptographic random");
    let mut password = String::with_capacity(4 + bytes.len() * 2);
    password.push_str("Nt-");
    for byte in bytes {
        write!(&mut password, "{byte:02x}").expect("write to String");
    }
    password.push_str("!A");
    password
}

async fn start_mssql(password: &str) -> ContainerAsync<GenericImage> {
    GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::log(LogWaitStrategy::stdout_or_stderr(
            "SQL Server is now ready for client connections",
        )))
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", password)
        .with_env_var("MSSQL_PID", "Developer")
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(std::collections::HashMap::new());
        })
        .start()
        .await
        .expect("SQL Server container")
}

fn assert_no_published_ports(container: &ContainerAsync<GenericImage>) {
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
    let mapped = Command::new("docker")
        .args(["port", container.id(), "1433"])
        .output()
        .expect("docker port");
    assert!(mapped.stdout.is_empty(), "1433 must have no host mapping");
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
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    port
}

fn config(port: u16, password: &str) -> MssqlConfig {
    MssqlConfig::new("127.0.0.1", port, "sa", password, "master")
        .tls(MssqlTls::Insecure)
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
}

async fn connect(port: u16, password: &str) -> MssqlPoolProvider {
    let mut last = neotales_dbapi::ErrorKind::Connection;
    for _ in 0..60 {
        match MssqlPoolProvider::connect(config(port, password)).await {
            Ok(provider) => return provider,
            Err(error) => {
                last = error.kind();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!("disposable SQL Server did not accept connections: {last:?}");
}

async fn query(provider: &MssqlPoolProvider, sql: &str) -> Vec<Row> {
    provider
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(64, 65_536).unwrap()),
        )
        .await
        .unwrap_or_else(|error| panic!("query failed for {sql:?}: {error:?}"))
}

async fn script(provider: &MssqlPoolProvider, sql: &str) {
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .unwrap_or_else(|error| panic!("script failed for {sql:?}: {error:?}"));
}

async fn exists(provider: &MssqlPoolProvider, object: &str) -> bool {
    query(provider, &format!("SELECT OBJECT_ID(N'{object}') AS id")).await[0]
        .get::<Option<i64>>("id")
        .unwrap()
        .is_some()
}

fn table(name: &str) -> String {
    format!("mig_{}_{name}", std::process::id())
}

fn backend(table: &str) -> SqlServerBackend {
    SqlServerBackend::new("dbo", table, Duration::from_secs(10)).unwrap()
}

fn definitions(entries: &[(u64, &str, String, String)]) -> Migrations {
    Migrations::new(
        entries
            .iter()
            .map(|(version, name, up, down)| {
                Migration::new(*version, *name, up.clone(), Some(down.clone())).unwrap()
            })
            .collect(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)]
async fn mssql_migration_contract() {
    let password = disposable_password();
    let container = start_mssql(&password).await;
    assert_no_published_ports(&container);
    let ip: IpAddr = container
        .get_bridge_ip_address()
        .await
        .expect("bridge address");
    let port = loopback_proxy(SocketAddr::new(ip, 1433)).await;
    let provider = connect(port, &password).await;

    multi_go_success_and_lexical_go(&provider).await;
    later_failure_rolls_back_every_batch(&provider).await;
    update_scripts_apply_and_roll_back(&provider).await;
    drift_is_rejected_without_effects(&provider).await;
    concurrent_up_serializes_on_transaction_lock(&provider).await;
    cancellation_releases_lock_without_partial_state(&provider).await;
    preflight_prevents_sequence_side_effect(&provider).await;
    modified_ledger_default_is_rejected(&provider).await;
    barrier_and_unregistered_down_are_preflighted(&provider).await;
    unowned_partial_and_binding_mismatch(&provider).await;

    drop(provider);
}

async fn multi_go_success_and_lexical_go(provider: &MssqlPoolProvider) {
    let table = table("go");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let up = format!(
        "CREATE TABLE dbo.{app} (id bigint PRIMARY KEY, note nvarchar(64) NOT NULL)\nGO\n\
         INSERT INTO dbo.{app} (id, note) VALUES (1, N'line one\nGO\nline two')\nGO\n\
         CREATE INDEX ix_note ON dbo.{app} (note)"
    );
    let migrations = definitions(&[(1, "app", up, format!("DROP TABLE dbo.{app}"))]);
    let state = backend
        .up(provider, "app", &migrations, "fp-v1", &[])
        .await
        .unwrap();
    assert_eq!(state.module(), "app");
    assert_eq!(state.fingerprint(), "fp-v1");
    assert_eq!(state.applied_count(), 1);
    assert_eq!(state.head().map(|(version, _)| version), Some(1));
    assert_eq!(
        query(provider, &format!("SELECT COUNT(*) AS n FROM dbo.{app}")).await[0]
            .get::<i64>("n")
            .unwrap(),
        1
    );
    let note = query(provider, &format!("SELECT note FROM dbo.{app}")).await[0]
        .get::<String>("note")
        .unwrap();
    assert!(
        note.contains("GO"),
        "GO inside a string is not a batch directive"
    );
    assert_eq!(
        query(
            provider,
            &format!(
                "SELECT COUNT(*) AS n FROM sys.indexes WHERE name = N'ix_note' AND object_id = OBJECT_ID(N'dbo.{app}')"
            )
        )
        .await[0]
            .get::<i64>("n")
            .unwrap(),
        1
    );
    // Repeated up with identical definitions is a no-op.
    assert_eq!(
        backend
            .up(provider, "app", &migrations, "fp-v1", &[])
            .await
            .unwrap(),
        state
    );
}

async fn later_failure_rolls_back_every_batch(provider: &MssqlPoolProvider) {
    let table = table("rollback");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let migrations = definitions(&[
        (
            1,
            "create",
            format!("CREATE TABLE dbo.{app} (id bigint)\nGO\nINSERT INTO dbo.{app} VALUES (1)"),
            format!("DROP TABLE dbo.{app}"),
        ),
        (
            2,
            "fail",
            format!("INSERT INTO dbo.{app} VALUES (2)\nGO\nSELECT * FROM dbo.mig_missing_relation"),
            "DELETE FROM dbo.mig_missing_relation".to_owned(),
        ),
    ]);
    assert!(matches!(
        backend.up(provider, "app", &migrations, "fp-v1", &[]).await,
        Err(Error::Database(_))
    ));
    // Bootstrap, both scripts and both ledger writes belong to the failed transaction.
    assert!(!exists(provider, &format!("dbo.{app}")).await);
    assert!(!exists(provider, &format!("dbo.{table}")).await);
    assert!(!exists(provider, &format!("dbo.{table}_modules")).await);
    let state = backend
        .initialize_module(provider, "app", "fp-v1", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert_eq!(state.head(), None);
}

async fn update_scripts_apply_and_roll_back(provider: &MssqlPoolProvider) {
    let table = table("update");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let seed_sql = format!(
        concat!(
            "CREATE TABLE dbo.{app} (id bigint PRIMARY KEY, note nvarchar(64) NULL, flag bit NOT NULL);\r",
            "GO\r",
            "INSERT INTO dbo.{app} (id, note, flag) VALUES (1, N'seed', 0);\r",
            "GO\r",
            "UPDATE dbo.{app} SET note = N'updated', flag = 1 -- comment\r",
            "WHERE id = 1;"
        ),
        app = app
    );
    let first = definitions(&[(1, "seed", seed_sql.clone(), format!("DROP TABLE dbo.{app}"))]);
    backend
        .up(provider, "app", &first, "fp-v1", &[])
        .await
        .unwrap();
    let row = || async {
        let rows = query(provider, &format!("SELECT note, flag FROM dbo.{app}")).await;
        (
            rows[0].get::<Option<String>>("note").unwrap(),
            rows[0].get::<bool>("flag").unwrap(),
        )
    };
    assert_eq!(row().await, (Some("updated".to_owned()), true));

    let changed = definitions(&[
        (1, "seed", seed_sql.clone(), format!("DROP TABLE dbo.{app}")),
        (
            2,
            "fail-after-update",
            format!(
                "UPDATE dbo.{app} SET note = N'leaked' WHERE id = 1; \
                 SELECT * FROM dbo.mig_missing_relation"
            ),
            format!("UPDATE dbo.{app} SET note = N'down' WHERE id = 1"),
        ),
    ]);
    assert!(matches!(
        backend.up(provider, "app", &changed, "fp-v1", &[]).await,
        Err(Error::Database(_))
    ));
    assert_eq!(
        row().await,
        (Some("updated".to_owned()), true),
        "a later failure must roll back the update"
    );

    let unsafe_set = definitions(&[
        (1, "seed", seed_sql, format!("DROP TABLE dbo.{app}")),
        (
            2,
            "unsafe",
            format!("UPDATE dbo.{app} SET note = N'leaked' -- comment\rSET NOEXEC ON"),
            format!("UPDATE dbo.{app} SET note = N'down' WHERE id = 1"),
        ),
    ]);
    match backend.up(provider, "app", &unsafe_set, "fp-v1", &[]).await {
        Err(Error::Database(error)) => assert_eq!(error.kind(), ErrorKind::Unsupported),
        other => panic!("a session SET batch must fail preflight, got {other:?}"),
    }
    assert_eq!(
        row().await,
        (Some("updated".to_owned()), true),
        "the unsafe batch must not run"
    );
}

async fn drift_is_rejected_without_effects(provider: &MssqlPoolProvider) {
    let table = table("drift");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let first = definitions(&[(
        1,
        "app",
        format!("CREATE TABLE dbo.{app} (id bigint)"),
        format!("DROP TABLE dbo.{app}"),
    )]);
    let state = backend
        .up(provider, "app", &first, "fp-v1", &[])
        .await
        .unwrap();
    let changed = definitions(&[(
        1,
        "app",
        format!("CREATE TABLE dbo.{app} (id bigint, changed bigint)"),
        format!("DROP TABLE dbo.{app}"),
    )]);
    assert!(matches!(
        backend.up(provider, "app", &changed, "fp-v1", &[]).await,
        Err(Error::Migration(neotales_migration::Error::Modified))
    ));
    assert_eq!(
        query(
            provider,
            &format!(
                "SELECT COUNT(*) AS n FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.{app}')"
            )
        )
        .await[0]
            .get::<i64>("n")
            .unwrap(),
        1
    );
    assert_eq!(
        backend
            .initialize_module(provider, "app", "fp-v1", &[])
            .await
            .unwrap(),
        state
    );
}

async fn concurrent_up_serializes_on_transaction_lock(provider: &MssqlPoolProvider) {
    let table = table("concurrent");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let migrations = definitions(&[(
        1,
        "app",
        format!("WAITFOR DELAY '00:00:02'\nGO\nCREATE TABLE dbo.{app} (id bigint)"),
        format!("DROP TABLE dbo.{app}"),
    )]);
    let (first, second) = tokio::join!(
        backend.up(provider, "app", &migrations, "fp-v1", &[]),
        backend.up(provider, "app", &migrations, "fp-v1", &[]),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first, second);
    assert_eq!(first.applied_count(), 1);
    assert_eq!(
        query(provider, &format!("SELECT COUNT(*) AS n FROM dbo.{app}")).await[0]
            .get::<i64>("n")
            .unwrap(),
        0
    );
}

async fn cancellation_releases_lock_without_partial_state(provider: &MssqlPoolProvider) {
    let table = table("cancel");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let migrations = definitions(&[(
        1,
        "app",
        format!("WAITFOR DELAY '00:00:05'\nGO\nCREATE TABLE dbo.{app} (id bigint)"),
        format!("DROP TABLE dbo.{app}"),
    )]);
    let cancelled = tokio::time::timeout(
        Duration::from_millis(300),
        backend.up(provider, "app", &migrations, "fp-v1", &[]),
    )
    .await;
    assert!(cancelled.is_err(), "the script must still be running");
    // initialize must acquire the same application lock and recreate a fresh ledger.
    let state = backend
        .initialize_module(provider, "app", "fp-v1", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(!exists(provider, &format!("dbo.{app}")).await);
}

async fn preflight_prevents_sequence_side_effect(provider: &MssqlPoolProvider) {
    let table = table("preflight");
    let app = format!("{table}_app");
    let sequence = format!("{table}_seq");
    script(
        provider,
        &format!("CREATE SEQUENCE dbo.{sequence} AS bigint START WITH 1 INCREMENT BY 1"),
    )
    .await;
    let sequence_value = || async {
        query(
            provider,
            &format!(
                "SELECT CAST(current_value AS bigint) AS value FROM sys.sequences WHERE name = N'{sequence}'"
            ),
        )
        .await[0]
            .get::<Option<i64>>("value")
            .unwrap()
    };
    let before = sequence_value().await;
    let backend = backend(&table);
    let migrations = definitions(&[(
        1,
        "side-effect",
        format!("SELECT NEXT VALUE FOR dbo.{sequence}\nGO\nSET NOEXEC ON"),
        format!("DROP TABLE dbo.{app}"),
    )]);
    match backend.up(provider, "app", &migrations, "fp-v1", &[]).await {
        Err(Error::Database(error)) => assert_eq!(error.kind(), ErrorKind::Unsupported),
        other => panic!("the prohibited second batch must fail preflight, got {other:?}"),
    }
    assert_eq!(
        sequence_value().await,
        before,
        "the first batch must not run and consume a nontransactional sequence value"
    );
    assert!(!exists(provider, &format!("dbo.{table}")).await);
    script(provider, &format!("DROP SEQUENCE dbo.{sequence}")).await;
}

async fn modified_ledger_default_is_rejected(provider: &MssqlPoolProvider) {
    let table = table("modified");
    let backend = backend(&table);
    backend
        .initialize_module(provider, "app", "fp-v1", &[])
        .await
        .unwrap();
    script(
        provider,
        &format!(
            "ALTER TABLE dbo.{table} ADD CONSTRAINT [dflt_{table}] DEFAULT ((0)) FOR [version]"
        ),
    )
    .await;
    assert!(matches!(
        backend
            .initialize_module(provider, "app", "fp-v1", &[])
            .await,
        Err(Error::History(HistoryReason::IncompatibleLedger))
    ));
}

async fn barrier_and_unregistered_down_are_preflighted(provider: &MssqlPoolProvider) {
    let table = table("barrier");
    let app = format!("{table}_app");
    let backend = backend(&table);
    let baseline = definitions(&[
        (
            1,
            "baseline",
            format!("CREATE TABLE dbo.{app} (id bigint)"),
            format!("DROP TABLE dbo.{app}"),
        ),
        (
            2,
            "insert",
            format!("INSERT INTO dbo.{app} VALUES (1)"),
            format!("DELETE FROM dbo.{app}"),
        ),
    ]);
    // Marking is only possible by constructing the core baseline definition.
    let baseline = Migrations::new(
        baseline
            .as_slice()
            .iter()
            .enumerate()
            .map(|(index, migration)| {
                if index == 0 {
                    Migration::new(
                        migration.version(),
                        migration.name(),
                        migration.up(),
                        migration.down().map(ToOwned::to_owned),
                    )
                    .unwrap()
                    .into_baseline()
                } else {
                    migration.clone()
                }
            })
            .collect(),
    )
    .unwrap();
    backend
        .up(provider, "app", &baseline, "fp-v1", &[])
        .await
        .unwrap();
    let state = backend
        .down(provider, "app", &baseline, "fp-v1", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert!(matches!(
        backend
            .down(provider, "app", &baseline, "fp-v1", 1, &[])
            .await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    assert!(matches!(
        backend
            .down(provider, "app", &baseline, "fp-v1", 2, &[])
            .await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    assert!(exists(provider, &format!("dbo.{app}")).await);
    assert!(matches!(
        backend
            .down(provider, "unregistered", &baseline, "fp-v1", 0, &[])
            .await,
        Err(Error::History(HistoryReason::UnregisteredModule))
    ));
}

async fn unowned_partial_and_binding_mismatch(provider: &MssqlPoolProvider) {
    let partial = table("partial");
    script(
        provider,
        &format!("CREATE VIEW dbo.{partial} AS SELECT 1 AS x"),
    )
    .await;
    assert!(matches!(
        backend(&partial)
            .initialize_module(provider, "app", "fp-v1", &[])
            .await,
        Err(Error::History(HistoryReason::PartialLedger))
    ));
    script(provider, &format!("DROP VIEW dbo.{partial}")).await;

    let unowned = table("unowned");
    script(provider, &format!("CREATE TABLE dbo.{unowned} (x int)")).await;
    script(
        provider,
        &format!("CREATE TABLE dbo.{unowned}_modules (x int)"),
    )
    .await;
    assert!(matches!(
        backend(&unowned)
            .initialize_module(provider, "app", "fp-v1", &[])
            .await,
        Err(Error::History(HistoryReason::IncompatibleLedger))
    ));
    script(provider, &format!("DROP TABLE dbo.{unowned}")).await;
    script(provider, &format!("DROP TABLE dbo.{unowned}_modules")).await;

    let binding = table("binding");
    let binding_backend = backend(&binding);
    binding_backend
        .initialize_module(provider, "app", "fp-v1", &[])
        .await
        .unwrap();
    assert!(matches!(
        binding_backend
            .initialize_module(provider, "app", "fp-v2", &[])
            .await,
        Err(Error::History(HistoryReason::BindingMismatch))
    ));
    // Binary collation keeps module identity bytewise: APP is a different module.
    let other = binding_backend
        .initialize_module(provider, "APP", "fp-v2", &[])
        .await
        .unwrap();
    assert_eq!(other.module(), "APP");
    assert_eq!(other.applied_count(), 0);
    assert_eq!(
        query(
            provider,
            &format!("SELECT COUNT(*) AS n FROM dbo.{binding}_modules"),
        )
        .await[0]
            .get::<i64>("n")
            .unwrap(),
        2
    );
    assert_eq!(
        binding_backend
            .initialize_module(provider, "app", "fp-v1", &[])
            .await
            .unwrap()
            .module(),
        "app"
    );

    let spaced = table("spaced");
    let spaced_backend = backend(&spaced);
    assert!(matches!(
        spaced_backend
            .initialize_module(provider, "app ", "fp-v1", &[])
            .await,
        Err(Error::Configuration)
    ));
    assert!(matches!(
        spaced_backend
            .initialize_module(provider, "app", "fp-v1 ", &[])
            .await,
        Err(Error::Configuration)
    ));
    assert!(!exists(provider, &format!("dbo.{spaced}")).await);
}
