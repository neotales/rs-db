//! MySQL migration integration coverage against a testcontainers-managed server.
//!
//! Requires Docker. `tests/live.sh` selects this ignored test. The container
//! publishes no ports and uses a random nonempty root password from the OS CSPRNG.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    time::Duration,
};

use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_mysql::{MySqlConfig, MySqlPoolProvider};
use neotales_migration::{FlattenManifest, Migration, Migrations};
use neotales_migration_mysql::{DirtyDirection, DirtyState, Error, MySqlBackend, ReconcileOutcome};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

const IMAGE: &str = "mysql";
const TAG: &str = "8.4@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902";

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

async fn start_mysql(password: &str, extra_args: &[&str]) -> ContainerAsync<GenericImage> {
    let image = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stdout_or_stderr("ready for connections").with_times(2),
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", password)
        .with_env_var("MYSQL_ROOT_HOST", "%")
        .with_env_var("MYSQL_DATABASE", "neotales_test")
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(std::collections::HashMap::new());
        });
    if extra_args.is_empty() {
        image.start().await.expect("MySQL container")
    } else {
        image
            .with_cmd(extra_args.iter().copied())
            .start()
            .await
            .expect("MySQL container")
    }
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
        .args(["port", container.id(), "3306"])
        .output()
        .expect("docker port");
    assert!(mapped.stdout.is_empty(), "3306 must have no host mapping");
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

async fn connect(port: u16, password: &str) -> MySqlPoolProvider {
    let mut last = neotales_dbapi::ErrorKind::Connection;
    for _ in 0..30 {
        let config = MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
            .expect("loopback configuration")
            .password(password)
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5));
        match MySqlPoolProvider::connect(config).await {
            Ok(provider) => return provider,
            Err(error) => {
                last = error.kind();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!("disposable MySQL did not accept connections: {last:?}");
}

fn migration(version: u64, name: &str, up: &str, down: Option<&str>) -> Migration {
    Migration::new(version, name, up, down.map(str::to_owned)).unwrap()
}

fn backend_for(table: &str) -> MySqlBackend {
    MySqlBackend::new("neotales_test", table, Duration::from_secs(5)).unwrap()
}

async fn run_sql(provider: &MySqlPoolProvider, sql: &str) {
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .unwrap();
}

async fn run_params(provider: &MySqlPoolProvider, sql: &str, parameters: &[Parameter]) {
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(sql, QueryOptions::new().bind_all(parameters))
        .await
        .unwrap();
}

async fn scalar_text(provider: &MySqlPoolProvider, sql: &str) -> String {
    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 4096).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("value").unwrap()
}

async fn scalar(provider: &MySqlPoolProvider, sql: &str) -> i64 {
    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            sql,
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("value").unwrap()
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)]
async fn migration_mysql_contract() {
    let password = disposable_password();
    let container = start_mysql(&password, &["--autocommit=0"]).await;
    assert_no_published_ports(&container);
    let ip: IpAddr = container
        .get_bridge_ip_address()
        .await
        .expect("bridge address");
    let port = loopback_proxy(SocketAddr::new(ip, 3306)).await;
    let provider = connect(port, &password).await;
    let observer = connect(port, &password).await;
    {
        let mut session = provider.acquire_async().await.unwrap();
        let rows = session
            .fetch_all_async(
                "SELECT @@SESSION.autocommit AS value",
                QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(
            rows[0].get::<i64>("value").unwrap(),
            1,
            "the provider must establish autocommit despite the server setting"
        );
    }

    mismatched_database_fails_before_writes(&provider).await;
    initialize_and_state_are_explicit(&provider).await;
    apply_and_repeat(&provider).await;
    partial_failure_requires_reconcile(&provider).await;
    down_failure_reconcile_applied(&provider).await;
    down_not_applied_restores_removed_history(&provider).await;
    reconcile_rejects_present_not_head(&provider).await;
    reconcile_rejects_absent_not_next(&provider).await;
    Box::pin(concurrent_writers_serialize(&provider, &observer)).await;
    lock_timeout_is_explicit(&provider, &observer).await;
    interruption_leaves_recoverable_dirty_state(&provider, &observer).await;
    untracked_and_unowned_ledgers_fail_closed(&provider).await;
    marked_ledgers_with_missing_constraints_are_rejected(&provider).await;
    initialized_ledger_constraint_drift_blocks_write(&provider).await;

    drop(provider);
}

async fn mismatched_database_fails_before_writes(provider: &MySqlPoolProvider) {
    run_sql(provider, "CREATE DATABASE neotales_other").await;
    let backend = MySqlBackend::new(
        "neotales_other",
        "ledger_mismatch_guard",
        Duration::from_secs(5),
    )
    .unwrap();
    let migrations = Migrations::new(vec![migration(
        1,
        "mismatch",
        "CREATE TABLE mismatch_target (id BIGINT)",
        None,
    )])
    .unwrap();
    assert!(matches!(
        backend
            .up(provider, "app", &migrations, "fp-mismatch", &[])
            .await,
        Err(Error::Configuration)
    ));
    assert_eq!(scalar(provider, "SELECT COUNT(*) AS value FROM information_schema.tables WHERE table_schema IN ('neotales_other', 'neotales_test') AND table_name IN ('ledger_mismatch_guard', 'ledger_mismatch_guard_meta', 'mismatch_target')").await, 0);
}

async fn initialize_and_state_are_explicit(provider: &MySqlPoolProvider) {
    let fresh = backend_for("ledger_fresh_state");
    assert!(fresh.state(provider, "app").await.unwrap().is_none());
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM information_schema.tables \
             WHERE table_schema='neotales_test' AND table_name IN \
             ('ledger_fresh_state', 'ledger_fresh_state_meta')"
        )
        .await,
        0,
        "state must not create ledger tables"
    );

    let backend = backend_for("ledger_init");
    let state = backend
        .initialize_module(provider, "app", "fp-init", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert!(matches!(
        backend
            .initialize_module(provider, "app", "fp-other", &[])
            .await
            .unwrap_err(),
        Error::History(neotales_migration_mysql::HistoryReason::BindingMismatch)
    ));
}

async fn apply_and_repeat(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_apply");
    let migrations = Migrations::new(vec![
        migration(
            1,
            "create",
            "CREATE TABLE app_apply (id BIGINT PRIMARY KEY); INSERT INTO app_apply VALUES (1);",
            Some("DROP TABLE app_apply"),
        ),
        migration(
            2,
            "add",
            "INSERT INTO app_apply VALUES (2);",
            Some("DELETE FROM app_apply WHERE id = 2"),
        ),
    ])
    .unwrap();
    let state = backend
        .up(provider, "app", &migrations, "fp-apply", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2);
    assert_eq!(
        scalar(provider, "SELECT COUNT(*) AS value FROM app_apply").await,
        2
    );

    let state = backend
        .up(provider, "app", &migrations, "fp-apply", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 2);
    assert_eq!(
        scalar(provider, "SELECT COUNT(*) AS value FROM app_apply").await,
        2
    );

    let state = backend
        .down(provider, "app", &migrations, "fp-apply", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        scalar(provider, "SELECT COUNT(*) AS value FROM app_apply").await,
        1
    );
    let state = backend
        .down(provider, "app", &migrations, "fp-apply", 1, &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);

    let legacy = Migrations::new(vec![migration(
        1,
        "legacy",
        "CREATE TABLE flatten_legacy (id BIGINT)",
        Some("DROP TABLE flatten_legacy"),
    )])
    .unwrap();
    let baseline = Migration::new(2, "baseline", "DO SLEEP(0)", None)
        .unwrap()
        .into_baseline();
    let manifest = FlattenManifest::new(legacy.as_slice().to_vec(), baseline).unwrap();
    assert!(matches!(
        backend
            .flatten(provider, "app", &manifest, "fp-apply", &[])
            .await
            .unwrap_err(),
        Error::UnsupportedFlatten
    ));
}

async fn partial_failure_requires_reconcile(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_partial");
    let migrations = Migrations::new(vec![migration(
        1,
        "needs-child",
        "CREATE TABLE IF NOT EXISTS partial_parent (id BIGINT PRIMARY KEY); \
         INSERT INTO partial_child (id) VALUES (1);",
        Some("DROP TABLE IF EXISTS partial_parent"),
    )])
    .unwrap();
    let error = backend
        .up(provider, "app", &migrations, "fp-partial", &[])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Failed(..)), "{error:?}");
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(
        state.dirty().map(DirtyState::direction),
        Some(DirtyDirection::Up)
    );
    assert!(
        backend
            .up(provider, "app", &migrations, "fp-partial", &[])
            .await
            .unwrap_err()
            .to_string()
            .contains("dirty")
    );
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM information_schema.tables \
             WHERE table_schema='neotales_test' AND table_name='partial_parent'"
        )
        .await,
        1,
        "the first statement committed despite the later failure"
    );

    run_sql(
        provider,
        "CREATE TABLE partial_child (id BIGINT PRIMARY KEY)",
    )
    .await;
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-partial",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert!(state.dirty().is_none());
    let state = backend
        .up(provider, "app", &migrations, "fp-partial", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(
        scalar(provider, "SELECT COUNT(*) AS value FROM partial_child").await,
        1
    );
}

async fn down_failure_reconcile_applied(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_down");
    let migrations = Migrations::new(vec![migration(
        1,
        "down-fails-after-drop",
        "CREATE TABLE down_table (id BIGINT PRIMARY KEY)",
        Some("DROP TABLE down_table; SELECT * FROM missing_after_drop"),
    )])
    .unwrap();
    backend
        .up(provider, "app", &migrations, "fp-down", &[])
        .await
        .unwrap();
    let error = backend
        .down(provider, "app", &migrations, "fp-down", 1, &[])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Failed(..)), "{error:?}");
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(
        state.dirty().map(DirtyState::direction),
        Some(DirtyDirection::Down)
    );
    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-down",
            ReconcileOutcome::Applied,
        )
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM information_schema.tables \
             WHERE table_schema='neotales_test' AND table_name='down_table'"
        )
        .await,
        0
    );
}

async fn down_not_applied_restores_removed_history(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_restore");
    let migrations = Migrations::new(vec![migration(
        1,
        "restore",
        "CREATE TABLE restore_table (id BIGINT PRIMARY KEY)",
        Some("DROP TABLE restore_table"),
    )])
    .unwrap();
    backend
        .up(provider, "app", &migrations, "fp-restore", &[])
        .await
        .unwrap();

    // Simulate a crash after the down script and history delete, before the dirty
    // marker was cleared. The operator reports that the down did not take effect.
    run_sql(
        provider,
        "DELETE FROM `ledger_restore` WHERE module='app' AND version=1",
    )
    .await;
    let checksum = Parameter::new(
        DbType::Text,
        Value::Text(migrations.as_slice()[0].checksum().to_owned()),
    )
    .unwrap();
    run_params(
        provider,
        "UPDATE `ledger_restore_meta` SET dirty_version=1, dirty_checksum=?, \
         dirty_direction='down', dirty_stage='script_done' WHERE module='app'",
        &[checksum],
    )
    .await;
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(
        state.dirty().map(DirtyState::direction),
        Some(DirtyDirection::Down)
    );

    let state = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-restore",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    assert_eq!(
        state.applied_count(),
        1,
        "the history row must be restored, not left missing"
    );
    assert!(state.dirty().is_none());
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM information_schema.tables \
             WHERE table_schema='neotales_test' AND table_name='restore_table'"
        )
        .await,
        1
    );
    run_sql(provider, "DROP TABLE restore_table").await;
}

// Hold the actual migration lock and observe both contenders in the server process list.
async fn concurrent_writers_serialize(provider: &MySqlPoolProvider, observer: &MySqlPoolProvider) {
    let backend = backend_for("ledger_concurrent");
    let migrations = Migrations::new(vec![migration(
        1,
        "create",
        "CREATE TABLE concurrent_table (id BIGINT PRIMARY KEY)",
        Some("DROP TABLE concurrent_table"),
    )])
    .unwrap();
    let lock = Parameter::new(DbType::Text, Value::Text(backend.lock_name())).unwrap();
    let mut barrier = observer.acquire_async().await.unwrap();
    let rows = barrier
        .fetch_all_async(
            "SELECT GET_LOCK(?, 10) AS acquired",
            QueryOptions::new()
                .bind(lock.clone())
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get_ordinal::<i64>(0).unwrap(), 1);
    let first = backend.up(provider, "app", &migrations, "fp-concurrent", &[]);
    let second = backend.up(provider, "app", &migrations, "fp-concurrent", &[]);
    let writers = async { tokio::join!(first, second) };
    tokio::pin!(writers);
    let observe = async {
        loop {
            let count = scalar(observer, "SELECT COUNT(*) AS value FROM information_schema.PROCESSLIST WHERE INFO LIKE CONCAT('SELECT GET_', 'LOCK(%')").await;
            if count >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        result = &mut writers => panic!("writers finished before barrier release: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(4), observe) => result.expect("both writers must contend for the migration lock"),
    }
    barrier
        .execute_async("SELECT RELEASE_LOCK(?)", QueryOptions::new().bind(lock))
        .await
        .unwrap();
    let (first, second) = writers.await;
    first.unwrap();
    second.unwrap();
    assert_eq!(
        backend
            .state(provider, "app")
            .await
            .unwrap()
            .unwrap()
            .applied_count(),
        1
    );
    assert_eq!(scalar(provider, "SELECT COUNT(*) AS value FROM information_schema.tables WHERE table_schema='neotales_test' AND table_name='concurrent_table'").await, 1);
}

async fn lock_timeout_is_explicit(provider: &MySqlPoolProvider, observer: &MySqlPoolProvider) {
    let backend = backend_for("ledger_lock");
    let short = MySqlBackend::new("neotales_test", "ledger_lock", Duration::from_secs(1)).unwrap();
    let lock = Parameter::new(DbType::Text, Value::Text(backend.lock_name())).unwrap();
    let mut barrier = observer.acquire_async().await.unwrap();
    let rows = barrier
        .fetch_all_async(
            "SELECT GET_LOCK(?, 10) AS acquired",
            QueryOptions::new()
                .bind(lock.clone())
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get_ordinal::<i64>(0).unwrap(), 1);
    let migrations = Migrations::new(vec![migration(
        1,
        "create",
        "CREATE TABLE lock_table (id BIGINT PRIMARY KEY)",
        Some("DROP TABLE lock_table"),
    )])
    .unwrap();
    assert!(matches!(
        short
            .up(provider, "app", &migrations, "fp-lock", &[])
            .await
            .unwrap_err(),
        Error::LockTimeout
    ));
    barrier
        .execute_async("SELECT RELEASE_LOCK(?)", QueryOptions::new().bind(lock))
        .await
        .unwrap();
    assert_eq!(
        backend
            .up(provider, "app", &migrations, "fp-lock", &[])
            .await
            .unwrap()
            .applied_count(),
        1
    );
}

// Observe the server-side barrier and the durable marker from another pool.
async fn wait_for_interrupted_script(observer: &MySqlPoolProvider) {
    loop {
        let active = scalar(observer, "SELECT COUNT(*) AS value FROM information_schema.PROCESSLIST WHERE INFO LIKE CONCAT('DO GET_', 'LOCK(', CHAR(39), 'neotales_interrupt_barrier', CHAR(39), '%')").await;
        let dirty = scalar(observer, "SELECT COUNT(*) AS value FROM ledger_interrupt_meta WHERE module='app' AND dirty_version=1 AND dirty_direction='up'").await;
        if active == 1 && dirty == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
}

async fn interruption_leaves_recoverable_dirty_state(
    provider: &MySqlPoolProvider,
    observer: &MySqlPoolProvider,
) {
    let backend = backend_for("ledger_interrupt");
    // Hold a server-side barrier so the migration cannot finish its script before
    // the observer sees both the dispatched statement and its committed marker.
    let mut barrier = observer.acquire_async().await.unwrap();
    let barrier_lock = Parameter::new(
        DbType::Text,
        Value::Text("neotales_interrupt_barrier".into()),
    )
    .unwrap();
    let acquired = barrier
        .fetch_all_async(
            "SELECT GET_LOCK(?, 10) AS acquired",
            QueryOptions::new()
                .bind(barrier_lock.clone())
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(acquired[0].get_ordinal::<i64>(0).unwrap(), 1);
    let migrations = Migrations::new(vec![migration(
        1,
        "barrier",
        "DO GET_LOCK('neotales_interrupt_barrier', 30); DO RELEASE_LOCK('neotales_interrupt_barrier')",
        Some("DO SLEEP(0)"),
    )]).unwrap();
    backend
        .initialize_module(provider, "app", "fp-interrupt", &[])
        .await
        .unwrap();
    {
        let operation = backend.up(provider, "app", &migrations, "fp-interrupt", &[]);
        tokio::pin!(operation);
        tokio::select! {
            result = &mut operation => panic!("migration finished before interruption: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(30), wait_for_interrupted_script(observer)) => {
                result.expect("migration must dispatch its blocked script after persisting the dirty marker");
            }
        }
        // Dropping the in-flight future here injects cancellation at the observed point.
    }
    barrier
        .execute_async(
            "SELECT RELEASE_LOCK(?)",
            QueryOptions::new().bind(barrier_lock),
        )
        .await
        .unwrap();

    // Obtain the migration lock after the cancelled session is discarded, without a fixed delay.
    let lock = Parameter::new(DbType::Text, Value::Text(backend.lock_name())).unwrap();
    let mut probe = observer.acquire_async().await.unwrap();
    let rows = probe
        .fetch_all_async(
            "SELECT GET_LOCK(?, 10) AS acquired",
            QueryOptions::new()
                .bind(lock.clone())
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get_ordinal::<i64>(0).unwrap(),
        1,
        "a cancelled migration must not leak its lock through the pool"
    );
    probe
        .execute_async("SELECT RELEASE_LOCK(?)", QueryOptions::new().bind(lock))
        .await
        .unwrap();

    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(
        state.dirty().map(DirtyState::direction),
        Some(DirtyDirection::Up),
        "the dirty marker was persisted before the script"
    );
    assert!(matches!(
        backend
            .up(provider, "app", &migrations, "fp-interrupt", &[])
            .await
            .unwrap_err(),
        Error::Dirty(_)
    ));
    backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-interrupt",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap();
    let reconciled = backend.state(observer, "app").await.unwrap().unwrap();
    assert!(reconciled.dirty().is_none());
    assert_eq!(reconciled.applied_count(), 0);
    let state = backend
        .up(provider, "app", &migrations, "fp-interrupt", &[])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    let persisted = backend.state(observer, "app").await.unwrap().unwrap();
    assert_eq!(persisted.applied_count(), 1);
    assert!(persisted.dirty().is_none());
}

async fn untracked_and_unowned_ledgers_fail_closed(provider: &MySqlPoolProvider) {
    run_sql(
        provider,
        "CREATE TABLE untracked_table (id BIGINT PRIMARY KEY)",
    )
    .await;
    let migrations = Migrations::new(vec![migration(1, "noop", "DO SLEEP(0)", None)]).unwrap();
    let backend = backend_for("ledger_untracked");
    assert!(matches!(
        backend
            .up(
                provider,
                "app",
                &migrations,
                "fp-untracked",
                &["untracked_table"]
            )
            .await
            .unwrap_err(),
        Error::UntrackedObjects
    ));
    assert!(backend.state(provider, "app").await.unwrap().is_none());

    run_sql(
        provider,
        "CREATE TABLE lookalike (module VARCHAR(255) NOT NULL, version BIGINT NOT NULL, \
         checksum VARCHAR(255) NOT NULL, fingerprint VARCHAR(255) NOT NULL, \
         PRIMARY KEY (module, version)); \
         CREATE TABLE lookalike_meta (module VARCHAR(255) NOT NULL PRIMARY KEY, \
         fingerprint VARCHAR(255) NOT NULL)",
    )
    .await;
    let lookalike = backend_for("lookalike");
    let error = lookalike
        .up(provider, "app", &migrations, "fp-lookalike", &[])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::UnownedLedger)
    ));

    // Correct markers but an incompatible shape: wrong varchar length and unsigned version.
    run_sql(
        provider,
        "CREATE TABLE marked (module VARCHAR(100) NOT NULL, \
         version BIGINT UNSIGNED NOT NULL, checksum VARCHAR(255) NOT NULL, \
         fingerprint VARCHAR(255) NOT NULL, PRIMARY KEY (module, version)) \
         ENGINE=InnoDB COMMENT='neotales-migration-mysql/ledger/v1/history'; \
         CREATE TABLE marked_meta (module VARCHAR(255) NOT NULL PRIMARY KEY, \
         fingerprint VARCHAR(255) NOT NULL) \
         ENGINE=InnoDB COMMENT='neotales-migration-mysql/ledger/v1/meta'",
    )
    .await;
    let marked = backend_for("marked");
    let error = marked
        .up(provider, "app", &migrations, "fp-marked", &[])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::IncompatibleLedger)
    ));
}

async fn marked_ledgers_with_missing_constraints_are_rejected(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_constraint");
    backend
        .initialize_module(provider, "app", "fp-constraint", &[])
        .await
        .unwrap();
    let name = scalar_text(
        provider,
        "SELECT constraint_name AS value FROM information_schema.table_constraints \
         WHERE table_schema='neotales_test' AND table_name='ledger_constraint' \
         AND constraint_type='CHECK' LIMIT 1",
    )
    .await;
    assert!(
        name.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "unexpected check name: {name}"
    );
    run_sql(
        provider,
        &format!("ALTER TABLE `ledger_constraint` DROP CHECK `{name}`"),
    )
    .await;
    let error = backend.state(provider, "app").await.unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::IncompatibleLedger)
    ));
}

async fn reconcile_rejects_present_not_head(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_present_drift");
    let migrations = Migrations::new(vec![
        migration(1, "first", "DO SLEEP(0)", None),
        migration(2, "second", "DO SLEEP(0)", None),
    ])
    .unwrap();
    backend
        .up(provider, "app", &migrations, "fp-present-drift", &[])
        .await
        .unwrap();

    // A down marker for version 1 is present in history but is not the head.
    let checksum = Parameter::new(
        DbType::Text,
        Value::Text(migrations.as_slice()[0].checksum().to_owned()),
    )
    .unwrap();
    run_params(
        provider,
        "UPDATE `ledger_present_drift_meta` SET dirty_version=1, dirty_checksum=?, \
         dirty_direction='down', dirty_stage='script_done' WHERE module='app'",
        &[checksum],
    )
    .await;
    let error = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-present-drift",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::InconsistentRows)
    ));
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(state.dirty().map(DirtyState::version), Some(1));
    assert_eq!(state.applied_count(), 2, "the dirty marker must not clear");
}

async fn reconcile_rejects_absent_not_next(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_absent_drift");
    let migrations = Migrations::new(vec![
        migration(1, "first", "DO SLEEP(0)", None),
        migration(2, "second", "DO SLEEP(0)", None),
        migration(3, "third", "DO SLEEP(0)", None),
    ])
    .unwrap();
    backend
        .up(provider, "app", &migrations, "fp-absent-drift", &[])
        .await
        .unwrap();
    run_sql(
        provider,
        "DELETE FROM `ledger_absent_drift` WHERE module='app' AND version IN (2,3)",
    )
    .await;
    let checksum = Parameter::new(
        DbType::Text,
        Value::Text(migrations.as_slice()[2].checksum().to_owned()),
    )
    .unwrap();
    run_params(
        provider,
        "UPDATE `ledger_absent_drift_meta` SET dirty_version=3, dirty_checksum=?, \
         dirty_direction='up', dirty_stage='started' WHERE module='app'",
        std::slice::from_ref(&checksum),
    )
    .await;
    let error = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-absent-drift",
            ReconcileOutcome::NotApplied,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::InconsistentRows)
    ));
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(state.dirty().map(DirtyState::version), Some(3));
    assert_eq!(state.applied_count(), 1, "the dirty marker must not clear");

    // The same absent-not-next marker must also be rejected for a down outcome.
    run_params(
        provider,
        "UPDATE `ledger_absent_drift_meta` SET dirty_direction='down', \
         dirty_stage='script_done' WHERE module='app'",
        &[],
    )
    .await;
    let error = backend
        .reconcile(
            provider,
            "app",
            &migrations,
            "fp-absent-drift",
            ReconcileOutcome::Applied,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::InconsistentRows)
    ));
    let state = backend.state(provider, "app").await.unwrap().unwrap();
    assert_eq!(state.dirty().map(DirtyState::version), Some(3));
}

async fn initialized_ledger_constraint_drift_blocks_write(provider: &MySqlPoolProvider) {
    let backend = backend_for("ledger_write_constraint");
    let migrations = Migrations::new(vec![migration(1, "noop", "DO SLEEP(0)", None)]).unwrap();
    backend
        .initialize_module(provider, "app", "fp-write-constraint", &[])
        .await
        .unwrap();
    let name = scalar_text(
        provider,
        "SELECT constraint_name AS value FROM information_schema.table_constraints \
         WHERE table_schema='neotales_test' AND table_name='ledger_write_constraint' \
         AND constraint_type='CHECK' LIMIT 1",
    )
    .await;
    assert!(
        name.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "unexpected check name: {name}"
    );
    run_sql(
        provider,
        &format!("ALTER TABLE `ledger_write_constraint` DROP CHECK `{name}`"),
    )
    .await;

    let error = backend
        .up(provider, "app", &migrations, "fp-write-constraint", &[])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::History(neotales_migration_mysql::HistoryReason::IncompatibleLedger)
    ));
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM `ledger_write_constraint`"
        )
        .await,
        0,
        "bootstrap verification must run before the first ledger write"
    );
    assert_eq!(
        scalar(
            provider,
            "SELECT COUNT(*) AS value FROM `ledger_write_constraint_meta` \
             WHERE dirty_version IS NOT NULL"
        )
        .await,
        0
    );
}
