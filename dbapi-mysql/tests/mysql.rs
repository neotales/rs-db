//! MySQL provider contract against a testcontainers-managed server.
//!
//! Requires Docker. The suite is `#[ignore]`d so a plain `cargo test` stays
//! fixture-free; `tests/live.sh` and the dedicated CI job run it explicitly.
//! The container is created and removed by `testcontainers`, so there is no
//! fixed host port and no shell cleanup to get wrong.

use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    Capabilities, DbEvent, DbType, ErrorKind, Operation, Parameter, PolicyPool, Pool, PoolPolicy,
    QueryOptions, Row, RowLimit, Session, TelemetryOutcome, TxOptions, Value,
};
use neotales_dbapi_mysql::{MySqlConfig, MySqlPoolProvider, MySqlTls};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{AccessMode, Mount, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

#[path = "support/keyset_plan.rs"]
mod keyset_plan;

/// `MySQL` 8.4 pinned by manifest digest.
const IMAGE: &str = "mysql";
const TAG: &str = "8.4@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902";
// The temporary initialization server and X plugin both emit generic readiness.
// The trailing space distinguishes the classic port from X plugin port 33060.
const MYSQL_FINAL_READY: &str = "port: 3306 ";

/// A disposable password from the operating system CSPRNG. It is never logged or committed.
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

/// Starts a private container with no published ports. The test reaches the
/// container's bridge address through a loopback-only proxy, so the database is
/// never bound to the host's external interfaces.
async fn start_mysql(
    password: &str,
    certs: Option<&Path>,
    extra_args: &[&str],
) -> ContainerAsync<GenericImage> {
    let started = std::time::Instant::now();
    let image = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stdout_or_stderr(MYSQL_FINAL_READY),
        ))
        .with_env_var("MYSQL_ROOT_PASSWORD", password)
        .with_env_var("MYSQL_ROOT_HOST", "%")
        .with_env_var("MYSQL_DATABASE", "neotales_test")
        // testcontainers 0.28 sets PublishAllPorts when no explicit mapping exists.
        // Disable that explicitly so the image's exposed 3306 is never published.
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(std::collections::HashMap::new());
        });
    let image = match certs {
        Some(directory) => image.with_mount(
            Mount::bind_mount(directory.to_string_lossy(), "/certs")
                .with_access_mode(AccessMode::ReadOnly),
        ),
        None => image,
    };
    let mut args: Vec<String> = Vec::new();
    if certs.is_some() {
        args.extend([
            "--ssl-ca=/certs/ca.pem".to_owned(),
            "--ssl-cert=/certs/server-cert.pem".to_owned(),
            "--ssl-key=/certs/server-key.pem".to_owned(),
        ]);
    }
    args.extend(extra_args.iter().map(|value| (*value).to_owned()));
    let image = if args.is_empty() {
        image
    } else {
        image.with_cmd(args)
    };
    // Cold initialization can exceed testcontainers' default startup deadline under load.
    // This bounds readiness only; it never retries a test operation or an unknown write.
    let container = image
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .expect("MySQL container");
    eprintln!(
        "fixture stage=container_ready elapsed_ms={}",
        started.elapsed().as_millis()
    );
    container
}

#[test]
fn final_readiness_marker_excludes_temporary_and_x_plugin_ports() {
    assert!("ready for connections. port: 3306  MySQL".contains(MYSQL_FINAL_READY));
    assert!(!"ready for connections. port: 0  MySQL".contains(MYSQL_FINAL_READY));
    assert!(!"ready for connections. port: 33060 ".contains(MYSQL_FINAL_READY));
}

/// Forwards a loopback listener to the container's bridge address and returns
/// the loopback port. No Docker-published port exists for these tests.
async fn loopback_proxy(bind: &str, target: SocketAddr) -> u16 {
    loopback_proxy_with_probe_stage(bind, target, None).await
}

async fn loopback_proxy_with_probe_stage(
    bind: &str,
    target: SocketAddr,
    probe_complete: Option<Arc<AtomicBool>>,
) -> u16 {
    let listener = tokio::net::TcpListener::bind((bind, 0))
        .await
        .expect("loopback proxy listener");
    let port = listener.local_addr().expect("proxy address").port();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                break;
            };
            let probe_complete = probe_complete.clone();
            tokio::spawn(async move {
                match tokio::net::TcpStream::connect(target).await {
                    Ok(mut outbound) => {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                    Err(error) => {
                        let phase = match probe_complete {
                            Some(stage) if stage.load(Ordering::SeqCst) => "after_plaintext_probe",
                            Some(_) => "before_plaintext_probe_complete",
                            None => "unspecified",
                        };
                        eprintln!(
                            "fixture stage=proxy_connect phase={phase} category={:?}",
                            error.kind()
                        );
                    }
                }
            });
        }
    });
    port
}

async fn bridge_address(container: &ContainerAsync<GenericImage>) -> SocketAddr {
    let ip: IpAddr = container
        .get_bridge_ip_address()
        .await
        .expect("container bridge address");
    SocketAddr::new(ip, 3306)
}

/// Asserts the actual Docker configuration, not just the testcontainers request.
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
    assert!(inspect.status.success(), "docker inspect failed");
    let text = String::from_utf8_lossy(&inspect.stdout).trim().to_owned();
    assert_eq!(
        text, "false|{}",
        "the container must not publish image-exposed ports"
    );
    let mapped = Command::new("docker")
        .args(["port", container.id(), "3306"])
        .output()
        .expect("docker port");
    assert!(
        mapped.stdout.is_empty(),
        "container port 3306 must have no host mapping: {}",
        String::from_utf8_lossy(&mapped.stdout)
    );
}

async fn connect(port: u16, password: &str) -> MySqlPoolProvider {
    let started = std::time::Instant::now();
    let mut last = ErrorKind::Connection;
    for attempt in 1..=30 {
        match MySqlPoolProvider::connect(
            MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
                .expect("loopback configuration")
                .password(password)
                .max_connections(8)
                .acquire_timeout(Duration::from_secs(5)),
        )
        .await
        {
            Ok(provider) => {
                eprintln!(
                    "fixture stage=provider_connected attempts={attempt} elapsed_ms={}",
                    started.elapsed().as_millis()
                );
                return provider;
            }
            Err(error) => {
                last = error.kind();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!(
        "fixture stage=provider_connect attempts=30 elapsed_ms={} category={last:?}",
        started.elapsed().as_millis()
    );
}

fn limit() -> RowLimit {
    RowLimit::new(100, 65_536).unwrap()
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).unwrap()
}

fn i64_parameter(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).unwrap()
}

async fn run(session: &mut (dyn Session + '_), sql: &str) {
    if let Err(error) = session.execute_async(sql, QueryOptions::new()).await {
        panic!("run failed for {sql:?}: {error:?}");
    }
}

async fn fetch(session: &mut (dyn Session + '_), sql: &str, parameters: &[Parameter]) -> Vec<Row> {
    session
        .fetch_all_async(sql, QueryOptions::new().bind_all(parameters).limit(limit()))
        .await
        .unwrap()
}

async fn drop_table(session: &mut (dyn Session + '_), name: &str) {
    run(session, &format!("DROP TABLE IF EXISTS `{name}`")).await;
}

const PAGE_ADMIN_SQL: &str = "SELECT tenant, seq, body FROM page_events \
    WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";
const PAGE_TENANT_SQL: &str = "SELECT tenant, seq, body FROM page_events \
    WHERE tenant = ? AND (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";

fn page_keys(pages: &[neotales_dbapi::paging::multi::PagedPage]) -> Vec<(i64, i64)> {
    pages
        .iter()
        .flat_map(|page| {
            page.rows().iter().map(|row| {
                (
                    row.read::<i64>("tenant").expect("tenant"),
                    row.read::<i64>("seq").expect("seq"),
                )
            })
        })
        .collect()
}

async fn collect_pages(
    provider: &MySqlPoolProvider,
    sql: &'static str,
    scope: keyset_plan::Scope,
    start: (i64, i64),
) -> (Vec<neotales_dbapi::paging::multi::PagedPage>, usize) {
    let plan = keyset_plan::KeysetPlan::new(sql, scope, start).expect("page plan");
    let requests = plan.requests();
    let source = neotales_dbapi::paging::multi::sql::SqlPageSource::new(
        provider.acquire_async().await.expect("page session"),
        plan,
        4_096,
        neotales_dbapi::paging::multi::sql::SqlPageAssertion::read_only_replay_safe_and_authorized(
        ),
    )
    .expect("page source");
    let pages = neotales_dbapi::paging::multi::PageSourceStream::new(
        Box::new(source),
        neotales_dbapi::paging::PageSize::try_from(2).expect("page size"),
        neotales_dbapi::paging::multi::PageBudget::new(100, 65_536, 10).expect("budget"),
    )
    .try_collect()
    .await
    .expect("pages");
    (pages, requests.load(Ordering::Relaxed))
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn mysql_sql_page_source_contract() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &[]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;
    let provider = connect(port, &password).await;

    let mut session = provider.acquire_async().await.expect("seed session");
    run(&mut *session, "DROP TABLE IF EXISTS page_events").await;
    run(
        &mut *session,
        "CREATE TABLE page_events (tenant BIGINT NOT NULL, seq BIGINT NOT NULL, \
         body VARCHAR(64) NOT NULL, PRIMARY KEY (tenant, seq))",
    )
    .await;
    for (tenant, seq, body) in [
        (1, 1, "alpha"),
        (1, 2, "bravo"),
        (1, 3, "charlie"),
        (2, 1, "delta"),
        (2, 2, "echo"),
        (3, 1, "foxtrot"),
        (3, 2, "golf"),
    ] {
        session
            .execute_async(
                "INSERT INTO page_events (tenant, seq, body) VALUES (?, ?, ?)",
                QueryOptions::new().bind_all(&[
                    i64_parameter(tenant),
                    i64_parameter(seq),
                    text(body),
                ]),
            )
            .await
            .expect("seed row");
    }
    drop(session);

    let (admin, admin_requests) = collect_pages(
        &provider,
        PAGE_ADMIN_SQL,
        keyset_plan::Scope::AllTenants,
        (0, 0),
    )
    .await;
    assert_eq!(
        page_keys(&admin),
        vec![(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 1), (3, 2)]
    );
    assert_eq!(admin_requests, 4);

    let (tenant, tenant_requests) = collect_pages(
        &provider,
        PAGE_TENANT_SQL,
        keyset_plan::Scope::Tenant(2),
        (2, 0),
    )
    .await;
    assert_eq!(page_keys(&tenant), vec![(2, 1), (2, 2)]);
    assert_eq!(tenant_requests, 1);
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)] // One container and one ordered contract, like the auth stores.
async fn mysql_provider_contract() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &[]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;
    let provider = connect(port, &password).await;

    database_selection_cannot_leak(port, &password).await;
    round_trips_supported_types(&provider).await;
    reader_is_bounded_and_single_set(&provider).await;
    constraint_errors_are_sanitized(&provider).await;
    transactions_commit_rollback_and_apply_isolation(&provider).await;
    ddl_inside_a_transaction_is_rejected_before_dispatch(&provider).await;
    standalone_transaction_control_is_rejected_without_poisoning(&provider).await;
    fetch_bounds_fail_instead_of_truncating(&provider).await;
    timeout_is_an_unknown_write_and_discards_the_connection(&provider).await;
    trusted_scripts_respect_transaction_safety(&provider).await;
    unsafe_indirect_execution_is_rejected(&provider).await;
    session_state_and_executable_comments_are_rejected(&provider).await;
    lock_wait_timeout_is_busy_not_a_transport_failure(&provider).await;
    read_only_transaction_rejects_writes(&provider).await;
    capabilities_and_policy_events_are_truthful(&provider).await;

    drop(provider);
}

async fn database_selection_cannot_leak(port: u16, password: &str) {
    let config = MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
        .unwrap()
        .password(password)
        .max_connections(1);
    let pool = MySqlPoolProvider::connect(config).await.unwrap();
    let mut session = pool.acquire_async().await.unwrap();
    for script in [
        "USE mysql",
        "SELECT 1; /* hide selection */ uSe `mysql`",
        "SELECT 1; /*!50000 USE mysql */",
        "SELECT 1; /*M!100000 USE mysql */",
        "SET NAMES gbk",
        "SET /* hide keyword */ NAMES gbk",
        "SET CHARACTER SET gbk",
        "SET @@SESSION.character_set_client = gbk",
        "SET SESSION collation_connection = gbk_chinese_ci",
        "SELECT 1; /*!50000 SET NAMES gbk */",
    ] {
        let error = session
            .execute_trusted_script_async(script, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{script}");
    }
    assert_eq!(
        session
            .execute_async("SET NAMES gbk", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        session
            .fetch_all_async(
                "SET NAMES gbk",
                QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap())
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    {
        let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
        assert_eq!(
            transaction
                .execute_trusted_script_async("SET NAMES gbk", QueryOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        transaction.rollback_async().await.unwrap();
    }
    drop(session);
    let mut next = pool.acquire_async().await.unwrap();
    // This UTF-8 string has a backslash-escaped quote before a USE-shaped payload.
    // With UTF-8 on both sides it remains one harmless literal, not a second statement.
    let payload = "SELECT '縗\\'; USE mysql; -- ' AS value";
    next.execute_trusted_script_async(payload, QueryOptions::new())
        .await
        .unwrap();
    let charset = next.fetch_all_async("SELECT @@SESSION.character_set_client AS c, @@SESSION.character_set_connection AS n, @@SESSION.character_set_results AS r", QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap())).await.unwrap();
    for index in 0..3 {
        assert_eq!(charset[0].get_ordinal::<String>(index).unwrap(), "utf8mb4");
    }

    let rows = next
        .fetch_all_async(
            "SELECT DATABASE() AS value",
            QueryOptions::new().limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get_ordinal::<String>(0).unwrap(), "neotales_test");
}

async fn reader_is_bounded_and_single_set(provider: &MySqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let mut reader = session
        .fetch_async(
            "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3",
            QueryOptions::new().limit(RowLimit::with_max_sets(3, 4096, 2).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .next_row_async()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>("value")
            .unwrap(),
        1
    );
    assert!(!reader.has_next_set_async().await.unwrap());
    assert!(reader.next_row_async().await.unwrap().is_none());
    reader.finish_async().await.unwrap();
    drop(reader);

    let error = session
        .fetch_async(
            "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3",
            QueryOptions::new().limit(RowLimit::with_max_sets(2, 4096, 1).unwrap()),
        )
        .await;
    assert_eq!(
        error.err().map(neotales_dbapi::DbError::kind),
        Some(ErrorKind::Overflow)
    );
}

async fn round_trips_supported_types(provider: &MySqlPoolProvider) {
    let table = "neotales_types";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!(
            "CREATE TABLE `{table}` (
                id BIGINT PRIMARY KEY,
                amount DOUBLE NOT NULL,
                name VARCHAR(64) NOT NULL,
                payload VARBINARY(8) NOT NULL,
                optional BIGINT NULL,
                flag TINYINT(1) NOT NULL
             )"
        ),
    )
    .await;
    let parameters = [
        i64_parameter(7),
        Parameter::new(DbType::F64, Value::F64(1.5)).unwrap(),
        text("hello"),
        Parameter::new(DbType::Bytes, Value::Bytes(vec![1, 2, 3])).unwrap(),
        Parameter::new(DbType::I64, Value::Null(DbType::I64)).unwrap(),
        Parameter::new(DbType::Bool, Value::Bool(true)).unwrap(),
    ];
    session
        .execute_async(
            &format!(
                "INSERT INTO `{table}` (id, amount, name, payload, optional, flag) VALUES (?,?,?,?,?,?)"
            ),
            QueryOptions::new().bind_all(&parameters))
        .await
        .unwrap();

    let rows = fetch(
        session.as_mut(),
        &format!("SELECT id, amount, name, payload, optional, flag FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("id").unwrap(), 7);
    assert!((rows[0].get::<f64>("amount").unwrap() - 1.5).abs() < f64::EPSILON);
    assert_eq!(rows[0].get::<String>("name").unwrap(), "hello");
    assert_eq!(rows[0].get::<Vec<u8>>("payload").unwrap(), vec![1, 2, 3]);
    assert_eq!(rows[0].get::<Option<i64>>("optional").unwrap(), None);
    // MySQL reports TINYINT(1) as BOOLEAN, but the same type stores -128..=127.
    // This provider preserves the integer instead of collapsing it to a bool.
    assert_eq!(rows[0].get::<i64>("flag").unwrap(), 1);

    let rows = fetch(
        session.as_mut(),
        "SELECT NULL AS untyped, CAST(1 AS UNSIGNED) AS unsigned_value, 1.25e0 AS real_value",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<Option<String>>("untyped").unwrap(), None);
    assert_eq!(rows[0].get::<i64>("unsigned_value").unwrap(), 1);
    assert!((rows[0].get::<f64>("real_value").unwrap() - 1.25).abs() < f64::EPSILON);

    let overflow = session
        .fetch_all_async(
            "SELECT CAST(18446744073709551615 AS UNSIGNED) AS too_large",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(overflow.kind(), ErrorKind::Overflow);

    // An unsupported column type fails the row rather than inventing text.
    let mut session = provider.acquire_async().await.unwrap();
    let decode = session
        .fetch_all_async(
            "SELECT CAST(1.25 AS DECIMAL(10,2)) AS decimal_value",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(decode.kind(), ErrorKind::Decode);

    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn constraint_errors_are_sanitized(provider: &MySqlPoolProvider) {
    let table = "neotales_unique";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, name VARCHAR(32) UNIQUE)"),
    )
    .await;
    let parameters = [i64_parameter(1), text("secret-value")];
    session
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&parameters),
        )
        .await
        .unwrap();
    let error = session
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&parameters),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    let display = error.to_string();
    assert!(!display.contains(table));
    assert!(!display.contains("secret-value"));

    // The statement error discarded the physical connection; the session is poisoned.
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn transactions_commit_rollback_and_apply_isolation(provider: &MySqlPoolProvider) {
    let table = "neotales_tx";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)"),
    )
    .await;
    session
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&[i64_parameter(1), i64_parameter(1)]),
        )
        .await
        .unwrap();

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&[i64_parameter(2), i64_parameter(0)]),
        )
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS count FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("count").unwrap(), 1);

    // A read-committed transaction must observe a concurrent commit between two
    // statements. Under the default REPEATABLE READ the second read would still be 1.
    let mut reader = provider.acquire_async().await.unwrap();
    let mut transaction = reader
        .begin_async(TxOptions {
            isolation: Some(neotales_dbapi::IsolationLevel::ReadCommitted),
            read_only: Some(true),
        })
        .await
        .unwrap();
    let first = transaction
        .fetch_all_async(
            &format!("SELECT value FROM `{table}` WHERE id = 1"),
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(first[0].get::<i64>("value").unwrap(), 1);
    session
        .execute_async(
            &format!("UPDATE `{table}` SET value = 2 WHERE id = 1"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let second = transaction
        .fetch_all_async(
            &format!("SELECT value FROM `{table}` WHERE id = 1"),
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(
        second[0].get::<i64>("value").unwrap(),
        2,
        "the requested isolation level must reach the server"
    );
    transaction.commit_async().await.unwrap();

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&[i64_parameter(3), i64_parameter(0)]),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();

    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS count FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("count").unwrap(), 2);

    drop_table(session.as_mut(), table).await;
}

async fn ddl_inside_a_transaction_is_rejected_before_dispatch(provider: &MySqlPoolProvider) {
    let table = "neotales_ddl";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let error = transaction
        .execute_async(
            &format!("CREATE TABLE `{table}` (id BIGINT)"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let error = transaction
        .execute_trusted_script_async(
            &format!("INSERT INTO `{table}` VALUES (1); CREATE TABLE `{table}` (id BIGINT)"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    transaction.rollback_async().await.unwrap();

    let rows = fetch(
        session.as_mut(),
        "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
        &[text(table)],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("count").unwrap(),
        0,
        "the rejected DDL must not have reached the server"
    );

    drop_table(session.as_mut(), table).await;
}

async fn standalone_transaction_control_is_rejected_without_poisoning(
    provider: &MySqlPoolProvider,
) {
    let mut session = provider.acquire_async().await.unwrap();
    for sql in ["COMMIT", "ROLLBACK", "START TRANSACTION", "SAVEPOINT point"] {
        let error = session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{sql}");
    }
    // Validation happens before dispatch, so the session is still usable.
    let rows = fetch(session.as_mut(), "SELECT 1 AS value", &[]).await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
}

async fn fetch_bounds_fail_instead_of_truncating(provider: &MySqlPoolProvider) {
    let table = "neotales_bounds";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, payload VARCHAR(100))"),
    )
    .await;
    for id in 1..=3 {
        session
            .execute_async(
                &format!("INSERT INTO `{table}` VALUES (?,?)"),
                QueryOptions::new().bind_all(&[i64_parameter(id), text("0123456789")]),
            )
            .await
            .unwrap();
    }
    let rows = session
        .fetch_all_async(
            &format!("SELECT id, payload FROM `{table}`"),
            QueryOptions::new().limit(RowLimit::new(2, 65_536).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(rows.kind(), ErrorKind::Overflow);

    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            &format!("SELECT id, payload FROM `{table}`"),
            QueryOptions::new().limit(RowLimit::new(10, 8).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(rows.kind(), ErrorKind::Overflow);

    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn timeout_is_an_unknown_write_and_discards_the_connection(provider: &MySqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT SLEEP(0.5) AS slept",
            QueryOptions::new()
                .timeout(Duration::from_millis(50))
                .limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection,
        "a timed-out operation must not leave its connection reusable"
    );

    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .execute_async(
            "DO SLEEP(0.5)",
            QueryOptions::new().timeout(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
}

async fn trusted_scripts_respect_transaction_safety(provider: &MySqlPoolProvider) {
    let table = "neotales_script";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    session
        .execute_trusted_script_async(
            &format!(
                "CREATE TABLE `{table}` (id BIGINT PRIMARY KEY); \
                 INSERT INTO `{table}` VALUES (1); \
                 INSERT INTO `{table}` VALUES (2);"
            ),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS count FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("count").unwrap(), 2);

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_trusted_script_async(
            &format!("INSERT INTO `{table}` VALUES (3); INSERT INTO `{table}` VALUES (4)"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS count FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("count").unwrap(), 4);

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let error = transaction
        .execute_trusted_script_async(
            &format!("INSERT INTO `{table}` VALUES (5); COMMIT"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    transaction.rollback_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS count FROM `{table}`"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("count").unwrap(),
        4,
        "a rejected script must not execute its first statement"
    );

    drop_table(session.as_mut(), table).await;
}

async fn unsafe_indirect_execution_is_rejected(provider: &MySqlPoolProvider) {
    let table = "neotales_indirect";
    let procedure = "neotales_indirect_proc";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    // Routine DDL is not accepted by the MySQL prepared-statement protocol.
    // The trusted script path uses COM_QUERY and still rejects CALL/EXECUTE/PREPARE.
    session
        .execute_trusted_script_async(
            &format!("DROP PROCEDURE IF EXISTS `{procedure}`"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    // A single-statement routine body needs no DELIMITER. If CALL were allowed,
    // the CREATE would implicitly commit the managed transaction.
    session
        .execute_trusted_script_async(
            &format!("CREATE PROCEDURE `{procedure}`() CREATE TABLE `{table}` (id BIGINT)"),
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    for (sql, label) in [
        (format!("CALL `{procedure}`()"), "CALL"),
        ("EXECUTE prepared_statement".to_owned(), "EXECUTE"),
        ("PREPARE statement FROM 'SELECT 1'".to_owned(), "PREPARE"),
    ] {
        let error = transaction
            .execute_async(&sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{label}");
    }
    let error = transaction
        .execute_trusted_script_async(
            &format!(
                "PREPARE statement FROM 'CREATE TABLE `{table}` (id BIGINT)'; EXECUTE statement"
            ),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    transaction.rollback_async().await.unwrap();

    let rows = fetch(
        session.as_mut(),
        "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
        &[text(table)],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("count").unwrap(),
        0,
        "indirect DDL must not have reached the server"
    );

    // Session statements reject the same indirect forms.
    for sql in [
        format!("CALL `{procedure}`()"),
        "EXECUTE prepared_statement".to_owned(),
        "PREPARE statement FROM 'SELECT 1'".to_owned(),
    ] {
        let error = session
            .execute_async(&sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    session
        .execute_trusted_script_async(
            &format!("DROP PROCEDURE IF EXISTS `{procedure}`"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn session_state_and_executable_comments_are_rejected(provider: &MySqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    for sql in [
        "SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'",
        "SET SESSION sql_mode = 'ANSI_QUOTES'",
        "SET @@SESSION.sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')",
        "SET autocommit = 0",
    ] {
        let error = session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    for sql in [
        "SELECT 1 /*!50000 , 2 */",
        "/*!40101 SET autocommit=0 */ SELECT 1",
    ] {
        let error = session
            .fetch_all_async(sql, QueryOptions::new().limit(limit()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    // Validation happens before dispatch, so neither mode nor autocommit changed.
    let rows = fetch(
        session.as_mut(),
        "SELECT @@SESSION.autocommit AS autocommit, @@SESSION.sql_mode AS mode",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("autocommit").unwrap(), 1);
    assert!(
        !rows[0]
            .get::<String>("mode")
            .unwrap()
            .to_ascii_uppercase()
            .contains("NO_BACKSLASH_ESCAPES")
    );
}

async fn lock_wait_timeout_is_busy_not_a_transport_failure(provider: &MySqlPoolProvider) {
    let table = "neotales_lock";
    let mut holder = provider.acquire_async().await.unwrap();
    drop_table(holder.as_mut(), table).await;
    run(
        holder.as_mut(),
        &format!("CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)"),
    )
    .await;
    holder
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (?,?)"),
            QueryOptions::new().bind_all(&[i64_parameter(1), i64_parameter(1)]),
        )
        .await
        .unwrap();

    let mut holder_transaction = holder.begin_async(TxOptions::default()).await.unwrap();
    holder_transaction
        .execute_async(
            &format!("UPDATE `{table}` SET value = 2 WHERE id = 1"),
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut waiter = provider.acquire_async().await.unwrap();
    run(waiter.as_mut(), "SET SESSION innodb_lock_wait_timeout = 1").await;
    let mut waiter_transaction = waiter.begin_async(TxOptions::default()).await.unwrap();
    let error = waiter_transaction
        .execute_async(
            &format!("UPDATE `{table}` SET value = 3 WHERE id = 1"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Busy,
        "a lock wait timeout is a retry-classified server rejection"
    );
    drop(waiter_transaction);
    drop(waiter);
    // The invalidated waiter connection closes when its session is dropped. Give the
    // pool's close task a chance to release the metadata lock before the final DDL.
    tokio::time::sleep(Duration::from_millis(100)).await;
    holder_transaction.rollback_async().await.unwrap();
    drop_table(holder.as_mut(), table).await;
}

async fn read_only_transaction_rejects_writes(provider: &MySqlPoolProvider) {
    let table = "neotales_readonly";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE `{table}` (id BIGINT PRIMARY KEY)"),
    )
    .await;
    let mut transaction = session
        .begin_async(TxOptions {
            isolation: None,
            read_only: Some(true),
        })
        .await
        .unwrap();
    let error = transaction
        .execute_async(
            &format!("INSERT INTO `{table}` VALUES (1)"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    drop(transaction);
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn capabilities_and_policy_events_are_truthful(provider: &MySqlPoolProvider) {
    assert_eq!(
        provider.capabilities(),
        Capabilities {
            transactions: true,
            isolation_level: true,
            read_only_transactions: true,
            trusted_scripts: true,
        }
    );

    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let captured = Arc::clone(&events);
    let pool = PolicyPool::new(
        provider.clone(),
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(Arc::new(move |event| {
                captured.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    pool.fetch_all_async(
        "SELECT 1 AS value",
        QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
    )
    .await
    .unwrap();
    let events_now = events.lock().unwrap().clone();
    assert_eq!(events_now.len(), 1);
    assert_eq!(events_now[0].operation, Operation::Fetch);
    assert_eq!(events_now[0].outcome, TelemetryOutcome::Success);
    assert!(events_now[0].sql.is_none());

    let error = pool
        .fetch_all_async(
            "SELECT * FROM neotales_missing_table",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let events_now = events.lock().unwrap().clone();
    assert_eq!(events_now.len(), 2);
    assert_eq!(
        events_now[1].outcome,
        TelemetryOutcome::Error(ErrorKind::InvalidArgument)
    );
    assert!(events_now[1].sql.is_none());
}

#[cfg(unix)]
fn openssl(directory: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("openssl is required for the TLS regression");
    assert!(
        output.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Generates a disposable CA and a server certificate whose only SAN is
/// `127.0.0.1`. The files are readable by the unprivileged container user.
#[cfg(unix)]
fn make_certificates() -> (tempfile::TempDir, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("certificate directory");
    fs::write(
        directory.path().join("ext.cnf"),
        "subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n",
    )
    .expect("extension file");
    openssl(
        directory.path(),
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca-key.pem",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=Neotales Test CA",
        ],
    );
    openssl(
        directory.path(),
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server-key.pem",
            "-out",
            "server.csr",
            "-subj",
            "/CN=127.0.0.1",
        ],
    );
    openssl(
        directory.path(),
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca-key.pem",
            "-CAcreateserial",
            "-out",
            "server-cert.pem",
            "-days",
            "1",
            "-sha256",
            "-extfile",
            "ext.cnf",
        ],
    );
    for name in ["ca.pem", "server-cert.pem", "server-key.pem"] {
        fs::set_permissions(
            directory.path().join(name),
            fs::Permissions::from_mode(0o644),
        )
        .expect("certificate permissions");
    }
    let ca = directory.path().join("ca.pem");
    (directory, ca)
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires Docker and openssl; run tests/live.sh"]
async fn verified_tls_trusts_only_the_expected_ca_and_host() {
    let started = std::time::Instant::now();
    let (certificates, ca) = make_certificates();
    let certificate_directory = certificates.path();
    let password = disposable_password();
    let container = start_mysql(&password, Some(certificate_directory), &[]).await;
    assert_no_published_ports(&container);
    let target = bridge_address(&container).await;
    let probe_complete = Arc::new(AtomicBool::new(false));
    let port =
        loopback_proxy_with_probe_stage("127.0.0.1", target, Some(probe_complete.clone())).await;
    let wrong_host_port =
        loopback_proxy_with_probe_stage("127.0.0.2", target, Some(probe_complete.clone())).await;
    // The entrypoint logs readiness before it finishes creating root@'%'. A plaintext
    // probe retries that window; the TLS assertions below then run once against a
    // known-ready account.
    eprintln!(
        "fixture stage=plaintext_probe_start elapsed_ms={}",
        started.elapsed().as_millis()
    );
    drop(connect(port, &password).await);
    probe_complete.store(true, Ordering::SeqCst);
    eprintln!(
        "fixture stage=plaintext_probe_complete elapsed_ms={}",
        started.elapsed().as_millis()
    );

    eprintln!(
        "fixture stage=trusted_tls_start elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let trusted = match MySqlPoolProvider::connect(
        MySqlConfig::new("127.0.0.1", port, "root", "neotales_test")
            .password(&password)
            .tls(MySqlTls::VerifyIdentity)
            .ca_file(&ca)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await
    {
        Ok(provider) => provider,
        Err(error) => {
            eprintln!(
                "fixture stage=trusted_tls_failed category={:?}",
                error.kind()
            );
            panic!("trusted CA and matching host must connect");
        }
    };
    trusted
        .fetch_all_async(
            "SELECT 1 AS value",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    drop(trusted);

    let wrong_host = MySqlPoolProvider::connect(
        MySqlConfig::new("127.0.0.2", wrong_host_port, "root", "neotales_test")
            .password(&password)
            .tls(MySqlTls::VerifyIdentity)
            .ca_file(&ca)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await
    .expect_err("a certificate for 127.0.0.1 must not verify for 127.0.0.2");
    assert_eq!(wrong_host.kind(), ErrorKind::Connection);

    let untrusted = MySqlPoolProvider::connect(
        MySqlConfig::new("127.0.0.1", port, "root", "neotales_test")
            .password(&password)
            .tls(MySqlTls::VerifyIdentity)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await
    .expect_err("built-in roots must not trust the disposable CA");
    assert_eq!(untrusted.kind(), ErrorKind::Connection);

    // Encryption without verification is the only mode that accepts an unknown CA.
    let insecure = MySqlPoolProvider::connect(
        MySqlConfig::new("127.0.0.1", port, "root", "neotales_test")
            .password(&password)
            .tls(MySqlTls::Insecure)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await
    .expect("insecure TLS encrypts without verifying");
    insecure
        .fetch_all_async(
            "SELECT 1 AS value",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    drop(insecure);
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn discarding_a_session_releases_its_connection_lock() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &[]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;
    let provider = connect(port, &password).await;

    let name = "neotales-discard-lock";
    let mut session = provider.acquire_mysql_session().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT GET_LOCK(?, 1) AS acquired",
            QueryOptions::new()
                .bind(text(name))
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("acquired").unwrap(), 1);
    session.discard();
    drop(session);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut other = provider.acquire_async().await.unwrap();
    let rows = other
        .fetch_all_async(
            "SELECT IS_USED_LOCK(?) AS used",
            QueryOptions::new()
                .bind(text(name))
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get::<Option<i64>>("used").unwrap(),
        None,
        "a discarded session must not return a locked connection to the pool"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn inherited_autocommit_zero_is_established() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &["--autocommit=0"]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;
    let provider = connect(port, &password).await;

    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT @@SESSION.autocommit AS value",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
    session
        .execute_async(
            "CREATE TABLE autocommit_probe (id BIGINT PRIMARY KEY)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async(
            "INSERT INTO autocommit_probe VALUES (1)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT COUNT(*) AS value FROM autocommit_probe",
            QueryOptions::new().limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "the write must be visible without an explicit commit"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn inherited_no_backslash_escapes_mode_is_rejected() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &["--sql-mode=NO_BACKSLASH_ESCAPES"]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;

    for _ in 0..30 {
        let config = MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
            .expect("loopback configuration")
            .password(&password)
            .acquire_timeout(Duration::from_secs(5));
        match MySqlPoolProvider::connect(config).await {
            Ok(provider) => match provider.acquire_async().await {
                Err(error) if error.kind() == ErrorKind::Unsupported => return,
                Err(error) if error.kind() == ErrorKind::Connection => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(error) => panic!("unexpected mode-check error: {error:?}"),
                Ok(_) => panic!("a session with NO_BACKSLASH_ESCAPES must be rejected"),
            },
            Err(error) if error.kind() == ErrorKind::Connection => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => panic!("unexpected connection error: {error:?}"),
        }
    }
    panic!("disposable MySQL did not become ready for the mode regression");
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn inherited_ansi_quotes_is_rejected_before_the_payload_can_run() {
    let password = disposable_password();
    let container = start_mysql(&password, None, &["--sql-mode=ANSI_QUOTES"]).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy("127.0.0.1", bridge_address(&container).await).await;

    // Under ANSI_QUOTES the review payload
    // `SELECT 1 AS "a\"; COMMIT; SELECT 1 -- "` closes the double-quoted
    // identifier at the backslash-quote and then executes COMMIT. The provider must
    // refuse the session before any statement is scanned with the wrong quoting rules.
    for _ in 0..30 {
        let config = MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
            .expect("loopback configuration")
            .password(&password)
            .acquire_timeout(Duration::from_secs(5));
        match MySqlPoolProvider::connect(config).await {
            Ok(provider) => match provider.acquire_async().await {
                Err(error) if error.kind() == ErrorKind::Unsupported => return,
                Err(error) if error.kind() == ErrorKind::Connection => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(error) => panic!("unexpected ANSI_QUOTES error: {error:?}"),
                Ok(_) => panic!("a session with ANSI_QUOTES must be rejected"),
            },
            Err(error) if error.kind() == ErrorKind::Connection => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => panic!("unexpected connection error: {error:?}"),
        }
    }
    panic!("disposable MySQL did not become ready for the ANSI_QUOTES regression");
}
