//! SQL Server integration coverage against a testcontainers-managed server.
//!
//! Requires Docker. `tests/live.sh` selects this ignored test. No host port is
//! published; the test uses a loopback-only proxy to the container bridge.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    sync::atomic::Ordering,
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    Capabilities, DbEvent, DbType, ErrorKind, Operation, Parameter, PolicyPool, Pool, PoolPolicy,
    QueryOptions, Row, RowLimit, Session, TelemetryOutcome, TxOptions, Value,
};
use neotales_dbapi_mssql::{MssqlConfig, MssqlPoolProvider, MssqlTls};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

#[path = "support/keyset_plan.rs"]
mod keyset_plan;

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
    let mut last = ErrorKind::Connection;
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
    if let Err(error) = session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
    {
        panic!("run failed for {sql:?}: {error:?}");
    }
}

async fn fetch(session: &mut (dyn Session + '_), sql: &str, parameters: &[Parameter]) -> Vec<Row> {
    match session
        .fetch_all_async(sql, QueryOptions::new().bind_all(parameters).limit(limit()))
        .await
    {
        Ok(rows) => rows,
        Err(error) => panic!("fetch failed for {sql:?}: {error:?}"),
    }
}

async fn drop_table(session: &mut (dyn Session + '_), name: &str) {
    run(session, &format!("DROP TABLE IF EXISTS dbo.{name}")).await;
}

const PAGE_ADMIN_SQL: &str = "SELECT TOP (@P3) tenant, seq, body FROM dbo.page_events \
    WHERE tenant > @P1 OR (tenant = @P1 AND seq > @P2) ORDER BY tenant, seq";
const PAGE_TENANT_SQL: &str = "SELECT TOP (@P4) tenant, seq, body FROM dbo.page_events \
    WHERE tenant = @P1 AND (tenant > @P2 OR (tenant = @P2 AND seq > @P3)) ORDER BY tenant, seq";

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
    provider: &MssqlPoolProvider,
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
async fn mssql_sql_page_source_contract() {
    let password = disposable_password();
    let container = start_mssql(&password).await;
    assert_no_published_ports(&container);
    let target = SocketAddr::new(
        container
            .get_bridge_ip_address()
            .await
            .expect("bridge address"),
        1433,
    );
    let port = loopback_proxy(target).await;
    let provider = connect(port, &password).await;

    let mut session = provider.acquire_async().await.expect("seed session");
    run(&mut *session, "DROP TABLE IF EXISTS dbo.page_events").await;
    run(
        &mut *session,
        "CREATE TABLE dbo.page_events (tenant BIGINT NOT NULL, seq BIGINT NOT NULL, \
         body NVARCHAR(64) NOT NULL, PRIMARY KEY (tenant, seq))",
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
                "INSERT INTO dbo.page_events (tenant, seq, body) VALUES (@P1, @P2, @P3)",
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
#[allow(clippy::too_many_lines)]
async fn mssql_provider_contract() {
    let password = disposable_password();
    let container = start_mssql(&password).await;
    assert_no_published_ports(&container);
    let ip: IpAddr = container
        .get_bridge_ip_address()
        .await
        .expect("bridge address");
    let port = loopback_proxy(SocketAddr::new(ip, 1433)).await;

    // The default verified-system-roots mode must reject the self-signed fixture.
    let verification = MssqlPoolProvider::connect(
        MssqlConfig::new("127.0.0.1", port, "sa", &password, "master")
            .tls(MssqlTls::VerifyIdentity)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await;
    assert!(
        verification.is_err(),
        "verified TLS must not trust the disposable self-signed certificate"
    );

    let provider = connect(port, &password).await;
    omitted_exec_is_rejected(&provider, port, &password).await;
    cr_terminated_line_comments_are_rejected(&provider, port, &password).await;
    round_trip_types(&provider).await;
    constraint_errors_are_sanitized(&provider).await;
    transactions_commit_rollback_and_isolation(&provider).await;
    update_statements_are_restricted_and_atomic(&provider).await;
    update_timeout_is_an_unknown_write(&provider).await;
    isolation_is_reset_on_checkout(&provider).await;
    app_lock_is_transaction_owned_and_released(&provider).await;
    multiple_sets_and_cleanup(&provider).await;
    cancelled_setup_does_not_poison_the_pool(&provider).await;
    transaction_control_and_indirect_are_rejected(&provider).await;
    fetch_bounds_fail_instead_of_truncating(&provider).await;
    timeout_is_an_unknown_write_and_discards(&provider).await;
    trusted_batches_and_go(&provider).await;
    transaction_drop_discards_uncommitted_work(&provider).await;
    capabilities_and_policy_events(&provider).await;

    drop(provider);
}

async fn omitted_exec_is_rejected(provider: &MssqlPoolProvider, port: u16, password: &str) {
    use sqlx::Connection as _;
    let url = format!("mssql://sa:{password}@127.0.0.1:{port}/master?trust_certificate=true");
    let mut direct = sqlx_mssql_rs_core::MssqlConnection::connect(&url)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE TABLE dbo.mssql_omitted_state (id INT)",
    ))
    .execute(&mut direct)
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE OR ALTER PROCEDURE dbo.mssql_omitted_exec AS INSERT INTO dbo.mssql_omitted_state VALUES (1)",
    ))
    .execute(&mut direct)
    .await
    .unwrap();
    // Prove this is executable server syntax and mutates state, without going through the guard.
    sqlx::raw_sql(sqlx::AssertSqlSafe("dbo.mssql_omitted_exec"))
        .execute(&mut direct)
        .await
        .unwrap();
    let count: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM dbo.mssql_omitted_state")
        .fetch_one(&mut direct)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let mut session = provider.acquire_async().await.unwrap();
    let result = session
        .fetch_async("dbo.mssql_omitted_exec", QueryOptions::new().limit(limit()))
        .await;
    assert_eq!(
        result.as_ref().err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    drop(result);
    let rows = fetch(
        session.as_mut(),
        "SELECT COUNT(*) AS value FROM dbo.mssql_omitted_state",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
    drop(session);
    sqlx::raw_sql(sqlx::AssertSqlSafe("DROP PROCEDURE dbo.mssql_omitted_exec"))
        .execute(&mut direct)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe("DROP TABLE dbo.mssql_omitted_state"))
        .execute(&mut direct)
        .await
        .unwrap();

    // Reproduce SQL Server's dangerous completion behavior outside the provider:
    // NOEXEC can acknowledge COMMIT without executing it, and suppresses the state query.
    sqlx::raw_sql(sqlx::AssertSqlSafe("BEGIN TRANSACTION"))
        .execute(&mut direct)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe("SET NOEXEC ON"))
        .execute(&mut direct)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe("COMMIT TRANSACTION"))
        .execute(&mut direct)
        .await
        .unwrap();
    let suppressed = sqlx::query(sqlx::AssertSqlSafe(
        "SELECT CAST(@@TRANCOUNT AS BIGINT), CAST(XACT_STATE() AS BIGINT)",
    ))
    .fetch_optional(&mut direct)
    .await
    .unwrap();
    assert!(suppressed.is_none());
    sqlx::raw_sql(sqlx::AssertSqlSafe("SET NOEXEC OFF"))
        .execute(&mut direct)
        .await
        .unwrap();
    let count: i32 = sqlx::query_scalar("SELECT @@TRANCOUNT")
        .fetch_one(&mut direct)
        .await
        .unwrap();
    assert_eq!(count, 1);
    sqlx::raw_sql(sqlx::AssertSqlSafe("ROLLBACK TRANSACTION"))
        .execute(&mut direct)
        .await
        .unwrap();
}

async fn cr_terminated_line_comments_are_rejected(
    provider: &MssqlPoolProvider,
    port: u16,
    password: &str,
) {
    use futures_util::TryStreamExt as _;
    use sqlx::Connection as _;

    let url = format!("mssql://sa:{password}@127.0.0.1:{port}/master?trust_certificate=true");
    let mut direct = sqlx_mssql_rs_core::MssqlConnection::connect(&url)
        .await
        .unwrap();
    // Raw server behavior: CR ends a line comment, so SET NOEXEC ON executes and suppresses
    // the next statement until it is explicitly turned off.
    let mut events = direct.fetch_events(sqlx::query("SELECT 1 AS a -- comment\rSET NOEXEC ON"));
    while events.try_next().await.unwrap().is_some() {}
    drop(events);
    assert!(
        sqlx::query("SELECT 1 AS a")
            .fetch_optional(&mut direct)
            .await
            .unwrap()
            .is_none(),
        "CR must end the line comment so SET NOEXEC ON takes effect"
    );
    sqlx::query("SET NOEXEC OFF")
        .execute(&mut direct)
        .await
        .unwrap();
    let value: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut direct)
        .await
        .unwrap();
    assert_eq!(value, 1);
    drop(direct);

    let mut session = provider.acquire_async().await.unwrap();
    let result = session
        .fetch_async(
            "SELECT 1 AS a -- comment\rSET NOEXEC ON",
            QueryOptions::new().limit(limit()),
        )
        .await;
    assert_eq!(
        result.as_ref().err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    drop(result);
    let rows = fetch(session.as_mut(), "SELECT 1 AS value", &[]).await;
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "rejection must happen before NOEXEC has any effect"
    );

    // Parameters after a CR comment are still validated and counted.
    let rows = fetch(
        session.as_mut(),
        "SELECT @P1 AS a, -- comment\r@P2 AS b",
        &[i64_parameter(1), i64_parameter(2)],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("a").unwrap(), 1);
    assert_eq!(rows[0].get::<i64>("b").unwrap(), 2);

    // An UPDATE with a hidden second statement after CR is rejected before dispatch and
    // has no effect.
    let table = "mssql_cr_update";
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)"),
    )
    .await;
    session
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (1, 1)"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    for sql in [
        format!("UPDATE dbo.{table} SET value = 2 -- comment\rSET NOEXEC ON"),
        format!("UPDATE dbo.{table} SET value = 2 -- comment\rDROP TABLE dbo.{table}"),
    ] {
        let error = session
            .execute_async(&sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT value FROM dbo.{table} WHERE id = 1"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "hidden statements must not run"
    );
    drop_table(session.as_mut(), table).await;
}

async fn round_trip_types(provider: &MssqlPoolProvider) {
    let table = "mssql_types";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!(
            "CREATE TABLE dbo.{table} (
                id BIGINT PRIMARY KEY,
                amount FLOAT NOT NULL,
                name NVARCHAR(64) NOT NULL,
                payload VARBINARY(8) NOT NULL,
                optional BIGINT NULL,
                flag BIT NOT NULL
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
                "INSERT INTO dbo.{table} (id, amount, name, payload, optional, flag) VALUES (@P1,@P2,@P3,@P4,@P5,@P6)"
            ),
            QueryOptions::new().bind_all(&parameters))
        .await
        .unwrap();

    let rows = fetch(
        session.as_mut(),
        &format!("SELECT id, amount, name, payload, optional, flag FROM dbo.{table}"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("id").unwrap(), 7);
    assert!((rows[0].get::<f64>("amount").unwrap() - 1.5).abs() < f64::EPSILON);
    assert_eq!(rows[0].get::<String>("name").unwrap(), "hello");
    assert_eq!(rows[0].get::<Vec<u8>>("payload").unwrap(), vec![1, 2, 3]);
    assert_eq!(rows[0].get::<Option<i64>>("optional").unwrap(), None);
    assert!(rows[0].get::<bool>("flag").unwrap());

    let rows = fetch(
        session.as_mut(),
        "SELECT CAST(1 AS INT) AS int_value, CAST(1 AS BIGINT) AS big_value, \
         CAST(1.5 AS FLOAT) AS float_value, CAST(1 AS BIT) AS bit_value, \
         CAST(N'x' AS NVARCHAR(10)) AS text_value, \
         CAST(0x0102 AS VARBINARY(4)) AS bytes_value, \
         CAST(NULL AS INT) AS null_value",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("int_value").unwrap(), 1);
    assert_eq!(rows[0].get::<i64>("big_value").unwrap(), 1);
    assert!((rows[0].get::<f64>("float_value").unwrap() - 1.5).abs() < f64::EPSILON);
    assert!(rows[0].get::<bool>("bit_value").unwrap());
    assert_eq!(rows[0].get::<String>("text_value").unwrap(), "x");
    assert_eq!(rows[0].get::<Vec<u8>>("bytes_value").unwrap(), vec![1, 2]);
    assert_eq!(rows[0].get::<Option<i64>>("null_value").unwrap(), None);

    let error = session
        .fetch_all_async(
            "SELECT CAST(1.25 AS DECIMAL(10,2)) AS decimal_value",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);

    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn constraint_errors_are_sanitized(provider: &MssqlPoolProvider) {
    let table = "mssql_unique";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, name NVARCHAR(32) UNIQUE)"),
    )
    .await;
    let parameters = [i64_parameter(1), text("secret-value")];
    session
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (@P1,@P2)"),
            QueryOptions::new().bind_all(&parameters),
        )
        .await
        .unwrap();
    let error = session
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (@P1,@P2)"),
            QueryOptions::new().bind_all(&parameters),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    let display = error.to_string();
    assert!(!display.contains(table));
    assert!(!display.contains("secret-value"));
    assert_eq!(
        session
            .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

#[allow(clippy::too_many_lines)] // Exercise rollback, commit and reuse of one session.
async fn transactions_commit_rollback_and_isolation(provider: &MssqlPoolProvider) {
    let table = "mssql_tx";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY)"),
    )
    .await;

    let mut transaction = session
        .begin_async(TxOptions {
            isolation: Some(neotales_dbapi::IsolationLevel::RepeatableRead),
            read_only: Some(false),
        })
        .await
        .unwrap();
    let rows = transaction
        .fetch_all_async(
            "SELECT transaction_isolation_level AS level \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("level").unwrap(), 3);
    transaction
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (@P1)"),
            QueryOptions::new().bind(i64_parameter(1)),
        )
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        "SELECT transaction_isolation_level AS level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        &[],
    ).await;
    assert_eq!(rows[0].get::<i64>("level").unwrap(), 2);
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS value FROM dbo.{table}"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 0);

    let mut transaction = session
        .begin_async(TxOptions {
            isolation: Some(neotales_dbapi::IsolationLevel::Serializable),
            read_only: None,
        })
        .await
        .unwrap();
    let error = transaction
        .execute_trusted_script_async("SET NOEXEC ON", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let result = transaction
        .fetch_async(
            "SET /* nested /* comment */ */ NOEXEC ON",
            QueryOptions::new().limit(limit()),
        )
        .await;
    assert_eq!(
        result.as_ref().err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    drop(result);
    transaction
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (@P1)"),
            QueryOptions::new().bind(i64_parameter(2)),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        "SELECT transaction_isolation_level AS level FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        &[],
    ).await;
    assert_eq!(rows[0].get::<i64>("level").unwrap(), 2);
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS value FROM dbo.{table}"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
    let mut independent = provider.acquire_async().await.unwrap();
    let rows = fetch(
        independent.as_mut(),
        &format!("SELECT COUNT(*) AS value FROM dbo.{table} WHERE id=2"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "commit must be visible on another session"
    );
    drop(independent);

    let Err(error) = session
        .begin_async(TxOptions {
            isolation: None,
            read_only: Some(true),
        })
        .await
    else {
        panic!("read-only transactions must be rejected before dispatch");
    };
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    drop_table(session.as_mut(), table).await;
}

#[allow(clippy::too_many_lines)] // One contract test covers grammar, rejection, and transaction effects.
async fn update_statements_are_restricted_and_atomic(provider: &MssqlPoolProvider) {
    let table = "mssql_update";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!(
            "CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, name NVARCHAR(64) NULL, flag BIT NOT NULL)"
        ),
    )
    .await;
    for id in [1, 2] {
        session
            .execute_async(
                &format!("INSERT INTO dbo.{table} (id, flag) VALUES (@P1, 0)"),
                QueryOptions::new().bind(i64_parameter(id)),
            )
            .await
            .unwrap();
    }

    let affected = session
        .execute_async(
            &format!(
                "/* leading */ UPDATE dbo.{table} SET name = @P1, flag = 1 -- trailing\n\
                 WHERE id = @P2;"
            ),
            QueryOptions::new().bind_all(&[text("first"), i64_parameter(1)]),
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT name, flag FROM dbo.{table} WHERE id = 1"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<Option<String>>("name").unwrap(),
        Some("first".to_owned())
    );
    assert!(rows[0].get::<bool>("flag").unwrap());

    // A complete UPDATE with quoted identifiers and a WHERE predicate is accepted.
    session
        .execute_async(
            &format!(
                "UPDATE [dbo].[{table}] SET [name] = N'second' WHERE [id] <> 1 AND [flag] IS NOT NULL"
            ),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT name FROM dbo.{table} WHERE id = 2"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<Option<String>>("name").unwrap(),
        Some("second".to_owned())
    );

    // Session settings, expressions, and hidden batches are rejected before dispatch.
    for sql in [
        "SET NOEXEC ON".to_owned(),
        "SET PARSEONLY ON".to_owned(),
        format!("UPDATE dbo.{table} SET flag = 1; SET NOEXEC ON"),
        format!("UPDATE dbo.{table} SET flag = 1 SET ANSI_DEFAULTS ON"),
        format!("UPDATE dbo.{table} SET name = (SELECT N'leak')"),
        format!("UPDATE dbo.{table} SET name = N'leak' FROM dbo.{table} AS other"),
        format!("UPDATE dbo.{table} SET name = N'leak' OUTPUT inserted.name"),
    ] {
        let error = session
            .execute_async(&sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS n FROM dbo.{table} WHERE name = N'leak'"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);

    // The same complete grammar gates trusted scripts; a session SET batch is rejected.
    session
        .execute_trusted_script_async(
            &format!("UPDATE dbo.{table} SET name = N'trusted' /* comment */ WHERE id = 2;"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let error = session
        .execute_trusted_script_async(
            &format!("UPDATE dbo.{table} SET name = N'unsafe' WHERE id = 2;\nSET NOEXEC ON"),
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT name FROM dbo.{table} WHERE id = 2"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<Option<String>>("name").unwrap(),
        Some("trusted".to_owned()),
        "a rejected batch must have no effect"
    );

    // UPDATE follows transaction semantics: rollback discards it, commit publishes it.
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("UPDATE dbo.{table} SET name = N'rolled-back' WHERE id = @P1"),
            QueryOptions::new().bind(i64_parameter(2)),
        )
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("UPDATE dbo.{table} SET name = N'committed' WHERE id = @P1"),
            QueryOptions::new().bind(i64_parameter(2)),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT name FROM dbo.{table} WHERE id = 2"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<Option<String>>("name").unwrap(),
        Some("committed".to_owned())
    );
    drop_table(session.as_mut(), table).await;
}

async fn update_timeout_is_an_unknown_write(provider: &MssqlPoolProvider) {
    let table = "mssql_update_timeout";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)"),
    )
    .await;
    session
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (@P1, 1)"),
            QueryOptions::new().bind(i64_parameter(1)),
        )
        .await
        .unwrap();

    let mut owner = provider.acquire_async().await.unwrap();
    let mut transaction = owner.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("UPDATE dbo.{table} SET value = 2 WHERE id = 1"),
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut contender = provider.acquire_async().await.unwrap();
    let error = contender
        .execute_async(
            &format!("UPDATE dbo.{table} SET value = 3 WHERE id = 1"),
            QueryOptions::new().timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        contender
            .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection,
        "an unknown write outcome must discard the connection"
    );
    drop(contender);
    transaction.rollback_async().await.unwrap();
    let rows = fetch(
        owner.as_mut(),
        &format!("SELECT value FROM dbo.{table} WHERE id = 1"),
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "the blocked update must not commit"
    );
    drop_table(owner.as_mut(), table).await;
}

async fn app_lock_is_transaction_owned_and_released(provider: &MssqlPoolProvider) {
    let options = TxOptions {
        isolation: None,
        read_only: None,
    };
    let mut holder = provider.acquire_mssql_session().await.unwrap();
    let holder_tx = holder
        .begin_app_locked_async(
            "neotales-dbapi-mssql/test-lock",
            Duration::from_secs(2),
            options,
        )
        .await
        .unwrap();

    let mut waiter = provider.acquire_mssql_session().await.unwrap();
    let Err(error) = waiter
        .begin_app_locked_async(
            "neotales-dbapi-mssql/test-lock",
            Duration::from_millis(100),
            options,
        )
        .await
    else {
        panic!("a held transaction lock must not be granted");
    };
    assert_eq!(error.kind(), ErrorKind::Timeout);
    let mut cancelled = provider.acquire_mssql_session().await.unwrap();
    let cancel_result = tokio::time::timeout(
        Duration::from_millis(100),
        cancelled.begin_app_locked_async(
            "neotales-dbapi-mssql/test-lock",
            Duration::from_secs(30),
            options,
        ),
    )
    .await;
    assert!(cancel_result.is_err(), "the waiter must still be blocked");
    drop(cancel_result);
    drop(cancelled);
    let Err(error) = waiter
        .begin_app_locked_async("neotales-dbapi-mssql/test-lock", Duration::ZERO, options)
        .await
    else {
        panic!("a zero timeout must be rejected");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let Err(error) = waiter
        .begin_app_locked_async(
            "neotales-dbapi-mssql/test-lock",
            Duration::from_secs(1),
            TxOptions {
                isolation: None,
                read_only: Some(true),
            },
        )
        .await
    else {
        panic!("read-only must be rejected before dispatch");
    };
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    holder_tx.commit_async().await.unwrap();
    let waiter_tx = waiter
        .begin_app_locked_async(
            "neotales-dbapi-mssql/test-lock",
            Duration::from_secs(2),
            options,
        )
        .await
        .unwrap();
    waiter_tx.rollback_async().await.unwrap();
}

#[allow(clippy::too_many_lines)] // Exercises one session's reader and transaction lifecycle.
async fn multiple_sets_and_cleanup(provider: &MssqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    run(
        session.as_mut(),
        "DROP TABLE IF EXISTS dbo.mssql_overflow_write",
    )
    .await;
    run(
        session.as_mut(),
        "CREATE TABLE dbo.mssql_overflow_write (id BIGINT)",
    )
    .await;
    let error = session.execute_async(
        "INSERT INTO dbo.mssql_overflow_write VALUES (1)\nINSERT INTO dbo.mssql_overflow_write VALUES (2)",
        QueryOptions::new(),
    ).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    let rows = fetch(
        session.as_mut(),
        "SELECT COUNT(*) AS value FROM dbo.mssql_overflow_write",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 0);
    let overflow_result = session.fetch_async(
        "SELECT CAST(1 AS BIGINT) AS a UNION ALL SELECT CAST(2 AS BIGINT); WAITFOR DELAY '00:00:00.2'; INSERT INTO dbo.mssql_overflow_write VALUES (9)",
        QueryOptions::new().limit(RowLimit::with_max_sets(1, 4096, 2).unwrap()),
    ).await;
    assert_eq!(
        overflow_result.as_ref().err().unwrap().kind(),
        ErrorKind::Overflow
    );
    drop(overflow_result);
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        "SELECT COUNT(*) AS value FROM dbo.mssql_overflow_write",
        &[],
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("value").unwrap(),
        1,
        "overflow was returned before the later write completed"
    );
    run(session.as_mut(), "DROP TABLE dbo.mssql_overflow_write").await;
    let limits = RowLimit::with_max_sets(10, 4096, 3).unwrap();
    let mut reader = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a\nSELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(limits),
        )
        .await
        .unwrap();
    let sets = reader.read_sets_async().await.unwrap();
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0][0].get::<i64>("a").unwrap(), 1);
    assert_eq!(sets[1][0].get::<i64>("b").unwrap(), 2);
    drop(reader);

    let mut reader = session
        .fetch_async(
            "SELECT CAST(@P1 AS BIGINT) AS a\nSELECT CAST(@P2 AS BIGINT) AS b",
            QueryOptions::new()
                .bind(i64_parameter(7))
                .bind(i64_parameter(8))
                .limit(limits),
        )
        .await
        .unwrap();
    let sets = reader.read_sets_async().await.unwrap();
    assert_eq!(sets[0][0].get::<i64>("a").unwrap(), 7);
    assert_eq!(sets[1][0].get::<i64>("b").unwrap(), 8);
    drop(reader);

    // SQL Server's current request text is the actual statement sent to sp_executesql.
    let original = "SELECT /* ? @P99 /* nested */ */ CAST(text AS NVARCHAR(MAX)) AS sql_text FROM sys.dm_exec_requests CROSS APPLY sys.dm_exec_sql_text(sql_handle) WHERE session_id = @@SPID AND 1 = @P1 AND 1 = @P1";
    let rows = fetch(session.as_mut(), original, &[i64_parameter(1)]).await;
    let observed = rows[0].get::<String>("sql_text").unwrap();
    assert_eq!(observed.strip_prefix("(@P1 bigint )"), Some(original));
    for bad in [
        "SELECT @P2",
        "SELECT @P1, @P3",
        "SELECT @local",
        "SELECT @P01",
        "SELECT @P1foo",
        "SELECT ?",
        "SET @P1/**/=2",
        "SELECT @P1 /* outer /* nested */ */ = 2",
        "SELECT @P1-- assignment\n+=2",
        "SELECT @P1 ^= 2",
    ] {
        let result = session
            .fetch_async(
                bad,
                QueryOptions::new().bind(i64_parameter(1)).limit(limits),
            )
            .await;
        assert_eq!(
            result.as_ref().err().unwrap().kind(),
            ErrorKind::InvalidArgument,
            "{bad}"
        );
        drop(result);
    }
    assert_eq!(
        fetch(session.as_mut(), "SELECT CAST(3 AS BIGINT) AS x", &[]).await[0]
            .get::<i64>("x")
            .unwrap(),
        3
    );

    let mut reader = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a WHERE 1=0; SELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(limits),
        )
        .await
        .unwrap();
    let sets = reader.read_sets_async().await.unwrap();
    assert_eq!(sets.len(), 2);
    assert!(sets[0].is_empty());
    assert_eq!(sets[1][0].get::<i64>("b").unwrap(), 2);
    drop(reader);

    let reader = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a; SELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(limits),
        )
        .await
        .unwrap();
    drop(reader);
    assert_eq!(
        fetch(session.as_mut(), "SELECT CAST(3 AS BIGINT) AS x", &[]).await[0]
            .get::<i64>("x")
            .unwrap(),
        3
    );

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let mut reader = transaction
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a; SELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(limits),
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .next_row_async()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>("a")
            .unwrap(),
        1
    );
    reader.finish_async().await.unwrap();
    drop(reader);
    transaction.commit_async().await.unwrap();

    let result = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a; SELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(RowLimit::with_max_sets(10, 4096, 1).unwrap()),
        )
        .await;
    assert!(result.is_err(), "set limit must not truncate");
    drop(result);
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let result = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a; SELECT CAST(2 AS BIGINT) AS b",
            QueryOptions::new().limit(RowLimit::with_max_sets(1, 4096, 2).unwrap()),
        )
        .await;
    assert!(result.is_err(), "row limit applies across sets");
    drop(result);
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let result = session
        .fetch_async(
            "SELECT CAST(1 AS BIGINT) AS a; THROW 50001, 'late failure', 1",
            QueryOptions::new().limit(limits),
        )
        .await;
    assert!(
        result.is_err(),
        "late failure must be observed before returning a reader"
    );
    drop(result);
    assert!(
        session
            .fetch_async("SELECT 1", QueryOptions::new().limit(limit()))
            .await
            .is_err()
    );
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let cancelled = tokio::time::timeout(
        Duration::from_millis(100),
        session.fetch_async(
            "WAITFOR DELAY '00:00:05'; SELECT CAST(1 AS BIGINT) AS a",
            QueryOptions::new().limit(limits),
        ),
    )
    .await;
    assert!(cancelled.is_err());
    drop(cancelled);
    assert!(
        session
            .fetch_async("SELECT 1", QueryOptions::new().limit(limit()))
            .await
            .is_err()
    );
}

async fn transaction_control_and_indirect_are_rejected(provider: &MssqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    for sql in [
        "COMMIT TRANSACTION",
        "BEGIN TRANSACTION",
        "USE master",
        "SELECT 1\nBEGIN TRANSACTION",
        "SELECT 1\nGO\nSELECT 2",
    ] {
        let error = session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{sql}");
    }
    for sql in [
        "EXEC('SELECT 1')",
        "sp_who",
        "SET IMPLICIT_TRANSACTIONS ON",
        "SET /* comment */ IMPLICIT_TRANSACTIONS ON",
        "SET ANSI_DEFAULTS ON",
        "SET NOEXEC ON",
        "SET PARSEONLY ON",
        "SET FMTONLY ON",
        "SET SHOWPLAN_XML ON",
    ] {
        let error = session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{sql}");
    }
    for sql in [
        "SELECT 'GO' AS value",
        "SELECT 1 -- GO",
        "SELECT 1 /* GO */",
    ] {
        let rows = fetch(session.as_mut(), sql, &[]).await;
        assert_eq!(rows[0].get::<i64>("value").unwrap_or(1), 1);
    }
    // A marker inside a nested comment does not bind the supplied argument.
    let error = session
        .fetch_all_async(
            "SELECT /* outer /* inner */ ? */ 1",
            QueryOptions::new().bind(i64_parameter(1)).limit(limit()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let rows = fetch(session.as_mut(), "SELECT 1 AS value", &[]).await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
}

async fn fetch_bounds_fail_instead_of_truncating(provider: &MssqlPoolProvider) {
    let table = "mssql_bounds";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, payload NVARCHAR(100))"),
    )
    .await;
    for id in 1..=3 {
        session
            .execute_async(
                &format!("INSERT INTO dbo.{table} VALUES (@P1,@P2)"),
                QueryOptions::new().bind_all(&[i64_parameter(id), text("0123456789")]),
            )
            .await
            .unwrap();
    }
    let error = session
        .fetch_all_async(
            &format!("SELECT id, payload FROM dbo.{table}"),
            QueryOptions::new().limit(RowLimit::new(2, 65_536).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow);
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            &format!("SELECT id, payload FROM dbo.{table}"),
            QueryOptions::new().limit(RowLimit::new(10, 8).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow);
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
}

async fn timeout_is_an_unknown_write_and_discards(provider: &MssqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .execute_trusted_script_async(
            "WAITFOR DELAY '00:00:00.500'",
            QueryOptions::new().timeout(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        session
            .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
}

async fn trusted_batches_and_go(provider: &MssqlPoolProvider) {
    let table = "mssql_script";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    session
        .execute_trusted_script_async(
            &format!(
                "CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY); \
                 INSERT INTO dbo.{table} VALUES (1); \
                 INSERT INTO dbo.{table} VALUES (2);"
            ),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!("SELECT COUNT(*) AS value FROM dbo.{table}"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 2);

    let ddl = "mssql_tx_ddl";
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_trusted_script_async(
            &format!("CREATE TABLE dbo.{ddl} (id BIGINT PRIMARY KEY)"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    let rows = fetch(
        session.as_mut(),
        &format!(
            "SELECT CASE WHEN OBJECT_ID(N'dbo.{ddl}', N'U') IS NULL THEN 0 ELSE 1 END AS value"
        ),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 0);

    let error = session
        .execute_trusted_script_async("SELECT 1\nGO\nSELECT 2", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    drop_table(session.as_mut(), table).await;
}

async fn transaction_drop_discards_uncommitted_work(provider: &MssqlPoolProvider) {
    let table = "mssql_drop_tx";
    let mut session = provider.acquire_async().await.unwrap();
    drop_table(session.as_mut(), table).await;
    run(
        session.as_mut(),
        &format!("CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY)"),
    )
    .await;
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            &format!("INSERT INTO dbo.{table} VALUES (1)"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(transaction);
    assert_eq!(
        session
            .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut fresh = provider.acquire_async().await.unwrap();
    let rows = fetch(
        fresh.as_mut(),
        &format!("SELECT COUNT(*) AS value FROM dbo.{table}"),
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 0);
    drop_table(fresh.as_mut(), table).await;
}

async fn capabilities_and_policy_events(provider: &MssqlPoolProvider) {
    assert_eq!(
        provider.capabilities(),
        Capabilities {
            transactions: true,
            isolation_level: true,
            read_only_transactions: false,
            trusted_scripts: true,
        }
    );
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::<DbEvent>::new()));
    let captured = std::sync::Arc::clone(&events);
    let pool = PolicyPool::new(
        provider.clone(),
        PoolPolicy {
            telemetry_enabled: true,
            observer: Some(std::sync::Arc::new(move |event| {
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
            "SELECT * FROM dbo.neotales_missing_table",
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
}

async fn isolation_is_reset_on_checkout(provider: &MssqlPoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let mut transaction = session
        .begin_async(TxOptions {
            isolation: Some(neotales_dbapi::IsolationLevel::Serializable),
            read_only: Some(false),
        })
        .await
        .unwrap();
    let rows = transaction
        .fetch_all_async(
            "SELECT transaction_isolation_level AS level \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("level").unwrap(), 4);
    transaction.rollback_async().await.unwrap();
    drop(session);

    // A different checkout must observe SQL Server's default, not the last request.
    let mut fresh = provider.acquire_async().await.unwrap();
    let rows = fresh
        .fetch_all_async(
            "SELECT transaction_isolation_level AS level \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
            QueryOptions::new().limit(limit()),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].get::<i64>("level").unwrap(),
        2,
        "isolation must be reset when a pooled connection is checked out"
    );
}

async fn cancelled_setup_does_not_poison_the_pool(provider: &MssqlPoolProvider) {
    // Repeatedly cancel acquisition at very short deadlines. The setup guard must
    // discard any connection whose setup did not finish.
    for _ in 0..32 {
        let _ = tokio::time::timeout(Duration::from_micros(1), provider.acquire_async()).await;
    }
    let mut session = provider.acquire_async().await.unwrap();
    let rows = fetch(session.as_mut(), "SELECT 1 AS value", &[]).await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
}
