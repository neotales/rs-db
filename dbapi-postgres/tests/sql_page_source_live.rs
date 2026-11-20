//! PostgreSQL live coverage for the shared caller-authored SQL page source.
//!
//! Requires Docker. This test is `#[ignore]`d, so a plain `cargo test` for this target skips it;
//! `tests/sql_page_source_live.sh` selects it explicitly. The rest of the crate's nonignored
//! runtime tests still require the external PostgreSQL server on `127.0.0.1:55433`. The container
//! is pinned by digest, publishes no host port, and is created and removed by `testcontainers`.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    process::Command,
    sync::atomic::Ordering,
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbType, ErrorKind, Parameter, Pool, QueryOptions, Value,
    paging::{
        PageSize,
        multi::{
            PageBudget, PageSourceStream, PagedPage,
            sql::{SqlPageAssertion, SqlPageSource},
        },
    },
};
use neotales_dbapi_postgres::{Config, PoolOptions, PostgresPool, Profile};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

#[path = "support/keyset_plan.rs"]
mod keyset_plan;

/// `postgres:17-alpine` pinned by manifest digest, shared with the TLS live lane.
const IMAGE: &str = "postgres";
const TAG: &str =
    "17-alpine@sha256:d3244e8ce3f165ee3c93d9c4a4b1d0fe48b3a89e2287b717fdcbbde92d405065";
const READY: &str = "database system is ready to accept connections";
const PAGE_ADMIN_SQL: &str = "SELECT tenant, seq, body FROM page_events \
    WHERE (tenant, seq) > ($1, $2) ORDER BY tenant, seq LIMIT $3";
const PAGE_TENANT_SQL: &str = "SELECT tenant, seq, body FROM page_events \
    WHERE tenant = $1 AND (tenant, seq) > ($2, $3) ORDER BY tenant, seq LIMIT $4";

fn disposable_password() -> String {
    use std::fmt::Write as _;

    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes).expect("OS cryptographic random");
    let mut password = String::with_capacity(4 + bytes.len() * 2);
    password.push_str("Nt-");
    for byte in bytes {
        write!(&mut password, "{byte:02x}").expect("write to String");
    }
    password
}

async fn start_postgres(password: &str) -> ContainerAsync<GenericImage> {
    GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stdout_or_stderr(READY).with_times(2),
        ))
        .with_env_var("POSTGRES_PASSWORD", password)
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(HashMap::new());
        })
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("PostgreSQL container")
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
    assert!(inspect.status.success(), "docker inspect failed");
    assert_eq!(
        String::from_utf8_lossy(&inspect.stdout).trim(),
        "false|{}",
        "the container must not publish image-exposed ports"
    );
    let mapped = Command::new("docker")
        .args(["port", container.id(), "5432"])
        .output()
        .expect("docker port");
    assert!(
        mapped.stdout.is_empty(),
        "container port 5432 must have no host mapping"
    );
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

async fn bridge_address(container: &ContainerAsync<GenericImage>) -> SocketAddr {
    let ip: IpAddr = container
        .get_bridge_ip_address()
        .await
        .expect("container bridge address");
    SocketAddr::new(ip, 5432)
}

fn policy() -> PoolOptions {
    PoolOptions::new(
        8,
        Profile::normal(Duration::from_secs(5), Duration::from_millis(100)).expect("profile"),
        Duration::from_secs(60),
    )
    .expect("pool options")
}

async fn connect(port: u16, password: &str) -> PostgresPool {
    let mut last = ErrorKind::Connection;
    for _ in 0..60 {
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .user("postgres")
            .dbname("postgres")
            .password(password);
        match PostgresPool::loopback_plaintext_for_tests(config, policy()) {
            Ok(pool) => match pool.acquire_async().await {
                Ok(session) => {
                    drop(session);
                    return pool;
                }
                Err(error) => last = error.kind(),
            },
            Err(error) => last = error.kind(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("disposable PostgreSQL did not accept connections: {last:?}");
}

fn i64_parameter(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("integer parameter")
}

fn text_parameter(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("text parameter")
}

fn page_keys(pages: &[PagedPage]) -> Vec<(i64, i64)> {
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
    provider: &PostgresPool,
    sql: &'static str,
    scope: keyset_plan::Scope,
    start: (i64, i64),
) -> (Vec<PagedPage>, usize) {
    let plan = keyset_plan::KeysetPlan::new(sql, scope, start).expect("page plan");
    let requests = plan.requests();
    let source = SqlPageSource::new(
        provider.acquire_async().await.expect("page session"),
        plan,
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("page source");
    let pages = PageSourceStream::new(
        Box::new(source),
        PageSize::try_from(2).expect("page size"),
        PageBudget::new(100, 65_536, 10).expect("budget"),
    )
    .try_collect()
    .await
    .expect("pages");
    (pages, requests.load(Ordering::Relaxed))
}

#[tokio::test]
#[ignore = "requires Docker; run tests/sql_page_source_live.sh"]
async fn postgres_sql_page_source_contract() {
    let password = disposable_password();
    let container = start_postgres(&password).await;
    assert_no_published_ports(&container);
    let port = loopback_proxy(bridge_address(&container).await).await;
    let provider = connect(port, &password).await;

    let mut session = provider.acquire_async().await.expect("seed session");
    session
        .execute_async("DROP TABLE IF EXISTS page_events", QueryOptions::new())
        .await
        .expect("drop");
    session
        .execute_async(
            "CREATE TABLE page_events (tenant BIGINT NOT NULL, seq BIGINT NOT NULL, \
             body TEXT NOT NULL, PRIMARY KEY (tenant, seq))",
            QueryOptions::new(),
        )
        .await
        .expect("create");
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
                "INSERT INTO page_events (tenant, seq, body) VALUES ($1, $2, $3)",
                QueryOptions::new().bind_all(&[
                    i64_parameter(tenant),
                    i64_parameter(seq),
                    text_parameter(body),
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
    assert_eq!(admin.len(), 4);
    assert_eq!(admin_requests, 4);

    let (tenant, tenant_requests) = collect_pages(
        &provider,
        PAGE_TENANT_SQL,
        keyset_plan::Scope::Tenant(2),
        (2, 0),
    )
    .await;
    assert_eq!(page_keys(&tenant), vec![(2, 1), (2, 2)]);
    assert_eq!(tenant.len(), 1);
    assert_eq!(tenant_requests, 1);
}
