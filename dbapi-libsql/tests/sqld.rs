//! Remote libSQL/Turso contract against a disposable Docker `sqld`.
//!
//! Requires Docker. The suite is `#[ignore]`d so a plain `cargo test` stays fixture-free;
//! `tests/live.sh` runs it explicitly. The image is pinned by index digest. The container
//! publishes no host port: the test reaches its bridge address through a loopback-only proxy,
//! matching the provider's cleartext-loopback exception.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    Cancellation, DbError, DbEvent, DbType, Delay, ErrorKind, ImmediateTransaction, Operation,
    Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions, QuerySetting, RetryPolicy, RetryRuntime,
    RetrySetting, RowLimit, Session, TelemetryOutcome, TxOptions, Value,
};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider, LibsqlSession};

#[path = "support/keyset_plan.rs"]
mod keyset_plan;
/// `libsql-server` 0.24.33 pinned by the published OCI index digest.
const IMAGE: &str = "ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb276d9d3604e4a48ac4a999a2e267814732d57d7e94c04ba71482333a67";
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const LONG_QUERY: &str = "WITH RECURSIVE counter(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM counter WHERE x < 50000000) SELECT count(*) FROM counter";

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

fn assert_no_published_ports(container: &Container) {
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
    let text = String::from_utf8_lossy(&inspect.stdout).trim().to_owned();
    assert!(
        text == "false|null" || text == "false|{}",
        "the container must not publish image-exposed ports: {text}"
    );
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).unwrap()
}

fn i64_parameter(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).unwrap()
}

fn text_parameter(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).unwrap()
}

async fn connect(port: u16) -> LibsqlPoolProvider {
    let mut last = ErrorKind::Connection;
    for _ in 0..30 {
        match LibsqlPoolProvider::connect(
            LibsqlConfig::plaintext_loopback(format!("http://127.0.0.1:{port}"), "")
                .expect("loopback configuration")
                .max_connections(8)
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

struct FlakyPool {
    inner: LibsqlPoolProvider,
    failures: AtomicUsize,
}

impl Pool for FlakyPool {
    fn capabilities(&self) -> neotales_dbapi::Capabilities {
        self.inner.capabilities()
    }

    fn acquire_async(&self) -> neotales_dbapi::DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move {
            if self
                .failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                Err(DbError::new(ErrorKind::Busy))
            } else {
                self.inner.acquire_async().await
            }
        })
    }
}

async fn reader_is_bounded_and_single_set(provider: &LibsqlPoolProvider) {
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

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)]
async fn sqld_remote_contract() {
    let _guard = TEST_LOCK.lock().await;
    let container = start_sqld();
    assert_no_published_ports(&container);
    let port = loopback_proxy(bridge_address(&container)).await;
    let provider = connect(port).await;

    // Configuration never downgrades a secure endpoint to cleartext.
    assert!(
        LibsqlPoolProvider::connect(
            LibsqlConfig::remote(format!("http://127.0.0.1:{port}"), "")
                .acquire_timeout(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
    assert!(
        LibsqlPoolProvider::connect(
            LibsqlConfig::remote(format!("https://127.0.0.1:{port}"), "")
                .acquire_timeout(Duration::from_secs(2))
        )
        .await
        .is_err(),
        "an HTTPS client must not fall back to cleartext"
    );

    let capabilities = provider.capabilities();
    assert!(capabilities.transactions);
    assert!(!capabilities.isolation_level);
    assert!(!capabilities.read_only_transactions);
    assert!(capabilities.trusted_scripts);
    reader_is_bounded_and_single_set(&provider).await;

    let mut session = provider.acquire_libsql_session().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE, score REAL, data BLOB)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async(
            "INSERT INTO users (id, name, score, data) VALUES (?, ?, ?, ?)",
            QueryOptions::new().bind_all(&[
                i64_parameter(1),
                text_parameter("alice"),
                Parameter::new(DbType::F64, Value::F64(1.5)).unwrap(),
                Parameter::new(DbType::Bytes, Value::Bytes(vec![1, 2])).unwrap(),
            ]),
        )
        .await
        .unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT id, name, score, data, NULL AS absent FROM users WHERE id = ?",
            QueryOptions::new()
                .bind(i64_parameter(1))
                .limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    let row = &rows[0];
    assert_eq!(row.get::<i64>("id").unwrap(), 1);
    assert_eq!(row.get::<String>("name").unwrap(), "alice");
    assert!((row.get::<f64>("score").unwrap() - 1.5).abs() < f64::EPSILON);
    assert_eq!(row.get::<Vec<u8>>("data").unwrap(), vec![1, 2]);
    assert_eq!(row.get::<Option<String>>("absent").unwrap(), None);
    assert_eq!(
        row.get::<String>("absent").unwrap_err().kind(),
        ErrorKind::Null
    );

    // Exact positional and placeholder behaviour.
    let rows = session
        .fetch_all_async(
            "SELECT ?2 AS second, ?1 AS first",
            QueryOptions::new()
                .bind_all(&[i64_parameter(10), i64_parameter(20)])
                .limit(limit(1, 1024)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get_ordinal::<i64>(0).unwrap(), 20);
    assert_eq!(rows[0].get_ordinal::<i64>(1).unwrap(), 10);
    for (sql, parameters, kind) in [
        (
            "SELECT ?, ?1",
            vec![i64_parameter(1), i64_parameter(2)],
            ErrorKind::InvalidArgument,
        ),
        ("SELECT ?", vec![], ErrorKind::InvalidArgument),
        ("SELECT :name", vec![], ErrorKind::Unsupported),
        ("SELECT 1; SELECT 2", vec![], ErrorKind::InvalidArgument),
        ("BEGIN", vec![], ErrorKind::InvalidArgument),
    ] {
        assert_eq!(
            session
                .execute_async(sql, QueryOptions::new().bind_all(&parameters))
                .await
                .unwrap_err()
                .kind(),
            kind,
            "{sql}"
        );
    }
    // A validation failure does not poison the session.
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit(1, 1024)))
            .await
            .unwrap()
            .len(),
        1
    );

    // Hard bounds fail instead of truncating.
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT 1 UNION ALL SELECT 2",
                QueryOptions::new().limit(limit(1, 1024))
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Overflow
    );
    let mut session = provider.acquire_libsql_session().await.unwrap();
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT 'abcdef' AS value",
                QueryOptions::new().limit(limit(1, 3))
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Overflow
    );

    // A remote server failure is opaque in libSQL 0.10, so a dispatched write is conservatively
    // unknown rather than guessed to be a constraint error.
    let mut session = provider.acquire_libsql_session().await.unwrap();
    assert_eq!(
        session
            .execute_async(
                "INSERT INTO users (id, name) VALUES (?, ?)",
                QueryOptions::new().bind_all(&[i64_parameter(1), text_parameter("duplicate")])
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );

    // Trusted scripts run several statements, and transactions cover DML.
    let mut session = provider.acquire_libsql_session().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE audit (id INTEGER); INSERT INTO audit (id) VALUES (1);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT count(*) AS count FROM audit",
                QueryOptions::new().limit(limit(1, 1024))
            )
            .await
            .unwrap()[0]
            .get::<i64>("count")
            .unwrap(),
        1
    );

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&[i64_parameter(2), text_parameter("bob")]),
        )
        .await
        .unwrap();
    assert_eq!(
        transaction
            .fetch_all_async(
                "SELECT count(*) AS count FROM users",
                QueryOptions::new().limit(limit(1, 1024))
            )
            .await
            .unwrap()[0]
            .get::<i64>("count")
            .unwrap(),
        2
    );
    transaction.commit_async().await.unwrap();

    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&[i64_parameter(3), text_parameter("carol")]),
        )
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT count(*) AS count FROM users",
                QueryOptions::new().limit(limit(1, 1024))
            )
            .await
            .unwrap()[0]
            .get::<i64>("count")
            .unwrap(),
        2
    );

    // Unsupported transaction options fail before dispatch.
    let Err(isolation_error) = session
        .begin_async(TxOptions {
            isolation: Some(neotales_dbapi::IsolationLevel::Serializable),
            read_only: None,
        })
        .await
    else {
        panic!("isolation request must fail before dispatch");
    };
    assert_eq!(isolation_error.kind(), ErrorKind::Unsupported);
    let Err(read_only_error) = session
        .begin_async(TxOptions {
            isolation: None,
            read_only: Some(true),
        })
        .await
    else {
        panic!("read-only request must fail before dispatch");
    };
    assert_eq!(read_only_error.kind(), ErrorKind::Unsupported);
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit(1, 1024)))
            .await
            .unwrap()
            .len(),
        1
    );

    // A dispatched statement deadline is an unknown write outcome, not a retryable timeout.
    let mut timed = provider.acquire_libsql_session().await.unwrap();
    assert_eq!(
        timed
            .execute_async(
                LONG_QUERY,
                QueryOptions::new().timeout(Duration::from_millis(10))
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        timed
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );

    // Dropping an in-flight future also poisons the session.
    let mut cancelled = provider.acquire_libsql_session().await.unwrap();
    {
        let future = cancelled.execute_async(LONG_QUERY, QueryOptions::new());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), future)
                .await
                .is_err()
        );
    }
    assert_eq!(
        cancelled
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );

    // PolicyPool telemetry and SQL capture.
    let events: Arc<Mutex<Vec<DbEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let observer: neotales_dbapi::DbObserver = {
        let events = Arc::clone(&events);
        Arc::new(move |event| events.lock().unwrap().push(event.clone()))
    };
    let sql_hook: neotales_dbapi::SqlCaptureHook = Arc::new(|sql| Some(sql.to_owned()));
    let policy = PoolPolicy {
        telemetry_enabled: true,
        sql_capture_enabled: true,
        sql_hook: Some(sql_hook),
        observer: Some(observer),
        retry: RetryPolicy::Disabled,
        retry_runtime: None,
        ..PoolPolicy::default()
    };
    let pool = PolicyPool::new(provider.clone(), policy);
    pool.fetch_all_async(
        "SELECT 7 AS value",
        QueryOptions::new().limit(limit(1, 1024)),
    )
    .await
    .unwrap();
    let captured = events.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].operation, Operation::Fetch);
    assert_eq!(captured[0].outcome, TelemetryOutcome::Success);
    assert_eq!(captured[0].sql.as_deref(), Some("SELECT 7 AS value"));

    // Per-query SQL capture off still emits sanitized telemetry.
    pool.fetch_all_async(
        "SELECT 8 AS value",
        QueryOptions::new()
            .sql_capture(QuerySetting::Off)
            .limit(limit(1, 1024)),
    )
    .await
    .unwrap();
    let captured = events.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    assert!(captured[1].sql.is_none());

    // PolicyPool retries a sanitized Busy before acquisition only when replay safety is explicit.
    let retry_runtime = RetryRuntime::new(
        Arc::new(Cancellation::new()),
        || Box::pin(std::future::pending()),
        |delay| Box::pin(tokio::time::sleep(delay)),
        || 0,
    );
    let flaky = PolicyPool::new(
        FlakyPool {
            inner: provider.clone(),
            failures: AtomicUsize::new(1),
        },
        PoolPolicy {
            telemetry_enabled: false,
            sql_capture_enabled: false,
            sql_hook: None,
            observer: None,
            retry: RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(1)), false).unwrap(),
            retry_runtime: Some(retry_runtime),
            ..PoolPolicy::default()
        },
    );
    flaky
        .execute_async(
            "SELECT 9",
            QueryOptions::new()
                .replay_safe()
                .retry(RetrySetting::Inherit),
        )
        .await
        .unwrap();

    // A failed statement can be followed by a fresh session, not by reuse of the discarded one.
    let mut fresh: LibsqlSession = provider.acquire_libsql_session().await.unwrap();
    assert_eq!(
        fresh
            .fetch_all_async(
                "SELECT 10 AS value",
                QueryOptions::new().limit(limit(1, 1024))
            )
            .await
            .unwrap()[0]
            .get::<i64>("value")
            .unwrap(),
        10
    );
}

#[cfg(target_os = "linux")]
fn peak_rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("proc status");
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|rest| rest.trim().strip_suffix("kB"))
                .and_then(|value| value.trim().parse().ok())
        })
        .expect("VmHWM")
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
    provider: &LibsqlPoolProvider,
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
async fn sqld_sql_page_source_contract() {
    let _guard = TEST_LOCK.lock().await;
    let container = start_sqld();
    assert_no_published_ports(&container);
    let port = loopback_proxy(bridge_address(&container)).await;
    let provider = connect(port).await;

    let mut session = provider.acquire_async().await.expect("seed session");
    session
        .execute_async("DROP TABLE IF EXISTS page_events", QueryOptions::new())
        .await
        .expect("drop");
    session
        .execute_async(
            "CREATE TABLE page_events (tenant INTEGER NOT NULL, seq INTEGER NOT NULL, \
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
                "INSERT INTO page_events (tenant, seq, body) VALUES (?, ?, ?)",
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

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires Docker; run tests/live.sh"]
async fn sqld_fetch_bounds_before_materializing_the_whole_result() {
    let _guard = TEST_LOCK.lock().await;
    let container = start_sqld();
    let port = loopback_proxy(bridge_address(&container)).await;
    let provider = connect(port).await;
    let mut session = provider.acquire_libsql_session().await.unwrap();
    // 20k rows of about 80 MiB total. A full client-side materialization would
    // move VmHWM by tens of MiB; the cursor path must stop after the second row.
    let before = peak_rss_kib();
    let error = session
        .fetch_all_async(
            "WITH RECURSIVE rows(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM rows WHERE x < 20000) SELECT randomblob(4096) AS payload FROM rows",
            QueryOptions::new().limit(limit(1, 8192)))
        .await
        .unwrap_err();
    let after = peak_rss_kib();
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert!(
        after.saturating_sub(before) < 32 * 1024,
        "fetch materialized too much: {before} KiB -> {after} KiB"
    );
}
