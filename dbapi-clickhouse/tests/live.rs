//! Native `ClickHouse` provider contract against a pinned testcontainers-managed server.
//!
//! Requires Docker. The suite is `#[ignore]`d so a plain `cargo test` stays fixture-free;
//! `tests/live.sh` runs it explicitly. The container is created and removed by `testcontainers`,
//! so there is no fixed host port and no shell cleanup to get wrong.

use std::{
    net::{IpAddr, SocketAddr},
    process::Command,
    time::Duration,
};

use futures_util::{StreamExt, TryStreamExt};
use klickhouse::{Client, ClientOptions};
use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, Pool, QueryOptions, Row, RowLimit, Session, TxOptions,
    Value,
};
use neotales_dbapi_clickhouse::{
    ClickHouseConfig, ClickHousePoolProvider, validate_no_count_statement,
};
#[cfg(unix)]
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
#[cfg(unix)]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};

#[path = "support/keyset_plan.rs"]
mod keyset_plan;
#[cfg(unix)]
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, pki_types::PrivateKeyDer},
};

/// `ClickHouse` 24.8 LTS pinned by manifest digest.
const IMAGE: &str = "clickhouse/clickhouse-server";
const TAG: &str =
    "24.8-alpine@sha256:b002e56ed5c16e224c312527f6fcba7e77216fec5d7a88a7828f59efc614feb5";
const NATIVE_PORT: u16 = 9000;

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
    let image = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::seconds(2))
        .with_env_var("CLICKHOUSE_PASSWORD", password)
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_host_config_modifier(|host_config| {
            host_config.publish_all_ports = Some(false);
            host_config.port_bindings = Some(std::collections::HashMap::new());
        });
    image.start().await.expect("ClickHouse container")
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
    for port in ["8123", "9000"] {
        let mapped = Command::new("docker")
            .args(["port", container.id(), port])
            .output()
            .expect("docker port");
        assert!(
            mapped.stdout.is_empty(),
            "container port {port} must have no host mapping"
        );
    }
}

async fn loopback_proxy(bind: &str, target: SocketAddr) -> u16 {
    let listener = tokio::net::TcpListener::bind((bind, 0))
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
    SocketAddr::new(ip, NATIVE_PORT)
}

async fn connect(port: u16, password: &str, request_timeout: Duration) -> ClickHousePoolProvider {
    let mut last = ErrorKind::Connection;
    for _ in 0..30 {
        let config = ClickHouseConfig::plaintext_loopback("127.0.0.1", port, "default", "default")
            .expect("loopback configuration")
            .password(password)
            .request_timeout(request_timeout)
            .connect_timeout(Duration::from_secs(2));
        match ClickHousePoolProvider::connect(config).await {
            Ok(provider) => return provider,
            Err(error) => {
                last = error.kind();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!("disposable ClickHouse did not accept connections: {last:?}");
}

async fn raw_client(address: SocketAddr, password: &str) -> Client {
    let options = ClientOptions {
        username: "default".to_owned(),
        password: password.to_owned(),
        default_database: "default".to_owned(),
        ..ClientOptions::default()
    };
    Client::connect(address, options)
        .await
        .expect("raw fixture connection")
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("positive limits")
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("text parameter")
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("integer parameter")
}

fn boolean(value: bool) -> Parameter {
    Parameter::new(DbType::Bool, Value::Bool(value)).expect("bool parameter")
}

fn bytes(value: Vec<u8>) -> Parameter {
    Parameter::new(DbType::Bytes, Value::Bytes(value)).expect("bytes parameter")
}

fn float(value: f64) -> Parameter {
    Parameter::new(DbType::F64, Value::F64(value)).expect("float parameter")
}

async fn fetch(
    session: &mut (dyn Session + '_),
    sql: &str,
    parameters: &[Parameter],
) -> Result<Vec<Row>, DbError> {
    session
        .fetch_all_async(
            sql,
            QueryOptions::new()
                .bind_all(parameters)
                .limit(limit(100, 65_536)),
        )
        .await
}

async fn assert_ok(
    session: &mut (dyn Session + '_),
    sql: &str,
    parameters: &[Parameter],
) -> Vec<Row> {
    fetch(session, sql, parameters)
        .await
        .unwrap_or_else(|error| panic!("fetch failed for {sql:?}: {error:?}"))
}

#[allow(clippy::needless_pass_by_value)] // Assertions consume the result they inspect.
fn assert_kind(result: Result<impl Sized, DbError>, expected: ErrorKind, context: &str) {
    match result {
        Ok(_) => panic!("{context}: expected {expected:?}, got success"),
        Err(error) => assert_eq!(error.kind(), expected, "{context}: {error:?}"),
    }
}

async fn raw_scalar(client: &Client, sql: &str) -> i64 {
    let mut events = client
        .query_events(sql, None)
        .await
        .expect("raw scalar query");
    let mut found = None;
    while let Some(event) = events.next().await {
        match event.expect("raw scalar event") {
            klickhouse::QueryEvent::Data(block) => {
                if found.is_none()
                    && let Some(value) = block
                        .column_data
                        .values()
                        .next()
                        .and_then(|values| values.first())
                {
                    found = Some(match value {
                        klickhouse::Value::Int64(value) => *value,
                        klickhouse::Value::UInt64(value) => {
                            i64::try_from(*value).expect("scalar fits i64")
                        }
                        klickhouse::Value::UInt8(value) => i64::from(*value),
                        other => panic!("unexpected raw scalar: {other:?}"),
                    });
                }
            }
            klickhouse::QueryEvent::EndOfStream => break,
        }
    }
    found.expect("raw scalar row missing")
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[allow(clippy::too_many_lines)] // One container and one ordered contract.
async fn clickhouse_native_provider_contract() {
    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    assert_no_published_ports(&container);
    let target = bridge_address(&container).await;
    let port = loopback_proxy("127.0.0.1", target).await;
    let provider = connect(port, &password, Duration::from_secs(10)).await;

    capabilities_are_truthful(&provider).await;
    version_is_gated(&provider).await;
    result_types_are_exact(&provider).await;
    nested_comments_are_scanned(&provider).await;
    binds_native_parameters(&provider).await;
    binds_declared_integer_types(&provider).await;
    nullable_parameter_and_result_roundtrip(&provider).await;
    rejects_unsupported_parameters(&provider).await;
    bounds_fail_closed(&provider).await;
    options_and_reader_semantics(&provider).await;
    local_failures_do_not_poison(&provider).await;
    timeouts_poison_after_dispatch(&provider).await;
    late_server_errors_are_unknown(&provider).await;
    let raw = raw_client(target, &password).await;
    alternate_placeholder_is_rejected_before_dispatch(&provider, &raw).await;
    unsupported_operations_are_predispatch(&provider, &raw).await;
    no_count_writes(&provider, &raw).await;
    observed_no_count_writes(&provider, &raw, target, &password).await;
    response_loss_after_applied_write(target, &password, &raw).await;
    escaped_backticks_are_server_compatible(&provider).await;
    synchronous_insert_overrides_server_defaults(port, target, &raw).await;
    settings_scanner_agrees_with_the_server_parser(&provider, &raw).await;
    duplicate_lock_create_is_not_classified_by_public_api(&provider, target, &password, &raw).await;

    drop(provider);
}

const PAGE_ADMIN_SQL: &str = "SELECT tenant, seq, body FROM nt_page_events \
    WHERE (tenant, seq) > ({p1:Int64}, {p2:Int64}) ORDER BY tenant, seq LIMIT {p3:Int64}";
const PAGE_TENANT_SQL: &str = "SELECT tenant, seq, body FROM nt_page_events \
    WHERE tenant = {p1:Int64} AND (tenant, seq) > ({p2:Int64}, {p3:Int64}) \
    ORDER BY tenant, seq LIMIT {p4:Int64}";

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
    provider: &ClickHousePoolProvider,
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
    (pages, requests.load(std::sync::atomic::Ordering::Relaxed))
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn clickhouse_sql_page_source_contract() {
    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    assert_no_published_ports(&container);
    let target = bridge_address(&container).await;
    let port = loopback_proxy("127.0.0.1", target).await;
    let provider = connect(port, &password, Duration::from_secs(10)).await;
    let raw = raw_client(target, &password).await;

    raw.execute("DROP TABLE IF EXISTS nt_page_events")
        .await
        .expect("drop table");
    raw.execute(
        "CREATE TABLE nt_page_events (tenant Int64, seq Int64, body String) \
         ENGINE=MergeTree ORDER BY (tenant, seq)",
    )
    .await
    .expect("create table");
    raw.execute(
        "INSERT INTO nt_page_events VALUES \
         (1, 1, 'alpha'), (1, 2, 'bravo'), (1, 3, 'charlie'), \
         (2, 1, 'delta'), (2, 2, 'echo'), (3, 1, 'foxtrot'), (3, 2, 'golf')",
    )
    .await
    .expect("seed rows");

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

    raw.execute("DROP TABLE IF EXISTS nt_page_events")
        .await
        .expect("drop table");
}

async fn alternate_placeholder_is_rejected_before_dispatch(
    provider: &ClickHousePoolProvider,
    raw: &Client,
) {
    raw.execute("CREATE TABLE neotales_placeholder_probe (s String) ENGINE=Memory")
        .await
        .unwrap();
    raw.execute("INSERT INTO neotales_placeholder_probe VALUES ('seed')")
        .await
        .unwrap();
    let sql = "INSERT INTO neotales_placeholder_probe SELECT {p1:String} FROM { p1:Identifier }";
    // A raw native value demonstrates the server grammar independently of the provider's
    // String-specific hex encoding (which Identifier interprets differently).
    let encoded = vec![("p1".to_owned(), "'neotales_placeholder_probe'".to_owned())];
    let mut events = raw.query_bound_validated(sql, encoded).await.unwrap();
    let mut terminal = false;
    while let Some(event) = events.next().await {
        if matches!(event.unwrap(), klickhouse::QueryEvent::EndOfStream) {
            terminal = true;
        }
    }
    assert!(terminal, "server must interpret both declarations");
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_placeholder_probe"
        )
        .await,
        2
    );
    let mut session = provider.acquire_clickhouse().await.unwrap();
    let error = session
        .execute_no_count(
            sql,
            QueryOptions::new().bind(text("neotales_placeholder_probe")),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_placeholder_probe"
        )
        .await,
        2,
        "rejected write must not be dispatched"
    );
    raw.execute("DROP TABLE neotales_placeholder_probe")
        .await
        .unwrap();
}

#[allow(clippy::too_many_lines)]
async fn observed_no_count_writes(
    provider: &ClickHousePoolProvider,
    raw: &Client,
    target: SocketAddr,
    password: &str,
) {
    use neotales_dbapi::{
        DbEvent, Delay, QuerySetting, RetryPolicy, RetrySetting, TelemetryOutcome,
    };
    use neotales_dbapi_clickhouse::ClickHouseTelemetry;
    use std::sync::{Arc, Mutex};

    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let sink = Arc::clone(&events);
    let observed = provider.clone().with_telemetry(ClickHouseTelemetry {
        telemetry_enabled: true,
        observer: Some(Arc::new(move |event| {
            sink.lock().unwrap().push(event.clone());
        })),
        ..ClickHouseTelemetry::default()
    });
    let mut session = observed.acquire_clickhouse().await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE nt_observed_provider (s String) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_no_count(
            "INSERT INTO nt_observed_provider SELECT {p1:String}",
            QueryOptions::new().bind(text("bound-secret")),
        )
        .await
        .unwrap();
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| event.outcome == TelemetryOutcome::Success && event.sql.is_none())
        );
    }
    let sql = "INSERT INTO nt_observed_provider SELECT 'literal-secret'";
    session
        .execute_no_count(
            sql,
            QueryOptions::new()
                .sql_capture(QuerySetting::On)
                .sql_hook(Arc::new(|_| Some("redacted".to_owned()))),
        )
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap().last().unwrap().sql.as_deref(),
        Some("redacted")
    );
    session
        .execute_no_count(
            "INSERT INTO nt_observed_provider SELECT 'capture-opt-in'",
            QueryOptions::new().sql_capture(QuerySetting::On),
        )
        .await
        .unwrap();
    assert_eq!(
        events.lock().unwrap().last().unwrap().sql.as_deref(),
        Some("INSERT INTO nt_observed_provider SELECT 'capture-opt-in'")
    );
    let count = events.lock().unwrap().len();
    session
        .execute_no_count(
            "INSERT INTO nt_observed_provider SELECT 'silent'",
            QueryOptions::new()
                .telemetry(QuerySetting::Off)
                .sql_capture(QuerySetting::On),
        )
        .await
        .unwrap();
    assert_eq!(events.lock().unwrap().len(), count);

    let error = session
        .execute_no_count("invalid statement", QueryOptions::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::InvalidArgument | ErrorKind::Unsupported
    ));
    assert_eq!(
        events.lock().unwrap().last().unwrap().outcome,
        TelemetryOutcome::Error(error.kind())
    );
    let retry = RetryPolicy::new(2, Delay::Fixed(Duration::ZERO), false).unwrap();
    let error = session
        .execute_no_count(
            "INSERT INTO nt_observed_provider SELECT 'must-not-run'",
            QueryOptions::new()
                .retry(RetrySetting::Use(retry))
                .replay_safe(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM nt_observed_provider WHERE s='must-not-run'"
        )
        .await,
        0
    );
    assert_eq!(
        events.lock().unwrap().last().unwrap().outcome,
        TelemetryOutcome::Error(ErrorKind::Unsupported)
    );

    let error = session
        .execute_no_count("DROP TABLE nt_observed_missing", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        events.lock().unwrap().last().unwrap().outcome,
        TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
    );

    let (port, mut detected) = telemetry_response_gate(target).await;
    let sink = Arc::clone(&events);
    let gated_provider = connect(port, password, Duration::from_secs(10))
        .await
        .with_telemetry(ClickHouseTelemetry {
            telemetry_enabled: true,
            observer: Some(Arc::new(move |event| {
                sink.lock().unwrap().push(event.clone());
            })),
            ..ClickHouseTelemetry::default()
        });
    let mut session = gated_provider.acquire_clickhouse().await.unwrap();
    let count = events.lock().unwrap().len();
    let write = tokio::spawn(async move {
        session
            .execute_no_count(
                "INSERT INTO nt_observed_provider SELECT 'telemetry-gate'",
                QueryOptions::new(),
            )
            .await
    });
    let gate = tokio::time::timeout(Duration::from_secs(5), detected.recv())
        .await
        .unwrap()
        .expect("write must reach proxy");
    tokio::time::timeout(Duration::from_secs(5), async {
        while raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM nt_observed_provider WHERE s='telemetry-gate'",
        )
        .await
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write must be applied before cancellation");
    assert_eq!(events.lock().unwrap().len(), count, "no success before EOS");
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    drop(gate);
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), count + 1);
        assert_eq!(
            events.last().unwrap().outcome,
            TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
        );
        assert!(
            events
                .iter()
                .all(|event| event.operation == neotales_dbapi::Operation::Execute)
        );
        assert!(
            events
                .iter()
                .all(|event| !format!("{event:?}").contains("bound-secret"))
        );
    }
    raw.execute("DROP TABLE nt_observed_provider")
        .await
        .unwrap();
}

async fn telemetry_response_gate(
    target: SocketAddr,
) -> (
    u16,
    tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const MARKER: &[u8] = b"INSERT INTO nt_observed_provider SELECT 'telemetry-gate'";
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
                            if seen.windows(MARKER.len()).any(|window| window == MARKER) {
                                let (resume, wait) = tokio::sync::oneshot::channel();
                                let _ = detected.send(resume);
                                let _ = wait.await;
                                break;
                            }
                            let keep = MARKER.len() - 1;
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

/// Differential check for the SETTINGS guard.
///
/// The raw native client is the oracle for `ClickHouse`'s parser, not this scanner. For every
/// candidate, the raw client runs the statement and records whether the `SETTINGS` clause actually
/// changed `max_block_size`. The scanner must never accept a statement where the server applied an
/// override. Candidates where the scanner rejects but the server did not apply an override are
/// conservative rejections, recorded rather than asserted away.
#[allow(clippy::too_many_lines, clippy::items_after_statements)]
async fn settings_scanner_agrees_with_the_server_parser(
    provider: &ClickHousePoolProvider,
    raw: &Client,
) {
    const SCRATCH: &str = "neotales_settings_scanner_probe";
    const SETTING: &str = "max_block_size";
    raw.execute(format!("DROP TABLE IF EXISTS {SCRATCH}"))
        .await
        .unwrap();
    raw.execute(format!("CREATE TABLE {SCRATCH} (s String) ENGINE=Memory"))
        .await
        .unwrap();
    let baseline = raw_scalar(raw, &format!("SELECT toInt64(getSetting('{SETTING}'))")).await;
    let mut override_value = baseline + 1000;
    assert_ne!(
        override_value, 256,
        "must differ from the provider's setting"
    );

    struct Case {
        label: String,
        tail: String,
        /// `Some(value)` when `ClickHouse` parses a bare SETTINGS clause with this override.
        clause: Option<i64>,
    }

    let mut cases: Vec<Case> = Vec::new();
    for (index, keyword) in ["SETTINGS", "settings", "SeTtInGs"].iter().enumerate() {
        override_value += 1;
        cases.push(Case {
            label: format!("clause-{index}"),
            tail: format!("{keyword} {SETTING}={override_value}"),
            clause: Some(override_value),
        });
    }
    for (index, keyword) in ["SETTINGS", "settings", "SeTtInGs"].iter().enumerate() {
        override_value += 1;
        cases.push(Case {
            label: format!("clause-gap-{index}"),
            tail: format!("{keyword}/* gap */ {SETTING}={override_value}"),
            clause: Some(override_value),
        });
        override_value += 1;
        cases.push(Case {
            label: format!("clause-line-{index}"),
            tail: format!("{keyword}\n{SETTING}={override_value}"),
            clause: Some(override_value),
        });
        override_value += 1;
        cases.push(Case {
            label: format!("clause-from-{index}"),
            tail: format!("FROM numbers(1) {keyword} {SETTING}={override_value}"),
            clause: Some(override_value),
        });
    }

    override_value += 1;
    let quoted = override_value;
    for (label, tail) in [
        (
            "literal-block",
            format!("/* SETTINGS {SETTING}={quoted} */"),
        ),
        (
            "literal-nested",
            format!("/* outer /* SETTINGS {SETTING}={quoted} */ outer */"),
        ),
        ("literal-line", format!("-- SETTINGS {SETTING}={quoted}\n")),
        ("literal-hash", format!("# SETTINGS {SETTING}={quoted}\n")),
        (
            "literal-single-quote",
            format!("WHERE 'SETTINGS {SETTING}={quoted}' != ''"),
        ),
        ("literal-backtick-alias", "AS `SETTINGS`".to_owned()),
        ("literal-substring", "AS settings_extra".to_owned()),
    ] {
        cases.push(Case {
            label: label.to_owned(),
            tail,
            clause: None,
        });
    }
    // Unquoted alias: ClickHouse accepts it as an alias, but the scanner rejects the bare token.
    cases.push(Case {
        label: "conservative-alias".to_owned(),
        tail: "AS settings".to_owned(),
        clause: None,
    });
    // The scanner does not model FORMAT: ClickHouse consumes the rest as data, so no override.
    cases.push(Case {
        label: "conservative-format".to_owned(),
        tail: format!("FORMAT Null SETTINGS {SETTING}={quoted}"),
        clause: None,
    });

    let mut clause_cases = 0usize;
    let mut accepted_nonclause = 0usize;
    let mut conservative = 0usize;
    for case in &cases {
        let sql = format!(
            "INSERT INTO {SCRATCH} (s) SELECT concat('{}:', toString(getSetting('{SETTING}'))) {}",
            case.label, case.tail
        );
        let scanner = validate_no_count_statement(&sql, &[]);
        raw.execute(&sql)
            .await
            .unwrap_or_else(|error| panic!("{}: raw oracle failed: {error:?}", case.label));
        let observed = settings_probe_row(provider, SCRATCH, &case.label).await;
        if let Some(expected) = case.clause {
            clause_cases += 1;
            assert_eq!(
                observed,
                expected.to_string(),
                "{}: ClickHouse did not apply the clause the test expects",
                case.label
            );
            let error = scanner.as_ref().err().unwrap_or_else(|| {
                panic!(
                    "{}: scanner accepted an applied SETTINGS clause",
                    case.label
                )
            });
            assert_eq!(
                error.kind(),
                ErrorKind::Unsupported,
                "{}: wrong pre-dispatch rejection",
                case.label
            );
            let mut session = provider.acquire_clickhouse().await.unwrap();
            assert_eq!(
                session
                    .execute_no_count(&sql, QueryOptions::new())
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Unsupported,
                "{}: provider must reject before dispatch",
                case.label
            );
            assert!(
                settings_probe_row_or_none(provider, SCRATCH, &case.label).await
                    == Some(observed.clone()),
                "{}: rejected provider write must not add a row",
                case.label
            );
        } else {
            assert_eq!(
                observed,
                baseline.to_string(),
                "{}: quoted or commented text changed {SETTING}",
                case.label
            );
            if let Err(error) = &scanner {
                conservative += 1;
                assert_eq!(
                    error.kind(),
                    ErrorKind::Unsupported,
                    "{}: unexpected pre-dispatch failure",
                    case.label
                );
                let mut session = provider.acquire_clickhouse().await.unwrap();
                assert_eq!(
                    session
                        .execute_no_count(&sql, QueryOptions::new())
                        .await
                        .unwrap_err()
                        .kind(),
                    ErrorKind::Unsupported,
                    "{}: conservative rejection must happen before dispatch",
                    case.label
                );
            } else {
                accepted_nonclause += 1;
                let provider_label = format!("{}-provider", case.label);
                let provider_sql = format!(
                    "INSERT INTO {SCRATCH} (s) SELECT concat('{provider_label}:', \
                     toString(getSetting('{SETTING}'))) {}",
                    case.tail
                );
                let mut session = provider.acquire_clickhouse().await.unwrap();
                session
                    .execute_no_count(&provider_sql, QueryOptions::new())
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{}: provider rejected accepted SQL: {error:?}", case.label)
                    });
                assert_eq!(
                    settings_probe_row(provider, SCRATCH, &provider_label).await,
                    "256",
                    "{}: the provider connection setting was not preserved",
                    case.label
                );
            }
        }
        // The fail-closed invariant for every executed case, stated independently of the branch.
        assert!(
            !(scanner.is_ok() && case.clause.is_some()),
            "{}: scanner accepted a clause ClickHouse applied",
            case.label
        );
    }
    assert!(clause_cases >= 9, "clause coverage regressed");
    assert!(accepted_nonclause >= 5, "quoted/comment coverage regressed");
    assert!(
        conservative >= 2,
        "conservative rejection coverage regressed"
    );
    raw.execute(format!("DROP TABLE {SCRATCH}")).await.unwrap();
}

async fn settings_probe_row(provider: &ClickHousePoolProvider, table: &str, label: &str) -> String {
    settings_probe_row_or_none(provider, table, label)
        .await
        .unwrap_or_else(|| panic!("{label}: expected one probe row"))
}

async fn settings_probe_row_or_none(
    provider: &ClickHousePoolProvider,
    table: &str,
    label: &str,
) -> Option<String> {
    let mut session = provider.acquire_clickhouse().await.unwrap();
    let rows = session
        .fetch_all_async(
            &format!("SELECT s FROM {table} WHERE startsWith(s, {{p1:String}})"),
            QueryOptions::new()
                .bind(text(&format!("{label}:")))
                .limit(limit(2, 65_536)),
        )
        .await
        .unwrap();
    match rows.as_slice() {
        [] => None,
        [row] => {
            let bytes = row.get::<Vec<u8>>("s").unwrap();
            let value = String::from_utf8(bytes).expect("probe value is UTF-8");
            Some(
                value
                    .strip_prefix(&format!("{label}:"))
                    .expect("probe label prefix")
                    .to_owned(),
            )
        }
        _ => panic!("{label}: more than one probe row"),
    }
}

async fn duplicate_lock_create_is_not_classified_by_public_api(
    provider: &ClickHousePoolProvider,
    target: SocketAddr,
    password: &str,
    raw: &Client,
) {
    raw.execute("CREATE TABLE neotales_lock_probe (owner Int64) ENGINE=Memory")
        .await
        .unwrap();
    raw.execute("INSERT INTO neotales_lock_probe VALUES (37)")
        .await
        .unwrap();
    let contender = raw_client(target, password).await;
    let error = contender
        .execute("CREATE TABLE neotales_lock_probe (owner String) ENGINE=Memory")
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            klickhouse::KlickhouseError::ServerException { code: 57, .. }
        ),
        "expected TABLE_ALREADY_EXISTS code 57"
    );
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_lock_probe WHERE owner=37"
        )
        .await,
        1
    );
    assert_eq!(raw_scalar(raw, "SELECT toInt64(count()) FROM system.columns WHERE database=currentDatabase() AND table='neotales_lock_probe' AND name='owner' AND type='Int64'").await, 1);
    let mut session = provider.acquire_clickhouse().await.unwrap();
    assert_eq!(
        session
            .execute_no_count(
                "CREATE TABLE neotales_lock_probe (owner String) ENGINE=Memory",
                QueryOptions::new()
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_lock_probe WHERE owner=37"
        )
        .await,
        1
    );
    raw.execute("DROP TABLE neotales_lock_probe").await.unwrap();
}

async fn escaped_backticks_are_server_compatible(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_clickhouse().await.unwrap();
    let rows = session
        .fetch_all_async(
            r"SELECT toInt64(7) AS `a\`b`",
            QueryOptions::new().limit(limit(1, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("a`b").unwrap(), 7);
}

#[allow(clippy::too_many_lines)] // One ordered server-default and override regression.
async fn synchronous_insert_overrides_server_defaults(port: u16, target: SocketAddr, raw: &Client) {
    let password = disposable_password();
    raw.execute(format!("CREATE USER neotales_async_test IDENTIFIED BY '{password}' SETTINGS async_insert=1, wait_for_async_insert=0, async_insert_busy_timeout_ms=60000")).await.unwrap();
    raw.execute("GRANT ALL ON default.* TO neotales_async_test")
        .await
        .unwrap();
    let default_client = Client::connect(
        target,
        ClientOptions {
            username: "neotales_async_test".into(),
            password: password.clone(),
            default_database: "default".into(),
            ..ClientOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        raw_scalar(
            &default_client,
            "SELECT toInt64(value) FROM system.settings WHERE name = 'async_insert'"
        )
        .await,
        1
    );
    let config =
        ClickHouseConfig::plaintext_loopback("127.0.0.1", port, "neotales_async_test", "default")
            .unwrap()
            .password(password);
    let provider = ClickHousePoolProvider::connect(config).await.unwrap();
    let mut session = provider.acquire_clickhouse().await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE neotales_async_guard (n Int64) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        session.execute_no_count(
            "INSERT INTO neotales_async_guard SETTINGS async_insert=1, wait_for_async_insert=0 VALUES (9)",
            QueryOptions::new()
        ).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        raw_scalar(raw, "SELECT toInt64(count()) FROM neotales_async_guard").await,
        0
    );
    session
        .execute_no_count(
            "INSERT INTO neotales_async_guard VALUES (7)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_async_guard WHERE n=7"
        )
        .await,
        1,
        "success must not be merely async enqueue"
    );
    raw.execute("CREATE TABLE neotales_dollar_guard (s String) ENGINE=Memory")
        .await
        .unwrap();
    let exploit = "INSERT INTO neotales_dollar_guard SELECT concat(toString(getSetting('async_insert')), $$'$$) SETTINGS async_insert=1, wait_for_async_insert=0 -- '";
    let mut native_settings = klickhouse::IndexMap::new();
    native_settings.insert("async_insert".to_owned(), "0".to_owned());
    let mut events = raw
        .query_events(exploit, Some(native_settings))
        .await
        .unwrap();
    let mut terminal = false;
    while let Some(event) = events.next().await {
        if matches!(event.unwrap(), klickhouse::QueryEvent::EndOfStream) {
            terminal = true;
        }
    }
    assert!(terminal);
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_dollar_guard WHERE hex(s) = '7472756527'"
        )
        .await,
        1,
        "SQL SETTINGS overrides the native async_insert=0 setting"
    );
    assert_eq!(
        session
            .execute_no_count(exploit, QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        raw_scalar(raw, "SELECT toInt64(count()) FROM neotales_dollar_guard").await,
        1,
        "rejected exploit must not dispatch a second write"
    );
    for exploit in [
        "INSERT INTO neotales_dollar_guard SELECT concat(toString(getSetting('async_insert')), 'line') // '\nSETTINGS async_insert=1, wait_for_async_insert=1 -- '",
        "INSERT INTO neotales_dollar_guard SELECT concat(toString(getSetting('async_insert')), ‘'’) SETTINGS async_insert=1, wait_for_async_insert=1 -- '",
    ] {
        let before = raw_scalar(raw, "SELECT toInt64(count()) FROM neotales_dollar_guard").await;
        let enabled_before = raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_dollar_guard WHERE startsWith(s, 'true')",
        )
        .await;
        let mut settings = klickhouse::IndexMap::new();
        settings.insert("async_insert".to_owned(), "0".to_owned());
        let mut events = raw.query_events(exploit, Some(settings)).await.unwrap();
        let mut terminal = false;
        while let Some(event) = events.next().await {
            if matches!(event.unwrap(), klickhouse::QueryEvent::EndOfStream) {
                terminal = true;
            }
        }
        assert!(terminal);
        assert_eq!(
            raw_scalar(raw, "SELECT toInt64(count()) FROM neotales_dollar_guard").await,
            before + 1,
            "raw oracle must execute the bypass"
        );
        assert_eq!(
            raw_scalar(
                raw,
                "SELECT toInt64(count()) FROM neotales_dollar_guard WHERE startsWith(s, 'true')"
            )
            .await,
            enabled_before + 1,
            "SQL override must defeat the native setting"
        );
        assert_eq!(
            session
                .execute_no_count(exploit, QueryOptions::new())
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported,
        );
        assert_eq!(
            raw_scalar(raw, "SELECT toInt64(count()) FROM neotales_dollar_guard").await,
            before + 1,
            "provider must reject before dispatch"
        );
    }
    raw.execute("DROP TABLE neotales_dollar_guard")
        .await
        .unwrap();
    session
        .execute_no_count("DROP TABLE neotales_async_guard", QueryOptions::new())
        .await
        .unwrap();
    raw.execute("DROP USER neotales_async_test").await.unwrap();
}

#[allow(clippy::too_many_lines)] // Ordered success, failure and cancellation on isolated tables.
async fn no_count_writes(provider: &ClickHousePoolProvider, raw: &Client) {
    let mut session = provider.acquire_clickhouse().await.unwrap();
    let result: () = session
        .execute_no_count(
            "CREATE TABLE neotales_no_count (n Int64, s String) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(result, ());
    session
        .execute_no_count(
            "INSERT INTO neotales_no_count SELECT {p1:Int64}, {p2:String}",
            QueryOptions::new().bind_all(&[integer(17), bytes(vec![0, 0xff, b'\''])]),
        )
        .await
        .unwrap();
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) FROM neotales_no_count WHERE n = 17 AND hex(s) = '00FF27'"
        )
        .await,
        1
    );
    assert_kind(
        session
            .execute_no_count(
                "ALTER TABLE neotales_no_count DELETE WHERE 1",
                QueryOptions::new(),
            )
            .await,
        ErrorKind::Unsupported,
        "unsupported no-count statement",
    );
    assert_kind(
        session
            .execute_no_count(
                "DROP TABLE neotales_no_count; DROP TABLE other",
                QueryOptions::new(),
            )
            .await,
        ErrorKind::InvalidArgument,
        "multiple statements",
    );
    assert_kind(
        session
            .execute_no_count(
                "INSERT INTO neotales_no_count SELECT {p1:Int64}, ''",
                QueryOptions::new().bind(text("wrong")),
            )
            .await,
        ErrorKind::InvalidArgument,
        "mismatched binding",
    );
    session
        .execute_no_count("DROP TABLE neotales_no_count", QueryOptions::new())
        .await
        .unwrap();

    let error = session
        .execute_no_count(
            "DROP TABLE neotales_table_that_does_not_exist",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        session
            .execute_no_count(
                "CREATE TABLE neotales_must_not_exist (n Int64) ENGINE=Memory",
                QueryOptions::new()
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );

    let mut session = provider.acquire_clickhouse().await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE neotales_no_count_timeout (n Int64) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .execute_no_count(
                "INSERT INTO neotales_no_count_timeout SELECT toInt64(sleep(2))",
                QueryOptions::new().timeout(Duration::from_millis(100)),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(
        session
            .execute_no_count("DROP TABLE neotales_no_count_timeout", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    // The timed-out query may still run. Wait for the server to finish before cleanup.
    tokio::time::sleep(Duration::from_secs(3)).await;
    raw.execute("DROP TABLE neotales_no_count_timeout")
        .await
        .unwrap();

    let mut session = provider.acquire_clickhouse().await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE neotales_no_count_cancel (n Int64) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    {
        let write = session.execute_no_count(
            "INSERT INTO neotales_no_count_cancel SELECT toInt64(sleep(2))",
            QueryOptions::new(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), write)
                .await
                .is_err()
        );
    }
    assert_eq!(
        session
            .execute_no_count("DROP TABLE neotales_no_count_cancel", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    raw.execute("DROP TABLE neotales_no_count_cancel")
        .await
        .unwrap();
}

#[allow(clippy::too_many_lines)] // The proxy barrier and write observation are one causal sequence.
async fn response_loss_after_applied_write(target: SocketAddr, password: &str, raw: &Client) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let pause = Arc::new(AtomicBool::new(false));
    let (blocked, mut blocked_rx) =
        tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
    let proxy_pause = pause.clone();
    let proxy = tokio::spawn(async move {
        loop {
            let Ok((incoming, _)) = listener.accept().await else {
                break;
            };
            let pause = proxy_pause.clone();
            let blocked = blocked.clone();
            tokio::spawn(async move {
                let Ok(outgoing) = tokio::net::TcpStream::connect(target).await else {
                    return;
                };
                let (mut from_client, mut to_client) = incoming.into_split();
                let (mut from_server, mut to_server) = outgoing.into_split();
                let upstream = tokio::io::copy(&mut from_client, &mut to_server);
                let downstream = async {
                    use tokio::io::AsyncWriteExt;
                    let mut buffer = [0u8; 8192];
                    while let Ok(n) = from_server.read(&mut buffer).await {
                        if n == 0 {
                            break;
                        }
                        if pause.load(Ordering::Acquire) {
                            let (close, closed) = tokio::sync::oneshot::channel();
                            let _ = blocked.send(close);
                            let _ = closed.await;
                            break;
                        }
                        if to_client.write_all(&buffer[..n]).await.is_err() {
                            break;
                        }
                    }
                };
                tokio::select! { _ = upstream => {}, () = downstream => {} }
            });
        }
    });
    let provider = connect(port, password, Duration::from_secs(10)).await;
    let mut session = provider.acquire_clickhouse().await.unwrap();
    session
        .execute_no_count(
            "CREATE TABLE neotales_no_count_loss (n Int64) ENGINE=Memory",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    pause.store(true, Ordering::Release);
    let write = tokio::spawn(async move {
        let result = session
            .execute_no_count(
                "INSERT INTO neotales_no_count_loss SELECT 41",
                QueryOptions::new(),
            )
            .await;
        (result, session)
    });
    let close = tokio::time::timeout(Duration::from_secs(5), blocked_rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if raw_scalar(
                raw,
                "SELECT toInt64(count()) FROM neotales_no_count_loss WHERE n = 41",
            )
            .await
                == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write must be visible before response is cut");
    let _ = close.send(());
    let (result, mut session) = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        session
            .execute_no_count("DROP TABLE neotales_no_count_loss", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    raw.execute("DROP TABLE neotales_no_count_loss")
        .await
        .unwrap();
    proxy.abort();
}

async fn capabilities_are_truthful(provider: &ClickHousePoolProvider) {
    let capabilities = provider.capabilities();
    assert!(!capabilities.transactions);
    assert!(!capabilities.isolation_level);
    assert!(!capabilities.read_only_transactions);
    assert!(!capabilities.trusted_scripts);
    let mut session = provider.acquire_async().await.unwrap();
    assert_eq!(session.defaults().timeout, Some(Duration::from_secs(10)));
    assert_kind(
        session.begin_async(TxOptions::default()).await,
        ErrorKind::Unsupported,
        "begin",
    );
}

async fn version_is_gated(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let rows = assert_ok(session.as_mut(), "SELECT version() AS v", &[]).await;
    let version = rows[0].get::<Vec<u8>>("v").unwrap();
    let version = std::str::from_utf8(&version).unwrap();
    assert!(
        version.starts_with("24.8."),
        "unexpected gated version {version:?}"
    );
}

async fn result_types_are_exact(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let rows = assert_ok(
        session.as_mut(),
        "SELECT toInt64(-64) AS i, 'text' AS s, reinterpretAsString(unhex('ff')) AS b",
        &[],
    )
    .await;
    let row = &rows[0];
    assert_eq!(row.get::<i64>("i").unwrap(), -64);
    assert_eq!(row.get::<Vec<u8>>("s").unwrap(), b"text");
    assert_eq!(row.get::<Vec<u8>>("b").unwrap(), vec![0xff]);

    let rows = assert_ok(
        session.as_mut(),
        "SELECT toUInt64(9223372036854775807) AS u64",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("u64").unwrap(), i64::MAX);
    assert_kind(
        fetch(
            session.as_mut(),
            "SELECT toUInt64('9223372036854775808') AS n",
            &[],
        )
        .await,
        ErrorKind::Overflow,
        "UInt64 overflow",
    );
    assert_eq!(
        assert_ok(session.as_mut(), "SELECT toInt64(3) AS n", &[]).await[0]
            .get::<i64>("n")
            .unwrap(),
        3
    );

    let empty = assert_ok(
        session.as_mut(),
        "SELECT toInt64(number) AS n FROM numbers(0)",
        &[],
    )
    .await;
    assert_eq!(empty.len(), 0);

    for (sql, context) in [
        ("SELECT toInt8(-1) AS n", "Int8"),
        ("SELECT toBool(true) AS b", "Bool"),
        ("SELECT toFloat64(1.5) AS f", "Float64"),
    ] {
        assert_kind(
            fetch(session.as_mut(), sql, &[]).await,
            ErrorKind::Decode,
            context,
        );
        let rows = assert_ok(session.as_mut(), "SELECT toInt64(3) AS n", &[]).await;
        assert_eq!(rows[0].get::<i64>("n").unwrap(), 3);
    }

    // These types are outside the vendored native block decoder's whitelist. The decoder
    // rejects the packet before a block exists, so the stream cannot be trusted and the query
    // outcome is unknown even though the provider would not have represented the type.
    for (sql, context) in [
        ("SELECT CAST(NULL AS Nullable(UInt32)) AS n", "Nullable"),
        ("SELECT toFixedString('ab', 4) AS s", "FixedString"),
        ("SELECT toDate('2024-01-01') AS d", "Date"),
        ("SELECT [1, 2] AS a", "Array"),
        (
            "SELECT toUUID('00000000-0000-0000-0000-000000000000') AS u",
            "UUID",
        ),
    ] {
        let mut isolated = provider.acquire_async().await.unwrap();
        assert_kind(
            fetch(isolated.as_mut(), sql, &[]).await,
            ErrorKind::WriteOutcomeUnknown,
            context,
        );
        assert_kind(
            fetch(isolated.as_mut(), "SELECT toInt64(1) AS n", &[]).await,
            ErrorKind::WriteOutcomeUnknown,
            "poisoned native decode",
        );
    }
}

async fn nullable_parameter_and_result_roundtrip(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let null_i = Parameter::new(DbType::I64, Value::Null(DbType::I64)).unwrap();
    let null_s = Parameter::new(DbType::Bytes, Value::Null(DbType::Bytes)).unwrap();
    let rows = assert_ok(
        session.as_mut(),
        "SELECT {p1:Nullable(Int64)} AS i, {p2:Nullable(String)} AS s",
        &[null_i, null_s],
    )
    .await;
    assert_eq!(rows[0].get::<Option<i64>>("i").unwrap(), None);
    assert_eq!(rows[0].get::<Option<Vec<u8>>>("s").unwrap(), None);
    let rows = assert_ok(
        session.as_mut(),
        "SELECT {p1:Nullable(UInt64)} AS n",
        &[Parameter::new(DbType::I64, Value::Null(DbType::I64)).unwrap()],
    )
    .await;
    assert_eq!(rows[0].get::<Option<i64>>("n").unwrap(), None);
    let rows = assert_ok(
        session.as_mut(),
        "SELECT {p1:Nullable(UInt64)} AS n",
        &[integer(i64::MAX)],
    )
    .await;
    assert_eq!(rows[0].get::<Option<i64>>("n").unwrap(), Some(i64::MAX));
    assert_kind(
        fetch(
            session.as_mut(),
            "SELECT CAST(toUInt64('9223372036854775808') AS Nullable(UInt64)) AS n",
            &[],
        )
        .await,
        ErrorKind::Overflow,
        "nullable UInt64 overflow",
    );
    for value in [b"\\N".to_vec(), vec![0, b'\\', b'\'', 0xff]] {
        let rows = assert_ok(
            session.as_mut(),
            "SELECT {p1:Nullable(Int64)} AS i, {p2:Nullable(String)} AS s",
            &[integer(-7), bytes(value.clone())],
        )
        .await;
        assert_eq!(rows[0].get::<Option<i64>>("i").unwrap(), Some(-7));
        assert_eq!(rows[0].get::<Option<Vec<u8>>>("s").unwrap(), Some(value));
    }
    let rows = assert_ok(
        session.as_mut(),
        "SELECT CAST(NULL AS Nullable(Int64)) AS i, CAST(NULL AS Nullable(String)) AS s WHERE 0",
        &[],
    )
    .await;
    assert_eq!(rows.len(), 0);
    assert_kind(
        session
            .fetch_all_async(
                "SELECT CAST('abcdef' AS Nullable(String)) AS s",
                QueryOptions::new().limit(limit(1, 3)),
            )
            .await,
        ErrorKind::Overflow,
        "nullable row limit",
    );
}

async fn nested_comments_are_scanned(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let rows = assert_ok(
        session.as_mut(),
        "SELECT toInt64(7) AS n /* outer /* inner ; {p1:Int64} */ still outer */",
        &[],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 7);

    let rows = assert_ok(
        session.as_mut(),
        "SELECT {p1:Int64} AS n /* outer /* {p2:Int64} */ outer */",
        &[integer(11)],
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 11);

    for sql in [
        "SELECT toInt64(1) AS n /* unclosed",
        "SELECT toInt64(1) AS n /* outer /* inner */",
    ] {
        assert_kind(
            fetch(session.as_mut(), sql, &[]).await,
            ErrorKind::InvalidArgument,
            "malformed block comment",
        );
    }
}

async fn binds_native_parameters(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let padded = assert_ok(
        session.as_mut(),
        "SELECT {p1: Int64 } AS n, {p1:Int64} AS repeated",
        &[integer(7)],
    )
    .await;
    assert_eq!(padded[0].get::<i64>("n").unwrap(), 7);
    assert_eq!(padded[0].get::<i64>("repeated").unwrap(), 7);
    let hostile = "quote '\" slash \\ tab\t newline\n nul\0 🦀";
    let binary = (0u8..=255).collect::<Vec<_>>();
    let parameters = [
        integer(7),
        boolean(true),
        text(hostile),
        bytes(binary),
        bytes(Vec::new()),
        text(""),
        float(0.0),
        float(-0.0),
        float(1.0),
        float(-1.0),
    ];
    let rows = assert_ok(
        session.as_mut(),
        "SELECT \
            {p1:Int64} AS i, \
            toInt64({p2:Bool}) AS b, \
            {p3:String} AS t, \
            toInt64(length({p4:String})) AS bl, \
            toInt64(length({p5:String})) AS el, \
            toInt64(length({p6:String})) AS et, \
            toInt64({p7:Float64} = 0) AS z, \
            toInt64({p8:Float64} = -0.0) AS nz, \
            toInt64({p9:Float64} = 1) AS one, \
            toInt64({p10:Float64} = -1) AS neg",
        &parameters,
    )
    .await;
    let row = &rows[0];
    assert_eq!(row.get::<i64>("i").unwrap(), 7);
    assert_eq!(row.get::<i64>("b").unwrap(), 1);
    assert_eq!(row.get::<Vec<u8>>("t").unwrap(), hostile.as_bytes());
    assert_eq!(row.get::<i64>("bl").unwrap(), 256);
    assert_eq!(row.get::<i64>("el").unwrap(), 0);
    assert_eq!(row.get::<i64>("et").unwrap(), 0);
    assert_eq!(row.get::<i64>("z").unwrap(), 1);
    assert_eq!(row.get::<i64>("nz").unwrap(), 1);
    assert_eq!(row.get::<i64>("one").unwrap(), 1);
    assert_eq!(row.get::<i64>("neg").unwrap(), 1);

    let repeated = assert_ok(
        session.as_mut(),
        "SELECT toInt64({p1:Int64}) + toInt64({p1:Int64}) AS n",
        &[integer(7)],
    )
    .await;
    assert_eq!(repeated[0].get::<i64>("n").unwrap(), 14);

    let quoted = assert_ok(
        session.as_mut(),
        "SELECT {p1:Int64} AS n /* comment */ WHERE 'x' = 'x' -- tail",
        &[integer(7)],
    )
    .await;
    assert_eq!(quoted[0].get::<i64>("n").unwrap(), 7);
}

async fn binds_declared_integer_types(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let parameters = [
        integer(-128),
        integer(65535),
        integer(-2_147_483_648),
        integer(i64::MAX),
    ];
    let rows = assert_ok(
        session.as_mut(),
        "SELECT \
            toInt64({p1:Int8}) AS i8, \
            toInt64({p2:UInt16}) AS u16, \
            toInt64({p3:Int32}) AS i32, \
            toInt64({p4:UInt64}) AS u64",
        &parameters,
    )
    .await;
    let row = &rows[0];
    assert_eq!(row.get::<i64>("i8").unwrap(), -128);
    assert_eq!(row.get::<i64>("u16").unwrap(), 65_535);
    assert_eq!(row.get::<i64>("i32").unwrap(), -2_147_483_648);
    assert_eq!(row.get::<i64>("u64").unwrap(), i64::MAX);
}

async fn rejects_unsupported_parameters(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let null = Parameter::new(DbType::Text, Value::Null(DbType::Text)).unwrap();
    for (sql, parameters, expected, context) in [
        (
            "SELECT {p1:String}",
            vec![null],
            ErrorKind::Unsupported,
            "null",
        ),
        (
            "SELECT {p1:Float64}",
            vec![float(1.5)],
            ErrorKind::Unsupported,
            "inexact float",
        ),
        (
            "SELECT {p1:String}",
            vec![text(&"x".repeat(8193))],
            ErrorKind::Unsupported,
            "oversized text",
        ),
        (
            "SELECT {p1:Nullable(UInt32)}",
            vec![integer(1)],
            ErrorKind::Unsupported,
            "nullable declaration",
        ),
        (
            "SELECT {p1:Float32}",
            vec![float(1.0)],
            ErrorKind::Unsupported,
            "float32 declaration",
        ),
        (
            "SELECT {p1:Int64}",
            vec![text("not-an-integer")],
            ErrorKind::InvalidArgument,
            "type mismatch",
        ),
        (
            "SELECT {p1:Int64}, {p2:Int64}",
            vec![integer(1)],
            ErrorKind::InvalidArgument,
            "arity",
        ),
        (
            "SELECT {p2:Int64}",
            vec![integer(1)],
            ErrorKind::InvalidArgument,
            "non-positional name",
        ),
        (
            "SELECT {p1:Int64}, {p1:String}",
            vec![integer(1)],
            ErrorKind::InvalidArgument,
            "conflicting declared type",
        ),
        (
            "SELECT {p1:UInt8}",
            vec![integer(256)],
            ErrorKind::InvalidArgument,
            "out of range",
        ),
        (
            "SELECT {p1:UInt8}",
            vec![integer(-1)],
            ErrorKind::InvalidArgument,
            "negative unsigned",
        ),
        (
            "SELECT 1; SELECT 2",
            vec![],
            ErrorKind::InvalidArgument,
            "multiple statements",
        ),
        (
            "BEGIN",
            vec![],
            ErrorKind::Unsupported,
            "transaction control",
        ),
        ("", vec![], ErrorKind::InvalidArgument, "empty SQL"),
    ] {
        let result = session
            .fetch_all_async(
                sql,
                QueryOptions::new()
                    .bind_all(&parameters)
                    .limit(limit(10, 4096)),
            )
            .await;
        assert_kind(result, expected, context);
    }
}

async fn bounds_fail_closed(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();

    let row_limit = session
        .fetch_all_async(
            "SELECT toInt64(number) AS n FROM numbers(3)",
            QueryOptions::new().limit(limit(2, 65_536)),
        )
        .await;
    assert_kind(row_limit, ErrorKind::Overflow, "row limit");

    let byte_limit = session
        .fetch_all_async(
            "SELECT repeat('x', 2000) AS s",
            QueryOptions::new().limit(limit(10, 100)),
        )
        .await;
    assert_kind(byte_limit, ErrorKind::Overflow, "byte limit");

    let exact = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 9)),
        )
        .await
        .unwrap();
    assert_eq!(exact.len(), 1);
    let too_small = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 8)),
        )
        .await;
    assert_kind(too_small, ErrorKind::Overflow, "exact byte boundary");

    let many = session
        .fetch_all_async(
            "SELECT toInt64(number) AS n FROM numbers(20000)",
            QueryOptions::new().limit(limit(20_000, 2_000_000)),
        )
        .await
        .unwrap();
    assert_eq!(many.len(), 20_000);

    let large = session
        .fetch_all_async(
            "SELECT repeat('x', 40000) AS a, repeat('y', 40000) AS b",
            QueryOptions::new().limit(limit(1, 100)),
        )
        .await;
    assert_kind(large, ErrorKind::Overflow, "large bounded row");
}

async fn options_and_reader_semantics(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let quick = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().no_timeout().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(quick.len(), 1);

    let zero = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new()
                .timeout(Duration::ZERO)
                .limit(limit(1, 64)),
        )
        .await;
    assert_kind(zero, ErrorKind::InvalidArgument, "zero timeout");

    let first = provider
        .fetch_first_async(
            "SELECT toInt64(number) AS n FROM numbers(3)",
            QueryOptions::new().limit(limit(3, 64)),
        )
        .await
        .unwrap()
        .expect("first row");
    assert_eq!(first.get::<i64>("n").unwrap(), 0);

    let one = provider
        .fetch_one_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(one.get::<i64>("n").unwrap(), 1);
    let none = provider
        .fetch_one_async(
            "SELECT toInt64(number) AS n FROM numbers(0)",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await;
    assert_kind(none, ErrorKind::NoRows, "one row");
    let many = provider
        .fetch_one_async(
            "SELECT toInt64(number) AS n FROM numbers(2)",
            QueryOptions::new().limit(limit(2, 64)),
        )
        .await;
    assert_kind(many, ErrorKind::TooManyRows, "too many rows");

    let scalar = provider
        .fetch_scalar_async(
            "SELECT toInt64(7) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(scalar, Value::I64(7));

    let mut reader = session
        .fetch_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(
        reader
            .next_row_async()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>("n")
            .unwrap(),
        1
    );
    assert!(!reader.has_next_set_async().await.unwrap());
    reader.finish_async().await.unwrap();
}

async fn local_failures_do_not_poison(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let overflow = session
        .fetch_all_async(
            "SELECT toInt64(number) AS n FROM numbers(3)",
            QueryOptions::new().limit(limit(2, 65_536)),
        )
        .await;
    assert_kind(overflow, ErrorKind::Overflow, "local overflow");
    let rows = session
        .fetch_all_async(
            "SELECT toInt64(9) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 9);
}

async fn timeouts_poison_after_dispatch(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let slow = session
        .fetch_all_async(
            "SELECT toInt64(number) AS n FROM numbers(1000) \
             WHERE sleepEachRow(0.01) = 0 SETTINGS max_block_size=1",
            QueryOptions::new()
                .timeout(Duration::from_millis(100))
                .limit(limit(2000, 1_000_000)),
        )
        .await;
    assert_kind(slow, ErrorKind::WriteOutcomeUnknown, "slow timeout");

    let poisoned = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await;
    assert_kind(poisoned, ErrorKind::WriteOutcomeUnknown, "poisoned session");

    let mut fresh = provider.acquire_async().await.unwrap();
    let rows = fresh
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 1);
}

async fn late_server_errors_are_unknown(provider: &ClickHousePoolProvider) {
    let mut session = provider.acquire_async().await.unwrap();
    let late = session
        .fetch_all_async(
            "SELECT toInt64(number) AS n, \
                    if(number = 2, throwIf(toInt64(1), 'late'), toInt64(0)) AS x \
             FROM numbers(5) SETTINGS max_block_size=1",
            QueryOptions::new().limit(limit(10, 4096)),
        )
        .await;
    assert_kind(late, ErrorKind::WriteOutcomeUnknown, "late server error");

    let poisoned = session
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await;
    assert_kind(
        poisoned,
        ErrorKind::WriteOutcomeUnknown,
        "poisoned after late error",
    );

    let mut fresh = provider.acquire_async().await.unwrap();
    let rows = fresh
        .fetch_all_async(
            "SELECT toInt64(1) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 1);
}

async fn unsupported_operations_are_predispatch(provider: &ClickHousePoolProvider, raw: &Client) {
    raw.execute("DROP TABLE IF EXISTS neotales_native_unsupported")
        .await
        .unwrap();
    raw.execute("CREATE TABLE neotales_native_unsupported (id Int64) ENGINE=Memory")
        .await
        .unwrap();
    let mut session = provider.acquire_async().await.unwrap();

    let execute = session
        .execute_async(
            "INSERT INTO neotales_native_unsupported VALUES (1)",
            QueryOptions::new(),
        )
        .await;
    assert_kind(execute, ErrorKind::Unsupported, "execute");

    let execute_select = session.execute_async("SELECT 1", QueryOptions::new()).await;
    assert_kind(execute_select, ErrorKind::Unsupported, "execute select");

    let execute_limit = session
        .execute_async(
            "INSERT INTO neotales_native_unsupported VALUES (1)",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await;
    assert_kind(execute_limit, ErrorKind::InvalidArgument, "execute limit");

    let script = session
        .execute_trusted_script_async(
            "CREATE TABLE neotales_native_script (id Int64) ENGINE=Memory; \
             INSERT INTO neotales_native_script VALUES (1)",
            QueryOptions::new(),
        )
        .await;
    assert_kind(script, ErrorKind::Unsupported, "trusted script");

    let script_parameter = session
        .execute_trusted_script_async("SELECT 1", QueryOptions::new().bind(integer(1)))
        .await;
    assert_kind(
        script_parameter,
        ErrorKind::InvalidArgument,
        "script parameter",
    );

    let begin = session.begin_async(TxOptions::default()).await;
    assert_kind(begin, ErrorKind::Unsupported, "begin");

    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) AS n FROM system.tables \
             WHERE name = 'neotales_native_script'"
        )
        .await,
        0,
        "unsupported script created a table"
    );
    assert_eq!(
        raw_scalar(
            raw,
            "SELECT toInt64(count()) AS n FROM neotales_native_unsupported"
        )
        .await,
        0,
        "unsupported execute wrote a row"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn verified_tls_rejects_wrong_ca_and_hostname() {
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

    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    assert_no_published_ports(&container);
    let target = bridge_address(&container).await;

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let forwarded = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&forwarded);
    let proxy = tokio::spawn(async move {
        let acceptor = TlsAcceptor::from(Arc::new(server));
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(socket).await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    if let Ok(mut native) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut tls, &mut native).await;
                    }
                }
            });
        }
    });

    let directory = tempfile::tempdir().unwrap();
    let ca_path = directory.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();

    let other_key = KeyPair::generate().unwrap();
    let mut other_params = CertificateParams::new(Vec::new()).unwrap();
    other_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let other_ca = other_params.self_signed(&other_key).unwrap();
    let other_path = directory.path().join("other-ca.pem");
    std::fs::write(&other_path, other_ca.pem()).unwrap();

    let config = |ca: &std::path::Path, name: &str| {
        ClickHouseConfig::new("127.0.0.1", port, "default", "default")
            .ca_file(ca)
            .tls_server_name(name)
            .password(&password)
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(10))
    };

    let wrong_ca = ClickHousePoolProvider::connect(config(&other_path, "clickhouse.test")).await;
    assert_kind(wrong_ca, ErrorKind::Connection, "wrong CA");
    assert_eq!(
        forwarded.load(Ordering::SeqCst),
        0,
        "wrong CA reached the native server"
    );

    let wrong_name = ClickHousePoolProvider::connect(config(&ca_path, "wrong.test")).await;
    assert_kind(wrong_name, ErrorKind::Connection, "wrong hostname");
    assert_eq!(
        forwarded.load(Ordering::SeqCst),
        0,
        "wrong hostname reached the native server"
    );

    let provider = ClickHousePoolProvider::connect(config(&ca_path, "clickhouse.test"))
        .await
        .expect("verified TLS connection");
    let mut session = provider.acquire_async().await.unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT toInt64(7) AS n",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 7);
    assert!(forwarded.load(Ordering::SeqCst) >= 1);

    proxy.abort();
    let _ = proxy.await;
}
