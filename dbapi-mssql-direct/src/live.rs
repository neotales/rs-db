//! End-to-end tests against a pinned disposable SQL Server container.

#[path = "../tests/support/capture.rs"]
mod capture;
#[path = "../tests/support/ledger.rs"]
pub(crate) mod ledger;
#[path = "../tests/support/startup.rs"]
mod startup;
#[path = "../tests/support/tls_diagnostic.rs"]
mod tls_diagnostic;
use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::{
    Error, ExclusivePool, Probe, bind::bind, migration::SqlServerBackend, provider::DirectPool,
};
use futures_util::TryStreamExt;
use mssql_tds::{
    connection::client_context::ClientContext,
    datatypes::{column_values::ColumnValues, sqltypes::SqlType},
    message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
};
use neotales_dbapi::{
    Cancellation, DbType, Delay, ErrorKind, Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions,
    RetryPolicy, RetryRuntime, RowLimit, TxOptions, Value,
    paging::{
        PageSize,
        multi::{
            PageBudget, PageSourceStream, PagedPage,
            sql::{SqlPageAssertion, SqlPageSource},
        },
    },
};

#[path = "live/keyset_plan.rs"]
mod keyset_plan;
use testcontainers::{
    CopyTargetOptions, GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConnectFailureCategory {
    TlsHandshake,
    CertificateExpired,
    CertificateMismatch,
    Transport,
    Protocol,
    SqlServerLogin,
    Io,
    ConnectTimeout,
    OtherTimeout,
    ConnectionClosed,
    TlsLibrary,
    Security,
    Redirection,
    Usage,
    Implementation,
    Other,
}

static LAST_CONNECT_FAILURE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
static LAST_SQL_LOGIN_DIAGNOSTIC: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static ARM_NEXT_COMMIT_FAULT: AtomicBool = AtomicBool::new(false);
static COMMIT_FAULT_ARMED: AtomicBool = AtomicBool::new(false);

tokio::task_local! {
    static LOCK_DIAGNOSTIC: LockDiagnostic;
    static RECONCILIATION_DIAGNOSTIC: ReconciliationDiagnostic;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReconciliationPhase {
    PoolWait,
    Reconnect,
    Connected,
    Begin,
    Begun,
    LockRequest,
    LockStatus(i64),
    Query,
    QueryComplete,
    LedgerDdl,
    LedgerDdlComplete(bool),
    Commit,
    CommitComplete(bool),
}

struct ReconciliationDiagnostic {
    start: std::time::Instant,
    events: Arc<std::sync::Mutex<Vec<(Duration, ReconciliationPhase)>>>,
}

pub(super) fn reconciliation_phase(phase: ReconciliationPhase) {
    let _ = RECONCILIATION_DIAGNOSTIC.try_with(|diagnostic| {
        let mut events = diagnostic.events.lock().unwrap();
        // Keep the diagnostic bounded even if a future code path loops.
        if events.len() < 256 {
            events.push((diagnostic.start.elapsed(), phase));
            tls_diagnostic::phase(match phase {
                ReconciliationPhase::PoolWait => "pool-wait",
                ReconciliationPhase::Reconnect => "reconnect",
                ReconciliationPhase::Connected => "connected",
                ReconciliationPhase::Begin => "begin",
                ReconciliationPhase::Begun => "begun",
                ReconciliationPhase::LockRequest => "lock-request",
                ReconciliationPhase::LockStatus(0 | 1) => "lock-granted",
                ReconciliationPhase::LockStatus(-1) => "lock-timeout",
                ReconciliationPhase::LockStatus(-2) => "lock-cancelled",
                ReconciliationPhase::LockStatus(-3) => "lock-deadlock",
                ReconciliationPhase::LockStatus(-999) => "lock-error",
                ReconciliationPhase::LockStatus(_) => "lock-unknown",
                ReconciliationPhase::Query => "query",
                ReconciliationPhase::QueryComplete => "query-complete",
                ReconciliationPhase::LedgerDdl => "ledger-ddl",
                ReconciliationPhase::LedgerDdlComplete(true) => "ledger-ddl-complete",
                ReconciliationPhase::LedgerDdlComplete(false) => "ledger-ddl-failed",
                ReconciliationPhase::Commit => "commit",
                ReconciliationPhase::CommitComplete(true) => "commit-complete",
                ReconciliationPhase::CommitComplete(false) => "commit-failed",
            });
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LockEvent {
    Request,
    Status(i64),
    CommitStart,
    CommitEnd(bool),
}

struct LockDiagnostic {
    writer: u8,
    start: std::time::Instant,
    events: Arc<std::sync::Mutex<Vec<(u8, Duration, LockEvent)>>>,
}

pub(super) fn lock_event(event: LockEvent) {
    let _ = LOCK_DIAGNOSTIC.try_with(|diagnostic| {
        diagnostic.events.lock().unwrap().push((
            diagnostic.writer,
            diagnostic.start.elapsed(),
            event,
        ));
    });
}

const TLS_CHILD_SCENARIO_DEADLINE: Duration = Duration::from_secs(100);
const TLS_CHILD_WATCHDOG: Duration = Duration::from_secs(120);
const TLS_FIXTURE_STARTUP_DEADLINE: Duration = Duration::from_secs(90);
const TLS_LOGIN_READINESS_DEADLINE: Duration = Duration::from_secs(60);
const OPENSSL_COMMAND_DEADLINE: Duration = Duration::from_secs(30);
// Sequential worst case: 2 containers * 90s + 2 login gates * 60s
// + 9 children * (120s wait/drain + 5s cleanup)
// + 8 OpenSSL commands * (30s wait/drain + 5s cleanup) = 1,705s.
// The async 1,750s outer deadline leaves 45s for setup between phases.
const TLS_SCENARIO_DEADLINE: Duration = Duration::from_secs(1_750);
const DIRECT_LIVE_STARTUP_DEADLINE: Duration = Duration::from_secs(90);
const DIRECT_LIVE_SCENARIO_DEADLINE: Duration = Duration::from_secs(600);
const CHILD_CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
const SUPERVISOR_CLEANUP_DEADLINE: Duration = Duration::from_secs(10);
static CHILD_SUPERVISORS: OnceLock<SupervisorTracker> = OnceLock::new();

#[derive(Default)]
struct SupervisorTracker {
    active: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

impl SupervisorTracker {
    fn start(&self) -> ChildSupervisorCount<'_> {
        self.active.fetch_add(1, Ordering::SeqCst);
        ChildSupervisorCount(self)
    }

    async fn wait(&self, deadline: Duration) -> Result<(), ()> {
        let deadline = tokio::time::Instant::now() + deadline;
        loop {
            // Register before checking the count: notify_waiters does not
            // retain a permit for a waiter that has not yet registered.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(());
            }
        }
    }
}

fn child_supervisors() -> &'static SupervisorTracker {
    CHILD_SUPERVISORS.get_or_init(SupervisorTracker::default)
}

async fn wait_for_child_supervisors(deadline: Duration) -> Result<(), ()> {
    child_supervisors().wait(deadline).await
}

struct ChildSupervisorCount<'a>(&'a SupervisorTracker);

impl Drop for ChildSupervisorCount<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

pub(super) fn reset_connect_failure() {
    LAST_CONNECT_FAILURE.store(0, Ordering::SeqCst);
    LAST_SQL_LOGIN_DIAGNOSTIC.store(0, Ordering::SeqCst);
}

pub(super) fn record_connect_failure(error: &mssql_tds::error::Error) {
    if let mssql_tds::error::Error::SqlServerError { diagnostics } = error
        && let Some(error) = diagnostics.errors.first()
    {
        let diagnostic = (u64::from(error.number) << 16)
            | (u64::from(error.state) << 8)
            | u64::from(u8::try_from(error.class).unwrap_or_default());
        LAST_SQL_LOGIN_DIAGNOSTIC.store(diagnostic, Ordering::SeqCst);
    }
    let category = classify_connect_failure(error);
    eprintln!("TLS_DRIVER={category:?}");
    if category == ConnectFailureCategory::Other {
        eprintln!(
            "sanitized unmapped driver error discriminant={:?}",
            std::mem::discriminant(error)
        );
    }
    let encoded = match category {
        ConnectFailureCategory::TlsHandshake => 1,
        ConnectFailureCategory::CertificateExpired => 2,
        ConnectFailureCategory::CertificateMismatch => 3,
        ConnectFailureCategory::Transport => 4,
        ConnectFailureCategory::Protocol => 5,
        ConnectFailureCategory::SqlServerLogin => 6,
        ConnectFailureCategory::Io => 7,
        ConnectFailureCategory::Other => 8,
        ConnectFailureCategory::ConnectTimeout => 9,
        ConnectFailureCategory::OtherTimeout => 10,
        ConnectFailureCategory::ConnectionClosed => 11,
        ConnectFailureCategory::TlsLibrary => 12,
        ConnectFailureCategory::Security => 13,
        ConnectFailureCategory::Redirection => 14,
        ConnectFailureCategory::Usage => 15,
        ConnectFailureCategory::Implementation => 16,
    };
    LAST_CONNECT_FAILURE.store(encoded, Ordering::SeqCst);
}

fn classify_connect_failure(error: &mssql_tds::error::Error) -> ConnectFailureCategory {
    match error {
        mssql_tds::error::Error::TlsHandshakeError { .. } => ConnectFailureCategory::TlsHandshake,
        mssql_tds::error::Error::CertificateExpired => ConnectFailureCategory::CertificateExpired,
        mssql_tds::error::Error::CertificateMismatch => ConnectFailureCategory::CertificateMismatch,
        mssql_tds::error::Error::ConnectionError(_) => ConnectFailureCategory::Transport,
        mssql_tds::error::Error::ProtocolError(_) => ConnectFailureCategory::Protocol,
        mssql_tds::error::Error::SqlServerError { .. } => ConnectFailureCategory::SqlServerLogin,
        mssql_tds::error::Error::Io(_) => ConnectFailureCategory::Io,
        mssql_tds::error::Error::TimeoutError(mssql_tds::error::TimeoutErrorType::String(
            message,
        )) if message == "Timeout while connecting" => ConnectFailureCategory::ConnectTimeout,
        mssql_tds::error::Error::TimeoutError(_) => ConnectFailureCategory::OtherTimeout,
        mssql_tds::error::Error::ConnectionClosed(_) => ConnectFailureCategory::ConnectionClosed,
        mssql_tds::error::Error::TlsError(_) => ConnectFailureCategory::TlsLibrary,
        mssql_tds::error::Error::Security(_) => ConnectFailureCategory::Security,
        mssql_tds::error::Error::Redirection { .. } => ConnectFailureCategory::Redirection,
        mssql_tds::error::Error::UsageError(_) => ConnectFailureCategory::Usage,
        mssql_tds::error::Error::ImplementationError(_) => ConnectFailureCategory::Implementation,
        _ => ConnectFailureCategory::Other,
    }
}

fn sql_login_diagnostic() -> Option<(u32, u8, u8)> {
    let diagnostic = LAST_SQL_LOGIN_DIAGNOSTIC.load(Ordering::SeqCst);
    (diagnostic != 0).then_some((
        u32::try_from(diagnostic >> 16).ok()?,
        u8::try_from((diagnostic >> 8) & 0xff).ok()?,
        u8::try_from(diagnostic & 0xff).ok()?,
    ))
}

pub(super) fn last_connect_failure() -> Option<ConnectFailureCategory> {
    match LAST_CONNECT_FAILURE.load(Ordering::SeqCst) {
        1 => Some(ConnectFailureCategory::TlsHandshake),
        2 => Some(ConnectFailureCategory::CertificateExpired),
        3 => Some(ConnectFailureCategory::CertificateMismatch),
        4 => Some(ConnectFailureCategory::Transport),
        5 => Some(ConnectFailureCategory::Protocol),
        6 => Some(ConnectFailureCategory::SqlServerLogin),
        7 => Some(ConnectFailureCategory::Io),
        8 => Some(ConnectFailureCategory::Other),
        9 => Some(ConnectFailureCategory::ConnectTimeout),
        10 => Some(ConnectFailureCategory::OtherTimeout),
        11 => Some(ConnectFailureCategory::ConnectionClosed),
        12 => Some(ConnectFailureCategory::TlsLibrary),
        13 => Some(ConnectFailureCategory::Security),
        14 => Some(ConnectFailureCategory::Redirection),
        15 => Some(ConnectFailureCategory::Usage),
        16 => Some(ConnectFailureCategory::Implementation),
        _ => None,
    }
}

pub(super) fn arm_next_commit_fault() {
    ARM_NEXT_COMMIT_FAULT.store(true, Ordering::SeqCst);
}

pub(super) fn before_commit_dispatch() {
    if ARM_NEXT_COMMIT_FAULT.swap(false, Ordering::SeqCst) {
        COMMIT_FAULT_ARMED.store(true, Ordering::SeqCst);
    }
}

fn context(password: &str) -> ClientContext {
    let mut context = ClientContext::default();
    context.user_name = "sa".into();
    context.password = password.into();
    context.database = "master".into();
    context.connect_timeout = 5;
    context
}

#[derive(Debug, Default)]
struct ProxyStats {
    accepted: std::sync::atomic::AtomicUsize,
    connected: std::sync::atomic::AtomicUsize,
    refused: std::sync::atomic::AtomicUsize,
    other_connect_errors: std::sync::atomic::AtomicUsize,
}

fn report_fixture_resources(fixture: &str) {
    let read_number = |path| {
        fs::read_to_string(path)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
    };
    eprintln!(
        "{fixture} cgroup memory.current={:?} memory.max={:?}",
        read_number("/sys/fs/cgroup/memory.current"),
        read_number("/sys/fs/cgroup/memory.max"),
    );
}

async fn proxy(target: SocketAddr) -> (u16, tokio::task::JoinHandle<()>, Arc<ProxyStats>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let stats = Arc::new(ProxyStats::default());
    let proxy_stats = Arc::clone(&stats);
    let task = tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            proxy_stats.accepted.fetch_add(1, Ordering::SeqCst);
            let stats = Arc::clone(&proxy_stats);
            tokio::spawn(async move {
                match tokio::net::TcpStream::connect(target).await {
                    Ok(mut outbound) => {
                        stats.connected.fetch_add(1, Ordering::SeqCst);
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        stats.refused.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        stats.other_connect_errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
        }
    });
    (port, task, stats)
}

/// Hold the encrypted COMMIT response until an independent observer confirms
/// both migration application and its ledger row, then cut the client stream.
async fn commit_fault_proxy(
    target: SocketAddr,
) -> (
    u16,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    COMMIT_FAULT_ARMED.store(false, Ordering::SeqCst);
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let Ok((client, _)) = listener.accept().await else {
            return;
        };
        let Ok(server) = tokio::net::TcpStream::connect(target).await else {
            return;
        };
        let (mut client_reader, mut client_writer) = client.into_split();
        let (mut server_reader, mut server_writer) = server.into_split();
        let request_seen = Arc::new(AtomicBool::new(false));
        let client_request_seen = request_seen.clone();
        let client_to_server = tokio::spawn(async move {
            let mut buffer = [0; 16 * 1024];
            loop {
                let count = client_reader.read(&mut buffer).await?;
                if count == 0 {
                    return Ok::<(), std::io::Error>(());
                }
                if COMMIT_FAULT_ARMED.load(Ordering::SeqCst) {
                    client_request_seen.store(true, Ordering::SeqCst);
                }
                server_writer.write_all(&buffer[..count]).await?;
            }
        });
        let mut release_rx = release_rx;
        let mut buffer = [0; 16 * 1024];
        while let Ok(count) = server_reader.read(&mut buffer).await {
            if count == 0 {
                break;
            }
            if COMMIT_FAULT_ARMED.load(Ordering::SeqCst) && request_seen.load(Ordering::SeqCst) {
                held_tx
                    .send(())
                    .expect("observer still awaits held COMMIT response");
                tokio::time::timeout(Duration::from_secs(40), &mut release_rx)
                    .await
                    .expect("observer exceeded the COMMIT response hold deadline")
                    .expect("observer dropped the COMMIT response release");
                let _ = client_writer.shutdown().await;
                client_to_server.abort();
                return;
            }
            if client_writer.write_all(&buffer[..count]).await.is_err() {
                break;
            }
        }
        client_to_server.abort();
    });
    (port, held_rx, release_tx, task)
}

fn int(value: i32) -> RpcParameter {
    RpcParameter::new(
        Some("@P1".into()),
        StatusFlags::NONE,
        SqlType::Int(Some(value)),
    )
}

async fn connect(source: SocketAddrV4, password: &str) -> Probe {
    for _ in 0..30 {
        if let Ok(client) = Probe::connect_insecure_test(context(password), source).await {
            return client;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("disposable server did not accept a connection")
}

fn scalar(sets: &[Vec<Vec<ColumnValues>>]) -> i32 {
    match sets[0][0][0] {
        ColumnValues::Int(value) => value,
        _ => panic!("unexpected scalar type"),
    }
}

fn tls_commit_migrations(table: &str) -> neotales_migration::Migrations {
    neotales_migration::Migrations::new(vec![
        neotales_migration::Migration::new(
            1,
            "create",
            format!("CREATE TABLE {table} ( n BIGINT )"),
            Some(format!("DROP TABLE {table}")),
        )
        .unwrap(),
        neotales_migration::Migration::new(
            2,
            "seed",
            format!("INSERT INTO {table} VALUES (7)"),
            None,
        )
        .unwrap(),
    ])
    .unwrap()
}

/// Child-process endpoint for TLS acceptance cases. A dedicated process lets
/// OpenSSL use `SSL_CERT_FILE` without mutating trust for the test runner.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one child process owns TLS identity checks and the held-COMMIT observer barrier"
)]
#[allow(
    clippy::large_futures,
    reason = "the child scenario is bounded and runs on its dedicated thread"
)]
fn tls_child() {
    let Ok(case) = std::env::var("DIRECT_TLS_CHILD_CASE") else {
        return;
    };
    tls_diagnostic::install_panic_hook();
    assert!(
        tls_diagnostic::CASES.contains(&case.as_str()),
        "unknown TLS case"
    );
    tls_diagnostic::phase("configure");
    let marker_case = case.clone();
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(TLS_CHILD_SCENARIO_DEADLINE, async move {
                        let port = std::env::var("DIRECT_TLS_PORT").unwrap().parse().unwrap();
                        let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
                        let mut client_context =
                            context(&std::env::var("DIRECT_TLS_PASSWORD").unwrap());
                        match case.as_str() {
                            "expired" | "pin-valid" => {
                                client_context.encryption_options.server_certificate =
                                    Some(std::env::var("DIRECT_TLS_PIN").unwrap().into());
                            }
                            "pin-invalid" => {
                                client_context.encryption_options.server_certificate =
                                    Some(std::env::var("DIRECT_TLS_BAD_PIN").unwrap().into());
                            }
                        _ => {
                            client_context.encryption_options.host_name_in_cert =
                                Some(std::env::var("DIRECT_TLS_EXPECTED_NAME").unwrap());
                        }
                    }
                    if case == "insecure-reachability" {
                        client_context.encryption_options.host_name_in_cert = None;
                    }
                        if case == "commit-lost" {
                            tls_diagnostic::phase("migration-start");
                            client_context.encryption_options.host_name_in_cert =
                                Some("sql.fixture.test".into());
                            let target_ip: Ipv4Addr = std::env::var("DIRECT_TLS_TARGET_IP")
                                .unwrap()
                                .parse()
                                .unwrap();
                            let (fault_port, held, release, fault_task) =
                                commit_fault_proxy(SocketAddr::new(target_ip.into(), 1433)).await;
                            let fault_address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, fault_port);
                            let direct = DirectPool::new(
                                ExclusivePool::new(
                                    client_context,
                                    fault_address,
                                    1,
                                    Duration::from_secs(5),
                                )
                                .unwrap(),
                            );
                            let table = format!("direct_tls_commit_{}", std::process::id());
                            let history =
                                format!("direct_tls_commit_history_{}", std::process::id());
                            let module = format!("direct_tls_commit_{}", std::process::id());
                            let migrations = tls_commit_migrations(&table);
                            let backend =
                                SqlServerBackend::new("dbo", &history, Duration::from_secs(5))
                                    .unwrap();
                            let migration_pool = direct.clone();
                            let migration_module = module.clone();
                            let migration_table = table.clone();
                            let mut migration_task = tokio::spawn(async move {
                                backend
                                    .up(
                                        &migration_pool,
                                        &migration_module,
                                        &migrations,
                                        "tls-commit-binding",
                                        &[("dbo", migration_table.as_str())],
                                    )
                                    .await
                            });
                            arm_next_commit_fault();
                            tokio::select! {
                                held_result = tokio::time::timeout(Duration::from_secs(20), held) => {
                                    held_result.expect("COMMIT response was not held")
                                        .expect("COMMIT proxy ended before holding its response");
                                }
                                migration_result = &mut migration_task => {
                                    panic!(
                                        "migration ended before the COMMIT response was held: {migration_result:?}"
                                    );
                                }
                            }

                            let server_ip = target_ip;
                            tls_diagnostic::phase("commit-held");
                            tls_diagnostic::phase("observer-connect");
                            let mut observer_context =
                                context(&std::env::var("DIRECT_TLS_PASSWORD").unwrap());
                            observer_context.encryption_options.host_name_in_cert =
                                Some("sql.fixture.test".into());
                            let mut observer = tokio::time::timeout(
                                Duration::from_secs(15),
                                Probe::connect(observer_context, SocketAddrV4::new(server_ip, 1433)),
                            )
                            .await
                            .expect("observer connection phase exceeded 15s")
                            .unwrap_or_else(|error| {
                                panic!("observer connection phase failed: {error:?}")
                            });
                            tls_diagnostic::phase("observer-query");
                            let application_count_sql = format!("SELECT COUNT(*) FROM dbo.{table}");
                            let ledger_count_sql = format!(
                                "SELECT COUNT(*) FROM dbo.{history} WHERE module = N'{module}'"
                            );
                            tokio::time::timeout(Duration::from_secs(20), async {
                                loop {
                                    let application_count = observer
                                        .query(&application_count_sql, vec![])
                                        .await
                                        .unwrap_or_else(|error| {
                                            panic!(
                                                "observer application query phase failed: {error:?}"
                                            )
                                        });
                                    let ledger_count = observer
                                        .query(&ledger_count_sql, vec![])
                                        .await
                                        .unwrap_or_else(|error| {
                                            panic!(
                                                "observer ledger query phase failed: {error:?}"
                                            )
                                        });
                                    if scalar(&application_count) == 1
                                        && scalar(&ledger_count) == 2
                                    {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            })
                            .await
                            .expect("observer deadline expired before committed state was visible");
                            tls_diagnostic::phase("commit-release");
                            release
                                .send(())
                                .expect("COMMIT proxy still waits for observer confirmation");
                            tls_diagnostic::phase("migration-result");
                            let migration_error =
                                tokio::time::timeout(Duration::from_secs(10), migration_task)
                                    .await
                                    .expect("migration did not return after connection cut")
                                    .expect("migration task panicked")
                                    .expect_err("migration falsely reported successful COMMIT");
                        assert!(
                            matches!(
                                migration_error,
                                    crate::migration::Error::Database(ref error)
                                        if error.kind() == ErrorKind::CommitOutcomeUnknown
                                ),
                            "unexpected migration error category after lost COMMIT"
                        );

                        let mut reconciliation_context =
                            context(&std::env::var("DIRECT_TLS_PASSWORD").unwrap());
                        reconciliation_context.encryption_options.host_name_in_cert =
                            Some("sql.fixture.test".into());
                        let reconciliation_pool = DirectPool::new(
                            ExclusivePool::new(
                                reconciliation_context,
                                address,
                                1,
                                Duration::from_secs(5),
                            )
                            .unwrap(),
                        );
                        let reconciliation_events = Arc::new(std::sync::Mutex::new(Vec::new()));
                        tls_diagnostic::phase("reconciliation");
                        let reconciled = RECONCILIATION_DIAGNOSTIC.scope(
                            ReconciliationDiagnostic {
                                start: std::time::Instant::now(),
                                events: reconciliation_events.clone(),
                            },
                            tokio::time::timeout(
                            Duration::from_secs(20),
                            SqlServerBackend::new("dbo", &history, Duration::from_secs(5))
                                .unwrap()
                                .up(
                                    &reconciliation_pool,
                                    &module,
                                    &tls_commit_migrations(&table),
                                    "tls-commit-binding",
                                    &[("dbo", table.as_str())],
                                ),
                            ),
                        ).await;
                        eprintln!("reconciliation sanitized phases: {:?}", reconciliation_events.lock().unwrap());
                        let reconciled = reconciled
                        .expect("reconciliation phase exceeded 20s")
                        .expect("reconciliation phase failed");
                            assert_eq!(reconciled.applied_count(), 2);
                            tls_diagnostic::phase("verify-application");
                            assert_eq!(
                                scalar(
                                    &observer
                                    .query(&application_count_sql, vec![])
                                    .await
                                    .unwrap_or_else(|error| {
                                        panic!(
                                            "reconciliation application verification failed: {error:?}"
                                        )
                                    })
                                ),
                                1,
                                "repeated up duplicated the application migration"
                            );
                            tls_diagnostic::phase("verify-ledger");
                            assert_eq!(
                                scalar(
                                    &observer
                                        .query(&ledger_count_sql, vec![])
                                        .await
                                        .unwrap_or_else(|error| {
                                            panic!(
                                                "reconciliation ledger verification failed: {error:?}"
                                            )
                                        })
                                ),
                                2,
                                "repeated up duplicated the history row"
                            );
                            fault_task.abort();
                        } else if case == "valid" || case == "pin-valid" {
                            tls_diagnostic::phase("connect-query");
                            let pool = DirectPool::new(
                                ExclusivePool::new(
                                    client_context,
                                    address,
                                    1,
                                    Duration::from_secs(5),
                                )
                                .unwrap(),
                            );
                            pool.fetch_all_async("SELECT 73 AS tls_probe", QueryOptions::new())
                                .await
                                .unwrap_or_else(|error| {
                                    panic!("secure SQL Server TLS connection failed: {error:?}")
                                });
                        } else if case == "insecure-reachability" {
                            tls_diagnostic::phase("connect-query");
                            let mut probe =
                                Probe::connect_insecure_test(client_context, address)
                                    .await
                                    .unwrap_or_else(|error| {
                                        panic!(
                                            "test-only reachability control failed: {error:?}; \
                                             driver_failure={:?}; \
                                             sql_login_number_state_class={:?}",
                                            last_connect_failure(),
                                            sql_login_diagnostic()
                                        )
                                    });
                            let rows = probe.query("SELECT 73", vec![]).await.unwrap();
                            assert_eq!(scalar(&rows), 73);
                        } else {
                            tls_diagnostic::phase("identity-rejection");
                            reset_connect_failure();
                            let pool = DirectPool::new(
                                ExclusivePool::new(
                                    client_context,
                                    address,
                                    1,
                                    Duration::from_secs(5),
                                )
                                .unwrap(),
                            );
                            let result = pool
                                .fetch_all_async("SELECT 73 AS tls_probe", QueryOptions::new())
                                .await;
                            assert!(
                                matches!(
                                    result,
                                    Err(ref error) if error.kind() == ErrorKind::Connection
                                ),
                                "invalid identity did not fail during connection setup"
                            );
                            let expected = match case.as_str() {
                                "pin-invalid" => ConnectFailureCategory::CertificateMismatch,
                            "expired" => ConnectFailureCategory::CertificateExpired,
                            "expired-ca" | "wrong-ca" | "wrong-host" => {
                                ConnectFailureCategory::TlsHandshake
                            }
                                _ => panic!("unknown TLS case"),
                            };
                            assert_eq!(
                                last_connect_failure(),
                                Some(expected),
                                "identity failure was not the expected TLS validation rejection"
                            );
                        }
                    })
                    .await
                    .expect("TLS child scenario exceeded its operation deadline");
                    tls_diagnostic::phase("complete");
                    eprintln!("TLS_CASE_COMPLETE={marker_case}");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_openssl(directory: &Path, arguments: &[&str]) {
    let mut command = tokio::process::Command::new("openssl");
    command.args(arguments).current_dir(directory);
    match run_child(command, OPENSSL_COMMAND_DEADLINE).await {
        Ok(output) => assert!(
            output.status.success(),
            "openssl failed to generate TLS fixture"
        ),
        Err(ChildRunFailure::Spawn(error)) => {
            panic!("openssl fixture process could not start: {error:?}")
        }
        Err(ChildRunFailure::Timeout { cleanup, .. }) => {
            panic!("openssl fixture process timed out; cleanup={cleanup:?}")
        }
        Err(ChildRunFailure::Wait { cleanup, .. }) => {
            panic!("openssl fixture process wait failed; cleanup={cleanup:?}")
        }
        Err(ChildRunFailure::Cancelled { cleanup }) => {
            panic!("openssl fixture process cancelled; cleanup={cleanup:?}")
        }
        Err(ChildRunFailure::Supervisor) => {
            panic!("openssl fixture process supervisor failed")
        }
        #[cfg(not(unix))]
        Err(ChildRunFailure::UnsupportedPlatform) => {
            panic!("OpenSSL fixture supervision requires Unix process groups")
        }
    }
}

struct TestDirectory(PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn make_tls_material(directory: &Path, expired: bool) {
    fs::create_dir_all(directory).unwrap();
    run_openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=Direct Fixture CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
    )
    .await;
    run_openssl(
        directory,
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=sql.fixture.test",
        ],
    )
    .await;
    fs::write(
        directory.join("server.ext"),
        "subjectAltName=DNS:sql.fixture.test\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n",
    )
    .unwrap();
    run_openssl(
        directory,
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            if expired { "0" } else { "2" },
            "-sha256",
            "-extfile",
            "server.ext",
        ],
    )
    .await;
    run_openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "wrong-ca.key",
            "-out",
            "wrong-ca.pem",
            "-days",
            "2",
            "-subj",
            "/CN=Wrong Fixture CA",
        ],
    )
    .await;
}

#[allow(
    clippy::too_many_arguments,
    reason = "child invocation needs isolated case, endpoint, trust and fixture inputs"
)]
async fn invoke_tls_child(
    _ready: &FixtureLoginReady,
    case: &str,
    port: u16,
    password: &str,
    expected_name: &str,
    ca_file: &Path,
    directory: &Path,
    target_ip: Option<std::net::Ipv4Addr>,
) {
    assert!(tls_diagnostic::CASES.contains(&case), "unknown TLS case");
    eprintln!("TLS case {case} started");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "live::tls_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("DIRECT_TLS_CHILD_CASE", case)
        .env("DIRECT_TLS_PORT", port.to_string())
        .env("DIRECT_TLS_PASSWORD", password)
        .env("DIRECT_TLS_EXPECTED_NAME", expected_name)
        .env("SSL_CERT_FILE", ca_file)
        .env("SSL_CERT_DIR", directory.join("empty-ca-dir"))
        .env("DIRECT_TLS_PIN", directory.join("server.pem"))
        .env("DIRECT_TLS_BAD_PIN", directory.join("wrong-ca.pem"))
        .env(
            "DIRECT_TLS_TARGET_IP",
            target_ip.map_or_else(String::new, |ip| ip.to_string()),
        );
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = match run_child(command, TLS_CHILD_WATCHDOG).await {
        Ok(output) => output,
        Err(ChildRunFailure::Timeout {
            stdout,
            stderr,
            cleanup,
        }) => panic!(
            "TLS child case {case} exceeded its 120s wait/drain watchdog; \
             cleanup={cleanup:?}; sanitized stdout: {}; sanitized stderr: {}",
            tls_diagnostic::summarize(&stdout),
            tls_diagnostic::summarize(&stderr)
        ),
        Err(ChildRunFailure::Wait {
            stdout,
            stderr,
            cleanup,
        }) => panic!(
            "TLS child case {case} wait failed; cleanup={cleanup:?}; sanitized stdout: {}; sanitized stderr: {}",
            tls_diagnostic::summarize(&stdout),
            tls_diagnostic::summarize(&stderr)
        ),
        Err(ChildRunFailure::Spawn(error)) => {
            panic!("TLS child case {case} could not start: {error:?}")
        }
        #[cfg(not(unix))]
        Err(ChildRunFailure::UnsupportedPlatform) => {
            panic!("TLS child process supervision requires Unix process groups")
        }
        Err(ChildRunFailure::Supervisor) => {
            panic!("TLS child case {case} supervisor task failed")
        }
        Err(ChildRunFailure::Cancelled { cleanup }) => {
            panic!("TLS child case {case} cancelled; cleanup={cleanup:?}")
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let executed_test = "test live::tls_child ... ok";
    let completion_marker = format!("TLS_CASE_COMPLETE={case}");
    let diagnostic = tls_diagnostic::summarize(&output.stderr);
    eprintln!("TLS case {case} diagnostic: {diagnostic}");
    assert!(
        output.status.success(),
        "isolated TLS case {case} failed; sanitized child output: {diagnostic}"
    );
    assert_eq!(
        stdout.matches("running 1 test").count(),
        1,
        "child must execute exactly one test"
    );
    assert_eq!(
        stdout.matches(executed_test).count(),
        1,
        "expected exactly one successful child test"
    );
    assert_eq!(
        stderr
            .lines()
            .filter(|line| *line == completion_marker)
            .count(),
        1,
        "child must emit its explicit case completion marker"
    );
}

#[derive(Debug)]
struct ChildOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Debug)]
struct ChildCleanup {
    group_kill_error: Option<std::io::ErrorKind>,
    child_kill_error: Option<std::io::ErrorKind>,
    initial_wait_error: Option<std::io::ErrorKind>,
    wait_error: Option<std::io::ErrorKind>,
    reaped: bool,
    drainers_joined: bool,
    drainer_error: Option<std::io::ErrorKind>,
}

impl ChildCleanup {
    fn succeeded(&self) -> bool {
        self.group_kill_error.is_none()
            && self.child_kill_error.is_none()
            && self.initial_wait_error.is_none()
            && self.wait_error.is_none()
            && self.reaped
            && self.drainers_joined
            && self.drainer_error.is_none()
    }
}

#[derive(Debug)]
enum ChildRunFailure {
    Spawn(std::io::ErrorKind),
    Supervisor,
    #[cfg(not(unix))]
    UnsupportedPlatform,
    Timeout {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        cleanup: ChildCleanup,
    },
    Cancelled {
        cleanup: ChildCleanup,
    },
    Wait {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        cleanup: ChildCleanup,
    },
}

#[cfg(unix)]
struct ChildSupervisor {
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<ChildOutput, ChildRunFailure>>,
}

#[cfg(unix)]
impl ChildSupervisor {
    fn start(mut command: tokio::process::Command, watchdog: Duration) -> Self {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let count = child_supervisors().start();
        let task = tokio::spawn(async move {
            let _count = count;
            let result = supervise_child(command, watchdog, cancel_rx).await;
            if let Err(ChildRunFailure::Cancelled { cleanup }) = &result
                && !cleanup.succeeded()
            {
                eprintln!("child cancellation cleanup incomplete: {cleanup:?}");
            }
            result
        });
        Self {
            cancel: Some(cancel_tx),
            task,
        }
    }

    async fn wait(mut self) -> Result<ChildOutput, ChildRunFailure> {
        let result = (&mut self.task)
            .await
            .unwrap_or(Err(ChildRunFailure::Supervisor));
        self.cancel.take();
        result
    }
}

#[cfg(unix)]
impl Drop for ChildSupervisor {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

async fn run_child(
    command: tokio::process::Command,
    watchdog: Duration,
) -> Result<ChildOutput, ChildRunFailure> {
    #[cfg(unix)]
    {
        ChildSupervisor::start(command, watchdog).wait().await
    }
    #[cfg(not(unix))]
    {
        let _ = (command, watchdog);
        Err(ChildRunFailure::UnsupportedPlatform)
    }
}

#[cfg(unix)]
enum ChildEvent {
    Exited(std::process::ExitStatus),
    WaitFailed(std::io::ErrorKind),
    Timeout,
    Cancelled,
}

#[cfg(unix)]
type ReaderResult = (Vec<u8>, std::io::Result<usize>);
#[cfg(unix)]
type ReaderTask = tokio::task::JoinHandle<(ReaderResult, ReaderResult)>;

#[cfg(unix)]
async fn collect_readers(
    stdout: impl std::future::Future<Output = ReaderResult>,
    stderr: impl std::future::Future<Output = ReaderResult>,
) -> (ReaderResult, ReaderResult) {
    tokio::join!(stdout, stderr)
}

#[cfg(unix)]
async fn supervise_child(
    mut command: tokio::process::Command,
    watchdog: Duration,
    mut cancel: tokio::sync::oneshot::Receiver<()>,
) -> Result<ChildOutput, ChildRunFailure> {
    let mut child = command
        .spawn()
        .map_err(|error| ChildRunFailure::Spawn(error.kind()))?;
    let child_id = child.id();
    let mut stdout = child.stdout.take().expect("child stdout is piped");
    let mut stderr = child.stderr.take().expect("child stderr is piped");
    // The two reads live inside one task. Cancelling a wait for its join handle
    // never consumes one completed read while leaving the other still pending.
    let mut readers = tokio::spawn(collect_readers(
        async move {
            let mut bytes = Vec::new();
            let result = stdout.read_to_end(&mut bytes).await;
            (bytes, result)
        },
        async move {
            let mut bytes = Vec::new();
            let result = stderr.read_to_end(&mut bytes).await;
            (bytes, result)
        },
    ));
    let deadline = tokio::time::Instant::now() + watchdog;
    let event = tokio::select! {
        result = child.wait() => match result {
            Ok(status) => ChildEvent::Exited(status),
            Err(error) => ChildEvent::WaitFailed(error.kind()),
        },
        () = tokio::time::sleep_until(deadline) => ChildEvent::Timeout,
        _ = &mut cancel => ChildEvent::Cancelled,
    };
    match event {
        ChildEvent::Exited(status) => {
            let drains = tokio::select! {
                result = &mut readers => Ok(result),
                () = tokio::time::sleep_until(deadline) => Err(false),
                _ = &mut cancel => Err(true),
            };
            if let Ok(drains) = drains {
                let (stdout, stderr, drainer_error) = join_readers(drains);
                if drainer_error.is_none() {
                    return Ok(ChildOutput {
                        status,
                        stdout,
                        stderr,
                    });
                }
                let cleanup = ChildCleanup {
                    group_kill_error: child_id.and_then(|id| kill_child_group(id).err()),
                    child_kill_error: None,
                    initial_wait_error: None,
                    wait_error: None,
                    reaped: true,
                    drainers_joined: true,
                    drainer_error,
                };
                Err(ChildRunFailure::Wait {
                    stdout,
                    stderr,
                    cleanup,
                })
            } else {
                let cancelled = matches!(drains, Err(true));
                let (cleanup, stdout, stderr) =
                    cleanup_child(&mut child, child_id, true, &mut readers).await;
                if cancelled {
                    Err(ChildRunFailure::Cancelled { cleanup })
                } else {
                    Err(ChildRunFailure::Timeout {
                        stdout,
                        stderr,
                        cleanup,
                    })
                }
            }
        }
        ChildEvent::WaitFailed(wait_error) => {
            let (cleanup, stdout, stderr) =
                cleanup_child(&mut child, child_id, false, &mut readers).await;
            Err(ChildRunFailure::Wait {
                stdout,
                stderr,
                cleanup: ChildCleanup {
                    initial_wait_error: Some(wait_error),
                    ..cleanup
                },
            })
        }
        ChildEvent::Timeout => {
            let (cleanup, stdout, stderr) =
                cleanup_child(&mut child, child_id, false, &mut readers).await;
            Err(ChildRunFailure::Timeout {
                stdout,
                stderr,
                cleanup,
            })
        }
        ChildEvent::Cancelled => {
            let (cleanup, _, _) = cleanup_child(&mut child, child_id, false, &mut readers).await;
            Err(ChildRunFailure::Cancelled { cleanup })
        }
    }
}

#[cfg(unix)]
fn join_readers(
    result: Result<(ReaderResult, ReaderResult), tokio::task::JoinError>,
) -> (Vec<u8>, Vec<u8>, Option<std::io::ErrorKind>) {
    match result {
        Ok(((stdout, stdout_result), (stderr, stderr_result))) => (
            stdout,
            stderr,
            stdout_result
                .err()
                .or_else(|| stderr_result.err())
                .map(|e| e.kind()),
        ),
        Err(_) => (Vec::new(), Vec::new(), Some(std::io::ErrorKind::Other)),
    }
}

#[cfg(unix)]
async fn cleanup_child(
    child: &mut tokio::process::Child,
    child_id: Option<u32>,
    already_reaped: bool,
    readers: &mut ReaderTask,
) -> (ChildCleanup, Vec<u8>, Vec<u8>) {
    let group_kill_error = match child_id {
        Some(id) => kill_child_group(id).err(),
        None => Some(std::io::ErrorKind::Unsupported),
    };
    let child_kill_error = if !already_reaped && group_kill_error.is_some() {
        child.start_kill().err().map(|error| error.kind())
    } else {
        None
    };
    let cleanup_deadline = tokio::time::Instant::now() + CHILD_CLEANUP_DEADLINE;
    let (reaped, wait_error) = if already_reaped {
        (true, None)
    } else {
        match tokio::time::timeout_at(cleanup_deadline, child.wait()).await {
            Ok(Ok(_)) => (true, None),
            Ok(Err(error)) => (false, Some(error.kind())),
            Err(_) => (false, Some(std::io::ErrorKind::TimedOut)),
        }
    };
    let drainer_join = tokio::time::timeout_at(cleanup_deadline, &mut *readers).await;
    let (drainers_joined, drainer_error, stdout, stderr) = if let Ok(result) = drainer_join {
        let (stdout, stderr, error) = join_readers(result);
        (true, error, stdout, stderr)
    } else {
        readers.abort();
        let _ = readers.await;
        (
            true,
            Some(std::io::ErrorKind::TimedOut),
            Vec::new(),
            Vec::new(),
        )
    };
    (
        ChildCleanup {
            group_kill_error,
            child_kill_error,
            initial_wait_error: None,
            wait_error,
            reaped,
            drainers_joined,
            drainer_error,
        },
        stdout,
        stderr,
    )
}

#[cfg(unix)]
fn kill_child_group(child_id: u32) -> Result<(), std::io::ErrorKind> {
    let process_group = i32::try_from(child_id).map_err(|_| std::io::ErrorKind::InvalidInput)?;
    match nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(process_group),
        nix::sys::signal::Signal::SIGKILL,
    ) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(error) => Err(std::io::Error::from_raw_os_error(error as i32).kind()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginReadinessFailure {
    SqlLogin18456State1Class14,
    Other {
        stage: &'static str,
        category: Option<ConnectFailureCategory>,
        diagnostic: Option<(u32, u8, u8)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginReadinessError {
    Failed(LoginReadinessFailure),
    Deadline {
        attempts: usize,
        last: Option<LoginReadinessFailure>,
    },
}

struct FixtureLoginReady;

async fn wait_until_ready<F, Fut>(
    mut attempt: F,
    deadline: Duration,
    retry_delay: Duration,
) -> Result<usize, LoginReadinessError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), LoginReadinessFailure>>,
{
    let deadline_at = tokio::time::Instant::now() + deadline;
    let mut attempts = 0;
    let mut last = None;
    loop {
        match tokio::time::timeout_at(deadline_at, attempt()).await {
            Err(_) => return Err(LoginReadinessError::Deadline { attempts, last }),
            Ok(Ok(())) => return Ok(attempts),
            Ok(Err(failure @ LoginReadinessFailure::Other { .. })) => {
                return Err(LoginReadinessError::Failed(failure));
            }
            Ok(Err(failure @ LoginReadinessFailure::SqlLogin18456State1Class14)) => {
                attempts += 1;
                last = Some(failure);
                if tokio::time::timeout_at(deadline_at, tokio::time::sleep(retry_delay))
                    .await
                    .is_err()
                {
                    return Err(LoginReadinessError::Deadline { attempts, last });
                }
            }
        }
    }
}

async fn run_after_readiness<A, AFut, Action, ActionFut, Output>(
    attempt: A,
    deadline: Duration,
    retry_delay: Duration,
    action: Action,
) -> Result<(usize, Output), LoginReadinessError>
where
    A: FnMut() -> AFut,
    AFut: std::future::Future<Output = Result<(), LoginReadinessFailure>>,
    Action: FnOnce(FixtureLoginReady) -> ActionFut,
    ActionFut: std::future::Future<Output = Output>,
{
    let retries = wait_until_ready(attempt, deadline, retry_delay).await?;
    let output = action(FixtureLoginReady).await;
    Ok((retries, output))
}

/// Run fixture cases only after the SQL login/query readiness probe succeeds.
/// SQL error 18456/state 1/class 14 is ambiguous: it may be transient startup
/// readiness or invalid credentials/configuration, so retry it only within a
/// strict deadline and report the sanitized number/state/class on exhaustion.
async fn with_fixture_login_ready<Action, ActionFut, Output>(
    address: SocketAddrV4,
    password: &str,
    fixture: &str,
    proxy_stats: &ProxyStats,
    action: Action,
) where
    Action: FnOnce(FixtureLoginReady) -> ActionFut,
    ActionFut: std::future::Future<Output = Output>,
{
    let started = std::time::Instant::now();
    let result = run_after_readiness(
        || async {
            let attempt_started = std::time::Instant::now();
            reset_connect_failure();
            let result = match Probe::connect_insecure_test(context(password), address).await {
                Ok(mut probe) => match probe.query("SELECT 73", vec![]).await {
                    Ok(rows) if scalar(&rows) == 73 => Ok(()),
                    Ok(_) | Err(_) => Err(LoginReadinessFailure::Other {
                        stage: "query",
                        category: None,
                        diagnostic: None,
                    }),
                },
                Err(Error::Connect)
                    if last_connect_failure() == Some(ConnectFailureCategory::SqlServerLogin)
                        && sql_login_diagnostic() == Some((18456, 1, 14)) =>
                {
                    Err(LoginReadinessFailure::SqlLogin18456State1Class14)
                }
                Err(_) => Err(LoginReadinessFailure::Other {
                    stage: "connect",
                    category: last_connect_failure(),
                    diagnostic: sql_login_diagnostic(),
                }),
            };
            if let Err(failure) = result {
                eprintln!(
                    "{fixture} readiness attempt elapsed={:?} sanitized={failure:?}",
                    attempt_started.elapsed()
                );
            }
            result
        },
        TLS_LOGIN_READINESS_DEADLINE,
        Duration::from_millis(500),
        action,
    )
    .await;
    eprintln!(
        "{fixture} readiness/actions elapsed={:?}; proxy port={} accepted={} connected={} refused={} other_connect_errors={}",
        started.elapsed(),
        address.port(),
        proxy_stats.accepted.load(Ordering::SeqCst),
        proxy_stats.connected.load(Ordering::SeqCst),
        proxy_stats.refused.load(Ordering::SeqCst),
        proxy_stats.other_connect_errors.load(Ordering::SeqCst),
    );
    report_fixture_resources(fixture);
    match result {
        Ok((attempts, _)) if attempts > 0 => eprintln!(
            "{fixture} login readiness recovered after {attempts} SQL error 18456/state 1/class 14 responses"
        ),
        Ok(_) => {}
        Err(LoginReadinessError::Failed(failure)) => {
            panic!("{fixture} login readiness failed immediately: {failure:?}")
        }
        Err(LoginReadinessError::Deadline { attempts, last }) => panic!(
            "{fixture} login readiness exceeded 60s after {attempts} matching login failures; \
             last sanitized failure: {last:?}. Credentials/configuration may be invalid."
        ),
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    #[tokio::test]
    async fn reconciliation_phases_survive_timeout_and_are_scoped_and_bounded() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let diagnostic = ReconciliationDiagnostic {
            start: std::time::Instant::now(),
            events: events.clone(),
        };
        let result = RECONCILIATION_DIAGNOSTIC
            .scope(diagnostic, async {
                tokio::time::timeout(Duration::from_millis(10), async {
                    reconciliation_phase(ReconciliationPhase::Reconnect);
                    reconciliation_phase(ReconciliationPhase::Connected);
                    reconciliation_phase(ReconciliationPhase::LockRequest);
                    reconciliation_phase(ReconciliationPhase::LockStatus(-3));
                    for _ in 0..300 {
                        reconciliation_phase(ReconciliationPhase::Query);
                    }
                    std::future::pending::<()>().await;
                })
                .await
            })
            .await;
        assert!(result.is_err());
        reconciliation_phase(ReconciliationPhase::Commit);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 256);
        assert_eq!(events[0].1, ReconciliationPhase::Reconnect);
        assert_eq!(events[1].1, ReconciliationPhase::Connected);
        assert_eq!(events[2].1, ReconciliationPhase::LockRequest);
        assert_eq!(events[3].1, ReconciliationPhase::LockStatus(-3));
        assert!(events.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    }

    #[tokio::test]
    async fn lock_diagnostics_are_scoped_to_each_writer() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let start = std::time::Instant::now();
        let diagnostic = |writer| LockDiagnostic {
            writer,
            start,
            events: events.clone(),
        };
        let writer = |diagnostic, status| async move {
            LOCK_DIAGNOSTIC
                .scope(diagnostic, async move {
                    lock_event(LockEvent::Request);
                    tokio::task::yield_now().await;
                    lock_event(LockEvent::Status(status));
                    lock_event(LockEvent::CommitStart);
                    lock_event(LockEvent::CommitEnd(true));
                })
                .await;
        };
        tokio::join!(writer(diagnostic(1), -1), writer(diagnostic(2), -3));
        lock_event(LockEvent::Request);
        let events = events.lock().unwrap();
        for (id, status) in [(1, -1), (2, -3)] {
            let selected: Vec<_> = events.iter().filter(|event| event.0 == id).collect();
            assert_eq!(selected.len(), 4);
            assert_eq!(selected[0].2, LockEvent::Request);
            assert_eq!(selected[1].2, LockEvent::Status(status));
            assert_eq!(selected[2].2, LockEvent::CommitStart);
            assert_eq!(selected[3].2, LockEvent::CommitEnd(true));
            assert!(selected.windows(2).all(|pair| pair[0].1 <= pair[1].1));
        }
        assert_eq!(events.len(), 8);
    }

    #[test]
    fn timeout_classification_does_not_expose_driver_message() {
        use mssql_tds::error::{Error as DriverError, TimeoutErrorType};
        let timeout =
            DriverError::TimeoutError(TimeoutErrorType::String("Timeout while connecting".into()));
        let other = DriverError::TimeoutError(TimeoutErrorType::String("sensitive".into()));
        assert_eq!(
            classify_connect_failure(&timeout),
            ConnectFailureCategory::ConnectTimeout
        );
        assert_eq!(
            classify_connect_failure(&other),
            ConnectFailureCategory::OtherTimeout
        );
    }

    #[tokio::test]
    async fn matching_login_failure_retries_until_success() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempt_calls = calls.clone();
        let result = wait_until_ready(
            move || {
                let call = attempt_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call < 2 {
                        Err(LoginReadinessFailure::SqlLogin18456State1Class14)
                    } else {
                        Ok(())
                    }
                }
            },
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await;
        assert_eq!(result, Ok(2));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn other_failure_stops_without_retry() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempt_calls = calls.clone();
        let failure = LoginReadinessFailure::Other {
            stage: "connect",
            category: Some(ConnectFailureCategory::Transport),
            diagnostic: None,
        };
        let result = wait_until_ready(
            move || {
                attempt_calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(failure) }
            },
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await;
        assert_eq!(result, Err(LoginReadinessError::Failed(failure)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn persistent_login_failure_stops_at_deadline() {
        let started = tokio::time::Instant::now();
        let result = wait_until_ready(
            || async { Err(LoginReadinessFailure::SqlLogin18456State1Class14) },
            Duration::from_millis(15),
            Duration::from_millis(2),
        )
        .await;
        assert!(matches!(
            result,
            Err(LoginReadinessError::Deadline {
                attempts: 1..,
                last: Some(LoginReadinessFailure::SqlLogin18456State1Class14),
            })
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn tls_action_runs_only_after_successful_readiness() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempt_count = attempts.clone();
        let actions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let action_count = actions.clone();
        let result = run_after_readiness(
            move || {
                let attempt = attempt_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(LoginReadinessFailure::SqlLogin18456State1Class14)
                    } else {
                        Ok(())
                    }
                }
            },
            Duration::from_secs(1),
            Duration::ZERO,
            move |_ready| {
                let action_count = action_count.clone();
                async move {
                    assert_eq!(attempts.load(Ordering::SeqCst), 3);
                    action_count.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;
        assert_eq!(result, Ok((2, ())));
        assert_eq!(actions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_readiness_never_invokes_tls_action() {
        let actions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let action_count = actions.clone();
        let failure = LoginReadinessFailure::Other {
            stage: "connect",
            category: Some(ConnectFailureCategory::Transport),
            diagnostic: None,
        };
        let result = run_after_readiness(
            || async { Err(failure) },
            Duration::from_secs(1),
            Duration::ZERO,
            move |_ready| {
                let action_count = action_count.clone();
                async move {
                    action_count.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;
        assert_eq!(result, Err(LoginReadinessError::Failed(failure)));
        assert_eq!(actions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn readiness_deadline_never_invokes_tls_action() {
        let actions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let action_count = actions.clone();
        let result = run_after_readiness(
            || async { Err(LoginReadinessFailure::SqlLogin18456State1Class14) },
            Duration::from_millis(15),
            Duration::from_millis(2),
            move |_ready| {
                let action_count = action_count.clone();
                async move {
                    action_count.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(LoginReadinessError::Deadline { attempts: 1.., .. })
        ));
        assert_eq!(actions.load(Ordering::SeqCst), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_runner_drains_large_stdout_and_stderr_while_waiting() {
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2");
        let output = run_child(command, Duration::from_secs(5))
            .await
            .expect("high-output child should complete");
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 131_072);
        assert_eq!(output.stderr.len(), 131_072);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_runner_kills_and_reaps_at_watchdog_with_output_drained() {
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("printf child-stdout; printf child-stderr >&2; sleep 5");
        let started = tokio::time::Instant::now();
        let output = run_child(command, Duration::from_millis(25)).await;
        let Err(ChildRunFailure::Timeout {
            stdout,
            stderr,
            cleanup,
        }) = output
        else {
            panic!("sleeping child should hit watchdog");
        };
        assert!(String::from_utf8_lossy(&stdout).contains("child-stdout"));
        assert!(String::from_utf8_lossy(&stderr).contains("child-stderr"));
        assert!(
            cleanup.reaped,
            "watchdog cleanup must await child exit: {cleanup:?}"
        );
        assert!(
            cleanup.drainers_joined,
            "watchdog cleanup must join both drainers: {cleanup:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watchdog_includes_descendant_holding_inherited_pipes() {
        let mut command = tokio::process::Command::new("sh");
        command.arg("-c").arg("sleep 30 & exit 0");
        let started = tokio::time::Instant::now();
        let result = run_child(command, Duration::from_millis(50)).await;
        let Err(ChildRunFailure::Timeout { cleanup, .. }) = result else {
            panic!("inherited descendant pipe should hit the full wait/drain watchdog");
        };
        assert!(cleanup.reaped, "parent child should be reaped: {cleanup:?}");
        assert!(
            cleanup.drainers_joined,
            "descendant pipe drainers should be joined: {cleanup:?}"
        );
        assert!(
            cleanup.group_kill_error.is_none(),
            "isolated process group termination failed: {cleanup:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn partial_reader_completion_survives_cancelled_join() {
        let (first_done, first_seen) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let mut readers = tokio::spawn(collect_readers(
            async move {
                first_done.send(()).unwrap();
                (b"first".to_vec(), Ok(5))
            },
            async move {
                released.await.unwrap();
                (b"second".to_vec(), Ok(6))
            },
        ));
        first_seen.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut readers)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let (stdout, stderr, error) = join_readers(readers.await);
        assert_eq!(stdout, b"first");
        assert_eq!(stderr, b"second");
        assert_eq!(error, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn watchdog_handles_one_pipe_closed_before_the_other() {
        let mut command = tokio::process::Command::new("sh");
        // The descendant inherits stderr only. Once the parent exits, stdout
        // is at EOF while stderr remains open until the watchdog kills it.
        command
            .arg("-c")
            .arg("printf closed; exec 1>&-; sleep 30 & exit 0");
        let result = run_child(command, Duration::from_millis(150)).await;
        let Err(ChildRunFailure::Timeout {
            stdout, cleanup, ..
        }) = result
        else {
            panic!("stderr inherited by descendant should hit the watchdog");
        };
        assert_eq!(stdout, b"closed");
        assert!(cleanup.succeeded(), "cleanup failed: {cleanup:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn all_supervisor_waiters_wake_when_count_reaches_zero() {
        let tracker = SupervisorTracker::default();
        let count = tracker.start();
        let first = tracker.wait(Duration::from_secs(1));
        let second = tracker.wait(Duration::from_secs(1));
        tokio::pin!(first, second);
        // Poll both waits to registration while the supervisor is still active.
        tokio::select! {
            biased;
            _ = &mut first => panic!("first waiter returned before cleanup"),
            _ = &mut second => panic!("second waiter returned before cleanup"),
            () = tokio::task::yield_now() => {}
        }
        drop(count);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first, Ok(()));
        assert_eq!(second, Ok(()));
    }

    #[cfg(unix)]
    fn published_pid(contents: &str) -> Option<i32> {
        let pid: i32 = contents.strip_suffix('\n')?.parse().ok()?;
        (pid > 0).then_some(pid)
    }

    #[cfg(unix)]
    #[test]
    fn pid_publication_requires_complete_positive_integer() {
        assert_eq!(published_pid(""), None);
        assert_eq!(published_pid("123"), None);
        assert_eq!(published_pid("0\n"), None);
        assert_eq!(published_pid("123\n"), Some(123));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn caller_cancellation_keeps_supervisor_until_child_is_reaped() {
        let mut suffix = [0; 16];
        getrandom::fill(&mut suffix).unwrap();
        let unique = suffix.iter().fold(String::new(), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").unwrap();
            output
        });
        let directory = TestDirectory(std::env::temp_dir().join(format!(
            "mssql-child-cancel-{}-{unique}",
            std::process::id()
        )));
        fs::create_dir(&directory.0).unwrap();
        let pid_file = directory.0.join("child.pid");
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("printf '%s\\n' \"$$\" > \"$1\"; exec sleep 30")
            .arg("isolated-child")
            .arg(&pid_file);
        let caller = tokio::spawn(run_child(command, Duration::from_secs(30)));
        let pid_ready = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(pid) = fs::read_to_string(&pid_file)
                    .ok()
                    .and_then(|contents| published_pid(&contents))
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        caller.abort();
        let _ = caller.await;
        wait_for_child_supervisors(SUPERVISOR_CLEANUP_DEADLINE)
            .await
            .expect("supervisor must finish kill/reap after caller cancellation");
        let pid = pid_ready.expect("child process never published its complete PID");
        assert_eq!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH),
            "child PID should be absent, not merely inaccessible"
        );
    }
}

const TLS_IMAGE: &str = "mcr.microsoft.com/mssql/server";
const TLS_IMAGE_TAG: &str =
    "2022-latest@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f";
const TLS_READY_MARKER: &str = "SQL Server is now ready for client connections";
const TLS_STARTUP_COMMAND: &str = "set -e; mkdir -p /var/opt/mssql/secrets; cp /fixture/server.pem /var/opt/mssql/secrets/server.pem; cp /fixture/server.key /var/opt/mssql/secrets/server.key; chown mssql:mssql /var/opt/mssql/secrets/server.pem /var/opt/mssql/secrets/server.key; chmod 600 /var/opt/mssql/secrets/server.pem /var/opt/mssql/secrets/server.key; /opt/mssql/bin/mssql-conf set network.tlscert /var/opt/mssql/secrets/server.pem; /opt/mssql/bin/mssql-conf set network.tlskey /var/opt/mssql/secrets/server.key; exec su mssql -s /bin/bash -c /opt/mssql/bin/sqlservr";

fn tls_container_request(
    directory: &Path,
    password: &str,
) -> testcontainers::ContainerRequest<GenericImage> {
    GenericImage::new(TLS_IMAGE, TLS_IMAGE_TAG)
        .with_wait_for(WaitFor::log(LogWaitStrategy::stdout_or_stderr(
            TLS_READY_MARKER,
        )))
        .with_entrypoint("/bin/bash")
        .with_copy_to(
            CopyTargetOptions::new("/fixture/server.pem").with_mode(0o644),
            directory.join("server.pem"),
        )
        .with_copy_to(
            CopyTargetOptions::new("/fixture/server.key").with_mode(0o600),
            directory.join("server.key"),
        )
        .with_cmd(["-c", TLS_STARTUP_COMMAND])
        .with_user("0")
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", password)
        .with_env_var("MSSQL_PID", "Developer")
        .with_host_config_modifier(|host| {
            host.publish_all_ports = Some(false);
            host.port_bindings = Some(std::collections::HashMap::default());
        })
        .with_startup_timeout(TLS_FIXTURE_STARTUP_DEADLINE)
        .with_label("neotales.test.fixture", "dbapi-mssql-direct-tls")
}

fn run_live_scenario(
    runtime: &tokio::runtime::Runtime,
    deadline: Duration,
    scenario: impl std::future::Future<Output = ()>,
    name: &'static str,
) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            Box::pin(tokio::time::timeout(deadline, scenario))
                .await
                .unwrap_or_else(|_| panic!("{name} exceeded its {deadline:?} deadline"));
        });
    }));
    let cleanup = runtime.block_on(wait_for_child_supervisors(SUPERVISOR_CLEANUP_DEADLINE));
    if cleanup.is_err() {
        eprintln!("{name} child-supervisor cleanup did not finish within 10s");
    }
    match result {
        Ok(()) => assert!(cleanup.is_ok(), "{name} child cleanup did not finish"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
#[ignore = "requires Docker, OpenSSL, and the pinned SQL Server image"]
fn direct_tls_validation() {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            run_live_scenario(
                &runtime,
                TLS_SCENARIO_DEADLINE,
                direct_tls_validation_inner(),
                "TLS validation scenario",
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

// All identity outcomes share the same generated issuer and test server; keeping
// the sequence together makes clear which certificate each negative case uses.
#[allow(
    clippy::too_many_lines,
    reason = "TLS identity cases share generated material and disposable server"
)]
async fn direct_tls_validation_inner() {
    let mut suffix = [0; 12];
    getrandom::fill(&mut suffix).unwrap();
    let unique = suffix.iter().fold(String::new(), |mut output, byte| {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").unwrap();
        output
    });
    let temporary = TestDirectory(
        std::env::temp_dir().join(format!("mssql-tls-{}-{unique}", std::process::id())),
    );
    let directory = &temporary.0;
    make_tls_material(directory, false).await;
    fs::create_dir_all(directory.join("empty-ca-dir")).unwrap();
    let mut random = [0; 16];
    getrandom::fill(&mut random).unwrap();
    let password = format!(
        "T!a{}",
        random.iter().fold(String::new(), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").unwrap();
            output
        })
    );
    let startup = std::time::Instant::now();
    let container = tls_container_request(directory, &password)
        .start()
        .await
        .expect("start TLS-configured SQL Server");
    let server_ip = container.get_bridge_ip_address().await.unwrap();
    eprintln!(
        "trusted TLS fixture container startup elapsed={:?}",
        startup.elapsed()
    );
    report_fixture_resources("trusted TLS fixture startup");
    let (proxy_port, proxy_task, proxy_stats) = proxy(SocketAddr::new(server_ip, 1433)).await;
    let password_ref = password.as_str();
    Box::pin(with_fixture_login_ready(
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, proxy_port),
        password_ref,
        "trusted TLS fixture",
        &proxy_stats,
        |ready| async move {
            invoke_tls_child(
                &ready,
                "valid",
                proxy_port,
                password_ref,
                "sql.fixture.test",
                &directory.join("ca.pem"),
                directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "wrong-ca",
                proxy_port,
                password_ref,
                "sql.fixture.test",
                &directory.join("wrong-ca.pem"),
                directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "wrong-host",
                proxy_port,
                password_ref,
                "wrong.fixture.test",
                &directory.join("ca.pem"),
                directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "pin-valid",
                proxy_port,
                password_ref,
                "ignored.by.pin",
                &directory.join("wrong-ca.pem"),
                directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "pin-invalid",
                proxy_port,
                password_ref,
                "ignored.by.pin",
                &directory.join("wrong-ca.pem"),
                directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "commit-lost",
                proxy_port,
                password_ref,
                "sql.fixture.test",
                &directory.join("ca.pem"),
                directory,
                Some(server_ip.to_string().parse().unwrap()),
            )
            .await;
        },
    ))
    .await;
    drop(container);
    proxy_task.abort();

    // The test-only control proves SQL Server presents the expired certificate.
    // The driver's pin validator also enforces expiry, so a pinned positive
    // control cannot be used for this case.
    let expired_directory = directory.join("expired");
    make_tls_material(&expired_directory, true).await;
    fs::create_dir_all(expired_directory.join("empty-ca-dir")).unwrap();
    let startup = std::time::Instant::now();
    let expired_container = tls_container_request(&expired_directory, &password)
        .start()
        .await
        .expect("start SQL Server with expired fixture certificate");
    let expired_ip = expired_container.get_bridge_ip_address().await.unwrap();
    eprintln!(
        "expired TLS fixture container startup elapsed={:?}",
        startup.elapsed()
    );
    report_fixture_resources("expired TLS fixture startup");
    let (expired_port, expired_proxy, proxy_stats) = proxy(SocketAddr::new(expired_ip, 1433)).await;
    let password_ref = password.as_str();
    Box::pin(with_fixture_login_ready(
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, expired_port),
        password_ref,
        "expired TLS fixture",
        &proxy_stats,
        |ready| async move {
            invoke_tls_child(
                &ready,
                "insecure-reachability",
                expired_port,
                password_ref,
                "ignored.by.pin",
                &expired_directory.join("wrong-ca.pem"),
                &expired_directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "expired",
                expired_port,
                password_ref,
                "sql.fixture.test",
                &expired_directory.join("ca.pem"),
                &expired_directory,
                None,
            )
            .await;
            invoke_tls_child(
                &ready,
                "expired-ca",
                expired_port,
                password_ref,
                "sql.fixture.test",
                &expired_directory.join("ca.pem"),
                &expired_directory,
                None,
            )
            .await;
        },
    ))
    .await;
    drop(expired_container);
    expired_proxy.abort();
}

#[test]
#[ignore = "requires Docker and the pinned SQL Server image"]
fn direct_live() {
    eprintln!(
        "direct_live_inner future: {} bytes",
        std::mem::size_of_val(&direct_live_inner())
    );
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            run_live_scenario(
                &runtime,
                DIRECT_LIVE_SCENARIO_DEADLINE,
                direct_live_inner(),
                "direct live scenario",
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

// One disposable server owns cross-feature protocol, migration, cancellation,
// and telemetry acceptance scenarios; splitting would duplicate its lifecycle
// and obscure the required ordering between scenarios.
#[allow(
    clippy::too_many_lines,
    reason = "single-server end-to-end scenario ordering"
)]
async fn direct_live_inner() {
    let mut random = [0; 24];
    getrandom::fill(&mut random).unwrap();
    let password = format!(
        "T!a{}",
        random.iter().fold(String::new(), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").unwrap();
            output
        })
    );
    let container = GenericImage::new(
        "mcr.microsoft.com/mssql/server",
        "2022-latest@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f",
    )
    .with_wait_for(WaitFor::log(LogWaitStrategy::stdout_or_stderr(
        "SQL Server is now ready for client connections",
    )))
    .with_env_var("ACCEPT_EULA", "Y")
    .with_env_var("MSSQL_SA_PASSWORD", &password)
    .with_env_var("MSSQL_PID", "Developer")
    .with_host_config_modifier(|host| {
        host.publish_all_ports = Some(false);
        host.port_bindings = Some(std::collections::HashMap::default());
    })
    .with_startup_timeout(DIRECT_LIVE_STARTUP_DEADLINE)
    .start()
    .await
    .expect("start disposable server");
    let ip = container.get_bridge_ip_address().await.unwrap();
    let (port, proxy_task, _proxy_stats) = proxy(SocketAddr::new(ip, 1433)).await;
    let source = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    // Install only after container startup: container tooling itself may log
    // its environment. No driver operation has run before this point.
    capture::install();
    let mut rejected_auth = context("AUTH_PASSWORD_SENTINEL");
    rejected_auth.user_name = "AUTH_NAME_SENTINEL".into();
    assert!(matches!(
        Probe::connect_insecure_test(rejected_auth, source).await,
        Err(Error::Connect)
    ));
    assert!(matches!(
        Probe::connect(context(&password), source).await,
        Err(Error::Connect)
    ));
    let mut probe = connect(source, &password).await;
    let mut observer = connect(source, &password).await;
    let pool = ExclusivePool::new(context(&password), source, 1, Duration::from_millis(500))
        .unwrap()
        .insecure_test();
    let mut lease = pool.acquire().await.unwrap();
    assert_eq!(scalar(&lease.query("SELECT 7", vec![]).await.unwrap()), 7);
    let lease_id = scalar(
        &lease
            .query("SELECT CAST(@@SPID AS int)", vec![])
            .await
            .unwrap(),
    );
    assert!(matches!(pool.acquire().await, Err(Error::AcquireTimeout)));
    drop(lease);
    for _ in 0..50 {
        let query =
            format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {lease_id}");
        if scalar(&observer.query(&query, vec![]).await.unwrap()) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let query = format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {lease_id}");
    assert_eq!(scalar(&observer.query(&query, vec![]).await.unwrap()), 0);
    let mut replacement = pool.acquire().await.unwrap();
    assert_eq!(
        scalar(&replacement.query("SELECT 8", vec![]).await.unwrap()),
        8
    );
    drop(replacement);
    let program_name = format!("direct_lock_test_{port}");
    let mut direct_context = context(&password);
    direct_context.application_name = program_name.clone();
    let direct = DirectPool::new(
        ExclusivePool::new(direct_context, source, 2, Duration::from_secs(5))
            .unwrap()
            .insecure_test(),
    );
    let lock_resource = format!("direct_lock_{port}");
    let normal_database = format!("direct_normal_{port}");
    let forced_database = format!("direct_forced_{port}");
    observer
        .query(&format!("CREATE DATABASE {normal_database}"), vec![])
        .await
        .unwrap();
    observer
        .query(&format!("CREATE DATABASE {forced_database}"), vec![])
        .await
        .unwrap();
    observer
        .query(
            &format!("ALTER DATABASE {normal_database} SET DELAYED_DURABILITY = ALLOWED"),
            vec![],
        )
        .await
        .unwrap();
    observer
        .query(
            &format!("ALTER DATABASE {forced_database} SET DELAYED_DURABILITY = FORCED"),
            vec![],
        )
        .await
        .unwrap();
    let backend = SqlServerBackend::new("dbo", "direct_history", Duration::from_secs(5)).unwrap();
    let mut normal_context = context(&password);
    normal_context.database = normal_database.clone();
    let normal_pool = DirectPool::new(
        ExclusivePool::new(normal_context, source, 1, Duration::from_secs(5))
            .unwrap()
            .insecure_test(),
    );
    backend
        .initialize_module(&normal_pool, "durability", "binding", &[])
        .await
        .unwrap();

    let mut forced_context = context(&password);
    forced_context.database = forced_database.clone();
    let forced_pool = DirectPool::new(
        ExclusivePool::new(forced_context, source, 1, Duration::from_secs(5))
            .unwrap()
            .insecure_test(),
    );
    assert!(matches!(
        SqlServerBackend::new("missing_schema", "direct_history", Duration::from_secs(5))
            .unwrap()
            .initialize_module(&forced_pool, "durability", "binding", &[])
            .await,
        Err(crate::migration::Error::ForcedDelayedDurability)
    ));
    assert_eq!(
        scalar(
            &observer
                .query(
                    &format!(
                        "SELECT COUNT(*) FROM {forced_database}.sys.tables WHERE name IN ('direct_history', 'direct_history_modules')"
                    ),
                    vec![]
                )
                .await
                .unwrap()
        ),
        0,
        "forced durability must be rejected before ledger DDL"
    );
    observer
        .query(&format!("DROP DATABASE {normal_database}"), vec![])
        .await
        .unwrap();
    observer
        .query(&format!("DROP DATABASE {forced_database}"), vec![])
        .await
        .unwrap();
    Box::pin(ledger::run(&direct, &mut observer, &mut probe)).await;
    let mut held = direct.begin_locked(&lock_resource, 0).await.unwrap();
    held.verify_server_version().await.unwrap();
    let locked_table = format!("direct_locked_{port}");
    let locked_script = format!(
        "CREATE TABLE {locked_table} ( n BIGINT )\rGO\rINSERT INTO {locked_table} VALUES (1)"
    );
    let invalid_plan = [&*locked_script, "SET NOEXEC ON"];
    assert!(
        matches!(held.execute_script_plan(&invalid_plan, QueryOptions::new()).await, Err(ref error) if error.kind() == ErrorKind::Unsupported)
    );
    let exists_before = format!("SELECT COUNT(*) FROM sys.tables WHERE name = '{locked_table}'");
    assert_eq!(
        scalar(&observer.query(&exists_before, vec![]).await.unwrap()),
        0
    );
    held.execute_script_plan(&[&locked_script], QueryOptions::new())
        .await
        .unwrap();
    assert!(
        matches!(direct.begin_locked(&lock_resource, 0).await, Err(ref e) if e.kind() == ErrorKind::Busy)
    );
    let check_sessions =
        format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name = '{program_name}'");
    let mut sessions = 2;
    for _ in 0..50 {
        sessions = scalar(&observer.query(&check_sessions, vec![]).await.unwrap());
        if sessions == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(sessions, 1);
    let waiting_pool = direct.clone();
    let waiting_resource = lock_resource.clone();
    let waiting =
        tokio::spawn(async move { waiting_pool.begin_locked(&waiting_resource, 10_000).await });
    for _ in 0..50 {
        sessions = scalar(&observer.query(&check_sessions, vec![]).await.unwrap());
        if sessions == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(sessions, 2);
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    for _ in 0..50 {
        sessions = scalar(&observer.query(&check_sessions, vec![]).await.unwrap());
        if sessions == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(sessions, 1);
    held.rollback().await.unwrap();
    let exists_locked = format!("SELECT COUNT(*) FROM sys.tables WHERE name = '{locked_table}'");
    assert_eq!(
        scalar(&observer.query(&exists_locked, vec![]).await.unwrap()),
        0
    );
    let mut committed = direct.begin_locked(&lock_resource, 0).await.unwrap();
    committed
        .execute_trusted_script_async(&locked_script, QueryOptions::new())
        .await
        .unwrap();
    committed.commit().await.unwrap();
    assert_eq!(
        scalar(
            &observer
                .query(&format!("SELECT COUNT(*) FROM {locked_table}"), vec![])
                .await
                .unwrap()
        ),
        1
    );
    observer
        .query(&format!("DROP TABLE {locked_table}"), vec![])
        .await
        .unwrap();
    drop(direct.begin_locked(&lock_resource, 0).await.unwrap());
    direct
        .begin_locked(&lock_resource, 0)
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    let backoffs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sleeper_count = backoffs.clone();
    let runtime = RetryRuntime::new(
        Arc::new(Cancellation::default()),
        || Box::pin(std::future::pending()),
        move |_| {
            sleeper_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async {})
        },
        || 0,
    );
    let retry_policy = PoolPolicy {
        retry: RetryPolicy::new(3, Delay::Fixed(Duration::ZERO), false).unwrap(),
        retry_runtime: Some(runtime),
        ..PoolPolicy::default()
    };
    let unused = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let unused_port = unused.local_addr().unwrap().port();
    drop(unused);
    let closed = DirectPool::new(
        ExclusivePool::new(
            context(&password),
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, unused_port),
            1,
            Duration::from_secs(2),
        )
        .unwrap(),
    );
    let unavailable = PolicyPool::new(closed, retry_policy.clone());
    assert!(
        unavailable
            .fetch_all_async("SELECT 1", QueryOptions::new().replay_safe())
            .await
            .is_err()
    );
    assert_eq!(backoffs.load(std::sync::atomic::Ordering::SeqCst), 2);
    backoffs.store(0, std::sync::atomic::Ordering::SeqCst);
    let observation_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_events = observation_count.clone();
    let policy = PoolPolicy {
        telemetry_enabled: true,
        observer: Some(Arc::new(move |_| {
            observed_events.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })),
        ..PoolPolicy::default()
    };
    let wrapped = PolicyPool::new(direct.clone(), policy);
    let parameter = Parameter::new(DbType::I64, Value::I64(42)).unwrap();
    let mut reader = wrapped
        .fetch_async(
            "SELECT @P1 AS n WHERE 1=0; SELECT @P1 AS n",
            QueryOptions::new()
                .bind(parameter.clone())
                .limit(RowLimit::with_max_sets(1, 9, 2).unwrap()),
        )
        .await
        .unwrap();
    let sets = reader.read_sets_async().await.unwrap();
    assert_eq!(sets.len(), 2);
    assert!(sets[0].is_empty());
    assert_eq!(sets[1][0].get::<i64>("n").unwrap(), 42);
    assert_eq!(
        observation_count.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let over_limit = direct
        .fetch_all_async(
            "SELECT @P1 AS n; SELECT @P1 AS n",
            QueryOptions::new()
                .bind(parameter.clone())
                .limit(RowLimit::with_max_sets(2, 17, 2).unwrap()),
        )
        .await;
    assert!(matches!(over_limit, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    let late_error = direct
        .fetch_all_async(
            "SELECT 1 AS n; SELECT CONVERT(int, @P1)",
            QueryOptions::new()
                .bind(Parameter::new(DbType::Text, Value::Text("invalid-number".into())).unwrap())
                .limit(RowLimit::with_max_sets(2, 100, 2).unwrap()),
        )
        .await;
    assert!(matches!(late_error, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    Box::pin(async {
    let mut session = direct.acquire_async().await.unwrap();
    let script_table = format!("direct_script_{port}");
    let invalid_table = format!("direct_invalid_{port}");
    let invalid_script = format!("CREATE TABLE {invalid_table} ( n BIGINT )\nGO\nSET NOEXEC ON");
    assert!(
        matches!(session.execute_trusted_script_async(&invalid_script, QueryOptions::new()).await, Err(ref e) if e.kind() == ErrorKind::Unsupported)
    );
    let exists = format!("SELECT COUNT(*) FROM sys.tables WHERE name = '{invalid_table}'");
    assert_eq!(scalar(&probe.query(&exists, vec![]).await.unwrap()), 0);
    let create_script = format!(
        "CREATE TABLE {script_table} ( n BIGINT )\nGO\nINSERT INTO {script_table} VALUES (1)"
    );
    assert!(matches!(session.execute_trusted_script_async(&create_script, QueryOptions::new().limit(RowLimit::new(1, 8).unwrap())).await, Err(ref e) if e.kind() == ErrorKind::InvalidArgument));
    assert!(matches!(session.execute_trusted_script_async(&create_script, QueryOptions::new().bind(Parameter::new(DbType::I64, Value::I64(3)).unwrap())).await, Err(ref e) if e.kind() == ErrorKind::InvalidArgument));
    session
        .execute_trusted_script_async(&create_script, QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(
        scalar(
            &probe
                .query(&format!("SELECT COUNT(*) FROM {script_table}"), vec![])
                .await
                .unwrap()
        ),
        1
    );
        let failing = format!(
            "INSERT INTO {script_table} VALUES (2)\nGO\nINSERT INTO {script_table} VALUES (99999999999999999999999999999999999999999999)"
        );
        assert!(matches!(session.execute_trusted_script_async(&failing, QueryOptions::new()).await, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
        assert_eq!(scalar(&probe.query(&format!("SELECT COUNT(*) FROM {script_table}"), vec![]).await.unwrap()), 2);
        drop(session);
        probe.query(&format!("DROP TABLE {script_table}"), vec![]).await.unwrap();
        let mut session = direct.acquire_async().await.unwrap();
        session.execute_trusted_script_async(&create_script, QueryOptions::new()).await.unwrap();
        let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    assert!(
        matches!(tx.execute_trusted_script_async(&failing, QueryOptions::new()).await, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown)
    );
    drop(tx);
    drop(session);
    let mut count = 2;
    for _ in 0..50 {
        count = scalar(
            &probe
                .query(&format!("SELECT COUNT(*) FROM {script_table}"), vec![])
                .await
                .unwrap(),
        );
        if count == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(count, 1);
    probe
        .query(&format!("DROP TABLE {script_table}"), vec![])
        .await
        .unwrap();
    let telemetry_table = format!("direct_telemetry_{port}");
    let mut policy_session = wrapped.acquire_async().await.unwrap();
    policy_session.execute_trusted_script_async(&format!("CREATE TABLE {telemetry_table} ( n BIGINT )"), QueryOptions::new()).await.unwrap();
    policy_session.execute_trusted_script_async(&format!("DROP TABLE {telemetry_table}"), QueryOptions::new()).await.unwrap();
    assert_eq!(observation_count.load(std::sync::atomic::Ordering::Relaxed), 3);
    capture::assert_quiet(&[&create_script]);
    }).await;
    let mut session = direct.acquire_async().await.unwrap();
    let table = format!("direct_probe_{port}");
    probe
        .query(
            &format!("CREATE TABLE {table} (n bigint PRIMARY KEY)"),
            vec![],
        )
        .await
        .unwrap();
    let insert = format!("INSERT INTO {table} VALUES (@P1)");
    let val = Parameter::new(DbType::I64, Value::I64(9123)).unwrap();
    assert_eq!(
        session
            .execute_async(&insert, QueryOptions::new().bind(val.clone()))
            .await
            .unwrap(),
        1
    );
    let mixed = format!("INSERT INTO {table} VALUES (@P1); SELECT @P2 AS data");
    let mixed_result = direct
        .fetch_all_async(
            &mixed,
            QueryOptions::new()
                .bind(Parameter::new(DbType::I64, Value::I64(9124)).unwrap())
                .bind(Parameter::new(DbType::Text, Value::Text("x".repeat(4096))).unwrap())
                .limit(RowLimit::with_max_sets(10, 100, 2).unwrap()),
        )
        .await;
    assert!(matches!(mixed_result, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9124"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    let mixed = format!("INSERT INTO {table} VALUES (@P1); SELECT GETDATE()");
    let unsupported_result = direct
        .fetch_all_async(
            &mixed,
            QueryOptions::new()
                .bind(Parameter::new(DbType::I64, Value::I64(9125)).unwrap())
                .limit(RowLimit::with_max_sets(10, 100, 2).unwrap()),
        )
        .await;
    assert!(matches!(unsupported_result, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9125"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    let update = format!("UPDATE {table} SET n = @P1 WHERE n = @P2");
    let delete = format!("DELETE FROM {table} WHERE n = @P1");
    let number = |n| Parameter::new(DbType::I64, Value::I64(n)).unwrap();
    assert_eq!(
        session
            .execute_async(
                &update,
                QueryOptions::new().bind(number(9126)).bind(number(9123))
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .execute_async(&delete, QueryOptions::new().bind(number(9126)))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .execute_async(&delete, QueryOptions::new().bind(number(9126)))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        session
            .execute_async(
                &update,
                QueryOptions::new()
                    .bind(number(741_852_963))
                    .bind(number(9130))
            )
            .await
            .unwrap(),
        0
    );
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        transaction
            .execute_async(
                &update,
                QueryOptions::new().bind(number(9127)).bind(number(9124))
            )
            .await
            .unwrap(),
        1
    );
    transaction.rollback_async().await.unwrap();
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9124"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        transaction
            .execute_async(&delete, QueryOptions::new().bind(number(9125)))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        transaction
            .execute_async(&insert, QueryOptions::new().bind(number(9129)))
            .await
            .unwrap(),
        1
    );
    transaction.commit_async().await.unwrap();
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9125"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        0
    );
    drop(session);
    let mut blocker = direct.acquire_async().await.unwrap();
    let mut locked = blocker.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        locked
            .execute_async(
                &update,
                QueryOptions::new().bind(number(9124)).bind(number(9124))
            )
            .await
            .unwrap(),
        1
    );
    for external_cancel in [false, true] {
        let mut waiting = direct.acquire_async().await.unwrap();
        let spid = waiting
            .fetch_one_async("SELECT CAST(@@SPID AS int) AS n", QueryOptions::new())
            .await
            .unwrap()
            .get::<i64>("n")
            .unwrap();
        let operation = waiting.execute_async(
            &update,
            QueryOptions::new()
                .bind(number(9130))
                .bind(number(9124))
                .timeout(Duration::from_secs(2)),
        );
        if external_cancel {
            assert!(
                tokio::time::timeout(Duration::from_millis(250), operation)
                    .await
                    .is_err()
            );
        } else {
            let result = operation.await;
            assert!(matches!(result, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
        }
        assert!(
            matches!(waiting.fetch_one_async("SELECT 1 AS n", QueryOptions::new()).await, Err(ref e) if e.kind() == ErrorKind::Connection)
        );
        let check = format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {spid}");
        let mut alive = 1;
        for _ in 0..50 {
            alive = scalar(&observer.query(&check, vec![]).await.unwrap());
            if alive == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            alive, 0,
            "interrupted write must close its physical connection"
        );
    }
    locked.rollback_async().await.unwrap();
    drop(blocker);
    let mut session = direct.acquire_async().await.unwrap();
    let duplicate = session
        .execute_async(&insert, QueryOptions::new().bind(number(9124)))
        .await;
    assert!(matches!(duplicate, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    drop(session);
    let mut nocount = connect(source, &password).await;
    nocount.fixture_batch("SET NOCOUNT ON").await.unwrap();
    assert!(matches!(
        nocount.fixture_write(&insert, vec![int(9131)]).await,
        Err(Error::WriteOutcomeUnknown)
    ));
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9131"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    let trigger = format!("direct_trigger_{port}");
    probe
        .fixture_batch(&format!(
            "CREATE TRIGGER {trigger} ON {table} AFTER INSERT AS SELECT 1 AS extra"
        ))
        .await
        .unwrap();
    let mut trigger_session = direct.acquire_async().await.unwrap();
    let trigger_result = trigger_session
        .execute_async(&insert, QueryOptions::new().bind(number(9128)))
        .await;
    assert!(matches!(trigger_result, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    let retrying = PolicyPool::new(direct.clone(), retry_policy);
    let unknown = retrying
        .execute_async(
            &insert,
            QueryOptions::new().bind(number(9132)).replay_safe(),
        )
        .await;
    assert!(matches!(unknown, Err(ref e) if e.kind() == ErrorKind::WriteOutcomeUnknown));
    assert_eq!(backoffs.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9132"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    assert_eq!(
        scalar(
            &probe
                .query(
                    &format!("SELECT COUNT(*) FROM {table} WHERE n = 9128"),
                    vec![]
                )
                .await
                .unwrap()
        ),
        1
    );
    drop(trigger_session);
    probe
        .query(&format!("DROP TABLE {table}"), vec![])
        .await
        .unwrap();
    let mut session = direct.acquire_async().await.unwrap();
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    assert_eq!(
        transaction
            .fetch_one_async("SELECT @P1 AS n", QueryOptions::new().bind(parameter))
            .await
            .unwrap()
            .get::<i64>("n")
            .unwrap(),
        42
    );
    transaction.commit_async().await.unwrap();
    let transaction = session.begin_async(TxOptions::default()).await.unwrap();
    drop(transaction);
    let after_drop = session
        .fetch_one_async("SELECT 1 AS n", QueryOptions::new())
        .await;
    assert!(matches!(after_drop, Err(ref e) if e.kind() == ErrorKind::Connection));
    let id = scalar(
        &probe
            .query("SELECT CAST(@@SPID AS int)", vec![])
            .await
            .unwrap(),
    );
    let sets = probe.query("SELECT @P1 AS n WHERE 1=0; SELECT @P1 AS n, '@P1' AS literal; SELECT @P1 AS n WHERE 1=0", vec![int(42)]).await.unwrap();
    assert_eq!(sets.len(), 3);
    assert_eq!(sets[0].len(), 0);
    assert_eq!(sets[1][0][0], ColumnValues::Int(42));
    assert!(
        matches!(&sets[1][0][1], ColumnValues::String(value) if value.to_utf8_string() == "@P1")
    );
    assert_eq!(sets[2].len(), 0);
    let params = [
        Parameter::new(DbType::Bool, Value::Bool(true)).unwrap(),
        Parameter::new(DbType::I64, Value::I64(i64::MIN)).unwrap(),
        Parameter::new(DbType::F64, Value::F64(1.25)).unwrap(),
        Parameter::new(DbType::Text, Value::Text("snow 雪".into())).unwrap(),
        Parameter::new(DbType::Bytes, Value::Bytes(vec![0, 255, 42])).unwrap(),
        Parameter::new(DbType::Bool, Value::Null(DbType::Bool)).unwrap(),
        Parameter::new(DbType::I64, Value::Null(DbType::I64)).unwrap(),
        Parameter::new(DbType::F64, Value::Null(DbType::F64)).unwrap(),
        Parameter::new(DbType::Text, Value::Null(DbType::Text)).unwrap(),
        Parameter::new(DbType::Bytes, Value::Null(DbType::Bytes)).unwrap(),
    ];
    let values = probe
        .query(
            "SELECT @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10",
            bind(&params),
        )
        .await
        .unwrap();
    let row = &values[0][0];
    assert_eq!(row[0], ColumnValues::Bit(true));
    assert_eq!(row[1], ColumnValues::BigInt(i64::MIN));
    assert_eq!(row[2], ColumnValues::Float(1.25));
    assert!(matches!(&row[3], ColumnValues::String(value) if value.to_utf8_string() == "snow 雪"));
    assert_eq!(row[4], ColumnValues::Bytes(vec![0, 255, 42]));
    assert!(row[5..].iter().all(|value| *value == ColumnValues::Null));
    let bounded = probe
        .fetch_bounded(
            "SELECT @P1 AS n WHERE 1=0; SELECT @P1 AS n; SELECT @P1 AS n WHERE 1=0",
            vec![int(42)],
            RowLimit::with_max_sets(1, 9, 3).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bounded.len(), 3);
    assert_eq!(bounded[1][0].get::<i64>("n").unwrap(), 42);
    let mut over = connect(source, &password).await;
    assert!(matches!(
        over.fetch_bounded(
            "SELECT REPLICATE(CAST('x' AS varchar(max)), 4096) AS v",
            vec![],
            RowLimit::new(1, 100).unwrap()
        )
        .await,
        Err(Error::WriteOutcomeUnknown)
    ));
    assert!(matches!(
        over.query("SELECT 1", vec![]).await,
        Err(Error::Discarded)
    ));
    let mut over = connect(source, &password).await;
    assert!(matches!(
        over.fetch_bounded(
            "SELECT 1 AS n; SELECT 2 AS n",
            vec![],
            RowLimit::with_max_sets(2, 17, 2).unwrap()
        )
        .await,
        Err(Error::WriteOutcomeUnknown)
    ));
    let mut late = connect(source, &password).await;
    assert!(matches!(
        late.fetch_bounded(
            "SELECT 1; THROW 50000, 'late_failure_sentinel', 1",
            vec![],
            RowLimit::with_max_sets(10, 100, 2).unwrap()
        )
        .await,
        Err(Error::WriteOutcomeUnknown)
    ));
    let row = probe
        .query("SELECT N'row_value_sentinel'", vec![])
        .await
        .unwrap();
    assert!(
        matches!(&row[0][0][0], ColumnValues::String(value) if value.to_utf8_string() == "row_value_sentinel")
    );

    let lock = "DECLARE @r int; EXEC @r = sys.sp_getapplock @Resource = N'direct-probe', @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = 0; SELECT @r";
    probe.begin().await.unwrap();
    observer.begin().await.unwrap();
    assert!(scalar(&probe.query(lock, vec![]).await.unwrap()) >= 0);
    assert_eq!(scalar(&observer.query(lock, vec![]).await.unwrap()), -1);
    probe.rollback().await.unwrap();
    observer.rollback().await.unwrap();

    let task = tokio::spawn(async {
        for _ in 0..10 {
            tracing::info!("unrelated_task_marker");
            tokio::task::yield_now().await;
        }
    });
    let err = probe
        .query(
            "SELECT @P1; THROW 50000, 'server_error_sentinel', 1",
            vec![int(987_654_321)],
        )
        .await;
    assert!(matches!(err, Err(Error::WriteOutcomeUnknown)));
    assert!(matches!(
        probe.query("SELECT 1", vec![]).await,
        Err(Error::Discarded)
    ));
    task.await.unwrap();
    let mut remaining = 1;
    for _ in 0..50 {
        let query = format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {id}");
        remaining = scalar(&observer.query(&query, vec![]).await.unwrap());
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(remaining, 0, "failed connection was not physically closed");

    let mut cancelled = connect(source, &password).await;
    let cancelled_id = scalar(
        &cancelled
            .query("SELECT CAST(@@SPID AS int)", vec![])
            .await
            .unwrap(),
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(300),
            cancelled.query("WAITFOR DELAY '00:00:10'; SELECT 1", vec![]),
        )
        .await
        .is_err()
    );
    assert!(matches!(
        cancelled.query("SELECT 1", vec![]).await,
        Err(Error::Discarded)
    ));
    let mut remaining = 1;
    for _ in 0..50 {
        let query =
            format!("SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {cancelled_id}");
        remaining = scalar(&observer.query(&query, vec![]).await.unwrap());
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        remaining, 0,
        "cancelled connection was not physically closed"
    );
    // Include physical closure of successful clients in the observed window.
    drop(observer);
    drop(probe);
    capture::assert_application_task_visible();
    capture::assert_quiet(&[
        "server_error_sentinel",
        "late_failure_sentinel",
        "987654321",
        "SELECT @P1",
        "WAITFOR DELAY",
        "row_value_sentinel",
        "741852963",
        &update,
        &delete,
        &lock_resource,
        &password,
        &source.to_string(),
        "127.0.0.1",
        "AUTH_PASSWORD_SENTINEL",
        "AUTH_NAME_SENTINEL",
    ]);
    proxy_task.abort();
}

const PAGE_ADMIN_SQL: &str = "SELECT TOP (@P3) tenant, seq, body FROM dbo.page_events \
    WHERE tenant > @P1 OR (tenant = @P1 AND seq > @P2) ORDER BY tenant, seq";
const PAGE_TENANT_SQL: &str = "SELECT TOP (@P4) tenant, seq, body FROM dbo.page_events \
    WHERE tenant = @P1 AND (tenant > @P2 OR (tenant = @P2 AND seq > @P3)) ORDER BY tenant, seq";

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
    pool: &DirectPool,
    sql: &'static str,
    scope: keyset_plan::Scope,
    start: (i64, i64),
) -> (Vec<PagedPage>, usize) {
    let plan = keyset_plan::KeysetPlan::new(sql, scope, start).expect("page plan");
    let requests = plan.requests();
    let source = SqlPageSource::new(
        pool.acquire_async().await.expect("page session"),
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

#[test]
#[ignore = "requires Docker and the pinned SQL Server image"]
fn direct_sql_page_source_live() {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            run_live_scenario(
                &runtime,
                DIRECT_LIVE_SCENARIO_DEADLINE,
                direct_sql_page_source_inner(),
                "direct SQL page source scenario",
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn direct_sql_page_source_inner() {
    let mut random = [0; 24];
    getrandom::fill(&mut random).unwrap();
    let password = format!(
        "T!a{}",
        random.iter().fold(String::new(), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").unwrap();
            output
        })
    );
    let container = GenericImage::new(
        "mcr.microsoft.com/mssql/server",
        "2022-latest@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f",
    )
    .with_wait_for(WaitFor::log(LogWaitStrategy::stdout_or_stderr(
        "SQL Server is now ready for client connections",
    )))
    .with_env_var("ACCEPT_EULA", "Y")
    .with_env_var("MSSQL_SA_PASSWORD", &password)
    .with_env_var("MSSQL_PID", "Developer")
    .with_host_config_modifier(|host| {
        host.publish_all_ports = Some(false);
        host.port_bindings = Some(std::collections::HashMap::default());
    })
    .with_startup_timeout(DIRECT_LIVE_STARTUP_DEADLINE)
    .start()
    .await
    .expect("start disposable server");
    let ip = container.get_bridge_ip_address().await.unwrap();
    let (port, proxy_task, _proxy_stats) = proxy(SocketAddr::new(ip, 1433)).await;
    let source = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);

    let mut observer = connect(source, &password).await;
    observer
        .query("DROP TABLE IF EXISTS dbo.page_events", vec![])
        .await
        .unwrap();
    observer
        .query(
            "CREATE TABLE dbo.page_events (tenant BIGINT NOT NULL, seq BIGINT NOT NULL, \
             body NVARCHAR(64) NOT NULL, PRIMARY KEY (tenant, seq))",
            vec![],
        )
        .await
        .unwrap();
    observer
        .query(
            "INSERT INTO dbo.page_events VALUES \
             (1, 1, N'alpha'), (1, 2, N'bravo'), (1, 3, N'charlie'), \
             (2, 1, N'delta'), (2, 2, N'echo'), (3, 1, N'foxtrot'), (3, 2, N'golf')",
            vec![],
        )
        .await
        .unwrap();

    let pool = DirectPool::new(
        ExclusivePool::new(context(&password), source, 1, Duration::from_secs(5))
            .unwrap()
            .insecure_test(),
    );

    let (admin, admin_requests) = collect_pages(
        &pool,
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
        &pool,
        PAGE_TENANT_SQL,
        keyset_plan::Scope::Tenant(2),
        (2, 0),
    )
    .await;
    assert_eq!(page_keys(&tenant), vec![(2, 1), (2, 2)]);
    assert_eq!(tenant_requests, 1);

    observer
        .query("DROP TABLE IF EXISTS dbo.page_events", vec![])
        .await
        .unwrap();
    proxy_task.abort();
}
