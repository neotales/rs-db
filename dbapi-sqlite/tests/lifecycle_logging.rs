//! What a permissive subscriber can observe from `SQLx` pool lifecycle logging.
//!
//! The provider disables `SQLx` statement logging, but `SQLx` 0.9's pool still owns
//! `ping`/`close`/hook log lines. This test forces synthetic faults and checks
//! that those scenarios emit no statement or driver-text-bearing line, that
//! driver errors do carry dangerous text to the caller, and that the provider's
//! errors never retain it.
//!
//! The capture is deliberately unrestricted (every target, every level, both
//! `tracing` and `log`). A stock `SQLx` pool with statement logging explicitly
//! enabled is the positive control: without it, an empty capture would prove
//! nothing.

use std::{fmt::Write as _, sync::Mutex, time::Duration};

use neotales_dbapi::{DbType, ErrorKind, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tracing::{
    Event as TraceEvent, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    layer::{Context, Layer},
    prelude::*,
};

/// Deliberately distinctive values so a captured event can be matched exactly.
const SQL_LITERAL: &str = "LIFECYCLE_SQL_LITERAL_SENTINEL";
const BIND_VALUE: &str = "LIFECYCLE_BIND_SENTINEL";
const ERROR_LITERAL: &str = "LIFECYCLE_ERROR_LITERAL_SENTINEL";
const TRANSACTION_VALUE: &str = "LIFECYCLE_TRANSACTION_SENTINEL";
const PATH_SENTINEL: &str = "LIFECYCLE_PATH_SENTINEL";
const REUSE_LITERAL: &str = "LIFECYCLE_REUSE_LITERAL_SENTINEL";

/// Unrestricted capture shared by the `tracing` layer and the `log` logger.
static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn captured() -> String {
    EVENTS.lock().expect("capture mutex").join("\n")
}

fn clear_capture() {
    EVENTS.lock().expect("capture mutex").clear();
}

/// `SQLx`'s statement logger runs on its connection worker thread, so an event
/// can arrive just after the caller receives the result. Poll briefly instead
/// of assuming the worker has already run.
async fn wait_for_captured(needle: &str) -> bool {
    for _ in 0..200 {
        let seen = EVENTS
            .lock()
            .expect("capture mutex")
            .iter()
            .any(|event| event.contains(needle));
        if seen {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

struct FieldText(String);

impl Visit for FieldText {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, "{}={value:?};", field.name());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let _ = write!(self.0, "{}={value};", field.name());
    }
}

/// Receives every event, at every level, from every target.
struct TraceCapture;

impl<S: Subscriber> Layer<S> for TraceCapture {
    fn on_event(&self, event: &TraceEvent<'_>, _context: Context<'_, S>) {
        let mut fields = FieldText(String::new());
        event.record(&mut fields);
        EVENTS.lock().expect("capture mutex").push(format!(
            "[tracing {} {}] {}",
            event.metadata().level(),
            event.metadata().target(),
            fields.0
        ));
    }
}

/// Receives every `log` record, for log-only crates `SQLx` might reach.
struct LogCapture;

static LOG_CAPTURE: LogCapture = LogCapture;

impl log::Log for LogCapture {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        EVENTS.lock().expect("capture mutex").push(format!(
            "[log {} {}] {}",
            record.level(),
            record.target(),
            record.args()
        ));
    }

    fn flush(&self) {}
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn text_parameter(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("valid parameter")
}

#[test]
fn pool_lifecycle_logging_cannot_carry_sql_or_sqlite_error_text() {
    // Install the broadest capture available before any SQLx call site runs so
    // the global interest cache enables everything.
    log::set_logger(&LOG_CAPTURE).expect("install log capture");
    log::set_max_level(log::LevelFilter::Trace);
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(TraceCapture))
        .expect("install tracing capture");

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            positive_control_capture_and_driver_error_text().await;
            provider_operations_are_silent().await;
            synthetic_lifecycle_faults_are_silent().await;
        });

    let events = captured();
    // `positive_control_capture_and_driver_error_text` clears the capture after
    // proving the subscriber works; only provider-phase events remain.
    for sentinel in [
        SQL_LITERAL,
        BIND_VALUE,
        ERROR_LITERAL,
        TRANSACTION_VALUE,
        PATH_SENTINEL,
        REUSE_LITERAL,
    ] {
        assert!(
            !events.contains(sentinel),
            "captured event contains {sentinel:?}:\n{events}"
        );
    }
    // No statement log and no driver-text-bearing pool line may appear in the
    // provider phase. `db.statement` is the field SQLx's statement logger fills
    // in; the control proved it is visible when statement logging is enabled.
    // Slow-acquire timing warnings are outside this surface and carry only
    // durations, so they are not asserted against.
    for forbidden in [
        "db.statement",
        "sqlx::query",
        "slow statement:",
        "syntax error",
        "unable to open database file",
    ] {
        assert!(
            !events.contains(forbidden),
            "unexpected {forbidden:?} in a provider-phase event:\n{events}"
        );
    }
}

async fn positive_control_capture_and_driver_error_text() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("control.sqlite");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .log_statements(log::LevelFilter::Trace)
        .log_slow_statements(log::LevelFilter::Trace, Duration::ZERO);
    let control = SqlitePool::connect_with(options)
        .await
        .expect("control pool");

    // 1. Statement logging at TRACE must be visible to the capture.
    sqlx::query("SELECT 'LIFECYCLE_CONTROL_LITERAL_SENTINEL' AS value")
        .fetch_one(&control)
        .await
        .expect("control query");
    assert!(
        wait_for_captured("LIFECYCLE_CONTROL_LITERAL_SENTINEL").await,
        "the capture did not see enabled statement logging:\n{}",
        captured()
    );
    assert!(
        captured().contains("sqlx::query"),
        "the capture did not see the SQLx query target:\n{}",
        captured()
    );
    // Prove the `log` leg of the capture too; SQLx statement logging itself
    // uses `tracing`, so otherwise that leg would be untested.
    log::info!("LIFECYCLE_CONTROL_LOG_SENTINEL");
    assert!(
        wait_for_captured("LIFECYCLE_CONTROL_LOG_SENTINEL").await,
        "the capture did not see log records:\n{}",
        captured()
    );

    // 2. A failing statement's driver error carries the offending literal, and
    //    the enabled statement logger emits the statement. This is the text the
    //    provider must not forward to any log.
    let error = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 NOT '{ERROR_LITERAL}'"
    )))
    .execute(&control)
    .await
    .expect_err("syntax error");
    let driver_text = format!("{error}\n{error:?}");
    assert!(
        driver_text.contains(ERROR_LITERAL),
        "SQLite did not echo the literal into its error: {driver_text}"
    );
    assert!(
        driver_text.contains("syntax error"),
        "unexpected driver error shape: {driver_text}"
    );
    assert!(
        wait_for_captured(ERROR_LITERAL).await,
        "the capture did not see the failing statement:\n{}",
        captured()
    );

    // 3. A failed open does not embed the database file name in SQLite's text.
    let missing = directory.path().join(PATH_SENTINEL).join("missing.sqlite");
    let open_error = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&missing)
                .create_if_missing(true),
        )
        .await
        .expect_err("open must fail");
    let open_text = format!("{open_error}\n{open_error:?}");
    assert!(
        open_text.contains("unable to open database file"),
        "unexpected open error shape: {open_text}"
    );
    assert!(
        !open_text.contains(PATH_SENTINEL),
        "SQLite embedded the file name in its error: {open_text}"
    );

    control.close().await;
    clear_capture();
}

async fn provider_operations_are_silent() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join(format!("{PATH_SENTINEL}.sqlite"));
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(1),
    )
    .await
    .expect("provider");

    let mut session = provider.acquire_async().await.expect("session");
    let parameters = [text_parameter(BIND_VALUE)];
    let rows = session
        .fetch_all_async(
            &format!("SELECT ?1 AS bound, '{SQL_LITERAL}' AS literal"),
            QueryOptions::new()
                .bind_all(&parameters)
                .limit(limit(1, 256)),
        )
        .await
        .expect("successful fetch");
    assert_eq!(
        rows[0].get::<String>("bound").expect("bound column"),
        BIND_VALUE
    );
    assert_eq!(
        rows[0].get::<String>("literal").expect("literal column"),
        SQL_LITERAL
    );

    // A dispatched failure invalidates the session, so dropping it exercises the
    // close-on-drop lifecycle path (`Floating::close`). The driver error carries
    // the literal; the provider's error must not.
    let error = session
        .fetch_all_async(
            &format!("SELECT 1 NOT '{ERROR_LITERAL}'"),
            QueryOptions::new().limit(limit(1, 256)),
        )
        .await
        .expect_err("syntax error");
    // Fetch can execute `INSERT ... RETURNING`, so a dispatched failure is
    // conservatively reported as an unknown write outcome.
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    let rendered = format!("{error} {error:?}");
    for sentinel in [ERROR_LITERAL, "syntax error", SQL_LITERAL] {
        assert!(
            !rendered.contains(sentinel),
            "provider error leaked {sentinel:?}: {rendered}"
        );
    }
    drop(session);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Acquiring again exercises the pool's `test_before_acquire` ping and the
    // release ping; both are silent on success.
    let _rows = provider
        .fetch_all_async(
            &format!("SELECT '{REUSE_LITERAL}' AS value"),
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .expect("reused connection");
    drop(provider);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

async fn synthetic_lifecycle_faults_are_silent() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory
        .path()
        .join(format!("{PATH_SENTINEL}-faults.sqlite"));
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(1),
    )
    .await
    .expect("provider");

    let mut setup = provider.acquire_async().await.expect("setup session");
    setup
        .execute_async(
            "CREATE TABLE lifecycle_rows (value TEXT UNIQUE)",
            QueryOptions::new(),
        )
        .await
        .expect("create table");
    drop(setup);

    // Open transaction, write a sentinel, then cancel a long-running fetch. The
    // transaction and connection are discarded on drop, exercising rollback
    // handling, invalidation and close.
    let mut session = provider.acquire_sqlite().await.expect("session");
    let mut transaction = session.begin_immediate_async().await.expect("begin");
    transaction
        .execute_async(
            "INSERT INTO lifecycle_rows(value) VALUES(?1)",
            QueryOptions::new().bind(text_parameter(TRANSACTION_VALUE)),
        )
        .await
        .expect("insert in transaction");
    let cancelled = tokio::time::timeout(
        Duration::from_millis(10),
        transaction.fetch_all_async(
            "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM counter WHERE x < 100000000) SELECT sum(x) AS total FROM counter",
            QueryOptions::new().limit(limit(1, 64)),
        ),
    )
    .await;
    assert!(cancelled.is_err(), "long fetch should be interrupted");
    drop(transaction);
    drop(session);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Reacquire to run the pool's ping path against fresh state.
    let mut session = provider.acquire_async().await.expect("session");
    let rows = session
        .fetch_all_async(
            "SELECT count(*) AS total FROM lifecycle_rows",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .expect("count rows");
    assert_eq!(rows[0].get::<i64>("total").expect("total"), 0);
    drop(session);

    // A failed open with the sentinel in a directory name must not log the path.
    let missing = directory
        .path()
        .join(format!("{PATH_SENTINEL}-missing"))
        .join("database.sqlite");
    let error = SqlitePoolProvider::connect(SqliteConfig::new(&missing))
        .await
        .expect_err("connect must fail");
    assert_eq!(error.kind(), ErrorKind::Connection);
    assert!(!format!("{error} {error:?}").contains(PATH_SENTINEL));

    drop(provider);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
