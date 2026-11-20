//! Trace-privacy test: a permissive subscriber and logger see nothing from this provider.
//!
//! The crate has no `tracing` or `log` dependency, rusqlite's `trace` feature is off, and every
//! error crossing the provider boundary is rebuilt from a stable category. Distinctive SQL, bind
//! values, file names, and error text must never reach a captured record. A positive control proves
//! the capture itself works before the provider runs.

use std::{fmt::Write as _, sync::Mutex, time::Duration};

use neotales_dbapi::{
    DbType, ErrorKind, Parameter, QueryOptions, RowLimit, Value, sync::Session as SyncSession,
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};
use tracing::{
    Event as TraceEvent, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    layer::{Context, Layer},
    prelude::*,
};

const SQL_SENTINEL: &str = "PROVIDER_SQL_SENTINEL";
const BIND_SENTINEL: &str = "PROVIDER_BIND_SENTINEL";
const PATH_SENTINEL: &str = "PROVIDER_PATH_SENTINEL";
const CONTROL_SENTINEL: &str = "PROVIDER_CAPTURE_CONTROL";

static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn captured() -> String {
    EVENTS.lock().expect("capture mutex").join("\n")
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

struct LogCapture;

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

#[test]
fn a_permissive_capture_sees_no_sql_values_paths_or_errors() {
    let subscriber = tracing_subscriber::registry().with(TraceCapture);
    tracing::subscriber::set_global_default(subscriber).expect("global tracing subscriber");
    log::set_logger(&LogCapture).expect("global logger");
    log::set_max_level(log::LevelFilter::Trace);

    // Positive control: both capture paths work.
    tracing::warn!(value = CONTROL_SENTINEL, "tracing control");
    log::warn!("log control {CONTROL_SENTINEL}");
    let control = captured();
    assert!(control.contains(CONTROL_SENTINEL), "{control}");
    EVENTS.lock().expect("capture mutex").clear();

    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join(format!("{PATH_SENTINEL}.sqlite"));
    let provider = SqlitePoolProvider::connect_sync(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(10))
            .max_connections(1),
    )
    .expect("provider");
    let mut session = provider.acquire_sync_sqlite().expect("session");
    SyncSession::execute(
        &mut session,
        &format!("CREATE TABLE probe (value TEXT /* {SQL_SENTINEL} */)"),
        QueryOptions::new(),
    )
    .unwrap();
    SyncSession::execute(
        &mut session,
        "INSERT INTO probe VALUES (?)",
        QueryOptions::new()
            .bind(Parameter::new(DbType::Text, Value::Text(BIND_SENTINEL.to_owned())).unwrap()),
    )
    .unwrap();
    let row = SyncSession::fetch_one(
        &mut session,
        "SELECT value FROM probe",
        QueryOptions::new().limit(limit(1, 128)),
    )
    .unwrap();
    assert_eq!(row.get::<String>("value").unwrap(), BIND_SENTINEL);

    // A dispatched error carries driver text to the worker but never to the caller or a log.
    let error = SyncSession::execute(
        &mut session,
        "INSERT INTO probe VALUES (NULL); SELECT 1",
        QueryOptions::new(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = SyncSession::execute(
        &mut session,
        "INSERT INTO missing_table VALUES (1)",
        QueryOptions::new(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert!(!format!("{error:?}").contains(PATH_SENTINEL));

    drop(session);
    let mut session = provider.acquire_sync_sqlite().expect("replacement session");
    let mut transaction = session.begin_immediate().unwrap();
    transaction
        .execute("INSERT INTO probe VALUES ('tx')", QueryOptions::new())
        .unwrap();
    transaction.rollback().unwrap();
    drop(session);
    provider.close();

    std::thread::sleep(Duration::from_millis(50));
    let captured = captured();
    for sentinel in [SQL_SENTINEL, BIND_SENTINEL, PATH_SENTINEL] {
        assert!(
            !captured.contains(sentinel),
            "the provider leaked {sentinel} into a captured record: {captured}"
        );
    }
}
