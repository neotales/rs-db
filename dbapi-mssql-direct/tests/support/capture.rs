//! Hostile process-global capture of both independent channels.
use std::{io, sync::Mutex};
use tracing_subscriber::fmt::format::FmtSpan;

static LOGS: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static TRACES: Mutex<Vec<u8>> = Mutex::new(Vec::new());
struct Logger;
static LOGGER: Logger = Logger;

impl log::Log for Logger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        use io::Write;
        writeln!(
            LOGS.lock().unwrap(),
            "{} {}",
            record.target(),
            record.args()
        )
        .unwrap();
    }
    fn flush(&self) {}
}

#[derive(Clone)]
struct Writer;
impl io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        TRACES.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn install() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_span_events(FmtSpan::FULL)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(|| Writer)
            .finish(),
    )
    .unwrap();
    positive_controls();
}

pub(crate) fn positive_controls() {
    log::trace!("APPLICATION_LOG_CONTROL");
    tracing::trace!("APPLICATION_TRACE_CONTROL");
    let span = tracing::trace_span!("APPLICATION_SPAN_CONTROL", value = "SPAN_FIELD_CONTROL");
    let _entered = span.enter();
}

pub(crate) fn assert_quiet(secrets: &[&str]) {
    positive_controls();
    let logs = String::from_utf8(LOGS.lock().unwrap().clone()).unwrap();
    let traces = String::from_utf8(TRACES.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("APPLICATION_LOG_CONTROL"));
    // Proves feature-unified log-always is active even with a real subscriber.
    assert!(logs.contains("APPLICATION_TRACE_CONTROL"));
    assert!(logs.contains("APPLICATION_SPAN_CONTROL"));
    assert!(logs.contains("SPAN_FIELD_CONTROL"));
    assert!(traces.contains("APPLICATION_TRACE_CONTROL"));
    assert!(traces.contains("APPLICATION_SPAN_CONTROL"));
    assert!(traces.contains("SPAN_FIELD_CONTROL"));
    for captured in [&logs, &traces] {
        for secret in secrets {
            assert!(
                !captured.contains(secret),
                "sensitive value reached capture"
            );
        }
        for driver_marker in ["mssql_tds", "Packet content:", "native_tls"] {
            assert!(
                !captured.contains(driver_marker),
                "driver emitted a diagnostic"
            );
        }
    }
}

pub(crate) fn assert_application_task_visible() {
    for bytes in [&LOGS, &TRACES] {
        assert!(
            String::from_utf8(bytes.lock().unwrap().clone())
                .unwrap()
                .contains("unrelated_task_marker")
        );
    }
}
