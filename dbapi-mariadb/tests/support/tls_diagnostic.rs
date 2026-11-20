//! Only fixed stage/category labels and elapsed milliseconds leave this helper.
use std::{
    cell::Cell,
    panic::{PanicHookInfo, take_hook},
    sync::Arc,
    time::Instant,
};

use neotales_dbapi::{DbError, ErrorKind};

#[derive(Clone, Copy, Debug)]
pub(super) enum Stage {
    Certificates,
    Startup,
    PortIsolation,
    Proxy,
    AccountReady,
    TrustedCaAndHost,
    TrustedQuery,
    WrongHost,
    NoCa,
    Insecure,
    InsecureQuery,
    Complete,
}

thread_local! {
    static CURRENT: Cell<Option<(Stage, Instant)>> = const { Cell::new(None) };
}

type Hook = Arc<dyn Fn(&PanicHookInfo<'_>) + Send + Sync>;

/// Scope the hook to the TLS test's current-thread runtime. Other test threads
/// keep their original reporting; no panic payload is inspected or rendered here.
pub(super) struct Diagnostic {
    previous: Hook,
}

impl Diagnostic {
    pub(super) fn install() -> Self {
        let previous: Hook = Arc::from(take_hook());
        let fallback = Arc::clone(&previous);
        std::panic::set_hook(Box::new(move |info| {
            if CURRENT.with(|current| current.get().is_some()) {
                emit("panic");
            } else {
                fallback(info);
            }
        }));
        stage(Stage::Certificates);
        Self { previous }
    }
}

impl Drop for Diagnostic {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(None));
        // Rust forbids changing the hook while unwinding. The installed hook
        // already delegates non-TLS panics and retains no fixture data.
        if !std::thread::panicking() {
            let previous = Arc::clone(&self.previous);
            std::panic::set_hook(Box::new(move |info| previous(info)));
        }
    }
}

fn emit(category: &'static str) {
    CURRENT.with(|current| {
        if let Some((stage, started)) = current.get() {
            eprintln!(
                "MARIADB_TLS stage={stage:?} category={category} elapsed_ms={}",
                started.elapsed().as_millis()
            );
        }
    });
}

pub(super) fn stage(stage: Stage) {
    CURRENT.with(|current| current.set(Some((stage, Instant::now()))));
    emit("begin");
}

pub(super) fn success() {
    emit("ok");
}

// Explicit allowlist rather than formatting any driver error or future payload.
fn category(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Connection => "connection",
        ErrorKind::Timeout => "timeout",
        ErrorKind::InvalidArgument => "invalid-argument",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::Decode => "decode",
        _ => "other-db-error",
    }
}

pub(super) fn observed_error(kind: ErrorKind) {
    emit(category(kind));
}

pub(super) fn required<T>(result: Result<T, DbError>) -> T {
    match result {
        Ok(value) => {
            success();
            value
        }
        Err(error) => {
            observed_error(error.kind());
            panic!("required TLS operation failed");
        }
    }
}

pub(super) fn rejected<T>(result: Result<T, DbError>) {
    emit("expected-connection");
    match result {
        Err(error) => {
            observed_error(error.kind());
            assert_eq!(error.kind(), ErrorKind::Connection);
        }
        Ok(value) => {
            drop(value);
            emit("unexpected-success");
            panic!("negative TLS case connected");
        }
    }
    success();
}

#[cfg(test)]
mod tests {
    use super::*;

    // Private classification branching cannot be reached via a public API.
    #[test]
    fn categories_are_fixed_and_do_not_format_errors() {
        assert_eq!(category(ErrorKind::Connection), "connection");
        assert_eq!(category(ErrorKind::Timeout), "timeout");
        assert_eq!(category(ErrorKind::InvalidArgument), "invalid-argument");
        assert_eq!(category(ErrorKind::Unsupported), "unsupported");
        assert_eq!(category(ErrorKind::Decode), "decode");
        assert_eq!(category(ErrorKind::Constraint), "other-db-error");
    }

    // The hook is process-global; isolate the payload-leak regression from the
    // integration runner's other threads and hooks.
    #[test]
    fn panic_payload_is_discarded_in_subprocess() {
        const CHILD: &str = "MARIADB_TLS_DIAGNOSTIC_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let _diagnostic = Diagnostic::install();
            stage(Stage::Startup);
            let result = std::panic::catch_unwind(|| {
                panic!("SQL_SENTINEL PASSWORD_SENTINEL CERT_SENTINEL /private/sentinel");
            });
            assert!(result.is_err());
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tls_diagnostic::tests::panic_payload_is_discarded_in_subprocess",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        let lines = stderr.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        for (line, prefix) in lines.iter().zip([
            "MARIADB_TLS stage=Certificates category=begin elapsed_ms=",
            "MARIADB_TLS stage=Startup category=begin elapsed_ms=",
            "MARIADB_TLS stage=Startup category=panic elapsed_ms=",
        ]) {
            assert!(line.strip_prefix(prefix).unwrap().parse::<u128>().is_ok());
        }
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            for forbidden in [
                "SQL_SENTINEL",
                "PASSWORD_SENTINEL",
                "CERT_SENTINEL",
                "/private/sentinel",
            ] {
                assert!(!text.contains(forbidden), "panic hook exposed payload");
            }
        }
    }
}
