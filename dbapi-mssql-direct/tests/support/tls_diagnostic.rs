//! Fail-closed diagnostics: never forward child panic payloads or log bodies.
pub(super) const CASES: &[&str] = &[
    "valid",
    "wrong-ca",
    "wrong-host",
    "pin-valid",
    "pin-invalid",
    "commit-lost",
    "insecure-reachability",
    "expired",
    "expired-ca",
];
const PHASES: &[&str] = &[
    "configure",
    "connect-query",
    "migration-start",
    "commit-held",
    "observer-connect",
    "observer-query",
    "commit-release",
    "migration-result",
    "reconciliation",
    "verify-application",
    "verify-ledger",
    "identity-rejection",
    "complete",
    "pool-wait",
    "reconnect",
    "connected",
    "begin",
    "begun",
    "lock-request",
    "query",
    "query-complete",
    "ledger-ddl",
    "ledger-ddl-complete",
    "commit",
    "commit-complete",
    "lock-granted",
    "lock-timeout",
    "lock-cancelled",
    "lock-deadlock",
    "lock-error",
    "lock-unknown",
    "ledger-ddl-failed",
    "commit-failed",
];
const DRIVER_CATEGORIES: &[&str] = &[
    "TlsHandshake",
    "CertificateExpired",
    "CertificateMismatch",
    "Transport",
    "Protocol",
    "SqlServerLogin",
    "Io",
    "ConnectTimeout",
    "OtherTimeout",
    "ConnectionClosed",
    "TlsLibrary",
    "Security",
    "Redirection",
    "Usage",
    "Implementation",
    "Other",
];
const CATEGORIES: &[&str] = &[
    "panic",
    "deadline",
    "connect",
    "query",
    "commit-hold",
    "migration",
    "reconciliation",
    "identity",
];

pub(super) fn phase(phase: &'static str) {
    assert!(PHASES.contains(&phase));
    eprintln!("TLS_PHASE={phase}");
}

pub(super) fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = info
            .payload()
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| info.payload().downcast_ref::<&str>().copied())
            .unwrap_or("");
        // Inspect only to select a fixed category; never render the payload.
        let category = if message.contains("exceeded") || message.contains("deadline") {
            "deadline"
        } else if message.contains("COMMIT response") {
            "commit-hold"
        } else if message.contains("reconciliation") {
            "reconciliation"
        } else if message.contains("migration") {
            "migration"
        } else if message.contains("identity") {
            "identity"
        } else if message.contains("connection") || message.contains("reachability") {
            "connect"
        } else if message.contains("query") {
            "query"
        } else {
            "panic"
        };
        eprintln!("TLS_FAILURE={category}");
    }));
}

pub(super) fn summarize(bytes: &[u8]) -> String {
    let decoded = String::from_utf8_lossy(bytes);
    let mut records = decoded
        .lines()
        .rev()
        .filter(|line| {
            line.strip_prefix("TLS_PHASE=")
                .is_some_and(|v| PHASES.contains(&v))
                || line
                    .strip_prefix("TLS_FAILURE=")
                    .is_some_and(|v| CATEGORIES.contains(&v))
                || line
                    .strip_prefix("TLS_CASE_COMPLETE=")
                    .is_some_and(|v| CASES.contains(&v))
                || line
                    .strip_prefix("TLS_DRIVER=")
                    .is_some_and(|v| DRIVER_CATEGORIES.contains(&v))
        })
        .take(64)
        .collect::<Vec<_>>();
    records.reverse();
    records.join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Private fail-closed parser branching is not reachable through a public API.
    #[test]
    fn whitelist_rejects_payloads_unknown_values_and_partial_markers() {
        let bytes = b"raw SQL password URL\nTLS_PHASE=observer-query\nTLS_FAILURE=query\nTLS_PHASE=observer-query secret\nprefix TLS_FAILURE=query\nTLS_FAILURE=unknown\nTLS_CASE_COMPLETE=commit-lost\n";
        assert_eq!(
            summarize(bytes),
            "TLS_PHASE=observer-query;TLS_FAILURE=query;TLS_CASE_COMPLETE=commit-lost"
        );
        assert_eq!(summarize(b"TLS_PHASE=\xff\nTLS_FAILURE=query\rsecret"), "");
    }

    #[test]
    // Private fail-closed parser branching is not reachable through a public API.
    fn whitelist_accepts_every_fixed_value_and_bounds_output() {
        for (prefix, values) in [
            ("TLS_PHASE=", PHASES),
            ("TLS_FAILURE=", CATEGORIES),
            ("TLS_CASE_COMPLETE=", CASES),
            ("TLS_DRIVER=", DRIVER_CATEGORIES),
        ] {
            for value in values {
                let line = format!("{prefix}{value}");
                assert_eq!(summarize(line.as_bytes()), line);
            }
        }
        assert_eq!(
            summarize("TLS_FAILURE=panic\n".repeat(100).as_bytes())
                .split(';')
                .count(),
            64
        );
        let output = summarize(
            format!("{}TLS_FAILURE=deadline\n", "TLS_PHASE=query\n".repeat(100)).as_bytes(),
        );
        assert!(output.ends_with("TLS_FAILURE=deadline"));
    }

    // The hook is process-global; an isolated process tests it without changing
    // any other test's panic reporting.
    #[tokio::test]
    async fn panic_hook_discards_payload_in_subprocess() {
        const CHILD: &str = "DIRECT_TLS_DIAGNOSTIC_PANIC_TEST";
        if std::env::var_os(CHILD).is_some() {
            install_panic_hook();
            let result = std::panic::catch_unwind(|| {
                panic!(
                    "observer connection phase failed: SQL_SENTINEL PASSWORD_SENTINEL https://sentinel.invalid /private/sentinel"
                );
            });
            assert!(result.is_err());
            return;
        }
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "live::tls_diagnostic::tests::panic_hook_discards_payload_in_subprocess",
                "--nocapture",
            ])
            .env(CHILD, "1");
        let output = super::super::run_child(command, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        assert!(output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr.trim(), "TLS_FAILURE=connect");
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            for forbidden in ["SQL_SENTINEL", "PASSWORD_SENTINEL", "https://", "/private/"] {
                assert!(!text.contains(forbidden), "panic hook exposed payload");
            }
        }
    }
}
