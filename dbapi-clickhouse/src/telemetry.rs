use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

use neotales_dbapi::{
    DbEvent, DbObserver, ErrorKind, Operation, QueryOptions, QuerySetting, SqlCaptureHook,
    TelemetryOutcome,
};

/// Observation policy for concrete `execute_no_count` calls.
///
/// Both switches default to false. SQL may contain secrets: enable capture only
/// with an appropriate redaction hook and a trusted observer. Bound parameter values,
/// rows, driver messages, and connection details are not passed separately. Captured
/// SQL literals or arbitrary hook output may nevertheless contain secrets.
#[derive(Clone, Default)]
pub struct ClickHouseTelemetry {
    /// Default for queries whose telemetry setting is inherited.
    pub telemetry_enabled: bool,
    /// Default for queries whose SQL-capture setting is inherited.
    pub sql_capture_enabled: bool,
    /// Pool-level SQL hook, replaced by a per-query hook if supplied.
    pub sql_hook: Option<SqlCaptureHook>,
    /// Receiver for sanitized events, suitable for tracing or metrics.
    pub observer: Option<DbObserver>,
}

impl std::fmt::Debug for ClickHouseTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTelemetry")
            .field("telemetry_enabled", &self.telemetry_enabled)
            .field("sql_capture_enabled", &self.sql_capture_enabled)
            .field("has_sql_hook", &self.sql_hook.is_some())
            .field("has_observer", &self.observer.is_some())
            .finish()
    }
}

fn enabled(setting: QuerySetting, default: bool) -> bool {
    match setting {
        QuerySetting::Inherit => default,
        QuerySetting::On => true,
        QuerySetting::Off => false,
    }
}

fn contain<T>(f: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => Some(value),
        // The payload's destructor may itself panic.
        Err(payload) => {
            std::mem::forget(payload);
            None
        }
    }
}

/// Emits an unknown outcome on future cancellation instead of premature success.
pub(crate) struct Observation {
    observer: Option<DbObserver>,
    sql: Option<String>,
    started: Instant,
    outcome: TelemetryOutcome,
}

impl Observation {
    pub(crate) fn new(policy: &ClickHouseTelemetry, sql: &str, options: &QueryOptions) -> Self {
        let started = Instant::now();
        let observer = if enabled(options.telemetry, policy.telemetry_enabled) {
            policy.observer.clone()
        } else {
            None
        };
        let sql = if observer.is_some() && enabled(options.sql_capture, policy.sql_capture_enabled)
        {
            match options.sql_hook.as_ref().or(policy.sql_hook.as_ref()) {
                Some(hook) => contain(|| hook(sql)).flatten(),
                None => Some(sql.to_owned()),
            }
        } else {
            None
        };
        Self {
            observer,
            sql,
            started,
            outcome: TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown),
        }
    }

    pub(crate) fn complete(&mut self, outcome: TelemetryOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            let event = DbEvent {
                operation: Operation::Execute,
                duration: self.started.elapsed(),
                outcome: self.outcome,
                sql: self.sql.take(),
            };
            let _ = contain(|| observer(&event));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn inherits_overrides_redacts_and_reports_drop_as_unknown() {
        let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
        let sink = Arc::clone(&events);
        let policy = ClickHouseTelemetry {
            telemetry_enabled: true,
            observer: Some(Arc::new(move |event| {
                sink.lock().unwrap().push(event.clone());
            })),
            ..ClickHouseTelemetry::default()
        };
        {
            let mut event = Observation::new(&policy, "INSERT secret", &QueryOptions::new());
            event.complete(TelemetryOutcome::Success);
        }
        {
            let options = QueryOptions::new()
                .sql_capture(QuerySetting::On)
                .sql_hook(Arc::new(|_| Some("redacted".to_owned())));
            let _cancelled = Observation::new(&policy, "INSERT secret", &options);
        }
        {
            let options = QueryOptions::new().telemetry(QuerySetting::Off);
            let _disabled = Observation::new(&policy, "INSERT secret", &options);
        }
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].operation, Operation::Execute);
        assert_eq!(events[0].outcome, TelemetryOutcome::Success);
        assert_eq!(events[0].sql, None);
        assert_eq!(events[1].sql.as_deref(), Some("redacted"));
        assert_eq!(
            events[1].outcome,
            TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
        );
    }

    #[test]
    fn callback_panics_are_contained_and_capture_is_explicit() {
        let policy = ClickHouseTelemetry {
            telemetry_enabled: false,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(|_| panic!("hook panic"))),
            observer: Some(Arc::new(|_| panic!("observer panic"))),
        };
        let mut observation = Observation::new(
            &policy,
            "secret",
            &QueryOptions::new().telemetry(QuerySetting::On),
        );
        assert_eq!(observation.sql, None);
        observation.complete(TelemetryOutcome::Error(ErrorKind::InvalidArgument));
        drop(observation);
    }

    #[test]
    fn query_can_enable_observation_and_override_pool_hook() {
        let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
        let sink = Arc::clone(&events);
        let policy = ClickHouseTelemetry {
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(|_| Some("pool".to_owned()))),
            observer: Some(Arc::new(move |event| {
                sink.lock().unwrap().push(event.clone());
            })),
            ..ClickHouseTelemetry::default()
        };
        drop(Observation::new(&policy, "secret", &QueryOptions::new()));
        let options = QueryOptions::new()
            .telemetry(QuerySetting::On)
            .sql_hook(Arc::new(|_| Some("query".to_owned())));
        drop(Observation::new(&policy, "secret", &options));
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sql.as_deref(), Some("query"));
    }

    #[test]
    fn duration_includes_capture_hook() {
        let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
        let sink = Arc::clone(&events);
        let delay = std::time::Duration::from_millis(10);
        let policy = ClickHouseTelemetry {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            sql_hook: Some(Arc::new(move |_| {
                std::thread::sleep(delay);
                None
            })),
            observer: Some(Arc::new(move |event| {
                sink.lock().unwrap().push(event.clone());
            })),
        };
        drop(Observation::new(&policy, "secret", &QueryOptions::new()));
        assert!(events.lock().unwrap()[0].duration >= delay);
    }
}
