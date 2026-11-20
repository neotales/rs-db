//! Public identifier, timeout, and lock-key contracts (no server required).
use neotales_dbapi::{Capabilities, DbError, DbFuture, ErrorKind, Pool, Session};
use neotales_migration_postgres::{Error, PostgresBackend};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn identifiers_and_derived_names_cannot_truncate_or_inject() {
    let timeout = Duration::from_secs(1);
    assert!(PostgresBackend::new("a".repeat(63), "a".repeat(55), timeout).is_ok());
    for schema in [
        "",
        "a.b",
        "public;drop",
        "A",
        "9schema",
        "é",
        "pg_temp",
        "pg_catalog",
    ] {
        assert!(matches!(
            PostgresBackend::new(schema, "history", timeout),
            Err(Error::Configuration)
        ));
    }
    for table in ["", "a.b", "a\"b", "A", "9table", "é"] {
        assert!(matches!(
            PostgresBackend::new("app", table, timeout),
            Err(Error::Configuration)
        ));
    }
    assert!(matches!(
        PostgresBackend::new("a".repeat(64), "history", timeout),
        Err(Error::Configuration)
    ));
    assert!(matches!(
        PostgresBackend::new("app", "a".repeat(56), timeout),
        Err(Error::Configuration)
    ));
}

#[test]
fn advisory_timeout_is_positive_bounded_integral_milliseconds() {
    for timeout in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_micros(1500),
        Duration::from_millis(i32::MAX as u64 + 1),
    ] {
        assert!(matches!(
            PostgresBackend::new("app", "history", timeout),
            Err(Error::Configuration)
        ));
    }
    assert!(PostgresBackend::new("app", "history", Duration::from_millis(i32::MAX as u64)).is_ok());
}

#[test]
fn keys_are_schema_scoped_and_stable_across_backend_instances() {
    let timeout = Duration::from_secs(1);
    let a = PostgresBackend::new("app", "history", timeout).unwrap();
    let b = PostgresBackend::new("app", "other_history", timeout).unwrap();
    let other = PostgresBackend::new("other", "history", timeout).unwrap();
    assert_eq!(a.advisory_lock_key(), b.advisory_lock_key());
    assert_ne!(a.advisory_lock_key(), other.advisory_lock_key());
    // Independent protocol vector is pinned below, not derived by the test.
    assert_eq!(a.advisory_lock_key(), 165_418_963_927_914_327);
}

#[derive(Debug, Default)]
struct RefusingPool(AtomicUsize);

impl Pool for RefusingPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(DbError::new(ErrorKind::Connection)) })
    }
}

#[tokio::test]
async fn identities_and_managed_identifiers_validate_before_database_acquisition() {
    let pool = RefusingPool::default();
    let backend = PostgresBackend::new("app", "history", Duration::from_secs(1)).unwrap();
    for value in [
        String::new(),
        "a".repeat(256),
        "é".repeat(128),
        "a\0b".to_owned(),
    ] {
        assert!(matches!(
            backend.initialize_module(&pool, &value, "fp", &[]).await,
            Err(Error::Configuration)
        ));
        assert!(matches!(
            backend.initialize_module(&pool, "app", &value, &[]).await,
            Err(Error::Configuration)
        ));
    }
    for objects in [
        [("app.extra", "kept")],
        [("app", "kept;drop")],
        [("app", "")],
        [("", "kept")],
        [("pg_temp", "kept")],
    ] {
        assert!(matches!(
            backend
                .initialize_module(&pool, "app", "fp", &objects)
                .await,
            Err(Error::Configuration)
        ));
    }
    assert_eq!(pool.0.load(Ordering::SeqCst), 0);
    let result = backend
        .initialize_module(&pool, &"a".repeat(255), &"b".repeat(255), &[])
        .await;
    assert!(matches!(result, Err(Error::Acquire(error)) if error.kind() == ErrorKind::Connection));
    assert_eq!(pool.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_execution_definitions_fail_before_acquiring_or_running_any_sql() {
    use neotales_migration::{Migration, Migrations};
    let pool = RefusingPool::default();
    let backend = PostgresBackend::new("app", "history", Duration::from_secs(1)).unwrap();
    for (version, up, down) in [
        (0, "SELECT 1", "SELECT 1"),
        (i64::MAX as u64 + 1, "SELECT 1", "SELECT 1"),
        (1, "SELECT 1\0; SELECT 2", "SELECT 1"),
        (1, "SELECT 1", "SELECT 1\0; SELECT 2"),
    ] {
        let migrations = Migrations::new(vec![
            Migration::new(version, "invalid", up, Some(down.to_owned())).unwrap(),
        ])
        .unwrap();
        assert!(matches!(
            backend.up(&pool, "app", &migrations, "fp", &[]).await,
            Err(Error::InvalidDefinitions)
        ));
        assert!(matches!(
            backend.down(&pool, "app", &migrations, "fp", 0, &[]).await,
            Err(Error::InvalidDefinitions)
        ));
    }
    assert_eq!(pool.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_flatten_manifest_versions_or_nul_fail_before_database_acquisition() {
    use neotales_migration::{FlattenManifest, Migration};
    let pool = RefusingPool::default();
    let backend = PostgresBackend::new("app", "history", Duration::from_secs(1)).unwrap();
    for (old_version, baseline_version, old_sql, baseline_sql) in [
        (0, 3, "SELECT 1", "SELECT 1"),
        (1, u64::MAX, "SELECT 1", "SELECT 1"),
        (1, 3, "SELECT 1\0", "SELECT 1"),
        (1, 3, "SELECT 1", "SELECT 1\0"),
    ] {
        let old = Migration::new(old_version, "old", old_sql, None).unwrap();
        let baseline = Migration::new(baseline_version, "baseline", baseline_sql, None)
            .unwrap()
            .into_baseline();
        let manifest = FlattenManifest::new(vec![old], baseline).unwrap();
        assert!(matches!(
            backend.flatten(&pool, "app", &manifest, "fp", &[]).await,
            Err(Error::InvalidDefinitions)
        ));
    }
    assert_eq!(pool.0.load(Ordering::SeqCst), 0);
}
