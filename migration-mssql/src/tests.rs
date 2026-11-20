use super::{SqlServerBackend, valid_identifier, valid_identity};
use std::time::Duration;

#[test]
fn configuration_rejects_unsafe_or_oversized_names() {
    for schema in ["", "Upper", "has-dash", "sys", "information_schema", "x"] {
        let table = "history";
        let result = SqlServerBackend::new(schema, table, Duration::from_secs(1));
        assert_eq!(
            result.is_err(),
            matches!(
                schema,
                "" | "Upper" | "has-dash" | "sys" | "information_schema"
            ),
            "schema {schema:?}"
        );
    }
    assert!(SqlServerBackend::new("dbo", "history", Duration::from_secs(1)).is_ok());
    assert!(SqlServerBackend::new("dbo", "t".repeat(120), Duration::from_secs(1)).is_ok());
    assert!(SqlServerBackend::new("dbo", "t".repeat(121), Duration::from_secs(1)).is_err());
    assert!(
        SqlServerBackend::new("dbo", "m".repeat(113), Duration::from_secs(1)).is_ok(),
        "table plus _modules must still fit sysname"
    );
}

#[test]
fn configuration_rejects_invalid_lock_timeouts() {
    for timeout in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_micros(1500),
        Duration::from_millis(i32::MAX as u64 + 1),
    ] {
        assert_eq!(
            SqlServerBackend::new("dbo", "history", timeout)
                .unwrap_err()
                .to_string(),
            "invalid backend configuration"
        );
    }
    assert!(SqlServerBackend::new("dbo", "history", Duration::from_millis(1)).is_ok());
    assert!(SqlServerBackend::new("dbo", "history", Duration::from_secs(2)).is_ok());
}

#[test]
fn lock_resource_is_stable_and_identity_bound() {
    let first = SqlServerBackend::new("dbo", "history", Duration::from_secs(1)).unwrap();
    let second = SqlServerBackend::new("dbo", "history", Duration::from_secs(2)).unwrap();
    let other = SqlServerBackend::new("dbo", "other", Duration::from_secs(1)).unwrap();
    assert_eq!(first.lock_resource(), second.lock_resource());
    assert_ne!(first.lock_resource(), other.lock_resource());
    assert!(
        first
            .lock_resource()
            .starts_with("neotales-migration-mssql/lock/v1/")
    );
    assert!(first.lock_resource().len() <= 255);
}

#[test]
fn identities_reject_trailing_spaces_and_nul() {
    for value in ["app", "app ", " ", "", &"a".repeat(255)] {
        let expected = matches!(value, "app" | "a") || value.len() == 255;
        assert_eq!(valid_identity(value), expected, "{value:?}");
    }
    assert!(!valid_identity("app\0"));
    assert!(!valid_identity(&format!("{} ", "a".repeat(255))));
}

#[test]
fn identifiers_are_conservative() {
    for value in ["a", "_a", "a0", "a_b"] {
        assert!(valid_identifier(value, 128));
    }
    for value in ["", "A", "0a", "a-b", "a b"] {
        assert!(!valid_identifier(value, 128));
    }
    assert!(!valid_identifier(&"a".repeat(129), 128));
}
