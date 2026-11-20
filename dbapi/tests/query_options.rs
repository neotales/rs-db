//! Unified query-option inheritance and builder checks.

use std::time::Duration;

use neotales_dbapi::{
    Capabilities, DbFuture, DbType, LimitSetting, Parameter, PolicyPool, Pool, PoolDefaults,
    PoolPolicy, QueryOptions, QuerySetting, ReplaySafety, RetrySetting, RowLimit, Session,
    TimeoutSetting, Value,
};

struct DeadlinePool;
impl Pool for DeadlinePool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults {
            timeout: Some(Duration::from_secs(4)),
            ..PoolDefaults::default()
        }
    }
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        panic!("checking defaults does not acquire")
    }
}

#[test]
fn policy_inherits_provider_deadline_unless_overridden() {
    let inherited = PolicyPool::new(DeadlinePool, PoolPolicy::default());
    assert_eq!(inherited.defaults().timeout, Some(Duration::from_secs(4)));
    let overridden = PolicyPool::new(
        DeadlinePool,
        PoolPolicy {
            default_timeout: Some(Duration::from_secs(1)),
            ..PoolPolicy::default()
        },
    );
    assert_eq!(overridden.defaults().timeout, Some(Duration::from_secs(1)));
}

#[test]
fn default_builder_inherits_globals_without_parameters() {
    let options = QueryOptions::default();
    assert_eq!(options.parameters.len(), 0);
    assert_eq!(options.timeout, TimeoutSetting::Inherit);
    assert_eq!(options.limit, LimitSetting::Inherit);
    assert_eq!(options.telemetry, QuerySetting::Inherit);
    assert_eq!(options.sql_capture, QuerySetting::Inherit);
    assert_eq!(options.retry, RetrySetting::Inherit);
    assert_eq!(options.replay_safety, ReplaySafety::Never);

    let defaults = PoolDefaults::default();
    assert_eq!(defaults.timeout, None);
    assert_eq!(options.resolved_timeout(defaults), defaults.timeout);
    assert_eq!(options.resolved_limit(defaults), RowLimit::default());
}

#[test]
fn builder_overrides_timeout_limit_and_parameters_without_global_mutation() {
    let parameter = Parameter::new(DbType::I64, Value::I64(7)).unwrap();
    let limit = RowLimit::with_max_sets(10, 4096, 2).unwrap();
    let options = QueryOptions::default()
        .bind(parameter)
        .bind_null(DbType::Text)
        .timeout(Duration::from_secs(2))
        .limit(limit)
        .telemetry(QuerySetting::Off)
        .sql_capture(QuerySetting::On)
        .retry(RetrySetting::Off)
        .replay_safe();
    assert_eq!(options.parameters.len(), 2);
    assert_eq!(
        options.resolved_timeout(PoolDefaults::default()),
        Some(Duration::from_secs(2))
    );
    assert_eq!(options.resolved_limit(PoolDefaults::default()), limit);
    assert_eq!(options.replay_safety, ReplaySafety::ExplicitlyReplaySafe);

    let defaults = PoolDefaults::default();
    assert!(defaults.limit.rows() > 0);
    assert!(defaults.limit.bytes() > 0);
    assert!(defaults.limit.max_sets() > 0);
}

#[test]
fn explicit_no_timeout_is_distinct_from_inherit() {
    let inherited = QueryOptions::default();
    let disabled = QueryOptions::default().no_timeout();
    let defaults = PoolDefaults::default();
    assert_eq!(inherited.resolved_timeout(defaults), defaults.timeout);
    assert_eq!(disabled.resolved_timeout(defaults), None);
    assert_ne!(inherited.timeout, disabled.timeout);
}

#[test]
fn limits_have_no_unbounded_off_variant() {
    let _ = LimitSetting::Inherit;
    let _ = LimitSetting::Set(RowLimit::new(1, 1).unwrap());
}
