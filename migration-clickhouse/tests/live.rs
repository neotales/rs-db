//! Exercise the normal dependency build both with and without experimental writes.

#[path = "support/live.rs"]
mod live;

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
#[cfg(not(feature = "experimental-native-migrations"))]
async fn the_backend_fails_closed_on_the_fetch_only_provider() {
    live::the_backend_fails_closed_on_the_fetch_only_provider().await;
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh --experimental"]
#[cfg(feature = "experimental-native-migrations")]
async fn experimental_dependency_recovery_contract() {
    live::clickhouse_migration_contract().await;
}
