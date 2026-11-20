//! Durable file, migration, audit-append, and concurrent-access scenarios.

use std::time::Duration;

use neotales_dbapi::{DbError, DbType, ErrorKind, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqliteDurability, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn text(value: impl Into<String>) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.into())).expect("valid text parameter")
}

async fn durable(path: &std::path::Path, max_connections: u32) -> SqlitePoolProvider {
    SqlitePoolProvider::connect(
        SqliteConfig::new(path)
            .durability(SqliteDurability::Durable)
            .busy_timeout(Duration::from_secs(10))
            .max_connections(max_connections),
    )
    .await
    .expect("durable provider")
}

#[tokio::test]
async fn user_version_migration_is_idempotent_and_persists_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("migration.sqlite");

    let provider = durable(&path, 2).await;
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL);",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let version = session
        .fetch_scalar_async(
            "PRAGMA user_version",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(version, Value::I64(0));

    // Apply version 1 only when the file still reports version 0.
    session
        .execute_async("PRAGMA user_version = 1", QueryOptions::new())
        .await
        .unwrap();
    session
        .execute_async(
            "INSERT INTO schema_migrations (version, applied) VALUES (?, ?)",
            QueryOptions::new()
                .bind(Parameter::new(DbType::I64, Value::I64(1)).unwrap())
                .bind(text("2026-11-15")),
        )
        .await
        .unwrap();
    let version = session
        .fetch_scalar_async(
            "PRAGMA user_version",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(version, Value::I64(1));

    // Re-running the migration is idempotent; the version row is unique.
    session
        .execute_trusted_script_async(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let applied = session
        .fetch_scalar_async(
            "SELECT count(*) FROM schema_migrations",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(applied, Value::I64(1));
    drop(session);
    drop(provider);

    let reopened = durable(&path, 1).await;
    let version = reopened
        .fetch_scalar_async(
            "PRAGMA user_version",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(version, Value::I64(1));
    let rows = reopened
        .fetch_all_async(
            "SELECT version, applied FROM schema_migrations",
            QueryOptions::new().limit(limit(4, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].read::<i64>("version").unwrap(), 1);
    assert_eq!(rows[0].read::<String>("applied").unwrap(), "2026-11-15");
}

#[tokio::test]
async fn concurrent_immediate_transactions_append_a_complete_ordered_audit_log() {
    const WRITERS: usize = 40;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("audit.sqlite");
    let provider = durable(&path, 4).await;
    provider
        .execute_async(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for index in 0..WRITERS {
        let provider = provider.clone();
        tasks.push(tokio::spawn(async move {
            let mut session = provider
                .acquire_sqlite()
                .await
                .map_err(|_| DbError::new(ErrorKind::Connection))?;
            let mut transaction = session
                .begin_immediate_async()
                .await
                .map_err(|_| DbError::new(ErrorKind::Connection))?;
            transaction
                .execute_async(
                    "INSERT INTO audit (event) VALUES (?)",
                    QueryOptions::new().bind(text(format!("event-{index}"))),
                )
                .await?;
            transaction.commit_async().await?;
            Ok::<(), DbError>(())
        }));
    }
    for task in tasks {
        task.await.expect("writer task").expect("writer result");
    }

    let rows = provider
        .fetch_all_async(
            "SELECT id, event FROM audit ORDER BY id",
            QueryOptions::new().limit(limit(WRITERS + 1, 100_000)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), WRITERS);
    let mut seen = std::collections::BTreeSet::new();
    for row in &rows {
        let event = row.read::<String>("event").unwrap();
        assert!(seen.insert(event.clone()), "duplicate event {event}");
    }
    assert_eq!(seen.len(), WRITERS);

    provider.close();
    provider.wait_for_cleanup().await.unwrap();
    drop(provider);

    let reopened = durable(&path, 1).await;
    let count = reopened
        .fetch_scalar_async(
            "SELECT count(*) FROM audit",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(
        count,
        Value::I64(i64::try_from(WRITERS).expect("writer count fits in i64"))
    );
}

#[tokio::test]
async fn wal_readers_see_complete_snapshots_while_a_writer_commits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("readers.sqlite");
    let provider = durable(&path, 4).await;
    provider
        .execute_async(
            "CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, amount INTEGER NOT NULL)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    provider
        .execute_async(
            "INSERT INTO ledger (amount) SELECT 1 FROM (WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 100) SELECT x FROM n)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let reader_provider = provider.clone();
    let readers = (0..3)
        .map(|_| {
            let provider = reader_provider.clone();
            tokio::spawn(async move {
                for _ in 0..20 {
                    let count = provider
                        .fetch_scalar_async(
                            "SELECT count(*) FROM ledger",
                            QueryOptions::new().limit(limit(1, 64)),
                        )
                        .await
                        .map_err(|_| DbError::new(ErrorKind::Connection))?;
                    let Value::I64(count) = count else {
                        return Err(DbError::new(ErrorKind::Decode));
                    };
                    assert!(count == 100 || count == 200, "partial snapshot {count}");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok::<(), DbError>(())
            })
        })
        .collect::<Vec<_>>();

    let writer = {
        let provider = provider.clone();
        tokio::spawn(async move {
            let mut session = provider.acquire_sqlite().await?;
            let mut transaction = session.begin_immediate_async().await?;
            transaction
                .execute_trusted_script_async(
                    "INSERT INTO ledger (amount) SELECT 2 FROM (WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 100) SELECT x FROM n);",
                    QueryOptions::new(),
                )
                .await?;
            transaction.commit_async().await?;
            Ok::<(), DbError>(())
        })
    };
    for reader in readers {
        reader.await.expect("reader task").expect("reader result");
    }
    writer.await.expect("writer task").expect("writer result");

    let count = provider
        .fetch_scalar_async(
            "SELECT count(*) FROM ledger",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(count, Value::I64(200));
}

#[tokio::test]
async fn audit_writes_survive_a_constraint_failure_on_the_same_logical_table() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("audit-gap.sqlite");
    let provider = durable(&path, 2).await;
    provider
        .execute_async(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, event TEXT NOT NULL UNIQUE)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    provider
        .execute_async(
            "INSERT INTO audit (id, event) VALUES (1, 'first')",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut session = provider.acquire_sqlite().await.unwrap();
    let error = session
        .execute_async(
            "INSERT INTO audit (id, event) VALUES (2, 'first')",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);

    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async(
            "INSERT INTO audit (id, event) VALUES (2, 'second')",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT id, event FROM audit ORDER BY id",
            QueryOptions::new().limit(limit(10, 512)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].read::<i64>("id").unwrap(), 2);
    assert_eq!(rows[1].read::<String>("event").unwrap(), "second");
}
