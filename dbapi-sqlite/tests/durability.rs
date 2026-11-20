//! Real-file integration tests for the per-pool SQLite durability policy.

use std::time::Duration;

use neotales_dbapi::{ErrorKind, QueryOptions, RowLimit, Session};
use neotales_dbapi_sqlite::{SqliteConfig, SqliteDurability, SqlitePoolProvider};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid row limit")
}

fn durable(path: &std::path::Path, max_connections: u32) -> SqliteConfig {
    SqliteConfig::new(path)
        .busy_timeout(Duration::from_millis(100))
        .max_connections(max_connections)
        .durability(SqliteDurability::Durable)
}

async fn inspector(path: &std::path::Path) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .busy_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("inspection connection")
}

async fn pragmas(session: &mut dyn Session) -> (String, i64) {
    let rows = session.fetch_all_async(
            "SELECT (SELECT journal_mode FROM pragma_journal_mode) AS journal_mode, (SELECT synchronous FROM pragma_synchronous) AS synchronous",
            QueryOptions::new().limit(limit(1, 512)),
        )
        .await
        .expect("read pragmas");
    (
        rows[0].get::<String>("journal_mode").unwrap(),
        rows[0].get::<i64>("synchronous").unwrap(),
    )
}

#[tokio::test]
async fn inherited_policy_keeps_sqlite_defaults_and_matches_the_default_config() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inherited.sqlite");
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(3),
    )
    .await
    .unwrap();
    assert_eq!(provider.durability(), SqliteDurability::Inherit);

    let mut sessions = Vec::new();
    for _ in 0..3 {
        sessions.push(provider.acquire_sqlite().await.unwrap());
    }
    for session in &mut sessions {
        let (journal_mode, synchronous) = pragmas(session.as_mut()).await;
        assert_eq!(
            journal_mode.to_ascii_lowercase(),
            "delete",
            "the default policy must not switch a fresh database to WAL"
        );
        assert_eq!(synchronous, 2, "SQLite's default synchronous is FULL");
    }
}

#[tokio::test]
async fn durable_policy_applies_to_every_simultaneous_connection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durable.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 4))
        .await
        .unwrap();
    assert_eq!(provider.durability(), SqliteDurability::Durable);

    // Hold every connection at once so the pool must open four distinct physical connections.
    let mut sessions = Vec::new();
    for _ in 0..4 {
        sessions.push(provider.acquire_sqlite().await.unwrap());
    }
    for session in &mut sessions {
        let (journal_mode, synchronous) = pragmas(session.as_mut()).await;
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(synchronous, 2, "synchronous=FULL is reported as 2");
    }
}

#[tokio::test]
async fn durable_wal_persists_across_reopen_and_remains_readable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("persistent.sqlite");
    {
        let provider = SqlitePoolProvider::connect(durable(&path, 1))
            .await
            .unwrap();
        let mut session = provider.acquire_sqlite().await.unwrap();
        session.execute_trusted_script_async(
                "CREATE TABLE durable_probe(value TEXT); INSERT INTO durable_probe VALUES ('kept');",
                QueryOptions::new(),
            )
            .await
            .unwrap();
    }

    // WAL is a property of the database file, so an inheriting provider observes it too.
    let inherited = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(1),
    )
    .await
    .unwrap();
    let mut session = inherited.acquire_sqlite().await.unwrap();
    let (journal_mode, synchronous) = pragmas(session.as_mut()).await;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2);
    let rows = session
        .fetch_all_async(
            "SELECT value FROM durable_probe",
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("value").unwrap(), "kept");
    drop(session);
    drop(inherited);

    let reopened = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = reopened.acquire_sqlite().await.unwrap();
    assert_eq!(
        pragmas(session.as_mut()).await.0.to_ascii_lowercase(),
        "wal"
    );
}

#[tokio::test]
async fn durable_policy_verifies_a_replacement_connection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replacement.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();

    let mut first = provider.acquire_sqlite().await.unwrap();
    first
        .execute_trusted_script_async(
            "CREATE TEMP TABLE connection_marker(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    // A dispatched failure invalidates and discards this physical connection.
    assert_eq!(
        first
            .execute_async(
                "INSERT INTO main.missing_table VALUES (1)",
                QueryOptions::new(),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    drop(first);

    let mut replacement = provider.acquire_sqlite().await.unwrap();
    let (journal_mode, synchronous) = pragmas(replacement.as_mut()).await;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2);
    // A temp table lives on one physical connection. Its absence proves this is a replacement,
    // not the discarded connection, and the readback above proves the replacement was verified.
    assert_eq!(
        replacement
            .fetch_all_async(
                "SELECT value FROM temp.connection_marker",
                QueryOptions::new().limit(limit(1, 128)),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
}

#[tokio::test]
async fn durable_policy_rejects_in_memory_database_names() {
    for name in [
        ":memory:",
        "file::memory:?cache=shared",
        "file:durable-memory?mode=memory&cache=shared",
        "file:durable-memdb?vfs=memdb",
    ] {
        let error = SqlitePoolProvider::connect(durable(std::path::Path::new(name), 1))
            .await
            .expect_err("an in-memory name must not satisfy the durable policy");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{name}");
        assert!(!format!("{error:?}").contains(name));
    }

    // The inherited policy retains its previous acceptance of an in-memory database.
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(":memory:").busy_timeout(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    assert_eq!(provider.durability(), SqliteDurability::Inherit);
}

#[tokio::test]
async fn durable_policy_fails_closed_while_another_connection_blocks_wal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("locked.sqlite");
    let setup = inspector(&path).await;
    sqlx::query("CREATE TABLE lock_probe(value INTEGER)")
        .execute(&setup)
        .await
        .unwrap();

    let blocker_pool = inspector(&path).await;
    let mut blocker = blocker_pool.begin().await.unwrap();
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sqlite_master")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();

    // Switching a rollback-journal database to WAL needs an exclusive lock. The open must report
    // the inherited mode as a failure instead of returning a provider that is not durable.
    let error = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .expect_err("a blocked WAL transition must fail closed");
    assert_eq!(
        error.kind(),
        ErrorKind::Busy,
        "SQLite reports the blocked journal-mode change as busy"
    );
    assert!(!format!("{error:?}").contains("locked.sqlite"));

    blocker.rollback().await.unwrap();
    blocker_pool.close().await;
    setup.close().await;

    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let (journal_mode, synchronous) = pragmas(session.as_mut()).await;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2);
}

async fn count_rows(path: &std::path::Path, table: &str) -> i64 {
    let pool = inspector(path).await;
    let count = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    count
}

#[tokio::test]
async fn durable_policy_fails_before_begin_after_synchronous_change() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("begin-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async("CREATE TABLE guarded(value INTEGER);", QueryOptions::new())
        .await
        .unwrap();
    drop(setup);

    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();
    let error = session
        .begin_immediate_async()
        .await
        .err()
        .expect("durable begin must fail closed");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(!format!("{error:?}").contains("begin-boundary.sqlite"));
    drop(session);

    // The weakened connection was discarded; a replacement is verified before use.
    let mut replacement = provider.acquire_sqlite().await.unwrap();
    replacement
        .execute_async("INSERT INTO guarded VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    drop(replacement);
    assert_eq!(count_rows(&path, "guarded").await, 1);
}

#[tokio::test]
async fn durable_policy_fails_before_fetch_after_synchronous_change() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fetch-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async(
            "CREATE TABLE fetch_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(setup);

    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();
    let error = session
        .fetch_all_async(
            "INSERT INTO fetch_probe VALUES (1) RETURNING value",
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    drop(session);
    assert_eq!(count_rows(&path, "fetch_probe").await, 0);
}

#[tokio::test]
async fn durable_policy_fails_before_execute_after_synchronous_change() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("execute-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async(
            "CREATE TABLE execute_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(setup);

    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();
    let error = session
        .execute_async("INSERT INTO execute_probe VALUES (1)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    drop(session);
    assert_eq!(count_rows(&path, "execute_probe").await, 0);
}

#[tokio::test]
#[cfg(unix)]
async fn durable_policy_fails_before_commit_after_file_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async(
            "CREATE TABLE commit_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(setup);

    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO commit_probe VALUES (1)", QueryOptions::new())
        .await
        .unwrap();

    // The write is in the open transaction. Replace the main file before COMMIT; the pre-commit
    // revalidation must refuse to dispatch the commit.
    let moved = directory.path().join("commit-moved.sqlite");
    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"").unwrap();

    let error = transaction.commit_async().await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported,
        "a pre-commit identity failure is not a dispatched commit outcome"
    );
    assert_ne!(error.kind(), ErrorKind::CommitOutcomeUnknown);

    // The session was invalidated; the connection closes and SQLite rolls the transaction back.
    let error = session
        .execute_async("INSERT INTO commit_probe VALUES (2)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Connection);
    drop(session);

    assert!(std::fs::metadata(&moved).is_ok());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}

#[tokio::test]
#[cfg(unix)]
async fn durable_policy_fails_transaction_statement_after_file_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("statement-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async(
            "CREATE TABLE statement_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(setup);

    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async(
            "INSERT INTO statement_probe VALUES (1)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let moved = directory.path().join("statement-moved.sqlite");
    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"").unwrap();

    let error = transaction
        .execute_async(
            "INSERT INTO statement_probe VALUES (2)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    drop(transaction);
    drop(session);
    assert!(std::fs::metadata(&moved).is_ok());
}

#[tokio::test]
async fn durable_trusted_scripts_reject_policy_and_attachment_keywords() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("script-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE script_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    for script in [
        "PRAGMA synchronous=OFF; INSERT INTO script_probe VALUES (1);",
        "/* leading */ pRaGmA/**/synchronous=OFF; INSERT INTO script_probe VALUES (2);",
        "ATTACH DATABASE 'other.sqlite' AS other; INSERT INTO script_probe VALUES (3);",
        "DETACH DATABASE other;",
        "SELECT load_extension('x');",
        "INSERT INTO script_probe VALUES (4); -- attach",
        "INSERT INTO script_probe VALUES (5); /* PRAGMA */",
    ] {
        let error = session
            .execute_trusted_script_async(script, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{script}");
        assert!(!format!("{error:?}").contains("script_probe"));
    }

    // A script without those words still runs, including more than one statement.
    session
        .execute_trusted_script_async(
            "INSERT INTO script_probe VALUES (10); INSERT INTO script_probe VALUES (11);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(session);

    let inspect = inspector(&path).await;
    let values: Vec<i64> = sqlx::query_scalar("SELECT value FROM script_probe ORDER BY value")
        .fetch_all(&inspect)
        .await
        .unwrap();
    assert_eq!(values, vec![10, 11]);
    inspect.close().await;
}

#[tokio::test]
async fn durable_statements_reject_attach_and_detach() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("attach-boundary.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();

    for sql in [
        "ATTACH DATABASE 'other.sqlite' AS other",
        "/* leading */ aTtAcH DATABASE 'other.sqlite' AS other",
        "-- leading\nDETACH DATABASE other",
    ] {
        let error = session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{sql}");
        assert!(!format!("{error:?}").contains("attach-boundary.sqlite"));
    }
}

#[tokio::test]
async fn durable_policy_is_per_connection_and_inherit_stays_generic() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("per-connection.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 2))
        .await
        .unwrap();
    let mut setup = provider.acquire_sqlite().await.unwrap();
    setup
        .execute_trusted_script_async("CREATE TABLE isolated(value INTEGER);", QueryOptions::new())
        .await
        .unwrap();
    drop(setup);

    let mut first = provider.acquire_sqlite().await.unwrap();
    let mut weakened = provider.acquire_sqlite().await.unwrap();
    weakened
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();

    // Synchronous is per connection: the first connection remains FULL and commits.
    let mut transaction = first.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO isolated VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();

    let error = weakened
        .begin_immediate_async()
        .await
        .err()
        .expect("weakened connection must not begin");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    drop(first);
    drop(weakened);
    assert_eq!(count_rows(&path, "isolated").await, 1);

    // Inherit keeps the generic SQLite behavior: the same sequence is allowed.
    let inherited = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(Duration::from_millis(100))
            .max_connections(1),
    )
    .await
    .unwrap();
    let mut session = inherited.acquire_sqlite().await.unwrap();
    session
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO isolated VALUES (2)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    session
        .execute_trusted_script_async(
            "PRAGMA synchronous=OFF; INSERT INTO isolated VALUES (3);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    drop(session);
    assert_eq!(count_rows(&path, "isolated").await, 3);
}

#[tokio::test]
async fn durable_policy_allows_busy_timeout_and_encoding_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("migration-pragmas.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();

    session
        .execute_async("PRAGMA busy_timeout = 500", QueryOptions::new())
        .await
        .unwrap();
    let rows = session
        .fetch_all_async(
            "PRAGMA main.encoding",
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0]
            .get_ordinal::<String>(0)
            .unwrap()
            .to_ascii_uppercase(),
        "UTF-8"
    );

    session
        .execute_trusted_script_async(
            "CREATE TABLE busy_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO busy_probe VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    drop(session);
    assert_eq!(count_rows(&path, "busy_probe").await, 1);
}

#[tokio::test]
#[cfg(unix)]
async fn durable_policy_detects_main_file_replacement_before_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replaceable.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE replacement_probe(value INTEGER);",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    // Move the file the connection has open and put a different file at the original path.
    let moved = directory.path().join("moved.sqlite");
    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"").unwrap();

    let error = session
        .begin_immediate_async()
        .await
        .err()
        .expect("durable begin must fail closed");
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported,
        "a replaced main file must fail closed before a transaction begins"
    );
    assert!(!format!("{error:?}").contains("replaceable.sqlite"));
    drop(session);

    // The moved original and the replacement file are preserved for inspection.
    assert!(std::fs::metadata(&moved).is_ok());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
}
