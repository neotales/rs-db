//! Real-file durability policy tests for the direct SQLite provider.

use std::time::Duration;

use neotales_dbapi::{ErrorKind, QueryOptions, RowLimit, Session, Value};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqliteDurability, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn durable(path: &std::path::Path, max_connections: u32) -> SqliteConfig {
    SqliteConfig::new(path)
        .busy_timeout(Duration::from_millis(100))
        .max_connections(max_connections)
        .durability(SqliteDurability::Durable)
}

fn inspector(path: &std::path::Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(path).expect("inspector");
    connection
        .busy_timeout(Duration::from_millis(500))
        .expect("busy timeout");
    connection
}

async fn pragmas(session: &mut dyn Session) -> (String, i64) {
    let rows = session
        .fetch_all_async(
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

fn count_rows(path: &std::path::Path, table: &str) -> i64 {
    inspector(path)
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .expect("count rows")
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
        session
            .execute_trusted_script_async(
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
                QueryOptions::new()
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

    // Inherited durability still requires one connection for an in-memory database, because a
    // second physical connection would be a different database.
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(":memory:")
            .busy_timeout(Duration::from_millis(10))
            .max_connections(1),
    )
    .await
    .unwrap();
    assert_eq!(provider.durability(), SqliteDurability::Inherit);
    let error = SqlitePoolProvider::connect(
        SqliteConfig::new(":memory:").busy_timeout(Duration::from_millis(10)),
    )
    .await
    .expect_err("a multi-connection in-memory pool is not safe");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn durable_policy_fails_closed_while_another_connection_blocks_wal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("locked.sqlite");
    let setup = inspector(&path);
    setup
        .execute_batch("CREATE TABLE lock_probe(value INTEGER)")
        .unwrap();

    let blocker = inspector(&path);
    let transaction = blocker.unchecked_transaction().unwrap();
    let _: i64 = transaction
        .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
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

    transaction.rollback().unwrap();
    drop(blocker);
    drop(setup);

    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let (journal_mode, synchronous) = pragmas(session.as_mut()).await;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2);
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
    assert_eq!(count_rows(&path, "guarded"), 1);
}

#[tokio::test]
async fn durable_async_checkout_revalidates_idle_pragma_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("idle-pragma-async.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();

    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("PRAGMA synchronous=OFF", QueryOptions::new())
        .await
        .unwrap();
    drop(session);

    // The idle ping must verify policy, discard the weakened connection, and open a fresh one
    // before returning a session.
    let mut session = provider.acquire_sqlite().await.unwrap();
    assert_eq!(pragmas(session.as_mut()).await.1, 2);
}

#[test]
fn durable_sync_checkout_revalidates_idle_pragma_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("idle-pragma-sync.sqlite");
    let provider = SqlitePoolProvider::connect_sync(durable(&path, 1)).unwrap();

    let mut session = provider.acquire_sync_sqlite().unwrap();
    neotales_dbapi::sync::Session::execute(
        &mut session,
        "PRAGMA synchronous=OFF",
        QueryOptions::new(),
    )
    .unwrap();
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let rows = neotales_dbapi::sync::Session::fetch_all(
        &mut session,
        "PRAGMA synchronous",
        QueryOptions::new().limit(limit(1, 64)),
    )
    .unwrap();
    assert_eq!(rows[0].read::<i64>("synchronous").unwrap(), 2);
}

#[tokio::test]
#[cfg(unix)]
async fn durable_async_checkout_rejects_replaced_idle_database_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("idle-replaced-async.sqlite");
    let moved = directory.path().join("idle-original-async.sqlite");
    let provider = SqlitePoolProvider::connect(durable(&path, 1))
        .await
        .unwrap();
    drop(provider.acquire_sqlite().await.unwrap());

    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"").unwrap();

    let Err(error) = provider.acquire_sqlite().await else {
        panic!("replacement file must fail durable identity verification");
    };
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
#[cfg(unix)]
fn durable_sync_checkout_rejects_replaced_idle_database_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("idle-replaced-sync.sqlite");
    let moved = directory.path().join("idle-original-sync.sqlite");
    let provider = SqlitePoolProvider::connect_sync(durable(&path, 1)).unwrap();
    drop(provider.acquire_sync_sqlite().unwrap());

    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"").unwrap();

    let error = provider
        .acquire_sync_sqlite()
        .expect_err("replacement file must fail durable identity verification");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
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
    assert_eq!(count_rows(&path, "fetch_probe"), 0);
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
    assert_eq!(count_rows(&path, "execute_probe"), 0);
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

    let inspect = inspector(&path);
    let values: Vec<i64> = inspect
        .prepare("SELECT value FROM script_probe ORDER BY value")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(values, vec![10, 11]);
}

#[tokio::test]
async fn durable_readback_rejects_a_non_wal_connection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("readback.sqlite");
    let connection = inspector(&path);
    let value = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
        .unwrap();
    assert_eq!(value.to_ascii_lowercase(), "delete");
    let synchronous = connection
        .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
        .unwrap();
    assert_eq!(synchronous, 2);
    assert_eq!(
        Value::I64(synchronous),
        Value::I64(2),
        "SQLite reports synchronous=FULL as 2"
    );
}
