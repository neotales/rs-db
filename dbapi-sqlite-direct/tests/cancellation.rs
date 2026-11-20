//! Cancellation and quarantine behavior for interrupted or abandoned operations.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use neotales_dbapi::{ErrorKind, Pool, QueryOptions, RowLimit};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn provider(path: &std::path::Path, busy: Duration) -> SqlitePoolProvider {
    SqlitePoolProvider::connect_sync(
        SqliteConfig::new(path)
            .busy_timeout(busy)
            .max_connections(1),
    )
    .expect("provider")
}

fn inspector(path: &std::path::Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(path).expect("inspector");
    connection
        .busy_timeout(Duration::from_millis(500))
        .expect("busy timeout");
    connection
}

/// Polls a boxed database future once so the command is dispatched, then leaves it pending.
fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T> + Send + '_>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}

async fn wait_for_slot(provider: &SqlitePoolProvider) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while provider.unused_connection_slots() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("capacity must eventually be released");
}

const LONG_COUNT: &str = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 20000000) SELECT count(*) FROM cnt";
const LONG_ROWS: &str = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 200000) SELECT x FROM cnt";

#[tokio::test]
async fn dropping_an_execute_future_interrupts_and_poisons_the_session() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(
        &directory.path().join("execute.sqlite"),
        Duration::from_secs(5),
    );
    let mut session = provider.acquire_async().await.unwrap();

    let mut future = session.execute_async(LONG_COUNT, QueryOptions::new());
    assert!(poll_once(&mut future).is_pending());
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(future);

    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled,
        "an abandoned operation must poison its session before dispatch"
    );
    drop(session);
    wait_for_slot(&provider).await;
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn dropping_a_fetch_future_interrupts_and_poisons_the_session() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(
        &directory.path().join("fetch.sqlite"),
        Duration::from_secs(5),
    );
    let mut session = provider.acquire_async().await.unwrap();

    let mut future = session.fetch_async(
        LONG_ROWS,
        QueryOptions::new().limit(limit(100_000, 10_000_000)),
    );
    assert!(poll_once(&mut future).is_pending());
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(future);

    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit(1, 64)))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled
    );
    drop(session);
    wait_for_slot(&provider).await;
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn dropping_a_begin_future_keeps_the_lease_until_the_worker_stops() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("begin.sqlite");
    let provider = provider(&path, Duration::from_secs(5));
    // Create the file before the blocker opens it.
    drop(provider.acquire_async().await.unwrap());

    let blocker = inspector(&path);
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut future = session.begin_immediate_async();
    assert!(poll_once(&mut future).is_pending());
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(future);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled
    );

    // The worker is still blocked in SQLite's busy handler. Dropping the session must not free the
    // slot, because the statement may still be running.
    drop(session);
    assert_eq!(provider.unused_connection_slots(), 0);

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    wait_for_slot(&provider).await;
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn dropping_a_commit_future_does_not_release_the_lease_early() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("commit.sqlite");
    let provider = provider(&path, Duration::from_secs(5));
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("CREATE TABLE rows (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO rows VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    drop(transaction);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);

    // A reader holds a shared lock, so a rollback-journal COMMIT blocks in the busy handler.
    let blocker = inspector(&path);
    blocker.execute_batch("BEGIN").unwrap();
    let _: i64 = blocker
        .query_row("SELECT count(*) FROM rows", [], |row| row.get(0))
        .unwrap();

    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO rows VALUES (2)", QueryOptions::new())
        .await
        .unwrap();
    let mut future = transaction.commit_async();
    assert!(poll_once(&mut future).is_pending());
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(future);
    assert_eq!(provider.unused_connection_slots(), 0);
    drop(session);

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    wait_for_slot(&provider).await;
    provider.close();
    provider.wait_for_cleanup().await.unwrap();
}

#[tokio::test]
async fn dropping_a_rollback_future_is_safe_and_eventually_recovers_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(
        &directory.path().join("rollback.sqlite"),
        Duration::from_secs(1),
    );
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("CREATE TABLE rows (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO rows VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    // Dropping the rollback future without polling is also a cancellation: the transaction is
    // consumed and its connection is discarded rather than silently left reusable.
    let future = transaction.rollback_async();
    drop(future);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);
    wait_for_slot(&provider).await;

    let mut session = provider.acquire_async().await.unwrap();
    let count = session
        .fetch_scalar_async(
            "SELECT count(*) FROM rows",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(count, neotales_dbapi::Value::I64(0));
}

#[tokio::test]
async fn an_abandoned_future_does_not_leak_an_interrupt_into_a_new_session() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-interrupt.sqlite");
    let provider = provider(&path, Duration::from_millis(200));
    drop(provider.acquire_async().await.unwrap());

    let blocker = inspector(&path);
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    // Drop immediately after dispatch, before the busy wait can finish. SQLite does not interrupt
    // a busy handler, so this deliberately exercises the late-interrupt window. The session is
    // still poisoned and its lease is held until the worker stops.
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut future = session.begin_immediate_async();
    assert!(poll_once(&mut future).is_pending());
    drop(future);
    drop(session);
    assert_eq!(provider.unused_connection_slots(), 0);

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    wait_for_slot(&provider).await;

    // A fresh worker and connection must be fully usable and unaffected by the earlier interrupt.
    let mut session = provider.acquire_async().await.unwrap();
    let value = session
        .fetch_scalar_async("SELECT 42", QueryOptions::new().limit(limit(1, 64)))
        .await
        .unwrap();
    assert_eq!(value, neotales_dbapi::Value::I64(42));
}

#[tokio::test]
async fn a_closed_pool_discards_an_abandoned_worker_and_reports_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(
        &directory.path().join("cleanup.sqlite"),
        Duration::from_secs(1),
    );
    let mut session = provider.acquire_async().await.unwrap();
    let mut future = session.fetch_async(
        LONG_ROWS,
        QueryOptions::new().limit(limit(100_000, 10_000_000)),
    );
    assert!(poll_once(&mut future).is_pending());
    drop(future);
    drop(session);

    provider.close();
    provider
        .wait_for_cleanup()
        .await
        .expect("interrupted worker cleanup is not a failure");
}
