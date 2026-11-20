//! Real direct-SQLite tests for paged reader views.
//!
//! The direct provider materializes its single bounded result set before returning a reader, so it
//! cannot produce a partial-read error or a second result set. Those lanes live in the crate-level
//! paging contract test with a scripted reader. This file proves the single-execution, parameter,
//! limit, empty-set, `RETURNING`, and drop behavior against a real file-backed SQLite database.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    DbEvent, DbType, ErrorKind, Operation, Parameter, Pool, PoolPolicy, QueryOptions, RowLimit,
    Session, SqlitePolicyPool, TelemetryOutcome, Value,
    paging::{Page, PageSize, PageStream, RowStream, SetRow},
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("valid integer parameter")
}

async fn provider(path: &std::path::Path) -> SqlitePoolProvider {
    SqlitePoolProvider::connect(
        SqliteConfig::new(path)
            .busy_timeout(Duration::from_millis(150))
            .max_connections(4),
    )
    .await
    .expect("provider")
}

fn page_values(page: &Page) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

fn set_row_values(rows: &[SetRow]) -> Vec<(usize, i64)> {
    rows.iter()
        .map(|item| {
            (
                item.set_index(),
                item.row().read::<i64>("value").expect("integer value"),
            )
        })
        .collect()
}

fn assert_send<T: Send>(_: &T) {}
fn assert_unpin<T: Unpin>(_: &T) {}

async fn count_items(session: &mut dyn Session) -> i64 {
    match session
        .fetch_scalar_async(
            "SELECT count(*) FROM items",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .expect("count")
    {
        Value::I64(count) => count,
        other => panic!("expected integer count, got {other:?}"),
    }
}

#[tokio::test]
async fn returning_write_is_one_execution_and_pages_are_only_a_view() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-returning.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();

    let reader = session
        .fetch_async(
            "INSERT INTO items(value) VALUES (1), (2), (3), (4), (5) RETURNING value",
            QueryOptions::new().limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    let stream = PageStream::new(reader, page_size(2));
    assert_send(&stream);
    assert_unpin(&stream);

    let pages: Vec<Page> = stream.try_collect().await.unwrap();
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![0, 0, 0]
    );
    assert_eq!(
        pages.iter().map(Page::len).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    assert_eq!(
        pages.iter().flat_map(page_values).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );

    // A second execution of the INSERT would duplicate rows. There is exactly one.
    assert_eq!(count_items(session.as_mut()).await, 5);
}

#[tokio::test]
async fn row_view_carries_set_identity_and_the_single_fetch_keeps_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-parameters.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    session
        .execute_trusted_script_async(
            "INSERT INTO items(value) VALUES (1), (2), (3), (4), (5)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let reader = session
        .fetch_async(
            "SELECT value FROM items WHERE value >= ? ORDER BY value",
            QueryOptions::new().bind(integer(3)).limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    let stream = RowStream::new(reader);
    assert_send(&stream);
    assert_unpin(&stream);

    let rows: Vec<SetRow> = stream.try_collect().await.unwrap();
    assert_eq!(set_row_values(&rows), vec![(0, 3), (0, 4), (0, 5)]);
}

#[tokio::test]
async fn page_size_does_not_replace_the_aggregate_row_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-limit.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();
    session
        .execute_trusted_script_async(
            "INSERT INTO items(value) VALUES (1), (2), (3), (4), (5)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let reader = session
        .fetch_async(
            "SELECT value FROM items ORDER BY value",
            QueryOptions::new().limit(limit(5, 4096)),
        )
        .await
        .unwrap();
    let pages: Vec<Page> = PageStream::new(reader, page_size(2))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(pages.iter().map(Page::len).sum::<usize>(), 5);

    // The page view never becomes permission to return a partial bounded batch.
    let Err(error) = session
        .fetch_async(
            "SELECT value FROM items ORDER BY value",
            QueryOptions::new().limit(limit(2, 4096)),
        )
        .await
    else {
        panic!("the aggregate row limit must fail rather than truncate");
    };
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[tokio::test]
async fn empty_returning_result_is_one_empty_page() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-empty.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();

    let reader = session
        .fetch_async(
            "INSERT INTO items(value) SELECT 99 WHERE 0 RETURNING value",
            QueryOptions::new().limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    let pages: Vec<Page> = PageStream::new(reader, page_size(2))
        .try_collect()
        .await
        .unwrap();

    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].set_index(), 0);
    assert!(pages[0].is_empty());
    assert_eq!(count_items(session.as_mut()).await, 0);
}

#[tokio::test]
async fn paging_adds_no_telemetry_event_beyond_the_single_fetch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-telemetry.sqlite");
    let provider = provider(&path).await;
    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let observed = Arc::clone(&events);
    let db = SqlitePolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: false,
            observer: Some(Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );

    let mut session = db.acquire_async().await.unwrap();
    let reader = session
        .fetch_async(
            "SELECT 1 AS value UNION ALL SELECT 2",
            QueryOptions::new().limit(limit(2, 4096)),
        )
        .await
        .unwrap();
    let pages: Vec<Page> = PageStream::new(reader, page_size(1))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(pages.len(), 2);

    let captured = events.lock().unwrap().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].operation, Operation::Fetch);
    assert_eq!(captured[0].outcome, TelemetryOutcome::Success);
    assert_eq!(captured[0].sql, None);
}

#[tokio::test]
async fn dropping_a_page_view_after_a_returning_write_does_not_retract_or_retry_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-drop.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();

    let reader = session
        .fetch_async(
            "INSERT INTO items(value) VALUES (7), (8), (9) RETURNING value",
            QueryOptions::new().limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    let mut stream = PageStream::new(reader, page_size(1));
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(page_values(&first), vec![7]);
    drop(stream);

    // The write happened once. Dropping the view neither rolls it back nor re-executes it.
    assert_eq!(count_items(session.as_mut()).await, 3);
}
