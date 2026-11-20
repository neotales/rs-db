//! Real direct-SQLite multi-request paging with keyset and offset sources.
//!
//! The example sources own their SQL, bound parameters, cursor, and continuation. The keyset source
//! uses a unique `(value, id)` composite cursor so it includes `i64::MIN` and does not skip duplicate
//! values at page boundaries. Both sources fetch one source-internal lookahead row with a separate,
//! source-owned byte allowance; only published rows are charged to the pager's aggregate budget.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    DbError, DbFuture, DbType, ErrorKind, Parameter, Pool, QueryOptions, RowLimit, Session, Value,
    paging::{
        PageSize,
        multi::{
            Continuation, PageBudget, PageRequest, PageSource, PageSourceStream, PagedPage,
            SourcePage, read_single_set,
        },
    },
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

const KEYSET_FIRST: &str = "SELECT value, id FROM items ORDER BY value, id LIMIT ?";
const KEYSET_NEXT: &str = "SELECT value, id FROM items \
     WHERE value > ? OR (value = ? AND id > ?) ORDER BY value, id LIMIT ?";
const OFFSET_SQL: &str = "SELECT value FROM items ORDER BY value, id LIMIT ? OFFSET ?";

// The extra lookahead row is source-internal. This allowance is separate from the pager's aggregate
// delivered-byte budget, so a page that exactly fits that budget can still be published with a
// continuation. A row larger than this source-owned allowance still fails closed at the provider.
const LOOKAHEAD_BYTES: usize = 16_384;

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn budget(rows: usize, bytes: usize, max_pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, max_pages).expect("valid page budget")
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("valid integer parameter")
}

fn lookahead_limit(request: PageRequest, rows: usize) -> Result<RowLimit, DbError> {
    RowLimit::new(
        rows,
        request.remaining_bytes().saturating_add(LOOKAHEAD_BYTES),
    )
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

fn page_values(page: &PagedPage) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

fn page_pairs(page: &PagedPage) -> Vec<(i64, i64)> {
    page.rows()
        .iter()
        .map(|row| {
            (
                row.read::<i64>("value").expect("integer value"),
                row.read::<i64>("id").expect("integer id"),
            )
        })
        .collect()
}

struct KeysetSource {
    session: Box<dyn Session>,
    calls: Arc<AtomicUsize>,
}

impl PageSource for KeysetSource {
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let cursor = match continuation {
                Some(token) => Some(
                    token
                        .downcast::<(i64, i64)>()
                        .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?,
                ),
                None => None,
            };
            let page_size = request.page_size().get();
            let effective_rows = page_size.min(request.remaining_rows());
            let fetch_rows = effective_rows.saturating_add(1);
            let fetch_rows =
                i64::try_from(fetch_rows).map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
            let row_limit = lookahead_limit(
                request,
                usize::try_from(fetch_rows)
                    .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?,
            )?;
            let (sql, options) = match cursor {
                Some((value, id)) => (
                    KEYSET_NEXT,
                    QueryOptions::new()
                        .bind(integer(value))
                        .bind(integer(value))
                        .bind(integer(id))
                        .bind(integer(fetch_rows))
                        .limit(row_limit),
                ),
                None => (
                    KEYSET_FIRST,
                    QueryOptions::new()
                        .bind(integer(fetch_rows))
                        .limit(row_limit),
                ),
            };
            let reader = self.session.fetch_async(sql, options).await?;
            let mut rows = read_single_set(reader).await?;
            let more = rows.len() > effective_rows;
            rows.truncate(effective_rows);
            let continuation = if more {
                let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
                Some(Continuation::new((
                    last.read::<i64>("value")?,
                    last.read::<i64>("id")?,
                )))
            } else {
                None
            };
            Ok(SourcePage::new(rows, continuation))
        })
    }
}

struct OffsetSource {
    session: Box<dyn Session>,
    calls: Arc<AtomicUsize>,
}

impl PageSource for OffsetSource {
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let offset = match continuation {
                Some(token) => token
                    .downcast::<usize>()
                    .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?,
                None => 0,
            };
            let page_size = request.page_size().get();
            let effective_rows = page_size.min(request.remaining_rows());
            let fetch_rows = effective_rows.saturating_add(1);
            let row_limit = lookahead_limit(request, fetch_rows)?;
            let fetch_limit =
                i64::try_from(fetch_rows).map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
            let offset_parameter =
                i64::try_from(offset).map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
            let options = QueryOptions::new()
                .bind(integer(fetch_limit))
                .bind(integer(offset_parameter))
                .limit(row_limit);
            let reader = self.session.fetch_async(OFFSET_SQL, options).await?;
            let mut rows = read_single_set(reader).await?;
            let more = rows.len() > effective_rows;
            rows.truncate(effective_rows);
            let continuation = if more {
                let next_offset = offset
                    .checked_add(rows.len())
                    .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
                Some(Continuation::new(next_offset))
            } else {
                None
            };
            Ok(SourcePage::new(rows, continuation))
        })
    }
}

async fn seed(path: &std::path::Path, rows: &str) -> SqlitePoolProvider {
    let provider = provider(path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE items (value INTEGER NOT NULL, id INTEGER PRIMARY KEY)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_trusted_script_async(
            &format!("INSERT INTO items(value, id) VALUES {rows}"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    provider
}

async fn one_row_bytes(provider: &SqlitePoolProvider) -> usize {
    let mut session = provider.acquire_async().await.unwrap();
    let row = session
        .fetch_one_async(
            "SELECT value, id FROM items ORDER BY value, id LIMIT 1",
            QueryOptions::new().limit(limit(1, 4096)),
        )
        .await
        .expect("one row");
    row.encoded_bytes().expect("encoded bytes")
}

#[tokio::test]
async fn keyset_source_pages_with_bound_cursor_and_limit_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = KeysetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10))
            .try_collect()
            .await
            .unwrap();

    assert_eq!(
        pages.iter().map(PagedPage::page_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert_eq!(page_values(&pages[1]), vec![3, 4]);
    assert_eq!(page_values(&pages[2]), vec![5]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn offset_source_pages_with_bound_limit_and_offset_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-offset.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = OffsetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10))
            .try_collect()
            .await
            .unwrap();

    assert_eq!(
        pages.iter().map(PagedPage::page_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert_eq!(page_values(&pages[1]), vec![3, 4]);
    assert_eq!(page_values(&pages[2]), vec![5]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn keyset_remaining_rows_smaller_than_page_size_publishes_then_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-remaining-rows.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = KeysetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let mut stream = PageSourceStream::new(Box::new(source), page_size(2), budget(1, 65_536, 10));

    let page = stream
        .next()
        .await
        .expect("first page item")
        .expect("the permitted one-row page is published despite the two-row page size");
    assert_eq!(page_values(&page), vec![1]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let error = stream
        .next()
        .await
        .expect("the continuation at the exact row bound fails closed")
        .expect_err("another row exists beyond the allowed budget");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn offset_remaining_rows_smaller_than_page_size_publishes_then_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-offset-remaining-rows.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = OffsetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let mut stream = PageSourceStream::new(Box::new(source), page_size(2), budget(1, 65_536, 10));

    let page = stream
        .next()
        .await
        .expect("first page item")
        .expect("the permitted one-row page is published despite the two-row page size");
    assert_eq!(page_values(&page), vec![1]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let error = stream
        .next()
        .await
        .expect("the continuation at the exact row bound fails closed")
        .expect_err("another row exists beyond the allowed budget");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn keyset_cursor_includes_i64_min_and_duplicate_values() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-duplicates.sqlite");
    let provider = seed(
        &path,
        "(-9223372036854775808, 1), (-9223372036854775808, 2), \
         (0, 3), (0, 4), (5, 5)",
    )
    .await;
    let session = provider.acquire_async().await.unwrap();
    let source = KeysetSource {
        session,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 65_536, 10))
            .try_collect()
            .await
            .unwrap();

    let pairs: Vec<(i64, i64)> = pages.iter().flat_map(page_pairs).collect();
    assert_eq!(
        pairs,
        vec![(i64::MIN, 1), (i64::MIN, 2), (0, 3), (0, 4), (5, 5),]
    );
}

#[tokio::test]
async fn keyset_final_page_exactly_fitting_byte_budget_completes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-exact-bytes.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2)").await;
    let row_bytes = one_row_bytes(&provider).await;
    let session = provider.acquire_async().await.unwrap();
    let source = KeysetSource {
        session,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let pages: Vec<PagedPage> = PageSourceStream::new(
        Box::new(source),
        page_size(2),
        budget(10, row_bytes * 2, 10),
    )
    .try_collect()
    .await
    .expect("a final page exactly fitting the byte budget completes");

    assert_eq!(pages.len(), 1);
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert_eq!(
        pages[0]
            .rows()
            .iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        row_bytes * 2
    );
}

#[tokio::test]
async fn keyset_lookahead_row_is_not_charged_to_published_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-lookahead.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3)").await;
    let row_bytes = one_row_bytes(&provider).await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = KeysetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let mut stream = PageSourceStream::new(
        Box::new(source),
        page_size(2),
        budget(10, row_bytes * 2, 10),
    );

    let page = stream
        .next()
        .await
        .expect("first page item")
        .expect("a page exactly fitting the byte budget is still published");
    assert_eq!(page_values(&page), vec![1, 2]);
    assert_eq!(
        page.rows()
            .iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        row_bytes * 2
    );

    let error = stream
        .next()
        .await
        .expect("the continuation at the exact byte bound fails closed")
        .expect_err("lookahead proves another page exists");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn keyset_exact_bound_without_a_lookahead_row_completes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-exact.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3), (4, 4)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = KeysetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(4, 65_536, 10))
            .try_collect()
            .await
            .expect("an exact bound with no continuation completes");

    assert_eq!(
        pages.iter().flat_map(page_values).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn keyset_exact_bound_with_a_continuation_fails_before_redispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paging-keyset-overflow.sqlite");
    let provider = seed(&path, "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)").await;
    let session = provider.acquire_async().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let source = KeysetSource {
        session,
        calls: Arc::clone(&calls),
    };
    let result: Result<Vec<PagedPage>, DbError> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(4, 65_536, 10))
            .try_collect()
            .await;

    assert_eq!(
        result
            .expect_err("a continuation at the exact row bound must fail closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
