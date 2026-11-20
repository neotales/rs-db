//! SQLite integration tests for the reference multi-request keyset source.
//!
//! These run the same source that `examples/keyset_paging.rs` demonstrates. They are not a
//! PostgreSQL test and make no portability claim beyond the source's provider-neutral
//! [`Session`](neotales_dbapi::Session) use and the caller-authored statement.

#[path = "../examples/support/events_fixture.rs"]
mod events_fixture;
#[path = "../examples/support/keyset_source.rs"]
mod keyset_source;

use std::sync::atomic::Ordering;

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    ErrorKind, Pool, QueryOptions,
    paging::{
        PageSize,
        multi::{PageBudget, PageSourceStream, PagedPage},
    },
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

use events_fixture::{create_table, page_keys, seed};
use keyset_source::{KeysetSource, TenantScope};

const ADMIN_SQL: &str = "SELECT tenant, seq, body FROM events \
                         WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";

const TENANT_SQL: &str = "SELECT tenant, seq, body FROM events \
                          WHERE tenant = ? AND (tenant, seq) > (?, ?) \
                          ORDER BY tenant, seq LIMIT ?";

async fn make_provider(seeded: bool) -> SqlitePoolProvider {
    let provider = SqlitePoolProvider::connect(SqliteConfig::new(":memory:").max_connections(1))
        .await
        .expect("in-memory SQLite");
    if seeded {
        seed(&provider).await.expect("seed");
    } else {
        create_table(&provider).await.expect("table");
    }
    provider
}

async fn admin_source(provider: &SqlitePoolProvider, source_bytes: usize) -> KeysetSource {
    let session = provider.acquire_async().await.expect("session");
    KeysetSource::new(
        session,
        ADMIN_SQL,
        TenantScope::AllTenants,
        (0, 0),
        source_bytes,
    )
    .expect("source")
}

async fn tenant_source(
    provider: &SqlitePoolProvider,
    tenant: i64,
    source_bytes: usize,
) -> KeysetSource {
    let session = provider.acquire_async().await.expect("session");
    KeysetSource::new(
        session,
        TENANT_SQL,
        TenantScope::Tenant(tenant),
        (tenant, 0),
        source_bytes,
    )
    .expect("source")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("non-zero page size")
}

fn budget(rows: usize, bytes: usize, pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, pages).expect("budget")
}

fn page_bytes(page: &PagedPage) -> usize {
    page.rows()
        .iter()
        .map(|row| row.encoded_bytes().expect("encoded bytes"))
        .sum()
}

fn total_bytes(pages: &[PagedPage]) -> usize {
    pages.iter().map(page_bytes).sum()
}

fn all_keys(pages: &[PagedPage]) -> Vec<(i64, i64)> {
    pages
        .iter()
        .flat_map(|page| page_keys(page).expect("keys"))
        .collect()
}

#[tokio::test]
async fn keyset_pages_preserve_composite_order_without_duplicates() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();

    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 10))
            .try_collect()
            .await
            .expect("pages");

    assert_eq!(pages.len(), 3);
    assert_eq!(
        page_keys(&pages[0]).expect("keys"),
        vec![(1, 1), (1, 2), (1, 3)]
    );
    assert_eq!(
        page_keys(&pages[1]).expect("keys"),
        vec![(2, 1), (2, 2), (3, 1)]
    );
    assert_eq!(page_keys(&pages[2]).expect("keys"), vec![(3, 2)]);
    assert_eq!(all_keys(&pages).len(), 7);
    assert_eq!(
        requests.load(Ordering::Relaxed),
        3,
        "one request per page, not one per lookahead row"
    );
}

#[tokio::test]
async fn effective_page_size_respects_the_remaining_row_budget_and_exact_budget_fails_closed() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(3), budget(4, 65_536, 10));

    let first = stream.next().await.expect("first page").expect("page");
    assert_eq!(
        page_keys(&first).expect("keys"),
        vec![(1, 1), (1, 2), (1, 3)]
    );

    // Only one aggregate row remains, so the source asks for one published row plus one lookahead.
    let second = stream.next().await.expect("second page").expect("page");
    assert_eq!(page_keys(&second).expect("keys"), vec![(2, 1)]);

    // The source knows there is more, so a continuation exists at an exact bound. The pager
    // fails closed without dispatching a third request.
    let error = stream.next().await.expect("terminal").expect_err("bounded");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert!(stream.next().await.is_none());
    assert_eq!(requests.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn an_exact_data_and_row_budget_boundary_completes_without_an_extra_probe() {
    let provider = make_provider(true).await;
    // Six rows: exactly two pages of three after this delete.
    let mut session = provider.acquire_async().await.expect("session");
    session
        .execute_async(
            "DELETE FROM events WHERE tenant = 3 AND seq = 2",
            QueryOptions::new(),
        )
        .await
        .expect("delete");
    drop(session);

    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(6, 65_536, 10))
            .try_collect()
            .await
            .expect("exact data and budget must complete");
    assert_eq!(pages.len(), 2);
    assert_eq!(all_keys(&pages).len(), 6);
    assert_eq!(
        requests.load(Ordering::Relaxed),
        2,
        "the short lookahead proves completion without a third probe"
    );
}

#[tokio::test]
async fn an_exact_byte_budget_completes_when_the_data_finishes() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 10))
            .try_collect()
            .await
            .expect("pages");
    let bytes = total_bytes(&pages);

    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let bounded: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, bytes, 10))
            .try_collect()
            .await
            .expect("exact byte budget must complete");
    assert_eq!(bounded.len(), pages.len());
    assert_eq!(total_bytes(&bounded), bytes);
    assert_eq!(requests.load(Ordering::Relaxed), pages.len());
}

#[tokio::test]
async fn an_exact_byte_budget_with_more_data_fails_closed_without_redispatch() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let mut probe = PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 10));
    let first = probe.next().await.expect("first").expect("page");
    let first_bytes = page_bytes(&first);
    drop(probe);

    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let mut stream =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, first_bytes, 10));
    let first = stream.next().await.expect("first").expect("page");
    assert_eq!(
        page_keys(&first).expect("keys"),
        vec![(1, 1), (1, 2), (1, 3)]
    );
    let error = stream.next().await.expect("terminal").expect_err("bounded");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert!(stream.next().await.is_none());
    assert_eq!(
        requests.load(Ordering::Relaxed),
        1,
        "no request follows an exact byte budget with more data"
    );
}

#[tokio::test]
async fn an_exact_page_budget_completes_when_the_data_finishes() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 3))
            .try_collect()
            .await
            .expect("exact page budget must complete");
    assert_eq!(pages.len(), 3);
    assert_eq!(all_keys(&pages).len(), 7);
    assert_eq!(requests.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn an_exact_page_budget_with_more_data_fails_closed_without_redispatch() {
    let provider = make_provider(true).await;
    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 1));
    let first = stream.next().await.expect("first").expect("page");
    assert_eq!(
        page_keys(&first).expect("keys"),
        vec![(1, 1), (1, 2), (1, 3)]
    );
    let error = stream.next().await.expect("terminal").expect_err("bounded");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert!(stream.next().await.is_none());
    assert_eq!(requests.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn the_separate_source_byte_allowance_bounds_the_accepted_rows() {
    let provider = make_provider(true).await;
    // Smaller than one page, let alone the lookahead row: the source must not publish a page.
    let source = admin_source(&provider, 16).await;
    let pages = PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 10))
        .try_collect::<Vec<_>>()
        .await;
    let error = pages.expect_err("source byte allowance");
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[tokio::test]
async fn a_real_continuation_from_another_source_is_rejected() {
    let provider = make_provider(true).await;
    let first = admin_source(&provider, 4096).await;
    // A real continuation issued by a distinct source instance. The source is dropped only to
    // release the single in-memory connection before acquiring the second one.
    let token = first.issue((1, 1));
    drop(first);

    let second = admin_source(&provider, 4096).await;
    assert_eq!(
        second
            .resolve_after(Some(second.issue((1, 1))))
            .expect("own token"),
        (1, 1)
    );
    let error = second
        .resolve_after(Some(token))
        .expect_err("foreign token");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn a_tenant_scoped_scan_binds_the_tenant_separately_from_the_cursor() {
    let provider = make_provider(true).await;
    let source = tenant_source(&provider, 2, 4096).await;
    let requests = source.requests();
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(2), budget(20, 65_536, 10))
            .try_collect()
            .await
            .expect("tenant pages");
    assert_eq!(all_keys(&pages), vec![(2, 1), (2, 2)]);
    assert_eq!(requests.load(Ordering::Relaxed), 1);

    // A token from this source with a cursor outside its authorized tenant is rejected; the tenant
    // is an authorization predicate, not something the cursor can change.
    let source = tenant_source(&provider, 2, 4096).await;
    let token = source.issue((3, 1));
    let error = source
        .resolve_after(Some(token))
        .expect_err("out-of-scope cursor");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    assert_eq!(source.resolve_after(None).expect("start"), (2, 0));
}

#[tokio::test]
async fn a_tenant_source_rejects_a_start_outside_its_tenant() {
    let provider = make_provider(true).await;
    let session = provider.acquire_async().await.expect("session");
    let error = KeysetSource::new(session, TENANT_SQL, TenantScope::Tenant(2), (3, 0), 4096)
        .expect_err("out-of-scope start");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn an_empty_selection_yields_one_empty_page() {
    let provider = make_provider(false).await;
    let source = admin_source(&provider, 4096).await;
    let requests = source.requests();
    let pages: Vec<PagedPage> =
        PageSourceStream::new(Box::new(source), page_size(3), budget(20, 65_536, 10))
            .try_collect()
            .await
            .expect("pages");
    assert_eq!(pages.len(), 1);
    assert!(pages[0].is_empty());
    assert_eq!(requests.load(Ordering::Relaxed), 1);
}
