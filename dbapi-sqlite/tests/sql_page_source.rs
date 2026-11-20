//! SQLite integration tests for the generic caller-authored SQL page source.
//!
//! These run the same plan as `examples/sql_page_source.rs`. Only SQLite is exercised; the other
//! provider SQL in the core documentation is a placeholder-shape example, not a live claim.

#[path = "../examples/support/events_fixture.rs"]
mod events_fixture;
#[path = "../examples/support/sql_keyset_plan.rs"]
mod sql_keyset_plan;

use std::sync::atomic::Ordering;

use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError, ErrorKind, Pool, Row, Value,
    paging::{
        PageSize,
        multi::{
            PageBudget, PageSourceStream, PagedPage,
            sql::{SqlPageAssertion, SqlPagePlan, SqlPageSource},
        },
    },
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

use events_fixture::{page_keys, seed};
use sql_keyset_plan::{KeysetPlan, Scope};

async fn make_provider() -> SqlitePoolProvider {
    let provider = SqlitePoolProvider::connect(SqliteConfig::new(":memory:").max_connections(1))
        .await
        .expect("in-memory SQLite");
    seed(&provider).await.expect("seed");
    provider
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn budget(rows: usize, bytes: usize, pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, pages).expect("valid page budget")
}

fn tenant_row(tenant: i64, seq: i64) -> Row {
    Row::new(vec![
        ("tenant".to_owned(), Value::I64(tenant)),
        ("seq".to_owned(), Value::I64(seq)),
    ])
    .expect("valid row")
}

async fn collect(
    provider: &SqlitePoolProvider,
    scope: Scope,
    start: (i64, i64),
    page_rows: usize,
    budget_rows: usize,
    source_bytes: usize,
) -> Result<(Vec<PagedPage>, usize), ErrorKind> {
    let session = provider.acquire_async().await.map_err(DbError::kind)?;
    let plan = KeysetPlan::new(scope, start).map_err(DbError::kind)?;
    let requests = plan.requests();
    let source = SqlPageSource::new(
        session,
        plan,
        source_bytes,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .map_err(DbError::kind)?;
    let pages = PageSourceStream::new(
        Box::new(source),
        page_size(page_rows),
        budget(budget_rows, 65_536, 10),
    )
    .try_collect()
    .await
    .map_err(DbError::kind)?;
    Ok((pages, requests.load(Ordering::Relaxed)))
}

fn keys(pages: &[PagedPage]) -> Vec<(i64, i64)> {
    pages
        .iter()
        .flat_map(|page| page_keys(page).expect("keys"))
        .collect()
}

#[tokio::test]
async fn generic_source_delivers_the_administrative_scan() {
    let provider = make_provider().await;
    let (pages, requests) = collect(&provider, Scope::AllTenants, (0, 0), 3, 100, 4_096)
        .await
        .expect("pages");

    assert_eq!(pages.len(), 3);
    assert_eq!(
        keys(&pages),
        vec![(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (3, 1), (3, 2)]
    );
    assert_eq!(requests, 3);
}

#[tokio::test]
async fn generic_source_keeps_the_authorized_tenant_separate_from_the_cursor() {
    let provider = make_provider().await;
    let (pages, requests) = collect(&provider, Scope::Tenant(2), (2, 0), 1, 100, 4_096)
        .await
        .expect("pages");

    assert_eq!(pages.len(), 2);
    assert_eq!(keys(&pages), vec![(2, 1), (2, 2)]);
    assert_eq!(requests, 2);
}

#[tokio::test]
async fn generic_source_fails_closed_at_an_exact_row_budget() {
    let provider = make_provider().await;
    let error = collect(&provider, Scope::AllTenants, (0, 0), 3, 3, 4_096)
        .await
        .expect_err("exact budget with more data");
    assert_eq!(error, ErrorKind::Overflow);
}

#[tokio::test]
async fn generic_source_rejects_a_too_small_source_byte_allowance() {
    let provider = make_provider().await;
    let error = collect(&provider, Scope::AllTenants, (0, 0), 3, 100, 8)
        .await
        .expect_err("byte allowance");
    assert_eq!(error, ErrorKind::Overflow);
}

#[test]
fn plan_rejects_a_start_outside_its_authorized_tenant() {
    let error = KeysetPlan::new(Scope::Tenant(1), (2, 0)).expect_err("out-of-scope start");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn plan_rejects_a_foreign_continuation() {
    let mut first = KeysetPlan::new(Scope::AllTenants, (0, 0)).expect("plan");
    let token = first
        .next_continuation(&[tenant_row(1, 1)], true)
        .expect("token")
        .expect("continuation");
    let second = KeysetPlan::new(Scope::AllTenants, (0, 0)).expect("plan");

    let error = second.resolve(Some(token)).expect_err("foreign token");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn plan_rejects_a_cursor_outside_its_authorized_tenant() {
    let mut plan = KeysetPlan::new(Scope::Tenant(1), (1, 0)).expect("plan");
    let token = plan
        .next_continuation(&[tenant_row(2, 1)], true)
        .expect("token")
        .expect("continuation");

    let error = plan.resolve(Some(token)).expect_err("out-of-scope cursor");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn plan_returns_no_continuation_without_a_lookahead() {
    let mut plan = KeysetPlan::new(Scope::AllTenants, (0, 0)).expect("plan");
    let continuation = plan
        .next_continuation(&[tenant_row(1, 1)], false)
        .expect("continuation");
    assert!(continuation.is_none());
}
