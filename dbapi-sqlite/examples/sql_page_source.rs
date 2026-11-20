//! Runnable SQLite example for the generic caller-authored SQL page source.
//!
//! Run it with `cargo run -p neotales-dbapi-sqlite --example sql_page_source`.
//!
//! The plan in `support/sql_keyset_plan.rs` authors SQLite `?` placeholders and a tenant predicate
//! separate from the cursor. The adapter supplies the hard per-request row and byte bound, removes
//! the lookahead row, and rejects a continuation that disagrees with the lookahead.
//!
//! This example is tested only against SQLite. No PostgreSQL, MySQL, MariaDB, SQL Server, libSQL,
//! or `ClickHouse` run is claimed.

#[path = "support/events_fixture.rs"]
mod events_fixture;
#[path = "support/sql_keyset_plan.rs"]
mod sql_keyset_plan;

use std::sync::atomic::Ordering;

use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError, Pool,
    paging::{
        PageSize,
        multi::{
            PageBudget, PageSourceStream, PagedPage,
            sql::{SqlPageAssertion, SqlPageSource},
        },
    },
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

use events_fixture::page_keys;
use sql_keyset_plan::{KeysetPlan, Scope};

async fn collect(
    provider: &SqlitePoolProvider,
    scope: Scope,
    start: (i64, i64),
    page_rows: usize,
) -> Result<(Vec<PagedPage>, usize), DbError> {
    let session = provider.acquire_async().await?;
    let plan = KeysetPlan::new(scope, start)?;
    let requests = plan.requests();
    let source = SqlPageSource::new(
        session,
        plan,
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )?;
    let pages = PageSourceStream::new(
        Box::new(source),
        PageSize::try_from(page_rows)?,
        PageBudget::new(100, 65_536, 10)?,
    )
    .try_collect()
    .await?;
    Ok((pages, requests.load(Ordering::Relaxed)))
}

fn print_pages(title: &str, pages: &[PagedPage], requests: usize) -> Result<(), DbError> {
    println!("{title}");
    let mut total = 0;
    for page in pages {
        let keys = page_keys(page)?;
        total += keys.len();
        println!("  page {}: {keys:?}", page.page_index());
    }
    println!("  {total} rows in {requests} requests");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let provider =
        SqlitePoolProvider::connect(SqliteConfig::new(":memory:").max_connections(1)).await?;
    events_fixture::seed(&provider).await?;

    let (admin, admin_requests) = collect(&provider, Scope::AllTenants, (0, 0), 3).await?;
    print_pages(
        "cross-tenant administrative scan (requires authorization for every tenant)",
        &admin,
        admin_requests,
    )?;
    assert_eq!(
        admin
            .iter()
            .map(|page| page_keys(page).expect("keys").len())
            .sum::<usize>(),
        7,
        "the administrative scan delivers every tenant once"
    );

    let (tenant, tenant_requests) = collect(&provider, Scope::Tenant(2), (2, 0), 1).await?;
    print_pages(
        "tenant 2 scan (authorized tenant bound separately, not the cursor)",
        &tenant,
        tenant_requests,
    )?;
    let tenant_keys: Vec<(i64, i64)> = tenant
        .iter()
        .flat_map(|page| page_keys(page).expect("keys"))
        .collect();
    assert!(
        tenant_keys.iter().all(|(tenant, _)| *tenant == 2),
        "the authorization predicate restricts every row to tenant 2"
    );
    assert_eq!(tenant_keys.len(), 2);
    Ok(())
}
