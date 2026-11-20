//! Worked SQLite keyset paging over the provider-neutral multi-request pager.
//!
//! Run it with `cargo run -p neotales-dbapi-sqlite --example keyset_paging`.
//!
//! This is an example, not a built-in source: `neotales-dbapi` ships no SQL `PageSource`. The
//! reference source in `support/keyset_source.rs` is compiled by this example and by
//! `tests/keyset_paging.rs`, so the documented code is the tested code.
//!
//! The first run is a **cross-tenant administrative scan**. It returns every tenant's rows and the
//! caller must be authorized for all of them:
//!
//! ```sql
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?
//! ```
//!
//! The second run is **tenant-scoped**. The authorized tenant is a separate predicate and parameter,
//! never taken from the continuation cursor:
//!
//! ```sql
//! SELECT tenant, seq, body FROM events
//! WHERE tenant = ? AND (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?
//! ```
//!
//! SQLite uses `?` placeholders. PostgreSQL uses `$1`, `$2`, `$3` (and `$4` for the scoped form) in
//! the same positions; nothing in the pager rewrites SQL, so the caller writes the provider's form.
//! The ordering key must be unique and `NOT NULL`; this table uses a `(tenant, seq)` primary key.
//! This example is tested only against SQLite; no live PostgreSQL test is claimed.
//!
//! The source publishes at most `min(page_size, remaining_rows)` rows per request, fetches one extra
//! lookahead row, and bounds the portable encoded bytes of the rows it accepts with a separate
//! `source_bytes` allowance (not a provider allocation or buffering bound). It does not retry,
//! replay, prefetch, or promise a snapshot; read-only and replay safety are the caller's
//! precondition.

#[path = "support/events_fixture.rs"]
mod events_fixture;
#[path = "support/keyset_source.rs"]
mod keyset_source;

use std::sync::atomic::Ordering;

use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError, Pool,
    paging::{
        PageSize,
        multi::{PageBudget, PageSourceStream, PagedPage},
    },
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

use events_fixture::page_keys;
use keyset_source::{KeysetSource, TenantScope};

const ADMIN_SQL: &str = "SELECT tenant, seq, body FROM events \
                         WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";

const TENANT_SQL: &str = "SELECT tenant, seq, body FROM events \
                          WHERE tenant = ? AND (tenant, seq) > (?, ?) \
                          ORDER BY tenant, seq LIMIT ?";

async fn collect(
    provider: &SqlitePoolProvider,
    sql: &str,
    scope: TenantScope,
    start: (i64, i64),
    page_rows: usize,
) -> Result<(Vec<PagedPage>, usize), DbError> {
    let session = provider.acquire_async().await?;
    let source = KeysetSource::new(session, sql, scope, start, 4096)?;
    let requests = source.requests();
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

    let (admin, admin_requests) =
        collect(&provider, ADMIN_SQL, TenantScope::AllTenants, (0, 0), 3).await?;
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

    let (tenant, tenant_requests) =
        collect(&provider, TENANT_SQL, TenantScope::Tenant(2), (2, 0), 1).await?;
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
