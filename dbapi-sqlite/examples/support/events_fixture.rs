//! Shared SQLite fixture for the keyset paging examples and tests.
//!
//! The source and plan examples are provider-neutral over a DBAPI `Session`; this file only creates
//! the SQLite `events` table they query. `(tenant, seq)` is `NOT NULL` and unique, so the keyset
//! order is total.

use neotales_dbapi::{
    DbError, DbType, Parameter, Pool, QueryOptions, Value, paging::multi::PagedPage,
};
use neotales_dbapi_sqlite::SqlitePoolProvider;

/// Creates the example table.
pub(crate) async fn create_table(provider: &SqlitePoolProvider) -> Result<(), DbError> {
    let mut session = provider.acquire_async().await?;
    session
        .execute_async(
            "CREATE TABLE events (\
                 tenant INTEGER NOT NULL, \
                 seq INTEGER NOT NULL, \
                 body TEXT NOT NULL, \
                 PRIMARY KEY (tenant, seq))",
            QueryOptions::new(),
        )
        .await?;
    Ok(())
}

/// Creates and seeds the example table with seven rows across three tenants.
pub(crate) async fn seed(provider: &SqlitePoolProvider) -> Result<(), DbError> {
    create_table(provider).await?;
    let mut session = provider.acquire_async().await?;
    for (tenant, seq, body) in [
        (1, 1, "alpha"),
        (1, 2, "bravo"),
        (1, 3, "charlie"),
        (2, 1, "delta"),
        (2, 2, "echo"),
        (3, 1, "foxtrot"),
        (3, 2, "golf"),
    ] {
        let options = QueryOptions::new()
            .bind(i64_param(tenant)?)
            .bind(i64_param(seq)?)
            .bind(Parameter::new(DbType::Text, Value::Text(body.to_owned()))?);
        session
            .execute_async(
                "INSERT INTO events (tenant, seq, body) VALUES (?, ?, ?)",
                options,
            )
            .await?;
    }
    Ok(())
}

/// Returns the composite key of each row in a delivered page.
pub(crate) fn page_keys(page: &PagedPage) -> Result<Vec<(i64, i64)>, DbError> {
    page.rows()
        .iter()
        .map(|row| Ok((row.read::<i64>("tenant")?, row.read::<i64>("seq")?)))
        .collect()
}

fn i64_param(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}
