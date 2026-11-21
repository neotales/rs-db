//! Open a durable SQLite pool, run an immediate transaction, and read a count.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p neotales-dbapi-sqlite --example durable_transaction
//! ```

use neotales_dbapi::{DbError, QueryOptions, RowLimit};
use neotales_dbapi_sqlite::{SqliteConfig, SqliteDurability, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let path = std::env::temp_dir().join(format!(
        "neotales-dbapi-durable-transaction-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(std::time::Duration::from_secs(2))
            .max_connections(4)
            .durability(SqliteDurability::Durable),
    )
    .await?;

    let mut session = provider.acquire_sqlite().await?;
    let mut transaction = session.begin_immediate_async().await?;
    transaction
        .execute_async(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, event TEXT NOT NULL)",
            QueryOptions::new(),
        )
        .await?;
    transaction.commit_async().await?;

    let row = session
        .fetch_one_async(
            "SELECT count(*) AS n FROM audit",
            QueryOptions::new().limit(RowLimit::new(1, 64)?),
        )
        .await?;
    let count: i64 = row.read("n")?;
    assert_eq!(count, 0);
    println!("{count} audit rows");

    drop(session);
    drop(provider);
    let _ = std::fs::remove_file(&path);
    Ok(())
}
