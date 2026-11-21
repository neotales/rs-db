//! Open a real SQLite database, create a table, write a row, and read it back.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p neotales-dbapi-sqlite --example connect_and_query
//! ```
//!
//! This is the runnable version of the connection sample in the
//! `neotales-dbapi` README. The `neotales-dbapi` crate cannot depend on a
//! provider without inverting the dependency, so the provider package carries
//! the executable copy.

use neotales_dbapi::{DbError, DbType, Parameter, QueryOptions, RowLimit, Value};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let path = std::env::temp_dir().join(format!(
        "neotales-dbapi-connect-and-query-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(&path)
            .busy_timeout(std::time::Duration::from_secs(2))
            .max_connections(2),
    )
    .await?;

    let mut session = provider.acquire_sqlite().await?;
    session
        .execute_async(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL)",
            QueryOptions::new(),
        )
        .await?;

    let insert = [
        Parameter::new(DbType::I64, Value::I64(7))?,
        Parameter::new(DbType::Text, Value::Text("Ada".to_owned()))?,
        // SQLite has no boolean storage class; bind the flag as integer 0/1.
        Parameter::new(DbType::Bool, Value::Bool(true))?,
    ];
    session
        .execute_async(
            "INSERT INTO users (id, name, active) VALUES (?, ?, ?)",
            QueryOptions::new().bind_all(&insert),
        )
        .await?;

    let lookup = [Parameter::new(DbType::I64, Value::I64(7))?];
    let row = session
        .fetch_one_async(
            "SELECT name, active FROM users WHERE id = ?",
            QueryOptions::new()
                .bind_all(&lookup)
                .limit(RowLimit::new(1, 256)?),
        )
        .await?;

    let name: String = row.read("name")?;
    // A fetched SQLite integer is I64, not Bool. Decode it as an integer and
    // compare; asking for bool would be a WrongType error.
    let active: i64 = row.read("active")?;
    assert_eq!(name, "Ada");
    assert_eq!(active, 1);
    println!("{name} is active={active}");

    drop(session);
    drop(provider);
    let _ = std::fs::remove_file(&path);
    Ok(())
}
