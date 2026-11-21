# neotales-migration-sqlite

Transactional SQLite migration backend over the driver-neutral `neotales_dbapi` SQLite contract.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration neotales-migration-sqlite
```

## What you can do 🔧

**Run migrations through either SQLite provider.** The default `sqlx-compat` feature keeps the
`SqlitePoolProvider` overloads; disable it to use `neotales-dbapi-sqlite-direct` with no `SQLx` in
the runtime graph.

```rust,no_run
use std::time::Duration;
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};
use neotales_migration::{Migration, Migrations};
use neotales_migration_sqlite::SqliteBackend;

async fn migrate() -> Result<(), Box<dyn std::error::Error>> {
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new("app.sqlite")
            .busy_timeout(Duration::from_millis(500))
            .max_connections(4),
    )
    .await?;
    let backend = SqliteBackend::new("migration_log", 500)?;
    let migrations = Migrations::new(vec![Migration::new(
        1,
        "create_users",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        Some("DROP TABLE users".to_owned()),
    )?])?;
    backend
        .up_with_pool(&provider, "users", &migrations, "application-v1", &["users"])
        .await?;
    Ok(())
}
```

**Choose the provider explicitly.** `up`, `down`, and `flatten` each acquire one exclusive session
and begin `BEGIN IMMEDIATE` on that same connection.

**Declare managed objects.** Pass the complete list; an unregistered module rejects existing
objects instead of adopting them.


## Scope

SQLite executor for `neotales-migration`, backed by the driver-neutral
`neotales_dbapi::SqlitePool` contract. The default `sqlx-compat` feature retains
the `SqlitePoolProvider` overloads; disable default features and call
`up_with_pool`, `down_with_pool`, and `flatten_with_pool` to use another SQLite
driver such as `neotales-dbapi-sqlite-direct` without `SQLx` in the runtime graph.

```rust,no_run
use std::time::Duration;
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};
use neotales_migration::{Migration, Migrations};
use neotales_migration_sqlite::SqliteBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
let database_path = std::env::temp_dir()
    .join(format!("neotales-migration-example-{}.sqlite", std::process::id()));
let provider = SqlitePoolProvider::connect(
    SqliteConfig::new(database_path.clone())
        .busy_timeout(Duration::from_millis(500))
        .max_connections(4),
).await?;
let backend = SqliteBackend::new("migration_log", 500)?;
let migrations = Migrations::new(vec![
    Migration::new(
        1,
        "create_users",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        Some("DROP TABLE users".to_owned()),
    )?,
])?;
backend.up_with_pool(&provider, "users", &migrations, "application-v1", &["users"]).await?;
drop(provider);
std::fs::remove_file(database_path)?;
Ok(())
}
```

The example uses the direct provider and neutral API, so it also works with
`--no-default-features`. The default `sqlx-compat` feature retains
`SqliteBackend::up`, `down`, and `flatten` overloads for
`neotales-dbapi-sqlite::SqlitePoolProvider`.

In this workspace the `SQLx`-free dependency selection is:

```toml
[dependencies]
neotales-migration-sqlite = { path = "../migration-sqlite", default-features = false }
neotales-dbapi-sqlite-direct = { path = "../dbapi-sqlite-direct" }
```

## Migrating from the `SQLx` API

The backend methods previously accepted `&sqlx::SqlitePool`; they now accept
`&SqlitePoolProvider`. Replace the `SQLx` pool construction with
`SqlitePoolProvider::connect(SqliteConfig::new(database_path)...)`, then pass
that provider to `up`, `down`, and `flatten`. `SQLx` remains a development
dependency for integration-test setup and inspection, not a normal dependency
or part of the migrator's public API.

Each operation acquires one exclusive provider session (which restores the
provider-configured timeout on checkout), applies the backend's `busy_timeout_ms`
override on that connection, begins `BEGIN IMMEDIATE` on the same session, and
commits or rolls back the whole operation. The provider's SQLite
progress handler interrupts virtual-machine execution and discards a connection
after cancellation or a dispatched statement failure; it cannot interrupt a
blocked user-defined function, VFS call, or busy handler. Lock waits are bounded
by `busy_timeout_ms`; prompt cancellation is not guaranteed. Discarding the last
connection to a private in-memory database can destroy that database.

Use trusted SQL definitions only: SQLite's trusted
transactional DDL/DML is assumed; scripts must not issue transaction control,
attach databases, or perform external/non-transactional side effects.
Scripts containing NUL are rejected before any planned script is executed:
`SQLx` 0.9 can otherwise spin in its multi-statement preparation loop without
invoking SQLite's progress callback.

A provider configured with `SqliteDurability::Durable` additionally rejects a
migration script containing `pragma`, `attach`, `detach`, or `load_extension`
before dispatch, and revalidates the connection before every statement and
commit. The backend's own `PRAGMA busy_timeout` statement remains allowed because
it is a single-statement pragma. A migration that needs those keywords or
arbitrary pragma access must use an inheriting provider; it cannot run under the
durable profile.

Pass an explicit managed-object list to `up`, `down`, and `flatten`; the constructor
does not take it. Fresh modules apply their baseline with `up`, not `flatten`.
For an unregistered module, these methods reject listed pre-existing main
objects rather than adopting schema. All three operations reject listed temporary
objects for all modules, because temporary objects can shadow main objects. Provide every
object that indicates an already initialized application. An empty list is only
safe for applications that truly have no managed objects and is not appropriate
for audit use. The history tables are constrained, safely quoted, and always
qualified in the main database.

History stores checksums, module identity (including modules with zero applied
migrations), binding fingerprint, baseline barrier, and expected applied count
and head. This detects missing history rows but does not protect against a
deliberate coordinated rewrite of the database. Older metadata without count
and head cannot be validated and is rejected; it must not be silently adopted.
`down` plans completely before executing SQL. Flatten accepts only the core
manifest's exact complete prior history and atomically replaces it; it does not
execute baseline SQL. A fresh module must apply the baseline with `up`; fresh
`flatten` is rejected even when another module has initialized the ledger.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
