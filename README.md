# rs-db

Driver-neutral SQL execution, providers for several database families, and validated
migration backends.

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

By [x0craft](https://github.com/x0craft).

<!-- badges: crates.io, docs.rs, CI. Add once the crates have a first version. -->

This repository is a virtual Cargo workspace. Every shippable crate has its own
top-level directory and shares version `0.0.0-alpha.0`.

## What you can do 🔧

**Use one SQL contract across database families.** A provider owns the connection, types, and
cleanup; application code uses the same traits. Add the contract and one provider:

```sh
cargo add neotales-dbapi neotales-dbapi-sqlite
cargo add tokio --features macros,rt-multi-thread
```

**Open a real SQLite session and read a typed row.** This is a complete program, compiled and run
by CI as `dbapi-sqlite/examples/connect_and_query.rs`:

```rust
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), neotales_dbapi::DbError> {
    let provider = SqlitePoolProvider::connect(SqliteConfig::new("app.sqlite")).await?;
    let mut session = provider.acquire_sqlite().await?;
    session
        .execute_async(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            QueryOptions::new(),
        )
        .await?;
    let insert = [
        Parameter::new(DbType::I64, Value::I64(7))?,
        Parameter::new(DbType::Text, Value::Text("Ada".to_owned()))?,
    ];
    session
        .execute_async(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&insert),
        )
        .await?;
    let lookup = [Parameter::new(DbType::I64, Value::I64(7))?];
    let row = session
        .fetch_one_async(
            "SELECT name FROM users WHERE id = ?",
            QueryOptions::new().bind_all(&lookup).limit(RowLimit::new(1, 256)?),
        )
        .await?;
    let name: String = row.read("name")?;
    assert_eq!(name, "Ada");
    Ok(())
}
```

Run it:

```sh
cargo run -p neotales-dbapi-sqlite --example connect_and_query
```

**Wrap policy around any pool.** `neotales_dbapi::PolicyPool` adds opt-in telemetry, SQL capture,
and replay-safe retries without changing the provider. SQL capture is off by default because
literals and comments can contain secrets.

## Crates

| Crate | Directory | What it does |
| --- | --- | --- |
| `neotales-dbapi` | `dbapi/` | Driver-neutral SQL execution with asynchronous and synchronous row access |
| `neotales-dbapi-clickhouse` | `dbapi-clickhouse/` | Native-protocol ClickHouse provider |
| `neotales-dbapi-d1` | `dbapi-d1/` | Experimental fail-closed Cloudflare D1 operations |
| `neotales-dbapi-libsql` | `dbapi-libsql/` | Remote libSQL/Turso provider |
| `neotales-dbapi-mariadb` | `dbapi-mariadb/` | MariaDB provider |
| `neotales-dbapi-mssql` | `dbapi-mssql/` | SQL Server provider |
| `neotales-dbapi-mssql-direct` | `dbapi-mssql-direct/` | Experimental direct TDS SQL Server provider, not published |
| `neotales-dbapi-mysql` | `dbapi-mysql/` | MySQL provider |
| `neotales-dbapi-postgres` | `dbapi-postgres/` | PostgreSQL provider |
| `neotales-dbapi-postgres-tls` | `dbapi-postgres-tls/` | Verified TLS adapter for the isolated PostgreSQL driver |
| `neotales-dbapi-sqlite` | `dbapi-sqlite/` | SQLite provider |
| `neotales-dbapi-sqlite-direct` | `dbapi-sqlite-direct/` | SQLite provider built directly on `rusqlite` |
| `neotales-migration` | `migration/` | Driver-free validated migration planning |
| `neotales-migration-clickhouse` | `migration-clickhouse/` | Explicit-recovery ClickHouse migrations |
| `neotales-migration-libsql` | `migration-libsql/` | Transactional remote libSQL/Turso migrations |
| `neotales-migration-mariadb` | `migration-mariadb/` | Explicit-recovery MariaDB migrations |
| `neotales-migration-mssql` | `migration-mssql/` | Transactional SQL Server migrations |
| `neotales-migration-mysql` | `migration-mysql/` | Explicit-recovery MySQL migrations |
| `neotales-migration-postgres` | `migration-postgres/` | Locked transactional PostgreSQL migrations |
| `neotales-migration-sqlite` | `migration-sqlite/` | Transactional SQLite migrations |

Transport forks ship from their own top-level directories so the
providers can be packaged:

| Crate | Directory | Origin |
| --- | --- | --- |
| `neotales-klickhouse` | `klickhouse/` | Fork of `klickhouse` 0.16.0 |
| `neotales-libsql` | `libsql/` | Remote-only fork of `libsql` 0.10.0 |
| `neotales-tokio-postgres` | `tokio-postgres/` | Fork of `tokio-postgres` 0.7.18 |
| `neotales-sqlx-mssql` | `sqlx-mssql/` | Fork of `sqlx-mssql-rs-core` 0.1.2 |

The fork READMEs and `PROVENANCE.md`/`FORK.md` files name the exact upstream
release, the local delta and the retained licence text.

`neotales-dbapi-mssql-direct` is intentionally `publish = false`. Its quiet
vendored roots live under `dbapi-mssql-direct/vendor/` and are covered by the
sealed inventory in `vendor/SHA256SUMS`.

## Build and test

The `check` job in `.github/workflows/ci.yml` is the source of truth for the
package list. Each gate takes one or more `-p` flags:

```sh
cargo fmt -p neotales-dbapi -p neotales-migration -- --check
cargo clippy -p neotales-dbapi --all-targets --no-deps -- -D warnings
cargo test -p neotales-dbapi --all-targets
eng/test-doctests.sh all
cargo doc -p neotales-dbapi --no-deps
```

CI names every fixture-free package explicitly instead of passing
`--workspace`. A workspace-wide test would also run the PostgreSQL server tests
without their fixture, and would compile the vendored quiet roots' own test
targets. The live lanes start their own pinned containers.

## Backend integration tests

Live tests are opt-in and `#[ignore]`d. Each live lane runs the exact ignored
test through a script, so an empty or renamed selection cannot be reported as a
pass. The scripts start digest-pinned containers through `testcontainers` or
Docker, generate disposable passwords with the OS CSPRNG, publish no host ports
where the client can reach the container network, and remove only the container
they created.

No credential is stored in this repository. A live lane either generates its
own password or reads a documented `NEOTALES_*_LIVE` opt-in. The fixtures are
not part of any published package: `dbapi-d1/emulator` and
`tokio-postgres/conformance` are detached workspaces, and the D1 emulator is
excluded from the `neotales-dbapi-d1` package.

The local D1 binding diagnostic needs an unprivileged user namespace for its
loopback-only network guard. It passes on a host that allows one and is blocked
on GitHub-hosted runners, which deny the UID mapping. CI warns and skips that
fixture rather than reporting a pass; the host and wasm D1 tests still run.

## Publish order

All publishable crates share one version and one tag. Publish in dependency
order: the transport forks first, then `neotales-dbapi` and
`neotales-migration`, then the providers, then the migration backends.
`RELEASE.md` has the exact list and the first-publish procedure.

## Licence 📜

[MIT](LICENSE.md). See [LICENSE.md](LICENSE.md). The forks retain their upstream licences:
`klickhouse` and `tokio-postgres` are MIT OR Apache-2.0, `neotales-libsql` is MIT, and
`neotales-sqlx-mssql` is Apache-2.0.
