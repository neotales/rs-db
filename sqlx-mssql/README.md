# neotales-sqlx-mssql

Fork of `sqlx-mssql-rs-core` 0.1.2, the core SQLx driver for Microsoft SQL Server built on
Microsoft's `mssql-tds` TDS implementation.

By [x0craft](https://github.com/x0craft).

Upstream by the sqlx-mssql-rs contributors. This fork is maintained by [x0craft](https://github.com/x0craft) for Neotales; upstream is not responsible for local changes.

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

This is a prerelease fork. The package is `neotales-sqlx-mssql`; the library name remains
`sqlx_mssql_rs_core`. Pin the exact version:

```sh
cargo add neotales-sqlx-mssql@=0.0.0-alpha.0
```

## What you can do 🔧

**Connect through SQLx and run a query.** The driver speaks TDS directly, so there is no ODBC
manager and no blocking bridge.

```rust,no_run
use sqlx_core::connection::Connection;
use sqlx_core::row::Row;
use sqlx_mssql_rs_core::MssqlConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = MssqlConnection::connect(
        "mssql://app:from-your-secret-store@db.example.com:1433/app?trust_certificate=true",
    )
    .await?;
    let row = sqlx_core::query::query("SELECT 1")
        .fetch_one(&mut conn)
        .await?;
    let value: i32 = row.try_get(0)?;
    assert_eq!(value, 1);
    conn.close().await?;
    Ok(())
}
```

**Use the fork through the supported provider.** `neotales-dbapi-mssql` owns the DBAPI traits,
restricted write grammar, session setup, and sanitized errors. This crate is its TDS transport.

**Bind native `@P1` names.** This fork sends the original statement unchanged and validates
`@P1`, `@P2`, ... references; it does not use the upstream `?` parameter form. The `?` examples in
the retained upstream sections below do not describe this fork.

**Know what this package contains.** It is the core driver only. The upstream CLI
(`sqlx-mssql-rs-cli`), the `full` example, and the upstream repository's test harness are not
packaged here. The sections below describe the upstream driver surface; where they mention the CLI,
the `full` example, or `?` parameters, treat them as upstream documentation rather than this
package.

## Upstream overview

## Minimal Query

```toml
[dependencies]
neotales-sqlx-mssql = "=0.0.0-alpha.0"
sqlx-core = { version = "0.9.0", default-features = false }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use sqlx_core::connection::Connection;
use sqlx_core::row::Row;
use sqlx_mssql_rs_core::MssqlConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = MssqlConnection::connect(
        "mssql://app:from-your-secret-store@db.example.com:1433/app?trust_certificate=true",
    ).await?;

    let row = sqlx_core::query::query("SELECT 1")
        .fetch_one(&mut conn)
        .await?;

    let value: i32 = row.try_get(0)?;
    println!("{value}");

    conn.close().await?;
    Ok(())
}
```

`MssqlConnection::connect()` accepts a standard `mssql://` URL:

```text
mssql://user:password@host:port/database?params
```

Port 1433 is assumed when omitted.

### URL query parameters

| Parameter | Description |
|---|---|
| `encrypt=` | `on` (default), `required`, `strict` (TDS 8.0) or `off` |
| `trust_certificate=true` | Skip server certificate validation |
| `server_certificate=...` | Path to a DER or PEM certificate to pin |
| `host_name_in_cert=...` | CN or SAN expected in the server certificate |
| `application_name=...` | Application name reported to the server |
| `connect_timeout=15` | Connection timeout, in seconds |
| `database=...` | Database name, overriding the URL path |

### Builder methods

```rust,no_run
use std::str::FromStr;
use std::time::Duration;
use sqlx_core::connection::ConnectOptions;
use sqlx_mssql_rs_core::{MssqlConnectOptions, MssqlEncryption};

let mut options = MssqlConnectOptions::from_str("mssql://localhost/testdb")?;
options.encrypt(true)
       .trust_certificate(true)
       .application_name("my-app")
       .connect_timeout(Duration::from_secs(15));
let conn = options.connect().await?;
```

`with_database()` returns a copy of the options pointing at a different
database; migrations use it to reach `master` when creating or dropping a
database.

`encryption()` sets a mode beyond on/off — `MssqlEncryption::Required` refuses a
plaintext connection and `MssqlEncryption::Strict` encrypts from the first byte,
for servers configured to demand TDS 8.0. `server_certificate()` pins a specific
certificate instead of relying on a trusted root, which is the alternative to
turning validation off with `trust_certificate()`:

```rust,no_run
options
    .encryption(MssqlEncryption::Strict)
    .server_certificate("/etc/ssl/server.pem");
```

## Encryption and Authentication

TLS is negotiated inside the TDS handshake, so there is no system TLS
configuration to manage and no certificate files to install. Connections request
encryption by default, matching the Microsoft ODBC driver's `Encrypt=yes`.
Setting `encrypt=false` prefers an unencrypted connection instead.

SQL logins work out of the box. Enable the `integrated-auth` feature for
Kerberos or NTLM authentication; on Unix the GSSAPI library is loaded at
runtime, so there is no build-time Kerberos dependency.

## Connection Pooling

```rust,no_run
use sqlx_mssql_rs_core::MssqlPoolOptions;

let pool = MssqlPoolOptions::new()
    .max_connections(10)
    .connect("mssql://app:from-your-secret-store@db.example.com:1433/app?trust_certificate=true")
    .await?;

let row = sqlx_core::query::query("SELECT 1")
    .fetch_one(&pool)
    .await?;
```

`MssqlPool` and `MssqlPoolOptions` are aliases for SQLx's generic `Pool` and
`PoolOptions` specialised to `Mssql`.

## Compile-time Checked Queries

The `query!()`, `query_as!()` and `query_scalar!()` macros verify your SQL
against a real database while compiling, using `DATABASE_URL` or an offline
cache in `.sqlx` (see `prepare` below).

### Parameter checking

The number of `?` markers is always checked against the number of arguments. The
driver also reports the SQL type of every parameter, so each bound value is
checked against the type the server inferred:

```rust,no_run
// `users.id` is `uniqueidentifier`, so this is a compile error:
//   expected `Uuid`, found `&str`
sqlx_mssql_rs_core::query!("SELECT description FROM users WHERE id = ?", "not-a-uuid");
```

Types that do not resolve to exactly one Rust type are reported as a count
instead, so only the argument count is checked for those. That covers the exact
numeric types (`decimal`, `money`) and the feature-gated types (`uuid`,
`chrono`, `time`) when their feature is not enabled.

An explicit cast skips the check for that argument, which is the escape hatch
when a valid binding is not the type the server inferred:

```rust,no_run
sqlx_mssql_rs_core::query!("SELECT id FROM users WHERE amount > ?", amount as f64);
```

The driver type-erases the futures it creates internally, so using the query
builders or these macros does not push a dependent crate past the compiler's
default recursion limit. If the compiler reports "queries overflow the depth
limit" while pinned to an older release, `#![recursion_limit = "512"]` is the
fix there.

## Features

| Feature | Default | Description |
|---|---|---|
| `macros` | yes | `query!()`, `query_as!()`, `query_scalar!()` and friends |
| `derive` | yes | `Encode`, `Decode`, `Type`, `FromRow` derive macros |
| `migrate` | yes | Migration support |
| `offline` | yes | Compile-time checking against a `.sqlx` cache |
| `sqlx-toml` | no | `sqlx.toml`: type overrides and preferred crates |
| `bigdecimal` | no | `BigDecimal` type support |
| `bstr` | no | `BString` / `BStr` support |
| `chrono` | no | `chrono` datetime types |
| `time` | no | `time` datetime types |
| `jiff` | no | `jiff` datetime types |
| `decimal` / `rust_decimal` | no | `Decimal` type support |
| `json` | no | `serde_json::Value` support |
| `uuid` | no | `uuid::Uuid` support |
| `spatial` | no | SQL Server `geometry` / `geography` support via `geo-types` |
| `spatial-serde` | no | `Serialize` / `Deserialize` for `MssqlGeometry` and `MssqlGeography` |
| `any` | no | `AnyConnection` support (used by the CLI) |
| `integrated-auth` | no | Kerberos / NTLM authentication |

There are no `runtime-*` or `tls-*` features: the driver is Tokio-only and TLS
lives inside the TDS handshake.

A `sqlx.toml` requires the `sqlx-toml` feature. Without it the query macros stop
with "SQLx found config file ... but the `sqlx-toml` feature was not enabled".

`bstr` is a value-level convenience: `BString`/`BStr` conversions delegate to the
driver's `Vec<u8>`/`&[u8]` impls, and the compile-time type checks still report a
`varbinary` parameter as `&[u8]`.

`jiff` has no section in `sqlx-macros-core`'s type-checking tables, unlike
`chrono` and `time`, so its types are matched directly. Enabling `jiff` therefore
takes precedence over `chrono` and `time` when the query macros infer a date or
time type.

## Spatial Types

SQL Server's `geometry` and `geography` are CLR UDTs whose payload is the
server's own serialization, not WKB. With the `spatial` feature the driver
decodes and encodes that serialization natively through two wrappers that keep
the SRID, which `geo_types::Geometry` cannot carry:

```rust,no_run
use geo_types::{Geometry, Point};
use sqlx_mssql_rs_core::{MssqlGeography, MssqlGeometry};

let point = Geometry::Point(Point::new(1.0, 2.0));

// INSERT INTO places (location) VALUES (?)
query("INSERT INTO places (location) VALUES (?)")
    .bind(MssqlGeometry::new(point.clone(), 4326))
    .execute(&mut conn)
    .await?;

// SELECT location FROM places
let stored: MssqlGeometry = row.try_get("location")?;
assert_eq!(stored.srid(), 4326);
```

`geo_types::Geometry<f64>` itself maps to WKB held in a `varbinary` column, not to
a spatial column. Select `.STAsBinary()` to read a spatial value that way. Only
two-dimensional, non-curved shapes are supported, since `geo-types` has no
representation for Z/M coordinates or curves.

With the `spatial-serde` feature both wrappers are `Serialize` and
`Deserialize`, so they can live in `serde`-derived structs:

```json
{"srid":4326,"geometry":{"Point":{"x":1.0,"y":2.0}}}
```

That shape is `geo-types`' derived representation, not GeoJSON or WKB; it is
meant for transport within your own system. Serialize `geometry()` with the
`geojson` crate if you need GeoJSON.

## CLI (`sqlx-mssql`)

A thin wrapper around `sqlx-cli` for managing MSSQL databases, running
migrations, and preparing offline query data.

### Install

```sh
cargo install --git https://github.com/strawberyy-coconut/sqlx-mssql-rs.git \
  sqlx-mssql-rs-cli --locked
```

`--locked` builds against the committed `Cargo.lock`, so dependency versions —
including the pinned `mssql-tds` git revision — are reproduced exactly. Add
`--branch`, `--tag` or `--rev` to install from somewhere other than the default
branch.

This installs two binaries:

- `sqlx-mssql` — a standalone command
- `cargo-sqlx-mssql` — the same tool as a cargo subcommand

After installation, both are available on your `PATH`.

### Usage

All standard `sqlx-cli` subcommands are supported. Provide your database URL
via `--database-url` or the `DATABASE_URL` environment variable (or a `.env`
file; pass `--no-dotenv` to disable that).

```sh
# Create / drop the database
sqlx-mssql database create
sqlx-mssql database drop -y

# Create and run migrations
sqlx-mssql migrate add <name>
sqlx-mssql migrate run

# Revert the last migration
sqlx-mssql migrate revert

# List migration status
sqlx-mssql migrate info

# Prepare offline query data (for compile-time checked queries)
sqlx-mssql prepare
```

The same commands work through cargo:

```sh
cargo sqlx-mssql migrate run
```

`database drop` and `database reset` prompt for confirmation; pass `-y` to skip
the prompt in scripts.

**Environment variable** (add to `.env` in your project root):

```text
DATABASE_URL=mssql://app:from-your-secret-store@127.0.0.1:1433/app?trust_certificate=true
```

Or use the `--database-url` flag:

```sh
sqlx-mssql migrate run --database-url mssql://app:from-your-secret-store@127.0.0.1:1433/app
```

### Run without installing

From a clone of this repository, the CLI can be run straight from source instead
of being installed:

```sh
cargo run -p sqlx-mssql-rs-cli -- migrate run
```

## Example

A commented walk-through lives in `examples/full`:

```sh
cargo run -p full
```

It connects, runs migrations, binds parameters, reads rows back, commits,
rolls back, and exercises each of the query macros. It reads
`MSSQL_DATABASE_URL` first and falls back to `DATABASE_URL`.

## Running Tests

```sh
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=MyPass" \
  -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest

DATABASE_URL="mssql://sa:MyPass@localhost:1433/master?trust_certificate=true" \
  cargo test --workspace
```

The integration tests create tables under unique names and drop them again, so
the suite is safe to run against a shared server. If `DATABASE_URL` is unset it
defaults to `mssql://sa:Password1!@localhost:1433/master?trust_certificate=true`.

Some tests are gated behind type features (`chrono`, `time`, `bstr`), so run the
whole suite with everything enabled:

```sh
DATABASE_URL="mssql://sa:MyPass@localhost:1433/master?trust_certificate=true" \
  cargo test --workspace --all-features
```
## Local fork notice

This is the upstream README retained for provenance. Its `?` placeholder examples
do not apply to this fork; see [NEOTALES.md](NEOTALES.md).

## Licence 📜

Apache-2.0, retained from upstream. See [LICENSE-APACHE](LICENSE-APACHE) and
[NEOTALES.md](NEOTALES.md) for the exact local delta.
