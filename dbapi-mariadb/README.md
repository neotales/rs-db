# neotales-dbapi-mariadb

MariaDB 11.4 provider for `neotales-dbapi`, sharing the reviewed MySQL wire implementation.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi neotales-dbapi-mariadb
```

## What you can do 🔧

**Connect with verified TLS and query with native `?` parameters.**

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_mariadb::{MariaDbConfig, MariaDbPoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = MariaDbPoolProvider::connect(
        MariaDbConfig::new("db.example.com", 3306, "app", "app")
            .password("from-your-secret-store")
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(3)),
    )
    .await?;
    let params = [Parameter::new(DbType::I64, Value::I64(7))?];
    let rows = provider
        .fetch_all_async(
            "SELECT name FROM users WHERE id = ?",
            QueryOptions::new().bind_all(&params).limit(RowLimit::new(10, 4096)?),
        )
        .await?;
    let _ = rows;
    Ok(())
}
```

**Get a concrete session when you own a named lock.** `acquire_mariadb_session` returns a session
with `discard()`, so an unreleased `GET_LOCK` closes its physical connection instead of returning
to the pool.

**Keep MariaDB and MySQL separate.** This crate is tested against MariaDB 11.4 only; MySQL has its
own provider and test lane.


## Scope

MariaDB implementation of the driver-neutral
[`neotales-dbapi`](https://docs.rs/neotales-dbapi) contract. The public API does
not expose `SQLx` or MySQL-driver types.

## Scope and reuse decision

This crate is a MariaDB-named facade over the reviewed MySQL wire implementation
in `neotales-dbapi-mysql`. Reuse was chosen only after running the full DBAPI
contract against MariaDB 11.4; the MySQL crate still claims only MySQL 8.4, and
its guarantees are not relabelled here without this separate live lane.

The wire protocol, `?` placeholders, quoting-comment lexer, autocommit
establishment, SQL-mode rejection, implicit-DDL-commit rejection, transaction
options, TLS modes, error sanitization, row limits, and policy-pool behaviour are
shared. MariaDB-specific behaviour covered by the lane:

- `mysql_native_password` authentication, including the disposable-image root
  account;
- MariaDB's default SQL mode (`NO_AUTO_CREATE_USER` and no `ANSI_QUOTES` or
  `NO_BACKSLASH_ESCAPES`) and rejection of sessions that inherit an unsafe mode;
- MariaDB error numbers for duplicate/foreign-key/not-null/check/syntax/lock
  failures, including `SQLx`'s MariaDB `4025` check-constraint mapping;
- `TINYINT(1)` as integer `I64`, `DOUBLE` as `F64`, text/blob types, `YEAR`,
  `BIT`, unsigned overflow, `SET` as `Text`, and unsupported `DECIMAL`/`DATE`
  values failing with `Decode`;
- MariaDB executable comments (`/*M! ... */`, and the lowercase `/*m!` case
  rejected conservatively) are rejected on every statement and script path;
- MariaDB `JSON` and `UUID` are reported through the binary wire type and are
  returned as `Bytes`, not as native JSON/UUID or `Text` types;
- `INSERT ... RETURNING`/`DELETE ... RETURNING` through `fetch_async` remains a write
  path for unknown-outcome purposes;
- transactional DML for `InnoDB`, and rejection of DDL/administrative commands
  inside a transaction because MariaDB implicitly commits them;
- read-committed and read-only transaction behaviour.

MariaDB version coverage is MariaDB 11.4 (the digest-pinned LTS image). No claim
is made for MariaDB 10.x, MySQL, or other MySQL-compatible servers. A type or
statement this lane does not prove is either rejected by the shared provider or
fails closed with `Unsupported`/`Decode`.

## TLS

[`MariaDbConfig::new`] requires TLS with certificate and host-name verification
against built-in `WebPKI` roots. Use [`MariaDbConfig::ca_file`] for a private CA.
[`MariaDbTls::Insecure`] encrypts without verification and is an explicit
downgrade. [`MariaDbConfig::plaintext_loopback`] is the only cleartext
constructor, and it accepts numeric loopback hosts only. There is no silent
fallback from a requested TLS mode.

Statement and slow-statement logging are disabled by the inner provider so SQL
text and embedded literals are not emitted. Pool lifecycle logging remains at
 defaults; it does not include SQL or bound values.

## Trusted SQL, transactions, and errors

Every statement and trusted script is caller-supplied trusted SQL. Stored
procedures, prepared statements, triggers, functions, events, views, generated
columns, and UDFs are trusted database objects and can hide DDL, commits, or
external effects. `CALL`, `EXECUTE`, and `PREPARE` are rejected, but an allowed
DML statement can still invoke a trusted trigger or function.

The provider rejects transaction-control statements on standalone APIs and
leading DDL/administrative commands inside a transaction with `Unsupported`
before dispatch. Supported transaction options are all four portable isolation
levels and read-only/read-write mode. Every acquired connection is set to
`autocommit=1` and read back; inherited unsafe SQL modes are rejected.

Errors retain only a stable `ErrorKind`; SQL, values, credentials, host names,
and driver messages are never attached. Constraint, busy/lock, cancellation,
timeout, and transport categories are mapped from the shared MySQL mapping and
the MariaDB-specific tests above.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. MariaDB shares the MySQL `?` placeholders and row-limit behavior; the adapter
supplies a hard row/byte bound for the page plus one lookahead row, rejects extra result sets, and
does not rewrite or classify SQL. The caller supplies the read-only/replay-safe/authorized assertion
and the scoped continuation.

The ignored live test `mariadb_sql_page_source_contract` in `tests/mariadb.rs` exercises this
statement against the crate's digest-pinned MariaDB 11.4 container. It covers the cross-tenant and
single-tenant scans, typed parameters, one-row lookahead, exact page order, and request counts.
`cargo test` stays fixture-free; select only this test with:

```sh
cargo test -p neotales-dbapi-mariadb --test mariadb -- --ignored --exact mariadb_sql_page_source_contract
```

```sql
SELECT tenant, seq, body FROM events
WHERE tenant = ? AND (tenant, seq) > (?, ?)
ORDER BY tenant, seq LIMIT ?;
```

The last `?` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page
size. `(tenant, seq)` must be unique and `NOT NULL`. This is test-schema coverage for the pinned
image only; it is not a claim about other MariaDB versions or deployments.

## Sessions and locks

[`MariaDbPoolProvider::acquire_mariadb_session`] returns a concrete session with
a [`MariaDbSession::discard`] method. A caller that owns a session-level
`GET_LOCK` and cannot guarantee an explicit release must call it before
dropping; the physical connection is then closed instead of returning to the
pool with the lock held.
Both concrete acquisition and trait-object acquisition, including
`Pool::acquire_async`, return `Unsupported` and discard the connection when
its character sets or SQL mode are unsafe to scan.

## Example

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_mariadb::{MariaDbConfig, MariaDbPoolProvider};

async fn example() -> Result<(), neotales_dbapi::DbError> {
let provider = MariaDbPoolProvider::connect(
    MariaDbConfig::new("db.example.com", 3306, "app", "app")
        .password("from-your-own-secret-store")
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(3)),
).await?;
let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
let rows = provider.fetch_all_async(
    "SELECT name FROM users WHERE id = ?",
    QueryOptions::new().bind_all(&parameters).limit(RowLimit::new(10, 4096)?),
).await?;
let _ = rows;
Ok(())
}
```

## Testing

```sh
cargo test -p neotales-dbapi-mariadb --all-targets
dbapi-mariadb/tests/live.sh
```

The live lane starts a digest-pinned MariaDB 11.4 container with
`testcontainers`, publishes no host ports, uses a random nonempty root password
from the OS CSPRNG, and reaches the container bridge through a loopback-only
proxy. It covers the shared contract, verified TLS with a disposable CA and
host-name mismatch/untrusted-CA negatives, MariaDB-specific error codes and
types, inherited `autocommit=0` and unsafe SQL modes, and transaction/DDL
rejection. `openssl` is required for the TLS test.

For a single TLS investigation, select exactly one ignored test:

```sh
cargo test -p neotales-dbapi-mariadb --test mariadb verified_tls_trusts_only_the_expected_ca_and_host -- --ignored --exact --nocapture
```

This test emits `MARIADB_TLS` records with fixed stage/category labels and
elapsed milliseconds. Negative cases report both the expected connection
rejection and the actual sanitized category. Certificate generation, container
startup, port isolation, proxy setup, account readiness, and each TLS/query case
have separate stages. Its current-thread panic hook discards payloads; it never
prints container logs, SQL, certificates, credentials, or raw driver errors.

`USE` statements are rejected, including inside trusted scripts, to prevent
database selection from leaking to another pool borrower. Executable comments
and indirect execution are also rejected.

Sessions require UTF-8 (`utf8mb4`) client, connection, and result character
sets; invariant-changing `SET` statements are rejected before execution.

## Licence 📜

MIT. See [LICENSE.md](LICENSE.md).
