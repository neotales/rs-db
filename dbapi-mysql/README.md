# neotales-dbapi-mysql

MySQL 8.4 provider for `neotales-dbapi`, with no `SQLx` type in the public API.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi neotales-dbapi-mysql
```

## What you can do 🔧

**Connect with verified TLS and query with native `?` parameters.**

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_mysql::{MySqlConfig, MySqlPoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = MySqlPoolProvider::connect(
        MySqlConfig::new("db.example.com", 3306, "app", "app")
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

**Get a concrete session when you own a named lock.** `acquire_mysql_session` returns a session
with `discard()`, so an unreleased `GET_LOCK` closes its physical connection instead of returning
to the pool.

**Do not treat `TINYINT(1)` as a boolean.** It decodes as `I64`; `Bool` parameters bind as 0/1 and
do not round-trip as `Bool`.


## Scope

`MySQL` implementation of the driver-neutral [`neotales_dbapi`] contract. The
public API does not expose `SQLx` types.

[`neotales_dbapi`]: https://docs.rs/neotales-dbapi

## Scope

This crate speaks the `MySQL` client/server protocol through the `SQLx` `MySQL`
driver. It is tested against `MySQL` 8.4 only. MariaDB compatibility is not
claimed: the wire protocol overlaps, but authentication plugins, error numbers,
DDL behaviour and SQL syntax differ and no MariaDB server is in this crate's
test lane. `ClickHouse` exposes a MySQL interface, but its statements,
transactions and error model are different; it is not a target of this provider.

The integration lane uses `testcontainers` to start a digest-pinned `MySQL` 8.4
server, runs the contract against it, and removes it. The container publishes no
ports: the test disables testcontainers' `PublishAllPorts` default through a
Docker `HostConfig` modifier and asserts Docker reports no 3306 host mapping.
It connects to the container bridge address through a listener bound to
`127.0.0.1`. Root uses a random nonempty password from the OS CSPRNG, never a
committed credential or a time/PID value. The test is `#[ignore]`d so a fixture-free
`cargo test` does not need Docker:

```sh
cargo test -p neotales-dbapi-mysql --all-targets
dbapi-mysql/tests/live.sh
```

The same lane runs verified TLS: it generates a disposable CA and a server
certificate whose SAN is `127.0.0.1`, then asserts that the trusted CA and
matching host connect, a wrong host fails, an untrusted CA fails, and
`MySqlTls::Insecure` connects without verification. `openssl` is required for
that test. The container still gets a nonempty password, and no published port
is used.

### Vitess compatibility

The provider connects to upstream `Vitess` `vttestserver` 24.0.5 on MySQL 8.4
(one keyspace, one shard, plaintext) because it no longer appends
`PIPES_AS_CONCAT`; this was verified manually. No automated `Vitess` fixture is
shipped. `vttestserver` 24.0.5 hardcodes
`vtcombo --mysql-auth-server-impl none`, and exposes no flag or environment
variable to configure authentication. Its `vtgate` MySQL listener therefore
accepts an unauthenticated `root` from any container or local user that can
reach the container address, and a test container cannot be made safe on a
shared Docker host. The automated lane is disabled rather than publishing that
listener or treating an isolated network as authentication.

That is not a `Vitess` or `PlanetScale` support claim: sharded or resharded
keyspaces, `VSchema` management, and `PlanetScale` deployments, deploy
requests, or `VReplication` workflows are not covered.

`MySqlConfig::new` requires TLS with certificate and host-name verification
against built-in `WebPKI` roots. Use [`MySqlConfig::ca_file`] for a private CA.
[`MySqlTls::Insecure`] encrypts without verifying the certificate and is an
explicit, documented downgrade. [`MySqlConfig::plaintext_loopback`] is the only
way to disable TLS, and it accepts numeric loopback hosts only. There is no
silent fallback from a requested TLS mode.

Statement and slow-statement logging are disabled on provider connections so
SQL text and embedded literals are not emitted. Pool lifecycle logging remains
at `SQLx` defaults; it does not include SQL or bound values.

## Trusted SQL and trusted database objects

This provider is not a sandbox or an SQL validator. Every statement and trusted
script is caller-supplied trusted SQL. `execute_trusted_script` is deliberately
unparameterized and must only receive SQL the application owns.

Database objects are trusted in the same sense. Stored procedures, prepared
statements, stored functions, triggers, events, views, generated columns, and
user-defined functions can perform nontransactional DDL or writes, change
session state, or have effects outside the connection. The lexical checks in
this crate cannot see inside them. `CALL`, `EXECUTE`, and `PREPARE` are rejected
as indirect statement paths, but an allowed `SELECT` or DML statement can still
invoke a trusted function or trigger. Review and own every installed routine and
trigger, and never expose this provider to untrusted SQL or untrusted database
objects.

## Placeholders and statements

Parameters are positional in slice order and use native `?` placeholders. SQL is
not rewritten. Providers reject a placeholder/parameter count mismatch before
dispatch. The placeholder scanner understands `MySQL` single/double/backtick
quoting, backslash escapes, `#` and `-- ` line comments, and `/* */` block
comments. It is a conservative lexer, not a SQL parser. Executable version
comments (`/*! ... */` and MariaDB `/*M! ... */`, including the lowercase `/*m!`
case, rejected conservatively) and `DELIMITER` directives are not interpreted.

`execute_async` and `fetch_async` accept exactly one statement, with optional trailing
semicolons and comments. Use `execute_trusted_script` for intentionally
multi-statement trusted SQL. Transaction-control statements are rejected on
session statements and scripts; use the transaction API.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. Parameters are the provider's native `?` placeholders, and the adapter supplies a
hard row/byte limit for the page plus one lookahead row. It does not rewrite SQL or classify a
statement as read-only; the caller supplies the read-only/replay-safe/authorized assertion and the
scoped continuation.

The ignored live test `mysql_sql_page_source_contract` in `tests/mysql.rs` exercises this statement
against the crate's digest-pinned MySQL 8.4 container. It covers the cross-tenant and single-tenant
scans, typed parameters, one-row lookahead, exact page order, and request counts. `cargo test` stays
fixture-free; select only this test with:

```sh
cargo test -p neotales-dbapi-mysql --test mysql -- --ignored --exact mysql_sql_page_source_contract
```

```sql
SELECT tenant, seq, body FROM events
WHERE tenant = ? AND (tenant, seq) > (?, ?)
ORDER BY tenant, seq LIMIT ?;
```

The last `?` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page
size. `(tenant, seq)` must be unique and `NOT NULL`. The plan must reject a token from another plan
or tenant and return a continuation exactly when the lookahead row exists. This is test-schema
coverage for the pinned image only; it is not a claim about other MySQL versions or deployments.

## Transactions and DDL

`MySQL` has transactional DML for `InnoDB`, but most DDL and administrative
statements cause an implicit commit. A provider cannot make those statements
atomic, so this crate does not pretend they are:

- transaction `execute_async` and `execute_trusted_script_async` reject leading DDL and
  administrative commands (`CREATE`, `ALTER`, `DROP`, `RENAME`, `TRUNCATE`,
  `GRANT`, `REVOKE`, `SET`, `LOCK`, `LOAD`, `ANALYZE`, and similar) with
  `Unsupported` before dispatch;
- the rejection is conservative. `CREATE TEMPORARY TABLE` does not implicitly
  commit, but it is rejected too because the leading-token check does not parse
  the statement;
- session statements and scripts may run DDL, but then the caller owns the fact
  that no surrounding transaction is atomic;
- `CALL`, `EXECUTE`, and `PREPARE` are rejected with `Unsupported` on every
  statement API, inside or outside a transaction. A procedure or prepared
  statement could otherwise hide `COMMIT` or DDL from the leading-token scan;
- executable comments (`/*! ... */`) are rejected, because their contents are
  not scanned;
- session statements that could change `autocommit`, `sql_mode`, or transaction
  characteristics are rejected with `Unsupported`. The check is a conservative
  byte scan: a `SET` statement is rejected when the same SQL text contains
  `autocommit`, `sql_mode`, `transaction`, `names`, `character`, `charset`, or
  `collation`, even inside a string. Acquisition verifies the client, connection,
  and result character sets are `utf8mb4` before scanning SQL;
- a session that inherits `NO_BACKSLASH_ESCAPES` or `ANSI_QUOTES` from the
  server is rejected at acquisition with `Unsupported`. The scanner assumes
  backslash escapes and double-quoted strings;
- stored functions, triggers, and events invoked by an otherwise allowed
  statement are not analyzed. Keep them free of DDL and session-state changes.

Supported transaction options are all four portable isolation levels and
read-only/read-write mode. Unsupported values never reach the server.

`MySqlPoolProvider::acquire_mysql_session` returns a concrete session with a
`discard()` method. A caller that owns a session-level lock and cannot guarantee
an explicit release must call it before dropping the session; the physical
connection is then closed instead of returning to the pool with the lock held.
Both concrete acquisition and trait-object acquisition, including
`Pool::acquire_async`, return `Unsupported` and discard the connection when
its character sets or SQL mode are unsafe to scan.

Every acquired physical connection is set to `autocommit=1` and read back before
the session is returned. A server or session inherited with `autocommit=0` is
overridden; if the read-back is not 1, acquisition fails closed. Public `SET`
statements that would change `autocommit` are rejected.

`SQLx` appends `PIPES_AS_CONCAT` to the session `sql_mode` by default, and some
MySQL-protocol servers, including `Vitess` `vtgate`, reject that addition at
connect time. This provider disables the addition, so `||` keeps whatever
meaning the server's configured `sql_mode` gives it. The provider never uses
`||` for concatenation and the placeholder scanner does not depend on it.
Trusted SQL that relied on the old appended mode must use `CONCAT(...)`; a
public `SET` that changes `sql_mode` is rejected. The fail-closed rejection of
`NO_BACKSLASH_ESCAPES` and `ANSI_QUOTES` is unchanged.

`MySQL` does not support nested transactions. Use one exclusive session for an
interactive transaction, never independent pool requests.

## Rows and types

Rows are detached and owned. Fetch limits are hard row and byte bounds; the
provider fails with `Overflow` rather than truncating. Names are supplied by the
server; duplicate names make name lookup fail.

Decoded types:

| `MySQL` type | `DbType` |
| --- | --- |
| `TINYINT`, `SMALLINT`, `INT`, `MEDIUMINT`, `BIGINT` (signed) | `I64` |
| unsigned integers, `BIT`, `YEAR` that fit in `i64` | `I64` |
| unsigned integers or `BIT` above `i64::MAX` | `Overflow` |
| `FLOAT`, `DOUBLE` | `F64` |
| `CHAR`, `VARCHAR`, `TEXT`, `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT`, `ENUM` | `Text` |
| `BINARY`, `VARBINARY`, `BLOB`, `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB` | `Bytes` |
| `NULL` expression | Null with `Text` |

`MySQL` has no boolean storage class. `TINYINT(1)` is reported as `BOOLEAN` by
the protocol, but the same wire type can hold `-128..=127`; decoding it as
`Bool` would collapse non-zero values. This provider therefore always returns
`I64` for it, and `Bool` parameters bind as integer `0`/`1`. A fetched `I64`
does not round-trip as `Bool`.

`DECIMAL`, temporal types, `JSON`, `SET`, `GEOMETRY`, and other spatial types
are not decoded; the row fails with `Decode`, including a `NULL` of one of those
types. Cast them in SQL (for example
`CAST(value AS CHAR)`) when text is sufficient. This provider does not invent a
lossy representation.

## Timeouts, errors, and cleanup

`QueryOptions::timeout` is enforced by the provider with a client-side
deadline. When it elapses, the in-flight future is dropped and the physical
connection is discarded before reuse. A dispatched write or fetch returns
`WriteOutcomeUnknown`; a dispatched `COMMIT` returns `CommitOutcomeUnknown`.
Dropping the future does not prove that the server stopped executing the
statement, so treat the outcome as unknown and reconcile by querying.

Errors retain only a stable `ErrorKind`. Driver messages, SQL, bound values, and
connection strings are never attached. Constraint errors, deadlocks and lock
waits, query interruption, statement timeout, malformed SQL, and transport
failures are distinguished. A statement that fails after dispatch discards its
connection; validation failures before dispatch keep it usable.

Direct provider, session, and transaction calls do not retry. Wrap the provider
in `PolicyPool` for telemetry and explicit replay-safe retries.

## Example

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_mysql::{MySqlConfig, MySqlPoolProvider};

async fn example() -> Result<(), neotales_dbapi::DbError> {
let provider = MySqlPoolProvider::connect(
    MySqlConfig::new("db.example.com", 3306, "app", "app")
        .password("from-your-own-secret-store")
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(3)),
).await?;
let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
let rows = provider.fetch_all_async(
    "SELECT name FROM users WHERE id = ?",
    QueryOptions::new().bind_all(&parameters).limit(RowLimit::new(10, 4096)?),
).await?;
if let Some(row) = rows.first() {
    println!("{:?}", row.get::<Option<String>>("name")?);
}
Ok(()) }
```

`USE` statements are rejected, including inside trusted scripts, to prevent
database selection from leaking to another pool borrower. Executable comments
and indirect execution are also rejected.

## Licence 📜

MIT. See [LICENSE.md](LICENSE.md).
