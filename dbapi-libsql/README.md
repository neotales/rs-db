# neotales-dbapi-libsql

Remote libSQL/Turso provider for `neotales-dbapi` over the Hrana HTTP protocol.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi neotales-dbapi-libsql
```

## What you can do 🔧

**Connect to Turso Cloud or a self-hosted `sqld`.** `LibsqlConfig::remote` accepts `libsql://` and
`https://` only and verifies the server certificate and host name.

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = LibsqlPoolProvider::connect(
        LibsqlConfig::remote("libsql://example.turso.io", "from-your-secret-store")
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5)),
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

**Keep the driver out of your public API.** The provider accepts SQLite `?` and `?NNN`
placeholders, returns detached rows, and maps remote failures to sanitized `DbError` kinds.

**Reconcile unknown commits explicitly.** A dropped `commit_async` future, or an error from it, is
not proof of rollback. The outcome is reported as unknown and is never retried automatically.


## Scope

Remote libSQL/Turso implementation of the driver-neutral
[`neotales-dbapi`](https://docs.rs/neotales-dbapi) contract. The public API does
not expose libSQL types.

## Engine selection

This crate speaks the remote libSQL HTTP (Hrana) protocol only. It connects to
Turso Cloud (`libsql://…`) or a self-hosted `sqld` over `https://…`. It does not
open a local libSQL file, run the in-process engine, create an embedded replica,
or synchronize frames. Use `neotales-dbapi-sqlite` for an in-process SQLite file;
those modes have different durability and cancellation behaviour and are not
claimed here.

`ws://` and `wss://` are not supported by the HTTP client. A URL must not include
user information, a query, a fragment, or a path other than `/`.

## TLS

`LibsqlConfig::remote` accepts only `libsql://` and `https://`. The transport is
libSQL's Rustls connector with certificate and host-name verification. The
configuration copy of the token is zeroized on drop; the vendor client keeps its
own copy for the lifetime of the provider, which this crate cannot zeroize. There is
no insecure TLS switch, no custom verifier, and no automatic downgrade to
cleartext. `LibsqlConfig::plaintext_loopback` is the only cleartext constructor;
it requires a numeric loopback IP address and is intended for a disposable local
server. The live test uses it through a loopback proxy that publishes no host
port.

Trust roots and host-name verification come from the vendor connector. This
crate does not add a second TLS stack or a private-CA path. A deployment that
needs a private CA must install it where the process trust store is configured;
the test lane does not claim to verify that deployment.
The fork tests real TLS handshakes with an ephemeral test CA, including wrong-CA
and wrong-hostname rejection. Subprocess tests also exercise the production
connector's native-root loading with a controlled `SSL_CERT_FILE` override,
including failure on an empty trust store. These tests do not validate the
platform-installed trust store, a deployment's CA installation, or hosted Turso.

The remote-only libSQL fork rejects all cross-origin Hrana `base_url` changes
and does not follow HTTP redirects. This prevents forwarding the bearer token
and stream baton to another origin or downgrading HTTPS. Some deployments may
require legitimate cross-origin routing; those are unsupported until an
explicitly safe routing policy is designed.

## Parameters and statements

Parameters are positional in slice order and use SQLite placeholder syntax. `?`
and `?NNN` are accepted; mixing them is rejected. The highest placeholder index
must equal the parameter count. Named `:name`, `@name`, and `$name` placeholders
are rejected. SQL is never rewritten.

`execute_async` and `fetch_async` accept one statement plus trailing semicolons and comments.
The scanners understand SQLite quote/comment forms but are conservative lexers,
not SQL parsers. Transaction-control SQL is rejected on standalone statement
methods. Validation failures before dispatch leave the session usable.

`execute_trusted_script` is unparameterized and executes a multi-statement batch.
The caller must keep transaction control out of the script. A batch is not
atomic, and the remote client materializes rows before dropping them, so use it
for trusted DDL/administration rather than unbounded result sets.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. Remote libSQL uses SQLite `?` placeholders; the adapter supplies a hard row/byte
bound for the page plus one lookahead row, drains and rejects extra result sets, and does not rewrite
or classify SQL. The caller supplies the read-only/replay-safe/authorized assertion and the scoped
continuation.

The ignored live test `sqld_sql_page_source_contract` in `tests/sqld.rs` exercises this statement
against the existing digest-pinned remote libSQL `sqld` fixture. It covers the cross-tenant and
single-tenant scans, typed parameters, one-row lookahead, exact page order, and request counts.
`cargo test` stays fixture-free; select only this test with:

```sh
cargo test -p neotales-dbapi-libsql --test sqld -- --ignored --exact sqld_sql_page_source_contract
```

```sql
SELECT tenant, seq, body FROM events
WHERE tenant = ? AND (tenant, seq) > (?, ?)
ORDER BY tenant, seq LIMIT ?;
```

The last `?` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page
size. `(tenant, seq)` must be unique and `NOT NULL`. This is test-schema coverage for the pinned
`sqld` image only; it is not a claim about hosted Turso or other deployments.

## Transactions

`Session::begin_async` starts `DEFERRED`; `ImmediateTransaction::begin_immediate_async`
starts `IMMEDIATE`. The provider reports `isolation_level: false` and
`read_only_transactions: false`. Any requested isolation level and
`read_only: Some(true)` fail with `Unsupported` before dispatch.
`read_only: Some(false)` requests the default access mode and is accepted. A
committed transaction has SQLite serializable semantics, but this provider does
not expose configurable isolation.

Dropping a transaction discards its Hrana stream and the session; libSQL makes a
best-effort rollback. A dropped `commit_async` future leaves the outcome unknown and
must be reconciled by querying. A commit that returns an error is reported as
`CommitOutcomeUnknown`, never as a rollback.

## Timeouts, cancellation, and errors

`QueryOptions::timeout` sets a client-side deadline. Hrana over HTTP cannot
interrupt a dispatched statement, so an elapsed deadline or a dropped future
poisons the session. A dispatched statement, including a fetch, returns
`WriteOutcomeUnknown`; the next call on that session returns `Connection`.
`begin_async`, `commit_async`, and `rollback_async` have no caller-supplied deadline.

Errors retain only a stable `ErrorKind`; SQL, values, tokens, endpoints, and
libSQL messages are never attached. The fork does not emit SQL, arguments,
tokens, endpoint URLs or Hrana batons in its tracing events, including cleanup
tasks. The provider also polls vendor operations under a no-op dispatcher.

`libSQL` 0.10 boxes remote server errors without a typed SQLite code. A
dispatched remote failure is therefore mapped to `Connection` and, for a write,
to `WriteOutcomeUnknown`; it is not classified as `Constraint` or `Busy`.
Locally detected validation, TLS, transport, and deadline failures keep their
specific categories. The conservative unknown result is never retried.

## Rows and types

Fetch limits are hard bounds on the detached result: the provider fails with
`Overflow` rather than truncating. The remote query path uses the Hrana cursor
and pulls rows one at a time; the row count and decoded byte budget are checked
before the next row is retained. It does not use the batch API for `fetch_async`. A
single row (or HTTP chunk) is materialized before its size can be checked, and
the server may buffer its own result, so `RowLimit` is **not** a hard transport
or process-memory cap on either side. It may decode one
row beyond the row count to establish that overflow exists. If a workload needs
a process-memory ceiling, use SQL filtering/aggregation; libSQL 0.10 exposes no
pre-decode transport limit here.

`execute_trusted_script` is different: it uses the batch API, which materializes
all returned rows before they are dropped. It is not row-bounded. Use it for
trusted DDL and administrative work, not unbounded result sets.

Rows are detached. Integer values become `I64`, reals `F64`, text `Text`, and
blobs `Bytes`. A remote null has no declared type, so it becomes
`Value::Null(DbType::Text)`; use `Option<String>` for a nullable cell. `Bool`
parameters bind as `0`/`1`, and fetched integers remain `I64`. Non-finite reals
fail with `Decode`.

## Policy pool

Direct calls do not retry. Wrap the provider in `neotales_dbapi::PolicyPool` for
sanitized telemetry and explicit replay-safe retries with a caller-supplied
`RetryRuntime`. Retry eligibility and cancellation are implemented by
`neotales_dbapi`; this crate only supplies sessions and sanitized outcomes.

## Example

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, QueryOptions, RowLimit, Value};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};

async fn example() -> Result<(), neotales_dbapi::DbError> {
let provider = LibsqlPoolProvider::connect(
    LibsqlConfig::remote("libsql://example.turso.io", "from-your-own-secret-store")
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5)),
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

## Testing

`cargo test -p neotales-dbapi-libsql --all-targets` is fixture-free. The live
contract is `#[ignore]`d and starts a digest-pinned `ghcr.io/tursodatabase/libsql-server`
container through the Docker CLI. The exact image digest must already be present
locally; the test does not pull images:

```sh
dbapi-libsql/tests/live.sh
```

The provider dependency graph, including test dependencies, can be checked
without advisory suppressions using
`cargo deny --manifest-path dbapi-libsql/Cargo.toml --config dbapi-libsql/deny.toml check advisories`.

The lane covers connect/probe, parameter and placeholder rules, row/byte bound
rejection (including an `VmHWM` regression that catches full-result buffering),
transaction commit/rollback and unsupported options, trusted scripts, timeout
and dropped-future poisoning, `PolicyPool` telemetry/SQL capture/retry, and a
cleartext-loopback negative for HTTPS. It does not cover Turso Cloud itself,
verified TLS against a local `sqld` (the disposable server is cleartext), a real
commit-unknown event, or server-side constraint classification (the vendor drops
the code).

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
