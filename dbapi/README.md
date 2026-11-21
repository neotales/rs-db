# neotales-dbapi

Driver-neutral SQL execution with bounded row access for asynchronous and blocking callers.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

`neotales-dbapi` is the contract, not a driver. Add it together with a provider:

```sh
cargo add neotales-dbapi neotales-dbapi-sqlite
cargo add tokio --features macros,rt-multi-thread
```

Replace `neotales-dbapi-sqlite` with `neotales-dbapi-postgres`, `neotales-dbapi-mysql`,
`neotales-dbapi-mariadb`, `neotales-dbapi-mssql`, `neotales-dbapi-clickhouse`, or
`neotales-dbapi-libsql` when that is the database you use. The provider crate owns the
connection constructor and depends on `neotales-dbapi`. The reverse dependency would be a
cycle, so this crate cannot ship the SQLite program as its own example; the runnable copy is
[`dbapi-sqlite/examples/connect_and_query.rs`](../dbapi-sqlite/examples/connect_and_query.rs),
compiled by CI and run with:

```sh
cargo run -p neotales-dbapi-sqlite --example connect_and_query
```

## What you can do 🔧

**Open a real SQLite connection, create a table, and read a typed row.** This is a complete
program, not a session that appears from nowhere. The provider is the SQLite package; the
traits come from this crate.

```rust
use neotales_dbapi::{DbError, DbType, Parameter, QueryOptions, RowLimit, Value};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new("app.sqlite")
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
    // A fetched SQLite integer is I64, not Bool. Decode it as an integer.
    let active: i64 = row.read("active")?;
    assert_eq!(name, "Ada");
    assert_eq!(active, 1);
    println!("{name} is active={active}");
    Ok(())
}
```

A synchronous caller uses the same provider through `sync::Pool`, or wraps an async pool with
`blocking::BlockingPool` when the `blocking` feature is enabled.

**Read rows without copying the schema into your types.** `Row` owns detached values in ordinal
order with provider-supplied names. `row.read::<T>("name")` reads by exact name;
`row.read_at::<T>(index)` reads by zero-based ordinal; `Option<T>` is a matching typed SQL null.

**Bound one batch across rows, bytes, and result sets.** `RowLimit` is a hard cap for the whole
fetch. Providers fail with `Overflow` rather than truncate. The limit also covers rows a
convenience call drains, not only the row it returns.

**Page over one fetch, or turn paging into explicit requests.** `paging::PageStream` and
`paging::RowStream` are views over a reader that was already fetched.
`paging::multi::PageSourceStream` asks a caller-authored source for one page at a time.

## 🚀 Quick start

The session-level shape is the same for every provider. The example below takes a `&mut dyn
Session`, so it compiles without choosing a driver; the SQLite program above shows the missing
connection step.

```rust,no_run
use neotales_dbapi::{
    DbError, DbType, Parameter, QueryOptions, RowLimit, Session, Value,
};

async fn first_active_name(session: &mut dyn Session) -> Result<Option<String>, DbError> {
    let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
    let mut reader = session
        .fetch_async(
            "SELECT active, name FROM users WHERE id = ?",
            QueryOptions::new().bind_all(&parameters).limit(RowLimit::new(10, 4096)?),
        )
        .await?;

    let mut name = None;
    while let Some(row) = reader.next_row_async().await? {
        // SQLite has no boolean storage class, so this column decodes as I64.
        // PostgreSQL's BOOL column would decode as bool.
        let active: i64 = row.read_at(0)?;
        if active != 0 {
            name = row.read("name")?;
            break;
        }
    }

    // Always finish: it drains the whole batch and surfaces a later statement error.
    reader.finish_async().await?;
    Ok(name)
}
```

SQLite uses `?`; PostgreSQL uses `$1`, `$2`. Parameters are positional in slice order. The provider
never rewrites SQL and rejects a placeholder/parameter count mismatch.

A provider crate owns its public connection/pool constructor and configuration types, then exposes
its implementation of the object-safe traits. The asynchronous traits are `Pool`, `Session`,
`Transaction`, and `Reader`; the synchronous counterparts live in the `sync` module. Async method
names carry an `_async` suffix (`fetch_async`, `next_row_async`, `commit_async`); synchronous names
stay unsuffixed (`fetch`, `next_row`, `commit`). One provider type can implement both families.

Consumers acquire asynchronous sessions through `Pool::acquire_async`; synchronous consumers use
`sync::Pool::acquire`.

The portable pool default has no statement timeout: not every provider can enforce one safely.
Fetches still have bounded row, byte, and result-set limits. An explicitly requested timeout must
be enforced or rejected as `Unsupported`; it must not be silently ignored. Provider-specific pool
defaults may choose an enforceable timeout.

> **Shared capability method.** `capabilities` is synchronous in both `Pool` traits, so an
> unqualified `provider.capabilities()` is ambiguous when both traits are in scope. Fully qualify
> the family you mean:

```rust,no_run
use neotales_dbapi::{Capabilities, Pool, sync};

fn both_capabilities(provider: &(impl Pool + sync::Pool)) -> (Capabilities, Capabilities) {
    (Pool::capabilities(provider), sync::Pool::capabilities(provider))
}
```

`defaults` is also shared by the two pool families. Session, transaction, and reader methods
differ by the `_async` suffix.

> **⚠️ Fetch is not proof of a read.** `INSERT ... RETURNING` can write. A dispatched statement whose
> result cannot be established must surface `WriteOutcomeUnknown`, never a retryable transport
> error.

## 📖 Reading rows

`Row` owns detached values in ordinal order with provider-supplied names.

- **`row.read::<T>("name")`** reads by exact name.
- **`row.read_at::<T>(index)`** reads by zero-based ordinal.
- **`Option<T>`** represents a matching typed SQL null.
- **`Row::get`** and **`Row::get_ordinal`** remain available with the same rules.
- Missing names, duplicate names, wrong types, mismatched null types, and numeric overflow are
  distinct stable `ErrorKind` values.

```rust,no_run
use neotales_dbapi::{DbError, DbType, Parameter, QueryOptions, RowLimit, Session, Value};

async fn read_row(session: &mut dyn Session) -> Result<(), DbError> {
    let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
    let row = session
        .fetch_one_async(
            "SELECT name, score FROM users WHERE id = $1",
            QueryOptions::new().bind_all(&parameters).limit(RowLimit::new(10, 4096)?),
        )
        .await?;

    let name: String = row.read("name")?;
    let score: Option<i64> = row.read_at(1)?;
    let _ = (name, score);
    Ok(())
}
```

## 🧭 Readers, sets, and limits

`Session::fetch_async` and `Transaction::fetch_async` return a `Reader`. A pool-level `fetch_async` also returns one;
the default implementation drains the batch into owned rows before releasing the session.

- **`next_row_async`** yields the next row of the current result set, or `None` at that set's end.
- **`has_next_set_async`** advances. It discards any unread rows in the current set and returns whether
  another set exists.
- **`finish_async`** drains and discards every remaining row and set, propagating the first later error.
- **`read_all_async`** drains the batch into a `Vec<Row>` for callers that want owned rows.
- **Dropping an unfinished reader** with unread results attached must discard the connection. That
  cleanup cannot report a later statement error, so call `finish_async` when the error matters.
- The SQLite and PostgreSQL providers in this tree materialize the single bounded set before
  returning, so their readers have nothing outstanding to discard.

`RowLimit` is a hard row, byte, and result-set bound for the whole batch. Providers fail rather
than truncate. Single-set providers report `has_next_set_async() == false` instead of inventing result
boundaries; a provider that reports multiple sets counts rows, bytes, and sets across them.

`RowLimit::new(rows, bytes)` permits one result set. Use
`RowLimit::with_max_sets(rows, bytes, max_sets)` when the provider can return multiple sets; the
set bound counts empty sets too. Byte accounting is portable: column-name UTF-8 bytes plus payload
bytes (bool 1, integer/float 8, text/bytes length, typed null 0). Providers and buffering wrappers
use the same rule.

## 📄 Paged views

`paging::PageStream` and `paging::RowStream` are views over a reader that was already fetched.
They do not take SQL, dispatch a second query, or add a limit of their own. The single
`fetch_async` call still carries the parameters, timeout, telemetry, retry policy, and aggregate
`RowLimit`.

```rust,no_run
use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError, QueryOptions, RowLimit, Session,
    paging::{Page, PageSize, PageStream},
};

async fn read_pages(session: &mut dyn Session) -> Result<Vec<Page>, DbError> {
    let reader = session
        .fetch_async(
            "SELECT name FROM users ORDER BY name",
            QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
        )
        .await?;

    // One await collects the bounded pages. PageSize is a view chunk, not RowLimit.
    PageStream::new(reader, PageSize::try_from(25)?)
        .try_collect()
        .await
}
```

- A page never crosses a result-set boundary. Every result set, including an empty one, contributes
  at least one page; an empty set is one page with no rows. `Page::set_index` is a view-relative
  label that counts empty sets. `PageStream::new`/`RowStream::new` start it at `0`; if the reader was
  already advanced, use `PageStream::at_set`/`RowStream::at_set` to supply the current set index.
  The stream cannot verify that label because `Reader` does not expose its position. A current set
  labelled `usize::MAX` is valid when it is the last set; a later set yields `Overflow` rather than
  reusing the label.
- `RowStream` yields `SetRow`, which carries the set index for every row. An empty set has no row,
  so use the page view when empty-set identity matters. After an empty set, `RowStream` wakes and
  yields `Pending`, so one poll advances at most one set.
- The streams are ordinary `futures_core::Stream` values whose items are `Result<_, DbError>`, so
  `TryStreamExt::try_collect`, `try_next`, and the rest of the standard extension methods work.
- Normal completion drains the reader and calls `finish_async`, so a later statement error is an
  error item. Dropping a stream early drops the reader without finishing it; the reader's normal
  cleanup and "cannot report a later error" rules apply.
- A reader is not proof of a read. Paging an `INSERT ... RETURNING` reader does not make the write
  safe to retry, and cancellation or a dropped view does not prove the write did not happen.
- This is a view over one execution. It does not claim to reduce the provider's materialization or
  memory use, and it does not implement offset, keyset, continuation-token, requery, or
  write-pagination semantics. `local::paging` mirrors the views for `!Send` local futures, and
  `sync::paging` provides blocking `PageIter`/`RowIter` variants. The root view remains `Send`; the
  local view makes no `Send` assertion; the sync view uses no runtime.

## 🔁 Multi-request paging

`paging::multi` adds an explicit `PageSource` pager for keyset, offset, or provider-continuation
paging. The source authors the request, SQL, parameters, keyset/offset logic, and continuation. The
pager never rewrites SQL, retries, replays, prefetches, or spawns a task; it requests one page only
when the stream is polled.

```rust,no_run
use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError,
    paging::{
        PageSize,
        multi::{PageBudget, PageSource, PageSourceStream, PagedPage},
    },
};

async fn collect_pages(source: Box<dyn PageSource>) -> Result<Vec<PagedPage>, DbError> {
    PageSourceStream::new(
        source,
        PageSize::try_from(50)?,
        PageBudget::new(1_000, 65_536, 100)?,
    )
    .try_collect()
    .await
}
```

- `PageBudget` bounds aggregate delivered rows, portable decoded bytes, and page count. Empty pages
  count toward the page bound, so an empty-page loop terminates. The row stream wakes and yields
  `Pending` after an empty page, so one poll performs at most one source request.
- `PageSize` is enforced per page. A source returning more rows is a `Decode` error; exceeding an
  aggregate row, byte, or page bound is `Overflow`.
- A source returns an opaque `Continuation` only when another request is useful. The pager passes it
  back unchanged and never prints it. When an aggregate bound is exact and a continuation still
  exists, the pager fails closed with `Overflow` instead of requesting another page.
- The source must finish each single-result-set request before publishing rows, surface late errors,
  and reject additional result sets. `read_single_set` drains and enforces that shape. The default
  `Reader::read_sets_async` always reports at least one set, so a no-row batch is one empty set and
  zero-set presence is not observable; the caller must issue a single-result-set statement.
- Read-only and replay safety are the source author's precondition. The pager does not classify SQL;
  requesting a page can repeat a write. Cancellation or a dropped stream is not proof that a write
  did not happen.
- Continuation scope, expiry, tenant/query binding, and validation are source-owned. Tokens are
  redacted in `Debug` output.
- The root pager requires a `Send` source. `local::paging::multi` is the `!Send` counterpart, using
  local futures and a local token/source; `sync::paging::multi` is the native blocking iterator
  variant. `paging::multi::sql` is the generic caller-authored SQL source for all three families;
  `local::paging::multi::sql` is the `!Send` plan adapter. None of these add a provider dispatch
  path or a D1 bound. D1 local operations remain fail-closed as documented above.
- Multi-request paging makes no distributed-consistency or materialization claim.

### Caller-authored SQL sources

`paging::multi::sql` provides a generic `SqlPageSource<'session, S>` over an exclusive `Session` and
a caller-implemented `SqlPagePlan`. The session may borrow caller state for `'session`; the source
does not require `'static`. The adapter is shared by the root, local, and synchronous pagers. It does
not build a query: the plan supplies the complete provider-specific statement, typed parameters,
keyset/offset logic, and continuation validation, plus an explicit
`SqlPageAssertion::read_only_replay_safe_and_authorized()`.

The adapter owns the mechanical bounds and page shape:

- it computes an effective page size of `min(request.page_size(), request.remaining_rows())` and
  asks for exactly one lookahead row (`effective + 1`);
- it replaces the fetch limit with a hard `RowLimit` over the lookahead rows and a separate
  `source_bytes` allowance, then rechecks the portable `Row::encoded_bytes` total;
- it drains one result set with `read_single_set`, rejects any additional set (including an empty
  one) and later statement errors before publishing, removes the lookahead, and rejects a
  continuation that disagrees with the lookahead. It does not claim to detect zero sets: the default
  reader reports a no-row batch as one empty set, so the caller must dispatch a single-result-set
  statement;
- it rejects an explicit enabled `RetrySetting::Use(RetryPolicy::Enabled { .. })` override or a
  retry runtime with `Unsupported`. `RetrySetting::Off` is accepted, a disabled `Use` override is
  accepted and normalized to `Off`, and every request is set to `RetrySetting::Off`. It never
  retries, replays, prefetches, or spawns a task.

The caller still owns read-only/replay safety, SQL and ordering, tenant authorization, and
continuation scope. `source_bytes` is not a provider allocation or buffering bound, and the adapter
makes no snapshot or distributed-consistency claim. A plan that sets its own fetch limit is rejected
rather than silently overridden.

The SQLite package compiles and exercises two complete references:

- [`examples/sql_page_source.rs`](../dbapi-sqlite/examples/sql_page_source.rs) — runnable example of
  the generic adapter.
- [`examples/support/sql_keyset_plan.rs`](../dbapi-sqlite/examples/support/sql_keyset_plan.rs) — the
  reusable tested `SqlPagePlan`.
- [`tests/sql_page_source.rs`](../dbapi-sqlite/tests/sql_page_source.rs) — SQLite integration tests.
- [`examples/keyset_paging.rs`](../dbapi-sqlite/examples/keyset_paging.rs),
  [`examples/support/keyset_source.rs`](../dbapi-sqlite/examples/support/keyset_source.rs), and
  [`tests/keyset_paging.rs`](../dbapi-sqlite/tests/keyset_paging.rs) — the hand-written
  `PageSource` reference, for callers that need a different shape than the generic plan.

Both examples run a **cross-tenant administrative scan** (the caller must be authorized for every
tenant) and a **tenant-scoped** scan whose authorized tenant is a separate predicate and parameter,
never the continuation cursor. They use a unique `NOT NULL` composite `(tenant, seq)` ordering, and
a continuation scoped to the issuing plan/source instance. A tenant-scoped plan additionally rejects
a cursor outside its authorized tenant.

SQLite uses `?` placeholders. PostgreSQL uses `$1`, `$2`, `$3` (and `$4` for the scoped form), for
example `WHERE tenant = $1 AND (tenant, seq) > ($2, $3) ORDER BY tenant, seq LIMIT $4`. MySQL and
MariaDB use `?`; SQL Server uses `@P1`-style names and `TOP (@P3)`; ClickHouse uses
`{p1:Int64}`-style native names. The adapter never rewrites SQL, so the caller writes the provider's
form. The SQLite forms are exercised by the fixture-free SQLite tests. Ignored live tests in the
PostgreSQL, MySQL, MariaDB, SQL Server, direct SQL Server, libSQL, and ClickHouse crates exercise
their own native forms against pinned containers; each provider README names its test and image,
and a plain `cargo test` does not run them.

A source makes no retry, replay, or snapshot promise; page consistency and read-only/replay safety
are the caller's precondition.

## ⚡ Convenience calls

All convenience calls finish the full batch, so a later statement error wins over an earlier row.

| Call | Result |
| --- | --- |
| `fetch_first_async` | `Option<Row>` — the first row of the first set |
| `fetch_one_async` | `Row` — exactly one row in the first set; `NoRows` or `TooManyRows` otherwise |
| `fetch_scalar_async` | `Value` — the first column of the exactly-one row |
| `fetch_all_async` | `Vec<Row>` — every row and set in the bounded batch |

```rust,no_run
use neotales_dbapi::{DbError, Pool, QueryOptions, RowLimit, Session, Value};

async fn totals(
    pool: &dyn Pool,
    session: &mut dyn Session,
) -> Result<(), DbError> {
    // The limit covers the rows that fetch_first_async drains, not only the row it returns.
    let first = session
        .fetch_first_async(
            "SELECT name FROM users ORDER BY name",
            QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
        )
        .await?;
    if let Some(row) = first {
        let _name: String = row.read("name")?;
    }

    let count = session
        .fetch_scalar_async(
            "SELECT count(*) FROM users",
            QueryOptions::new().limit(RowLimit::new(1, 64)?),
        )
        .await?;
    assert!(matches!(count, Value::I64(_)));

    let rows = pool
        .fetch_all_async(
            "SELECT name FROM users ORDER BY name",
            QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
        )
        .await?;
    let _ = rows;
    Ok(())
}
```

## ✍️ Execute and scripts

```rust,no_run
use neotales_dbapi::{DbError, DbType, Parameter, QueryOptions, Session, Value};

async fn write(session: &mut dyn Session) -> Result<(), DbError> {
    let affected = session
        .execute_async(
            "UPDATE users SET active = $1 WHERE id = $2",
            QueryOptions::new().bind_all(&[
                Parameter::new(DbType::Bool, Value::Bool(true))?,
                Parameter::new(DbType::I64, Value::I64(7))?,
            ]),
        )
        .await?;

    session
        .execute_trusted_script_async(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER)",
            QueryOptions::new(),
        )
        .await?;
    let _ = affected;
    Ok(())
}
```

`execute_trusted_script_async` is unparameterized and must only receive trusted SQL without
transaction-control statements. Interactive transactions use one exclusive session and the
`begin_async`/`commit_async`/`rollback_async` methods; never independent requests.
An explicitly set fetch limit is invalid for execute and trusted-script calls and is rejected
before dispatch. An inherited pool fetch limit does not apply to those operations.

## 🧱 Transactions and cleanup

A transaction exclusively borrows its session until consumed or dropped. Dropping without commit or
rollback, or interrupting a `begin_async`/`commit_async`/`rollback_async` future, must not leave a
connection reusable while a transaction may remain open. The provider must complete rollback or discard that connection before
reuse. Dropping a Rust value is not proof of rollback. A dispatched commit with unconfirmed success
is `CommitOutcomeUnknown`, not a rollback.

## 🛡️ Errors

Errors retain only a stable `ErrorKind`. Formatting, debug output, and the source chain do not
retain SQL, values, connection strings, or driver messages.

- Invalid caller arguments carry static `InvalidArgumentReason` values, never user values.
- `WriteOutcomeUnknown` and `CommitOutcomeUnknown` are distinct and are never reported as retryable
  `Connection`, `Timeout`, or `Cancelled`.
- `NoRows` and `TooManyRows` describe cardinality-one convenience failures.
- `Missing`, `Ambiguous`, `Null`, `WrongType`, and `Overflow` describe row decoding.
- `Reentrant` is reported only by the blocking adapter when a call is made from its own worker
  thread. It is never a provider outcome.

The core does not promise cancellation. If a statement, transaction, or script future is
interrupted or dropped, the provider must roll back or discard its connection before reuse.

## 🔁 Policy, retries, and telemetry

`PolicyPool` is an optional wrapper. Pool, session, and transaction execution, fetch, and trusted
script methods accept owned `QueryOptions` carrying per-query overrides. Scripts reject parameters
and fetch limits; session and transaction operations do not independently retry.

- Telemetry and exact SQL capture are **disabled by default**.
- SQL capture is separate from telemetry and must be explicitly enabled; SQL text can contain
  secrets in literals or comments.
- Observers receive a fixed operation name, duration, sanitized `ErrorKind` outcome, and optional
  captured SQL. They never receive parameters, rows, driver messages, or connection strings.

Standalone `PolicyPool::execute_async` and `PolicyPool::fetch_async` use the configured pool retry policy; the
default is `RetryPolicy::Disabled`. Retries run only when the individual `QueryOptions` explicitly
set `ReplaySafety::ExplicitlyReplaySafe`. SQL shape, including `SELECT`, is never used to infer that
assertion.

- Defaults retry only sanitized `Busy` and `Connection`.
- `Timeout` is eligible only when enabled in the retry policy.
- A dispatched write timeout is normalized to `WriteOutcomeUnknown` and is never retried.
- Unknown write/commit outcomes, transaction methods, scripts, and caller cancellation are never
  retried.
- Each attempt acquires a fresh session, drains and finishes its reader, and releases the session
  before backoff.

Retry requires a caller-provided `RetryRuntime`: a cancellation token, total-budget timer factory,
backoff sleeper, and random source. The timer spans acquisition, every attempt, and backoff. Its
factory is invoked exactly once, before the first acquisition. No executor, runtime, or task is
created by dbapi.

An explicitly replay-safe operation with an enabled policy but no runtime fails closed with
`Unsupported` before acquisition. Only standalone `PolicyPool` operations retry; explicit
enabled retry overrides on direct sessions and transactions are rejected as `Unsupported`.

### Defaults and overrides

`QueryOptions::new()` inherits the containing pool's settings and starts with **no parameters**.
The portable `PoolDefaults` has bounded fetch limits, no statement timeout, telemetry and SQL
capture off, and retry disabled. Providers can supply their own defaults. `PolicyPool` accepts
explicit `PoolPolicy` defaults: its `default_timeout: None` inherits the wrapped provider's
timeout, while its limit, telemetry, SQL-capture and retry fields are the policy's own values.
There is no process-wide mutable parameter list or implicit environment configuration.

Use `.bind()` or `.bind_all()` for ordered, validated typed parameters; `.bind_null(DbType::...)`
specifies a typed null. A per-call `.timeout()`, `.no_timeout()`, `.limit()`, `.telemetry()`,
`.sql_capture()`, or `.retry()` replaces the corresponding inherited setting. A provider that
cannot honor an explicit timeout or disabled timeout must reject it, not ignore it.

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{
    DbError, DbType, Delay, Parameter, PolicyPool, Pool, PoolPolicy, QueryOptions,
    QuerySetting, RetryPolicy, RetryRuntime, RetrySetting, RowLimit, Value,
};

fn configure<P: Pool + 'static>(provider: P, runtime: RetryRuntime) -> Result<PolicyPool, DbError> {
    Ok(PolicyPool::new(provider, PoolPolicy {
        default_timeout: Some(Duration::from_secs(3)),
        default_limit: RowLimit::new(100, 65_536)?,
        telemetry_enabled: true, // supply observer to receive events
        sql_capture_enabled: false,
        retry: RetryPolicy::new(3, Delay::Fixed(Duration::from_millis(20)), false)
            .expect("valid static retry policy"),
        retry_runtime: Some(runtime),
        ..PoolPolicy::default()
    }))
}

async fn lookup(pool: &dyn Pool) -> Result<(), DbError> {
    let key = Parameter::new(DbType::I64, Value::I64(7))?;
    let query = QueryOptions::new()
        .bind(key)
        .bind_null(DbType::Text)
        .limit(RowLimit::new(10, 4096)?)
        .timeout(Duration::from_secs(1))
        .telemetry(QuerySetting::Off)
        .sql_capture(QuerySetting::Off)
        .retry(RetrySetting::Off);
    let _rows = pool.fetch_all_async("SELECT ? AS id, ? AS note", query).await?;
    Ok(())
}

// Only use this after proving the entire operation replay-safe in your application.
async fn proven_safe_lookup(pool: &dyn Pool) -> Result<(), DbError> {
    let retry = RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(10)), false)
        .expect("valid static retry policy");
    let options = QueryOptions::new()
        .bind(Parameter::new(DbType::I64, Value::I64(7))?)
        .retry(RetrySetting::Use(retry))
        .replay_safe()
        .limit(RowLimit::new(1, 1024)?);
    let _ = pool.fetch_all_async("SELECT ? AS id", options).await?;
    Ok(())
}
```

The configured retry default does **not** assert replay safety. Only add `.replay_safe()` after
the application has proved that replaying the entire operation is safe, including writes,
triggers and external effects. Never use SQL shape as proof. Unknown outcomes are not retried,
even with that assertion. The caller must supply a real total-budget timer and sleeper in
`RetryRuntime`; do not substitute an immediate sleeper in production.

## 🔄 Synchronous counterparts

The `sync` module mirrors the reader lifecycle without a runtime: `sync::Reader`, `sync::Pool`,
`sync::Session`, and `sync::Transaction` reuse `Row`, `RowLimit`, `DbError`, and every option type.
A natively blocking provider implements them directly; it can stream from its driver, and dropping
an unfinished reader follows the same connection-discard rule as the async reader. Synchronous
method names stay unsuffixed (`execute`, `fetch`, `next_row`); asynchronous methods use the
`_async` suffix, so both trait families can coexist on one provider type.

```rust,no_run
use neotales_dbapi::{DbError, QueryOptions, RowLimit, sync};

fn count(pool: &dyn sync::Pool) -> Result<i64, DbError> {
    let row = pool.fetch_one(
        "SELECT count(*) AS n FROM users",
        QueryOptions::new().limit(RowLimit::new(1, 64)?),
    )?;
    row.read("n")
}
```

`sync::paging::PageIter` and `RowIter` provide blocking page and row iterators over any
`sync::Reader`. They preserve set boundaries, empty sets, aggregate limits, terminal errors, and
reader cleanup; `sync::paging::multi` provides the blocking `PageSourceIter`/`PageSourceRowIter`.
These are native iterators, not futures, and use no `block_on`. A `BlockingReader` implements
`sync::Reader`, so the same iterators run over the blocking adapter without changing its worker
lifecycle.

`blocking::BlockingPool` adapts an asynchronous pool to the synchronous traits. It is available only
with the `blocking` feature on non-wasm32 targets:

```rust,no_run
use neotales_dbapi::{DbError, Pool, QueryOptions, RowLimit, blocking::BlockingPool, sync::Pool as _};

fn run(asynchronous_pool: impl Pool + 'static) -> Result<(), DbError> {
let blocking = BlockingPool::new(asynchronous_pool);
let rows = blocking.fetch_all(
    "SELECT name FROM users ORDER BY name",
    QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
)?;
let _ = rows;
Ok(())
}
```

Each acquired session gets one dedicated worker thread with its own current-thread Tokio runtime. A caller never
runs `block_on` on its own thread, so the adapter may be called from inside another async runtime
without nesting runtimes. The worker owns the asynchronous session, reader, and transaction, so
advancing sets, finishing, dropping an unfinished reader, rollback-on-drop, and unknown
write/commit outcomes stay on the same connection. Dropping a reader or transaction waits for the
worker's cleanup unless the drop itself runs on that worker.

The `blocking` feature is unsupported on wasm32 and deliberately fails compilation there, including
when it is enabled transitively or by `--all-features`. Its dedicated worker threads and Tokio
network driver are not a Workers runtime; it also cannot adapt local, `!Send` futures. Other
providers may use the local async traits without this feature. D1's experimental `local::Pool`
database operations currently all fail closed before dispatch, so those traits do not provide usable
D1 operations. Its D1-specific `execute_uncertain_async` and `batch_uncertain_async` methods are
outside the DBAPI contract: dropping a dispatched future does not cancel its promise, and rejection
or dropping it leaves the outcome unknown. Callers must coordinate access and reconcile outcomes
independently; do not blindly retry. These methods make no hosted-behavior guarantee. The
runtime-neutral synchronous traits remain available without the adapter.

The adapter is blocking, not native, and it is not a general-purpose runtime. Pool-level calls run
the asynchronous pool's own `execute_async` and `fetch_async`, so wrapping the
asynchronous pool in `PolicyPool` preserves pool-level retry, deadline, and telemetry behavior.
Session and transaction calls never retry, matching the asynchronous contract. The adapter opens one
runtime per pool operation or session and refuses a call from its own worker thread with `Reentrant`
instead of deadlocking. Applications that already own an async runtime should await the async
traits, or put the blocking call on that runtime's blocking pool such as
`tokio::task::spawn_blocking`; calling a blocking adapter directly on an async worker can starve the
executor even though no nested `block_on` occurs.

A command that could not be enqueued returns `Connection`. If a command was enqueued and its reply
is lost because the worker panicked after dispatch, execute, fetch, and trusted script return
`WriteOutcomeUnknown`; commit returns `CommitOutcomeUnknown`. A reader whose worker is gone returns
`WriteOutcomeUnknown` from `next_row`, `has_next_set`, and `finish`, including when the worker died
before a later command could be enqueued, and it stays poisoned until dropped. Rollback reply loss
returns `Connection`. A known provider error is preserved, and a successful or known-error `finish`
ends the reader lifecycle. The adapter never turns a possibly dispatched write into a retryable
transport error.

## 🧩 Provider seams

Provider-specific acquisition belongs in that provider crate's own constructors and types; nothing
driver-specific is exposed here. `ImmediateTransaction` is the extension seam for SQLite-like
immediate starts without a driver enum or raw integer.

The root async traits and futures are `Send`. The `local` module mirrors them without that bound,
including `local::paging` and `local::paging::multi`, for browser Worker APIs such as Cloudflare D1
whose futures are `!Send`; moving a local future across threads still needs a separate safety and
lifecycle proof. The blocking adapter inherits the root `Send` bound and cannot adapt a `!Send`
provider. The existence of local traits does not imply that D1 DBAPI operations are supported; see
the fail-closed boundary above.

The synchronous traits are runtime-neutral. The blocking adapter is an optional feature with a
Tokio dependency; the default crate still contains no runtime, executor, or task.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
