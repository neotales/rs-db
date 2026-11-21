# neotales-dbapi-sqlite

SQLite provider for `neotales-dbapi`, backed by `SQLx` with statement logging disabled.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi neotales-dbapi-sqlite
cargo add tokio --features macros,rt-multi-thread
```

## What you can do 🔧

**Open a file-backed database and run a transaction.** `SqlitePoolProvider::connect` opens the
pool; `acquire_sqlite` returns an exclusive session with immediate-transaction support. The
database file is created when absent. This is a complete program.

```rust
use neotales_dbapi::{DbError, Pool, QueryOptions, RowLimit};
use neotales_dbapi_sqlite::{SqliteConfig, SqliteDurability, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new("audit.sqlite")
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
    println!("{count} audit rows");
    Ok(())
}
```

Run the same connection shape without the durability policy in
[`examples/connect_and_query.rs`](examples/connect_and_query.rs), or run this durable program as
[`examples/durable_transaction.rs`](examples/durable_transaction.rs):

```sh
cargo run -p neotales-dbapi-sqlite --example connect_and_query
cargo run -p neotales-dbapi-sqlite --example durable_transaction
```

**Choose the durability policy per pool.** `SqliteDurability::Inherit` changes nothing.
`SqliteDurability::Durable` opens every connection with `journal_mode=WAL` and
`synchronous=FULL`, reads both back, and revalidates them before every write, transaction,
trusted script, and `COMMIT`.

**Use a native blocking consumer.** The synchronous traits and `blocking::BlockingPool` work
without changing the provider. Explicit per-statement timeouts are rejected with `Unsupported`;
the configured busy timeout is a SQLite lock wait, not a statement deadline.

## Scope

Configure it with `SqliteConfig` (file path, busy timeout, maximum connections, and durability
policy), then open it with `SqlitePoolProvider::connect`. No SQLx type appears in the public API.
`acquire_sqlite` returns a session implementing `ImmediateTransaction`, whose immediate begin
issues `BEGIN IMMEDIATE` on that same exclusive connection.

SQLite inherits the portable no-statement-timeout default. Its configured busy timeout is a
separate SQLite lock-wait setting, not a deadline for arbitrary SQL. Explicit per-statement
timeouts are rejected with `Unsupported` before dispatch.

## Durability policy

`SqliteConfig::durability` selects a per-pool policy. The default, `SqliteDurability::Inherit`,
changes nothing and leaves SQLite's own journal mode and `synchronous` setting in effect.

`SqliteDurability::Durable` opens every physical connection with
`journal_mode=WAL` and `synchronous=FULL` in the SQLx connect options. At open and before every
`execute_async`, `fetch_async`, transaction statement, `begin_async`, `begin_immediate_async`, trusted script, and `COMMIT`,
the provider reads `pragma_journal_mode`, `pragma_synchronous`, and `pragma_database_list`, and
compares the main-file identity with the one recorded at open. A different value, a changed file
identity, a non-file-backed main database, or a failed read discards that connection and returns a
sanitized `DbError` (`Unsupported`, `Busy`, `Connection`, or another driver-derived kind); no
unverified connection is used. A replacement connection opened after a discard is checked at its
acquisition as well.

A failed pre-commit check does not dispatch `COMMIT`. It is not reported as
`CommitOutcomeUnknown`; the connection is discarded and SQLite rolls back its open transaction when
the connection closes. A single `PRAGMA` statement is permitted because the migration backend sets
`busy_timeout` and the spill reads `encoding`. If it changes the durability policy, the next
boundary fails closed and the session must be discarded; a restoring `PRAGMA` on the same session
is not dispatched.

### Durable script constraint

A trusted script executes several statements before returning, so it could change the policy and
write in one call. Durable sessions reject a trusted script that contains `pragma`, `attach`,
`detach`, or `load_extension` anywhere, including comments and string literals. This is a
conservative byte exclusion, not a SQL parser: SQLite requires the literal keyword for those
statements, so excluding it prevents them from running, and a script that uses one of those words
as an ordinary identifier or literal is also rejected. Durable sessions reject `ATTACH`/`DETACH`
as the leading command of a single statement. Attached databases are outside this policy.

The policy requires a stable, file-backed database path and a filesystem that supports SQLite
shared memory and locking. `:memory:`, `file:` URIs with `mode=memory`, and the `memdb` VFS are
rejected with `InvalidArgument` before any connection is opened. A competing connection can stop
the initial change to WAL; the open then fails closed instead of retaining the previous mode.
A custom memory-backed VFS cannot be recognized from its name, so use a real file-backed path.
Prefer an absolute path: the main-file identity is resolved from the path at each check, so a
changed working directory or a retargeted symlink makes the check fail closed. The check is
check-time evidence, not inode pinning: an ABA replacement that restores the path before the next
check is not detected, and non-Unix platforms compare the canonical path only.

`Durable` is a connection-level policy, not proof against power loss. Filesystem and storage-device
write caches, WAL checkpointing, backups, and retained event copies remain the application's
responsibility. It also does not change unrelated pool settings such as the busy timeout, maximum
connections, page cache, page size, foreign keys, or statement logging.

```rust
use neotales_dbapi::DbError;
use neotales_dbapi_sqlite::{SqliteConfig, SqliteDurability, SqlitePoolProvider};

#[tokio::main]
async fn main() -> Result<(), DbError> {
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new("audit.sqlite")
            .durability(SqliteDurability::Durable),
    )
    .await?;

    // Use the provider here. Dropping it closes its idle connections.
    drop(provider);
    Ok(())
}
```

Fetches detach all rows. The aggregate byte limit counts UTF-8 bytes in every returned column
name plus portable value payload bytes: bool 1, integer/float 8, text/blob length, and typed
null 0. The byte, row, and result-set ceilings are checked as each row is decoded, before it is added to
the result. An excess fails the entire fetch; no partial rows are returned. One executable statement
has one result set, so `has_next_set_async` returns `false` and the batch fits any validated `max_sets`.

SQLite dynamic storage classes are decoded strictly: INTEGER as `i64`, REAL as `f64`, TEXT as
UTF-8 `String`, BLOB as bytes, and NULL as a typed null inferred from the result column's declared
type affinity. When SQLite supplies no declared type (for example, a bare `NULL` expression), the
provider uses `Text`. SQLite has no distinct boolean storage class. Parameters declared `Bool` bind
as integer 0/1; returned integers are `I64`, not booleans.

An installed SQLite progress handler replaces any existing handler. It stays installed while an
internal pooled connection is idle and is replaced on the provider's next acquisition; callers
cannot access this private pool. Dropping an unfinished operation requests interruption and
discards its connection; progress callbacks interrupt VM
execution, not blocked user functions, VFS calls, or a busy handler. Busy timeout bounds lock
waits, but prompt cancellation is not guaranteed. A canceled/failed transaction connection is
discarded to avoid reuse after implicit rollback or stale SQLx transaction state. Discarding the
last connection can destroy a private in-memory database.

Ordinary `execute_async` and `fetch_async` accept exactly one executable statement. A conservative lexical
check recognizes common SQLite quote and comment delimiters but is not a SQL parser; ambiguous or
malformed input is rejected. It also rejects ordinary `CREATE TRIGGER` definitions containing
body semicolons. Ordinary parameterized statements accept either anonymous `?` placeholders or
indexed `?NNN` placeholders, but reject SQL mixing the forms. Use trusted script SQL for trigger
definitions or intentional multi-statement execution; scripts must not manage transactions,
attach databases, or create external side effects.

Standalone pool/session `execute_async` and `fetch_async` reject transaction-control commands before dispatch;
use the transaction interfaces instead. Leading commands must be unquoted ASCII tokens after
ASCII space, tab, CR, LF, form feed, or line/block comments. Other prefix forms, including BOM and
vertical tab, fail closed with `InvalidArgument`, even when SQLite accepts them for ordinary SQL.
This is lexical screening of the leading command, not a general SQL parser.

Validation failures detected before dispatch—including unsupported options, NUL-containing SQL,
placeholder/parameter count mismatches, mixed placeholder forms, and multiple statements—leave a
live session/transaction unchanged.
Dispatched statement errors or interruption invalidate and discard the connection; callers must
not assume the transaction remains usable. A failed/interrupted transaction is never committed.
If a dispatched standalone write's result itself is unknown, the provider returns
`WriteOutcomeUnknown`. If a dispatched commit's result itself is unknown, it returns
`CommitOutcomeUnknown`. Neither category is retried or downgraded to a cancellation/deadline.
Standalone fetches can execute `INSERT ... RETURNING`; their transport/interruption failures
conservatively report `WriteOutcomeUnknown` too, rather than assuming fetch means read-only.

For per-pool convenience methods, policy, and telemetry, wrap the provider in
`neotales_dbapi::PolicyPool`. SQL capture is opt-in because SQL literals and
comments can contain secrets; SQLx statement logging remains disabled. Policy
events never include parameter values, rows, affected-row counts, driver
messages, or connection strings. `RetryPolicy::Disabled` is the default.
Standalone pool `execute_async`/`fetch_async` retries require an explicit
`ReplaySafety::ExplicitlyReplaySafe` per-operation assertion and caller-supplied
`RetryRuntime`; pool/session transaction methods, scripts, and commit outcomes
are never retried. Busy and Connection errors are candidates; Timeout is opt-in.
Acquisition and transaction begin/commit/rollback boundaries are not separately
observed.

## Keyset paging example

`examples/keyset_paging.rs` runs the provider-neutral multi-request pager against an in-memory
database with a hand-written `PageSource`. It shows both a cross-tenant administrative scan and a
tenant-scoped scan whose authorized tenant is a separate predicate, not the continuation cursor. The
reusable source is `examples/support/keyset_source.rs`, exercised by `tests/keyset_paging.rs`; it
requires a unique `NOT NULL` ordering key and authors SQLite `?` placeholders. Its `source_bytes`
allowance bounds the portable encoded bytes of the rows accepted for one request, not provider
allocations or buffering.

`examples/sql_page_source.rs` is the generic counterpart: a `SqlPagePlan` in
`examples/support/sql_keyset_plan.rs` supplies the same SQL, typed parameters, and scoped
continuation, and `neotales_dbapi::paging::multi::sql::SqlPageSource` owns the hard `RowLimit`,
lookahead removal, single-result-set completion, and no-retry behavior. `tests/sql_page_source.rs`
exercises the same plan against SQLite. Its crafted tokens prove that a foreign plan and a cursor
outside the authorized tenant are rejected.

SQLite, libSQL, MySQL, and MariaDB use `?` placeholders. PostgreSQL uses `$1`-style placeholders,
SQL Server uses `@P1`-style names and `TOP (@P3)`, and ClickHouse uses `{p1:Int64}`-style names.
The SQLite forms are exercised by the fixture-free `tests/sql_page_source.rs`. Ignored live tests in
the other provider crates exercise their own native forms against pinned containers; each provider
README names its test and image, and a plain `cargo test` does not run them.

## Logging boundary

Every pooled connection is opened with `ConnectOptions::disable_statement_logging`,
so SQLx's `sqlx::query` statement and slow-statement events stay off at every
level. The provider itself emits no `tracing` or `log` records.

SQLx 0.9.0 still logs from the pool lifecycle, and this provider cannot silence
that without replacing SQLx. For this configuration the reachable lines cannot
carry user data:

- Idle-acquire and release pings (`sqlx-core` `pool::inner::check_idle_conn` and
  `pool::connection::return_to_pool`) log the ping error. SQLite's `ping` only
  round-trips a command to its worker thread, so the only possible error is
  `sqlx::Error::WorkerCrashed`, whose text is the fixed string "attempted to
  communicate with a crashed background worker". SQL text, bound values, SQLite
  messages, and file names cannot reach it.
- Closing a pooled connection (`sqlx-core` `pool::connection::Floating::close`)
  logs a close error at debug level. `optimize_on_close` is left disabled, so
  closing runs no SQL; the only failure is the same worker handshake.
- Slow-acquire warnings (`sqlx-core` `pool::inner`) appear only when a pool
  acquire exceeds the default two-second threshold and carry the elapsed and
  threshold durations, with no SQL, parameter values, driver messages, or file
  names.
- `after_connect`, `after_release`, and `before_acquire` log lines embed
  caller-supplied hook errors. The provider installs no hooks.
- `sqlx-sqlite` has one error-level line, "failed to rollback cancelled
  transaction", that embeds a SQLite error message
  (`sqlx-sqlite` `connection::worker`). It is reachable only through SQLx's
  `Connection::begin`/`Transaction` commands. This provider never calls that
  API: it issues `BEGIN`, `COMMIT`, and `ROLLBACK` as ordinary statements and
  tracks transaction state itself. Keep it that way; adopting SQLx transactions
  would make that line reachable.

Driver errors returned to the provider can contain SQL text (for example, a
syntax error echoes the offending token), but the provider converts them into
`DbError` values with no message or source and never logs them.
`tests/lifecycle_logging.rs` captures every `tracing` event and `log` record
under an unrestricted subscriber, with a stock SQLx pool as the positive
control, and asserts these boundaries.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
