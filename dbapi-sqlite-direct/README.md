# neotales-dbapi-sqlite-direct

SQLite provider for `neotales-dbapi`, built directly on [`rusqlite`](https://crates.io/crates/rusqlite)
instead of `SQLx`. It implements the same asynchronous and synchronous traits, option types, rows,
limits, and sanitized errors; no `SQLx` crate appears anywhere in its dependency graph.

The direct provider implements DBAPI's `SqlitePool` extension and is usable by
`neotales-migration-sqlite` and `neotales-x-audit-store-sqlite` through their
driver-neutral APIs. Those consumers retain the SQLx-backed provider overloads
behind the default `sqlx-compat` feature; disable their default features to keep
SQLx out of the normal runtime dependency graph. Integration tests exercise the
real migration and audit consumer flows against this provider as well as the
existing SQLx provider.

Use DBAPI's `SqlitePolicyPool` when telemetry or SQL capture is needed. It
retains the immediate-transaction extension and delegates policy behavior to the
same observer/capture implementation as `PolicyPool`; telemetry and SQL capture
remain opt-in, session/transaction statements are never retried, and observers
receive only sanitized fixed-cardinality events unless SQL capture is explicitly
enabled.

## Why rusqlite

- Safe wrapper over SQLite's C API. All `unsafe` lives in rusqlite/libsqlite3-sys; this crate is
  covered by the workspace's `unsafe_code = "forbid"`.
- Exposes the pieces this contract needs: an `InterruptHandle` for cancellation, declared column
  types for typed nulls, prepared-statement caching, raw positional binding, and an autocommit
  check for transaction state.
- `bundled` compiles the vendored SQLite source with `cc`, so no system package or preinstalled
  `libsqlite3` is required. The default build uses it. Disable default features to link the
  platform library instead.
- The selected version, `rusqlite 0.39`, uses `libsqlite3-sys 0.37`, the same native library
  version already linked by the workspace's `SQLx` SQLite users. Cargo's `links = "sqlite3"`
  rule permits only one version in a build; matching avoids a workspace-wide conflict.
- Native targets only. The `bundled` build compiles the vendored C source. WebAssembly needs a
  different rusqlite configuration and is out of scope for this provider.

## Usage

```rust
use std::time::Duration;
use neotales_dbapi::{DbError, Pool, QueryOptions, RowLimit, Session};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

# async fn example() -> Result<(), DbError> {
let provider = SqlitePoolProvider::connect(
    SqliteConfig::new("app.sqlite")
        .busy_timeout(Duration::from_secs(2))
        .max_connections(4),
)
.await?;
let mut session = provider.acquire_async().await?;
let row = session
    .fetch_one_async(
        "SELECT 1 AS answer",
        QueryOptions::new().limit(RowLimit::new(1, 64)?),
    )
    .await?;
assert_eq!(row.get::<i64>("answer")?, 1);
# Ok(())
# }
```

Synchronous-only callers use `SqlitePoolProvider::connect_sync`, `acquire_sync_sqlite`, and the
`neotales_dbapi::sync` traits. The concrete `SqliteSession` also exposes `begin_immediate`, the
synchronous counterpart of `ImmediateTransaction::begin_immediate_async`.

## Worker and pool model

`rusqlite::Connection` is `Send` but not `Sync` and uses interior mutability. Each physical
connection is owned for its whole life by one blocking worker thread. Callers exchange owned
commands and owned rows over channels:

- Async callers await a one-shot reply and never block the executor.
- Sync callers block on a reply.
- The pool bounds live workers by `max_connections`, reuses clean idle workers for `idle_ttl`, and
  pings a reused worker before handing it out.
- Opening a connection, durability readback, schema work, and statement execution all run on the
  worker thread.

A worker is **quarantined** after any dispatched failure, interrupted or abandoned operation, or
panic. It reports the result and then closes; its connection is never reused. Capacity remains
held by the worker thread until it actually stops, so dropping a future never frees a slot while
its statement may still be running.

Dropping a session with an open transaction closes the worker, which rolls the transaction back in
SQLite. Dropping a transaction without commit or rollback invalidates the session. A dispatched
`COMMIT` whose result is unknown is `CommitOutcomeUnknown`, never a rollback claim.

## Semantics

- **Positional parameters.** SQLite `?` and `?NNN` forms are accepted, but not mixed. Named forms
  (`:name`, `@name`, `$name`) are rejected as `Unsupported`. Parameter count must match the
  highest placeholder. SQL is never rewritten.
- **Booleans.** SQLite has no boolean storage class. `Bool` parameters bind as integer 0/1 and
  fetched integers remain `I64`.
- **Typed nulls.** A SQL null is typed from the column's declared affinity: `INT*` → `I64`,
  `REAL`/`FLOA`/`DOUB` → `F64`, `BLOB` → `Bytes`, `BOOL*` → `Bool`, anything else or no declared
  type → `Text`. An expression without a declared type therefore yields a `Text` null.
- **Limits.** `RowLimit` bounds rows, portable bytes (column-name UTF-8 plus payload), and result
  sets. Fetches materialize one bounded set, fail with `Overflow` rather than truncate, and return
  no partial rows. `has_next_set_async` returns `false`.
- **Foreign keys.** Enabled on every connection.
- **Statements.** Ordinary statement calls accept one executable statement, with optional trailing
  semicolons/comments. The lexical boundary check is conservative and rejects, for example, an
  ordinary `CREATE TRIGGER` body containing a semicolon. Use a trusted script for that.
- **Transaction control.** Standalone pool/session statement calls reject `BEGIN`, `COMMIT`,
  `ROLLBACK`, `SAVEPOINT`, and `RELEASE` before dispatch. Use the transaction traits.
- **Scripts.** Trusted scripts are unparameterized and may contain multiple statements. They are
  trusted input: do not manage transactions or create external side effects.

## No statement timeout

The portable default has no statement deadline, and SQLite cannot enforce one for arbitrary SQL.
An explicitly requested timeout is rejected with `Unsupported` before dispatch, never silently
ignored. `busy_timeout` is a separate SQLite lock-wait setting; it does not bound a long-running
statement or a user function.

## Cancellation and unknown outcomes

Dropping an async operation future interrupts the worker through `sqlite3_interrupt` and poisons
the session. The session cannot dispatch again because the interrupt could still land; it is
discarded on drop. The worker is not reused even if SQLite reports a clean rollback, because an
interrupt is not proof of rollback. SQLite may ignore an interrupt before a statement starts or
between script statements. A worker-lifetime cancellation latch is also checked before each command,
before preparing and stepping each script statement, and by SQLite's progress callback every 1,000
VM instructions. Scripts use SQLite's parser, including trigger bodies; no transaction is added and
no existing transaction is implicitly committed. Once a boundary observes cancellation, no next
statement is prepared or stepped. Cancellation is cooperative, not atomic with a boundary check:
preparation and a short statement racing the last check can still finish; earlier autocommit effects
can remain. Never infer non-application from cancellation. The latch is never reset: the lease stays
held until the worker stops, and a new session gets a new connection.

Dropping an async future means the caller receives no result. If the reply channel is lost for a
dispatched execute/fetch/script, the provider reports `WriteOutcomeUnknown`; a lost commit reply is
`CommitOutcomeUnknown`. Neither is retried or downgraded to a retryable transport error.
`sqlite3_interrupt` cannot interrupt a blocked user function, VFS call, or busy handler; busy
waits are bounded by the configured busy timeout.

## Durability

`SqliteDurability::Inherit` is the default and changes nothing. `SqliteDurability::Durable` opens
every connection with `journal_mode=WAL` and `synchronous=FULL` and reads both back. It also
records the main database file's canonical path (plus device/inode on Unix) and revalidates the
settings and identity before every write, transaction begin, trusted script, and `COMMIT`.

- A mismatch, unreadable policy, or changed identity discards the connection and returns a
  sanitized error.
- A failed pre-commit check does not dispatch `COMMIT` and is not `CommitOutcomeUnknown`.
- A durable trusted script may not contain `pragma`, `attach`, `detach`, or `load_extension`
  anywhere, including comments and literals. This is a conservative byte exclusion, not a parser,
  and may reject valid SQL. Durable sessions also reject `ATTACH`/`DETACH` as a leading command of
  a single statement.
- The policy requires a stable, file-backed path and a filesystem with SQLite shared memory and
  locking. It is check-time evidence, not inode pinning: an ABA replacement that restores the path
  before the next check is not detected. Non-Unix platforms compare the canonical path only.
- `Durable` is not proof against power loss, filesystem/storage write caches, or missing WAL
  checkpoints/backups. Those remain the application's responsibility.

## In-memory databases

Each physical SQLite connection to `:memory:` has its own private database. A multi-connection pool
would therefore hand out isolated databases under one name. In-memory names are accepted only with
`max_connections(1)`; the default multi-connection configuration and any durable policy are
rejected with `InvalidArgument`. The idle reuse window does not expire an in-memory worker, because
that connection holds the only copy of the data. Discarding the worker after an error, or closing
the provider, destroys the in-memory database.

`SqliteConfig` rejects **all** `file:` URIs, including ordinary file-backed URIs and URI options
such as `mode`, `cache`, and `vfs`. SQLite's URI parser can select temporary or in-memory databases
through multiple syntactic forms; rejecting the entire scheme avoids misclassifying them. Use a
plain filesystem path instead, or explicit `:memory:` under the restrictions above. On Unix, a
literal filename beginning with `file:` can be addressed with a path such as `./file:name`.

## Telemetry, retries, and logging

Wrap the provider in `neotales_dbapi::PolicyPool` for observer events, opt-in SQL capture, and
retries. The provider itself never retries. `PolicyPool` retries only standalone pool operations
with an explicit `ReplaySafety::ExplicitlyReplaySafe` assertion and a caller-supplied
`RetryRuntime`; session, transaction, script, commit, and unknown outcomes are never retried.
Observer events carry a fixed operation name, duration, sanitized outcome, and optional captured
SQL. They never carry parameters, rows, affected-row counts, driver messages, or connection
strings.

This crate has no `tracing` or `log` dependency and emits no records. rusqlite's `trace` feature is
off, so SQL text, bound values, rows, and file paths are not logged. `tests/lifecycle_logging.rs`
installs a permissive `tracing` subscriber and `log` logger, proves the capture works with a
positive control, and asserts that distinctive SQL, bind values, and paths never appear.

## Verification

```sh
cargo test -p neotales-dbapi-sqlite-direct
cargo test -p neotales-migration-sqlite -p neotales-x-audit-store-sqlite --all-targets
cargo test -p neotales-migration-sqlite -p neotales-x-audit-store-sqlite --no-default-features --all-targets
cargo clippy -p neotales-dbapi-sqlite-direct --all-targets -- -D warnings
cargo doc -p neotales-dbapi-sqlite-direct --no-deps
eng/check-sqlite-no-sqlx.sh
```

The test suite covers the core provider contract, typed values and nulls, placeholder validation,
row/byte limits, constraints, immediate transactions, synchronous traits, pool saturation, idle
reuse, worker discard, close/cleanup, durability readback and file replacement, async future
cancellation for execute/fetch/begin/commit/rollback, worker panics, and trace privacy. The
migration and audit consumer integration suites also run against this provider, covering migration
up/down/flatten, failure rollback, history drift and untracked-object rejection, plus audit append,
replay, conflict, persistence after reopen, and concurrent writes. The consumer crates keep SQLx
as a development dependency for compatibility-provider tests and inspection; the script checks
that SQLx is absent from each package's normal runtime dependency graph, not from test/dev graphs.
Tests use unique `tempfile` databases and need no database server.

## Limitations

- One OS thread per live physical connection. That is the price of a `!Sync` driver and is bounded
  by `max_connections`.
- Interruption is cooperative at the SQLite VM level. A blocked busy handler, user function, or VFS
  call is not interrupted promptly; the session and its capacity stay held until it returns.
- A short statement may complete before the progress callback observes cancellation. The outcome
  remains unknown; the session is discarded and is never reused.
- The lexical statement/placeholder screening is deliberately conservative, not a SQL parser.
- The in-memory database rule requires an explicit single connection.
- No statement deadline; explicit timeouts are rejected.
- The crate does not implement encryption, backup, WAL checkpointing, or an untrusted-SQL sandbox.
- The `bundled` build is the portable configuration: it was verified on Linux with `libsqlite3.a`
  produced from the vendored source. Windows and macOS use the same `cc` path but were not
  executed in this environment; the only platform-specific code is the Unix device/inode
  refinement of the durable file identity.
