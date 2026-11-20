# Quiet PostgreSQL dbapi provider

`PostgresPool` now implements this checkout's `neotales-dbapi::Pool`, `Session`
and borrowed `Transaction` traits over the quiet compiled transport. The old
SQLx provider and its known server-notice logging path have been removed from
the public implementation and normal dependencies. No global logger/filter is
changed. This is still an alpha library, not a production-readiness claim.

## Construction and migration

The URL-only `PostgresPool::connect(url)` constructor is removed. It could not
express required trust anchors and finite pooling/deadline policy without
inventing insecure or surprising defaults. Use:

- `PostgresPool::new(Config, VerifiedTls, PoolOptions)` for normal use. TLS is
  required and certificate chain/hostname verified against explicit DER roots,
  even if Config requests a weaker SSL mode. There is no plaintext fallback.
  SCRAM channel binding is supported: `channel_binding=prefer` uses
  `SCRAM-SHA-256-PLUS` only when the server offers it and a usable binding can
  be computed from the verified certificate; otherwise it falls back to plain
  `SCRAM-SHA-256`. `require` fails unless the bound mechanism is used, and a
  certificate with no usable signature digest (Ed25519 has none; RSA-PSS's
  parameter-dependent digest is not exposed by the safe OpenSSL binding) fails
  `require` explicitly rather than claiming a binding. `disable` sends plain
  `SCRAM-SHA-256`.
  This uses the OpenSSL adapter (a native system dependency), not SQLx's former
  rustls backend; see `dbapi-postgres-tls/README.md` in the `rs/lib` source
  tree.
- `PostgresPool::loopback_plaintext_for_tests(Config, PoolOptions)` only for
  disposable test servers. Every host must be a numeric loopback IP and every
  supplied hostaddr must be loopback too.
- `PoolOptions::new(max_connections, Profile, idle_ttl)` requires positive bounded
  capacity and finite integral-millisecond TTL. Profile constructors require
  caller-chosen statement ceiling and cancellation grace; no unlimited profile
  or implicit timeout is provided. Rustdoc includes a complete verified-TLS example.

Capability flags report transactions, native isolation selection, read-only
transactions and trusted scripts. Isolation supports READ COMMITTED (default),
REPEATABLE READ and SERIALIZABLE. READ UNCOMMITTED is explicitly rejected before
BEGIN, not silently upgraded. Default access is READ WRITE.

This is a breaking alpha API/lifecycle change, not an alias for the leaky
provider. Dropping a transaction or interrupting begin/commit/rollback invalidates
its Session instead of implicitly adopting a replacement connection.

## Deadlines and cancellation

Acquire, reset, begin, commit and rollback use the caller's finite profile ceiling
as their whole-client budget. Session/transaction statement operations also use
that budget by default. `QueryOptions::timeout(duration)` selects a shorter
positive whole-client budget; zero or values above the profile ceiling are rejected
before dispatch without damaging the Session/transaction.

The budget covers typed preparation, savepoint recovery where needed, dispatch,
and authoritative completion. It does not change the configured server
`statement_timeout`, which remains per statement:

- Expiry before user Execute is `Timeout`; no user statement was dispatched.
- Unconfirmed dispatched statement/fetch/script is `WriteOutcomeUnknown`.
- Unconfirmed dispatched COMMIT is `CommitOutcomeUnknown`.
- Interrupted begin/rollback never returns a reusable connection.
- Dropping an operation future cannot return an error; reconcile any possibly
  dispatched writes/commit. Its socket is discarded, not automatically retried.

The public adapter discards on deadline/cancellation; it does not launch detached
cancel or rollback requests. The older experimental `batch_deadline` escape hatch
can send a separate bounded cancellation request; such sockets are additional
to the data-connection limit. Sending cancellation is not outcome confirmation.

Capacity bounds **client data socket owners**, not PostgreSQL backends or server
work after disconnect. The socket-owning future is destroyed before its permit.
There is no strict aggregate server-work bound or total transaction-lifetime cap.
Idle locks remain caller-owned until verified closure/drop; leaking a handle can
retain them indefinitely. All SQL is trusted: statements/scripts must not manage
transaction ownership/savepoints/characteristics or override the timeout policy.
This is not an untrusted-SQL sandbox.

## Reuse, cleanup and idle TTL

Each Session exclusively owns one connection. Commit and rollback restore that
same connection to the borrowed Session only after verified closure. A dropped,
failed-begin, or uncertain transaction leaves the Session invalid; operations
return `Connection`. There is no hidden reacquisition or cross-transaction retry.

Dropping a healthy Session schedules explicitly owned, profile-bounded cleanup:
ROLLBACK, clear fork type/statement handles, then DISCARD ALL. Only successful
uninterrupted reset can return the connection to the idle pool. This clears
temporary objects, GUCs, advisory locks and prepared statements before reuse.

Drop Sessions and then await `pool.wait_for_cleanup()` to observe cleanup:

- It waits until registered reset work is quiescent; it does not close active
  owners. Concurrent drops can extend the wait, and drops after it returns are
  outside the barrier.
- The first sanitized reset failure is returned persistently, not silently lost.
  A failed reset discards and never replaces/masks a statement/commit result.
- No active Tokio runtime means discard and a recorded cleanup failure.
- `close()` rejects/wakes acquisitions, aborts idle workers and wakes pending
  reset tasks to intentionally discard. This planned `Closed` result is not a
  reset failure. Other failures remain reported.
- Close is not a join barrier and does not forcibly revoke outstanding owners.

Idle TTL is monotonic and checked lazily on checkout. An expired idle connection
is never reused; its driver must release its permit before replacement. There
is no background TTL reaper promising wall-clock disconnection without traffic.
Dropping the final pool/Session references also closes the underlying pool.

## Typed statements, bounded detached rows and outcomes

Native `$n` placeholders are preserved, including repeats and reordered indices.
Parameters support bool, i64, finite f64, UTF-8 text, bytes and matching typed nulls.
There is no SQL rewriting/interpolation. Typed execute/fetch require exactly one
statement: empty/comment-only/bare-semicolon input is rejected; the extended
protocol rejects multiple statements. Trusted scripts use a separate unparameterized
streaming path.

The fork's `prepare_typed` supplies types, so `SELECT $1` and typed nulls work
without caller-added casts. PostgreSQL accepts unused type declarations; a
shortened Parse probe verifies actual arity before Execute. Only after full typed
preparation succeeds may known nonfatal inference errors be accepted. In a
transaction this preflight is protected by SAVEPOINT, verified ROLLBACK TO and
RELEASE, preserving preceding writes and recovering expected inference errors.
Extra bindings dispatch no user writes. Known server execution errors abort the
transaction, allowing only commit/rollback. COMMIT's actual ROLLBACK tag yields
`Cancelled`, never false success; deferred constraint failures retain their category.

Fetch supports exact BOOL, INT8, finite FLOAT8, TEXT/VARCHAR, BYTEA and typed nulls.
Other result types fail before Execute. Duplicate names are accessible by ordinal;
name lookup returns `Ambiguous`. Returned dbapi rows retain no driver handles.

`query_raw` is pulled incrementally; aggregate row/byte caps are hard and never
truncate. Bytes include each row's column-name UTF-8 bytes and non-null binary
payload (bool 1, integer/float 8, text/bytes length). Null payload costs zero.
Checks occur before copying cells. Allocation overhead and incoming wire buffers are excluded;
column names are counted, but other metadata is not; this is not a process-memory/wire-message
limit. One prepared statement has one result set, so `has_next_set_async` returns `false` and the batch
fits any validated `max_sets`.
Cap/decode failure before completion is `WriteOutcomeUnknown`: fetch can write,
and the provider will not drain unbounded results or pretend it was read-only.

Typed statements and transaction controls require CommandComplete plus appropriate
ReadyForQuery state, not rows or either message alone. An empty trusted script can
complete with EmptyQueryResponse and ReadyForQuery. Transaction control is recognized from protocol metadata,
not SQL prefixes; earlier script control tags are retained even if a later command
errors. Fatal/missing-severity errors and explicit unknown-outcome SQLSTATEs are
conservative. Returned errors keep no SQL, values, server text or source chain.
Pool/transaction Debug is redacted; detached row Debug intentionally exposes data
to its caller, but the provider never logs it.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. The caller supplies the statement, typed parameters, continuation, and the
explicit read-only/replay-safe/authorized assertion; the adapter never rewrites SQL or infers that a
`SELECT` is read-only. The adapter supplies a hard `RowLimit` for the published page plus one
lookahead row, drains and rejects extra result sets, and rejects an explicit retry request.

This statement shape runs against a live server in this crate's own lane:

```sql
SELECT tenant, seq, body FROM events
WHERE tenant = $1 AND (tenant, seq) > ($2, $3)
ORDER BY tenant, seq LIMIT $4;
```

`dbapi-postgres/tests/sql_page_source_live.sh` requires a Cargo toolchain, a reachable Docker
daemon, and the digest-pinned `postgres:17-alpine` image present locally or pullable by Docker. From
`rs/lib` it lists `tests/sql_page_source_live.rs`, refuses to pass unless
`postgres_sql_page_source_contract` is present, starts the container with no published host port,
connects through a loopback listener, asserts the cross-tenant and single-tenant reads including
exact flattened order, page counts, and request counts, and removes the container. It reads no
credential or environment opt-in.

Only this new page-source test is `#[ignore]`d; it does not make the rest of the crate fixture-free.
The crate's other nonignored runtime tests still require the external PostgreSQL server described
below. Selecting this test target skips the ignored case instead of running it:

```bash
cargo test -p neotales-dbapi-postgres --test sql_page_source_live
```

The `data` pipeline runs this script in its own Linux job in both the monorepo and the `rs/lib`
satellite. This is not a claim about other platforms, other PostgreSQL versions, or a hosted
PostgreSQL service.

`$4` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page size.
`(tenant, seq)` must be unique and `NOT NULL` for a total keyset order. The plan must return a
continuation exactly when the lookahead row exists and must reject a token from another plan or
tenant.

## Integration boundary and verification

`experimental` remains an explicit lower-level API; the canonical public trait
adapter uses it. Public session and transaction operations accept owned `QueryOptions`;
the optional `PolicyPool` supplies telemetry and replay-safe pool-level retries.
Broader upstream fork conformance remains separate work.

From `rs/lib`, with disposable trust-authenticated PostgreSQL 17 at
`127.0.0.1:55433`:

```sh
cargo fmt --all --check
cargo clippy -p neotales-dbapi-postgres -p neotales-dbapi-postgres-tls -p neotales-tokio-postgres --all-targets -- -D warnings
cargo test -p neotales-dbapi-postgres -p neotales-tokio-postgres -- --test-threads=25
cargo test --doc -p neotales-dbapi-postgres -p neotales-dbapi-postgres-tls -p neotales-tokio-postgres
RUSTDOCFLAGS='-D warnings' cargo doc -p neotales-dbapi-postgres -p neotales-dbapi-postgres-tls -p neotales-tokio-postgres --no-deps
```

The `data` pipeline provides that disposable server as a pinned `postgres:17`
service on loopback and runs the same crate tests in a Linux job. That job is
separate from the fixture-free format, clippy, doctest and rustdoc job. The
fixture-free job runs this crate's doctests, which are all `no_run`, but none of
its runtime tests: a green fixture-free `cargo test` is not evidence for this
provider because its non-ignored tests connect to the server above. A doctest
that needs a connection belongs in a live job. The TLS, upstream-conformance
and paged SQL source lanes are separate jobs as well.

After stopping that server run `dbapi-postgres-tls/tests/live.sh`, which
creates/cleans its own TLS-only server and synthetic certificates. It runs the
adapter tests and this crate's ignored `tests/tls_live.rs` provider regression.
That covers the public dyn Pool adapter, borrowed transactions, PID-affine
verified TLS, SCRAM channel binding (`SCRAM-SHA-256-PLUS` under `require`,
`prefer`/`disable` behavior, wrong-binding rejection and the unsupported-digest
fallback), reset/reuse, rejected roots/hostnames and quiet logging. Public
tests also exercise 25 concurrent clients sharing three data connections, TTL,
cleanup failure and no-runtime cleanup, cancellation, and independently
observed unconfirmed writes/commit. Reviewed lower-level transport tests remain
in place.
