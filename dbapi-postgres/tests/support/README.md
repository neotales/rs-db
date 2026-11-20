# PostgreSQL pool test support

The test-only `experimental_pool.rs` prototype has been removed and replaced by
the compiled [`experimental` API](../../src/experimental.rs). The integration
tests import that production module rather than a copy under `tests/support`.
See the [provider README](../../README.md) for its current contract and commands.
The canonical public `PostgresPool` now wraps this compiled implementation for
the local dbapi traits. The SQLx provider and its notice leak have been removed.
The lower-level API and public adapter are not production-readiness claims.

The caller must supply a finite, nonzero millisecond server statement ceiling
and a finite nonzero cancellation grace for the normal profile. A separately
constructed migration profile requires a caller-selected ceiling strictly longer
than that normal profile; there is no unlimited profile and no default. Before
each lease is returned, including after reuse, the pool sets and verifies
`statement_timeout` through parameterized `pg_catalog.set_config` on that
connection. Setup failure discards the connection without returning a lease.

`batch_deadline` can set a shorter, never longer, per-statement server limit
and has a separate caller-supplied total client deadline covering the setting,
execution and restoration. Its response may take up to the separately supplied
cancel grace after that deadline. PostgreSQL's timeout applies **per statement**,
including each statement in a multi-statement script, not to the total script.
SQL is trusted: SQL can run `SET`, `RESET`, `set_config`, or transaction commands
to change the policy. These settings are not a defense against hostile SQL.

Each physical connection has one permit held by its socket-owning driver future.
The socket future is dropped before that permit, including on task abort. A
lease owns the client and worker; dropping it aborts the worker. Only an
explicitly completed `recycle` can return a connection to the idle list:
it separately awaits `ROLLBACK`, clears all fork-owned type metadata and type
lookup statements (queuing their `CLOSE` messages), then awaits `DISCARD ALL`.
Clearing only the type map is insufficient: the fork also retains three
server-side prepared statements. No fork row or user statement may outlive
this reset. Interrupted operations leave
the lease poisoned; interrupted or failed reset drops the lease instead of
reusing it. Returned rows are detached because the fork's `Row` retains a
statement whose later drop can send `CLOSE` to a reused connection.

Closing the pool aborts idle workers and rejects/wakes new acquisitions.
It also interrupts a new or reused connection whose initialization is stalled;
the connection is discarded and its driver releases its permit when it exits.
Outstanding leases are **not forcibly revoked**: they are discarded when
their owners drop or recycle them. A leaked lease can therefore retain a
connection indefinitely. The profile ceiling bounds whole client acquisitions
and resets; callers should also bound plain operations with their own client
timeouts. An aborted worker is scheduled for cancellation;
`close` does not synchronously await worker termination.
Closing the client socket does not guarantee prompt server-side cancellation:
PostgreSQL can continue an already-running query and hold its locks until it
observes disconnection. A separate cancellation policy remains necessary.
The limit is on pool-owned **data connections**, not a strict bound on server
backends still finishing work after their client socket was closed.
Cancellation requests open separate sockets that are excluded from this limit
and never wait for the data-connection semaphore; their duration is bounded by
the caller-configured cancellation grace. Concurrent cancellation requests can
therefore temporarily make the total socket count exceed the data limit.
Repeated interruptions can produce **unbounded server-side work**; the max=1
integration test observes the original `pg_sleep` backend active after a
replacement is already acquired. The compiled experimental pool is not a
server-work limiter.

On an explicit client deadline, the experimental pool attempts a separate cancellation
request (verified TLS for TLS leases), bounded by the configured grace, then
returns `OutcomeUnknown` and poisons the lease. It never reuses that connection,
even if the request was sent: a cancellation request is not an acknowledgement
that the operation stopped and may arrive late. Dropping an arbitrary operation
future cannot run asynchronous cancellation; it poisons the lease and dropping
it closes the client connection. The server timeout can bound trusted statements
but does not confirm termination or bound the aggregate number of server backends.
On a lost connection or partition, preserving a strict server-work bound would
require withholding capacity indefinitely or an independent, authoritative
server-side admission/termination mechanism. This pool implements neither.

The old test-only reset pause and initializer injection have been removed.
`experimental_lifecycle.rs` uses wire peers/proxies to reject initialization or
withhold `DISCARD ALL` after rollback, testing reset cancellation, deadline, and
close wakeups against the compiled implementation. It does not simulate every
possible interruption while the server executes `DISCARD ALL`.
`experimental_pool.rs` also rewrites real fatal ErrorResponses with translated
severity text to check unknown-outcome classification and sanitized logging.
The independent-observer write regression proves a committed but unconfirmed
write is `OutcomeUnknown` on the experimental API and `WriteOutcomeUnknown`
through the public execute/fetch trait adapter. There is no legacy SQLx path.

`experimental_typed.rs` now exercises dbapi parameter binding, strict supported
types and typed nulls, detached duplicate-name semantics, exact aggregate row/
byte ceilings while streaming, arity rejection, and script/statement separation.
The independent-observer proxy also withholds `ReadyForQuery` after forwarding
rows and `CommandComplete`: neither typed execute nor fetch may report success.
Typed methods support autocommit or transactions owned through `Lease::begin`,
not adoption of raw/script-created transactions.
Polymorphic typed-parameter tests cover execute/fetch and typed nulls, unused
bindings, full-preparation failures, and empty/comment-only statement rejection.
Live probe-response proxies distinguish inference errors from fatal, absent-
severity, and transport failures, and verify that failed leases remain poisoned.

`experimental_transactions.rs` exercises connection-affine options and advisory
transaction locks, savepoint-safe inference/arity checks, scripts, aborted and
deferred-constraint commits, drop/discard, and hard row caps. The shared
`transaction_proxy.rs` withholds a matching command's ReadyForQuery (not an earlier
statement-drop CLOSE/Sync), covering begin/query/commit/rollback cancellation and
deadlines, retained-owner cancellation, and independently observed unconfirmed
commit. TLS live tests also exercise the owned transaction API and quiet canaries.

`postgres.rs` exercises the public dyn Pool/Session/Transaction traits, 25
concurrent clients on three connections, reset/reuse, TTL, deadline/close behavior
and logging canaries. `public_proxy.rs` tests unconfirmed COMMIT, interrupted
borrowed controls and observable background reset failures. No-runtime drop and
close/reset races are covered without production hooks. The dependency graph
gate rejects a normal SQLx dependency. TLS live tests now exercise the public API.

Main's newer convenience/PolicyPool core API, arbitrary type conversion,
fairness and production shutdown remain separate work. These tests are not a
production-readiness claim.
