# neotales-migration-clickhouse

Experimental ClickHouse migration backend over `neotales-dbapi-clickhouse`.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration neotales-migration-clickhouse --features experimental-native-migrations
```

## What you can do 🔧

**Opt in explicitly.** Without the `experimental-native-migrations` feature, every operation
validates locally and then returns `Unsupported` before acquiring a session. Cargo features are
additive, so an ordinary dependency or `--all-features` can enable this path.

```rust,no_run
use std::time::Duration;
use neotales_migration::{Migration, Migrations};
use neotales_migration_clickhouse::ClickHouseBackend;

async fn migrate(
    pool: &neotales_dbapi_clickhouse::ClickHousePoolProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    let backend = ClickHouseBackend::new("app", "schema_history", Duration::from_secs(5))?;
    let migrations = Migrations::new(vec![Migration::new(
        1,
        "create-users",
        "CREATE TABLE users (id Int64) ENGINE=MergeTree ORDER BY id",
        Some("DROP TABLE users".to_owned()),
    )?])?;
    backend
        .up(pool, "app", &migrations, "binding-fingerprint-v1", &["users"])
        .await?;
    Ok(())
}
```

**Read the recovery contract first.** ClickHouse has no interactive transactions, so schema and
ledger writes are not atomic. A dirty marker is persisted before each script and recovery is
manual and exclusive.

**Do not treat a passing run as stability.** The live recovery gate has an unresolved timeout;
the README's diagnostic section describes the bounded evidence, not a guarantee.


## Scope

> **Experimental, disabled by default:** opt in with the Cargo feature
> `experimental-native-migrations`. Without it, operations return `Error::Database` with
> `ErrorKind::Unsupported` after local validation and before acquiring a session. Cargo features
> are additive: another dependency or `--all-features` can enable this feature.
> Independent safety review is required before production use.

The caller must trust the SQL and provide complete managed-object declarations. Every writer
must share a single coordination domain covering both ledger and effects, or use a correctly
scoped external lock. The table lock is not cluster-wide fencing. Ledger and effects are not
transactional together; recovery requires an exclusive manual window in which the old owner
cannot resume and all its outstanding work has stopped. The backend is tested against 24.8 only;
it makes no power-loss durability guarantee.

As a fail-closed prerequisite, all up and down definitions are locally preflighted before
session acquisition. Currently each must be a single statement accepted by the provider's
no-count API; multi-statement scripts and unbound placeholders are rejected. This preflight
does not establish that server execution will succeed or that SQL is safe.

ClickHouse backend for [`neotales-migration`] over [`neotales-dbapi`]. It is
tested against ClickHouse 24.8 LTS only.

[`neotales-migration`]: https://docs.rs/neotales-migration
[`neotales-dbapi`]: https://docs.rs/neotales-dbapi

## No transactional promise

ClickHouse has no interactive transactions and no transactional DDL or DML.
This provider therefore does **not** promise that schema changes and ledger
writes are atomic. Instead, every script is preceded by a persisted dirty marker
and followed by a ledger insert:

1. the dirty marker (`direction`, `version`, checksum, `stage=started`);
2. the trusted migration script, one statement at a time;
3. `stage=script_done` (or `script_failed` after an error);
4. the history row insert or tombstone;
5. clearing the dirty marker.

Failures before a dirty marker can return configuration, acquisition, lock, or
preflight errors without a dirty record. After a dirty marker is persisted, a
confirmed lock release allows a later operation to report [`Error::Dirty`].
An uncertain write can retain the lock; later lock attempts instead return
[`Error::LockOutcomeUnknown`] before inspecting the dirty marker. Quiesce writers
and establish that server work has stopped before inspecting and reconciling.
The provider never retries an uncertain outcome. If the script fails and the follow-up `script_failed`
stage write also fails, [`Error::FailedStage`] preserves both sanitized
outcomes; the lock is retained when either outcome is uncertain.

Stages mean:

| Stage | Meaning |
| --- | --- |
| `started` | The marker was committed. The script may not have run, may be running, or may have partially run. |
| `script_done` | The script returned success. The history row may or may not have been synchronized. |
| `script_failed` | Best effort: the script returned an error. Earlier statements may already have applied. |

## Recovery

If no lock is retained, inspect the schema and ledger with [`ClickHouseBackend::state`], then
call [`ClickHouseBackend::reconcile`]. If a lock is retained, `state()` and `reconcile()` cannot
acquire it. Recovery requires an **operator-managed exclusive window**:

1. Quiesce **all cooperating migration writers**, including scheduled jobs and instances on
   every relevant host. Positively establish that the original lock owner has stopped and cannot
   resume. The lock stores no owner identity, lease, or fencing token; an absent query in
   `system.processes` does **not** prove that the owner is dead—it may be between queries.
   Dropping its lock could allow overlapping migrations and harm the live owner.
2. Verify that all outstanding server-side work from the owner, including lock, ledger and script
   operations, has stopped. Inspect `system.processes` using appropriate identifiers and, if
   necessary, stop a verified query and recheck. This check is necessary but is not proof that
   the owner process has stopped.
3. Inspect the schema and ledger read-only as shown below. While all writers remain quiesced,
   carefully remove the exact lock table only after both owner and server work are confirmed
   stopped. Then use `state()` and `reconcile()` and verify the result before restarting writers.

If any of these conditions cannot be established, **leave the lock in place** and escalate; the
backend never automatically removes or steals it.

Inspect the schema and ledger **read-only** through an independent administrative connection.
For example, for database `app`, ledger `schema_history`, and module `app`:

```sql
SELECT module, fingerprint, barrier_version, barrier_checksum,
       dirty_version, dirty_checksum, dirty_direction, dirty_stage, revision
FROM `app`.`schema_history_meta` FINAL WHERE module = 'app';
SELECT module, version, checksum, revision, is_deleted
FROM `app`.`schema_history` FINAL WHERE module = 'app' ORDER BY version;
```

`FINAL` shows logical history: a latest deletion tombstone is hidden. To inspect physical
revisions, query the history table without `FINAL`; ordinary merges may remove older versions.
Do not manually clean up tombstones or mutate ledger tables. Revision allocation reads the maximum
physical history and metadata revisions so a later reapplication outranks retained tombstones.
The process-restart tests preserve the container's writable layer; they do not establish survival
of host power loss, filesystem failure, or loss of that layer. `EndOfStream` is not a disk-fsync
guarantee.

Use the exact validated database and table identifiers from backend configuration and
[`ClickHouseBackend::lock_table`]; the examples are not generic text templates. For other
module names, use a bound `String` parameter rather than interpolating untrusted text. Verify
the dirty stage, actual schema, and history, and follow the exclusive-window steps above before
removing the lock and reconciling:

- `ReconcileOutcome::Applied` — the intended effect is present. For `up`, append
  the history row (idempotently). For `down`, remove the reverted row.
- `ReconcileOutcome::NotApplied` — the intended effect is absent. For `down`, the original
  schema effect remains: append a replacement if its history row is missing, otherwise leave
  history unchanged, then clear the marker. For `up`, reject an already-recorded history row;
  otherwise clear the marker without adding history. A conflicting later history row is rejected.

The provider cannot verify arbitrary DDL. `Applied` for a `started` stage is the
operator's assertion that the complete script took effect. Review the schema
first.

For the narrow interrupted-baseline case where an `up` dirty marker and the matching baseline
history row are persisted but its barrier is missing, `Applied` restores the barrier before
clearing the dirty marker. The marker may be `started`, `script_failed`, or `script_done`:
an earlier `Applied` reconciliation may itself have been interrupted after writing the row.
`NotApplied` is rejected while that history row exists; an operator must not use it to discard
a recorded baseline. Normal history validation remains strict.

## Locking and ownership

Migration writes use the concrete ClickHouse session's no-count API, not the
driver-neutral `PolicyPool`. Configure `ClickHouseTelemetry` on the concrete
provider to observe lock, ledger, and script writes. It emits sanitized `DbEvent`
values suitable for tracing or metrics; SQL capture is separately opt-in and may
contain secrets. Reads and acquisition are outside this provider observer's scope.
No automatic retries are performed.

The target database must use the Atomic engine. The provider acquires a mutex
by creating a `Memory` table named `<table>_lock` in that database. ClickHouse
DDL for an Atomic database is atomic. A duplicate-CREATE error, like any post-dispatch
failure, returns [`Error::LockOutcomeUnknown`]; the backend does not classify contention
or retry. The timeout bounds one create attempt. Only a confirmed create is considered
owned and eligible for release. Release drops the table. The lock is per ClickHouse
server, not cluster-wide: there must be one writer across the entire domain of
shared migration effects and ledger state, or correctly scoped external coordination
covering that domain. One writer per server is sufficient only when their effects
and ledgers are truly independent.

This is a deliberately fail-closed lock, not a lease. A cancelled operation, a
process crash, or a lost connection can leave the lock table behind. In
particular, when a migration script returns `WriteOutcomeUnknown`, a native
dispatch may have timed out or lost its connection while ClickHouse keeps
executing it.
The provider therefore **retains the lock** instead of releasing it, because
releasing would let another writer overlap a possibly running script. During the exclusive
window described above, an operator can inspect server work using a carefully chosen filter,
verify the actual query ID, and if necessary:

```sql
SELECT query_id, query FROM system.processes WHERE query LIKE '%<your table>%';
KILL QUERY WHERE query_id = '<query_id>';
```

An empty result does not prove the owner stopped. Only after quiescing all writers, positively
establishing the owner stopped, confirming outstanding server work stopped, and performing the
read-only inspection above may an operator remove the exact retained table named by
[`ClickHouseBackend::lock_table`]. Keep writers stopped through reconciliation. The provider does
not automatically steal a stale lock because it cannot distinguish a dead owner from a slow live
one. A concurrent `up`, `down`,
`reconcile`, or `state` during that interval returns [`Error::LockOutcomeUnknown`].

The ledger is two `ReplacingMergeTree` tables: `<table>` and `<table>_meta`.
Reads use `FINAL` so unmerged inserts are not mistaken for multiple rows. Both
tables carry a format marker in the table comment. The provider verifies the
exact engine, comment, primary key, column order, column types, default-kind
absence, primary-key membership, `engine_full`, sorting key, and empty partition key.
The exact ClickHouse 24.8 catalog engine definition includes `index_granularity = 8192`;
different defaults are rejected rather than silently adopted. An existing pair that does not match is
rejected, never adopted. Exactly one existing table is a partial ledger and also
fails closed.

`up` registers a fresh module only after every declared managed object is absent.
A registered-but-empty module is checked again before its first script. `down`
requires a registered module. `flatten` is unsupported: replacing history plus a
baseline cannot be atomic without transactions.

If module metadata is absent but physical history remains, including a retained
deletion tombstone hidden by `FINAL`, the module is inconsistent and cannot be
registered or reconciled automatically. Quiesce writers and investigate rather
than adopting or deleting the rows. A partially created ledger pair likewise
requires operator investigation; it is not automatically bootstrapped over.
Interrupted reconciliation with intact dirty metadata follows the recovery process
above; missing metadata is not treated as an interrupted reconciliation.

## Trusted SQL and trusted database objects

This is not a sandbox or SQL validator. Every migration script and every
database object is trusted:

- migration SQL may perform arbitrary DDL/DML and may have nontransactional
  effects;
- dictionaries, materialized views, table functions, UDFs, and executable
  expressions can change schema, session state, or external state outside the
  script text;
- the DBAPI provider rejects transaction control and multi-statement fetches, but a
  trusted `SELECT` can invoke a table function or UDF;
- this backend cannot dispatch `execute` or `execute_trusted_script` at all, because
  the native provider rejects both with `Unsupported`.

Review and own every installed object. Never feed this backend untrusted SQL or
untrusted database objects.

## API

```rust,no_run
use neotales_migration::{Migration, Migrations};
use neotales_migration_clickhouse::ClickHouseBackend;
use std::time::Duration;

async fn example(pool: &neotales_dbapi_clickhouse::ClickHousePoolProvider) -> Result<(), Box<dyn std::error::Error>> {
let backend = ClickHouseBackend::new("app", "schema_history", Duration::from_secs(5))?;
let migrations = Migrations::new(vec![
    Migration::new(1, "create-users", "CREATE TABLE users (id Int64) ENGINE=MergeTree ORDER BY id", Some("DROP TABLE users".to_owned()))?,
])?;
let state = backend
    .up(pool, "app", &migrations, "binding-fingerprint-v1", &["users"])
    .await?;
assert_eq!(state.applied_count(), 1);
Ok(()) }
```

The example requires the `experimental-native-migrations` feature. Without it the backend
fails closed before acquiring a session or creating a lock. Ledger text readers validate
ClickHouse `String` bytes as UTF-8; invalid UTF-8 is rejected, never replaced.
Registration does not run migration SQL; `state` returns `None` for an unregistered
module without creating ledger tables and includes a dirty marker when recovery is required.

## Tests

The live script starts a digest-pinned ClickHouse 24.8 container through `testcontainers`, publishes
no ports, uses a random nonempty password from the OS CSPRNG, and connects through a loopback-only
proxy. The default script checks the disabled ordinary dependency build; the experimental lane
exercises the recovery contract in an enabled ordinary dependency build.

```sh
cargo test -p neotales-migration-clickhouse --all-targets
migration-clickhouse/tests/live.sh
migration-clickhouse/tests/live.sh --experimental
migration-clickhouse/tests/live.sh --test-harness
python3 migration-clickhouse/tests/packaged.py
```

The unit harness uses the same explicit feature gate as the ordinary dependency; `cfg(test)`
does not bypass it. The packaged-consumer check archives the tracked source closure (including
the patched native driver), extracts it outside the checkout, and runs both feature states
from an independent consumer workspace. This is a **source-archive** check, not verification of a
registry `.crate`: `cargo package` currently refuses the migration crate's unversioned path
dependencies. It requires Python 3.12+, Docker, cached Cargo dependencies, and 8 GiB free in
the home cache. It must not be described as registry-publication verification.

Both ordinary dependency and ignored exact unit-harness contracts use fixed case labels,
including bootstrap and every restart/recovery case. Test-only backend/session forwarding
wrappers retain the last 128 enum-only phase/result events per case; results, deadlines, and
assertions are unchanged. Spawned writers inherit their case. On panic/interruption the case
guard emits the bounded trace, including typed error class and database kind when available,
without inspecting panic payloads or formatting backend/driver errors. Arguments, SQL, rows,
credentials, and server log bodies are never retained by this trace.

`LockOutcomeUnknown` identifies an unconfirmed lock CREATE, not its transport/server cause.
An unlocalized database error stays unlocalized; an assertion or interruption without a typed
error is labeled as such. API-phase elapsed times include that API's preceding acquisition and
validation work, not just its CREATE deadline. Instrumentation adds bounded bookkeeping and
can still perturb timing. The live shell selects its ignored test with `--exact --nocapture`.
Owned containers carry `io.neotales.fixture-owner=plab-clickhouse-phase-b8ba17e`; any exceptional
manual cleanup must target only that fixture label, never generic Docker cleanup.
Case work is boxed before constructing the async scope: boxing only the outer scope overflowed
the external Rust 1.100 test-thread stack before fixture startup. A Docker-free regression enters
the actual full enabled contract and stops at its first container boundary; another checks that
large captured work does not inflate the scope future. This fixes a tracing-fixture entry bug,
not the earlier live lock-outcome instability.

### Unresolved live-gate instability

The live recovery gate is **not established as stable**. A separate bounded probe reproduced
`WriteOutcomeUnknown` at the unchanged two-second deadline while creating/dropping a `Memory`
lock table through the ordinary native provider, without any response-fault proxy. Temporary
bounded diagnostics observed both the query and terminating data block flushed, but no response
packet before the deadline. This excludes response-fault injection as a necessary cause; it does
not distinguish a server stall from transport delay. Host I/O pressure was observed, not proven
causal. A subsequent passing run does not resolve the failure.

The opt-in diagnostic below attempts at most 256 create/drop cycles on one disposable pinned
server, stopping new cycles after 60 seconds. An independent observer thread pre-acquires two
read-only sessions. At one second it matches the exact synthetic statement in `system.processes`,
samples bounded server-thread wait categories, and concurrently reads only its fixture's host
socket queues/counters plus host CPU/I/O pressure and sampler scheduler counters. Observer
queries have separate 600 ms bounds, and `ss` children are bounded and reaped. Write completion
is timestamped independently, before joining the observer; records are emitted only afterward.
Each write retains its two-second deadline, with no retry or outcome reclassification.

The 60-second check is an **admission bound, not an overall deadline**: an admitted cycle can
still take its five-second session acquisition plus two two-second writes. Startup readiness,
observer draining, proxy shutdown, and container removal are outside that admission bound.

The observer never extends or cancels the write deadline; its extra load can still perturb
timing. Absence from `system.processes` is inconclusive, and local flushes or peer TCP
acknowledgments do not prove server parsing. Missing permissions/capabilities are reported as
unavailable categories. The diagnostic emits no SQL, packet payloads, credentials, raw stack
symbols, or server log bodies. It is not a replacement for either live recovery gate or evidence
of stability when it passes:

```sh
cargo test -p neotales-migration-clickhouse --test live -- \
  --ignored --exact live::memory_lock_lifecycle_without_fault_injection --nocapture
```

Docker-free controlled tests deliberately keep a synthetic operation pending until the real
one-second observer trigger fires. Fake sessions exercise exact bound-identity lookup, redaction,
and cancellation of a deliberately over-budget observation at its 600 ms deadline. Real owned loopback
sockets exercise proxy teardown; synthetic `ss` responses verify the exact socket filters and
numeric-only output. A causal handshake verifies that writer completion is timestamped before
observer cleanup finishes. These tests do not validate ClickHouse's live system-table behavior
or resolve the intermittent timeout:

```sh
cargo test -p neotales-migration-clickhouse --test live controlled_tests
```

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
