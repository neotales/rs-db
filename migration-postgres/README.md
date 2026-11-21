# neotales-migration-postgres

Locked transactional PostgreSQL 17 migrations over the quiet `neotales-dbapi` provider.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration neotales-migration-postgres
```

## What you can do 🔧

**Apply up and down inside one schema-wide advisory lock.** The backend requires PostgreSQL 17,
validates the exact applied checksum prefix, and commits scripts with ledger writes.

```rust,no_run
use std::time::Duration;
use neotales_migration::{Migration, Migrations};
use neotales_migration_postgres::PostgresBackend;

async fn migrate(
    pool: &dyn neotales_dbapi::Pool,
) -> Result<(), Box<dyn std::error::Error>> {
    let definitions = Migrations::new(vec![Migration::new(
        1,
        "orders",
        "CREATE TABLE app.orders (id bigint PRIMARY KEY)",
        Some("DROP TABLE app.orders".to_owned()),
    )?])?;
    let backend = PostgresBackend::new("app", "migration_history", Duration::from_secs(2))?;
    backend
        .up(pool, "orders", &definitions, "binding-fingerprint-v1", &[("app", "orders")])
        .await?;
    Ok(())
}
```

**Flatten is ledger-only.** It replaces the exact complete legacy history with the marked
baseline; it never executes the baseline SQL.

**Reconcile unknown commits.** An unconfirmed `COMMIT` is never reported as success or rollback;
inspect count and head before another operation.


## Scope

PostgreSQL 17 transactional up/down and ledger-only flatten over quiet
`neotales-dbapi`, using
the driver-free `neotales-migration` planner. This alpha crate supports history
initialization, trusted SQL execution and exact legacy-ledger replacement.

`PostgresBackend::initialize_module` acquires an exclusive dbapi session, begins
a READ COMMITTED transaction, and obtains a schema-wide transaction advisory lock
**before** schema/catalog inspection or DDL. Bootstrap, verification and module
registration commit together. Calls for the same or different modules/ledger tables
within a schema deliberately serialize; separate schemas can progress independently.
This coarse lock prevents first-schema/bootstrap races and makes whole-ledger
verification coherent. It is not a claim of parallel module execution.

`up` and `down` use that same locked transaction boundary. They validate the exact
applied checksum-matching prefix against core `Migrations` before executing SQL.
Scripts and ledger writes commit together; a failed script rolls back all earlier
steps in the batch. The ledger is reverified against structure, metadata and
definitions before commit. `up` executes pending scripts once; repeated identical
calls do not replay applied migrations. `down(count)` preflights the **entire**
rollback, including every down script, count and baseline barrier, before effects.
Missing down SQL or an attempted barrier crossing cannot partially roll back.
Unregistered modules cannot use `down`, even for a zero count; `up` at zero applied
count rejects declared existing managed relations even after an earlier empty
registration. Rollback to zero retains module identity/fingerprint.

After acquiring the advisory lock, the backend checks `server_version_num` and
requires **major version 17**, before ledger inspection or DDL. Other majors return
`Error::UnsupportedVersion`, not a history-corruption error. The v1 constraint
verifier deliberately compares PostgreSQL 17's canonical constraint definitions;
support for another major requires separate verification.

Identifiers are separately supplied lowercase ASCII SQL identifiers, always
quoted and schema-qualified. Schema names have at most 63 bytes (reserved `pg_`
names are rejected); history table names at most 55 bytes to leave room for
`_modules`. A local `pg_catalog` search path prevents caller search-path shadowing.
Module identity and binding fingerprint are opaque nonempty UTF-8 strings, at most
255 bytes without NUL; the caller computes the fingerprint. All identity,
fingerprint and checksum text columns explicitly use `pg_catalog."C"` collation
for bytewise equality, independent of database-default/ICU collation behavior.

## Ownership and integrity

Bootstrap creates exactly two permanent ordinary tables:

- The configured history table: module, positive version, checksum, fingerprint;
  primary key `(module, version)`.
- Its `_modules` companion: module primary key, fingerprint, expected applied
  count, head version/checksum (both NULL for an empty history).

Versioned table comments explicitly declare ownership and format
(`neotales-migration-postgres/ledger/v1/history` and
`neotales-migration-postgres/ledger/v1/modules`). Existing tables must have these
markers **and** the exact v1 structure. Columns/types/nullability, actual `C`
collation OIDs, defaults, constraints and primary index are checked. Additional
columns, indexes, rules, triggers, RLS, inheritance or partitioning are rejected.
The backend never adds markers to existing tables, repairs, adopts, or drops them.
Older, partial, unmarked or unsupported ledgers require operator investigation;
do not add markers just to pass validation.

`Error::History(HistoryReason)` distinguishes `BindingMismatch`, `PartialLedger`,
`UnownedLedger`, `IncompatibleLedger` (including unsupported markers/collations),
and `InconsistentRows`. These reasons contain no database values or driver text.
Previously created draft tables with default collation are incompatible: this
backend never silently changes their collation or metadata.

Table locks exclude ordinary concurrent DML/DDL while checking the complete ledger.
Every module's count, head and row fingerprints must agree; orphan rows, empty
or oversized checksums and incomplete metadata are rejected, including corruption belonging
to another module. Verification aggregates on the server with bounded detached
results. Both history checksums and non-NULL metadata head checksums must have
`1..=255` UTF-8 bytes; their sizes are validated before a head is read into memory.
`initialize_module` alone does not know expected definitions, so it cannot detect
a changed checksum that still agrees with metadata. Missing both entire ledger
tables cannot be distinguished from a fresh database.

Execution checks every applied checksum against the complete supplied definition
set, not just the head. Definition checksums include up SQL, down SQL, name, version
and the core baseline marker. V1's exact two-table format does not add a mutable
barrier column: after exact checksum validation, an applied first definition marked
by `Migration::into_baseline` determines the permanent rollback barrier. Removing
that marker changes its checksum and is rejected as drift. Keep the baseline
definition in subsequent deployments. Existing ledger tables are never altered
or silently upgraded by the executor.

For first registration, supply every managed application relation whose presence
means the application was already initialized. Existing declared relations
(including views, sequences and indexes) reject registration without adoption;
an empty list cannot detect untracked application objects. Only declared schemas
are checked; schema owners and pool configuration must be trusted. These integrity
checks detect accidental corruption, not a coordinated rewrite by a database owner.

```rust,no_run
use neotales_dbapi::Pool;
use neotales_migration_postgres::{Error, ModuleState, PostgresBackend};
use std::time::Duration;

// The application owns its PostgreSQL 17 pool and configures finite provider
// acquisition/statement deadlines. No driver types are needed at this boundary.
async fn initialize(pool: &dyn Pool) -> Result<ModuleState, Error> {
    let backend = PostgresBackend::new("app", "migration_history", Duration::from_secs(2))?;
    // On error, propagate it; never assume an uncertain commit rolled back.
    backend.initialize_module(
        pool, "orders", "application-binding-v1", &[("app", "orders")],
    ).await
}
```

## Deadlines, cancellation and interoperability

The positive integral-millisecond lock timeout (at most `i32::MAX` ms) bounds the
entire advisory-lock acquisition loop, including round trips. No blocking advisory
lock call is issued. Pool acquisition, begin, table locks, inspection and commit
use the provider's own finite budgets; this is **not** a total-operation deadline.
Explicit failures before commit attempt rollback without masking the original
error. Database errors retain only dbapi's sanitized cause/category.

The caller cancels by dropping the future. Transaction/session ownership delegates
rollback or connection discard to the dbapi provider. Drop is not confirmation of
rollback or prompt server lock release; the server may finish work before observing
disconnect. Interrupted commit has an unknown outcome. Retry initialization to
reconcile; it is safe to repeat with the same inputs. Returned `ModuleState` is a
detached snapshot, not a locked scope.

Both execution methods return a detached `ModuleState` only after confirmed
commit; errors never return a plausible success. **`down(count)` is not
idempotent.** After an uncertain commit or cancellation, do not blindly retry it:
another call could revert another batch. First acquire/inspect with
`initialize_module` and reconcile the observed count/head against the expected
old/new states (and any concurrent writers). Identical `up` calls are safe to
repeat for reconciliation. No execution or transaction retry is implicit.

Cooperating writers must acquire `PostgresBackend::advisory_lock_key()` with
`pg_advisory_xact_lock` or `pg_try_advisory_xact_lock` before inspection, and hold
it until transaction closure. Session-level `pg_advisory_lock` is **not**
equivalent: it can outlive transaction closure. The backend itself only uses the
nonblocking transaction-scoped try-lock. The signed 64-bit key is the
first eight SHA-256 bytes, interpreted big-endian two's-complement, over
`neotales-migration-postgres/schema-lock/v1\0`, followed by the schema UTF-8 byte
length as an unsigned 64-bit big-endian integer and then the schema bytes.
It is independent of table/module names. Hash collisions remain possible.

## Trusted SQL files

The caller loads UTF-8 files and constructs/binds core `Migration` definitions
before database acquisition. No automatic directory discovery, file I/O, or SQL
rewriting happens inside the transaction. Supply the same complete bindings for
every script, persist the fingerprint returned by `bind_identifiers`, and list
every managed relation that could indicate an existing application. Core sorting
and duplicate-definition checks apply. PostgreSQL versions must be positive and
fit signed `bigint`; all definition SQL (including unused down SQL) must be NUL-free.

The transaction uses a local `pg_catalog` search path. **Schema-qualify application
relations in every script.** SQL definitions and schema owners are trusted: scripts
must not manage transactions/savepoints, advisory locks, timeout/search-path
settings, or the ledger itself. They must not cause nontransactional effects
(external functions, existing sequence increments, and similar effects cannot be
undone by rollback). This is not a SQL sandbox; atomicity applies to trusted
transactional PostgreSQL DDL/DML meeting that contract, not arbitrary SQL.

For example, a caller-owned up file can contain:

```sql
CREATE TABLE {{schema}}.{{table}} (id bigint PRIMARY KEY);
```

and its down file:

```sql
DROP TABLE {{schema}}.{{table}};
```

Load, bind and execute them through the public API:

```rust,no_run
use std::{collections::BTreeMap, path::Path, time::Duration};
use neotales_dbapi::Pool;
use neotales_migration::{bind_identifiers, Dialect, Identifier, Migration, Migrations};
use neotales_migration_postgres::{ModuleState, PostgresBackend};

async fn migrate_files(
    pool: &dyn Pool,
    up_file: &Path,
    down_file: &Path,
) -> Result<ModuleState, Box<dyn std::error::Error>> {
    let bindings = BTreeMap::from([
        ("schema".to_owned(), Identifier::new("app")?),
        ("table".to_owned(), Identifier::new("orders")?),
    ]);
    // Finish file I/O and binding before opening the database transaction.
    let (up, fingerprint) = bind_identifiers(
        &std::fs::read_to_string(up_file)?, &bindings, Dialect::PostgreSql,
    )?;
    let (down, down_fingerprint) = bind_identifiers(
        &std::fs::read_to_string(down_file)?, &bindings, Dialect::PostgreSql,
    )?;
    if fingerprint != down_fingerprint {
        return Err(neotales_migration::Error::BindingDrift.into());
    }
    let definitions = Migrations::new(vec![Migration::new(1, "orders", up, Some(down))?])?;
    let backend = PostgresBackend::new("app", "migration_history", Duration::from_secs(2))?;
    // Propagate failures, including unknown commit outcomes; do not log SQL/values.
    Ok(backend.up(pool, "orders", &definitions, &fingerprint, &[("app", "orders")]).await?)
}
```

## Ledger-only flatten

`PostgresBackend::flatten` uses the same schema-wide transaction advisory lock,
PG17 gate and exact owned two-table ledger verification as execution. It requires
an already registered **nonempty**, complete legacy history matching
`FlattenManifest::legacy()` exactly: every version/checksum and the identical
binding fingerprint. Missing legacy steps, unknown/modified history, partial or
unowned ledgers, and changed bindings fail before replacement.

After core `FlattenManifest::assess(Some(history), fingerprint)` accepts that exact
state, the backend replaces only the target module's history with the intrinsic
marked baseline identity and persists count `1`, its head, and the same binding.
The baseline's checksum pins the rollback barrier; retain the marked baseline as
the first definition in future `up`/`down` calls. The v1 ledger schema is unchanged.
Other modules and application data are not rewritten.

**No baseline/up/down SQL is executed by flatten.** Its fresh-schema baseline SQL
may legitimately conflict with the existing application. The manifest's baseline
must describe that application's current schema; flatten verifies ledger identity,
not semantic equivalence of arbitrary SQL or application schemas.

Fresh/unregistered and registered-empty modules are rejected, even if another
module already initialized the ledger. Existing declared managed objects on that
untracked/empty state reject adoption. To initialize a genuinely fresh application,
execute `up` with `Migrations::new(vec![manifest.baseline().clone()])` and the full
managed-object list; do not record a baseline that has not run.

```rust,no_run
use neotales_dbapi::Pool;
use neotales_migration::{FlattenManifest, Migrations};
use neotales_migration_postgres::{Error, ModuleState, PostgresBackend};

async fn flatten_existing(
    pool: &dyn Pool,
    backend: &PostgresBackend,
    manifest: &FlattenManifest,
    fingerprint: &str,
) -> Result<ModuleState, Error> {
    // Only valid after this exact legacy set is completely applied.
    // Propagate uncertainty; never assume an errored COMMIT rolled back.
    backend.flatten(pool, "orders", manifest, fingerprint, &[("app", "orders")]).await
}

async fn apply_fresh_baseline(
    pool: &dyn Pool,
    backend: &PostgresBackend,
    manifest: &FlattenManifest,
    fingerprint: &str,
) -> Result<ModuleState, Box<dyn std::error::Error>> {
    let baseline = Migrations::new(vec![manifest.baseline().clone()])?;
    Ok(backend.up(pool, "orders", &baseline, fingerprint, &[("app", "orders")]).await?)
}
```

Flatten is not an idempotent-success/retry API. Reflattening the baseline with the
legacy manifest rejects drift; concurrent flatten callers serialize and only one
can succeed. After cancellation/unknown commit, inspect count/head under
`initialize_module` and reconcile against legacy versus baseline state before
further action. No retry, fresh-state adoption or SQL execution is implicit.
Pre-commit failures roll back all ledger mutations; unknown COMMIT is never
reported as success or proof of rollback.

## Verification

From `rs/lib`:

```sh
cargo fmt --all --check
cargo clippy -p neotales-migration -p neotales-migration-postgres --all-targets -- -D warnings
cargo test -p neotales-migration -p neotales-migration-postgres
cargo test --doc -p neotales-migration -p neotales-migration-postgres
migration-postgres/tests/live.sh
RUSTDOCFLAGS='-D warnings' cargo doc -p neotales-migration -p neotales-migration-postgres --no-deps
```

The normal test run explicitly ignores live tests. `tests/live.sh` is the required
live lane: it requires Docker, creates a disposable PostgreSQL 17 container with
synthetic trust-authenticated test identity on loopback port 55435, runs **all**
ignored live tests, and removes its own container via an EXIT trap. Cleanup is
tied to the container ID recorded by this invocation and a per-invocation label;
it never removes by name, so a colliding name cannot delete another run. The
label also catches a container Docker created before a failed `--cidfile` write.
Port conflict, missing Docker, readiness failure and test failure fail the lane.
It never falls back to an existing server. Do not run the ignored tests directly
against any persistent database. The `data` CI pipeline runs this script in its
own `ubuntu-24.04` job; the fixture-free test job is not a substitute.

The live lane covers caller-owned SQL files, fresh/repeated up, two-step down,
missing-down/baseline preflight, syntax failures in both directions, concurrent
runners, drift, untracked-object preservation and cancellation during a script.
A local protocol proxy withholds the actual server's COMMIT completion for both
up and down: an independent connection observes committed application/ledger
state while the caller must still return `CommitOutcomeUnknown`, never success
or an automatic replay. No SQL, packet contents, values or driver text are logged.

Flatten live tests cover complete exact replacement without baseline execution,
application object/data and other-module preservation, partial/modified/unknown
versions and binding mismatch, fresh/empty/untracked policy, baseline rollback
barriers, concurrent callers and reflatten rejection. The protocol fixture also
holds an actual ledger DELETE before cancellation to prove rollback of removed
legacy rows, and withholds an actual flatten COMMIT completion to prove unknown
outcome handling despite an independently observed committed baseline.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
