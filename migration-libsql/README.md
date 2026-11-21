# neotales-migration-libsql

Transactional migration backend for remote libSQL/Turso over `neotales-dbapi-libsql`.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration neotales-migration-libsql
```

## What you can do 🔧

**Run up, down, and flatten against a remote libSQL database.** Each operation acquires one
exclusive session, starts `BEGIN IMMEDIATE`, and commits ledger and schema work together.

```rust,no_run
use std::time::Duration;
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};
use neotales_migration::{Migration, Migrations};
use neotales_migration_libsql::LibsqlBackend;

async fn migrate() -> Result<(), Box<dyn std::error::Error>> {
    let provider = LibsqlPoolProvider::connect(
        LibsqlConfig::remote("libsql://example.turso.io", "from-your-secret-store")
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5)),
    )
    .await?;
    let backend = LibsqlBackend::new("migration_log")?;
    let migrations = Migrations::new(vec![Migration::new(
        1,
        "create_users",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        Some("DROP TABLE users".to_owned()),
    )?])?;
    backend
        .up(&provider, "users", &migrations, "application-v1", &["users"])
        .await?;
    Ok(())
}
```

**Treat an interrupted commit as unknown.** Remote rollback is best effort. Use `inspect` under
external coordination; do not retry blindly.

**Declare every managed object.** An unregistered module rejects listed pre-existing objects
rather than adopting schema.


## Scope

Transactional migration executor for `neotales-migration` over a remote
libSQL/Turso database. It uses `neotales-dbapi-libsql`, so the public API accepts
a `LibsqlPoolProvider`; no libSQL driver type is exposed.

The secured provider enforces same-origin Hrana routing and does not follow
redirects. Hosted cross-origin routing is unsupported; Turso Cloud compatibility
has not been verified. It does not silently downgrade TLS.

```rust,no_run
use std::time::Duration;
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};
use neotales_migration::{Migration, Migrations};
use neotales_migration_libsql::LibsqlBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
let provider = LibsqlPoolProvider::connect(
    LibsqlConfig::remote("libsql://example.turso.io", "from-your-own-secret-store")
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5)),
).await?;
let backend = LibsqlBackend::new("migration_log")?;
let migrations = Migrations::new(vec![
    Migration::new(
        1,
        "create_users",
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        Some("DROP TABLE users".to_owned()),
    )?,
])?;
backend
    .up(&provider, "users", &migrations, "application-v1", &["users"])
    .await?;
Ok(())
}
```

The backend is remote-only, matching the provider. It does not use a local file,
an embedded replica, or a synchronized database. Use
`neotales-migration-sqlite` for an in-process SQLite file.

## Transaction and recovery model

Each `up`, `down`, and `flatten` acquires one exclusive provider session, starts
`BEGIN IMMEDIATE` on it, performs all ledger and schema work, and commits.
Before commit is dispatched, failures and cancellation discard the session and
attempt rollback on a best-effort basis; remote rollback is not guaranteed.
**Cancellation after COMMIT is dispatched may leave a committed transaction.**
Dropping the future does not establish rollback and returns no error to inspect:
use the same externally coordinated reconciliation as for an unknown commit.
If the dispatch boundary is unknown, assume commit may have happened.

A remote commit whose result cannot be established is reported as
`Error::Database`, never as success. libSQL 0.10 also hides the SQLite error code
for remote statement failures, so this backend cannot distinguish a constraint
error from a transport failure. This is deliberate rather than guessed; inspect
the persisted ledger and the managed objects before deciding what happened.
The public `Error::Database` does not distinguish a failure before dispatch from
an unknown commit outcome. Unless the caller independently knows the boundary,
reconcile every such error before retrying.

**Coordinate exclusively across all migration runners from before dispatch
through reconciliation, managed-object verification, any retry decision and
completion of any retry.** If a retry is interrupted or its outcome is unknown,
retain coordination through further reconciliation and any subsequent retry.
The backend's `BEGIN IMMEDIATE` protects each operation, but it does
not hold a lock across a separate ledger inspection and a later retry. Without
external coordination, another runner can change the head between those steps;
do not treat an earlier inspection as an atomic retry precondition.

After an unknown or cancelled `up`, compare the complete validated history,
binding fingerprint, baseline barrier and internally checked summary metadata
against both expected pre- and post-operation snapshots. A matching head alone
is insufficient. Only consider another `up` if the entire expected pre-operation
state still matches and external coordination has been maintained.

After an unknown or cancelled `down`, **do not blindly repeat the same call**. Read the
module metadata and applied rows using `inspect`, then compare the complete
validated state with the expected pre-down and post-down states.
Only consider another `down` when the complete expected pre-down history is
still present. If the complete expected post-down history is present and no
other runner could have changed it since dispatch, treat the first call as
committed only after verifying managed objects and side effects. Investigate any
other state or intervening concurrency. Repeating a committed call removes an
additional migration. A retried `down(count=1)` after a committed first
`down(count=1)` therefore undoes two migrations. Apply the same rule
before repeating `flatten`.

### Checked inspection

Hold application-level exclusive coordination across **all** migration runners
from before dispatch through reconciliation and completion of any retry. If a
retry is interrupted or its outcome is unknown, retain coordination through
further reconciliation and any subsequent retry.
`inspect` checks the full applied prefix against the supplied definitions and
fingerprint, validates the stored count, head and barrier metadata, and returns
the complete `PersistedHistory` (or `None` for an unregistered module). It does
not hold the database lock after it returns. Compare complete expected states,
not merely their heads; independently verify managed objects and side effects.

```rust,no_run
use neotales_dbapi_libsql::LibsqlPoolProvider;
use neotales_migration::{Migrations, PersistedHistory};
use neotales_migration_libsql::{Error, LibsqlBackend};

async fn compare_after_interruption(
    backend: &LibsqlBackend,
    provider: &LibsqlPoolProvider,
    definitions: &Migrations,
    before: Option<PersistedHistory>,
    expected_after: Option<PersistedHistory>,
) -> Result<&'static str, Error> {
    // Caller holds exclusive coordination from before dispatch through any
    // retry and further reconciliation if that retry is interrupted.
    let actual = backend.inspect(provider, "users", definitions, "application-v1").await?;
    if actual == before && actual == expected_after {
        Ok("ambiguous: states coincide; investigate")
    } else if actual == before {
        Ok("matches complete pre-operation history; verify objects before deciding")
    } else if actual == expected_after {
        Ok("matches complete post-operation history; verify objects before deciding")
    } else {
        Ok("unexpected history; investigate without retry")
    }
}
```

`inspect` validates against the definitions supplied to it. After a flatten,
use definitions matching the baseline history being inspected. A mismatch,
partial ledger, binding drift, corrupt metadata or database failure is an error,
not a retry signal. Since `inspect` itself may fail or be cancelled, never treat
its absence of a result as evidence of any state.

`flatten` replaces complete prior history with the baseline marker but does not
execute baseline SQL. A fresh module applies its baseline through `up`.

## Managed objects and history

Pass the complete managed-object list to `up`, `down`, and `flatten`. For an
unregistered module, these methods reject listed pre-existing main objects rather
than adopting schema. All three methods reject listed temporary objects because a
temporary object can shadow a main object. An empty list is only correct for an
application that truly has no managed objects.

History stores checksums, module identity (including modules with no applied
migrations), binding fingerprint, baseline barrier, applied count, and head.
Nullable version markers are stored as text because a remote libSQL null has no
declared SQLite type at this layer. It
detects missing rows, drift, and checksum changes, but not a deliberate
coordinated rewrite. `down` plans every script before executing any of them.
Ledger rows are materialized without a configured row or byte ceiling; use this
backend only with a trusted server and bounded migration history.
The history table name must be a non-empty ASCII lowercase identifier and is
quoted and qualified in `main`.

**Ledger ownership precondition:** existing `main."<table>"` and
`main."<table>_modules"` tables are adopted by name. This backend does **not**
authenticate ownership or fully validate their schema, constraints, indexes or
triggers. A compatible-looking table can run unexpected trigger SQL or impose
unexpected constraints. Before every `up`, `down`, `flatten` or `inspect`, the
caller must ensure both are trusted backend-owned ledger tables with the
expected definitions and no hostile triggers, and protect them from untrusted
DDL and writes. The same trust applies when the tables already existed before
this backend was first used. History checks do not establish that trust. There
is no compatible ownership marker in existing ledgers; deployment must establish
this precondition externally.

Use trusted migration SQL. Scripts must not manage transactions or perform
external, non-transactional side effects. NUL-containing SQL is rejected before
execution.

## Testing

The live integration test starts a digest-pinned `sqld` container with
the Docker CLI, publishes no host port, and reaches the container bridge
through a loopback proxy. It is `#[ignore]`d so a fixture-free `cargo test` does
not need Docker. The exact digest must already exist locally; the test does not
pull images:

```sh
cargo test -p neotales-migration-libsql --all-targets
migration-libsql/tests/live.sh
```

The lane covers up/down, history drift, untracked objects, flatten, and the
remote transaction path. It deliberately drops one COMMIT response from a
pinned local `sqld`, checks that the operation reports a database error, then
reconciles the actual head and checksum and confirms the managed objects. It
also cancels an operation after the COMMIT response is reached and checks the
complete inspected history, tests corrupt metadata and a changed earlier row
with an unchanged head, and demonstrates that a trigger on an adopted ledger
does execute. A separate regression repeats
`down(count = 1)` and observes two migrations removed, documenting why an
unknown commit must be reconciled against the complete expected state instead of blindly retried.
It does not cover an actual Turso Cloud deployment,
verified TLS to `sqld` (the disposable server speaks cleartext through the
provider's loopback exception), or all possible commit-loss timing windows.
Those remain integration environments for the application, not claims of this
test.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
