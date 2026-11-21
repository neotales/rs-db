# neotales-migration-mariadb

MariaDB 11.4 migration backend with explicit dirty-marker recovery.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration neotales-migration-mariadb
```

## What you can do 🔧

**Apply a migration set with a persisted dirty marker.** MariaDB implicitly commits most DDL, so
schema and ledger writes are not promised atomic.

```rust,no_run
use std::time::Duration;
use neotales_migration::{Migration, Migrations};
use neotales_migration_mariadb::MariaDbBackend;

async fn migrate(
    pool: &neotales_dbapi_mariadb::MariaDbPoolProvider,
) -> Result<(), Box<dyn std::error::Error>> {
    let backend = MariaDbBackend::new("app", "schema_history", Duration::from_secs(5))?;
    let migrations = Migrations::new(vec![Migration::new(
        1,
        "create-users",
        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
        Some("DROP TABLE users".to_owned()),
    )?])?;
    backend
        .up(pool, "app", &migrations, "binding-fingerprint-v1", &["users"])
        .await?;
    Ok(())
}
```

**Reconcile before retrying.** Inspect the real schema and ledger with `state`, then call
`reconcile`. A dirty marker blocks further automated operations until it is resolved.

**Serialise writers with `GET_LOCK`.** The lock is named from the `database.table` pair and held
for the whole operation.


## Scope

MariaDB backend for [`neotales-migration`] over [`neotales-dbapi`]. It is tested
against MariaDB 11.4 (LTS) only. MySQL compatibility is not claimed; MySQL has
its own backend and test lane.

[`neotales-migration`]: https://docs.rs/neotales-migration
[`neotales-dbapi`]: https://docs.rs/neotales-dbapi

## No transactional promise

MariaDB implicitly commits most DDL. This provider therefore does **not** promise
that schema changes and ledger writes are atomic. Instead, every script is
preceded by a persisted dirty marker and followed by a ledger write:

1. the dirty marker (`direction`, `version`, checksum, `stage=started`);
2. the trusted migration script;
3. `stage=script_done` (or `script_failed` after an error);
4. the history row insert/delete;
5. clearing the dirty marker.

Any failure or interruption can leave the schema and ledger out of step. The
provider never retries an uncertain outcome and blocks all further automated
`up`, `down`, and `initialize_module` calls with [`Error::Dirty`] until an
operator reconciles.

Stages mean:

| Stage | Meaning |
| --- | --- |
| `started` | The marker was committed. The script may not have run, may be running, or may have partially run. |
| `script_done` | The script returned success. The history row may or may not have been synchronized. |
| `script_failed` | Best effort: the script returned an error. A provider that discards the connection on a script error may leave the stage at `started`. Either way, earlier statements may have committed. |

## Recovery

Inspect the real schema and the ledger with [`MariaDbBackend::state`], then call
[`MariaDbBackend::reconcile`]:

- `ReconcileOutcome::Applied` — the intended effect is present. For `up`, append
  the history row (idempotently). For `down`, remove the reverted row
  (idempotently).
- `ReconcileOutcome::NotApplied` — the intended effect is absent. Leave history
  unchanged and clear the marker. If a `down` crash removed the history row but
  the schema effect is still present, the row is restored instead of clearing
  over a missing ledger entry. A conflicting later history row is rejected
  without clearing.

The provider cannot verify arbitrary DDL. `Applied` for a `started` stage is the
operator's assertion that the complete script took effect. Review the schema
first. `reconcile` validates the dirty version/checksum and the resulting history
prefix, but it cannot detect every manual change.

## Locking and ownership

Each operation acquires a server-wide MariaDB `GET_LOCK` named from the
`database.table` pair before inspecting the ledger. The lock is held for the
whole operation and released explicitly. Lock timeout is a positive whole number
of seconds because of `GET_LOCK` semantics.

The API accepts the concrete [`neotales_dbapi_mariadb::MariaDbPoolProvider`], not
`&dyn Pool`. The provider also establishes and verifies `autocommit=1` on each
acquired physical connection; an inherited server `autocommit=0` is overridden
so standalone ledger writes and dirty markers are durable without an explicit
commit. That is deliberate: a session-level `GET_LOCK` is only safe on
cancellation if dropping the session discards its physical connection. The
concrete provider enforces that discard on interruption; a generic pool could
return a session that silently leaks its server lock. MariaDB releases session
locks when it observes the disconnect, so a long-running cancelled statement can
delay the release even though the provider has discarded the connection.

The ledger is two `InnoDB` tables: `<table>` and `<table>_meta`. Both carry a
format marker in the table comment and are verified for exact column order,
`VARCHAR` lengths, integer signedness, nullability, case-sensitive collation,
defaults, generated-column absence, primary-key column order, the exact `CHECK`
clauses, and the expected `CHECK`/`PRIMARY KEY` constraint counts. Foreign keys and extra unique
constraints are rejected. An existing pair that does not
match is rejected, never adopted. Exactly one existing table is a partial ledger
and also fails closed. Text comparisons use `utf8mb4_0900_as_cs`, so checksums
and fingerprints are case-sensitive.

MariaDB metadata differs from MySQL in ways the verification accounts for:
integer columns report a display width (`bigint(20)`), MariaDB stores the
literal `NULL` default on nullable columns, and `CHECK` clauses are returned
without MySQL's outer parentheses. MariaDB also reuses `CONSTRAINT_n` check
names per table, so the `information_schema.CHECK_CONSTRAINTS` join includes
`TABLE_NAME`; matching on schema and name alone cross-joins checks from other
tables. These differences are covered by the live ledger tests.

`up` registers a fresh module only after every declared managed object is absent.
A registered-but-empty module is checked again before its first script.
`down` requires a registered module. `flatten` is unsupported because replacing
history plus a baseline cannot be made atomic when DDL implicitly commits.

## Trusted SQL and trusted database objects

This is not a sandbox or SQL validator. Every migration script and every
database object is trusted:

- migration SQL may perform arbitrary DDL/DML and may have nontransactional
  effects;
- stored procedures, prepared statements, functions, triggers, events, views,
  generated columns, and UDFs can change schema, session state, or external
  state outside the script text;
- the DBAPI provider rejects `CALL`, `EXECUTE`, `PREPARE`, executable comments,
  transaction control, and session mode changes, but an allowed `SELECT`/DML can
  still invoke a trusted function or trigger;
- `DELIMITER` and similar client-side directives are not interpreted. Routine
  or trigger bodies that need them are outside this provider's supported script
  grammar.

Review and own every routine, trigger, and event. Never feed this backend
untrusted SQL or untrusted database objects.

## API

```rust,no_run
use neotales_migration::{Migration, Migrations};
use neotales_migration_mariadb::MariaDbBackend;
use std::time::Duration;

async fn example(pool: &neotales_dbapi_mariadb::MariaDbPoolProvider) -> Result<(), Box<dyn std::error::Error>> {
let backend = MariaDbBackend::new("app", "schema_history", Duration::from_secs(5))?;
let migrations = Migrations::new(vec![
    Migration::new(1, "create-users", "CREATE TABLE users (id BIGINT PRIMARY KEY)", Some("DROP TABLE users".to_owned()))?,
])?;
let state = backend
    .up(pool, "app", &migrations, "binding-fingerprint-v1", &["users"])
    .await?;
assert_eq!(state.applied_count(), 1);
Ok(()) }
```

`initialize_module` registers without running SQL. `state` returns `None` for an
unregistered module without creating ledger tables, and includes a dirty marker
when recovery is required.

## Tests

The integration suite starts a digest-pinned MariaDB 11.4 container through
`testcontainers`. It publishes no ports, uses a random nonempty root password
from the OS CSPRNG, and connects through a loopback-only proxy. It covers:

- ordered apply/repeat and down;
- partial script failure with a persisted dirty marker and explicit recovery;
- down failure after a partial effect, reconciled as applied;
- concurrent writers serializing on `GET_LOCK`;
- explicit lock timeout;
- interruption by dropping the backend future, then recovery, including a
  separate observer pool proving the cancelled session did not leak its
  `GET_LOCK`;
- untracked application objects, unowned lookalikes, marked ledgers with an
  incompatible shape or missing constraint, and constraint drift introduced
  after a ledger was already initialized (rejected before a write);
- inherited `autocommit=0` (the suite starts the server that way);
- a simulated crash after a down history delete, reconciled as not applied;
- dirty markers whose present version is not the history head, or whose absent
  version is not exactly the next position, rejected without clearing.

```sh
cargo test -p neotales-migration-mariadb --all-targets
migration-mariadb/tests/live.sh
```

The configured ledger database must match the selected database on the locked
physical session. A mismatch returns `Error::Configuration` before ledger access
or script execution.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
