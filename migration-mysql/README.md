# neotales-migration-mysql

MySQL backend for [`neotales-migration`] over [`neotales-dbapi`]. It is tested
against MySQL 8.4. MariaDB compatibility is not claimed.

[`neotales-migration`]: https://docs.rs/neotales-migration
[`neotales-dbapi`]: https://docs.rs/neotales-dbapi

## No transactional promise

MySQL implicitly commits most DDL. This provider therefore does **not** promise
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

Inspect the real schema and the ledger with [`MySqlBackend::state`], then call
[`MySqlBackend::reconcile`]:

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

Each operation acquires a server-wide MySQL `GET_LOCK` named from the
`database.table` pair before inspecting the ledger. The lock is held for the
whole operation and released explicitly. Lock timeout is a positive whole number
of seconds because of `GET_LOCK` semantics. The `GET_LOCK` and `RELEASE_LOCK`
results are read by column position: a proxy such as Vitess `vtgate` may rewrite
the `AS` label of a function projection. The projection has exactly one column,
so its position identifies it on every server.

The API accepts the concrete [`neotales_dbapi_mysql::MySqlPoolProvider`], not
`&dyn Pool`. The provider also establishes and verifies `autocommit=1` on each
acquired physical connection; an inherited server `autocommit=0` is overridden
so standalone ledger writes and dirty markers are durable without an explicit
commit. That is deliberate: a session-level `GET_LOCK` is only safe on
cancellation if dropping the session discards its physical connection. The
concrete provider enforces that discard on interruption; a generic pool could
return a session that silently leaks its server lock. MySQL releases session
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
- `DELIMITER` and SQLCMD-style directives are not interpreted. Routine or
  trigger bodies that need them are outside this provider's supported script
  grammar.

Review and own every routine, trigger, and event. Never feed this backend
untrusted SQL or untrusted database objects.

## API

```rust,no_run
use neotales_migration::{Migration, Migrations};
use neotales_migration_mysql::MySqlBackend;
use std::time::Duration;

# async fn example(pool: &neotales_dbapi_mysql::MySqlPoolProvider) -> Result<(), Box<dyn std::error::Error>> {
let backend = MySqlBackend::new("app", "schema_history", Duration::from_secs(5))?;
let migrations = Migrations::new(vec![
    Migration::new(1, "create-users", "CREATE TABLE users (id BIGINT PRIMARY KEY)", Some("DROP TABLE users".to_owned()))?,
])?;
let state = backend
    .up(pool, "app", &migrations, "binding-fingerprint-v1", &["users"])
    .await?;
assert_eq!(state.applied_count(), 1);
# Ok(()) }
```

`initialize_module` registers without running SQL. `state` returns `None` for an
unregistered module without creating ledger tables, and includes a dirty marker
when recovery is required.

## Tests

The integration suite starts a digest-pinned MySQL 8.4 container through
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

```bash
cargo test -p neotales-migration-mysql --all-targets
migration-mysql/tests/live.sh
```

## Vitess compatibility

The DBAPI and this backend were verified manually against upstream
`vitess/vttestserver` v24.0.5 on MySQL 8.4 (one keyspace, one shard, no TLS)
and its `vtgate` MySQL listener. The probe ran inside the server's
`--network none` namespace, so no database socket was reachable from the host.
No automated `Vitess` lane is shipped: `vttestserver` 24.0.5 hardcodes
`vtcombo --mysql-auth-server-impl none` and offers no flag or environment
variable to configure authentication. Its MySQL listener therefore accepts an
unauthenticated `root` from any container or local user that can reach the
container address, which cannot be made safe on a shared Docker host. The lane
is disabled rather than publishing that listener or treating an isolated
network as authentication.

This is not a support claim: sharded or resharded keyspaces, `VSchema`
management, and `PlanetScale` deployments, deploy requests, or `VReplication`
workflows are not covered.

The configured ledger database must match the selected database on the locked
physical session. A mismatch returns `Error::Configuration` before ledger access
or script execution.
