# neotales-migration-mssql

Transactional SQL Server migrations over [`neotales-dbapi`] and the
[`neotales-dbapi-mssql`] provider. This alpha crate is **not yet accepted for
use**: the SQL Server provider itself is unaccepted, and this backend's public
surface and the application-lock extension require independent review.

[`neotales-dbapi`]: https://docs.rs/neotales-dbapi
[`neotales-dbapi-mssql`]: https://docs.rs/neotales-dbapi-mssql

## Transactional contract

`initialize_module`, `up`, and `down` each acquire one exclusive session, begin a
`READ COMMITTED` transaction, and acquire an **exclusive application lock owned
by that transaction** before inspecting the ledger or running DDL. The lock uses
`sp_getapplock` with `@LockOwner = 'Transaction'`, so the server releases it on
commit, rollback, or connection loss. It is not session-owned: it cannot outlive
transaction closure or leak through a pooled connection. A lock that cannot be
acquired within the configured timeout fails with `Error::LockTimeout` and rolls
back before the session is reused.

The app lock serializes cooperating writers for one `(schema, table)` pair in one
database. The resource name is the SHA-256 of a fixed domain and the
length-prefixed schema and table. `SqlServerBackend::lock_resource()` exposes it
for interoperability with other cooperating writers.

After the lock, the backend also takes `TABLOCKX, HOLDLOCK` on both ledger tables
for the rest of the transaction. That excludes non-cooperating readers and
writers during ledger verification and execution.

DDL and ledger writes share one transaction. A failing later batch or script
rolls back every earlier batch and every ledger write. The backend re-verifies
the complete ledger before commit and returns a detached snapshot only after a
confirmed commit. A commit error is not proof of rollback: it is reported as
`Error::Database` and may carry a dbapi `CommitOutcomeUnknown`. Reconcile by
repeating `up` or `initialize_module`; **`down(count)` is not idempotent**, so
inspect count/head before repeating it.

The backend requires SQL Server major version 16 (SQL Server 2022). Other majors
fail with `Error::UnsupportedVersion` before ledger inspection. The exact
catalog signatures in this crate are verified against that major only.

## Ledger

Bootstrap creates exactly two permanent ordinary tables in an already-existing
schema:

- The configured history table: binary-collated `module`, positive `version`,
  `checksum`, `fingerprint`, and a constant `ledger_format` marker; primary key
  `(module, version)`.
- `<table>_modules`: binary-collated `module` primary key, `fingerprint`,
  nonnegative `applied_count`, nullable `head_version`/`head_checksum`, and a
  constant `ledger_format` marker.

All identity, fingerprint, checksum, and marker text columns use
`Latin1_General_100_BIN2`, so equality is bytewise and independent of the
database collation. Constraints reject empty/inverted head metadata, negative
counts, and nonpositive versions. The marker columns carry named check
constraints; their exact canonical definitions, column signatures, primary
index, absence of extra indexes, foreign keys, triggers, security predicates,
identity/sparse/masked/generated/computed/hidden columns, and table kind are
verified before use. Existing tables are never repaired, adopted, altered, or
dropped. Missing one table is `PartialLedger`; a wrong shape or marker is
`IncompatibleLedger`.

Checksums, count, head, per-row fingerprint, marker values, trailing spaces, NUL
characters, and absent modules are verified across the entire ledger, including
rows for other modules. Lengths are checked in the database; only bounded
detached values cross into the process. Checksums, fingerprints, and module
names are limited to 255 UTF-8 bytes on input and must not end with a space;
the same trailing-space rule is enforced on persisted rows before any reuse.

As with the other reviewed backends, the ledger detects accidental corruption
and definition/binding drift, not a coordinated rewrite by a database owner.
`initialize_module` cannot detect a changed checksum that still agrees with
metadata because it has no definitions; `up`/`down` validate the complete
supplied definition set.

## SQL scripts and `GO`

Scripts are trusted. The caller loads and binds them before any database work.
`GO` is split client-side by `neotales_migration::split_sql_server_batches`; it
is never sent to the server. **A `GO` boundary is a client batch boundary, not a
transaction boundary.** A `GO` line may end in LF, CR, or CRLF. Every batch of
every planned script executes on the same transaction, in order, before commit.
All planned scripts and all of their batches are split and validated before the
first batch runs, so an unterminated string/comment or `GO n` directive fails
before any effect.

Each batch is executed through the provider's trusted-script path, which is a
conservative lexical gate, not a SQL parser or sandbox. The provider rejects
`EXEC`/`EXECUTE`, `sp_`/`xp_` entry points, transaction control, `USE`, `GO`,
unrecognized leading batch keywords, and every session `SET` statement. An
unquoted `SET` token that belongs to an `UPDATE ... SET` assignment list is
accepted only when the entire statement is the provider's complete restricted
ordinary `UPDATE` form and every statement in that batch starts with a
recognized keyword. Because a newline is not a statement boundary, a batch that
mixes `UPDATE` with other statements must separate them with semicolons, and an
`UPDATE` inside `IF`/`BEGIN`, a CTE, or a `MERGE` is not accepted. Other
data-changing forms remain `Unsupported`; this backend does not weaken that
boundary.

Scripts must not manage transactions or locks, change session options, or touch
the ledger. SQL Server functions, triggers, and other trusted objects can have
effects the lexical gate cannot see. Migration versions are positive and fit
signed `bigint`; all definition SQL must be NUL-free.

```rust,no_run
use std::time::Duration;
use neotales_dbapi_mssql::MssqlPoolProvider;
use neotales_migration::{Migration, Migrations};
use neotales_migration_mssql::SqlServerBackend;

async fn migrate(
    provider: &MssqlPoolProvider,
    up_sql: String,
    down_sql: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let definitions = Migrations::new(vec![Migration::new(
        1,
        "orders",
        up_sql,
        Some(down_sql),
    )?])?;
    let backend = SqlServerBackend::new("dbo", "migration_history", Duration::from_secs(5))?;
    backend
        .up(provider, "orders", &definitions, "application-binding-v1", &[])
        .await?;
    Ok(())
}
```

The caller owns the provider, its TLS policy, and its credential source. A script
may contain `GO` lines between batches.

## Verification

From `rs/lib`:

```sh
cargo fmt --all --check
cargo clippy -p neotales-migration -p neotales-dbapi-mssql -p neotales-migration-mssql --all-targets -- -D warnings
cargo test -p neotales-migration -p neotales-dbapi-mssql -p neotales-migration-mssql
cargo test --doc -p neotales-migration -p neotales-dbapi-mssql -p neotales-migration-mssql
migration-mssql/tests/live.sh
RUSTDOCFLAGS='-D warnings' cargo doc -p neotales-migration -p neotales-dbapi-mssql -p neotales-migration-mssql --no-deps
```

The normal test run ignores the live target. `tests/live.sh` requires Docker and
runs the ignored test, which starts one digest-pinned SQL Server 2022
testcontainer with a CSPRNG password, no published ports, and a loopback-only
proxy. It covers multi-batch `GO`, rollback of earlier batches on a later
failure, drift, concurrent transaction-owned lock serialization, lock-wait
cancellation and script cancellation, preflight rejection without consuming a
nontransactional sequence value, barrier preflight, unregistered `down`,
modified-ledger default rejection, trailing-space identity rejection, and
unowned/partial ledger rejection. It uses the provider's explicit insecure
TLS mode only for that disposable fixture.

Unknown-commit injection is **not** exercised live. The provider requires TLS on
every connection, so a loopback byte proxy cannot identify and withhold a
`COMMIT` acknowledgement without terminating TLS. The backend still propagates
the provider's `CommitOutcomeUnknown` unchanged; recover by repeating `up` or
`initialize_module` with identical definitions and binding, never by blindly
repeating `down`. Flatten is not implemented in this revision.

## License

MIT. See the workspace `LICENSE.md`.
