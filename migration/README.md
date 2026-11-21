# neotales-migration

Driver-free SQL migration definitions, checksums, and planning.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-migration
```

## What you can do 🔧

**Plan validated SQL without opening a database.** `Migrations::plan_up` returns SQL only after
the complete history, checksums, binding fingerprint, and rollback preflight agree.

```rust,no_run
use neotales_migration::{Dialect, Identifier, Migration, Migrations, bind_identifiers};
use std::collections::BTreeMap;

fn plan() -> Result<(), Box<dyn std::error::Error>> {
    let bindings = BTreeMap::from([("table".to_owned(), Identifier::new("events")?)]);
    let (up, fingerprint) = bind_identifiers(
        "CREATE TABLE {{table}} (id INTEGER)",
        &bindings,
        Dialect::Sqlite,
    )?;
    let migration = Migration::new(1, "events", up, Some("DROP TABLE "events"".into()))?;
    let migrations = Migrations::new(vec![migration])?;
    let history = migrations.validate_state(vec![], fingerprint.clone(), None)?;
    let plan = migrations.plan_up(&history, &fingerprint)?;
    assert_eq!(plan.len(), 1);
    Ok(())
}
```

**Detect edited definitions.** `Migration::new` pins the version, name, up SQL, down SQL, and
baseline marker into a SHA-256 checksum. Changed history is rejected, not reconciled.

**Keep execution yours.** This crate never connects, executes SQL, or stores a ledger. Backends
such as `neotales-migration-sqlite` and `neotales-migration-postgres` own that boundary.


## Scope

Driver-free SQL migration definitions and planning. This crate never connects to
a database and never executes SQL. The caller owns ledger reads, transactions,
execution, and ledger writes.

## Typical workflow

1. Define the application's complete migration set.
2. Read and validate the persisted history.
3. Request a plan and execute it in a transaction.
4. Persist each success: version, checksum, and binding fingerprint.

For rollback, request a count. The returned SQL is in reverse order, and the
**whole request is preflighted** before a plan is returned.

The core crate does not prescribe a database schema. `PersistedHistory` is the
logical state a backend must preserve. SQLite and PostgreSQL use different
physical ledgers (below); custom backends must preserve the same invariants.

The remote-only [`neotales-migration-libsql`](../migration-libsql/README.md)
backend uses the guarded libSQL provider. Its commit-unknown reconciliation and
hosted-routing limitations are documented separately; it is not an in-process
SQLite backend.

`Migration::new` computes a SHA-256 checksum over a versioned domain plus
length-prefixed version, name, up SQL, and down SQL bytes (including an explicit
presence marker for down SQL). `Migrations` orders definitions and rejects
duplicate versions/names. `Migrations::validate_persisted` requires the applied rows to be an
exact checksum-matching prefix; unknown, missing-prefix, or modified history is
not silently reconciled.

### Runnable planning example

This plans SQL locally; it deliberately opens no database connection and
executes nothing. Run it from the `rs/lib` workspace with
`cargo run -p neotales-migration --example plan`.

```rust
use neotales_migration::{Dialect, Identifier, Migration, Migrations, bind_identifiers};
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bindings = BTreeMap::from([
        ("table".to_owned(), Identifier::new("events")?),
    ]);
    let (up, fingerprint) = bind_identifiers(
        "CREATE TABLE {{table}} (id INTEGER); INSERT INTO {{table}} VALUES (1)",
        &bindings,
        Dialect::Sqlite,
    )?;
    let migration = Migration::new(
        1,
        "events",
        up,
        Some("DROP TABLE \"events\"".into()),
    )?;
    let migrations = Migrations::new(vec![migration])?;
    let history = migrations.validate_state(vec![], fingerprint.clone(), None)?;
    let plan = migrations.plan_up(&history, &fingerprint)?;
    assert_eq!(plan.len(), 1);
    println!("{}", plan[0].sql);
    Ok(())
}
```

`Migrations::plan_up` and `Migrations::plan_down` return SQL only after validating the complete history.
Rollback preflights count, baseline, and every down script before returning any
plan. The caller must execute the returned plan in its own transaction if its
database permits transactional DDL. Some SQL engines/DDL operations cannot be
rolled back; this library cannot guarantee atomicity and does not retry.

Flatten manifests require a baseline marked with `Migration::into_baseline`.
That marker is part of the definition checksum; it cannot be removed from a
persisted ledger to authorize rollback. Keep the marked definition on every
subsequent deployment. `FlattenManifest::assess` accepts `None` for a truly
fresh ledger, or the complete persisted old history, and compares its binding
fingerprint with the requested one. Its decision matters:

- `FlattenDecision::ReplaceExactHistory`: the old history was verified; a
  backend may replace its ledger with the marked baseline.
- `FlattenDecision::FreshBaseline`: this is only a logical assessment. **Do
  not persist its returned state yet.** Execute the baseline SQL first, then
  record the resulting history. Prefer a backend's ordinary `up` method, which
  handles execution and ledger writes together where supported.

For a custom executor, persist `assessment.state.persisted()` only after the
baseline execution succeeds, in the same transaction when the database allows.
Revalidate persisted state with `Migrations::validate_persisted`. Ledger
integrity and the distinction between absent and deleted history are the
caller's responsibility; this crate cannot authenticate external storage.

## What the ledger represents

Conceptually, a persisted history contains:

| Value | Meaning |
| --- | --- |
| `applied` | Ordered `(version, checksum)` identities for every applied definition. The list must be a prefix of the supplied definitions. |
| `binding_fingerprint` | Identity of the identifier bindings used to produce the SQL. A changed binding is drift, not an instruction to rewrite history. |
| `barrier` | Optional `(version, checksum)` identity of the irreversible baseline. It is present only when the first definition is marked with `into_baseline` and has been applied. |

The checksum pins the definition, not the resulting database contents. It
detects edits to migration name/version/up/down SQL and to the baseline marker;
it is not authentication and cannot prove that SQL was actually run. The
binding fingerprint likewise detects a changed binding set, not a changed
database schema. Never treat ledger data as a security boundary against an
administrator able to rewrite both rows and metadata.

### Physical tables in the supplied backends

The reviewed SQLite backend creates two tables in `main`, named by
`SqliteBackend::new` as `<table>` and `<table>_modules`:

| Table | Columns and role |
| --- | --- |
| `<table>` | `module TEXT NOT NULL`, `version INTEGER NOT NULL`, `checksum TEXT NOT NULL`, `fingerprint TEXT NOT NULL`, nullable `barrier INTEGER` and `barrier_checksum TEXT`; primary key `(module, version)`. One row per applied migration. The fingerprint and barrier are repeated on rows so mixed or partial row state can be detected. |
| `<table>_modules` | `module TEXT PRIMARY KEY`, `fingerprint TEXT NOT NULL`, nullable `barrier` and `barrier_checksum`, `applied_count INTEGER NOT NULL`, nullable `head_version` and `head_checksum`. One row per registered module, including a module with zero migrations; count/head summarize expected history. |

The reviewed PostgreSQL backend creates a configured history table and its
`_modules` companion, but its v1 schema is intentionally different:

| Table | Columns and role |
| --- | --- |
| History | `module text`, positive `version bigint`, `checksum text`, `fingerprint text`; primary key `(module, version)`. |
| Modules | `module text PRIMARY KEY`, `fingerprint text`, nonnegative `applied_count bigint`, nullable `head_version bigint` and `head_checksum text`. Constraints require the head fields to be jointly null and null exactly when the count is zero. |

These physical layouts describe the SQLite and PostgreSQL implementations at
this revision, not a promise that every backend shares a schema. See the
`neotales-migration-mysql` and `neotales-migration-mariadb` provider READMEs
for their distinct ledger and
recovery semantics.
PostgreSQL does not store barrier columns: after validating the complete
definition checksums, the first applied definition's baseline marker supplies
the barrier. SQLite stores the barrier explicitly in both ledger tables and
checks agreement. Do not add a supposed universal `barrier` column to PostgreSQL
or assume SQLite's metadata columns exist in PostgreSQL. PostgreSQL's backend
also validates ownership markers and exact table structure; neither backend
silently adopts or repairs an incompatible pre-existing ledger.

`module` lets one ledger serve multiple independently-versioned application
areas. The companion module row preserves identity even at applied count zero,
and count/head catch a row deletion or inconsistent tail without relying only
on whatever prefix the current process happens to supply. The per-migration
rows retain full history so a changed old migration cannot hide behind an
unchanged head.

### Why the baseline barrier exists

Flattening replaces a long, fully applied legacy history with one baseline
definition describing the schema as it exists now. This keeps future planning
small, but the baseline's SQL is a recipe for creating that schema from empty;
it is not a reversible step that can safely be run against the already-created
schema. `into_baseline()` makes that distinction part of its checksum.

**The barrier is a rollback floor:** later migrations may be rolled back, but
the baseline cannot be crossed as if the database were empty.

**Example:** the complete legacy definitions for versions 1–40 are all
applied. The manifest's legacy side is exactly those definitions. Its marked
baseline is version 41, and later migrations start at 42.

Flatten replaces that verified history with the baseline row at 41; it does
**not** execute the baseline SQL. New migrations start at 42. Rolling back
later versions is allowed when their down SQL exists; a request including 41
is rejected. Keep the marked version-41 baseline first in future definitions:
removing the marker changes its checksum and fails validation.

**Core assessment is not execution.** `FlattenManifest::assess(None, ...)` can
assess a truly fresh logical history, but proves neither that baseline SQL ran
nor persists anything. SQLite/PostgreSQL backend `flatten` requires an existing
registered module with the exact complete legacy history; fresh and empty
modules are rejected. For a fresh database, run the marked baseline through
ordinary backend `up`.

### Do I need flattening?

No. It is an optional way to compact a mature, fully applied history into a
current-schema starting point. It deliberately makes earlier steps
irreversible; it does not make old history reversible.

### Why more than `schema_migrations`?

A version-only table says “what version is newest,” but cannot reliably detect
edited definitions, changed identifier bindings, history holes, or rollback
past a flattened baseline. Checksums and summary metadata let the backends
reject ambiguous history rather than guess how to repair it.

**Use only what you need:** no dynamic identifiers means no binding setup;
never flatten and there is no baseline. Transactions and locking belong to the
database backend. Core planning alone provides neither concurrency control nor
execution atomicity, and cannot undo non-transactional SQL effects.

`FlattenManifest::legacy` and `FlattenManifest::baseline` expose immutable
definition views so backends can bound ledger reads and callers can apply a
fresh baseline through their normal executor.

Identifier templates use `{{key}}` only in SQL code. Values are lowercase ASCII
identifiers of at most 63 bytes, quoted per dialect. Distinct keys may not map to
the same physical name, but each key can occur repeatedly. Placeholders in
comments, quoted strings and identifiers, and PostgreSQL dollar strings are
rejected. PostgreSQL nested comments and ASCII dollar tags are recognized;
non-ASCII dollar tags are rejected. SQLite bracket identifiers and backticks,
MySQL double quotes and block comments, and backslashes in quoted forms are
rejected rather than guessed. PostgreSQL dollar signs immediately following an
unquoted identifier, digit, another dollar sign, or a placeholder, and
digit-starting dollar sequences, are rejected rather than guessed. SQLite
line comments end at LF (including the LF in CRLF); PostgreSQL and MySQL line
comments end at CR or LF. The supported
subset is intentionally conservative; this is a placeholder lexer, not an SQL
parser or validator. Review static SQL for the target engine and configuration.
The binding fingerprint hashes sorted length-prefixed bindings and must be
persisted with the ledger. Plans reject a different current fingerprint.

Empty or whitespace-only down SQL is rejected; no-op reversals are not
implicit. Database transactional behavior is outside this crate.

`run_bounded` polls independent caller futures with a nonzero concurrency
limit, retains input order, and returns every success/error. It requires no
runtime and spawns no workers. A future that never resolves necessarily prevents
the result from completing; cancellation is the caller's responsibility.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
