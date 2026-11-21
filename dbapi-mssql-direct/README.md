# Direct SQL Server feasibility probe

This is an isolated **experimental DBAPI provider, not suitable for
production**. It depends on Microsoft's `mssql-tds` 0.3.0 and not `SQLx`.
**Linux sequential TCP connections use a source-quieted vendor** of that exact
release, with quiet native-tls and a path-wired Tokio TLS wrapper. Other targets
fail closed with `UnsafeTracing` pending platform validation. Every TDS
event/instrumentation site is removed or expands to an empty token consumer,
so `tracing/log-always` cannot expose its arguments. Production code never
changes a global logger, subscriber, or level. See [the audit](TRACE-AUDIT.md)
and [source provenance and reproduction](vendor/PROVENANCE.md).
It includes an exclusive finite-capacity connection owner which always
discards on release, and shared DBAPI parameter mapping for all five portable
types and typed nulls. `provider::DirectPool` implements DBAPI bounded fetch
and read-committed transactions, and can be wrapped in `PolicyPool`. Execute
supports only `INSERT INTO table VALUES (@P1)`, `UPDATE table SET column = @P1
WHERE column = @P2`, and `DELETE FROM table WHERE column = @P1`, with whitespace
between tokens and simple single-part identifiers. Extra trigger results or
suppressed/missing counts yield `WriteOutcomeUnknown`, even when a write took effect.
Trusted scripts accept only one whitespace-tokenized statement per GO-separated
batch: `CREATE TABLE table ( column BIGINT )`, `DROP TABLE table`, or
`INSERT INTO table VALUES (integer)`. Every batch is validated before dispatch.
GO must stand alone on its line; quotes and comments are rejected. This
deliberately narrow splitter is not the migration core's general SQL Server
splitter. It rejects custom transaction options, strings, comments,
quoted identifiers and several session-control keywords. Its conservative
lexical preflight is not a general SQL parser; SQL remains trusted. The lower
level `Probe::query` still materializes unbounded driver values. This narrow
adapter and its tracing boundary require independent review.
Fetch preflight permits `@@SPID` as well as positional `@P1` parameters.

A script on a plain `Session` runs each batch under SQL Server autocommit:
an earlier batch can persist if a later batch fails. A script on a transaction
shares that transaction, but atomicity depends on the SQL Server statements
being transactional. A dispatched failure or interruption is an unknown
outcome and discards the connection; it does not itself prove rollback.

```rust,no_run
use neotales_dbapi_mssql_direct::provider::DirectPool;
use neotales_dbapi::{Pool, QueryOptions, TxOptions};
async fn example(pool: &DirectPool) -> Result<(), Box<dyn std::error::Error>> {
let mut session = pool.acquire_async().await?;
let mut tx = session.begin_async(TxOptions::default()).await?;
tx.execute_trusted_script_async("CREATE TABLE example ( n BIGINT )\rGO\rINSERT INTO example VALUES (1)", QueryOptions::new()).await?;
tx.rollback_async().await?;
Ok(())
}
```
`DirectPool::begin_locked` offers an experimental transaction-owned application
lock on the same exclusive connection. It invokes `sp_getapplock` through a
fixed `sp_executesql` batch with bound resource and timeout values; it is not
a direct stored-procedure RPC. Return codes are checked before returning the
handle, whose commit, rollback or drop releases the transaction/connection.
The handle can execute the same narrowly validated scripts on its locked
transaction. `verify_server_version` gates on SQL Server major version 16;
`execute_script_plan` validates all supplied scripts before executing any of
them. Neither method manages a ledger or validates history, checksums, dirty
state, bindings, drift, or barriers on their own.
The lock timeout is solely the server lock-wait timeout, not a total deadline;
acquisition uses the pool deadline, while BEGIN and network I/O need a
caller-enforced total deadline. For example, with a Tokio runtime:

```rust,no_run
use neotales_dbapi_mssql_direct::provider::DirectPool;
use std::time::Duration;
async fn example(pool: &DirectPool) -> Result<(), Box<dyn std::error::Error>> {
let locked = tokio::time::timeout(Duration::from_secs(5), pool.begin_locked("job", 1000)).await??;
// `locked` may execute the supported trusted-script subset before rollback.
locked.rollback().await?;
Ok(())
}
```

Dropping the acquisition future or handle discards the connection; a cancelled
commit does not prove rollback.

## Direct migration ledger

`migration::SqlServerBackend` now provides `initialize_module`, `up`, and
`down` over `DirectPool`, without `SQLx`. It ports the comparison backend's
two-table ledger format and catalog verifier: schema/ownership markers,
binary collations, exact columns/checks/indexes, row consistency, module
binding, checksum prefix validation and baseline rollback barrier. The schema
must exist. Declared managed objects cannot be adopted on first application.
Internal catalog/DDL/ledger SQL is crate-private, not a public grammar bypass.
Application scripts retain the narrow grammar above; this is **not** general
T-SQL migration support.

Ledger/planner source ported from `plab-mssql-integration` commit
`92df4416ba868481d4df0d1bfe51779bdaced5b3` (`migration-mssql`).

Every call acquires the transaction-owned application lock before inspecting
or creating the ledger; scripts and ledger writes commit in that transaction.
After acquiring the lock and checking the SQL Server version, but before ledger
inspection or DDL, the backend reads the current database's
`sys.databases.delayed_durability_desc` value. `FORCED` is rejected before any
ledger inspection or DDL, because SQL Server may acknowledge a transaction
before its log records are hardened. `DISABLED` and `ALLOWED` are accepted;
missing, unreadable, or unrecognized metadata fails closed. This validates the
setting at call time only; it does not serialize with `ALTER DATABASE`. A
database administrator must not change delayed durability to `FORCED` while
migration operations rely on durable commit acknowledgement.
The complete up/down plan is preflighted before the first application batch.
Errors attempt rollback without masking the original error; interrupted or
failed dispatched operations discard the socket. This does not acknowledge
server rollback or promise prompt lock release. There is no dirty-state row:
unconfirmed transactions must be reconciled after connection closure.

The lock timeout bounds only the server lock wait, not network or total time.
Use a caller-enforced total deadline. An unconfirmed commit remains
`CommitOutcomeUnknown`. Repeat `up` with identical definitions and binding to
reconcile; **never blindly retry `down`** because it is not idempotent. Use
`initialize_module` to compare the observed count/head with the intended states.
This backend and the direct provider remain experimental.

### Complete migration call and unknown-commit reconciliation

This `no_run` example includes provider setup and a complete migration
definition. After an unknown commit outcome, reconcile under the same module
identity and binding. If the observed state is the expected old or new state,
repeating `up` with identical definitions is safe. Never apply this retry rule
to `down`.

```rust,no_run
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

use neotales_dbapi_mssql_direct::{
    ExclusivePool,
    migration::{Error as MigrationError, SqlServerBackend},
    provider::DirectPool,
};
use mssql_tds::connection::client_context::ClientContext;
use neotales_dbapi::ErrorKind;
use neotales_migration::{Migration, Migrations};

async fn migration_example() -> Result<(), Box<dyn std::error::Error>> {
    // Supply deployment-specific connection configuration through your
    // application's secret/configuration boundary; do not hard-code credentials.
    let mut context = ClientContext::default();
    context.user_name = "migration-user".into();
    context.password = "provided-at-runtime".into();
    context.database = "application".into();
    let owner = ExclusivePool::new(
        context,
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1433),
        2,
        Duration::from_secs(5),
    )
    .map_err(|_| std::io::Error::other("invalid pool configuration"))?;
    let pool = DirectPool::new(owner);
    let backend = SqlServerBackend::new("dbo", "schema_history", Duration::from_secs(5))?;
    let managed_objects = [("dbo", "example")];
    let migrations = Migrations::new(vec![Migration::new(
        1,
        "create example",
        "CREATE TABLE example ( n BIGINT )",
        Some("DROP TABLE example".into()),
    )?])?;

    match backend
        .up(&pool, "application", &migrations, "binding-v1", &managed_objects)
        .await
    {
        Ok(state) => assert_eq!(state.applied_count(), 1),
        Err(MigrationError::Database(error))
            if error.kind() == ErrorKind::CommitOutcomeUnknown =>
        {
            let observed = backend
                .initialize_module(&pool, "application", "binding-v1", &managed_objects)
                .await?;
            let is_expected_old = observed.applied_count() == 0 && observed.head().is_none();
            let is_expected_new =
                observed.applied_count() == 1 && observed.head().is_some_and(|(v, _)| v == 1);
            if !is_expected_old && !is_expected_new {
                return Err("migration state is neither the expected old nor new state".into());
            }
            // `up` with exactly the same definitions and binding validates the
            // history and does not rerun SQL that the ledger marks as applied.
            backend
                .up(&pool, "application", &migrations, "binding-v1", &managed_objects)
                .await?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
```

The pinned live lane covers up/repeat/down/reapply, binding and definition
drift, baseline/missing-down barriers, managed-object non-adoption, whole-plan
preflight, concurrent writers, lock timeout, cancellation, post-dispatch failure
and reconciliation, partial ledger, incompatible shape and inconsistent rows.

The probe forces required encryption, does not trust unverified certificates,
and supports the driver's certificate pin field. The pinned live lane also
generates an ephemeral CA and SQL Server certificate, then exercises trusted
CA/hostname success, wrong-CA, wrong-hostname, expired-certificate, and
correct/incorrect pin cases through the public `DirectPool` verification path.
The generated trust root is provided only to separate child test processes via
`SSL_CERT_FILE`; neither the test runner nor application system trust is changed.
The migration fault case holds the encrypted COMMIT reply while an independent
TLS session confirms both the application effect and history row are committed.
Only then does the proxy cut the client connection; migration returns
`CommitOutcomeUnknown`, and repeating `up` reconciles without duplicating either.
The ordinary protocol fixture uses required encryption without certificate
validation only in its test-only harness. The only insecure constructor
exists only under Rust's `cfg(test)` in the library's private unit-test
harness. The production crate has no insecure-test feature or public insecure
constructor; downstream `--all-features` builds cannot enable one. The live
lane does not establish production readiness.

Queries pass the original SQL to `sp_executesql`; all driver errors after
dispatch are mapped to unknown outcome. Dropping the query future owns and
drops the client, so it cannot return to this probe. Sequential TCP mode
avoids the driver's parallel-connect spawn path. Quietness does not depend on
future/task dispatcher inheritance: all driver source paths, including
platform-specific auth/TLS/routing code, use discarded diagnostic token trees.
The live test captures both global logging channels with hostile feature
unification, connection/query/error/auth/packet/cleanup sentinels, and concurrent
application positive controls. The exact-source guard prevents unaudited vendor
source-file or crate-manifest changes within its sealed inventory; it does not
seal the enclosing workspace's resolved dependency closure. CI separately checks
that this crate's normal dependency tree contains no `SQLx`. Other OS transports
and integrated authentication have not been live-validated. In particular,
accepting an arbitrary `ClientContext` is still not a safe production
configuration boundary.

DBAPI fetch limits count aggregate detached row names and values, rows and
sets (including empty sets); overflow discards the result and connection and
returns `WriteOutcomeUnknown` because the batch was not drained to completion.
They do not bound wire buffers or a single transient driver-materialized cell.
Text decoding may also temporarily allocate a converted value before checking
its detached byte size. The driver's pull cursor (`next_row_cursor`,
`read_row_column`, `read_active_plp_chunk`) may reduce large-cell transient
allocation, but has not been integrated or tested here. Unknown-length PLP
values use an owned allocation path in the driver's ordinary row decoder.

```rust,no_run
use std::{net::SocketAddrV4, time::Duration};
use neotales_dbapi_mssql_direct::{ExclusivePool, provider::DirectPool};
use mssql_tds::connection::client_context::ClientContext;
use neotales_dbapi::{Pool, QueryOptions, Parameter, DbType, Value};
async fn demo(context: ClientContext, address: SocketAddrV4) -> Result<(), Box<dyn std::error::Error>> {
let owner = ExclusivePool::new(context, address, 4, Duration::from_secs(5))
    .map_err(|_| "invalid pool configuration")?;
let pool = DirectPool::new(owner);
let value = Parameter::new(DbType::I64, Value::I64(42))?;
let row = pool.fetch_one_async("SELECT @P1 AS answer", QueryOptions::new().bind(value)).await?;
assert_eq!(row.get::<i64>("answer")?, 42);
Ok(())
}
```

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` also works over `provider::DirectPool`'s DBAPI
`Session` for bounded reads. The caller supplies the native `@P1`-style SQL, typed parameters, and
the explicit read-only/replay-safe/authorized assertion; the adapter supplies the lookahead
`RowLimit`.

The ignored live test `live::direct_sql_page_source_live` in `src/live.rs` exercises this against
the same digest-pinned SQL Server 2022 container as the direct-provider suite. It seeds through the
low-level `Probe` because this provider's `execute_async` grammar is intentionally narrow, then
pages through `DirectPool`. It covers cross-tenant and single-tenant keyset pages, typed parameters,
one-row lookahead, exact page order, and request counts. Select it with:

```sh
cargo test -p neotales-dbapi-mssql-direct --lib live::direct_sql_page_source_live -- --ignored --exact --nocapture
```

```sql
SELECT TOP (@P3) tenant, seq, body FROM dbo.page_events
WHERE tenant > @P1 OR (tenant = @P1 AND seq > @P2) ORDER BY tenant, seq;
```

`@P3` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page size.
This is test-schema coverage for the pinned image only; it does not change the experimental,
not-production-ready warnings above or claim other SQL Server versions, deployments, write support,
or a materialization bound.

Run the exact disposable-container test explicitly from this crate directory:

```sh
tests/live.sh
```

Run the non-container leak and source guards with `cargo test --locked`.

TLS child diagnostics retain only fixed, allowlisted case, phase, driver category,
and panic category records. Raw child stdout/stderr and panic payloads are never
forwarded on failure. Reconciliation records distinguish lock rejection outcomes;
bounded summaries retain the latest stages rather than discarding the final failure.

For the separately scheduled, test-only start-boundary probe, see
[startup diagnosis](STARTUP-DIAGNOSTICS.md). It does not change the normal
fixture timeout or production TLS policy.

It pins the image digest, creates a random disposable password, publishes no
ports, and connects through a dynamically allocated loopback proxy. Docker
daemon administrators can inspect container environment variables. The test
does not use any external database credentials.
