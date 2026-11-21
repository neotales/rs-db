# neotales-dbapi-mssql

SQL Server provider for `neotales-dbapi`, built on the native TDS driver `mssql-tds`.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi neotales-dbapi-mssql
```

## What you can do 🔧

**Connect with verified encryption and native `@P1` parameters.** `MssqlConfig::new` requires
encryption and verifies the certificate chain and host name against system roots.

```rust,no_run
use neotales_dbapi::{Pool, QueryOptions, RowLimit};
use neotales_dbapi_mssql::{MssqlConfig, MssqlPoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = MssqlPoolProvider::connect(MssqlConfig::new(
        "sql.example.com",
        1433,
        "app",
        "from-your-secret-store",
        "app",
    ))
    .await?;
    let rows = provider
        .fetch_all_async(
            "SELECT name FROM users",
            QueryOptions::new().limit(RowLimit::new(10, 4096)?),
        )
        .await?;
    let _ = rows;
    Ok(())
}
```

**Pin a private server certificate.** `MssqlTls::PinnedCertificate(path)` keeps verification while
accepting an exact self-signed certificate. `MssqlTls::Insecure` is an explicit downgrade.

**Use the restricted write grammar.** `execute_async` accepts one complete `INSERT ... VALUES` or
restricted `UPDATE`; use a trusted script for other batches. The session scanner is conservative,
not a SQL parser.


## Scope

SQL Server provider for [`neotales-dbapi`], built on the native TDS driver
`sqlx-mssql-rs-core` (`mssql-tds`).

**Not yet accepted for use:** the session-control scanner is not a parser and has not
established a safe boundary for arbitrary batches or trusted database objects. The
native-parameter and restricted-execute changes require independent review and wider testing.

[`neotales-dbapi`]: https://docs.rs/neotales-dbapi

## Driver choice

`tiberius` was evaluated. This workspace already pins and uses the native TDS
driver `sqlx-mssql-rs-core` in `neotales-x-auth-store-mssql`; it is fully
asynchronous, has no ODBC/background-thread bridge, and carries SQL Server type
and error metadata the DBAPI rows need. Adding a second native TDS stack would
duplicate TLS, authentication, and type-mapping review. This crate therefore
uses a local fork of `sqlx-mssql-rs-core`, not `tiberius`, and does not claim `tiberius`
compatibility.

## TLS

`MssqlConfig::new` requires encryption and verifies the server certificate chain
and host name against system roots (`MssqlEncryption::Required`,
`trust_certificate=false`). `MssqlTls::PinnedCertificate` keeps verification by
pinning the exact server certificate. `MssqlTls::Insecure` encrypts without
verification and is an explicit downgrade. There is no plaintext mode.

The testcontainer lane uses the explicit insecure mode because the disposable
SQL Server image presents a self-signed certificate. A verified-system-roots
lane needs a trusted server certificate fixture and is not claimed as tested.

## SQL execution

Parameters use SQL Server's native `@P1`, `@P2`, ... names, in bound slice order.
The original SQL text is passed unchanged as the statement to `sp_executesql`;
the driver supplies separately named RPC values. Repeated references are allowed,
but every supplied index must be referenced and gaps, lowercase names, `?` markers,
local variables and `DECLARE` are rejected. `@@` system variables are allowed.
For safety the validator also rejects a parameter immediately followed (after
whitespace) by an operator or comment opener: place the parameter on the right
of comparisons. Quoted strings and identifiers, nested block comments and line
comments do not contain parameter references.

`fetch_async` accepts batches and buffers all result sets before returning, including empty
sets; late errors are returned by `fetch_async`, not deferred until reader finish.
Rows, bytes, and set counts are bounded across the batch. `execute_async` accepts exactly one
complete restricted write statement:

- `INSERT INTO [schema.]table [(column,...)] VALUES (value,...)`, where names are unquoted
  ASCII identifiers and values are unsigned integer literals or native parameter references
  (`@P1`, ...). This form does not accept comments.
- `UPDATE [schema.]table SET column = value [, column = value ...] [WHERE predicate]` with an
  optional trailing `;`. Identifiers may be unquoted ASCII, `[bracketed]`, or `"quoted"`;
  values are signed integer literals, `N'...'`/`'...'` text, `NULL`, native parameter
  references, or a bare column reference. A predicate supports `=`, `<>`, `!=`, `<`, `<=`,
  `>`, `>=`, `IS [NOT] NULL`, `AND`, `OR`, and parentheses. Line comments end at
  CR or LF, as SQL Server does; nested block comments are allowed anywhere the
  grammar allows whitespace.

No other statement is accepted by `execute_async`; functions, subqueries, `FROM`/`OUTPUT`
clauses, session `SET`, and a second statement return `Unsupported`. `INSERT` retains its
narrower historical grammar. `execute_trusted_script_async` accepts a trusted batch. The
preflight rejects listed code tokens outside strings and comments, not only at a semicolon
boundary. This catches `SELECT 1\nBEGIN TRANSACTION` and `SET /* comment */ IMPLICIT_TRANSACTIONS ON`.
Unquoted `COMMIT`, `ROLLBACK`, `SAVE`, `USE`, `GO`, `EXEC`/`EXECUTE`,
`PREPARE`/`DEALLOCATE`, and `sp_`/`xp_` identifiers are rejected conservatively;
quote an identifier if it legitimately uses one of those names. `BEGIN TRY` and
`BEGIN CATCH` blocks remain allowed. Only a conservative list of leading batch
keywords is accepted; an omitted `EXEC` procedure invocation such as `dbo.proc`
is rejected. This does not validate the grammar of the rest of the batch.

A batch containing an unquoted `SET` token is held to validated semicolon boundaries. Every
statement in that batch must start with a recognized leading keyword, and every statement that
contains `SET` must be one complete restricted `UPDATE` as described above. CR or LF is not a
statement boundary, so `UPDATE ... SET ...\rSET NOEXEC ON` (or the same with LF) returns
`Unsupported` rather than running two statements. Batches that mix `UPDATE` with other statements
must separate them with semicolons; an `UPDATE` wrapped in `IF`/`BEGIN` or a CTE is not accepted.
A batch without `SET` keeps the lexical rules above.

`GO` is a client directive, not T-SQL. This provider rejects a `GO` code token
rather than sending it to the server; a `GO` inside a string literal or comment
is ignored. Use `neotales_migration::split_sql_server_batches` (or an equivalent
caller-owned splitter) and execute batches individually. A `GO` boundary is not
a transaction boundary.

Session statements reject transaction control (`COMMIT`, `ROLLBACK`, `SAVE`,
`BEGIN TRANSACTION`), `USE`, dynamic execution (`EXEC`/`EXECUTE`/`PREPARE`),
`sp_`/`xp_` entry points, session `SET` statements, and `GO`. This includes
`SET NOEXEC`, `PARSEONLY`, `FMTONLY`, `SHOWPLAN`, `ANSI_DEFAULTS`, and
`IMPLICIT_TRANSACTIONS`. `UPDATE ... SET` is accepted only through the complete
restricted grammar above; the `SET` of an assignment list is not a session
statement. The fetch path keeps a blanket `SET` rejection because it does not
offer a write grammar.
Transaction
statements allow DDL because SQL Server DDL is transactional in the ordinary
case, but still reject those indirect and transaction-control forms.

Every acquired session runs `SET IMPLICIT_TRANSACTIONS OFF` and verifies the
session option bit, so standalone writes and transaction boundaries do not
silently begin implicit transactions. It also resets the session isolation level
to `READ COMMITTED` and verifies it, so a pooled connection cannot leak a prior
caller's isolation. Every `BEGIN TRANSACTION` sets the requested level, or the
`READ COMMITTED` default, before starting. After a successful commit or rollback it
resets to `READ COMMITTED` before returning the session. The lexical checks reject
some direct changes, but do not prove that all paths which change session settings
are blocked.

The provider verifies both `@@TRANCOUNT` and `XACT_STATE()` before and after
BEGIN and COMMIT, and after ROLLBACK. A missing or unexpected state invalidates the connection;
an unverified commit reports `CommitOutcomeUnknown`. A confirmed commit followed
by isolation-reset failure still succeeds while the connection is discarded.

The connection is wrapped in a setup guard immediately after checkout. If the
acquisition future is cancelled during setup queries, the physical connection is
discarded instead of returning with partially changed session state.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. SQL Server uses native `@P1`-style names in bound slice order; the adapter
supplies a hard row/byte bound for the page plus one lookahead row, drains and rejects extra result
sets, and does not rewrite or classify SQL. The caller supplies the read-only/replay-safe/authorized
assertion and the scoped continuation.

The ignored live test `mssql_sql_page_source_contract` in `tests/live.rs` exercises this statement
against the crate's digest-pinned SQL Server 2022 container. It covers the cross-tenant and
single-tenant scans, native `@P1` parameters, one-row lookahead, exact page order, and request
counts. `cargo test` stays fixture-free; select only this test with:

```sh
cargo test -p neotales-dbapi-mssql --test live -- --ignored --exact mssql_sql_page_source_contract
```

```sql
SELECT TOP (@P4) tenant, seq, body FROM events
WHERE tenant = @P1
  AND (tenant > @P2 OR (tenant = @P2 AND seq > @P3))
ORDER BY tenant, seq;
```

`@P4` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page size.
T-SQL has no row-value comparison here, so the predicate is expanded. `(tenant, seq)` must be unique
and `NOT NULL`. This is test-schema coverage for the pinned image only; it is not a claim about
other SQL Server versions or deployments.

## Rows and values

Supported decoded types, by SQL Server type name:

| SQL Server type | `DbType` |
| --- | --- |
| `bit` | `Bool` |
| `tinyint`, `smallint`, `int`, `bigint` | `I64` |
| `real`, `float` | `F64` |
| `char`, `varchar`, `text`, `nchar`, `nvarchar`, `ntext`, `xml`, `json` | `Text` |
| `binary`, `varbinary`, `image` | `Bytes` |

`decimal`, `money`, date/time types, `uniqueidentifier`, spatial types,
`sql_variant`, and `hierarchyid` are not decoded; the row fails with `Decode`.
Cast them in SQL when a lossless text or numeric form is sufficient. Untyped
`NULL` follows SQL Server's inferred type (usually `int`).

Fetch limits are hard row, byte, and result-set bounds; the provider fails with `Overflow`
rather than truncating. On a local limit or row-decoding failure, it keeps consuming
the batch before returning the error, because later statements may write. A transport
failure or timeout during that drain is reported as an unknown write outcome instead.
Text and binary values are length-checked before the
provider copies them into an owned DBAPI cell. The TDS driver may already have
buffered the wire value before that check, so this bounds provider-level
detached-row allocation, not the driver's internal receive buffer. Names are
server-supplied; duplicate names make name lookup fail.

## Transactions, timeouts, and errors

Interactive transactions use one exclusive session. All four portable isolation
levels are applied before `BEGIN TRANSACTION`. Read-only transactions are
rejected as `Unsupported`. Dropping a transaction, or cancelling a begin, statement, commit, or rollback future, discards the physical connection instead
of returning it to the pool.

`ExecuteOptions::timeout_ms` is a client-side deadline. On expiry the in-flight
future is dropped and the connection is discarded. A dispatched write or fetch
returns `WriteOutcomeUnknown`; a dispatched commit returns
`CommitOutcomeUnknown`. Dropping the future does not stop the server from
finishing the statement.

Once COMMIT is acknowledged, a later failure resetting isolation discards the
connection but does not change the successful commit result.

Errors retain only a stable `ErrorKind`; SQL, parameter values, connection
strings, and driver messages are not attached. Constraint, deadlock/lock-wait,
syntax/object, timeout, and transport failures are distinguished. Statement and
slow-statement logging are disabled on provider connections. Direct provider,
session, and transaction calls do not retry. `PolicyPool` telemetry and explicit
replay-safe retries work through the DBAPI traits.

## Trusted SQL

This is not a sandbox or SQL validator. All SQL is trusted. SQL Server stored
procedures, functions, triggers, views, and other objects are trusted database
objects and can have effects the lexical checks cannot see. `EXEC`/`sp_`/`xp_`
entry points are rejected, but an allowed `SELECT`/DML can still invoke a trusted
function or trigger. Never feed this provider untrusted SQL or install untrusted
objects.

## Tests

```sh
cargo test -p neotales-dbapi-mssql --all-targets
dbapi-mssql/tests/live.sh
sqlx-mssql/tests/live.sh
```

The provider live suite starts a digest-pinned SQL Server container through
`testcontainers`, publishes no ports, uses an OS-CSPRNG nonempty SA password, and
connects through a loopback-only proxy. It covers types/nulls, constraints,
text/binary rows, bounds, transactions and cleanup, restricted `UPDATE`
successes with parameters and comments, rejected session settings and hidden
batches, `UPDATE` transaction rollback/commit, an `UPDATE` lock-wait timeout
reported as an unknown write outcome, transaction-control/indirect rejection,
implicit-transaction rejection, timeouts, trusted batches, `GO` rejection,
secure-default failure against the self-signed fixture, telemetry,
unknown-outcome poisoning, isolation reset across checkouts, nested-comment
rejection, string/comment `GO` handling, and cancelled setup.

The forked driver lane runs the exact ignored cancellation/pool-reuse test; its
script fails before running anything unless `--list` reports that exact test.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
