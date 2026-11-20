# neotales-dbapi-clickhouse

Native-protocol (`TCP 9000`/`9440`) ClickHouse provider for the driver-neutral
[`neotales-dbapi`](https://docs.rs/neotales-dbapi) contract.

> **Narrow native API.** The provider supports bounded result fetching. `execute_async`,
> `execute_trusted_script_async`, and `begin_async` return `Unsupported` before dispatch, and all
> reported capabilities are `false`. ClickHouse native progress does not establish an authoritative
> affected-row count for every write, so this provider never invents one. Concrete sessions also
> expose `execute_no_count` for a single trusted CREATE, DROP, or INSERT, returning `()` only after
> EndOfStream. Queries set `async_insert=0`, and the method rejects unquoted `SETTINGS` tokens
> to prevent SQL overriding that setting. EndOfStream is not a promise of disk durability,
> replication, or completion of external effects. A post-dispatch error, timeout or cancellation
> leaves the write outcome unknown.
> The companion `neotales-migration-clickhouse` backend is disabled by default; its explicit
> `experimental-native-migrations` Cargo feature enables this method in ordinary dependency
> builds as well as tests. It requires trusted SQL, complete managed-object declarations,
> one coordination domain covering ledger and effects (or a correctly scoped external lock),
> and exclusive manual recovery. It provides no cluster-wide fencing, transactional coupling,
> or power-loss guarantee. Live timeout instability remains unresolved; see the companion
> backend's README before opting in. Cargo features are additive, including `--all-features`.

The SQL scanner rejects unquoted dollar signs, including dollar-quoted strings, rather than
attempting to parse that syntax. Dollar signs inside ordinary quoted strings, quoted identifiers,
and comments are allowed.

There is no HTTP path and no fallback. The only server version accepted is ClickHouse 24.8 LTS.
`ClickHousePoolProvider::connect` opens a probe connection, verifies the version, and discards it;
every acquired session opens one exclusive native connection and repeats the version gate. Other
versions fail with `Unsupported` before a session is returned.

## Operations and capabilities

| Capability | Value | Reason |
| --- | --- | --- |
| `transactions` | `false` | ClickHouse has no interactive transactions. |
| `isolation_level` | `false` | There is no portable transaction to isolate. |
| `read_only_transactions` | `false` | There is no transaction access mode. |
| `trusted_scripts` | `false` | Multi-statement scripts need authoritative write outcomes and are rejected before dispatch. |

`fetch_async` and the shared convenience methods (`fetch_all_async`, `fetch_first_async`,
`fetch_one_async`, `fetch_scalar_async`) are supported alongside the concrete no-count write method.
The no-count method validates one statement and native parameters without rewriting SQL; it is
not a sandbox. A lost response or server error must be reconciled before retrying, because a write
may have taken effect or still be running. Fetching is not proof of a
read: a trusted `SELECT` can invoke a table function or UDF with side effects. Errors after dispatch
are therefore reported as `WriteOutcomeUnknown`.

A session owns one native connection and is not `Clone`. An interrupted fetch, a transport failure,
a server exception, or a native stream closure without `EndOfStream` poisons the session. Later
operations return `WriteOutcomeUnknown` before dispatch. Acquire a new session after an uncertain
outcome. A local `Overflow` or `Decode` failure is drained through the explicit terminal packet, so
the connection remains reusable when the server completed the query.

## No-count telemetry and retry

`ClickHousePoolProvider::with_telemetry` configures `ClickHouseTelemetry` for concrete
`execute_no_count` calls, including migration lock, ledger, and script writes. It emits
`DbEvent` with fixed `Execute` operation, elapsed duration, and sanitized outcome; cancellation
after polling emits `WriteOutcomeUnknown`. Success is emitted only after EndOfStream. It does
not instrument fetches or acquisitions; the generic `PolicyPool` cannot wrap this concrete API.
Telemetry and SQL capture default off. Per-query `QueryOptions` overrides inherit from the
provider policy; capture hooks may redact or suppress SQL. Bound parameter values, rows,
driver messages, and connection details are not added to events. Captured SQL literals or
arbitrary hook output can nevertheless contain credentials or other secrets. Hook and observer panics are
contained; as with the generic DBAPI observer, each callback panic can leak its panic payload
because dropping it could panic again. This path never retries automatically and rejects retry
requests; reconcile unknown writes instead of replaying them.

## Results and limits

`fetch_async` materializes one bounded result set before returning a `BufferedReader`. Success
requires the explicit native `EndOfStream` packet. Provisional blocks are never returned when a
later server or transport error arrives.

Supported result types are `Int64`, `UInt64`, `String`, `Nullable(Int64)`,
`Nullable(UInt64)`, and `Nullable(String)`.

- `String` is always returned as DBAPI `Bytes`, including valid UTF-8. The provider never infers a
  different DBAPI type from a particular row's contents.
- `Int64` is returned as `Value::I64`.
- Nullable integer and string columns preserve null as `Null(I64)` and `Null(Bytes)` respectively;
  non-null values retain their exact integer or byte representation.
- `Nullable(UInt64)` rejects non-null values above `i64::MAX` with `Overflow`.
- `UInt64` is returned as `Value::I64` only when it fits.
  Larger values fail with `Overflow`, without truncation, and the stream is drained.
- Types the native decoder can materialize but this provider does not represent (`Bool`,
  `Int8`/`Int16`/`Int32`, `UInt8`/`UInt16`/`UInt32`, `Float32`/`Float64`) fail with `Decode`. The
  collector drains through `EndOfStream`, so the session remains reusable.
- Types the vendored native block decoder rejects before a block can be assembled (other `Nullable`,
  `FixedString`, `Date`, `DateTime`, `Decimal`, `UUID`, `Array`, `Tuple`, `Map`, and most `Enum`
  forms) are post-dispatch failures. The packet stream cannot be trusted, so the session is
  poisoned and the error is `WriteOutcomeUnknown`. Acquire a new session.
- Empty results are accepted. An empty block that carries a schema is validated exactly like a
  non-empty block; an unsupported empty schema fails with `Decode`.
- Blocks in one result must keep the same column order, names, and types. Drift fails with
  `Decode`.
- `RowLimit` is a hard aggregate bound over the materialized set. Exceeding the row or byte bound
  fails with `Overflow`; the provider never truncates. The native client also enforces its own
  packet, block, column, row, and string decode caps. A payload that exceeds a native decode cap is
  a post-dispatch failure and is reported as `WriteOutcomeUnknown`.
- A native response is one result set, so `has_next_set_async` returns `false`. The provider does
  not fabricate set boundaries.

## Parameters and SQL

SQL is never rewritten. Parameter `i` in the slice binds the native named placeholder
`{p{i+1}:Type}`; names must be exactly `p1`..`pN`, may repeat with the same declared type, and must
not leave gaps.

Supported declared types and values:

| Declared type | DBAPI value | Notes |
| --- | --- | --- |
| `Bool` | `Bool` | |
| `Int8`..`Int64` | `I64` | Exact range checked before dispatch. |
| `UInt8`..`UInt64` | `I64` | Value must be non-negative and in range; `UInt64` is capped at `i64::MAX` by the DBAPI value model. |
| `Float64` | `F64` | Only `0.0`, `-0.0`, `1.0`, and `-1.0`; ClickHouse 24.8's text parser has no exact round-trip guarantee for other finite values. |
| `String` | `Text` or `Bytes` | At most 8192 bytes per value. Empty values are supported. |
| `Nullable(Int64)` | `I64` or `Null(I64)` | |
| `Nullable(UInt64)` | non-negative `I64` or `Null(I64)` | Values are capped at `i64::MAX`. |
| `Nullable(String)` | `Text`, `Bytes`, or matching typed null | At most 8192 bytes per non-null value. |

Other `Null`/`Nullable(T)` combinations, `LowCardinality(T)`, `Float32`, `UInt64` values above `i64::MAX`, and every
other declared type return `Unsupported` before dispatch. A declared type that does not match the
value variant, an out-of-range integer, a placeholder/parameter count mismatch, a non-`pN` name, a
duplicate name with a conflicting type, and conflicting declared types all return
`InvalidArgument`.

The nullable encoding follows ClickHouse 24.8.14.39's
[`ReplaceQueryParameterVisitor`](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Interpreters/ReplaceQueryParameterVisitor.cpp)
and [`SerializationNullable`](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/DataTypes/Serializations/SerializationNullable.cpp):
native parameter values are parsed as escaped text, with `\N` as null; result columns carry a
null map followed by nested values. The native custom-setting field adds a separate escaping layer.

The provider sets `max_block_size=256` on every native connection. The vendored decoder rejects a
block above 4096 rows or 16,384 cells; with at most 64 columns, 256 rows is the largest block that
always fits that cell ceiling. This is a connection setting, not a rewrite of the caller's SQL.

The provider owns the placeholder scan. It understands ClickHouse single/double/backtick quoting,
backslash escapes, doubled quote characters, `--`/`//` line comments ending at LF, and nested block
comments. Bare `#`, unquoted dollar signs, and non-ASCII code (including placeholder types) are
rejected; non-ASCII text remains supported in recognized quoted strings, identifiers, and comments.
Placeholder names must immediately follow `{` and be immediately followed by `:`. ASCII space,
tab, CR, or LF may surround the declared type, as in `{p1: Int64 }`; whitespace within a type
name is not supported. Comments inside placeholders are not supported. Other unquoted opening
braces, including map literals and placeholders with whitespace before the name or colon, are
rejected rather than bypassing placeholder validation.
Placeholders inside strings or comments are ignored. Exactly one statement is accepted for
`fetch_async`; `BEGIN`, `COMMIT`, `ROLLBACK`, `ABORT`, `START TRANSACTION`, and `SET TRANSACTION`
are rejected with `Unsupported` before dispatch. NUL SQL and empty SQL are rejected with
`InvalidArgument`.

## Paging source

`neotales_dbapi::paging::multi::sql::SqlPageSource` can drive a caller-authored keyset page over an
acquired `Session`. The plan authors the native `{pN:Type}` placeholders; the adapter supplies a hard
row/byte bound for the page plus one lookahead row, drains and rejects extra result sets, and does
not rewrite or classify SQL. The caller supplies the read-only/replay-safe/authorized assertion and
the scoped continuation.

The ignored live test `clickhouse_sql_page_source_contract` in `tests/live.rs` exercises this
statement against the crate's digest-pinned ClickHouse 24.8 container. It covers the cross-tenant
and single-tenant scans, native `{pN:Type}` parameters, one-row lookahead, exact page order, and
request counts. `cargo test` stays fixture-free; select only this test with:

```bash
cargo test -p neotales-dbapi-clickhouse --test live -- --ignored --exact clickhouse_sql_page_source_contract
```

```sql
SELECT tenant, seq, body FROM events
WHERE tenant = {p1:Int64}
  AND (tenant, seq) > ({p2:Int64}, {p3:Int64})
ORDER BY tenant, seq
LIMIT {p4:Int64};
```

`{p4:Int64}` is the lookahead count (`min(page_size, remaining_rows) + 1`), not the published page
size. This is test-schema coverage for the pinned image only, not a claim about other ClickHouse
versions or deployments. A ClickHouse fetch is not proof of a read, and the provider reports a
post-dispatch failure as `WriteOutcomeUnknown`; the assertion is the caller's.

## Errors, timeouts, and cancellation

Errors retain only a stable `ErrorKind`. SQL, parameter values, credentials, host names, paths, and
driver text are never attached.

- Invalid configuration, options, SQL shape, parameter binding, or a zero timeout returns
  `InvalidArgument` / `Unsupported` before dispatch.
- A connection, TLS verification, or version-probe failure returns `Connection`.
- A failure after a query may have been dispatched is `WriteOutcomeUnknown`, including server
  exceptions, transport loss, a missing terminal packet, and a client-side timeout.
- No provider call retries.

[`ClickHouseConfig::request_timeout`] sets the default per-statement deadline (thirty seconds).
Each operation may override it with `QueryOptions::timeout` or disable it with
`QueryOptions::no_timeout`; a zero timeout is rejected. A timeout after dispatch drops the native
stream, poisons the session, and returns `WriteOutcomeUnknown` because server work may continue.
`no_timeout` transfers cancellation responsibility to the caller; dropping the operation still
poisons the session.

## TLS and address verification

[`ClickHouseConfig::new`] requires verified TLS. There is no insecure TLS mode and no built-in
root fallback:

- Supply an explicit PEM CA bundle with [`ClickHouseConfig::ca_file`].
- The certificate name is the host, or [`ClickHouseConfig::tls_server_name`] when the transport
  address and the verified DNS name differ. The name must be a DNS name, not an IP literal.
- Certificate-chain and host-name verification are performed by rustls.
- No client certificate authentication is offered.

[`ClickHouseConfig::plaintext_loopback`] is the only cleartext constructor. It accepts only a
numeric loopback host, and the connector checks the actual connected peer address is loopback
before sending the native handshake. `localhost` and any non-loopback address are rejected.

[`ClickHouseConfig::connect_timeout`] bounds the whole connection setup as one deadline:
resolution, the CA bundle read, TCP connect, TLS handshake, the native hello/initialisation
exchange, and the version probe. A peer that accepts a connection but never reads or never
completes the native handshake fails with `Connection` instead of hanging the caller.

CA bundles must be regular PEM files no larger than 4 MiB. On Unix they are opened nonblocking,
then the opened descriptor is checked before reading, so replacing a path with a FIFO cannot trap
the open. Directories, FIFOs, devices, and larger files are rejected. Other platforms currently
return `Unsupported` for TLS roots rather than risk a blocking special-file open. Filesystem work
runs on Tokio's blocking pool within the connection deadline; cancellation bounds the caller's
wait, not the time an unhealthy or remote filesystem can keep a worker blocked. Reads are capped
at 4 MiB plus one byte.

The password is zeroized in the configuration and is never included in `Debug`. A copy is passed to
the vendored native client, which owns it for the connection lifetime. Authentication uses the
native protocol handshake. The password does not appear in a URL, log line, or error value.

## Version scope

Only ClickHouse 24.8 LTS is tested. The provider reads `version()` after every connection and
rejects any other major/minor with `Unsupported`. The vendored fork advertises native protocol
revision 54459 and negotiates the lower of that and the server revision. Other ClickHouse-compatible
servers and other ClickHouse versions are not supported and must not be assumed compatible.

## Example

```no_run
use std::time::Duration;
use neotales_dbapi::{DbType, Parameter, Pool, RowLimit, Value};
use neotales_dbapi_clickhouse::{ClickHouseConfig, ClickHousePoolProvider};

# async fn example() -> Result<(), neotales_dbapi::DbError> {
let provider = ClickHousePoolProvider::connect(
    ClickHouseConfig::plaintext_loopback("127.0.0.1", 9000, "default", "app")
        .expect("loopback configuration")
        .password("from-your-own-secret-store")
        .request_timeout(Duration::from_secs(10)),
).await?;
let parameters = [Parameter::new(DbType::I64, Value::I64(7))?];
let rows = provider.fetch_all_async(
    "SELECT toInt64({p1:Int64}) AS id, name FROM users WHERE id = {p1:Int64}",
    neotales_dbapi::QueryOptions::new()
        .bind_all(&parameters)
        .limit(RowLimit::new(10, 4096)?),
).await?;
# let _ = rows;
# Ok(())
# }
```

For verified TLS, use `ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")` with
`.ca_file("/path/to/private-ca.pem")` loaded from the deployment's secret or config store.

## Testing

```bash
cargo test -p neotales-dbapi-clickhouse --all-targets
dbapi-clickhouse/tests/live.sh
```

The live lane starts a digest-pinned ClickHouse 24.8 container with `testcontainers`, publishes no
host ports, uses a random nonempty password from the OS CSPRNG, and reaches the container through a
loopback-only proxy. It exercises the actual `Pool`/`Session` path: capabilities, the 24.8 version
gate, supported and rejected result types, empty results, native parameter binding (including hostile
text, binary, empty values, repeats, and integer range checks), hard limits, reader semantics,
timeouts and session poisoning, late server errors, and pre-dispatch rejection of execute, scripts,
and transactions. A separate verified-TLS test uses an in-memory CA and a TLS proxy to prove that a
wrong CA and a wrong host name fail without reaching the native server, while the correct CA and name
work.

Unit tests cover the SQL scanner, parameter validation, native event collection (including EOS,
schema drift, empty schemas, string-to-bytes, and failure precedence), configuration validation, and
the version parser.

## Vendored native client

`klickhouse` is a fork of crates.io `klickhouse` 0.16.0. The local changes expose the
explicit `EndOfStream` packet, make block streams reject closure without it, propagate active read
errors, bind insert blocks to query IDs, poison the connection on an abandoned active insert, and
add the validated parameter path used here. The crate is pinned by path and is not a general
ClickHouse SDK recommendation. See `klickhouse/NEOTALES-PROTOTYPE.md` for the exact scope and
remaining decode/allocation caveats.

## License

MIT. See the workspace `LICENSE.md`.
