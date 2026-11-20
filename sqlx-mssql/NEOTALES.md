# SQL Server driver adaptation

This is `sqlx-mssql-rs-core` 0.1.2 from crates.io, licensed Apache-2.0.
The package is renamed `neotales-sqlx-mssql`; its Rust library name remains
`sqlx_mssql_rs_core`. The upstream source is
<https://github.com/strawberyy-coconut/sqlx-mssql-rs>.

Unlike upstream, this fork accepts native `@P1` etc. RPC references and sends
the original statement unchanged. It validates positional references without
rewriting SQL; local variables, `DECLARE` and ambiguous assignments are rejected.
The upstream README below is retained for provenance and its `?` examples do not
describe this fork's parameter API.

The local change exposes `MssqlConnection::fetch_events`, returning
`ResultSetStart(columns)`, `Row`, `ResultSetEnd`, and
`NonRow { rows_affected }`. Empty row sets have both boundaries and metadata.
Non-row statements without a count have `None`; TDS may collapse no-op
statements altogether. Existing SQLx `fetch_many` projects these events back
to its original row/result interface.

The stream borrows the connection. Consume it fully or drop it before using the
connection again. Dropping between events marks the connection for deferred
draining. Cancellation or failure during TDS I/O poisons the connection:
subsequent driver operations fail, and SQLx's pool release ping discards it.
This deliberately includes server errors where recovery might have been safe.
Pending drain and rollback flags are cleared only after success. The live test
exercises interrupted submission, interrupted drain, and pool replacement;
it does not establish safety for every pool lifecycle or transaction failure.

Synthetic-token unit tests exercise empty sets and a DML count. The opt-in
`tests/live.rs` requires Docker, uses a digest-pinned SQL Server image, an
ephemeral random test password, and a dynamically assigned loopback-only port.
It disables certificate validation only for that disposable test server.
`tests/live.sh` checks Docker, requires `--list` to report the exact ignored
test name, and only then runs it; both data workflows invoke that script so an
empty or renamed selection cannot be reported as a pass.
