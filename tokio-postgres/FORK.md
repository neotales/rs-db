# Isolated quiet PostgreSQL driver fork

This directory contains the `src` directory, README and MIT/Apache-2.0 licenses
from the crates.io `tokio-postgres` **0.7.18** release. The normalized upstream
manifest was retained and edited as described below; upstream benchmarks and
their dev-dependencies are not vendored. The unmodified upstream tests are
vendored separately under `conformance/`; in the `rs/lib` source tree that is
`tokio-postgres/conformance/`. It is a detached test workspace
excluded from the packaged crate. The fork source is approximately 380 KiB.
Neotales maintains this copy as part of `rs-lib`; upstream is not responsible for
the changes. Both upstream license texts are included
unchanged.

Package identity is `neotales-tokio-postgres` `0.0.0-alpha.0`, a fork of upstream `tokio-postgres` `0.7.18`, library name
`neotales_tokio_postgres`. It is an independent path dependency, not a Cargo
`[patch]`: stock `tokio-postgres` remains usable alongside it. Its prerelease
version must be pinned exactly by consumers. For a crates.io distribution, the
fork must first be published under this distinct package name and version, then
the depending crate can resolve its version; local path dependencies alone do
not publish the dependency. Availability of that package name is not established.

Changes from 0.7.18:

* `query.rs`: remove logging and debug formatting of bound parameters.
* `prepare.rs` and `simple_query.rs`: remove logging of SQL query text.
* `connection.rs`: the `Connection` future discards notices instead of logging
  their text, and low-level trace calls are removed. `poll_message` still exposes
  notices to an explicit caller.
* `copy_in.rs` and `copy_out.rs`: remove copy statement debug logging.
* `lib.rs` and `client.rs`: update examples to the distinct library name.
* `client.rs`: expose exclusive clearing of cached type lookup statements and
  type definitions before server-side `DISCARD ALL`.
* `completion.rs`, `query.rs`, `simple_query.rs`, and `client.rs`: expose bounded,
  classified command tags and authoritative ReadyForQuery transaction status.
  `batch_complete` discards result rows and records whether any command managed
  transactions. Typed and simple-query streams retain completion metadata,
  including earlier transaction-control tags if a later script command errors.
  No server command string is retained or logged. The parent owner can distinguish COMMIT from
  PostgreSQL's successful ROLLBACK response in an aborted transaction.
* `generic_client.rs` and `config.rs`: repair upstream rustdoc links and a bare URL.
* `error/mod.rs`: expose safe boolean TLS and transport classification without
  formatting or revealing the error's source.
* `README.md`: link to this fork description ahead of the upstream README.
* Manifest: distinct package/library/version, omit unavailable upstream test and
  bench declarations and their dev dependencies (except Tokio for doctests), omit
  the ignored resolver, remove the unused `log` dependency, and exclude the
  conformance harness from the packaged crate.

The fork's source contains no logging calls. Errors returned to a raw driver
caller can contain server-provided text: callers must sanitize the entire error
chain and must not log it. The parent crate's compiled `experimental` API returns
sanitized categories without retaining the raw driver error or source chain.
TLS libraries, connectors and other pool implementations can log independently
and need separate review.

## Ownership and updates

The `rs-lib` maintainers own the fork and must track upstream security releases.
For each update, import the exact new crates.io release in a separate change,
compare its entire source and dependency logging surface (including newly added
macros, tracing and connection startup/error paths), reapply the minimal patch,
record the version and diff here, run the live canary tests plus upstream
conformance tests, and review licenses. Do not assume this patch applies safely
to later releases. The unmodified upstream tests are vendored under
`conformance/` (at `tokio-postgres/conformance/` in the `rs/lib` source tree). The default feature lane is run in CI; default, no-default, all-features,
genuine MD5 authentication and a separate Unix-socket test have also been run
manually. The verified TLS adapter implements RFC 5929 `tls-server-end-point`
SCRAM channel binding; its README records the supported signature digests and
the explicit `require` failure for certificates without a usable digest, and
benchmarks and other platforms are not covered; see its README for exact results
and limitations. A green conformance run is evidence for the
covered surface, not proof of general compatibility.

## Integration limits

The fork's client, connection, `MakeTlsConnect` and TLS stream trait types are
distinct from upstream's despite similar APIs. Stock `tokio-postgres` pool and TLS
adapter implementations cannot simply accept these types. The separate
verified TLS adapter at `dbapi-postgres-tls/README.md` in the `rs/lib`
source tree implements the fork's traits. The former test-only pool prototype
has been replaced by the parent crate's compiled `experimental` API at
`dbapi-postgres/src/experimental.rs`, which owns driver permits,
acquisition/reset deadlines, cleanup, and best-effort cancellation. Its live
tests exercise that implementation rather than including a test-only copy.

The compiled transport uses `prepare_typed` for dbapi parameter binding and
`query_raw` for bounded, detached typed rows. It waits for `ReadyForQuery` before
reporting success; local streaming failures or lost completion after dispatch
remain unknown write outcomes. The parent API now has connection-affine owned
transactions, savepoint-protected preparation, finite client deadlines and
verified commit/rollback completion. It deliberately does not use the fork's
stock `Transaction::commit` (which discards the command tag). It remains an
explicitly experimental lower-level API. The parent public `PostgresPool` now
implements the local dbapi Pool/Session/Transaction traits over it, including
bounded reusable cleanup and idle TTL. The old SQLx public provider and its
notice leak have been removed, not filtered globally. Wider conformance and
main's evolving core API reconciliation remain separate work; see the
provider README at `dbapi-postgres/README.md` in the `rs/lib` source tree.
This is not a production-safety claim.
