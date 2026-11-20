# Direct TDS quietness boundary

The original upstream `log-always` bypass is repaired at source, not suppressed
with a no-op subscriber. See [vendor provenance](vendor/PROVENANCE.md) for exact
archives, licensing, deterministic transformation, and the source inventory.
Linux sequential TCP connections are enabled. Other targets still return
`UnsafeTracing`: their native TLS/runtime dependency paths and live cleanup have
not been validated. This remains an experimental provider, not a production
certification.

## Transitive audit (locked Linux graph)

The resolved normal graph has 103 packages including this probe. Using
`cargo metadata --locked --filter-platform x86_64-unknown-linux-gnu`, traverse
only `resolve.nodes[].deps[].dep_kinds` whose kind is null, starting at the root.
Inspect those packages' normal dependency declarations for `log`, `tracing`,
`tracing-core`, or `tracing-subscriber`. The only remaining declarations are:

| Package | Potential diagnostics | Assessment |
| --- | --- | --- |
| memchr 2.8.3 | optional `log`, used through `logging` | CPU/searcher algorithm selection and fixed messages, no haystack/needle/SQL/packet values |
| mio 1.2.5 | optional `log` | poll registration token/interest, deregistration, platform poll fd state/errors; no network address or packet/auth content |
| tokio-util 0.7.20 | optional `tracing` | framed codec state transitions and remaining byte counts; no buffer contents |
| tokio 1.54.1 | optional `tracing`, `tokio_unstable` | runtime task/resource metadata, source locations, readiness and counters; no network endpoints or application payload |

None of these optional logging features is enabled in the probe's normal
graph. Their source sites were also inspected rather than treating a disabled
feature as permanent: downstream feature unification may enable them. These
runtime diagnostics are not guaranteed silent; they do not contain the protected
SQL/error/authentication/endpoint/packet values. The driver and native-tls,
which do see those values, have no active tracing/log dependency at all.
`cargo tree -e normal -i tracing` and `-i log` print no reverse dependencies.
The inventory pins the complete lockfile, so updates require renewed review.

`tokio-native-tls` has no diagnostics and is forked solely to propagate the local
native-tls path without a consumer-specific `[patch]`. Linux native-tls uses
OpenSSL, not rustls. The four OpenSSL certificate-loading log sites were removed
as well: root file/directory errors could otherwise disclose configuration
paths. openssl/openssl-sys, openssl-probe, socket2, dns-lookup, hostname,
x509-parser and other normal graph packages do not depend on either Rust
logging facade. OS resolver/TLS diagnostics, external callbacks deliberately
installed by an application, panics, and application logging of returned values
are outside this Rust event/span/log boundary. No TLS key-log callback is
installed by the provider. No SQLx dependency is present.

## Proof and limits

`tests/vendor_guard.rs` parses all forked Rust sources across platform/cfg
boundaries and checks exact source/manifest/build-script and lock hashes. Its
mutation controls prove it detects diagnostic macros, instrument attributes,
spans and aliases. Hash checking also rejects indirect emission mechanisms,
new modules, or modified empty `quiet!` expansion until explicitly re-reviewed.
It is a regression boundary for this reviewed release, not a general proof that
arbitrary Rust cannot emit a log.

`tests/quiet_driver.rs` directly exercises the raw TDS client without any
dispatcher suppression: SQL success, SQL/server-error failure, outgoing packet
serialization, failed prelogin, authentication configuration and socket cleanup.
It also proves discarded arguments cannot execute.

`tests/live.rs` exercises the pinned SQL Server image with a process-global TRACE
subscriber (including span lifecycle/fields) **and** an independent global
`log` logger with `tracing/log-always`. Positive controls assert that application
events reach both channels, application spans/fields reach tracing, and a
concurrent application task remains visible. The subscriber is installed after
container startup, before the first driver operation: container-management
tooling may log its environment and is not the provider under test.

The live capture covers required-TLS/self-signed rejection, failed auth,
successful login/query/row packets, SQL parameters, server error, unknown
outcome, dropped successful clients, cancelled query, failed connection
cleanup, pool leases/transactions, and the migration ledger. Failed/cancelled
session disappearance is observed from a separate server session. Positive
controls are repeated after the driver has run. No logger, level, or subscriber
is changed by production code.

The inventory was also tested with an actual extra, unreferenced Rust source
file containing a `tracing::trace!` invocation in the vendor directory. The
provider still compiled; the inventory test failed with “unreviewed additions
or removals.” The file was then removed without resealing the inventory.

The fixture deliberately uses unverified self-signed TLS only through private
`cfg(test)` constructors in the library unit-test harness. The production
package has no insecure TLS feature or constructor. Valid production
certificate success, other OS native
TLS implementations, integrated authentication, server routing and reconnect
are not live-tested here. Their TDS source emission sites are compiled out,
but only the Linux transport/dependency path has been exercised. Do not remove
the non-Linux gate on the strength of this Linux run.
