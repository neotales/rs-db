# libsql remote transport fork

This is derived from the `libsql` 0.10.0 crates.io package, from
<https://github.com/tursodatabase/libsql> commit
`d8bc862d20bedc72945a1da27c8b7045414c0261`, path `libsql`. The upstream
MIT license is in `LICENSE.md`; `.cargo_vcs_info.json` records the package
provenance. The original package manifest declares the same license.

Only the remote HTTP feature is supported. This fork replaces Hyper 0.14 and
its rustls connector with Hyper 1 and rustls 0.23, keeps streaming response
bodies, and rejects requests to any origin other than the original endpoint.
The latter applies to Hrana `base_url` changes as well as all later requests;
HTTP redirects are not followed. Cross-origin Turso routing is unsupported.
The loopback-only plaintext exception is enforced by both the fork and the
consuming provider.
TLS uses rustls' ring provider explicitly for each connector, with native trust
roots and safe default protocol versions. It neither installs nor depends on a
process-wide crypto provider. This matters when another dependency enables
aws-lc alongside ring: rustls cannot infer a default from that combined graph.
The pinned rustls dependency enables ring for production; the test dependency
also enables aws-lc so the TLS regressions cannot accidentally test only a
single-provider graph. The transport and native-root tests cover successful
verified TLS, rejection of an unrelated CA and a mismatched hostname, and
failure on an empty native trust store.
Local databases, replication, sync and WASM are not supported by this fork.
Tracing omits endpoint, SQL, arguments, credentials and batons, including
best-effort cleanup, and non-success HTTP responses are dropped without reading
or exposing their bodies.
