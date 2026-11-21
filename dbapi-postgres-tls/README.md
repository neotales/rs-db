# neotales-dbapi-postgres-tls

Verified TLS adapter for the isolated `neotales-tokio-postgres` fork.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi-postgres-tls neotales-tokio-postgres
```

## What you can do 🔧

**Connect with verify-full TLS and SCRAM channel binding.** The wrapper always requests TLS and
checks the host name and certificate chain against explicit DER trust anchors, even when the
supplied configuration asks for a weaker mode.

```rust,no_run
use std::{net::Ipv4Addr, time::Duration};
use neotales_dbapi_postgres_tls::{connect, VerifiedTls};
use neotales_tokio_postgres::Config;

async fn connect_once(root_der: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let tls = VerifiedTls::new(&[root_der])?;
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr(Ipv4Addr::LOCALHOST.into())
        .user("app")
        .dbname("app");
    // Bound DNS/TCP, SSL negotiation, TLS, and startup as one deadline.
    let (client, connection, cancel) =
        tokio::time::timeout(Duration::from_secs(5), connect(&config, tls)).await??;
    let worker = tokio::spawn(connection);
    let _ = client;
    drop(cancel);
    worker.abort();
    let _ = worker.await;
    Ok(())
}
```

**Understand the binding limit.** `SCRAM-SHA-256-PLUS` needs a certificate signature digest.
Ed25519 has none; RSA-PSS does not expose the digest safely. Those certificates work with
`prefer` as plain SCRAM and fail `require` explicitly.

**Do not bypass the wrapper.** Calling the fork's `Config::connect` directly with a weak
`sslmode` skips the verification and channel-binding policy this crate exists to enforce.


## Scope

This crate supplies the public quiet provider's OpenSSL-based adapter for the
isolated `neotales-tokio-postgres` fork. The old SQLx provider is removed. The
`connect` wrapper always requests TLS and checks both the hostname and the
certificate chain against the explicit DER trust anchors supplied by the caller.
It requires TLS 1.2 or newer. Its behavior is equivalent to a strict
verify-full policy, including when the supplied config says `disable` or
`prefer`; it is stricter than libpq's ordinary `require`. The fork does not
parse `verify-full` URLs. Do not bypass the wrapper by calling the fork's
`Config::connect` directly with a weak `sslmode`.
Only PostgreSQL's SSLRequest negotiation is supported; direct TLS negotiation is
rejected before opening a connection.

## SCRAM channel binding

The adapter implements RFC 5929 `tls-server-end-point` channel binding for
`SCRAM-SHA-256-PLUS`. The binding is the hash of the DER certificate that just
passed chain and hostname verification; no weaker verification path is used to
obtain it. The hash algorithm is the certificate's signature digest, except MD5
and SHA-1, which RFC 5929 section 4.1 replaces with SHA-256.

`channel_binding=prefer` (the driver default) uses `SCRAM-SHA-256-PLUS` only
when the server offers it and a usable binding can be computed from the verified
certificate; otherwise it falls back to `SCRAM-SHA-256`. `require` fails the
connection unless that mechanism is used; `disable` sends `SCRAM-SHA-256`
without binding data. The adapter's `connect` enforces `require` after the TLS
handshake. If the peer certificate has no usable signature digest, it returns
`ErrorKind::Unsupported` rather than claiming a binding.

Two different cases cannot produce this binding. Ed25519 has no certificate
signature digest at all: it signs the message in one step and OpenSSL reports a
null digest. RSA-PSS does have a digest, but it is selected by the signature
parameters rather than fixed by the signature algorithm; the safe `openssl`
crate does not expose `X509_get_signature_info`, so those parameters cannot be
read and guessing the hash would be wrong. A server with either certificate
works with `prefer` (plain `SCRAM-SHA-256`) only because no binding is
available, and fails `require` explicitly. Signature digests SHA-256, SHA-384,
SHA-512 and SHA-224 map directly; SHA-1 and MD5 map to SHA-256 as the RFC
requires. OpenSSL's own security level still decides whether a weak certificate
signature is acceptable for the TLS handshake at all.

The live harness starts a second PostgreSQL 17 server with a synthetic Ed25519
certificate to exercise both the explicit `require` failure and the `prefer`
fallback against a real server. It also proves the positive case: with an RSA
SHA-256 certificate, a `require` connection succeeds, which is only possible
when the driver selected `SCRAM-SHA-256-PLUS` and PostgreSQL accepted the
certificate binding. Tests that deliberately report a wrong binding use the
adapter's real verified handshake and confirm PostgreSQL rejects it with
SQLSTATE 28000 and `SCRAM channel binding check failed`.

`VerifiedCancel`, returned by `connect`, retains the verified connector and a
token from the TLS-required configuration. Its cancellation request opens a new
verified TLS connection; cancellation is racy, and a successful request does
not prove that the server cancelled the query. The owner must poll the returned
connection and manage its lifetime. Dropping a connection-establishment future
closes its in-progress socket; it does not guarantee cancellation of any
already-established server operation.

The adapter has no full handshake deadline. Bound `connect` with a caller-owned
timeout covering the entire future, and separately bound operations and cleanup.
The crate-level documentation contains a complete example.

Errors from this crate expose only coarse categories and have no source chain;
the raw fork client/connection can return
errors containing server-provided strings; callers must sanitize them. This
adapter does not support client certificates. It
does not configure system roots, use a global logger filter, or claim security
for other dependencies or applications. The OpenSSL and tokio-openssl crates
retain their own licenses; system OpenSSL is an external runtime dependency.
The parent crate's compiled transport owns lifecycle, deadlines, reset and
connection-affine transactions, and its public dbapi trait adapter now uses it.
The combined live harness also verifies the provider's public dyn
Pool/Session/Transaction PID-affine TLS, commit, rollback, reset/reuse and quiet
SQL/bind/notice/error handling, plus a `channel_binding=require` pool. Broader
dependency logging/TLS review and fork conformance remain before production use.

## Live test

From `rs/lib`, run
`dbapi-postgres-tls/tests/live.sh`. It requires Docker, OpenSSL, free
`127.0.0.1:55433` and `127.0.0.1:55434`, and a writable scratch directory
(`/tmp/opencode`, or `$TMPDIR` when set). It creates short-lived synthetic test
keys and certificates there and starts two PostgreSQL 17 containers:

- port 55433 has an RSA SHA-256 server certificate, trust for `postgres` and
  SCRAM for the synthetic `plab_scram` role;
- port 55434 has a self-signed Ed25519 server certificate and the same role,
  for the unsupported-digest fallback.

The SCRAM password is generated per run, passed to the tests through the
environment, never printed, and exists only inside those disposable
containers. The harness runs the adapter tests (including
`tests/channel_binding.rs`) and the provider TLS regression in
`dbapi-postgres/tests/tls_live.rs`, then removes both containers and the
files. Cleanup uses the container IDs recorded by this invocation after checking
their per-invocation label; it never removes a container by name, so a name
collision cannot delete another run's container. If Docker creates a container
but fails to write `--cidfile`, the label finds and removes only that container.
No real credentials are involved. Alternatively, run `cargo test -p
neotales-dbapi-postgres-tls` and `cargo test -p neotales-dbapi-postgres
--test tls_live -- --ignored` against compatible TLS test servers with
`DBAPI_TLS_TEST_CA_DER` / `DBAPI_TLS_TEST_OTHER_CA_DER` pointing to the trusted
and an unrelated public DER root, `DBAPI_TLS_TEST_ED25519_CA_DER` pointing to a
self-signed Ed25519 server certificate, and
`DBAPI_TLS_TEST_SCRAM_USER` / `DBAPI_TLS_TEST_SCRAM_PASSWORD` naming a SCRAM
role. The RSA server certificate must be valid for `localhost`; the Ed25519 one
must be a self-signed `localhost` certificate on the second port.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
