# Upstream conformance lane for the quiet PostgreSQL fork

This directory runs the **unmodified upstream** `tokio-postgres` 0.7.18 test
suite against the `neotales-tokio-postgres` fork through Cargo's dependency
rename. It is the evidence the parent [`FORK.md`](../FORK.md) asks for before a
release, and it is deliberately narrower than "full conformance".

- The default, no-default and all-features configurations are available; CI runs
  only default. A green result covers one PostgreSQL version and platform, not
  every integration or the fork-specific completion/error APIs.
- Vendored test files remain byte-identical. The ignored upstream Unix socket
  test remains ignored; a separate fixture test exercises a disposable socket.

## Pinned upstream source

| Item | Value |
| --- | --- |
| Crate | `tokio-postgres` 0.7.18 from crates.io |
| Crate archive SHA-256 | `a528f7d280f6d5b9cd149635c8705b0dd049754bc67d81d31fa25169a93809d3` |
| Source commit (`.cargo_vcs_info.json`) | `f1cb6ec0d5766b136cbd68f3010d64142a5daa66` |
| Path in upstream VCS | `tokio-postgres` |
| License | MIT OR Apache-2.0, © Steven Fackler and contributors |

`tests/test/` is copied byte-for-byte from the cached crate archive. The
`upstream.sha256` manifest is checked by `run.sh` before a server starts, so a
modified or partially copied test tree fails the lane. `run.sh` also lists the
vendored `upstream` target before starting the server, so a missing or empty
target cannot pass as the smaller `fork_identity` test. `PROVENANCE.md` records
what was copied and what was not.

The archive checksum matches the stock `tokio-postgres` entry in
`rs/lib/Cargo.lock`, which is an independent pin of the same release.

## Run

```sh
# from anywhere in the checkout
tokio-postgres/conformance/run.sh
```

Requirements: Linux, bash, Docker, OpenSSL and GNU `sha256sum`. The upstream
suite hardcodes `127.0.0.1:5433`, so that port must be free; the script refuses
to run when it is occupied. It creates a disposable PostgreSQL 17 container,
runs the tests, and removes the container and its temporary certificates on
exit, interrupt or failure. Cleanup uses the container ID recorded by this
invocation after checking its per-invocation label; it never removes a
container by name, so a name collision cannot delete another run's container.
If Docker creates the container but fails to write `--cidfile`, the label finds
and removes only that container. No real credentials are read or written.

Extra arguments are passed to `cargo test`, for example:

```sh
# upstream all-features (including all optional type tests)
tokio-postgres/conformance/run.sh --all-features

# upstream no-default-features
tokio-postgres/conformance/run.sh --no-default-features

# optional upstream array feature (adds types::test_array_array_params)
tokio-postgres/conformance/run.sh --features array-impls

# one test while debugging; the fixture is still created and removed
tokio-postgres/conformance/run.sh --test upstream -- types::inet
```

Test sources are in-repo, so obtaining them needs no network. Cargo still
resolves the ordinary crate dependencies listed in `Cargo.lock`; use
`CARGO_NET_OFFLINE=true` when the cache already contains them.

## Fixture

- `postgres:17` pinned by manifest digest
  `sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07`
  (PostgreSQL 17.12 at the time of the run).
- Port `5433` bound to `127.0.0.1` only. The server also enables SSL with a
  short-lived, locally generated CA and `localhost` server certificate; nothing
  trusts or verifies that certificate in this lane.
- Synthetic roles adapted from upstream `docker/sql_setup.sh`: `pass_user`,
  `md5_user`, `scram_user` (password `password`), passwordless `ssl_user`, and
  `postgres` via trust on loopback. `hstore`, `citext` and `ltree` are created.
- The password values are fixed fixture strings for a throwaway loopback server.
  They are not credentials, are not used outside the container, and must not be
  reused anywhere.

## Observed results

2026-11-14, rustc/cargo 1.98.1, Docker 29.9.2, PostgreSQL 17.12:

| Command | Upstream target | Fork identity | Separate socket |
| --- | --- | --- | --- |
| `run.sh` | 108 passed, 1 ignored | 1 passed | 1 passed |
| `run.sh --no-default-features` | 96 passed | 1 passed | 0 compiled (runtime disabled) |
| `run.sh --all-features` | 168 passed, 1 ignored | 1 passed | 1 passed |

All commands exited 0. All-features includes the 59 optional type tests and
one array test. It also enables feature flags with no matching vendored tests;
passing is not evidence for those integrations. Individual `--features` flags
can be selected through Cargo. The fork dependency has default features disabled
in the harness, so the no-default run genuinely omits its runtime feature.

The fixture intentionally differs from upstream SQL: it sets
`password_encryption = 'md5'` before creating `md5_user`, then switches to SCRAM
for `scram_user`. The server classifies the verifier and returns only a fixed
`md5` or `other` label; no verifier body is read or printed. With the `md5` HBA
rule this makes upstream's three `md5_password_*` tests exercise genuine MD5
challenge authentication. These are synthetic disposable credentials only.

The socket test uses a per-invocation mounted directory and the same server,
without changing the upstream test that assumes `/var/run/postgresql` on the
host. The original test remains ignored, including in all-features.

## Remaining limitations and release blockers

- **TLS channel binding:** no upstream test uses a TLS connector, so this lane
  does not exercise SCRAM channel binding. The separate verified TLS adapter
  implements `tls-server-end-point` and is covered by `../../tls/tests/live.sh`,
  including `SCRAM-SHA-256-PLUS` under `require`, `prefer`/`disable`, wrong
  bindings and the unsupported-digest fallback. Plaintext upstream channel
  binding rejection is tested here.
- No upstream test uses a TLS connector. PostgreSQL 17 on Linux is the only
  observed platform/version; benchmarks and other upstream CI platforms are
  not run. CI runs only default features.
- Upstream tests do not cover the fork's completion tags, transaction status,
  cache clearing and safe error classification; those need their own reviewed
  tests. Passing upstream tests is not a production-safety claim.

## CI

The `dbapi` pipeline runs this lane on `ubuntu-24.04`:

- Monorepo: `.github/workflows/rs-dbapi.yml` invokes
  `tokio-postgres/conformance/run.sh`.
- Satellite (`neotales/rs-lib`): `.github/workflows/dbapi.yml` invokes
  `tokio-postgres/conformance/run.sh`.

Both jobs are Linux-only because the lane needs Docker. They pin every action
by full commit SHA, use no repository secrets or credentials, and have a
20-minute timeout that includes image and build startup. The pipeline's path
filter covers `dbapi-postgres/**` (and the same tree under the
satellite root), so a driver or fixture change runs this lane.

The job invokes `run.sh` with no arguments, so only the default feature lane
is continuously exercised. The other configurations above are manual evidence,
not CI coverage. A green run is not general compatibility evidence.

## Updating upstream

1. Import the new crate release and record its archive hash and VCS commit.
2. Replace `tests/test/` from the cached archive without editing the files.
3. Regenerate `upstream.sha256`, update `PROVENANCE.md` and this file, then rerun
   the lane.
4. Re-run the parent fork diff review and the fork's live tests. Do not assume
   the existing patch applies.
