# Provenance of the vendored upstream tests

## Upstream source

- Project: [`rust-postgres`](https://github.com/rust-postgres/rust-postgres)
- Crate: `tokio-postgres` 0.7.18, downloaded from crates.io and cached at
  `$CARGO_HOME/registry/src/*/tokio-postgres-0.7.18/`
- Crate archive SHA-256:
  `a528f7d280f6d5b9cd149635c8705b0dd049754bc67d81d31fa25169a93809d3`
  (matches `checksum` for `tokio-postgres` 0.7.18 in `rs/lib/Cargo.lock`)
- `Cargo.toml.orig` VCS information: commit
  `f1cb6ec0d5766b136cbd68f3010d64142a5daa66`, path `tokio-postgres`
- License: MIT OR Apache-2.0. Both upstream texts are copied unchanged as
  `LICENSE-MIT` and `LICENSE-APACHE` in this directory. Upstream authors are
  credited in the fork manifest (`Steven Fackler <sfackler@gmail.com>`) and the
  crate metadata.

## Files copied

Copied verbatim, with no edits, from the crate archive's `tests/test/` tree:

```text
tests/test/binary_copy.rs
tests/test/main.rs
tests/test/parse.rs
tests/test/runtime.rs
tests/test/types/*.rs          (all 17 files, including feature-gated modules)
```

`upstream.sha256` records the SHA-256 of every vendored file and is verified by
`run.sh`. To reproduce the manifest from the crate archive:

```sh
crate_dir=$CARGO_HOME/registry/src/*/tokio-postgres-0.7.18
cp -a "$crate_dir"/tests/test/. tests/test/
find tests/test -type f -print0 | sort -z | xargs -0 sha256sum > upstream.sha256
```

Not copied: upstream `Cargo.toml`, benches, examples and other crates from the
repository. The conformance package supplies its own manifest and dev
dependencies because the fork's manifest intentionally omits upstream test and
bench declarations.

## Changes made by Neotales

None to the vendored test sources. `Cargo.toml` makes the fork a renamed
dev-dependency so the tests import it as `tokio_postgres`; that is what lets the
files stay unchanged. `tests/fork_identity.rs`, `tests/socket.rs`, `run.sh`,
`fixture/`, `README.md` and this file are Neotales additions, not upstream
material.

## Fixture provenance

`fixture/pg_hba.conf` and the SQL role/extension block in `fixture/roles.sql`
are adapted from upstream `docker/sql_setup.sh` at the commit above. That file's
SHA-256 is:

```text
d974956ae86530da96c1cba287ad71fa74a5e44bc4a8acccbe2cd6e1f036453e
```

Differences from upstream's file are deliberate and limited to the disposable
fixture: the committed private key and self-signed certificate are replaced by
certificates generated per run, and the setup is split into `pg_hba.conf` plus
an initdb SQL script instead of writing into `$PGDATA`. Unlike upstream,
`roles.sql` explicitly sets `password_encryption` to `md5` before creating
`md5_user`, then to `scram-sha-256` before creating `scram_user`, so PostgreSQL
17 exercises a genuine MD5 challenge. The role names, auth methods and
extension list are unchanged.
