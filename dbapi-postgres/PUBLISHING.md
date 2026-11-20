# Publishing the PostgreSQL dbapi crates

This file was the release plan in the `rs/lib` monorepo. Release management for
this repository now lives in `../RELEASE.md`, which lists all 23 publishable
crates in dependency order.

The PostgreSQL chain in this repository is:

| Order | Package | Directory | Internal dependencies |
| ---: | --- | --- | --- |
| 1 | `neotales-tokio-postgres` | `../tokio-postgres/` | none (fork of `tokio-postgres` 0.7.18) |
| 2 | `neotales-dbapi` | `../dbapi/` | `neotales-resilience` from the registry |
| 3 | `neotales-dbapi-postgres-tls` | `../dbapi-postgres-tls/` | `neotales-tokio-postgres` |
| 4 | `neotales-dbapi-postgres` | `./` | `neotales-dbapi`, `neotales-tokio-postgres`, `neotales-dbapi-postgres-tls` |

Every crate in this repository starts at `0.0.0-alpha.0` and carries the same
version. Internal path dependencies carry an exact version requirement. The
conformance harness is `../tokio-postgres/conformance/`, and the verified TLS
live lane is `../dbapi-postgres-tls/tests/live.sh`.

The first upload of each crate is manual because crates.io cannot bootstrap a
trusted publisher for a crate that does not exist yet. See `../RELEASE.md`.

`PUBLISHING.md` is excluded from the provider package; the packaged crate still
carries `README.md` and `LICENSE.md`.
