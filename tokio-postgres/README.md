# neotales-tokio-postgres

`neotales-tokio-postgres` **0.0.0-alpha.0** is an experimental Neotales
fork of the crates.io `tokio-postgres` **0.7.18** release by Steven Fackler and
the rust-postgres contributors. Neotales maintains this fork; upstream is not
responsible for its changes.

This package is not upstream `tokio-postgres`. It has a distinct package and
library name, so stock `tokio-postgres` can be used alongside it, and consumers
must pin this fork's exact prerelease version. See [FORK.md](FORK.md) for the
patch list, compatibility limits, upstream conformance lane, maintenance
strategy and ownership.

The fork's MIT and Apache-2.0 license texts are included unchanged as
`LICENSE-MIT` and `LICENSE-APACHE`. The parent `neotales-dbapi-postgres`
provider, its TLS adapter and the upstream conformance harness live in the
`rs/lib` source tree under `dbapi-postgres/`; they are separate packages
and are not bundled here.
