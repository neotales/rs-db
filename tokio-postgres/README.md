# neotales-tokio-postgres

Experimental fork of `tokio-postgres` 0.7.18, used by the quiet PostgreSQL provider and its TLS
adapter.

By [x0craft](https://github.com/x0craft).

Upstream by Steven Fackler and the rust-postgres contributors. This fork is maintained by [x0craft](https://github.com/x0craft) for Neotales; upstream is not responsible for its changes.

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

The package and library names differ from upstream, so stock `tokio-postgres` can be used
alongside it. Pin the exact prerelease:

```sh
cargo add neotales-tokio-postgres@=0.0.0-alpha.0
```

## What you can do 🔧

**Use the driver through the supported provider.** `neotales-dbapi-postgres` owns the DBAPI
traits, verified-TLS policy, bounded rows, deadlines, and reset lifecycle. The fork supplies the
compiled transport underneath it.

```rust,no_run
use std::time::Duration;
use neotales_dbapi::{Pool, QueryOptions, RowLimit};
use neotales_dbapi_postgres::{Config, PoolOptions, PostgresPool, Profile, VerifiedTls};

async fn query(root_der: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("db.example.com").user("app").dbname("app");
    let policy = PoolOptions::new(
        4,
        Profile::normal(Duration::from_secs(5), Duration::from_secs(1))?,
        Duration::from_secs(60),
    )?;
    let pool = PostgresPool::new(config, VerifiedTls::new(&[root_der])?, policy)?;
    let mut session = pool.acquire_async().await?;
    let row = session
        .fetch_one_async(
            "SELECT 42::bigint AS answer",
            QueryOptions::new().limit(RowLimit::new(1, 14)?),
        )
        .await?;
    assert_eq!(row.get::<i64>("answer")?, 42);
    Ok(())
}
```

**Do not call the raw fork for production traffic.** Its client errors can contain server-provided
strings. The provider and TLS adapter sanitize outcomes and enforce verified TLS; the raw
`Config::connect` path with a weak `sslmode` bypasses that policy.

The fork's patch list, compatibility limits, upstream conformance lane, maintenance strategy, and
ownership are in [FORK.md](FORK.md). The MIT and Apache-2.0 texts are included unchanged as
`LICENSE-MIT` and `LICENSE-APACHE`.

The parent `neotales-dbapi-postgres` provider, its TLS adapter, and the upstream conformance
harness live in the `rs/lib` source tree under `dbapi-postgres/`; they are separate packages and
are not bundled here.

## Licence 📜
MIT OR Apache-2.0, retained from upstream. See [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
