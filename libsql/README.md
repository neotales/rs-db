# neotales-libsql

Remote-only fork of libSQL 0.10.0, used by `neotales-dbapi-libsql`.

By [x0craft](https://github.com/x0craft).

Upstream by the libSQL authors. This remote-only fork is maintained by [x0craft](https://github.com/x0craft) for Neotales.

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

This is a prerelease fork with a distinct package name. Pin it exactly:

```sh
cargo add neotales-libsql@=0.0.0-alpha.0
```

## What you can do 🔧

**Speak Hrana over HTTPS.** The fork keeps only the remote Hrana HTTP client used by
`neotales-dbapi-libsql`. The default features are `remote` and `tls`.

```rust,no_run
use neotales_dbapi::{Pool, QueryOptions, RowLimit};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = LibsqlPoolProvider::connect(
        LibsqlConfig::remote("libsql://example.turso.io", "from-your-secret-store"),
    )
    .await?;
    let rows = provider
        .fetch_all_async(
            "SELECT 1 AS answer",
            QueryOptions::new().limit(RowLimit::new(1, 64)?),
        )
        .await?;
    assert_eq!(rows.len(), 1);
    Ok(())
}
```

**Reject a token leak to another origin.** The fork rejects cross-origin Hrana `base_url` changes
and does not follow HTTP redirects, so a bearer token and stream baton cannot be forwarded to
another origin.

**Use a local SQLite file elsewhere.** This fork does not open a local file, run the in-process
engine, create an embedded replica, or synchronize frames. Use `neotales-dbapi-sqlite` for that.
Those modes have different durability and cancellation behaviour and are not claimed here.

The fork retains the upstream MIT licence. Upstream is not responsible for the local changes.
Keep `Hrana`/`libSQL` types behind the provider API; the provider is the supported surface.

## Provenance

See [PROVENANCE.md](PROVENANCE.md) for the exact upstream release and the local delta, and
[LICENSE.md](LICENSE.md) for the retained licence text.

## Licence 📜
MIT. See [LICENSE.md](LICENSE.md).
