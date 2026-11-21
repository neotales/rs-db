# neotales-klickhouse

Fork of `klickhouse` 0.16.0, used as the native ClickHouse transport for `neotales-dbapi-clickhouse`.

By [x0craft](https://github.com/x0craft).

Upstream by Protryon and the klickhouse contributors. This fork is maintained by [x0craft](https://github.com/x0craft) for Neotales.

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

This is a prerelease fork with a distinct package and library name. Pin it exactly:

```sh
cargo add neotales-klickhouse@=0.0.0-alpha.0
```

## What you can do 🔧

**Use it through the supported provider.** Applications normally depend on
`neotales-dbapi-clickhouse`, which owns the DBAPI traits, TLS policy, version gate, and sanitized
errors. This crate is its native transport.

```rust,no_run
use neotales_dbapi::{Pool, QueryOptions, RowLimit};
use neotales_dbapi_clickhouse::{ClickHouseConfig, ClickHousePoolProvider};

async fn query() -> Result<(), neotales_dbapi::DbError> {
    let provider = ClickHousePoolProvider::connect(
        ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
            .password("from-your-secret-store")
            .ca_file("/etc/ssl/private-ca.pem"),
    )
    .await?;
    let rows = provider
        .fetch_all_async(
            "SELECT number FROM numbers(3)",
            QueryOptions::new().limit(RowLimit::new(10, 4096)?),
        )
        .await?;
    assert_eq!(rows.len(), 3);
    Ok(())
}
```

**Use the upstream API directly only if you own the transport contract.** The fork's local delta
is described in `NEOTALES-PROTOTYPE.md`: explicit `EndOfStream`, stricter block-stream closure,
propagated read errors, query-bound insert blocks, connection poisoning after an abandoned insert,
and the validated parameter path used by the provider. The upstream project's
[basic example](https://github.com/Protryon/klickhouse/blob/master/klickhouse/examples/basic.rs)
still shows the raw client shape.

The fork keeps the upstream `MIT OR Apache-2.0` licence. Upstream is not responsible for the local
changes.

## Example Usage
## Example Usage

See [example usage](https://github.com/Protryon/klickhouse/blob/master/klickhouse/examples/basic.rs).

## Unsupported Features

- Clickhouse `Enum8` and `Enum16` types -- use `LowCardinality` instead.

## Running the tests

A Clickhouse server is required to run the integration tests. One can be started easily in a Docker container:

```sh
docker run  --rm --name clickhouse -p 19000:9000 --ulimit nofile=262144:262144 clickhouse
export KLICKHOUSE_TEST_ADDR=127.0.0.1:19000
# export KLICKHOUSE_TEST_USER=default
# export KLICKHOUSE_TEST_PASSWORD=default
# export KLICKHOUSE_TEST_DATABASE=default
cargo nextest run
```

(running the tests simultaneously with `cargo test` is currently not suported, due to loggers initializations.)

## Feature flags

- `derive`: Enable [klickhouse_derive], providing a derive macro for the [Row] trait. Default.
- `compression`: `lz4` compression for client/server communication. Default.
- `serde`: Derivation of [serde::Serialize] and [serde::Deserialize] on various objects, and JSON support. Default.
- `tls`: TLS support via [tokio-rustls](https://crates.io/crates/tokio-rustls).
- `refinery`: Migrations via [refinery](https://crates.io/crates/refinery).
- `geo-types`: Conversion of geo types to/from the [geo-types](https://crates.io/crates/geo-types) crate.
- `bb8`: Enables a `ConnectionManager` managed by bb8

## Credit

`klickhouse_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at lowest time-cost. In a prototype, `serde` was directly used, but this was abandoned due to lock-in of `serde`'s data model.

## Licence 📜
MIT OR Apache-2.0, retained from upstream. See [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
