# neotales-dbapi-d1

Experimental Cloudflare D1 adapter for `neotales-dbapi` local (`!Send`) futures.

By [x0craft](https://github.com/x0craft).

[![Neotales](https://github.com/neotales/brand/raw/main/assets/logo.png)](https://github.com/neotales/rs-db)

<!-- badges: crates.io, docs.rs, CI. Add once the crate name and first version exist. -->

## Install 📦

```sh
cargo add neotales-dbapi-d1 neotales-dbapi
```

The D1 binding is `wasm32` only. A host target builds the value, placeholder,
and paging helpers so shared code stays testable, but `D1Pool` exists only under
`cfg(target_arch = "wasm32")`.

## What you can do 🔧

**Wrap a Worker binding.** `D1Pool::from_env` reads the named binding from the
Workers environment. The pool is `Clone`, and every clone shares the same
binding.

```rust,no_run
use neotales_dbapi::DbError;
use neotales_dbapi_d1::D1Pool;
use worker::Env;

fn pool(env: &Env) -> Result<D1Pool, DbError> {
    D1Pool::from_env(env, "DB")
}
```

**Dispatch one write with an explicitly uncertain outcome.** There is no DBAPI
session and no isolation. Dropping the future or receiving
`WriteOutcomeUnknown` is not proof that the statement did not execute.

```rust,no_run
use neotales_dbapi::{DbError, DbType, Parameter, Value};
use neotales_dbapi_d1::D1Pool;

async fn insert_event(pool: &D1Pool, seq: i64, body: &str) -> Result<u64, DbError> {
    let parameters = [
        Parameter::new(DbType::I64, Value::I64(seq))?,
        Parameter::new(DbType::Text, Value::Text(body.to_owned()))?,
    ];
    pool.execute_uncertain_async(
        "INSERT INTO events (seq, body) VALUES (?, ?)",
        &parameters,
    )
    .await
}
```

**Run several statements as one uncertain batch.** Cloudflare documents a D1
batch as atomic, but it is not an interactive transaction: there is no begin,
commit, or rollback handle, and `Capabilities::transactions` remains `false`.
Results are in statement order and carry each statement's reported change count.

```rust,no_run
use neotales_dbapi::{DbError, DbType, Parameter, Value};
use neotales_dbapi_d1::{D1BatchStatement, D1Pool};

async fn insert_pair(pool: &D1Pool) -> Result<Vec<u64>, DbError> {
    let statements = vec![
        D1BatchStatement::new(
            "INSERT INTO events (seq) VALUES (?)",
            vec![Parameter::new(DbType::I64, Value::I64(1))?],
        )?,
        D1BatchStatement::new(
            "INSERT INTO events (seq) VALUES (?)",
            vec![Parameter::new(DbType::I64, Value::I64(2))?],
        )?,
    ];
    pool.batch_uncertain_async(statements).await
}
```

**Page with an explicit requery contract.** `D1Pager` re-executes one
caller-supplied statement with caller-supplied bindings. Row, page, and byte
budgets are enforced after D1 materializes a page, before publication. They are
publication bounds, not pre-materialization allocation limits.

## Boundaries

- **DBAPI operations fail closed.** `local::Pool`, `local::Session`, and every
  trait operation return `Unsupported` before dispatch. Cancellation and
  uncertain-outcome isolation are not established, so acquisition cannot hand
  out a safe session.
- **Uncertain writes are not DBAPI.** `execute_uncertain_async` and
  `batch_uncertain_async` are separate, explicitly named methods. Dropping a
  dispatched future does not cancel its promise. The caller owns coordination,
  reconciliation, and retry decisions.
- **Numeric types are approximate at the binding.** Integers outside
  JavaScript's safe range and integral floating-point parameters (including
  signed zero) are rejected. D1 may store an integer parameter as SQLite `REAL`
  in a column without affinity, so the uncertain API does not promise an exact
  SQL numeric storage class. `numeric_to_f64` converts a value obtained
  separately but cannot recover type or precision already lost.
- **No hosted-behavior claim.** A local Miniflare diagnostic does not establish
  Cloudflare-hosted behavior. The emulator is a detached workspace and is
  excluded from the published package.

## Compile check

The crate has no host-side D1 dispatcher, so a normal workspace build works. The
wasm target proves the binding path:

```sh
rustup target add wasm32-unknown-unknown
cargo check --target wasm32-unknown-unknown -p neotales-dbapi-d1
```

No D1 database, credential, or live request is needed for that check. The host
and wasm tests are fixture-free. The separate Miniflare thread needs an
unprivileged user namespace and is reported as blocked, not passed, on runners
that deny the UID mapping.

## Licence 📜

MIT. See [LICENSE.md](LICENSE.md).
