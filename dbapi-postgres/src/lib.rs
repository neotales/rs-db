//! Quiet PostgreSQL implementation of the local `neotales-dbapi` traits.
//!
//! `PostgresPool` uses the reviewed quiet fork, not `SQLx`. TLS is required and
//! hostname/chain verified against explicit roots. There is no insecure fallback.
//! The separately named plaintext constructor accepts numeric loopback hosts only.
//! The old URL-only `connect` constructor has been removed rather than silently
//! inventing trust/deadline policy or preserving its server-notice logging leak.
//!
//! Typed `$n` bindings, finite floats/nulls and detached bounded rows share the
//! compiled transport implementation. Row byte caps count names plus binary
//! payload, not allocation overhead or wire buffers. Any statement/fetch may
//! write: unconfirmed dispatched work is `WriteOutcomeUnknown`; unconfirmed COMMIT
//! is `CommitOutcomeUnknown`. No operation is retried.
//!
//! Session drop starts explicitly owned, deadline-bounded reset work when healthy.
//! `PostgresPool::wait_for_cleanup` reports reset failures; failed/uncertain or
//! dropped transactions discard and invalidate their Session. No detached rollback
//! is started. Successful commit/rollback restores the same Session connection.
//! Idle TTL is checked lazily on checkout, not by a background socket reaper.
//! Capacity bounds client data socket owners, never server work after disconnect.
//! Acquire/operations and Session-drop reuse require an active Tokio runtime
//! with I/O and timers enabled.
//!
//! `QueryOptions::timeout` selects a positive whole-client budget no greater
//! than the profile ceiling. Inheritance uses that finite ceiling; explicitly disabling
//! it is unsupported. It covers preparation,
//! dispatch and completion; it does not change the server's per-statement ceiling.
//! All SQL is trusted: do not manage transactions/characteristics or override
//! timeout policy in statements/scripts. This is not an untrusted-SQL sandbox.
//!
//! The public traits consume owned `QueryOptions` and can be wrapped with `PolicyPool`.
//!
//! ```no_run
//! use std::time::Duration;
//! use neotales_dbapi::{Pool, QueryOptions, RowLimit};
//! use neotales_dbapi_postgres::{Config, PoolOptions, PostgresPool, Profile, VerifiedTls};
//! # async fn example(root_der: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = Config::new();
//! config.host("db.example.com").user("app").dbname("app");
//! let policy = PoolOptions::new(4,
//!     Profile::normal(Duration::from_secs(5), Duration::from_secs(1))?,
//!     Duration::from_secs(60))?;
//! let pool = PostgresPool::new(config, VerifiedTls::new(&[root_der])?, policy)?;
//! let mut session = pool.acquire_async().await?;
//! let row = session.fetch_one_async("SELECT 42::bigint AS answer",
//!     QueryOptions::new().limit(RowLimit::new(1, 14)?)).await?;
//! assert_eq!(row.get::<i64>("answer")?, 42);
//! drop(session);
//! pool.wait_for_cleanup().await?;
//! pool.close();
//! # Ok(()) }
//! ```

mod adapter;
pub mod experimental;

pub use adapter::{PoolOptions, PostgresPool};
pub use experimental::{Config, Profile, VerifiedTls};
