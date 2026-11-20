//! Lower-level quiet transport and pool used by the public dbapi trait adapter.
//!
//! A lease is discarded unless `recycle` completes rollback and reset. An operation
//! cancelled after polling leaves the lease permanently poisoned. A driver owns
//! the physical-connection permit until the task actually terminates.
//!
//! This API deliberately exposes the fork's `Config` and `ToSql` for transport
//! experimentation; it is not yet a replacement dbapi provider. [`Lease::execute`]
//! and [`Lease::fetch`] bind dbapi parameters and fetch detached, bounded typed
//! rows. The older fork-parameter [`Lease::query`] diagnostic API supports only
//! non-null `INT4`, `BOOL`, and `TEXT` single-row results, without byte limits.
//! All SQL is trusted:
//! callers must not change `statement_timeout` or override it inside functions.
//! Server ceilings are per statement, not a strict bound on total server work.
//! Socket shutdown and cancellation are not rollback or outcome confirmation.
//! No operation is retried. Errors are sanitized and this module does not log.
//!
//! ```no_run
//! use std::time::Duration;
//! use neotales_dbapi_postgres::experimental::{Config, Pool, Profile, VerifiedTls};
//! # async fn example(root_der: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = Config::new();
//! config.host("db.example.com").user("app").dbname("app");
//! let profile = Profile::normal(Duration::from_secs(5), Duration::from_secs(1))?;
//! let pool = Pool::new(config, VerifiedTls::new(&[root_der])?, 4, profile)?;
//! let mut lease = pool.acquire().await?;
//! let result = lease.batch_deadline("SELECT 1", Duration::from_secs(1),
//!     Duration::from_secs(2)).await;
//! if result.is_ok() { lease.recycle().await?; } else { drop(lease); }
//! pool.close();
//! result?;
//! # Ok(()) }
//! ```

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use neotales_dbapi_postgres_tls::VerifiedCancel;
/// Explicit verified trust store for the default TLS transport.
pub use neotales_dbapi_postgres_tls::VerifiedTls;
use neotales_tokio_postgres as pg;
/// Fork configuration and parameter interoperability for this experimental API.
pub use neotales_tokio_postgres::{Config, types::ToSql};
use tokio::{
    sync::{Notify, Semaphore, watch},
    task::JoinHandle,
};

mod transaction;
mod typed;
pub use transaction::Transaction;

#[derive(Clone)]
enum Transport {
    /// Only for a disposable local server; never selected implicitly.
    LoopbackPlaintextForTests,
    Verified(VerifiedTls),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
/// Sanitized failures; no underlying driver/server error or source is retained.
pub enum Error {
    /// The pool is closed.
    Closed,
    /// Connection establishment failed.
    Connection,
    /// The server rejected an operation, or result decoding failed.
    Operation,
    /// A failed or interrupted operation makes the lease unusable.
    Poisoned,
    /// Invalid capacity, host, or duration policy; no SQL was dispatched.
    InvalidPolicy,
    /// Installing or verifying the server ceiling failed.
    Initializer,
    /// The operation may have taken effect; reconcile before retrying writes.
    /// Includes server errors lacking a nonlocalized severity field.
    OutcomeUnknown,
    /// PostgreSQL reported query cancellation; earlier statements may have completed.
    ServerCancelled,
    /// The client acquisition or reset deadline expired.
    Timeout,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgreSQL experimental pool failed ({self:?})")
    }
}

impl std::error::Error for Error {}

/// Caller-selected finite policy. Durations must be integral milliseconds in
/// `1..=i32::MAX`. The ceiling also bounds each whole acquisition (including
/// capacity wait/startup/initialization) and reset on the client.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    ceiling_ms: u32,
    cancel_grace: Duration,
}

impl Profile {
    pub(crate) fn ceiling(self) -> Duration {
        Duration::from_millis(u64::from(self.ceiling_ms))
    }

    /// Creates a per-statement server ceiling and a separate cancellation grace.
    ///
    /// # Errors
    /// Returns `InvalidPolicy` for zero, fractional-millisecond, or excessive durations.
    pub fn normal(ceiling: Duration, cancel_grace: Duration) -> Result<Self, Error> {
        let ceiling_ms = milliseconds(ceiling)?;
        milliseconds(cancel_grace)?;
        Ok(Self {
            ceiling_ms,
            cancel_grace,
        })
    }

    /// A separately configured profile for migrations. The caller chooses the duration.
    ///
    /// # Errors
    /// Returns `InvalidPolicy` unless durations are valid and the ceiling exceeds `normal`.
    pub fn migration(
        normal: Self,
        ceiling: Duration,
        cancel_grace: Duration,
    ) -> Result<Self, Error> {
        let migration = Self::normal(ceiling, cancel_grace)?;
        if migration.ceiling_ms <= normal.ceiling_ms {
            return Err(Error::InvalidPolicy);
        }
        Ok(migration)
    }
}

fn milliseconds(value: Duration) -> Result<u32, Error> {
    let millis = value.as_millis();
    if millis == 0 || millis > i32::MAX as u128 || !value.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(Error::InvalidPolicy);
    }
    u32::try_from(millis).map_err(|_| Error::InvalidPolicy)
}

enum Canceller {
    Plain(pg::CancelToken),
    Verified(VerifiedCancel),
}

impl Canceller {
    async fn send(&self) {
        match self {
            Self::Plain(token) => {
                let _ = token.cancel_query(pg::NoTls).await;
            }
            Self::Verified(token) => {
                let _ = token.cancel().await;
            }
        }
    }
}

struct Connection {
    client: pg::Client,
    worker: JoinHandle<()>,
    canceller: Canceller,
    idle_since: Instant,
}

type DriverFuture = Pin<Box<dyn Future<Output = Result<(), pg::Error>> + Send>>;

impl Drop for Connection {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

struct State {
    closed: bool,
    idle: Vec<Connection>,
}

struct Shared {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    available: Notify,
    closed: watch::Sender<bool>,
    config: pg::Config,
    transport: Transport,
    profile: Profile,
    idle_ttl: Option<Duration>,
}

#[derive(Clone)]
/// Cloneable bounded pool. Use `close` to reject acquisitions and abort idle
/// workers; outstanding leases remain owned by their callers. Dropping the last
/// pool and its leases also closes all workers. Capacity bounds socket owners,
/// not PostgreSQL backend work, which can outlive socket closure.
pub struct Pool(Arc<Shared>);

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Pool(..)")
    }
}

// Fields are dropped in declaration order: the socket-owning future must go
// away before capacity can be handed to a replacement connection.
struct Driver<F> {
    connection: Pin<Box<F>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<F: Future<Output = Result<(), pg::Error>>> Future for Driver<F> {
    type Output = Result<(), pg::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().connection.as_mut().poll(cx)
    }
}

fn spawn_driver<F>(
    client: pg::Client,
    connection: F,
    permit: tokio::sync::OwnedSemaphorePermit,
    canceller: Canceller,
) -> Connection
where
    F: Future<Output = Result<(), pg::Error>> + Send + 'static,
{
    let driver = Driver {
        connection: Box::pin(connection),
        _permit: permit,
    };
    let worker = tokio::spawn(async move {
        let _ = driver.await;
    });
    Connection {
        client,
        worker,
        canceller,
        idle_since: Instant::now(),
    }
}

impl Pool {
    pub(crate) fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        // Called only by the public adapter before this new pool is shared.
        Arc::get_mut(&mut self.0)
            .expect("unshared new pool")
            .idle_ttl = Some(ttl);
        self
    }

    /// Creates a lazy pool using required, hostname-verified TLS, regardless of
    /// `config`'s SSL mode. No network work starts until acquisition.
    ///
    /// # Errors
    /// Returns `InvalidPolicy` for invalid capacity, absent hosts, Unix sockets,
    /// mismatched address/port counts, direct TLS negotiation, or required channel
    /// binding (unsupported). PostgreSQL connection defaults otherwise apply;
    /// an empty port list means 5432.
    pub fn new(
        config: pg::Config,
        tls: VerifiedTls,
        max: usize,
        profile: Profile,
    ) -> Result<Self, Error> {
        Self::build(config, Transport::Verified(tls), max, profile)
    }

    /// Explicit insecure escape hatch for disposable loopback test servers only.
    /// Every host must be a numeric loopback address, and every hostaddr (if any)
    /// must also be loopback; DNS names and Unix sockets are rejected.
    ///
    /// # Errors
    /// Returns `InvalidPolicy` for invalid capacity, hosts, or unsupported options.
    pub fn loopback_plaintext_for_tests(
        config: pg::Config,
        max: usize,
        profile: Profile,
    ) -> Result<Self, Error> {
        Self::build(config, Transport::LoopbackPlaintextForTests, max, profile)
    }

    fn build(
        config: pg::Config,
        transport: Transport,
        max: usize,
        profile: Profile,
    ) -> Result<Self, Error> {
        let hosts = config.get_hosts();
        let addresses = config.get_hostaddrs();
        let ports = config.get_ports();
        if max == 0 || max > Semaphore::MAX_PERMITS
            || hosts.is_empty()
            || (!addresses.is_empty() && addresses.len() != hosts.len())
            || (ports.len() > 1 && ports.len() != hosts.len())
            || ports.contains(&0)
            || !hosts.iter().all(|host| matches!(host, pg::config::Host::Tcp(name) if !name.is_empty()))
            || config.get_ssl_negotiation() != pg::config::SslNegotiation::Postgres
            || (matches!(transport, Transport::LoopbackPlaintextForTests)
                && (config.get_channel_binding() == pg::config::ChannelBinding::Require
                    || !hosts.iter().all(|host| matches!(host, pg::config::Host::Tcp(name) if name.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())))
                    || !addresses.iter().all(std::net::IpAddr::is_loopback)))
        {
            return Err(Error::InvalidPolicy);
        }
        let (closed, _) = watch::channel(false);
        Ok(Self(Arc::new(Shared {
            state: Mutex::new(State {
                closed: false,
                idle: Vec::new(),
            }),
            slots: Arc::new(Semaphore::new(max)),
            available: Notify::new(),
            closed,
            config,
            transport,
            profile,
            idle_ttl: None,
        })))
    }

    async fn initialize(&self, connection: Connection) -> Result<Lease, Error> {
        let mut lease = Lease {
            pool: self.clone(),
            connection: Some(connection),
            in_flight: false,
            poisoned: false,
            transaction: TransactionState::Autocommit,
        };
        lease
            .set_timeout(self.0.profile.ceiling_ms)
            .await
            .map_err(|_| Error::Initializer)?;
        if self.0.state.lock().expect("pool lock").closed {
            return Err(Error::Closed);
        }
        Ok(lease)
    }

    async fn initialize_or_closed(
        &self,
        connection: Connection,
        close: &mut watch::Receiver<bool>,
    ) -> Result<Lease, Error> {
        // A receiver subscribed after shutdown already sees true, but `changed`
        // waits for a future version. Check the current value before selecting.
        if *close.borrow() {
            return Err(Error::Closed);
        }
        tokio::select! {
            biased;
            _ = close.changed() => Err(Error::Closed),
            result = self.initialize(connection) => result,
        }
    }

    /// Acquires exclusive ownership; concurrent callers may wait, in unspecified
    /// order. The profile ceiling bounds the entire wait/startup/initialization.
    /// Dropping this future releases any in-progress socket before its permit.
    ///
    /// # Errors
    /// Returns `Closed`, `Timeout`, `Connection`, or `Initializer`.
    pub async fn acquire(&self) -> Result<Lease, Error> {
        tokio::time::timeout(self.lifecycle_timeout(), self.acquire_inner())
            .await
            .map_err(|_| Error::Timeout)?
    }

    fn lifecycle_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.0.profile.ceiling_ms))
    }

    async fn establish(&self) -> Result<(pg::Client, DriverFuture, Canceller), Error> {
        match &self.0.transport {
            Transport::LoopbackPlaintextForTests => {
                let mut config = self.0.config.clone();
                config.ssl_mode(pg::config::SslMode::Disable);
                let (client, driver) = config
                    .connect(pg::NoTls)
                    .await
                    .map_err(|_| Error::Connection)?;
                let canceller = Canceller::Plain(client.cancel_token());
                Ok((client, Box::pin(driver), canceller))
            }
            Transport::Verified(tls) => {
                let (client, driver, cancel) =
                    neotales_dbapi_postgres_tls::connect(&self.0.config, tls.clone())
                        .await
                        .map_err(|_| Error::Connection)?;
                Ok((client, Box::pin(driver), Canceller::Verified(cancel)))
            }
        }
    }

    async fn acquire_inner(&self) -> Result<Lease, Error> {
        let mut close = self.0.closed.subscribe();
        loop {
            let notified = self.0.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let idle = {
                let mut state = self.0.state.lock().expect("pool lock");
                if state.closed {
                    return Err(Error::Closed);
                }
                let mut found = None;
                while let Some(connection) = state.idle.pop() {
                    if !connection.client.is_closed()
                        && !connection.worker.is_finished()
                        && self
                            .0
                            .idle_ttl
                            .is_none_or(|ttl| connection.idle_since.elapsed() < ttl)
                    {
                        found = Some(connection);
                        break;
                    }
                }
                found
            };
            if let Some(connection) = idle {
                return self.initialize_or_closed(connection, &mut close).await;
            }
            tokio::select! {
                permit = self.0.slots.clone().acquire_owned() => {
                    // The permit is outside the establishment future. Cancellation
                    // drops that socket-owning future before this older local.
                    let permit = permit.map_err(|_| Error::Closed)?;
                    let (client, driver, canceller) = tokio::select! {
                        result = self.establish() => result?,
                        _ = close.changed() => return Err(Error::Closed),
                    };
                    let connection = spawn_driver(client, driver, permit, canceller);
                    if self.0.state.lock().expect("pool lock").closed {
                        return Err(Error::Closed);
                    }
                    return self.initialize_or_closed(connection, &mut close).await;
                }
                () = &mut notified => {}
                _ = close.changed() => return Err(Error::Closed),
            }
        }
    }

    /// Rejects new acquisitions and aborts idle workers. Outstanding leases
    /// are discarded when dropped or recycled; this does not forcibly revoke them.
    /// Idempotent and synchronous: requests abort but does not join workers.
    ///
    /// # Panics
    /// Panics if an internal invariant panic previously poisoned the pool mutex.
    pub fn close(&self) {
        let mut state = self.0.state.lock().expect("pool lock");
        if !state.closed {
            state.closed = true;
            state.idle.clear();
            self.0.slots.close();
            self.0.closed.send_replace(true);
            self.0.available.notify_waiters();
        }
    }

    /// Snapshot of unused physical connection permits, including after close.
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.0.slots.available_permits()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TransactionState {
    Autocommit,
    Active,
    Aborted,
}

/// Exclusive connection owner. Drop discards; only successful `recycle` permits
/// reuse. Dropping an operation future after its first poll permanently poisons
/// this lease. Plain operations rely on the server ceiling; callers should also
/// wrap them in a client deadline to cover transport stalls.
pub struct Lease {
    pool: Pool,
    connection: Option<Connection>,
    in_flight: bool,
    poisoned: bool,
    transaction: TransactionState,
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Lease(..)")
    }
}

enum Cell {
    Int(i32),
    Bool(bool),
    Text(String),
}

/// Owns decoded values only; driver rows retain statement handles that can
/// otherwise issue a delayed CLOSE on a subsequently reused connection.
/// Debug output is redacted; getters intentionally expose result data.
pub struct Row(Vec<Cell>);

impl std::fmt::Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Row(..)")
    }
}

impl Row {
    /// Reads an `INT4` column by zero-based index.
    /// # Errors
    /// Returns `Operation` for an absent column or wrong type.
    pub fn int(&self, index: usize) -> Result<i32, Error> {
        match self.0.get(index) {
            Some(Cell::Int(value)) => Ok(*value),
            _ => Err(Error::Operation),
        }
    }
    /// Reads a `BOOL` column by zero-based index.
    /// # Errors
    /// Returns `Operation` for an absent column or wrong type.
    pub fn boolean(&self, index: usize) -> Result<bool, Error> {
        match self.0.get(index) {
            Some(Cell::Bool(value)) => Ok(*value),
            _ => Err(Error::Operation),
        }
    }
    /// Borrows a `TEXT` column by zero-based index.
    /// # Errors
    /// Returns `Operation` for an absent column or wrong type.
    pub fn text(&self, index: usize) -> Result<&str, Error> {
        match self.0.get(index) {
            Some(Cell::Text(value)) => Ok(value),
            _ => Err(Error::Operation),
        }
    }
}

impl Lease {
    pub(crate) fn reusable(&self) -> bool {
        !self.poisoned
            && !self.in_flight
            && self.connection.is_some()
            && self.transaction == TransactionState::Autocommit
    }

    fn check(&mut self) -> Result<&pg::Client, Error> {
        if self.poisoned || self.in_flight {
            return Err(Error::Poisoned);
        }
        self.in_flight = true;
        Ok(&self.connection.as_ref().expect("leased connection").client)
    }

    async fn set_timeout(&mut self, millis: u32) -> Result<(), Error> {
        let setting = format!("{millis}ms");
        self.query(
            "SELECT pg_catalog.set_config('statement_timeout', $1::text, false)",
            &[&setting],
        )
        .await?;
        let actual = self
            .query(
                "SELECT setting::int4 FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'",
                &[],
            )
            .await?;
        if u32::try_from(actual.int(0)?).ok() != Some(millis) {
            self.poisoned = true;
            return Err(Error::Initializer);
        }
        Ok(())
    }

    /// The server limit applies to each statement; total bounds client-side setup,
    /// dispatch, execution, and restoring the ceiling. After total expires the
    /// caller may wait up to the configured cancellation grace as well.
    /// A cancel request is not confirmation: the lease is never reusable.
    /// # Errors
    /// Returns `InvalidPolicy` before dispatch if limits are invalid or exceed the
    /// ceiling. Other operation failures poison the lease. `OutcomeUnknown` means
    /// writes may have completed, including when only ceiling restoration failed.
    /// # Panics
    /// Panics if the internal invariant that a live lease owns a connection is broken.
    pub async fn batch_deadline(
        &mut self,
        sql: &str,
        statement_timeout: Duration,
        total: Duration,
    ) -> Result<(), Error> {
        let millis = milliseconds(statement_timeout)?;
        if millis > self.pool.0.profile.ceiling_ms || milliseconds(total).is_err() {
            return Err(Error::InvalidPolicy);
        }
        let ceiling = self.pool.0.profile.ceiling_ms;
        let operation = async {
            self.set_timeout(millis).await?;
            self.batch(sql).await?;
            self.set_timeout(ceiling).await
        };
        if let Ok(result) = tokio::time::timeout(total, operation).await {
            result
        } else {
            // Set before awaiting cancel, so dropping this future cannot
            // allow a late cancellation to hit a reused backend.
            self.poisoned = true;
            let connection = self.connection.as_ref().expect("leased connection");
            let _ = tokio::time::timeout(
                self.pool.0.profile.cancel_grace,
                connection.canceller.send(),
            )
            .await;
            Err(Error::OutcomeUnknown)
        }
    }

    /// Executes trusted SQL; no retry. Errors or interruption poison the lease.
    /// A batch may partially commit even on a reported error.
    /// # Errors
    /// Returns `Poisoned`, `Operation`, `ServerCancelled`, or `OutcomeUnknown`.
    pub async fn batch(&mut self, sql: &str) -> Result<(), Error> {
        let result = self.check()?.batch_execute(sql).await;
        self.in_flight = false;
        if result.is_err() {
            self.poisoned = true;
        }
        result.map_err(|error| operation_error(&error))
    }

    /// Executes trusted SQL with fork `ToSql` parameters and eagerly decodes one
    /// row into owned values, retaining no driver statement handles.
    /// # Errors
    /// Returns `Poisoned`, `Operation` (including unsupported/null result types),
    /// `ServerCancelled`, or `OutcomeUnknown`. Failures poison the lease and do
    /// not imply that writes were rolled back.
    pub async fn query(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error> {
        let result = self
            .check()?
            .query_one(sql, params)
            .await
            .map_err(|error| operation_error(&error))
            .and_then(|row| {
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| match *column.type_() {
                        pg::types::Type::INT4 => row
                            .try_get(index)
                            .map(Cell::Int)
                            .map_err(|_| Error::Operation),
                        pg::types::Type::BOOL => row
                            .try_get(index)
                            .map(Cell::Bool)
                            .map_err(|_| Error::Operation),
                        pg::types::Type::TEXT => row
                            .try_get(index)
                            .map(Cell::Text)
                            .map_err(|_| Error::Operation),
                        _ => Err(Error::Operation),
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(Row)
            });
        self.in_flight = false;
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Checks preparation, immediately dropping the statement handle. Does not
    /// execute the statement; errors or interruption poison the lease.
    /// # Errors
    /// Returns `Poisoned` or a sanitized operation failure.
    pub async fn prepare(&mut self, sql: &str) -> Result<(), Error> {
        let result = self.check()?.prepare(sql).await;
        self.in_flight = false;
        if result.is_err() {
            self.poisoned = true;
        }
        result.map(drop).map_err(|_| Error::Operation)
    }

    /// The future owns the lease; cancellation at either await drops it and aborts
    /// the worker, so an unconfirmed reset cannot be placed in the idle pool.
    /// Cleanup is bounded by the profile ceiling and wakes on pool close.
    /// # Errors
    /// Returns `Poisoned`, `Closed`, `Timeout`, or a sanitized operation failure.
    /// Any failure discards the connection.
    pub async fn recycle(self) -> Result<(), Error> {
        let mut closed = self.pool.0.closed.subscribe();
        if *closed.borrow() {
            return Err(Error::Closed);
        }
        let timeout = self.pool.lifecycle_timeout();
        tokio::select! {
            biased;
            _ = closed.changed() => Err(Error::Closed),
            result = tokio::time::timeout(timeout, self.recycle_inner()) => {
                result.map_err(|_| Error::Timeout)?
            }
        }
    }

    async fn recycle_inner(mut self) -> Result<(), Error> {
        if self.in_flight || self.poisoned {
            return Err(Error::Poisoned);
        }
        self.reset_command("ROLLBACK", pg::CommandTag::Rollback)
            .await?;
        self.connection
            .as_mut()
            .expect("leased connection")
            .client
            .clear_cached_typeinfo();
        self.reset_command("DISCARD ALL", pg::CommandTag::Other)
            .await?;
        let mut state = self.pool.0.state.lock().expect("pool lock");
        if state.closed {
            return Err(Error::Closed);
        }
        self.connection
            .as_mut()
            .expect("recycled connection")
            .idle_since = Instant::now();
        state
            .idle
            .push(self.connection.take().expect("leased connection"));
        self.pool.0.available.notify_one();
        Ok(())
    }

    async fn reset_command(&mut self, sql: &str, tag: pg::CommandTag) -> Result<(), Error> {
        let result = self.check()?.batch_complete(sql).await;
        self.in_flight = false;
        match result {
            Ok(result)
                if result.commands == 1
                    && result.tag == tag
                    && result.status == pg::TransactionStatus::Idle =>
            {
                Ok(())
            }
            _ => {
                self.poisoned = true;
                Err(Error::Operation)
            }
        }
    }
}

fn operation_error(error: &pg::Error) -> Error {
    // Only protocol field V is nonlocalized. Without it we cannot distinguish
    // a statement rejection from a fatal disconnect using field S, so preserve
    // an unknown outcome rather than guessing from translated server text.
    if error.as_db_error().is_some_and(|error| {
        matches!(
            error.parsed_severity(),
            None | Some(pg::error::Severity::Fatal | pg::error::Severity::Panic)
        )
    }) {
        return Error::OutcomeUnknown;
    }
    // SQLSTATE's explicit unknown-completion / connection-exception categories
    // also do not establish a write outcome, even with a known nonfatal severity.
    match error.code().map(pg::error::SqlState::code) {
        Some(code) if code == "40003" || code.starts_with("08") => Error::OutcomeUnknown,
        Some("57014") => Error::ServerCancelled,
        Some(_) => Error::Operation,
        None => Error::OutcomeUnknown,
    }
}
