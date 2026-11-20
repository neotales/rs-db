use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, ErrorKind, ExecuteOptions, IsolationLevel,
    Pool, PoolDefaults, QueryOptions, Reader, Session, TimeoutSetting, Transaction, TxOptions,
};
use tokio::sync::Notify;

use crate::experimental::{self, Config, Lease, Profile, VerifiedTls};

fn error(kind: ErrorKind) -> DbError {
    DbError::new(kind)
}

impl From<experimental::Error> for DbError {
    fn from(cause: experimental::Error) -> Self {
        use experimental::Error as E;
        error(match cause {
            E::InvalidPolicy => ErrorKind::InvalidArgument,
            E::Timeout => ErrorKind::Timeout,
            E::ServerCancelled => ErrorKind::Cancelled,
            E::OutcomeUnknown => ErrorKind::WriteOutcomeUnknown,
            E::Operation => ErrorKind::Decode,
            E::Closed | E::Connection | E::Poisoned | E::Initializer => ErrorKind::Connection,
        })
    }
}

/// Validated, finite per-instance pooling/deadline policy. No environment defaults
/// or unlimited timeout is implicit. Debug output contains policy values only.
#[derive(Clone, Copy, Debug)]
pub struct PoolOptions {
    max_connections: usize,
    profile: Profile,
    idle_ttl: Duration,
}

impl PoolOptions {
    fn defaults(self) -> PoolDefaults {
        PoolDefaults {
            timeout: Some(self.profile.ceiling()),
            ..PoolDefaults::default()
        }
    }

    /// Sets a positive socket-owner capacity, caller-selected statement/cancel
    /// profile and finite idle TTL. TTL is checked lazily at checkout; expired
    /// idle connections are discarded before a replacement obtains capacity.
    /// TTL must be integral milliseconds in `1..=i32::MAX`.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid capacity or TTL. Writes no SQL.
    pub fn new(
        max_connections: usize,
        profile: Profile,
        idle_ttl: Duration,
    ) -> Result<Self, DbError> {
        if max_connections == 0
            || max_connections > tokio::sync::Semaphore::MAX_PERMITS
            || idle_ttl.is_zero()
            || idle_ttl.as_millis() > i32::MAX as u128
            || !idle_ttl.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(error(ErrorKind::InvalidArgument));
        }
        Ok(Self {
            max_connections,
            profile,
            idle_ttl,
        })
    }

    fn budget(self, options: ExecuteOptions) -> Result<Duration, DbError> {
        match options.timeout_ms {
            None => Ok(self.profile.ceiling()),
            Some(millis)
                if millis != 0 && u128::from(millis) <= self.profile.ceiling().as_millis() =>
            {
                Ok(Duration::from_millis(millis))
            }
            Some(_) => Err(error(ErrorKind::InvalidArgument)),
        }
    }
}

#[derive(Default)]
struct Cleanup {
    pending: AtomicUsize,
    closed: AtomicBool,
    first_failure: Mutex<Option<DbError>>,
    changed: Notify,
}

impl Cleanup {
    fn failed(&self, cause: DbError) {
        let mut failure = self.first_failure.lock().expect("cleanup lock");
        if failure.is_none() {
            *failure = Some(cause);
        }
    }
}

struct Pending {
    cleanup: Arc<Cleanup>,
    finished: bool,
}

impl Pending {
    fn finish(&mut self, result: Result<(), experimental::Error>) {
        if let Err(cause) = result {
            // Close intentionally discards instead of recycling. It does not
            // hide an operation outcome: only already-settled leases enter here.
            if cause != experimental::Error::Closed || !self.cleanup.closed.load(Ordering::Acquire)
            {
                // These are only internal ROLLBACK/DISCARD operations after
                // settled user work, never an unknown user write/commit.
                self.cleanup
                    .failed(error(if cause == experimental::Error::Timeout {
                        ErrorKind::Timeout
                    } else {
                        ErrorKind::Connection
                    }));
            }
        }
        self.finished = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.finished && !self.cleanup.closed.load(Ordering::Acquire) {
            self.cleanup.failed(error(ErrorKind::Connection));
        }
        self.cleanup.pending.fetch_sub(1, Ordering::AcqRel);
        self.cleanup.changed.notify_waiters();
    }
}

struct Shared {
    pool: experimental::Pool,
    policy: PoolOptions,
    cleanup: Arc<Cleanup>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.cleanup.closed.store(true, Ordering::Release);
        self.pool.close();
    }
}

/// Cloneable bounded, reusable quiet PostgreSQL pool. Uses verified TLS by default.
/// Connections are exclusive to Sessions and owned transactions; no cross-session
/// or cross-transaction retry occurs. Debug never exposes connection configuration.
/// Isolation capability covers read-committed, repeatable-read and serializable;
/// read-uncommitted is explicitly rejected, not silently upgraded.
#[derive(Clone)]
pub struct PostgresPool(Arc<Shared>);

impl std::fmt::Debug for PostgresPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PostgresPool(..)")
    }
}

impl PostgresPool {
    /// Creates a lazy pool with required, hostname/chain-verified TLS. No socket
    /// opens until acquire. The supplied Config's weak SSL mode is overridden;
    /// there is never a plaintext fallback.
    ///
    /// # Errors
    /// Returns sanitized `InvalidArgument` for unsupported/invalid configuration.
    pub fn new(config: Config, tls: VerifiedTls, policy: PoolOptions) -> Result<Self, DbError> {
        let pool = experimental::Pool::new(config, tls, policy.max_connections, policy.profile)?
            .with_idle_ttl(policy.idle_ttl);
        Ok(Self::wrap(pool, policy))
    }

    /// Explicit insecure constructor for disposable loopback test servers only.
    /// Every host must be a numeric loopback IP and any hostaddrs also loopback.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for nonloopback/ambiguous or invalid configuration.
    pub fn loopback_plaintext_for_tests(
        config: Config,
        policy: PoolOptions,
    ) -> Result<Self, DbError> {
        let pool = experimental::Pool::loopback_plaintext_for_tests(
            config,
            policy.max_connections,
            policy.profile,
        )?
        .with_idle_ttl(policy.idle_ttl);
        Ok(Self::wrap(pool, policy))
    }

    fn wrap(pool: experimental::Pool, policy: PoolOptions) -> Self {
        Self(Arc::new(Shared {
            pool,
            policy,
            cleanup: Arc::new(Cleanup::default()),
        }))
    }

    /// Rejects/wakes acquisitions, aborts idle workers and requests cleanup
    /// discard. Idempotent; outstanding Sessions/transactions stay caller-owned.
    /// This is not a join barrier or server-side work revocation.
    pub fn close(&self) {
        self.0.cleanup.closed.store(true, Ordering::Release);
        self.0.pool.close();
    }

    /// Waits for currently registered Session-drop reset tasks. Drop Sessions
    /// first; this does not close active owners. Concurrent drops can extend the
    /// wait; drops after it returns are outside the barrier. Planned discard
    /// after close is not a reset failure.
    ///
    /// # Errors
    /// Returns the first sanitized cleanup failure, persistently on subsequent
    /// calls too. Runtime shutdown/no runtime is a cleanup failure and discards.
    /// Reset never masks or replaces the preceding statement/commit result.
    ///
    /// # Panics
    /// Panics only if an internal panic previously poisoned the cleanup mutex.
    pub async fn wait_for_cleanup(&self) -> Result<(), DbError> {
        loop {
            let changed = self.0.cleanup.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.cleanup.pending.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
        self.0
            .cleanup
            .first_failure
            .lock()
            .expect("cleanup lock")
            .map_or(Ok(()), Err)
    }

    /// Snapshot of unused physical socket-owner permits, not idle lease capacity.
    /// Idle reusable connections still hold a physical permit.
    #[must_use]
    pub fn unused_connection_slots(&self) -> usize {
        self.0.pool.free_slots()
    }
}

impl Pool for PostgresPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: true,
            isolation_level: true,
            read_only_transactions: true,
            trusted_scripts: true,
        }
    }

    fn defaults(&self) -> PoolDefaults {
        self.0.policy.defaults()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(async move {
            let lease = self.0.pool.acquire().await.map_err(DbError::from)?;
            Ok(Box::new(PgSession {
                lease: Some(lease),
                shared: self.0.clone(),
            }) as Box<dyn Session>)
        })
    }
}

struct PgSession {
    lease: Option<Lease>,
    shared: Arc<Shared>,
}

impl Drop for PgSession {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        if !lease.reusable() || self.shared.cleanup.closed.load(Ordering::Acquire) {
            return;
        }
        let cleanup = self.shared.cleanup.clone();
        cleanup.pending.fetch_add(1, Ordering::AcqRel);
        let mut pending = Pending {
            cleanup,
            finished: false,
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                pending.finish(lease.recycle().await);
            });
        }
        // No runtime: pending records the failure and the owned lease discards.
    }
}

impl PgSession {
    fn lease(&mut self) -> Result<&mut Lease, DbError> {
        self.lease
            .as_mut()
            .ok_or_else(|| error(ErrorKind::Connection))
    }
}

impl Session for PgSession {
    fn defaults(&self) -> PoolDefaults {
        self.shared.policy.defaults()
    }

    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            options.prepare_execute_provider(self.shared.policy.defaults())?;
            let budget = self
                .shared
                .policy
                .budget(options.provider_execute_options())?;
            self.lease()?
                .execute_bounded(sql, &options.parameters, Some(budget))
                .await
        })
    }
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            let limit = options.prepare_provider(self.shared.policy.defaults())?;
            let budget = self
                .shared
                .policy
                .budget(options.provider_execute_options())?;
            let rows = self
                .lease()?
                .fetch_bounded(sql, &options.parameters, limit, Some(budget))
                .await?;
            Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
        })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            options.prepare_script_provider(self.shared.policy.defaults())?;
            let budget = self
                .shared
                .policy
                .budget(options.provider_execute_options())?;
            self.lease()?.script_bounded(sql, Some(budget)).await
        })
    }
    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async move {
            if options.isolation == Some(IsolationLevel::ReadUncommitted) {
                return Err(error(ErrorKind::Unsupported));
            }
            let lease = self
                .lease
                .take()
                .ok_or_else(|| error(ErrorKind::Connection))?;
            let transaction = lease.begin(options).await?;
            Ok(Box::new(PgTransaction {
                transaction: Some(transaction),
                slot: &mut self.lease,
                policy: self.shared.policy,
            }) as Box<dyn Transaction<'a> + 'a>)
        })
    }
}

struct PgTransaction<'a> {
    transaction: Option<experimental::Transaction>,
    slot: &'a mut Option<Lease>,
    policy: PoolOptions,
}

impl PgTransaction<'_> {
    fn transaction(&mut self) -> Result<&mut experimental::Transaction, DbError> {
        self.transaction
            .as_mut()
            .ok_or_else(|| error(ErrorKind::Connection))
    }
}

impl<'session> Transaction<'session> for PgTransaction<'session> {
    fn defaults(&self) -> PoolDefaults {
        self.policy.defaults()
    }
    fn execute_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            options.prepare_execute_provider(self.policy.defaults())?;
            let budget = self.policy.budget(options.provider_execute_options())?;
            self.transaction()?
                .execute_with_budget(sql, &options.parameters, budget)
                .await
        })
    }
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            let limit = options.prepare_provider(self.policy.defaults())?;
            let budget = self.policy.budget(options.provider_execute_options())?;
            let rows = self
                .transaction()?
                .fetch_with_budget(sql, &options.parameters, limit, budget)
                .await?;
            Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader + 'a>)
        })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            if options.timeout == TimeoutSetting::Disabled {
                return Err(error(ErrorKind::Unsupported));
            }
            options.prepare_script_provider(self.policy.defaults())?;
            let budget = self.policy.budget(options.provider_execute_options())?;
            self.transaction()?.script_with_budget(sql, budget).await
        })
    }
    fn commit_async(mut self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let transaction = self
                .transaction
                .take()
                .ok_or_else(|| error(ErrorKind::Connection))?;
            let lease = transaction.commit().await?;
            *self.slot = Some(lease);
            Ok(())
        })
    }
    fn rollback_async(mut self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async move {
            let transaction = self
                .transaction
                .take()
                .ok_or_else(|| error(ErrorKind::Connection))?;
            let lease = transaction.rollback().await?;
            *self.slot = Some(lease);
            Ok(())
        })
    }
}
