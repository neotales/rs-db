#![doc = include_str!("../README.md")]

use neotales_dbapi::{
    Capabilities, DbError, DbFuture, Pool, QueryOptions, Reader, Session, Transaction, TxOptions,
};
use neotales_dbapi_mysql::{MySqlConfig, MySqlPoolProvider, MySqlSession, MySqlTls};
use std::{fmt, path::PathBuf, time::Duration};

/// TLS policy for a [`MariaDbConfig`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MariaDbTls {
    /// Encrypt the connection, verify the server certificate, and verify the server host name.
    VerifyIdentity,
    /// Encrypt without verifying the certificate or host name.
    ///
    /// This accepts an attacker who can intercept the connection. It is an explicit downgrade
    /// and is never selected automatically.
    Insecure,
}

impl From<MariaDbTls> for MySqlTls {
    fn from(value: MariaDbTls) -> Self {
        match value {
            MariaDbTls::VerifyIdentity => Self::VerifyIdentity,
            MariaDbTls::Insecure => Self::Insecure,
        }
    }
}

/// Validated MariaDB connection configuration.
///
/// This is a MariaDB-named facade over the reviewed MySQL wire implementation. The password is
/// zeroized by the inner configuration and is never included in `Debug`.
pub struct MariaDbConfig {
    inner: MySqlConfig,
}

impl MariaDbConfig {
    /// Creates a TLS-required configuration that verifies the server identity.
    ///
    /// Built-in `WebPKI` roots are used until [`Self::ca_file`] selects a private bundle.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            inner: MySqlConfig::new(host, port, user, database),
        }
    }

    /// Creates a no-TLS configuration for a numeric loopback host only.
    ///
    /// This is intended for a disposable local server and is the only cleartext constructor.
    ///
    /// # Errors
    /// Returns `InvalidArgument` unless the host is a numeric loopback address, or another field
    /// is invalid.
    pub fn plaintext_loopback(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, DbError> {
        Ok(Self {
            inner: MySqlConfig::plaintext_loopback(host, port, user, database)?,
        })
    }

    /// Sets the password. Prefer a value loaded from the caller's secret store.
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner = self.inner.password(password);
        self
    }

    /// Selects the TLS policy and clears any plaintext-loopback selection.
    #[must_use]
    pub fn tls(mut self, tls: MariaDbTls) -> Self {
        self.inner = self.inner.tls(tls.into());
        self
    }

    /// Uses the given PEM CA bundle for [`MariaDbTls::VerifyIdentity`].
    #[must_use]
    pub fn ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.ca_file(path);
        self
    }

    /// Sets the maximum number of pooled physical connections. The default is five.
    #[must_use]
    pub fn max_connections(mut self, count: u32) -> Self {
        self.inner = self.inner.max_connections(count);
        self
    }

    /// Bounds waiting for a pooled connection, including opening a replacement.
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.acquire_timeout(timeout);
        self
    }
}

impl fmt::Debug for MariaDbConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MariaDbConfig")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// MariaDB pool implementing the driver-neutral [`neotales_dbapi`] traits.
#[derive(Clone)]
pub struct MariaDbPoolProvider {
    inner: MySqlPoolProvider,
}

impl fmt::Debug for MariaDbPoolProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MariaDbPoolProvider")
            .finish_non_exhaustive()
    }
}

impl MariaDbPoolProvider {
    /// Opens the inner pool and establishes at least one physical connection.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for invalid configuration, `Timeout` when the acquisition
    /// budget elapses, or a sanitized `Connection` error.
    pub async fn connect(config: MariaDbConfig) -> Result<Self, DbError> {
        Ok(Self {
            inner: MySqlPoolProvider::connect(config.inner).await?,
        })
    }

    /// Acquires one exclusive MariaDB session.
    ///
    /// A caller that owns a session-level `GET_LOCK` and cannot guarantee an explicit release
    /// must call [`MariaDbSession::discard`] before dropping the session.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, a sanitized `Connection` error,
    /// or `Unsupported` when the session's character sets or SQL mode are unsafe to scan.
    pub async fn acquire_mariadb_session(&self) -> Result<MariaDbSession, DbError> {
        Ok(MariaDbSession(self.inner.acquire_mysql_session().await?))
    }

    /// Acquires one exclusive session as a trait object.
    ///
    /// # Errors
    /// Returns `Timeout` when the acquisition budget elapses, a sanitized `Connection` error,
    /// or `Unsupported` when the session's character sets or SQL mode are unsafe to scan.
    pub async fn acquire_mariadb(&self) -> Result<Box<dyn Session>, DbError> {
        Ok(Box::new(self.acquire_mariadb_session().await?))
    }

    /// Closes the inner pool and waits for checked-out sessions to be dropped.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}

impl Pool for MariaDbPoolProvider {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        Box::pin(
            async move { Ok(Box::new(self.acquire_mariadb_session().await?) as Box<dyn Session>) },
        )
    }
}

/// One exclusive MariaDB session acquired by [`MariaDbPoolProvider::acquire_mariadb_session`].
///
/// Dropping a normally completed session returns its connection to the pool. A caller that owns
/// a session-level lock and cannot guarantee an explicit release must call [`Self::discard`].
pub struct MariaDbSession(MySqlSession);

impl fmt::Debug for MariaDbSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MariaDbSession")
            .finish_non_exhaustive()
    }
}

impl MariaDbSession {
    /// Marks this session's physical connection for discard when the session is dropped.
    pub fn discard(&mut self) {
        self.0.discard();
    }
}

impl Session for MariaDbSession {
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64> {
        self.0.execute_async(sql, options)
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        self.0.fetch_async(sql, options)
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        self.0.execute_trusted_script_async(sql, options)
    }

    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        self.0.begin_async(options)
    }
}
