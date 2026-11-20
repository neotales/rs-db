//! Validated native ClickHouse connection configuration.
//!
//! TLS is verified against an explicit PEM root bundle and a host name. The only cleartext
//! mode is explicit numeric loopback, and the connector checks the peer address before the
//! native handshake. There is no insecure TLS mode and no silent fallback.

use std::{
    fmt,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use neotales_dbapi::{DbError, ErrorKind, PoolDefaults};
use rustls_pki_types::ServerName;
use tokio::io::AsyncReadExt;
use zeroize::Zeroizing;

/// Default per-statement deadline.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Default TCP/TLS connection establishment deadline.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest CA bundle accepted from disk.
const MAX_CA_FILE_BYTES: u64 = 4 * 1024 * 1024;

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

/// Validated native ClickHouse configuration.
///
/// The password is zeroized when the configuration is dropped and is never included in `Debug`.
/// Host names, paths, database names, SQL, and credentials are not retained in error values.
///
/// [`ClickHouseConfig::new`] selects verified TLS. A PEM root bundle must be supplied with
/// [`ClickHouseConfig::ca_file`], and the certificate name is the host unless
/// [`ClickHouseConfig::tls_server_name`] separates the transport address from the verified name.
/// [`ClickHouseConfig::plaintext_loopback`] is the only cleartext constructor.
#[derive(Clone)]
pub struct ClickHouseConfig {
    host: String,
    port: u16,
    user: String,
    password: Option<Zeroizing<String>>,
    database: String,
    ca_file: Option<PathBuf>,
    server_name: Option<String>,
    plaintext: bool,
    request_timeout: Duration,
    connect_timeout: Duration,
}

impl ClickHouseConfig {
    /// Creates a verified-TLS configuration for a native-protocol endpoint.
    ///
    /// Built-in roots are not used. Call [`Self::ca_file`] with an explicit PEM bundle before
    /// connecting. The host is both the transport address and, unless
    /// [`Self::tls_server_name`] overrides it, the certificate name. Host, user, and database
    /// must be non-empty UTF-8 without NUL bytes and the port must be nonzero. A password is
    /// optional and must be supplied separately.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            user: user.into(),
            password: None,
            database: database.into(),
            ca_file: None,
            server_name: None,
            plaintext: false,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Creates a no-TLS configuration for a numeric loopback host only.
    ///
    /// This is intended for a disposable local server. A host name such as `localhost` is
    /// rejected because name resolution can leave the loopback interface. The connector also
    /// checks the actual peer address before sending the native handshake. No other
    /// constructor disables TLS.
    ///
    /// # Errors
    /// Returns `InvalidArgument` unless the host is a numeric loopback address, or another
    /// field is invalid.
    pub fn plaintext_loopback(
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, DbError> {
        let mut config = Self::new(host, port, user, database);
        config.plaintext = true;
        config.ca_file = None;
        config.server_name = None;
        config.validate()?;
        Ok(config)
    }

    /// Sets the password. Prefer a value loaded from the caller's secret store.
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(Zeroizing::new(password.into()));
        self
    }

    /// Uses the given PEM CA bundle as the only TLS trust roots.
    #[must_use]
    pub fn ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// Verifies the TLS certificate against this host name instead of the transport host.
    ///
    /// This is for a verified TLS connection whose transport address differs from its
    /// certificate identity, such as a loopback test proxy. The name must be a DNS name, not
    /// an IP literal.
    #[must_use]
    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Bounds one statement, including native result collection. The default is thirty seconds.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Bounds establishing a native connection, TLS handshake, and version probe. The default is
    /// five seconds.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub(crate) fn configured_host(&self) -> &str {
        &self.host
    }

    pub(crate) fn configured_port(&self) -> u16 {
        self.port
    }

    pub(crate) fn configured_user(&self) -> &str {
        &self.user
    }

    pub(crate) fn configured_password(&self) -> Option<&str> {
        self.password.as_deref().map(String::as_str)
    }

    pub(crate) fn configured_database(&self) -> &str {
        &self.database
    }

    pub(crate) fn configured_ca_file(&self) -> Option<&Path> {
        self.ca_file.as_deref()
    }

    pub(crate) fn configured_server_name(&self) -> &str {
        self.server_name.as_deref().unwrap_or(&self.host)
    }

    pub(crate) const fn is_plaintext(&self) -> bool {
        self.plaintext
    }

    pub(crate) const fn configured_connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub(crate) fn defaults(&self) -> PoolDefaults {
        PoolDefaults {
            timeout: Some(self.request_timeout),
            ..PoolDefaults::default()
        }
    }

    /// Validates every connection field without opening a socket or reading the trust bundle.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for an invalid host, port, user, database, password, timeout,
    /// TLS name, or a missing TLS root path.
    pub(crate) fn validate(&self) -> Result<(), DbError> {
        if !valid_text(&self.host, 255)
            || self.port == 0
            || !valid_text(&self.user, 255)
            || !valid_text(&self.database, 255)
            || self
                .password
                .as_ref()
                .is_some_and(|value| value.as_bytes().contains(&0))
            || self.request_timeout.as_millis() == 0
            || self.connect_timeout.as_millis() == 0
        {
            return Err(invalid());
        }
        if self.plaintext {
            if self.ca_file.is_some()
                || self.server_name.is_some()
                || !self
                    .host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
            {
                return Err(invalid());
            }
            return Ok(());
        }
        let Some(ca_file) = &self.ca_file else {
            return Err(invalid());
        };
        if ca_file.as_os_str().is_empty() || !valid_tls_name(self.configured_server_name()) {
            return Err(invalid());
        }
        Ok(())
    }

    /// Reads the explicit PEM root bundle through cancellable asynchronous filesystem I/O.
    ///
    /// The opened descriptor must name a regular file no larger than [`MAX_CA_FILE_BYTES`].
    /// Opening uses nonblocking mode so replacement with a FIFO cannot trap a blocking worker.
    /// The caller races filesystem operations against the connection deadline; regular files on
    /// remote or unhealthy filesystems can still stall in the operating system.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when the path is not a regular file, exceeds the cap, cannot be
    /// read, or contains no usable certificate.
    pub(crate) async fn read_roots(&self) -> Result<tokio_rustls::rustls::RootCertStore, DbError> {
        let path = self.configured_ca_file().ok_or_else(invalid)?;
        let path = path.to_owned();
        let file = tokio::task::spawn_blocking(move || open_ca_file(&path))
            .await
            .map_err(|_| invalid())??;
        let file = tokio::fs::File::from_std(file);
        let metadata = file.metadata().await.map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.len() > MAX_CA_FILE_BYTES {
            return Err(invalid());
        }
        let mut pem = Vec::new();
        file.take(MAX_CA_FILE_BYTES + 1)
            .read_to_end(&mut pem)
            .await
            .map_err(|_| invalid())?;
        if pem.len() as u64 > MAX_CA_FILE_BYTES {
            return Err(invalid());
        }
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        let mut found = false;
        for certificate in rustls_pemfile::certs(&mut pem.as_slice()) {
            let certificate = certificate.map_err(|_| invalid())?;
            roots.add(certificate).map_err(|_| invalid())?;
            found = true;
        }
        if found { Ok(roots) } else { Err(invalid()) }
    }
}

#[cfg(unix)]
fn open_ca_file(path: &Path) -> Result<std::fs::File, DbError> {
    use rustix::fs::{Mode, OFlags, open};
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| invalid())?;
    let file = std::fs::File::from(fd);
    let metadata = file.metadata().map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() > MAX_CA_FILE_BYTES {
        return Err(invalid());
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_ca_file(_path: &Path) -> Result<std::fs::File, DbError> {
    Err(DbError::new(ErrorKind::Unsupported))
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.as_bytes().contains(&0)
}

fn valid_tls_name(name: &str) -> bool {
    valid_text(name, 255)
        && name.parse::<IpAddr>().is_err()
        && ServerName::try_from(name.to_owned()).is_ok()
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseConfig")
            .field("host", &"[redacted]")
            .field("port", &"[redacted]")
            .field("user", &"[redacted]")
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("database", &"[redacted]")
            .field("ca_file", &self.ca_file.as_ref().map(|_| "[redacted]"))
            .field(
                "server_name",
                &self.server_name.as_ref().map(|_| "[redacted]"),
            )
            .field("plaintext", &self.plaintext)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::ClickHouseConfig;
    use std::time::Duration;

    fn tls() -> ClickHouseConfig {
        ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
            .ca_file("/run/secrets/clickhouse-ca.pem")
    }

    #[test]
    fn plaintext_is_numeric_loopback_only() {
        assert!(
            ClickHouseConfig::plaintext_loopback("127.0.0.1", 9000, "default", "default").is_ok()
        );
        assert!(ClickHouseConfig::plaintext_loopback("::1", 9000, "default", "default").is_ok());
        assert!(
            ClickHouseConfig::plaintext_loopback("localhost", 9000, "default", "default").is_err()
        );
        assert!(
            ClickHouseConfig::plaintext_loopback("192.0.2.10", 9000, "default", "default").is_err()
        );
        assert!(
            ClickHouseConfig::plaintext_loopback("127.0.0.1", 9000, "default", "default")
                .unwrap()
                .ca_file("/tmp/ca.pem")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn tls_requires_explicit_roots_and_a_hostname() {
        assert!(tls().validate().is_ok());
        assert!(
            ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
                .validate()
                .is_err()
        );
        assert!(
            ClickHouseConfig::new("127.0.0.1", 9440, "app", "app")
                .ca_file("/run/secrets/clickhouse-ca.pem")
                .validate()
                .is_err()
        );
        assert!(
            ClickHouseConfig::new("127.0.0.1", 9440, "app", "app")
                .ca_file("/run/secrets/clickhouse-ca.pem")
                .tls_server_name("clickhouse.example")
                .validate()
                .is_ok()
        );
        assert!(
            ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
                .ca_file("/run/secrets/clickhouse-ca.pem")
                .tls_server_name("127.0.0.1")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn invalid_text_and_zero_timeouts_are_rejected() {
        for config in [
            ClickHouseConfig::new("", 9440, "app", "app").ca_file("/ca.pem"),
            ClickHouseConfig::new("host", 0, "app", "app").ca_file("/ca.pem"),
            ClickHouseConfig::new("host", 9440, "", "app").ca_file("/ca.pem"),
            ClickHouseConfig::new("host", 9440, "app", "").ca_file("/ca.pem"),
            ClickHouseConfig::new("host", 9440, "app", "app")
                .ca_file("/ca.pem")
                .request_timeout(Duration::ZERO),
            ClickHouseConfig::new("host", 9440, "app", "app")
                .ca_file("/ca.pem")
                .connect_timeout(Duration::ZERO),
        ] {
            assert!(config.validate().is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn roots_require_a_bounded_regular_pem_file() {
        let directory = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let certificate = params.self_signed(&ca_key).unwrap();
        let ca_path = directory.path().join("ca.pem");
        std::fs::write(&ca_path, certificate.pem()).unwrap();

        let valid =
            ClickHouseConfig::new("clickhouse.example", 9440, "app", "app").ca_file(&ca_path);
        assert!(valid.read_roots().await.is_ok());

        let missing = ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
            .ca_file(directory.path().join("missing.pem"));
        assert!(missing.read_roots().await.is_err());

        let not_a_file = ClickHouseConfig::new("clickhouse.example", 9440, "app", "app")
            .ca_file(directory.path());
        assert!(not_a_file.read_roots().await.is_err());

        let huge = directory.path().join("huge.pem");
        let file = std::fs::File::create(&huge).unwrap();
        file.set_len(super::MAX_CA_FILE_BYTES + 1).unwrap();
        let oversized =
            ClickHouseConfig::new("clickhouse.example", 9440, "app", "app").ca_file(&huge);
        assert!(oversized.read_roots().await.is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fifo_symlink_and_replacement_never_block_open() {
        use rustix::fs::{CWD, Mode, mkfifoat};
        use std::{os::unix::fs::symlink, sync::mpsc, time::Duration};

        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        let link = directory.path().join("link");
        symlink(&fifo, &link).unwrap();
        let replaced = directory.path().join("replaced");
        std::fs::write(&replaced, b"not a certificate").unwrap();
        assert!(std::fs::metadata(&replaced).unwrap().is_file());
        // Model replacement immediately after the old path-metadata check.
        std::fs::rename(&fifo, &replaced).unwrap();
        symlink(&replaced, &fifo).unwrap();

        // Reproduce the old metadata-then-blocking-open race and release its worker.
        let (done, result) = mpsc::channel();
        let target = replaced.clone();
        let old_open = std::thread::spawn(move || {
            let opened = std::fs::File::open(target);
            let _ = done.send(opened.is_ok());
        });
        assert!(result.recv_timeout(Duration::from_millis(100)).is_err());
        let writer = rustix::fs::open(
            &replaced,
            rustix::fs::OFlags::WRONLY | rustix::fs::OFlags::NONBLOCK,
            Mode::empty(),
        )
        .unwrap();
        assert!(result.recv_timeout(Duration::from_secs(1)).unwrap());
        old_open.join().unwrap();
        drop(writer);

        for path in [&replaced, &link, &fifo] {
            let path = path.to_owned();
            let (done, result) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                done.send(super::open_ca_file(&path).is_err()).unwrap();
            });
            assert!(
                result
                    .recv_timeout(Duration::from_secs(1))
                    .expect("FIFO open blocked")
            );
            thread.join().unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn fifo_ca_path_is_rejected_without_blocking_runtime() {
        use rustix::fs::{CWD, Mode, mkfifoat};
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        mkfifoat(CWD, &fifo, Mode::RUSR | Mode::WUSR).unwrap();
        let config = ClickHouseConfig::new("host", 9440, "app", "app").ca_file(&fifo);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), config.read_roots())
                .await
                .expect("FIFO root open blocked")
                .is_err()
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn roots_fail_closed_when_nonblocking_open_is_unavailable() {
        let error = super::open_ca_file(std::path::Path::new("unused")).unwrap_err();
        assert_eq!(error.kind(), neotales_dbapi::ErrorKind::Unsupported);
    }

    #[test]
    fn debug_redacts_every_identity_and_path() {
        let config = tls().password("super-secret");
        let debug = format!("{config:?}");
        for secret in [
            "clickhouse.example",
            "9440",
            "app",
            "super-secret",
            "/run/secrets",
        ] {
            assert!(!debug.contains(secret), "leaked {secret:?}: {debug}");
        }
    }
}
