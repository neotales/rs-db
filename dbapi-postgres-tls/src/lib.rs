//! Experimental TLS adapter for the isolated `neotales-tokio-postgres` fork.
//!
//! [`connect`] always requires TLS and verifies the server certificate chain and
//! hostname against explicitly supplied DER roots. Its `require` behavior is
//! deliberately stricter than libpq's ordinary `sslmode=require`: it always
//! verifies the hostname, equivalent to verify-full with those roots. The fork
//! does not parse `verify-full` in connection URLs; configure a `Config` and
//! use this function instead. Direct TLS negotiation and client certificates
//! are not supported. The public dbapi provider now uses this adapter through
//! the compiled quiet transport, with no TLS fallback.
//!
//! # SCRAM channel binding
//!
//! The adapter returns RFC 5929 `tls-server-end-point` data computed from the
//! verified peer certificate. The hash is the certificate's signature digest;
//! MD5 and SHA-1 are replaced with SHA-256 as the RFC requires. The driver uses
//! `SCRAM-SHA-256-PLUS` when the server offers it and `channel_binding` is not
//! `disable`.
//!
//! A certificate whose signature algorithm has no usable digest (for example
//! Ed25519, or RSA-PSS whose parameters the safe OpenSSL binding does not
//! expose) produces no binding. In that case `require` fails with
//! [`ErrorKind::Unsupported`] instead of claiming a binding, while `prefer`
//! falls back to plain `SCRAM-SHA-256`. The binding is derived from the same
//! certificate that passed chain and hostname verification; verification is
//! never weakened to obtain it.
//!
//! Errors returned here contain only a stable category. Do not log the underlying
//! driver's errors from other APIs; they can contain server-provided data.
//!
//! The adapter has **no full handshake deadline**. The caller must bound the
//! entire `connect` future, as well as application operations, and own the
//! connection task's cleanup. Cancelling a query future does not prove the server
//! stopped it; closing the connection below is conservative, not an outcome proof.
//!
//! ```no_run
//! use std::{net::Ipv4Addr, time::Duration};
//! use neotales_dbapi_postgres_tls::{connect, VerifiedTls};
//! use neotales_tokio_postgres::Config;
//!
//! async fn example(root_der: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
//!     let tls = VerifiedTls::new(&[root_der])?;
//!     let mut config = Config::new();
//!     config.host("localhost").hostaddr(Ipv4Addr::LOCALHOST.into())
//!         .user("app").dbname("app");
//!     // Covers DNS/TCP, PostgreSQL SSL negotiation, TLS and startup.
//!     let (client, connection, cancel) =
//!         tokio::time::timeout(Duration::from_secs(5), connect(&config, tls)).await??;
//!     let worker = tokio::spawn(connection);
//!     let result = tokio::time::timeout(
//!         Duration::from_secs(2), client.simple_query("SELECT 1")
//!     ).await;
//!     // Never log raw driver errors. Close even after a timeout or query error.
//!     let succeeded = matches!(result, Ok(Ok(_)));
//!     drop(cancel);
//!     drop(client);
//!     worker.abort();
//!     let _ = worker.await;
//!     if succeeded { Ok(()) } else { Err("query failed or timed out".into()) }
//! }
//! ```

#![forbid(unsafe_code)]

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use neotales_tokio_postgres::{
    self as postgres, Socket,
    tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream},
};
use openssl::{
    hash::MessageDigest,
    nid::Nid,
    ssl::{Ssl, SslConnector, SslMethod, SslVerifyMode, SslVersion},
    x509::{X509, X509Ref, store::X509StoreBuilder},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Safe, coarse-grained failure categories; no original error is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Direct TLS negotiation is unsupported, or a required channel binding
    /// cannot be computed from the peer certificate's signature algorithm.
    Unsupported,
    /// Trust anchors are empty, malformed, or could not be installed.
    InvalidRoots,
    /// The TLS handshake or certificate verification was rejected.
    TlsRejected,
    /// A transport or operating-system I/O operation failed.
    Io,
    /// A driver, server, authentication, or other failure could not be classified safely.
    Connection,
}

/// A sanitized failure with no underlying error or source chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Error(ErrorKind);

impl Error {
    /// Returns the safe category.
    #[must_use]
    pub fn kind(self) -> ErrorKind {
        self.0
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL TLS operation failed ({:?})", self.0)
    }
}

impl std::error::Error for Error {}

fn driver_error(err: &postgres::Error) -> Error {
    use std::error::Error as _;
    if let Some(safe) = err
        .source()
        .and_then(|source| source.downcast_ref::<Error>())
    {
        return *safe;
    }
    if err.is_tls_error() {
        Error(ErrorKind::TlsRejected)
    } else if err.is_io_error() {
        Error(ErrorKind::Io)
    } else {
        Error(ErrorKind::Connection)
    }
}

/// Connector with an explicit, immutable trust store. It does not log certificate data.
#[derive(Clone)]
pub struct VerifiedTls {
    connector: Arc<SslConnector>,
    /// Set by [`connect`] when the caller requested `channel_binding=require`.
    require_channel_binding: bool,
}

impl fmt::Debug for VerifiedTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedTls(..)")
    }
}

impl VerifiedTls {
    /// Builds a connector using only the supplied DER-encoded trust anchors.
    ///
    /// # Errors
    /// Returns `InvalidRoots` if the roots are empty, malformed, or cannot be installed.
    pub fn new(roots: &[&[u8]]) -> Result<Self, Error> {
        if roots.is_empty() {
            return Err(Error(ErrorKind::InvalidRoots));
        }
        let mut store = X509StoreBuilder::new().map_err(|_| Error(ErrorKind::InvalidRoots))?;
        for der in roots {
            store
                .add_cert(X509::from_der(der).map_err(|_| Error(ErrorKind::InvalidRoots))?)
                .map_err(|_| Error(ErrorKind::InvalidRoots))?;
        }
        let mut builder = SslConnector::builder(SslMethod::tls_client())
            .map_err(|_| Error(ErrorKind::Connection))?;
        builder.set_cert_store(store.build());
        builder.set_verify(SslVerifyMode::PEER);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|_| Error(ErrorKind::Connection))?;
        Ok(Self {
            connector: Arc::new(builder.build()),
            require_channel_binding: false,
        })
    }
}

/// A validated per-host connector, created by the fork.
pub struct HostTls {
    ssl: Ssl,
    require_channel_binding: bool,
}

impl fmt::Debug for HostTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HostTls(..)")
    }
}

impl<S> MakeTlsConnect<S> for VerifiedTls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = VerifiedStream<S>;
    type TlsConnect = HostTls;
    type Error = Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Error> {
        if domain.is_empty() {
            return Err(Error(ErrorKind::TlsRejected));
        }
        let ssl = self
            .connector
            .configure()
            .map_err(|_| Error(ErrorKind::TlsRejected))?
            .into_ssl(domain)
            .map_err(|_| Error(ErrorKind::TlsRejected))?;
        Ok(HostTls {
            ssl,
            require_channel_binding: self.require_channel_binding,
        })
    }
}

impl<S> TlsConnect<S> for HostTls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = VerifiedStream<S>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let HostTls {
                ssl,
                require_channel_binding,
            } = self;
            let mut stream = tokio_openssl::SslStream::new(ssl, stream)
                .map_err(|_| Error(ErrorKind::TlsRejected))?;
            Pin::new(&mut stream).connect().await.map_err(|err| {
                if err.io_error().is_some() {
                    Error(ErrorKind::Io)
                } else {
                    Error(ErrorKind::TlsRejected)
                }
            })?;
            // The peer certificate has now passed chain and hostname
            // verification. Compute the RFC 5929 binding from exactly that
            // certificate. A missing digest is never reported as a binding.
            let channel_binding = tls_server_end_point(&stream);
            if require_channel_binding && channel_binding.is_none() {
                return Err(Error(ErrorKind::Unsupported));
            }
            Ok(VerifiedStream {
                stream,
                channel_binding,
            })
        })
    }
}

/// A validated TLS stream. Its internals and peer certificate are not exposed.
pub struct VerifiedStream<S> {
    stream: tokio_openssl::SslStream<S>,
    channel_binding: Option<Vec<u8>>,
}

impl<S> fmt::Debug for VerifiedStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedStream(..)")
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for VerifiedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for VerifiedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for VerifiedStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        match &self.channel_binding {
            Some(value) => ChannelBinding::tls_server_end_point(value.clone()),
            None => ChannelBinding::none(),
        }
    }
}

/// Returns the peer certificate's verified `tls-server-end-point` binding, if
/// its signature algorithm has a usable digest.
fn tls_server_end_point<S>(stream: &tokio_openssl::SslStream<S>) -> Option<Vec<u8>> {
    let certificate = stream.ssl().peer_certificate()?;
    certificate_channel_binding(&certificate)
}

/// Computes the RFC 5929 `tls-server-end-point` value for `certificate`.
///
/// The hash is the certificate's signature digest, except MD5 and SHA-1, which
/// are replaced with SHA-256. `None` means the algorithm has no digest that
/// this safe OpenSSL binding can read, such as Ed25519 or RSA-PSS.
fn certificate_channel_binding(certificate: &X509Ref) -> Option<Vec<u8>> {
    let signature = certificate.signature_algorithm().object().nid();
    let digest_nid = signature.signature_algorithms()?.digest;
    let digest = signature_digest(digest_nid)?;
    certificate.digest(digest).ok().map(|bytes| bytes.to_vec())
}

/// Maps a certificate signature digest to the hash RFC 5929 requires.
///
/// MD5 and SHA-1 are replaced with SHA-256. `None` means the signature
/// algorithm carries no fixed digest (for example Ed25519 or RSA-PSS), so no
/// interoperable binding can be derived from it.
fn signature_digest(digest_nid: Nid) -> Option<MessageDigest> {
    if digest_nid == Nid::MD5 || digest_nid == Nid::SHA1 {
        Some(MessageDigest::sha256())
    } else if digest_nid == Nid::UNDEF {
        None
    } else {
        MessageDigest::from_nid(digest_nid)
    }
}

/// Opens a connection with TLS required, regardless of the supplied config's mode.
/// The returned connection must be polled by its owner.
///
/// `channel_binding=require` is enforced after the TLS handshake: if the peer
/// certificate has no usable signature digest, this returns
/// [`ErrorKind::Unsupported`] instead of proceeding without a binding.
///
/// # Errors
/// Returns a sanitized category for an unsupported option or failed connection.
pub async fn connect(
    config: &postgres::Config,
    mut tls: VerifiedTls,
) -> Result<
    (
        postgres::Client,
        postgres::Connection<Socket, VerifiedStream<Socket>>,
        VerifiedCancel,
    ),
    Error,
> {
    if config.get_ssl_negotiation() != postgres::config::SslNegotiation::Postgres {
        return Err(Error(ErrorKind::Unsupported));
    }
    tls.require_channel_binding =
        config.get_channel_binding() == postgres::config::ChannelBinding::Require;
    let mut config = config.clone();
    config.ssl_mode(postgres::config::SslMode::Require);
    let (client, connection) = config
        .connect(tls.clone())
        .await
        .map_err(|err| driver_error(&err))?;
    let cancel = VerifiedCancel {
        token: client.cancel_token(),
        tls,
    };
    Ok((client, connection, cancel))
}

/// Cancellation capability created only from a required-TLS connection.
pub struct VerifiedCancel {
    token: postgres::CancelToken,
    tls: VerifiedTls,
}

impl fmt::Debug for VerifiedCancel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("VerifiedCancel(..)")
    }
}

impl VerifiedCancel {
    /// Sends a cancellation request over a new verified TLS connection.
    /// Cancellation is racy; success does not confirm the query was cancelled.
    ///
    /// # Errors
    /// Returns a sanitized category if the cancellation connection or request fails.
    pub async fn cancel(&self) -> Result<(), Error> {
        self.token
            .cancel_query(self.tls.clone())
            .await
            .map_err(|err| driver_error(&err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::{
        asn1::Asn1Time,
        bn::BigNum,
        pkey::PKey,
        rsa::Rsa,
        x509::{X509Builder, X509NameBuilder},
    };

    fn self_signed(key: &PKey<openssl::pkey::Private>, digest: MessageDigest) -> X509 {
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "plab channel binding fixture")
            .unwrap();
        let name = name.build();
        let mut builder = X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(key).unwrap();
        let serial = BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
        builder.set_serial_number(&serial).unwrap();
        builder
            .set_not_before(Asn1Time::days_from_now(0).unwrap().as_ref())
            .unwrap();
        builder
            .set_not_after(Asn1Time::days_from_now(1).unwrap().as_ref())
            .unwrap();
        builder.sign(key, digest).unwrap();
        builder.build()
    }

    #[test]
    fn sha256_signature_uses_the_certificate_sha256_digest() {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let certificate = self_signed(&key, MessageDigest::sha256());
        assert_eq!(
            certificate.signature_algorithm().object().nid(),
            Nid::SHA256WITHRSAENCRYPTION
        );
        let expected = certificate.digest(MessageDigest::sha256()).unwrap();
        let binding = certificate_channel_binding(&certificate).unwrap();
        assert_eq!(binding, expected.to_vec());
        assert_eq!(binding.len(), 32);
    }

    #[test]
    fn signature_digest_follows_rfc_5929() {
        assert!(signature_digest(Nid::SHA256) == Some(MessageDigest::sha256()));
        assert!(signature_digest(Nid::SHA384) == Some(MessageDigest::sha384()));
        assert!(signature_digest(Nid::SHA512) == Some(MessageDigest::sha512()));
        // RFC 5929 section 4.1 replaces these weak digests with SHA-256.
        assert!(signature_digest(Nid::SHA1) == Some(MessageDigest::sha256()));
        assert!(signature_digest(Nid::MD5) == Some(MessageDigest::sha256()));
        // No fixed digest in the signature algorithm.
        assert!(signature_digest(Nid::UNDEF).is_none());
        assert!(signature_digest(Nid::from_raw(0)).is_none());
    }

    #[test]
    fn sha1_signed_certificate_is_hashed_with_sha256() {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let certificate = self_signed(&key, MessageDigest::sha1());
        assert_eq!(
            certificate.signature_algorithm().object().nid(),
            Nid::SHA1WITHRSAENCRYPTION
        );
        let expected = certificate.digest(MessageDigest::sha256()).unwrap();
        assert_eq!(
            certificate_channel_binding(&certificate),
            Some(expected.to_vec())
        );
    }

    #[test]
    fn ed25519_certificate_has_no_channel_binding() {
        let key = PKey::generate_ed25519().unwrap();
        // Ed25519 is a one-shot signature; OpenSSL signs with a null digest.
        let certificate = self_signed(&key, MessageDigest::null());
        assert_eq!(certificate_channel_binding(&certificate), None);
    }
}
