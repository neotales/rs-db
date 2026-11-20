//! Live SCRAM channel-binding tests.
//!
//! `tls/tests/live.sh` starts two disposable PostgreSQL 17 servers with
//! synthetic certificates and a synthetic SCRAM role, then runs this target:
//!
//! - `127.0.0.1:55433` uses an RSA certificate signed with SHA-256.
//! - `127.0.0.1:55434` uses a self-signed Ed25519 certificate, which has no
//!   signature digest usable for RFC 5929 `tls-server-end-point`.
//!
//! The role password is generated per run and passed in the environment. It is
//! disposable, is never printed, and does not exist outside the container.
//!
//! A successful `channel_binding=require` connection is positive proof that
//! `SCRAM-SHA-256-PLUS` was used: the driver refuses every other mechanism when
//! binding is required, and PostgreSQL rejects a wrong binding with SQLSTATE
//! 28000. The tests that override the binding use the adapter's real verified
//! TLS connector, so certificate and hostname verification stay unchanged.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use neotales_dbapi_postgres_tls::{
    Error as TlsError, ErrorKind, HostTls, VerifiedStream, VerifiedTls, connect,
};
use neotales_tokio_postgres::{
    Config,
    config::{ChannelBinding, SslMode},
    tls::{ChannelBinding as StreamBinding, MakeTlsConnect, TlsConnect, TlsStream},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const PRIMARY_PORT: u16 = 55433;
const ED25519_PORT: u16 = 55434;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set by tls/tests/live.sh"))
}

fn root(name: &str) -> Vec<u8> {
    let path = env(name);
    std::fs::read(&path).unwrap_or_else(|_| panic!("{name} must name a readable DER certificate"))
}

fn config(user: &str, password: Option<&str>, port: u16) -> Config {
    let mut config = Config::new();
    config
        .host("localhost")
        .hostaddr(std::net::Ipv4Addr::LOCALHOST.into())
        .port(port)
        .user(user)
        .dbname("postgres")
        // The adapter must override this to a verified TLS connection.
        .ssl_mode(SslMode::Disable);
    if let Some(password) = password {
        config.password(password);
    }
    config
}

fn scram_config(port: u16) -> Config {
    config(
        &env("DBAPI_TLS_TEST_SCRAM_USER"),
        Some(&env("DBAPI_TLS_TEST_SCRAM_PASSWORD")),
        port,
    )
}

async fn connect_and_check(cfg: &Config, tls: VerifiedTls) {
    let (client, connection, cancel) = connect(cfg, tls).await.expect("verified TLS connection");
    let worker = tokio::spawn(connection);
    let row = client
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("pg_stat_ssl query");
    assert!(row.get::<_, bool>(0));
    drop(cancel);
    drop(client);
    worker
        .await
        .expect("connection worker")
        .expect("clean close");
}

#[tokio::test]
async fn require_authenticates_with_scram_sha256_plus() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    let mut cfg = scram_config(PRIMARY_PORT);
    cfg.channel_binding(ChannelBinding::Require);
    // The only mechanism the driver accepts under `require` is
    // SCRAM-SHA-256-PLUS, and the server validates the certificate binding.
    connect_and_check(&cfg, tls).await;
}

#[tokio::test]
async fn prefer_uses_an_available_binding() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    let mut cfg = scram_config(PRIMARY_PORT);
    cfg.channel_binding(ChannelBinding::Prefer);
    connect_and_check(&cfg, tls).await;
}

#[tokio::test]
async fn disable_ignores_the_binding() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    let mut cfg = scram_config(PRIMARY_PORT);
    cfg.channel_binding(ChannelBinding::Disable);
    connect_and_check(&cfg, tls).await;
}

#[tokio::test]
async fn require_without_scram_authentication_fails() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    // The fixture authenticates `postgres` with trust, so no SASL exchange can
    // carry the required binding.
    let mut cfg = config("postgres", None, PRIMARY_PORT);
    cfg.channel_binding(ChannelBinding::Require);
    let error = connect(&cfg, tls).await.err().expect("require must fail");
    assert_eq!(error.kind(), ErrorKind::Connection);
}

#[tokio::test]
async fn unsupported_digest_require_fails_explicitly() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_ED25519_CA_DER")]).unwrap();
    let mut cfg = scram_config(ED25519_PORT);
    cfg.channel_binding(ChannelBinding::Require);
    let error = connect(&cfg, tls).await.err().expect("require must fail");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[tokio::test]
async fn unsupported_digest_prefer_falls_back_to_scram_sha256() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_ED25519_CA_DER")]).unwrap();
    let mut cfg = scram_config(ED25519_PORT);
    cfg.channel_binding(ChannelBinding::Prefer);
    connect_and_check(&cfg, tls).await;
}

#[tokio::test]
async fn wrong_binding_is_rejected_when_the_client_uses_it() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    for mode in [ChannelBinding::Prefer, ChannelBinding::Require] {
        let mut cfg = scram_config(PRIMARY_PORT);
        // This path bypasses the adapter's `connect`, which normally forces
        // TLS. Force it here so the test cannot fall back to plaintext.
        cfg.ssl_mode(SslMode::Require).channel_binding(mode);
        let error = cfg
            .connect(WrongBinding(tls.clone()))
            .await
            .err()
            .expect("server must reject a wrong channel binding");
        let db = error.as_db_error().expect("server error");
        assert_eq!(db.code().code(), "28000");
        assert_eq!(db.message(), "SCRAM channel binding check failed");
    }
}

#[tokio::test]
async fn disable_does_not_send_a_wrong_binding() {
    let tls = VerifiedTls::new(&[&root("DBAPI_TLS_TEST_CA_DER")]).unwrap();
    let mut cfg = scram_config(PRIMARY_PORT);
    cfg.ssl_mode(SslMode::Require)
        .channel_binding(ChannelBinding::Disable);
    let (client, connection) = cfg
        .connect(WrongBinding(tls))
        .await
        .expect("disable must ignore the binding, even a wrong one");
    let worker = tokio::spawn(connection);
    drop(client);
    worker
        .await
        .expect("connection worker")
        .expect("clean close");
}

/// Uses the adapter's verified TLS handshake but reports an intentionally wrong
/// `tls-server-end-point` value to the driver.
struct WrongBinding(VerifiedTls);

impl<S> MakeTlsConnect<S> for WrongBinding
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = WrongBindingStream<VerifiedStream<S>>;
    type TlsConnect = WrongBindingConnect;
    type Error = TlsError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        Ok(WrongBindingConnect(
            <VerifiedTls as MakeTlsConnect<S>>::make_tls_connect(&mut self.0, domain)?,
        ))
    }
}

struct WrongBindingConnect(HostTls);

impl<S> TlsConnect<S> for WrongBindingConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = WrongBindingStream<VerifiedStream<S>>;
    type Error = TlsError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let inner = self.0.connect(stream).await?;
            Ok(WrongBindingStream(inner))
        })
    }
}

struct WrongBindingStream<S>(S);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WrongBindingStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WrongBindingStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for WrongBindingStream<S> {
    fn channel_binding(&self) -> StreamBinding {
        StreamBinding::tls_server_end_point(vec![0x5a; 32])
    }
}
