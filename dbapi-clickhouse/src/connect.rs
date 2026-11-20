//! Native-protocol connection establishment, address verification, and server-version gate.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use klickhouse::{Client, ClientOptions, IndexMap};
use neotales_dbapi::{DbError, ErrorKind, RowLimit};
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};

use crate::{collect, config::ClickHouseConfig};

/// The only ClickHouse server minor version this provider is tested against.
const REQUIRED_MAJOR: u64 = 24;
const REQUIRED_MINOR: u64 = 8;

fn connection() -> DbError {
    DbError::new(ErrorKind::Connection)
}

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

/// Opens one exclusive native connection and verifies the server version.
pub(crate) async fn connect(config: &ClickHouseConfig) -> Result<Client, DbError> {
    let deadline = Instant::now()
        .checked_add(config.configured_connect_timeout())
        .ok_or_else(connection)?;
    let client = tokio::time::timeout(remaining(deadline), connect_native(config, deadline))
        .await
        .map_err(|_| connection())??;
    verify_version(&client, remaining(deadline)).await?;
    Ok(client)
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn connect_native(config: &ClickHouseConfig, deadline: Instant) -> Result<Client, DbError> {
    let options = client_options(config);
    if config.is_plaintext() {
        connect_plaintext(config, options, deadline).await
    } else {
        connect_tls(config, options, deadline).await
    }
}

fn client_options(config: &ClickHouseConfig) -> ClientOptions {
    let mut settings = IndexMap::new();
    // The vendored decoder rejects a block above 4096 rows or 16,384 cells. With at most 64
    // columns, 256 rows is the largest block that always fits the cell ceiling. Setting it on
    // the connection keeps both bound and unbound queries inside the decoder's contract.
    settings.insert("max_block_size".to_owned(), "256".to_owned());
    // INSERT SETTINGS clauses are rejected by the no-count path. Explicitly override
    // user/server defaults so EndOfStream cannot merely acknowledge async enqueue.
    settings.insert("async_insert".to_owned(), "0".to_owned());
    ClientOptions {
        username: config.configured_user().to_owned(),
        password: config.configured_password().unwrap_or_default().to_owned(),
        default_database: config.configured_database().to_owned(),
        tcp_nodelay: true,
        settings,
    }
}

async fn connect_plaintext(
    config: &ClickHouseConfig,
    options: ClientOptions,
    deadline: Instant,
) -> Result<Client, DbError> {
    let addresses = tokio::time::timeout(
        remaining(deadline),
        tokio::net::lookup_host((config.configured_host(), config.configured_port())),
    )
    .await
    .map_err(|_| connection())?
    .map_err(|_| connection())?
    .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(connection());
    }
    let mut last = connection();
    for address in addresses {
        if !address.ip().is_loopback() {
            continue;
        }
        match connect_plaintext_address(address, &options, deadline).await {
            Ok(client) => return Ok(client),
            Err(error) => last = error,
        }
    }
    Err(last)
}

async fn connect_plaintext_address(
    address: SocketAddr,
    options: &ClientOptions,
    deadline: Instant,
) -> Result<Client, DbError> {
    let stream = tokio::time::timeout(remaining(deadline), TcpStream::connect(address))
        .await
        .map_err(|_| connection())?
        .map_err(|_| connection())?;
    // The config only admits numeric loopback, but verify the actual peer as well so a proxy
    // or unusual resolver cannot turn this into a non-loopback cleartext connection.
    if !stream
        .peer_addr()
        .is_ok_and(|peer| peer == address && peer.ip().is_loopback())
    {
        return Err(invalid());
    }
    stream.set_nodelay(true).map_err(|_| connection())?;
    let (read, write) = stream.into_split();
    // `connect_stream` also performs the native hello and the initial `SET` handshake. Bound
    // that whole exchange, not just the TCP connect, so a silent peer cannot hang the caller.
    tokio::time::timeout(
        remaining(deadline),
        Client::connect_stream(read, write, options.clone()),
    )
    .await
    .map_err(|_| connection())?
    .map_err(|_| connection())
}

async fn connect_tls(
    config: &ClickHouseConfig,
    options: ClientOptions,
    deadline: Instant,
) -> Result<Client, DbError> {
    let roots = tokio::time::timeout(remaining(deadline), config.read_roots())
        .await
        .map_err(|_| connection())??;
    let name = config.configured_server_name().to_owned();
    let name = ServerName::try_from(name).map_err(|_| invalid())?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| connection())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls));
    let stream = tokio::time::timeout(
        remaining(deadline),
        TcpStream::connect((config.configured_host(), config.configured_port())),
    )
    .await
    .map_err(|_| connection())?
    .map_err(|_| connection())?;
    stream.set_nodelay(true).map_err(|_| connection())?;
    let stream = tokio::time::timeout(remaining(deadline), connector.connect(name, stream))
        .await
        .map_err(|_| connection())?
        .map_err(|_| connection())?;
    let (read, write) = tokio::io::split(stream);
    // Bound the native hello and initial `SET` exchange after the TLS handshake as well.
    tokio::time::timeout(
        remaining(deadline),
        Client::connect_stream(read, write, options),
    )
    .await
    .map_err(|_| connection())?
    .map_err(|_| connection())
}

async fn verify_version(client: &Client, timeout: Duration) -> Result<(), DbError> {
    let limit = RowLimit::new(1, 256).map_err(|_| connection())?;
    let work = async {
        let events = client
            .query_events("SELECT version() AS v", None)
            .await
            .map_err(|_| connection())?;
        Ok::<_, DbError>(collect::collect(events, limit).await)
    };
    let collected = tokio::time::timeout(timeout, work)
        .await
        .map_err(|_| connection())??;
    if !collected.connection_clean {
        return Err(connection());
    }
    let rows = collected.rows.map_err(|_| connection())?;
    let version = rows
        .first()
        .ok_or_else(connection)?
        .get::<Vec<u8>>("v")
        .map_err(|_| connection())?;
    let (major, minor) = parse_server_version(&version).ok_or_else(connection)?;
    if (major, minor) != (REQUIRED_MAJOR, REQUIRED_MINOR) {
        return Err(unsupported());
    }
    Ok(())
}

/// Parses the leading `major.minor` components of a ClickHouse version string.
fn parse_server_version(version: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(version).ok()?.trim();
    let mut parts = text.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some_and(str::is_empty) {
        return None;
    }
    Some((major, minor))
}

#[cfg(test)]
mod tests {
    use super::{client_options, parse_server_version};
    use crate::ClickHouseConfig;

    #[test]
    fn connection_bounds_the_server_block_size() {
        let config =
            ClickHouseConfig::plaintext_loopback("127.0.0.1", 9000, "default", "default").unwrap();
        let options = client_options(&config);
        assert_eq!(
            options.settings.get("max_block_size").map(String::as_str),
            Some("256")
        );
    }

    #[test]
    fn the_version_gate_accepts_only_24_8() {
        for accepted in ["24.8", "24.8.1.1", "24.8.14.39"] {
            assert_eq!(parse_server_version(accepted.as_bytes()), Some((24, 8)));
        }
        for rejected in [
            "24.9.1.1", "25.3.1.1", "23.8.1.1", "24", "24.x.1", "", "24..1",
        ] {
            assert_ne!(
                parse_server_version(rejected.as_bytes()),
                Some((24, 8)),
                "{rejected:?}"
            );
        }
    }
}
