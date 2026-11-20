//! Live checkout regressions using the underlying `SQLx` pool.

use super::{MySqlConfig, MySqlSession};
use neotales_dbapi::ErrorKind;
use sqlx::Connection as _;
use std::{
    fmt::Write as _,
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

const MAX_READINESS_ATTEMPTS: usize = 30;
// MySQL's temporary server and its X plugin both report generic readiness
// before the final server starts listening on the classic protocol port.
const MYSQL_FINAL_READY: &str = "port: 3306 ";

// Never render a SQLx error: its message can contain connection details.
fn readiness_category(error: &sqlx::Error) -> (&'static str, bool) {
    match error {
        sqlx::Error::Io(error) => match error.kind() {
            io::ErrorKind::ConnectionRefused => ("io_connection_refused", true),
            io::ErrorKind::ConnectionReset => ("io_connection_reset", true),
            io::ErrorKind::ConnectionAborted => ("io_connection_aborted", true),
            io::ErrorKind::UnexpectedEof => ("io_unexpected_eof", true),
            io::ErrorKind::TimedOut => ("io_timed_out", true),
            _ => ("io_other", false),
        },
        sqlx::Error::Database(_) => ("database", false),
        sqlx::Error::Tls(_) => ("tls", false),
        sqlx::Error::Protocol(_) => ("protocol", false),
        sqlx::Error::PoolTimedOut => ("pool_timed_out", false),
        _ => ("other", false),
    }
}

fn should_retry(attempt: usize, transient: bool) -> bool {
    transient && attempt < MAX_READINESS_ATTEMPTS
}

fn server_log_readiness(prefix: &str) -> LogWaitStrategy {
    if prefix == "MYSQL" {
        LogWaitStrategy::stdout_or_stderr(MYSQL_FINAL_READY)
    } else {
        LogWaitStrategy::stdout_or_stderr("ready for connections").with_times(2)
    }
}

async fn wait_for_readiness(config: &MySqlConfig, started: Instant) {
    // SQLx pool acquisition can hide individual errors behind PoolTimedOut.
    // This probe runs before the regression's SQL writes.
    for attempt in 1..=MAX_READINESS_ATTEMPTS {
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::mysql::MySqlConnection::connect_with(&config.connect_options()),
        )
        .await;
        let (category, transient) = match result {
            Ok(Ok(connection)) => {
                drop(connection);
                eprintln!(
                    "fixture stage=readiness_complete attempt={attempt} elapsed_ms={}",
                    started.elapsed().as_millis()
                );
                return;
            }
            Ok(Err(error)) => readiness_category(&error),
            Err(_) => ("connect_timeout", true),
        };
        eprintln!(
            "fixture stage=readiness_connect attempt={attempt} elapsed_ms={} category={category}",
            started.elapsed().as_millis()
        );
        assert!(
            should_retry(attempt, transient),
            "fixture stage=readiness_failed attempt={attempt} category={category}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn rejects_and_discards_unsafe_charset(image: &str, tag: &str, prefix: &str) {
    let started = Instant::now();
    let mut random = [0u8; 24];
    getrandom::fill(&mut random).expect("OS random source");
    let mut password = String::from("Nt-");
    for byte in random {
        write!(password, "{byte:02x}").unwrap();
    }
    let container = GenericImage::new(image, tag)
        .with_wait_for(WaitFor::log(server_log_readiness(prefix)))
        .with_env_var(format!("{prefix}_ROOT_PASSWORD"), &password)
        .with_env_var(format!("{prefix}_ROOT_HOST"), "%")
        .with_env_var(format!("{prefix}_DATABASE"), "neotales_test")
        .with_host_config_modifier(|config| {
            config.publish_all_ports = Some(false);
            config.port_bindings = Some(std::collections::HashMap::new());
        })
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .expect("disposable database startup");
    eprintln!(
        "fixture stage=container_ready elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let target = SocketAddr::new(container.get_bridge_ip_address().await.unwrap(), 3306);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            tokio::spawn(async move {
                match tokio::net::TcpStream::connect(target).await {
                    Ok(mut outbound) => {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                    Err(error) => {
                        let (category, _) = readiness_category(&sqlx::Error::Io(error));
                        eprintln!(
                            "fixture stage=bridge_connect elapsed_ms={} category={category}",
                            started.elapsed().as_millis()
                        );
                    }
                }
            });
        }
    });
    let config = MySqlConfig::plaintext_loopback("127.0.0.1", port, "root", "neotales_test")
        .unwrap()
        .password(password);
    wait_for_readiness(&config, started).await;
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(config.connect_options())
        .await
        .unwrap_or_else(|error| {
            let (category, _) = readiness_category(&error);
            panic!(
                "fixture stage=pool_connect_failed elapsed_ms={} category={category}",
                started.elapsed().as_millis()
            );
        });

    // Bypass the public API only in this private test, so checkout must detect
    // an already-contaminated pooled physical connection.
    for (variable, command) in [
        (
            "character_set_client",
            "SET SESSION character_set_client = gbk",
        ),
        (
            "character_set_connection",
            "SET SESSION character_set_connection = gbk",
        ),
        (
            "character_set_results",
            "SET SESSION character_set_results = gbk",
        ),
    ] {
        let mut raw = pool.acquire().await.unwrap();
        let old_id: u64 = sqlx::query_scalar("SELECT CONNECTION_ID()")
            .fetch_one(&mut *raw)
            .await
            .unwrap();
        sqlx::raw_sql(command).execute(&mut *raw).await.unwrap();
        drop(raw);

        let error = MySqlSession::acquire(&pool).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported, "{variable}");
        let mut replacement = pool.acquire().await.unwrap();
        let new_id: u64 = sqlx::query_scalar("SELECT CONNECTION_ID()")
            .fetch_one(&mut *replacement)
            .await
            .unwrap();
        assert_ne!(old_id, new_id, "{variable}: physical connection was reused");
    }
    pool.close().await;
    proxy.abort();
}

#[test]
fn readiness_retry_categories_are_explicit_and_bounded() {
    for kind in [
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::UnexpectedEof,
        io::ErrorKind::TimedOut,
    ] {
        let (_, transient) = readiness_category(&sqlx::Error::Io(io::Error::from(kind)));
        assert!(should_retry(1, transient));
        assert!(!should_retry(MAX_READINESS_ATTEMPTS, transient));
    }
    for error in [
        sqlx::Error::Io(io::Error::from(io::ErrorKind::PermissionDenied)),
        sqlx::Error::Protocol("sensitive".into()),
        sqlx::Error::Tls(Box::new(io::Error::other("sensitive"))),
        sqlx::Error::Configuration(Box::new(io::Error::other("sensitive"))),
        sqlx::Error::PoolTimedOut,
    ] {
        let (category, transient) = readiness_category(&error);
        assert!(!category.contains("sensitive"));
        assert!(!should_retry(1, transient));
    }
    assert!(should_retry(MAX_READINESS_ATTEMPTS - 1, true));
    assert!(!should_retry(MAX_READINESS_ATTEMPTS + 1, true));
}

#[test]
fn mysql_final_readiness_marker_excludes_temporary_and_x_plugin_ports() {
    assert!("ready for connections. port: 3306  MySQL".contains(MYSQL_FINAL_READY));
    assert!(!"ready for connections. port: 0  MySQL".contains(MYSQL_FINAL_READY));
    assert!(!"ready for connections. port: 33060 ".contains(MYSQL_FINAL_READY));
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn checkout_rejects_gbk_mysql() {
    rejects_and_discards_unsafe_charset(
        "mysql",
        "8.4@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902",
        "MYSQL",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires Docker; run tests/live.sh"]
async fn checkout_rejects_gbk_mariadb() {
    rejects_and_discards_unsafe_charset(
        "mariadb",
        "11.4@sha256:5e73baf8179b29f115101bb102763a47a017361f3e9792daccba6646c3ab1532",
        "MARIADB",
    )
    .await;
}
