//! Public configuration boundary tests do not require a server.
use std::{net::Ipv4Addr, time::Duration};

use neotales_dbapi_postgres::experimental::{Config, Error, Pool, Profile};

fn profile() -> Profile {
    Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap()
}

fn config(host: &str) -> Config {
    let mut config = Config::new();
    config.host(host);
    config
}

fn invalid(config: Config) {
    assert!(matches!(
        Pool::loopback_plaintext_for_tests(config, 1, profile()),
        Err(Error::InvalidPolicy)
    ));
}

#[test]
fn finite_integral_millisecond_policy_is_required() {
    let valid = Duration::from_secs(1);
    for duration in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_micros(1500),
        Duration::from_millis(u64::try_from(i32::MAX).unwrap() + 1),
        Duration::MAX,
    ] {
        assert_eq!(
            Profile::normal(duration, valid).unwrap_err(),
            Error::InvalidPolicy
        );
        assert_eq!(
            Profile::normal(valid, duration).unwrap_err(),
            Error::InvalidPolicy
        );
    }
    let maximum = Duration::from_millis(u64::try_from(i32::MAX).unwrap());
    assert!(Profile::normal(maximum, maximum).is_ok());
    assert_eq!(
        Profile::migration(profile(), Duration::from_secs(5), valid).unwrap_err(),
        Error::InvalidPolicy
    );
    assert!(Profile::migration(profile(), Duration::from_secs(6), valid).is_ok());
}

#[test]
fn insecure_transport_rejects_nonloopback_and_ambiguous_hosts_before_io() {
    invalid(Config::new());
    for host in ["", "localhost", "192.0.2.1", "::", "/tmp/postgres"] {
        invalid(config(host));
    }
    let mut mixed = config("127.0.0.1");
    mixed.host("192.0.2.1");
    invalid(mixed);
    let mut address = config("127.0.0.1");
    address.hostaddr(Ipv4Addr::new(192, 0, 2, 1).into());
    invalid(address);
    let mut mismatched = config("127.0.0.1");
    mismatched.host("::1").hostaddr(Ipv4Addr::LOCALHOST.into());
    invalid(mismatched);
    let mut ports = config("127.0.0.1");
    ports.port(5432).port(5433);
    invalid(ports);
    let mut zero_port = config("127.0.0.1");
    zero_port.port(0);
    invalid(zero_port);
    let mut direct = config("127.0.0.1");
    direct.ssl_negotiation(neotales_tokio_postgres::config::SslNegotiation::Direct);
    invalid(direct);
    let mut binding = config("127.0.0.1");
    binding.channel_binding(neotales_tokio_postgres::config::ChannelBinding::Require);
    // Required channel binding needs TLS. The loopback plaintext test
    // transport cannot provide it; the verified transport accepts it.
    invalid(binding);
    for host in ["127.0.0.1", "::1"] {
        let accepted = Pool::loopback_plaintext_for_tests(config(host), 1, profile()).unwrap();
        assert_eq!(accepted.free_slots(), 1);
        // Construction is lazy and works without a runtime or a live server.
        accepted.close();
        accepted.close();
    }
}
