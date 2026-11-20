//! Public normal dependencies must not silently retain `SQLx` or its logging path.
#[test]
fn public_provider_uses_only_the_quiet_verified_transport() {
    let output = std::process::Command::new(env!("CARGO"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "tree",
            "-p",
            "neotales-dbapi-postgres",
            "--no-default-features",
            "-e",
            "normal",
        ])
        .output()
        .expect("cargo tree must run");
    assert!(output.status.success());
    let graph = String::from_utf8(output.stdout).unwrap();
    assert!(!graph.contains("sqlx"));
    assert!(graph.contains("neotales-tokio-postgres v"));
    assert!(graph.contains("neotales-dbapi-postgres-tls v"));
}
