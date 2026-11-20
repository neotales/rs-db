//! Separate socket coverage; the upstream socket test stays unchanged and ignored.

#[cfg(all(unix, feature = "runtime"))]
#[tokio::test]
async fn disposable_server_unix_socket() {
    let socket = std::env::var_os("CONFORMANCE_SOCKET_DIR")
        .expect("run this test through conformance/run.sh");
    let mut config = tokio_postgres::Config::new();
    config
        .host_path(socket)
        .port(5433)
        .user("postgres")
        .dbname("postgres");
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let worker = tokio::spawn(connection);
    assert_eq!(
        client
            .query_one("SELECT 1::int4", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(client);
    worker.await.unwrap().unwrap();
}
