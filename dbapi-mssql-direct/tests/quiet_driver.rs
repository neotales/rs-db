//! No per-future dispatcher, no global filtering, and `tracing/log-always`.
#[path = "support/capture.rs"]
mod capture;

use mssql_tds::{
    connection::client_context::ClientContext,
    test_client_support::{done_no_more, sql_error, tds_client_from_tokens},
};
use neotales_dbapi_mssql_direct::{Error, Probe};

#[tokio::test]
async fn raw_driver_and_connection_failure_are_quiet_under_both_global_channels() {
    capture::install();
    mssql_tds::quiet_arguments_regression_probe();
    tokio::spawn(async { tracing::info!("unrelated_task_marker") })
        .await
        .unwrap();
    let mut client = tds_client_from_tokens(vec![
        sql_error(50000, 16, "SERVER_ERROR_SENTINEL"),
        done_no_more(),
    ]);
    assert!(
        client
            .execute("SELECT 'SQL_SENTINEL'".into(), ())
            .await
            .is_err()
    );
    drop(client);
    let mut success = tds_client_from_tokens(vec![done_no_more()]);
    success
        .execute("SELECT 'SQL_SUCCESS_SENTINEL'".into(), ())
        .await
        .unwrap();
    drop(success);

    // Linux has an audited quiet transport and exercises post-connect
    // prelogin failure. Other targets must fail closed before opening a socket.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        unreachable!();
    };
    let mut context = ClientContext::default();
    context.user_name = "AUTH_NAME_SENTINEL".into();
    context.password = "AUTH_PASSWORD_SENTINEL".into();
    context.database = "AUTH_DATABASE_SENTINEL".into();

    #[cfg(target_os = "linux")]
    {
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        assert!(matches!(
            Probe::connect(context, address).await,
            Err(Error::Connect)
        ));
        peer.await.unwrap();
    }

    #[cfg(not(target_os = "linux"))]
    {
        assert!(matches!(
            Probe::connect(context, address).await,
            Err(Error::UnsafeTracing)
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "a non-Linux connection attempt must not reach the listener"
        );
    }
    capture::assert_application_task_visible();
    capture::assert_quiet(&[
        "SQL_SENTINEL",
        "SQL_SUCCESS_SENTINEL",
        "SERVER_ERROR_SENTINEL",
        "AUTH_NAME_SENTINEL",
        "AUTH_PASSWORD_SENTINEL",
        "AUTH_DATABASE_SENTINEL",
        "127.0.0.1",
        &address.port().to_string(),
    ]);
}
