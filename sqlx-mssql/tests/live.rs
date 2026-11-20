//! Opt-in SQL Server integration test. The port is bound only on loopback.
use std::{collections::HashMap, time::Duration};

use bollard::models::PortBinding;
use futures_util::StreamExt;
use sqlx_core::{column::Column, connection::Connection, query::query, row::Row};
use sqlx_mssql_rs_core::{MssqlPoolOptions, MssqlResultEvent};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

#[tokio::test]
#[ignore = "requires Docker and the pinned SQL Server image"]
async fn boundaries_and_abandoned_stream_pool_reuse() {
    let password = format!("Tmp-{}-Az9!", uuid::Uuid::new_v4().simple());
    let container = GenericImage::new(
        "mcr.microsoft.com/mssql/server",
        "2022-latest@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f",
    )
    .with_exposed_port(1433.tcp())
    .with_wait_for(WaitFor::log(LogWaitStrategy::stdout_or_stderr(
        "SQL Server is now ready for client connections",
    )))
    .with_env_var("ACCEPT_EULA", "Y")
    .with_env_var("MSSQL_SA_PASSWORD", &password)
    .with_startup_timeout(Duration::from_secs(180))
    .with_host_config_modifier(|config| {
        config.publish_all_ports = Some(false);
        config.port_bindings = Some(HashMap::from([(
            "1433/tcp".to_owned(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_owned()),
                host_port: Some("0".to_owned()),
            }]),
        )]));
    })
    .start()
    .await
    .expect("start isolated SQL Server");
    let port = container.get_host_port_ipv4(1433.tcp()).await.unwrap();
    // trust_certificate is confined to this disposable loopback test server.
    let url = format!("mssql://sa:{password}@127.0.0.1:{port}/master?trust_certificate=true");
    // The image logs readiness during initial setup, before the final SA login is ready.
    let mut pool = None;
    for _ in 0..60 {
        if let Ok(connected) = MssqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
        {
            pool = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let pool = pool.expect("SQL Server did not become ready");
    let mut conn = pool.acquire().await.unwrap();
    let mut stream = conn.fetch_events(query(
        "SELECT CAST(1 AS int) AS a WHERE 1=0; SELECT CAST(2 AS int) AS b;",
    ));
    assert!(matches!(stream.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetStart(columns) if columns[0].name() == "a"));
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetEnd
    ));
    assert!(matches!(stream.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetStart(columns) if columns[0].name() == "b"));
    match stream.next().await.unwrap().unwrap() {
        MssqlResultEvent::Row(row) => assert_eq!(row.try_get::<i32, _>(0).unwrap(), 2),
        other => panic!("unexpected event: {other:?}"),
    }
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetEnd
    ));
    assert!(stream.next().await.is_none());
    drop(stream);
    let mut abandoned = conn.fetch_events(query("SELECT 1 AS a UNION ALL SELECT 2"));
    assert!(matches!(
        abandoned.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetStart(_)
    ));
    drop(abandoned);
    drop(conn);
    let mut conn = pool.acquire().await.unwrap();
    let row = query("SELECT CAST(@P1 AS int)")
        .bind(7_i32)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(row.try_get::<i32, _>(0).unwrap(), 7);

    // Interrupt submission while the server has not produced its first result.
    query("EXEC sys.sp_set_session_context N'cancel_test', 1")
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut waiting = conn.fetch_events(query("WAITFOR DELAY '00:00:05'; SELECT 1"));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), waiting.next())
            .await
            .is_err()
    );
    drop(waiting);
    assert!(query("SELECT 1").fetch_one(&mut *conn).await.is_err());
    drop(conn);
    let mut conn = pool.acquire().await.unwrap();
    let marker: i32 =
        query("SELECT CASE WHEN SESSION_CONTEXT(N'cancel_test') IS NULL THEN 0 ELSE 1 END")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
    assert_eq!(marker, 0, "cancelled submission must not return to pool");

    // A drain can itself be interrupted while waiting for a later result.
    query("EXEC sys.sp_set_session_context N'cancel_test', 1")
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut waiting = conn.fetch_events(query(
        "SELECT 1 AS a; RAISERROR(N'flush', 0, 1) WITH NOWAIT; WAITFOR DELAY '00:00:05'; SELECT 2 AS b"
    ));
    assert!(matches!(
        waiting.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetStart(_)
    ));
    drop(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), conn.flush())
            .await
            .is_err()
    );
    assert!(query("SELECT 1").fetch_one(&mut *conn).await.is_err());
    drop(conn);
    let mut conn = pool.acquire().await.unwrap();
    let marker: i32 =
        query("SELECT CASE WHEN SESSION_CONTEXT(N'cancel_test') IS NULL THEN 0 ELSE 1 END")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
    assert_eq!(marker, 0, "interrupted cleanup must not return to pool");

    query("EXEC sys.sp_set_session_context N'cancel_test', 1")
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut failing = conn.fetch_events(query(
        "SELECT 1 AS a; RAISERROR(N'failure', 16, 1) WITH NOWAIT",
    ));
    assert!(matches!(
        failing.next().await.unwrap().unwrap(),
        MssqlResultEvent::ResultSetStart(_)
    ));
    drop(failing);
    assert!(
        conn.flush().await.is_err(),
        "drain should report the server error"
    );
    assert!(query("SELECT 1").fetch_one(&mut *conn).await.is_err());
    drop(conn);
    let mut conn = pool.acquire().await.unwrap();
    let marker: i32 =
        query("SELECT CASE WHEN SESSION_CONTEXT(N'cancel_test') IS NULL THEN 0 ELSE 1 END")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
    assert_eq!(marker, 0, "failed cleanup must not return to pool");
    drop(conn);
    pool.close().await;
}
