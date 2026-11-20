//! Evidence for a future reusable pool, not an implementation of that pool.
//! Requires the disposable local PostgreSQL instance on port 55433.

use neotales_tokio_postgres::{Client, NoTls};

async fn client() -> (Client, tokio::task::JoinHandle<()>) {
    let address = std::net::Ipv4Addr::LOCALHOST;
    let url = format!("postgres://postgres@{address}:55433/postgres");
    let (client, mut connection) = neotales_tokio_postgres::connect(&url, NoTls)
        .await
        .expect("disposable test PostgreSQL required");
    let worker = tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if message.is_err() {
                break;
            }
        }
    });
    (client, worker)
}

#[tokio::test]
async fn reset_clears_session_state_after_explicit_rollback() {
    let (connection, worker) = client().await;
    let (observer, observer_worker) = client().await;
    let initial: String = connection
        .query_one("SHOW application_name", &[])
        .await
        .unwrap()
        .get(0);
    connection
        .batch_execute(
            "CREATE TEMP TABLE pool_reset_fixture (v bigint);
             SET application_name = 'pool_reset_fixture';
             SELECT pg_advisory_lock(483219821::bigint);
             BEGIN;
             INSERT INTO pool_reset_fixture VALUES (1);",
        )
        .await
        .unwrap();
    let available: bool = observer
        .query_one("SELECT pg_try_advisory_lock(483219821::bigint)", &[])
        .await
        .unwrap()
        .get(0);
    assert!(!available);

    // DISCARD ALL cannot run inside a transaction block: these must be separate.
    connection.batch_execute("ROLLBACK").await.unwrap();
    connection.batch_execute("DISCARD ALL").await.unwrap();
    let restored: String = connection
        .query_one("SHOW application_name", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(restored, initial);
    let gone: bool = connection
        .query_one(
            "SELECT to_regclass('pg_temp.pool_reset_fixture') IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(gone);
    let available: bool = observer
        .query_one("SELECT pg_try_advisory_lock(483219821::bigint)", &[])
        .await
        .unwrap()
        .get(0);
    assert!(available);

    drop(connection);
    drop(observer);
    worker.await.unwrap();
    observer_worker.await.unwrap();
}

#[tokio::test]
async fn clear_type_cache_alone_does_not_remove_lookup_statements() {
    let (connection, worker) = client().await;
    connection
        .batch_execute("CREATE TYPE pg_temp.cache_only_a AS ENUM ('a')")
        .await
        .unwrap();
    drop(
        connection
            .prepare("SELECT NULL::pg_temp.cache_only_a")
            .await
            .unwrap(),
    );
    connection.clear_type_cache();
    connection.batch_execute("DISCARD ALL").await.unwrap();
    connection
        .batch_execute("CREATE TYPE pg_temp.cache_only_b AS ENUM ('b')")
        .await
        .unwrap();
    let error = connection
        .prepare("SELECT NULL::pg_temp.cache_only_b")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "26000");
    drop(connection);
    worker.await.unwrap();
}

#[tokio::test]
async fn clear_cached_typeinfo_before_discard_allows_new_types() {
    let (mut connection, worker) = client().await;
    connection
        .batch_execute("CREATE TYPE pg_temp.pool_enum_a AS ENUM ('a')")
        .await
        .unwrap();
    drop(
        connection
            .prepare("SELECT NULL::pg_temp.pool_enum_a")
            .await
            .unwrap(),
    );
    connection.clear_cached_typeinfo();
    connection.batch_execute("DISCARD ALL").await.unwrap();
    connection
        .batch_execute("CREATE TYPE pg_temp.pool_enum_b AS ENUM ('b')")
        .await
        .unwrap();
    drop(
        connection
            .prepare("SELECT NULL::pg_temp.pool_enum_b")
            .await
            .unwrap(),
    );
    drop(connection);
    worker.await.unwrap();
}
