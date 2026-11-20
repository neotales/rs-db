//! Public and experimental unknown-write outcomes confirmed by an independent observer.
//! Requires disposable PostgreSQL on port 55433.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use neotales_dbapi::{ErrorKind, Pool, QueryOptions, RowLimit};
use neotales_dbapi_postgres::PostgresPool;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
#[allow(clippy::too_many_lines)] // The proxy and observer must share this controlled sequence.
async fn public_execute_marks_committed_write_unknown_after_lost_completion() {
    unconfirmed_write(Provider::PublicExecute).await;
}

#[tokio::test]
async fn public_fetch_marks_committed_write_unknown_after_lost_completion() {
    unconfirmed_write(Provider::PublicFetch).await;
}

#[tokio::test]
async fn experimental_transport_marks_unconfirmed_committed_write_unknown() {
    unconfirmed_write(Provider::Raw).await;
}

#[tokio::test]
async fn typed_fetch_requires_ready_for_query_even_after_rows_and_command_complete() {
    unconfirmed_write(Provider::Fetch).await;
}

#[tokio::test]
async fn typed_execute_requires_ready_for_query_even_after_command_complete() {
    unconfirmed_write(Provider::Execute).await;
}

#[derive(Clone, Copy)]
enum Provider {
    PublicExecute,
    PublicFetch,
    Raw,
    Fetch,
    Execute,
}

#[allow(clippy::too_many_lines)] // The proxy and observer share a controlled sequence.
async fn unconfirmed_write(provider: Provider) {
    let url = "postgres://postgres@127.0.0.1:55433/postgres?sslmode=disable";
    let (observer, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let observer_task = tokio::spawn(connection);
    let table = match provider {
        Provider::PublicExecute => "dbapi_public_execute_unknown_write_fixture",
        Provider::PublicFetch => "dbapi_public_fetch_unknown_write_fixture",
        Provider::Raw => "dbapi_experimental_unknown_write_fixture",
        Provider::Fetch => "dbapi_fetch_unknown_write_fixture",
        Provider::Execute => "dbapi_execute_unknown_write_fixture",
    };
    observer
        .batch_execute(&format!("CREATE TABLE {table} (v bigint)"))
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let gate = Arc::new(AtomicBool::new(false));
    let (dispatched_tx, dispatched_rx) = tokio::sync::oneshot::channel();
    let (withheld_tx, withheld_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let proxy_gate = gate.clone();
    let proxy = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = tokio::net::TcpStream::connect(("127.0.0.1", 55433))
            .await
            .unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let output_gate = proxy_gate.clone();
        let inbound = tokio::spawn(async move {
            let mut write_completed = false;
            while let Ok(tag) = server_read.read_u8().await {
                let len = server_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; len as usize - 4];
                server_read.read_exact(&mut body).await.unwrap();
                if tag == b'C' && output_gate.load(Ordering::SeqCst) && body == b"INSERT 0 1\0" {
                    write_completed = true;
                }
                // Let DataRow and CommandComplete through, but never the final
                // authoritative ReadyForQuery. The observer sees the committed write.
                if tag == b'Z' && write_completed {
                    withheld_tx.send(()).unwrap();
                    std::future::pending::<()>().await;
                    break;
                }
                if client_write.write_u8(tag).await.is_err() {
                    break;
                }
                client_write.write_u32(len).await.unwrap();
                client_write.write_all(&body).await.unwrap();
            }
        });
        // The startup packet has a length but no message tag.
        let startup_len = client_read.read_u32().await.unwrap();
        let mut startup = vec![0; startup_len as usize - 4];
        client_read.read_exact(&mut startup).await.unwrap();
        server_write.write_u32(startup_len).await.unwrap();
        server_write.write_all(&startup).await.unwrap();
        let mut saw_statement = false;
        let mut dispatched = Some(dispatched_tx);
        loop {
            let tag = tokio::select! {
                result = client_read.read_u8() => match result { Ok(tag) => tag, Err(_) => break },
                _ = &mut stop_rx => break,
            };
            let len = client_read.read_u32().await.unwrap();
            let mut body = vec![0; len as usize - 4];
            client_read.read_exact(&mut body).await.unwrap();
            if tag == b'P'
                && body
                    .windows(b"unknown_write_fixture".len())
                    .any(|window| window == b"unknown_write_fixture")
            {
                saw_statement = true;
            }
            if tag == b'E' && saw_statement {
                proxy_gate.store(true, Ordering::SeqCst);
            }
            server_write.write_u8(tag).await.unwrap();
            server_write.write_u32(len).await.unwrap();
            server_write.write_all(&body).await.unwrap();
            if tag == b'E'
                && saw_statement
                && let Some(sender) = dispatched.take()
            {
                sender.send(()).unwrap();
            }
        }
        inbound.abort();
        let _ = inbound.await;
    });

    let result_type = if matches!(provider, Provider::Raw) {
        "int4"
    } else {
        "bigint"
    };
    let insert = format!("INSERT INTO {table} VALUES (1) RETURNING v::{result_type}");
    let write = match provider {
        Provider::Raw | Provider::Fetch | Provider::Execute => {
            use neotales_dbapi_postgres::experimental::{Config, Error, Pool, Profile};
            let mut config = Config::new();
            config
                .host("127.0.0.1")
                .port(port)
                .user("postgres")
                .dbname("postgres");
            let pool = Pool::loopback_plaintext_for_tests(
                config,
                1,
                Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
            )
            .unwrap();
            let mut lease = pool.acquire().await.unwrap();
            tokio::spawn(async move {
                match provider {
                    Provider::Raw => assert_eq!(
                        lease.query(&insert, &[]).await.unwrap_err(),
                        Error::OutcomeUnknown,
                    ),
                    Provider::Fetch => assert_eq!(
                        lease
                            .fetch(&insert, &[], RowLimit::new(1, 100).unwrap())
                            .await
                            .unwrap_err()
                            .kind(),
                        ErrorKind::WriteOutcomeUnknown,
                    ),
                    Provider::Execute => assert_eq!(
                        lease.execute(&insert, &[]).await.unwrap_err().kind(),
                        ErrorKind::WriteOutcomeUnknown,
                    ),
                    Provider::PublicExecute | Provider::PublicFetch => unreachable!(),
                }
                assert_eq!(lease.recycle().await, Err(Error::Poisoned));
                pool.close();
            })
        }
        Provider::PublicExecute | Provider::PublicFetch => {
            use neotales_dbapi_postgres::{Config, PoolOptions, Profile};
            let mut config = Config::new();
            config
                .host("127.0.0.1")
                .port(port)
                .user("postgres")
                .dbname("postgres");
            let policy = PoolOptions::new(
                1,
                Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
                Duration::from_secs(60),
            )
            .unwrap();
            let pool = PostgresPool::loopback_plaintext_for_tests(config, policy).unwrap();
            let mut session = pool.acquire_async().await.unwrap();
            tokio::spawn(async move {
                let error = if matches!(provider, Provider::PublicFetch) {
                    session
                        .fetch_all_async(
                            &insert,
                            QueryOptions::new().limit(RowLimit::new(1, 100).unwrap()),
                        )
                        .await
                        .unwrap_err()
                } else {
                    session
                        .execute_async(&insert, QueryOptions::new())
                        .await
                        .unwrap_err()
                };
                assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
                drop(session);
                pool.wait_for_cleanup().await.unwrap();
                pool.close();
            })
        }
    };
    tokio::time::timeout(Duration::from_secs(5), dispatched_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), withheld_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !write.is_finished(),
        "returned success before ReadyForQuery"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = observer
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await
                .unwrap()
                .get(0);
            if count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("server did not commit dispatched write");
    stop_tx.send(()).unwrap();
    proxy.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    observer
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    drop(observer);
    observer_task.await.unwrap().unwrap();
}
