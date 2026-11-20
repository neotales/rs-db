//! Faults at the wire boundary exercise production cleanup, with no pool hooks.
use std::{sync::Arc, time::Duration};

use neotales_dbapi_postgres::experimental::{Config, Error, Pool, Profile};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    task::JoinSet,
};

fn pool(port: u16) -> Pool {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    Pool::loopback_plaintext_for_tests(
        config,
        1,
        Profile::normal(Duration::from_millis(700), Duration::from_millis(100)).unwrap(),
    )
    .unwrap()
}

async fn capacity_restored(pool: &Pool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.free_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("socket owner did not release capacity");
}

async fn startup(socket: &mut TcpStream) {
    let len = socket.read_u32().await.unwrap();
    assert!((8..=4096).contains(&len));
    let mut startup = vec![0; len as usize - 4];
    socket.read_exact(&mut startup).await.unwrap();
}

#[tokio::test]
async fn startup_deadline_drops_socket_and_restores_capacity() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pool = pool(listener.local_addr().unwrap().port());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        startup(&mut socket).await;
        assert_eq!(
            socket.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    });
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
    capacity_restored(&pool).await;
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    pool.close();
}

#[tokio::test]
async fn rejected_initializer_drops_worker_without_test_injection() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pool = pool(listener.local_addr().unwrap().port());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        startup(&mut socket).await;
        socket
            .write_all(&[
                b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2, b'Z', 0,
                0, 0, 5, b'I',
            ])
            .await
            .unwrap();
        let mut received = Vec::new();
        let mut buffer = [0; 4096];
        while !received
            .windows(b"set_config".len())
            .any(|b| b == b"set_config")
        {
            let n = socket.read(&mut buffer).await.unwrap();
            assert_ne!(n, 0);
            received.extend_from_slice(&buffer[..n]);
        }
        let message = b"SERROR\0CXX000\0Minitializer_server_canary\0\0";
        socket.write_u8(b'E').await.unwrap();
        socket
            .write_u32(u32::try_from(message.len() + 4).unwrap())
            .await
            .unwrap();
        socket.write_all(message).await.unwrap();
        socket.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await.unwrap();
        // The driver may have already queued Sync; drain it before checking EOF.
        while socket.read(&mut buffer).await.unwrap() != 0 {}
    });
    let error = pool.acquire().await.unwrap_err();
    assert_eq!(error, Error::Initializer);
    assert!(!format!("{error:?} {error}").contains("initializer_server_canary"));
    assert!(std::error::Error::source(&error).is_none());
    capacity_restored(&pool).await;
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    pool.close();
}

/// A real-server proxy stops a reset after ROLLBACK has completed and DISCARD
/// has been sent, before forwarding DISCARD. No production timing hook is needed.
async fn reset_proxy(listener: TcpListener, blocked: Arc<Notify>, mut stop: oneshot::Receiver<()>) {
    let mut peers = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => break,
            accepted = listener.accept() => {
                let (incoming, _) = accepted.unwrap();
                let blocked = blocked.clone();
                peers.spawn(async move {
                    let upstream = TcpStream::connect(("127.0.0.1", 55433)).await.unwrap();
                    let (mut client_read, mut client_write) = incoming.into_split();
                    let (mut server_read, mut server_write) = upstream.into_split();
                    let outbound = async {
                        let mut tail = Vec::new();
                        let mut buffer = [0; 8192];
                        let mut held = false;
                        loop {
                            let n = client_read.read(&mut buffer).await.unwrap();
                            if n == 0 { break; }
                            tail.extend_from_slice(&buffer[..n]);
                            if !held && tail.windows(b"DISCARD ALL".len()).any(|b| b == b"DISCARD ALL") {
                                held = true;
                                blocked.notify_one();
                            }
                            if !held {
                                server_write.write_all(&buffer[..n]).await.unwrap();
                            }
                            if tail.len() > 8192 { tail.drain(..tail.len() - 10); }
                        }
                        server_write.shutdown().await.unwrap();
                    };
                    let inbound = async {
                        let _ = tokio::io::copy(&mut server_read, &mut client_write).await;
                    };
                    tokio::join!(outbound, inbound);
                });
            }
            joined = peers.join_next(), if !peers.is_empty() => {
                joined.unwrap().unwrap();
            }
        }
    }
    peers.abort_all();
    while let Some(result) = peers.join_next().await {
        assert!(result.is_ok() || result.unwrap_err().is_cancelled());
    }
}

#[derive(Clone, Copy)]
enum Interrupt {
    Drop,
    Deadline,
    Close,
}

async fn interrupted_reset(interrupt: Interrupt) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pool = pool(listener.local_addr().unwrap().port());
    let blocked = Arc::new(Notify::new());
    let (stop_tx, stop_rx) = oneshot::channel();
    let proxy = tokio::spawn(reset_proxy(listener, blocked.clone(), stop_rx));
    let mut lease = pool.acquire().await.unwrap();
    let original = lease
        .query("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .int(0)
        .unwrap();
    lease
        .batch("CREATE TEMP TABLE reset_canary(v int); BEGIN")
        .await
        .unwrap();
    let reset = tokio::spawn(lease.recycle());
    tokio::time::timeout(Duration::from_secs(2), blocked.notified())
        .await
        .unwrap();
    match interrupt {
        Interrupt::Drop => {
            reset.abort();
            assert!(reset.await.unwrap_err().is_cancelled());
        }
        Interrupt::Deadline => assert_eq!(reset.await.unwrap(), Err(Error::Timeout)),
        Interrupt::Close => {
            pool.close();
            assert_eq!(reset.await.unwrap(), Err(Error::Closed));
        }
    }
    capacity_restored(&pool).await;
    if !matches!(interrupt, Interrupt::Close) {
        let mut replacement = pool.acquire().await.unwrap();
        let row = replacement
            .query(
                "SELECT pg_backend_pid(), to_regclass('pg_temp.reset_canary') IS NULL",
                &[],
            )
            .await
            .unwrap();
        assert_ne!(row.int(0).unwrap(), original);
        assert!(row.boolean(1).unwrap());
        drop(replacement);
    }
    pool.close();
    capacity_restored(&pool).await;
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dropped_reset_discards_connection() {
    interrupted_reset(Interrupt::Drop).await;
}

#[tokio::test]
async fn stalled_reset_hits_client_deadline() {
    interrupted_reset(Interrupt::Deadline).await;
}

#[tokio::test]
async fn pool_close_wakes_stalled_reset() {
    interrupted_reset(Interrupt::Close).await;
}
