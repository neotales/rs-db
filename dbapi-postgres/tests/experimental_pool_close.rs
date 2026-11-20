//! Regressions for shutdown while an acquisition is initializing.
use neotales_dbapi_postgres::experimental as pool;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use neotales_tokio_postgres::Config;
use pool::{Error, Pool, Profile};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

fn config(port: u16) -> Config {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    config
}

fn pool(port: u16) -> Pool {
    Pool::loopback_plaintext_for_tests(
        config(port),
        1,
        Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
    )
    .unwrap()
}

async fn assert_closed(pool: &Pool, acquire: tokio::task::JoinHandle<Result<pool::Lease, Error>>) {
    pool.close();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), acquire)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Closed)
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.free_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("driver did not release capacity");
}

#[tokio::test]
async fn close_interrupts_fresh_connection_initializer() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (blocked_tx, blocked_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let len = socket.read_u32().await.unwrap();
        assert!((8..=4096).contains(&len));
        let mut startup = vec![0; len as usize - 4];
        socket.read_exact(&mut startup).await.unwrap();
        // AuthenticationOk, BackendKeyData, ReadyForQuery; all synthetic.
        socket
            .write_all(&[
                b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2, b'Z', 0,
                0, 0, 5, b'I',
            ])
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1024];
        while !bytes
            .windows(b"set_config".len())
            .any(|b| b == b"set_config")
        {
            let n = socket.read(&mut buffer).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        blocked_tx.send(()).unwrap();
        loop {
            if socket.read(&mut buffer).await.unwrap() == 0 {
                break;
            }
        }
    });
    let pool = pool(port);
    let pending = pool.clone();
    let acquire = tokio::spawn(async move { pending.acquire().await });
    tokio::time::timeout(Duration::from_secs(2), blocked_rx)
        .await
        .unwrap()
        .unwrap();
    assert_closed(&pool, acquire).await;
    tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn close_interrupts_reused_connection_initializer() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let blocked = Arc::new(AtomicBool::new(false));
    let gate = blocked.clone();
    let initializer_sent = Arc::new(AtomicBool::new(false));
    let outbound_started = initializer_sent.clone();
    let inbound_started = initializer_sent;
    let (blocked_tx, blocked_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (incoming, _) = listener.accept().await.unwrap();
        let upstream = tokio::net::TcpStream::connect(("127.0.0.1", 55433))
            .await
            .unwrap();
        let (mut client_read, mut client_write) = incoming.into_split();
        let (mut server_read, mut server_write) = upstream.into_split();
        let outbound = tokio::spawn(async move {
            let mut buffer = [0; 8192];
            let mut tail = Vec::new();
            loop {
                let n = client_read.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                if gate.load(Ordering::SeqCst) {
                    tail.extend_from_slice(&buffer[..n]);
                    if tail
                        .windows(b"set_config".len())
                        .any(|b| b == b"set_config")
                    {
                        outbound_started.store(true, Ordering::SeqCst);
                    }
                    if tail.len() > 8192 {
                        tail.drain(..tail.len() - 9);
                    }
                }
                server_write.write_all(&buffer[..n]).await.unwrap();
            }
        });
        let inbound = tokio::spawn(async move {
            let mut buffer = [0; 8192];
            let mut blocked_tx = Some(blocked_tx);
            loop {
                let n = server_read.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                if inbound_started.load(Ordering::SeqCst) {
                    blocked_tx.take().unwrap().send(()).unwrap();
                    let _ = stop_rx.await;
                    break;
                }
                client_write.write_all(&buffer[..n]).await.unwrap();
            }
        });
        outbound.await.unwrap();
        inbound.await.unwrap();
    });
    let pool = pool(port);
    let lease = pool.acquire().await.unwrap();
    lease.recycle().await.unwrap();
    blocked.store(true, Ordering::SeqCst);
    let pending = pool.clone();
    let acquire = tokio::spawn(async move { pending.acquire().await });
    tokio::time::timeout(Duration::from_secs(2), blocked_rx)
        .await
        .unwrap()
        .unwrap();
    assert_closed(&pool, acquire).await;
    let _ = stop_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .unwrap()
        .unwrap();
}
