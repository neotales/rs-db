//! Faults target actual protocol commands, never library test hooks.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use neotales_dbapi::{ErrorKind, TxOptions};
use neotales_dbapi_postgres::experimental::Pool;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    sync::oneshot,
    task::JoinHandle,
};

use super::{capacity, count, make_pool, pid};

#[derive(Clone, Copy, Debug)]
pub(super) enum Stage {
    Begin,
    Preflight,
    Query,
    Commit,
    Rollback,
}

struct Proxy {
    pool: Pool,
    arm: Arc<AtomicBool>,
    blocked: oneshot::Receiver<()>,
    stop: oneshot::Sender<()>,
    worker: JoinHandle<()>,
}

#[allow(clippy::too_many_lines)] // One duplex fixture correlates request, tag, and ReadyForQuery.
async fn start(stage: Stage, budget: Duration) -> Proxy {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let arm = Arc::new(AtomicBool::new(false));
    let armed = arm.clone();
    let gate = Arc::new(AtomicBool::new(false));
    let outbound_gate = gate.clone();
    let (blocked_tx, blocked) = oneshot::channel();
    let (stop, mut stop_rx) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let (incoming, _) = listener.accept().await.unwrap();
        let upstream = tokio::net::TcpStream::connect(("127.0.0.1", 55433))
            .await
            .unwrap();
        incoming.set_nodelay(true).unwrap();
        upstream.set_nodelay(true).unwrap();
        let (mut client_read, mut client_write) = incoming.into_split();
        let (mut server_read, mut server_write) = upstream.into_split();
        let outbound = async {
            let len = client_read.read_u32().await.unwrap();
            assert!((8..=4096).contains(&len));
            let mut body = vec![0; len as usize - 4];
            client_read.read_exact(&mut body).await.unwrap();
            server_write.write_u32(len).await.unwrap();
            server_write.write_all(&body).await.unwrap();
            let mut target_statement = Vec::new();
            let mut bound = false;
            while let Ok(tag) = client_read.read_u8().await {
                let len = client_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; len as usize - 4];
                client_read.read_exact(&mut body).await.unwrap();
                if tag == b'P'
                    && body
                        .windows(b"tx_fault_query".len())
                        .any(|part| part == b"tx_fault_query")
                {
                    target_statement = body.split(|byte| *byte == 0).next().unwrap().to_vec();
                }
                if tag == b'B' && !target_statement.is_empty() {
                    bound =
                        body.split(|byte| *byte == 0).nth(1) == Some(target_statement.as_slice());
                }
                let target = match stage {
                    Stage::Begin => tag == b'Q' && body.starts_with(b"BEGIN ISOLATION LEVEL "),
                    Stage::Preflight => tag == b'Q' && body == b"SAVEPOINT neotales_arity\0",
                    Stage::Query => tag == b'E' && bound,
                    Stage::Commit => tag == b'Q' && body == b"COMMIT\0",
                    Stage::Rollback => tag == b'Q' && body == b"ROLLBACK\0",
                };
                if armed.load(Ordering::SeqCst) && target {
                    outbound_gate.store(true, Ordering::SeqCst);
                }
                if server_write.write_u8(tag).await.is_err() {
                    break;
                }
                server_write.write_u32(len).await.unwrap();
                if server_write.write_all(&body).await.is_err() {
                    break;
                }
            }
        };
        let inbound = async {
            let mut completed = false;
            while let Ok(tag) = server_read.read_u8().await {
                let len = server_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; len as usize - 4];
                server_read.read_exact(&mut body).await.unwrap();
                if tag == b'C' && gate.load(Ordering::SeqCst) {
                    completed = match stage {
                        Stage::Begin => body == b"BEGIN\0",
                        Stage::Preflight => body == b"SAVEPOINT\0",
                        Stage::Query => body == b"SELECT 1\0",
                        Stage::Commit => body == b"COMMIT\0",
                        Stage::Rollback => body == b"ROLLBACK\0",
                    };
                }
                // Forward CommandComplete but never the targeted ReadyForQuery.
                // Earlier statement-drop CLOSE/Sync requests also have ReadyForQuery;
                // matching the target CommandComplete avoids blocking one of those.
                if tag == b'Z' && completed {
                    let _ = blocked_tx.send(());
                    std::future::pending::<()>().await;
                    break;
                }
                if client_write.write_u8(tag).await.is_err() {
                    break;
                }
                if client_write.write_u32(len).await.is_err() {
                    break;
                }
                if client_write.write_all(&body).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! { () = outbound => {}, () = inbound => {}, _ = &mut stop_rx => {} }
    });
    Proxy {
        pool: make_pool(port, budget),
        arm,
        blocked,
        stop,
        worker,
    }
}

pub(super) async fn cancelled(stage: Stage) {
    let Proxy {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(stage, Duration::from_secs(3)).await;
    let mut lease = pool.acquire().await.unwrap();
    let original = pid(&mut lease).await;
    let operation = if matches!(stage, Stage::Begin) {
        arm.store(true, Ordering::SeqCst);
        tokio::spawn(async move {
            drop(lease.begin(TxOptions::default()).await);
        })
    } else {
        let mut tx = lease.begin(TxOptions::default()).await.unwrap();
        tx.execute("SELECT pg_advisory_xact_lock(75823384)", &[])
            .await
            .unwrap();
        arm.store(true, Ordering::SeqCst);
        tokio::spawn(async move {
            match stage {
                Stage::Query | Stage::Preflight => {
                    let _ = tx.execute("SELECT 1 /* tx_fault_query */", &[]).await;
                    drop(tx);
                }
                Stage::Commit => {
                    drop(tx.commit().await);
                }
                Stage::Rollback => {
                    drop(tx.rollback().await);
                }
                Stage::Begin => unreachable!(),
            }
        })
    };
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    assert!(!operation.is_finished());
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    capacity(&pool).await;
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
    pool.close();
    let mut observer = super::pool().acquire().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = observer
                .fetch(
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1) AS gone",
                    &[super::integer(original)],
                    super::limit(),
                )
                .await
                .unwrap();
            if rows[0].get::<bool>("gone").unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled transaction backend retained ownership");
    observer.recycle().await.unwrap();
}

pub(super) async fn lost_commit() {
    let Proxy {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::Commit, Duration::from_secs(3)).await;
    let mut observer = super::pool().acquire().await.unwrap();
    observer
        .execute("CREATE TABLE quiet_tx_commit_fault(v bigint)", &[])
        .await
        .unwrap();
    let mut tx = pool
        .acquire()
        .await
        .unwrap()
        .begin(TxOptions::default())
        .await
        .unwrap();
    tx.execute("INSERT INTO quiet_tx_commit_fault VALUES (1)", &[])
        .await
        .unwrap();
    arm.store(true, Ordering::SeqCst);
    let commit = tokio::spawn(tx.commit());
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !commit.is_finished(),
        "CommandComplete alone was treated as success"
    );
    assert_eq!(count(&mut observer, "quiet_tx_commit_fault").await, 1);
    stop.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(
        commit.await.unwrap().unwrap_err().kind(),
        ErrorKind::CommitOutcomeUnknown
    );
    assert_eq!(count(&mut observer, "quiet_tx_commit_fault").await, 1);
    capacity(&pool).await;
    pool.close();
    observer
        .execute("DROP TABLE quiet_tx_commit_fault", &[])
        .await
        .unwrap();
    observer.recycle().await.unwrap();
}

pub(super) async fn deadline(stage: Stage) {
    let Proxy {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(stage, Duration::from_millis(500)).await;
    let lease = pool.acquire().await.unwrap();
    let action = if matches!(stage, Stage::Begin) {
        arm.store(true, Ordering::SeqCst);
        tokio::spawn(async move { lease.begin(TxOptions::default()).await.unwrap_err() })
    } else {
        let mut tx = lease.begin(TxOptions::default()).await.unwrap();
        arm.store(true, Ordering::SeqCst);
        tokio::spawn(async move {
            match stage {
                Stage::Preflight | Stage::Query => {
                    let error = tx
                        .execute("SELECT 1 /* tx_fault_query */", &[])
                        .await
                        .unwrap_err();
                    assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Connection);
                    error
                }
                Stage::Commit => tx.commit().await.unwrap_err(),
                Stage::Rollback => tx.rollback().await.unwrap_err(),
                Stage::Begin => unreachable!(),
            }
        })
    };
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), action)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        error.kind(),
        match stage {
            Stage::Begin | Stage::Preflight | Stage::Rollback => ErrorKind::Timeout,
            Stage::Query => ErrorKind::WriteOutcomeUnknown,
            Stage::Commit => ErrorKind::CommitOutcomeUnknown,
        }
    );
    capacity(&pool).await;
    let _ = stop.send(());
    worker.await.unwrap();
    pool.close();
}

pub(super) async fn retained_owner_cancellation() {
    let Proxy {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::Query, Duration::from_secs(3)).await;
    let mut tx = pool
        .acquire()
        .await
        .unwrap()
        .begin(TxOptions::default())
        .await
        .unwrap();
    arm.store(true, Ordering::SeqCst);
    {
        let query = tx.execute("SELECT 1 /* tx_fault_query */", &[]);
        tokio::pin!(query);
        tokio::select! {
            _ = &mut query => panic!("query completed before the withheld ReadyForQuery"),
            result = tokio::time::timeout(Duration::from_secs(2), blocked) => {
                result.unwrap().unwrap();
            }
        }
    }
    // The owner still exists. Dropping only its borrowed operation must already
    // discard the connection, not leave a reusable or idle-in-transaction socket.
    capacity(&pool).await;
    assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Connection);
    let _ = stop.send(());
    worker.await.unwrap();
    pool.close();
}
