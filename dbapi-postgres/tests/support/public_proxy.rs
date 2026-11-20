//! Public trait-surface faults at real command-completion boundaries.
use super::{at, count, limit, options, unused};
use neotales_dbapi::{ErrorKind, Pool, QueryOptions, TxOptions};
use neotales_dbapi_postgres::PostgresPool;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    sync::oneshot,
    task::JoinHandle,
};

#[derive(Clone, Copy)]
pub(super) enum Stage {
    Begin,
    Commit,
    Rollback,
    Reset,
    MissingCompletion,
    MissingScript,
    MissingReset,
}

struct Wire {
    pool: PostgresPool,
    arm: Arc<AtomicBool>,
    blocked: oneshot::Receiver<()>,
    stop: oneshot::Sender<()>,
    worker: JoinHandle<()>,
}

#[allow(clippy::too_many_lines)] // One duplex fixture correlates each command and its final status.
async fn start(stage: Stage, budget: Duration) -> Wire {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let arm = Arc::new(AtomicBool::new(false));
    let armed = arm.clone();
    let gate = Arc::new(AtomicBool::new(false));
    let out_gate = gate.clone();
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
            while let Ok(tag) = client_read.read_u8().await {
                let len = client_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; len as usize - 4];
                client_read.read_exact(&mut body).await.unwrap();
                let target = if matches!(stage, Stage::MissingCompletion) {
                    tag == b'E'
                } else {
                    tag == b'Q'
                        && match stage {
                            Stage::Begin => body.starts_with(b"BEGIN ISOLATION LEVEL "),
                            Stage::Commit => body == b"COMMIT\0",
                            Stage::Rollback => body == b"ROLLBACK\0",
                            Stage::Reset | Stage::MissingReset => body == b"DISCARD ALL\0",
                            Stage::MissingCompletion => unreachable!(),
                            Stage::MissingScript => body == b"SELECT 1\0",
                        }
                };
                if target && armed.load(Ordering::Acquire) {
                    out_gate.store(true, Ordering::Release);
                }
                if server_write.write_u8(tag).await.is_err() {
                    break;
                }
                if server_write.write_u32(len).await.is_err() {
                    break;
                }
                if server_write.write_all(&body).await.is_err() {
                    break;
                }
            }
        };
        let inbound = async {
            let mut complete = false;
            let mut blocked_tx = Some(blocked_tx);
            while let Ok(tag) = server_read.read_u8().await {
                let len = server_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; len as usize - 4];
                server_read.read_exact(&mut body).await.unwrap();
                if tag == b'C' && gate.load(Ordering::Acquire) {
                    complete = match stage {
                        Stage::Begin => body == b"BEGIN\0",
                        Stage::Commit => body == b"COMMIT\0",
                        Stage::Rollback => body == b"ROLLBACK\0",
                        Stage::Reset | Stage::MissingReset => body == b"DISCARD ALL\0",
                        Stage::MissingCompletion | Stage::MissingScript => body == b"SELECT 1\0",
                    };
                    if complete
                        && matches!(
                            stage,
                            Stage::MissingCompletion | Stage::MissingScript | Stage::MissingReset
                        )
                    {
                        continue; // Remove CommandComplete but still forward ReadyForQuery.
                    }
                }
                if tag == b'Z' && complete {
                    if let Some(sender) = blocked_tx.take() {
                        let _ = sender.send(());
                    }
                    if !matches!(
                        stage,
                        Stage::MissingCompletion | Stage::MissingScript | Stage::MissingReset
                    ) {
                        std::future::pending::<()>().await;
                        break;
                    }
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
    Wire {
        pool: at(port, 1, budget, Duration::from_secs(60)),
        arm,
        blocked,
        stop,
        worker,
    }
}

pub(super) async fn commit() {
    let Wire {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::Commit, Duration::from_secs(3)).await;
    let observer_pool = super::pool(1);
    let mut observer = observer_pool.acquire_async().await.unwrap();
    observer
        .execute_async(
            "CREATE TABLE public_unknown_commit(v bigint)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let mut session = pool.acquire_async().await.unwrap();
    let action = tokio::spawn(async move {
        let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
        tx.execute_async(
            "INSERT INTO public_unknown_commit VALUES(1)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
        arm.store(true, Ordering::Release);
        let failure = tx.commit_async().await.unwrap_err();
        assert_eq!(
            session
                .fetch_all_async("SELECT 1::bigint", QueryOptions::new().limit(limit()))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Connection
        );
        failure
    });
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    assert!(!action.is_finished());
    assert_eq!(count(&mut *observer, "public_unknown_commit").await, 1);
    stop.send(()).unwrap();
    worker.await.unwrap();
    assert_eq!(
        action.await.unwrap().kind(),
        ErrorKind::CommitOutcomeUnknown
    );
    assert_eq!(count(&mut *observer, "public_unknown_commit").await, 1);
    unused(&pool).await;
    pool.close();
    observer
        .execute_async("DROP TABLE public_unknown_commit", QueryOptions::new())
        .await
        .unwrap();
    drop(observer);
    observer_pool.wait_for_cleanup().await.unwrap();
    observer_pool.close();
}

pub(super) async fn cleanup() {
    let Wire {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::Reset, Duration::from_millis(500)).await;
    let mut session = pool.acquire_async().await.unwrap();
    session
        .execute_async("SELECT 1", QueryOptions::new())
        .await
        .unwrap();
    arm.store(true, Ordering::Release);
    drop(session);
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            pool.wait_for_cleanup().await.unwrap_err().kind(),
            ErrorKind::Timeout
        );
    }
    unused(&pool).await;
    let _ = stop.send(());
    worker.await.unwrap();
    pool.close();
}

pub(super) async fn close_cleanup() {
    let Wire {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::Reset, Duration::from_secs(3)).await;
    let session = pool.acquire_async().await.unwrap();
    arm.store(true, Ordering::Release);
    drop(session);
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    pool.close();
    tokio::time::timeout(Duration::from_secs(2), pool.wait_for_cleanup())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pool.acquire_async().await.err().unwrap().kind(),
        ErrorKind::Connection
    );
    unused(&pool).await;
    let _ = stop.send(());
    worker.await.unwrap();
}

pub(super) async fn cancelled(stage: Stage) {
    let Wire {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(stage, Duration::from_secs(3)).await;
    let mut session = pool.acquire_async().await.unwrap();
    match stage {
        Stage::Begin => {
            arm.store(true, Ordering::Release);
            {
                let begin = session.begin_async(TxOptions::default());
                tokio::pin!(begin);
                tokio::select! {
                    _ = &mut begin => panic!("begin returned before ReadyForQuery"),
                    result = tokio::time::timeout(Duration::from_secs(2), blocked) => { result.unwrap().unwrap(); }
                }
            }
        }
        Stage::Rollback => {
            let tx = session.begin_async(TxOptions::default()).await.unwrap();
            arm.store(true, Ordering::Release);
            {
                let rollback = tx.rollback_async();
                tokio::pin!(rollback);
                tokio::select! {
                    _ = &mut rollback => panic!("rollback returned before ReadyForQuery"),
                    result = tokio::time::timeout(Duration::from_secs(2), blocked) => { result.unwrap().unwrap(); }
                }
            }
        }
        _ => unreachable!(),
    }
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);
    unused(&pool).await;
    let _ = stop.send(());
    worker.await.unwrap();
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

pub(super) async fn missing_completion() {
    for operation in 0..3 {
        let Wire {
            pool,
            arm,
            blocked,
            stop,
            worker,
        } = start(
            if operation == 2 {
                Stage::MissingScript
            } else {
                Stage::MissingCompletion
            },
            Duration::from_secs(3),
        )
        .await;
        let mut session = pool.acquire_async().await.unwrap();
        arm.store(true, Ordering::Release);
        let error = match operation {
            1 => session
                .fetch_all_async("SELECT 1::bigint AS n", QueryOptions::new().limit(limit()))
                .await
                .unwrap_err(),
            2 => session
                .execute_trusted_script_async("SELECT 1", options())
                .await
                .unwrap_err(),
            _ => session
                .execute_async("SELECT 1", QueryOptions::new())
                .await
                .unwrap_err(),
        };
        assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
        tokio::time::timeout(Duration::from_secs(2), blocked)
            .await
            .unwrap()
            .unwrap();
        drop(session);
        unused(&pool).await;
        let _ = stop.send(());
        worker.await.unwrap();
        pool.close();
    }
}

pub(super) async fn missing_reset() {
    let Wire {
        pool,
        arm,
        blocked,
        stop,
        worker,
    } = start(Stage::MissingReset, Duration::from_secs(3)).await;
    let session = pool.acquire_async().await.unwrap();
    arm.store(true, Ordering::Release);
    drop(session);
    tokio::time::timeout(Duration::from_secs(2), blocked)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pool.wait_for_cleanup().await.unwrap_err().kind(),
        ErrorKind::Connection
    );
    unused(&pool).await;
    let _ = stop.send(());
    worker.await.unwrap();
    pool.close();
}
