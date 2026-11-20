//! Fault injection on actual PostgreSQL protocol completion; packets are never logged.
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
    task::JoinHandle,
};

pub(super) struct Proxy {
    pub(super) port: u16,
    pub(super) observed: oneshot::Receiver<()>,
    worker: JoinHandle<()>,
}

impl Proxy {
    pub(super) async fn start() -> Self {
        Self::hold_completion(b"COMMIT\0").await
    }

    pub(super) async fn hold_delete() -> Self {
        Self::hold_completion(b"DELETE 1\0").await
    }

    async fn hold_completion(target: &'static [u8]) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (signal, observed) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let (incoming, _) = listener.accept().await.unwrap();
            let upstream = TcpStream::connect(("127.0.0.1", 55435)).await.unwrap();
            incoming.set_nodelay(true).unwrap();
            upstream.set_nodelay(true).unwrap();
            let (mut client_read, client_write) = incoming.into_split();
            let (server_read, mut server_write) = upstream.into_split();
            tokio::select! {
                _ = tokio::io::copy(&mut client_read, &mut server_write) => {}
                _ = forward_until_completion(server_read, client_write, signal, target) => {}
            }
        });
        Self {
            port,
            observed,
            worker,
        }
    }

    pub(super) async fn stop(&mut self) {
        self.worker.abort();
        let result = tokio::time::timeout(Duration::from_secs(2), &mut self.worker)
            .await
            .unwrap();
        assert!(result.is_ok() || result.unwrap_err().is_cancelled());
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

async fn forward_until_completion(
    mut server: OwnedReadHalf,
    mut client: OwnedWriteHalf,
    signal: oneshot::Sender<()>,
    target: &'static [u8],
) -> io::Result<()> {
    let mut completed = false;
    loop {
        let tag = server.read_u8().await?;
        let length = server.read_u32().await?;
        if !(4..=1_048_576).contains(&length) {
            return Err(io::Error::other(
                "unexpected PostgreSQL fixture packet length",
            ));
        }
        let mut body = vec![0; length as usize - 4];
        server.read_exact(&mut body).await?;
        if tag == b'C' && body == target {
            completed = true;
        }
        if completed {
            if tag == b'Z' {
                let _ = signal.send(());
                std::future::pending::<()>().await;
                return Ok(());
            }
            continue;
        }
        client.write_u8(tag).await?;
        client.write_u32(length).await?;
        client.write_all(&body).await?;
    }
}
