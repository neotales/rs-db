use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::{Stream, StreamExt, stream};
use indexmap::IndexMap;
use protocol::CompressionMethod;
use tokio::{
    io::{AsyncRead, AsyncWrite, BufReader, BufWriter},
    net::{TcpStream, ToSocketAddrs},
    select,
    sync::{
        broadcast,
        mpsc::{self, Receiver},
        oneshot,
    },
};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::{
    KlickhouseError, ParsedQuery, RawRow, Result,
    block::{Block, BlockInfo},
    convert::Row,
    internal_client_in::InternalClientIn,
    internal_client_out::{
        CLIENT_NAME, ClientHello, ClientInfo, InternalClientOut, OpenTelemetry, Query, QueryKind,
        QueryProcessingStage, check_settings_revision,
    },
    io::{ClickhouseRead, ClickhouseWrite},
    progress::Progress,
    protocol::{self, ServerPacket},
};

// Maximum number of progress statuses to keep in memory. New statuses evict old ones.
const PROGRESS_CAPACITY: usize = 100;

/// A native response event. Data is provisional until `EndOfStream`.
#[derive(Debug)]
pub enum QueryEvent {
    /// One provisional server data block.
    Data(Block),
    /// The explicit native server EndOfStream packet.
    EndOfStream,
}

/// Candidate parameter types. Null and UInt64 remain unsupported.
pub enum NativeParameter<'a> {
    /// A Boolean.
    Bool(bool),
    /// A 64-bit floating-point value; only positive/negative zero and ±1 are supported.
    Float64(f64),
    /// A signed 64-bit value.
    Int64(i64),
    /// An unsigned 64-bit value.
    UInt64(u64),
    /// UTF-8 text.
    Text(&'a str),
    /// Arbitrary bytes.
    Bytes(&'a [u8]),
    /// A SQL null; the placeholder must have a nullable type.
    Null,
    /// A null for a caller-validated `Nullable(Int64)` placeholder.
    NullableInt64Null,
    /// A null for a caller-validated `Nullable(UInt64)` placeholder.
    NullableUInt64Null,
    /// A null for a caller-validated `Nullable(String)` placeholder.
    NullableStringNull,
}

fn byte_parameter_dump(bytes: &[u8]) -> Result<String> {
    // Each source byte becomes an ASCII \xHH escape in the server's TSV-escaped
    // parameter value; Field::dump quotes that string and doubles each backslash.
    // Restrict input so this temporary wire representation stays bounded.
    if bytes.len() > 8192 {
        return Err(KlickhouseError::ProtocolError(
            "native parameter exceeds limit".to_string(),
        ));
    }
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 5 + 2);
    out.push('\'');
    for &byte in bytes {
        out.push('\\');
        out.push('\\');
        out.push('x');
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 15)] as char);
    }
    out.push('\'');
    Ok(out)
}

fn encode_parameter_values(
    parameters: &[(&str, NativeParameter<'_>)],
) -> Result<BTreeMap<String, (String, String)>> {
    let invalid =
        || KlickhouseError::ProtocolError("unsupported or invalid native parameters".to_string());
    let mut values = BTreeMap::new();
    for (name, value) in parameters {
        let prototype_name = name.starts_with("ntp_")
            && name.len() > 4
            && name.len() <= 64
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
        let positional_name = name.strip_prefix('p').is_some_and(|digits| {
            !digits.is_empty()
                && digits.len() <= 9
                && !digits.starts_with('0')
                && digits.bytes().all(|b| b.is_ascii_digit())
        });
        if !prototype_name && !positional_name {
            return Err(invalid());
        }
        let (declared, dump) = match value {
            NativeParameter::Int64(value) => ("Int64", format!("'{value}'")),
            NativeParameter::Bool(value) => {
                ("Bool", if *value { "'1'" } else { "'0'" }.to_string())
            }
            // ClickHouse 24.8's default float text parser can round finite f64::MAX to
            // infinity. Limit this to values whose encodings are exactly 0 or ±1.
            NativeParameter::Float64(value) if *value == 0.0 || *value == 1.0 || *value == -1.0 => {
                ("Float64", format!("'{value}'"))
            }
            NativeParameter::Text(value) => ("String", byte_parameter_dump(value.as_bytes())?),
            NativeParameter::Bytes(value) => ("String", byte_parameter_dump(value)?),
            // ClickHouse parses a custom Field string first, then parses the resulting
            // parameter using deserializeTextEscaped: the latter expects \N for null.
            NativeParameter::NullableInt64Null => ("Nullable(Int64)", r"'\\N'".to_owned()),
            NativeParameter::NullableUInt64Null => ("Nullable(UInt64)", r"'\\N'".to_owned()),
            NativeParameter::NullableStringNull => ("Nullable(String)", r"'\\N'".to_owned()),
            _ => return Err(invalid()),
        };
        // The value sent is a ClickHouse custom Settings Field string dump.
        if values
            .insert((*name).to_string(), (declared.to_owned(), dump))
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(values)
}

/// Encodes caller-validated native parameter values without scanning SQL.
///
/// The caller must ensure every placeholder in the query has a matching entry with the
/// declared type used for the dump. This is the value half of [`Client::query_bound`] and
/// is used by adapters that own their own ClickHouse placeholder scanner.
pub fn encode_native_parameters(
    parameters: &[(&str, NativeParameter<'_>)],
) -> Result<Vec<(String, String)>> {
    encode_parameter_values(parameters).map(|values| {
        values
            .into_iter()
            .map(|(name, (_, value))| (name, value))
            .collect()
    })
}

fn encode_bound(
    query: &str,
    parameters: &[(&str, NativeParameter<'_>)],
) -> Result<Vec<(String, String)>> {
    let invalid =
        || KlickhouseError::ProtocolError("unsupported or invalid native parameters".to_string());
    let values = encode_parameter_values(parameters)?;
    let bytes = query.as_bytes();
    let mut used = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' | b'#' | b';' | b'$' => return Err(invalid()),
            b'-' if bytes.get(i + 1) == Some(&b'-') => return Err(invalid()),
            b'/' if matches!(bytes.get(i + 1), Some(b'*' | b'/')) => return Err(invalid()),
            b'}' => return Err(invalid()),
            b'{' => {
                let rest = &query[i + 1..];
                let end = rest.find('}').ok_or_else(invalid)?;
                let placeholder = &rest[..end];
                let (name, ty) = placeholder.split_once(':').ok_or_else(invalid)?;
                if values
                    .get(name)
                    .is_none_or(|(declared, _)| ty != declared.as_str())
                {
                    return Err(invalid());
                }
                used.insert(name.to_owned());
                i += end + 2;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    if values.is_empty() || used.len() != values.len() {
        return Err(invalid());
    }
    Ok(values
        .into_iter()
        .map(|(name, (_, value))| (name, value))
        .collect())
}

type FailureSlot = Arc<Mutex<Option<KlickhouseError>>>;

/// Event stream that preserves an asynchronous connection failure even when its queue is full.
pub struct QueryEvents {
    receiver: ReceiverStream<Result<QueryEvent>>,
    failure: FailureSlot,
    done: bool,
    guard: Option<InsertGuard>,
}

impl QueryEvents {
    fn new(receiver: mpsc::Receiver<Result<QueryEvent>>, failure: FailureSlot) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            failure,
            done: false,
            guard: None,
        }
    }

    async fn recv(&mut self) -> Option<Result<QueryEvent>> {
        self.next().await
    }
}

impl Stream for QueryEvents {
    type Item = Result<QueryEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.done {
            return std::task::Poll::Ready(None);
        }
        match Pin::new(&mut self.receiver).poll_next(cx) {
            std::task::Poll::Ready(Some(item)) => {
                if matches!(item, Ok(QueryEvent::EndOfStream) | Err(_)) {
                    self.done = true;
                    if matches!(item, Ok(QueryEvent::EndOfStream))
                        && let Some(guard) = &mut self.guard
                    {
                        guard.armed = false;
                    }
                    self.guard.take();
                }
                std::task::Poll::Ready(Some(item.map_err(postdispatch_error)))
            }
            std::task::Poll::Ready(None) => {
                self.done = true;
                self.guard.take();
                let error = self
                    .failure
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                std::task::Poll::Ready(Some(Err(postdispatch_error(
                    error.unwrap_or_else(unexpected_eof),
                ))))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn unexpected_eof() -> KlickhouseError {
    KlickhouseError::ProtocolError("connection closed before EndOfStream".to_string())
}

fn postdispatch_error(error: KlickhouseError) -> KlickhouseError {
    match error {
        KlickhouseError::QueryOutcomeUnknown(_)
        | KlickhouseError::InsertOutcomeUnknown(_)
        | KlickhouseError::ServerException { .. } => error,
        other => KlickhouseError::QueryOutcomeUnknown(Box::new(other)),
    }
}

fn unknown_insert(error: KlickhouseError) -> KlickhouseError {
    match error {
        KlickhouseError::InsertOutcomeUnknown(_) => error,
        other => KlickhouseError::InsertOutcomeUnknown(Box::new(other)),
    }
}

fn insert_dispatch_error(error: KlickhouseError) -> KlickhouseError {
    match error {
        KlickhouseError::QueryOutcomeUnknown(_) => unknown_insert(error),
        other => other, // An explicit rejection before any query bytes were sent.
    }
}

fn terminal_blocks(events: QueryEvents) -> Pin<Box<dyn Stream<Item = Result<Block>> + Send>> {
    terminal_blocks_guarded(events, None)
}

fn terminal_blocks_guarded(
    events: QueryEvents,
    guard: Option<InsertGuard>,
) -> Pin<Box<dyn Stream<Item = Result<Block>> + Send>> {
    Box::pin(stream::unfold(
        (events, false, guard),
        |(mut events, done, mut guard)| async move {
            if done {
                return None;
            }
            match events.next().await {
                Some(Ok(QueryEvent::Data(block))) => Some((Ok(block), (events, false, guard))),
                Some(Ok(QueryEvent::EndOfStream)) => {
                    if let Some(guard) = &mut guard {
                        guard.armed = false;
                    }
                    None
                }
                Some(Err(error)) => Some((Err(error), (events, true, guard))),
                None => Some((
                    Err(postdispatch_error(unexpected_eof())),
                    (events, true, guard),
                )),
            }
        },
    ))
}

#[cfg(test)]
mod terminal_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    struct TrackingRead {
        inner: tokio::io::ReadHalf<tokio::io::DuplexStream>,
        count: Arc<std::sync::atomic::AtomicUsize>,
        notify: Arc<tokio::sync::Notify>,
    }

    impl tokio::io::AsyncRead for TrackingRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let before = buf.filled().len();
            let result = Pin::new(&mut self.inner).poll_read(cx, buf);
            let read = buf.filled().len() - before;
            if read > 0 {
                self.count.fetch_add(read, Ordering::SeqCst);
                self.notify.notify_one();
            }
            result
        }
    }

    struct BlockedWrite {
        inner: tokio::io::WriteHalf<tokio::io::DuplexStream>,
        blocked: Arc<Notify>,
    }

    impl tokio::io::AsyncWrite for BlockedWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
            if result.is_pending() {
                self.blocked.notify_one();
            }
            result
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    fn empty_block() -> Block {
        Block {
            info: BlockInfo::default(),
            rows: 0,
            column_types: IndexMap::new(),
            column_data: IndexMap::new(),
        }
    }

    #[tokio::test]
    async fn partial_packet_survives_concurrent_request() {
        use tokio::io::AsyncWriteExt;
        let (io, mut peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let tracked = TrackingRead {
            inner: read,
            count: count.clone(),
            notify: notify.clone(),
        };
        let mut inner = InnerClient::new(tracked, write, ClientOptions::default());
        inner.input.as_mut().unwrap().server_hello.revision_version = 54428;
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(4);
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (requests, request_rx) = mpsc::channel(4);
        let (_abort, mut abort_rx) = mpsc::unbounded_channel();
        let task =
            tokio::spawn(
                async move { inner.run_controlled(request_rx, &mut abort_rx, false).await },
            );
        // Progress packet id and first field; parser must wait for the remaining fields.
        peer.write_all(&[3, 1]).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while count.load(Ordering::SeqCst) < 2 {
                notify.notified().await;
            }
        })
        .await
        .unwrap();
        let (started, waiting) = oneshot::channel();
        requests
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(ServerPacket::Pong, started),
            })
            .await
            .unwrap();
        waiting.await.unwrap();
        peer.write_all(&[2, 3, 4, 5, 5]).await.unwrap(); // Remaining progress and EndOfStream.
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), events.next())
                .await
                .unwrap(),
            Some(Ok(QueryEvent::EndOfStream))
        ));
        task.abort();
    }

    #[tokio::test]
    async fn malformed_packet_poison_closes_connection() {
        use tokio::io::AsyncWriteExt;
        let (io, mut peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(4);
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (_abort, mut abort_rx) = mpsc::unbounded_channel();
        let task =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        peer.write_all(&[0x80; 10]).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(matches!(
            events.next().await,
            Some(Err(KlickhouseError::QueryOutcomeUnknown(source)))
                if matches!(*source, KlickhouseError::ProtocolError(_))
        ));
        assert!(sender.is_closed());
    }

    #[tokio::test]
    async fn extra_result_packets_fail_closed() {
        for totals in [true, false] {
            let (io, _peer) = tokio::io::duplex(1024);
            let (read, write) = tokio::io::split(io);
            let mut inner = InnerClient::new(read, write, ClientOptions::default());
            let id = Uuid::new_v4();
            let (events, _receiver) = mpsc::channel(1);
            inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
            let data = protocol::ServerData {
                table_name: String::new(),
                block: empty_block(),
            };
            let packet = if totals {
                ServerPacket::Totals(data)
            } else {
                ServerPacket::Extremes(data)
            };
            assert!(matches!(
                inner.receive_packet(packet).await,
                Err(KlickhouseError::ProtocolError(_))
            ));
        }
    }

    #[tokio::test]
    async fn nested_exception_poison_closes_before_reuse_or_terminal_success() {
        use crate::io::ClickhouseWrite;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(4);
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (_abort, mut abort_rx) = mpsc::unbounded_channel();
        let task =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let (reply, pending) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "SELECT 1".into(),
                    settings: IndexMap::new(),
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        let (barrier, queued) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InspectPending(barrier),
            })
            .await
            .unwrap();
        assert_eq!(queued.await.unwrap(), 1);
        peer.write_var_uint(2).await.unwrap(); // Exception packet.
        peer.write_i32_le(123).await.unwrap();
        peer.write_string("outer").await.unwrap();
        peer.write_string("message").await.unwrap();
        peer.write_string("").await.unwrap();
        peer.write_u8(1).await.unwrap(); // Nested exception follows.
        peer.write_u8(5).await.unwrap(); // Could be mistaken for EndOfStream if desynchronized.
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(
            matches!(
                tokio::time::timeout(std::time::Duration::from_secs(1), events.next())
                    .await
                    .unwrap(),
                Some(Err(KlickhouseError::QueryOutcomeUnknown(source)))
                    if matches!(*source, KlickhouseError::ProtocolError(_))
            ),
            "nested exception must be a protocol failure"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(events.next().await.is_none());
        assert!(matches!(
            pending.await,
            Ok(Err(KlickhouseError::QueryNotDispatched))
        ));
        assert!(sender.is_closed());
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty(), "pending query must never be dispatched");
    }

    #[tokio::test]
    async fn late_server_exception_closes_and_discards_queued_write() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(4);
        event_tx
            .send(Ok(QueryEvent::Data(empty_block())))
            .await
            .unwrap();
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (_aborts, mut abort_rx) = mpsc::unbounded_channel();
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let (reply, pending) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "INSERT INTO t VALUES (1)".into(),
                    settings: IndexMap::new(),
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        let (started, wait) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(
                    ServerPacket::Exception(protocol::ServerException {
                        code: 395,
                        name: String::new(),
                        message: String::new(),
                        stack_trace: String::new(),
                        has_nested: false,
                    }),
                    started,
                ),
            })
            .await
            .unwrap();
        wait.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(matches!(events.next().await, Some(Ok(QueryEvent::Data(_)))));
        assert!(matches!(
            events.next().await,
            Some(Err(KlickhouseError::ServerException { .. }))
        ));
        assert!(events.next().await.is_none());
        assert!(matches!(
            pending.await,
            Ok(Err(KlickhouseError::QueryNotDispatched))
        ));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes.len(), 0);
    }

    #[tokio::test]
    async fn active_failure_reports_queued_public_queries_not_dispatched() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (events, _rx) = mpsc::channel(4);
        inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let unbound_client = client.clone();
        let unbound = tokio::spawn(async move {
            matches!(
                unbound_client
                    .query_events("INSERT INTO t VALUES (1)", None)
                    .await,
                Err(KlickhouseError::QueryNotDispatched)
            )
        });
        let bound_client = client.clone();
        let bound = tokio::spawn(async move {
            matches!(
                bound_client
                    .query_bound(
                        "INSERT INTO t SELECT {ntp_a:Int64}",
                        &[("ntp_a", NativeParameter::Int64(1))]
                    )
                    .await,
                Err(KlickhouseError::QueryNotDispatched)
            )
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let (reply, receive) = oneshot::channel();
                sender
                    .send(ClientRequest {
                        data: ClientRequestData::InspectPending(reply),
                    })
                    .await
                    .unwrap();
                if receive.await.unwrap() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (started, wait) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(
                    ServerPacket::Hello(protocol::ServerHello::default()),
                    started,
                ),
            })
            .await
            .unwrap();
        wait.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(
            unbound.await.unwrap(),
            "unbound request lost pre-dispatch status"
        );
        assert!(
            bound.await.unwrap(),
            "bound request lost pre-dispatch status"
        );
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty(), "queued writes must not reach the peer");
        assert!(client.is_closed());
    }

    #[tokio::test]
    async fn partial_query_send_error_retains_original_io_cause() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(1);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let worker = client.clone();
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let query = tokio::spawn(async move {
            matches!(
                worker.query_events("INSERT INTO t VALUES (1)", None).await,
                Err(KlickhouseError::QueryOutcomeUnknown(source)) if matches!(*source, KlickhouseError::Io(ref error) if error.kind() == std::io::ErrorKind::BrokenPipe)
            )
        });
        let mut byte = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer.read_exact(&mut byte),
        )
        .await
        .unwrap()
        .unwrap();
        drop(peer);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), query)
                .await
                .unwrap()
                .unwrap(),
            "dispatch error must preserve original I/O failure"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(client.is_closed());
    }

    #[tokio::test]
    async fn abort_blocked_dispatch_reports_mailbox_requests_not_dispatched() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(1);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let active_client = client.clone();
        let active = tokio::spawn(async move {
            active_client
                .query_events("INSERT INTO t VALUES (1)", None)
                .await
                .is_err()
        });
        let mut first = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer.read_exact(&mut first),
        )
        .await
        .unwrap()
        .unwrap();
        let unbound_client = client.clone();
        let unbound = tokio::spawn(async move {
            matches!(
                unbound_client
                    .query_events("INSERT INTO t VALUES (2)", None)
                    .await,
                Err(KlickhouseError::QueryNotDispatched)
            )
        });
        let bound_client = client.clone();
        let bound = tokio::spawn(async move {
            matches!(
                bound_client
                    .query_bound(
                        "INSERT INTO t SELECT {ntp_a:Int64}",
                        &[("ntp_a", NativeParameter::Int64(3))]
                    )
                    .await,
                Err(KlickhouseError::QueryNotDispatched)
            )
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while sender.capacity() > 6 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        active.abort();
        let _ = active.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(unbound.await.unwrap());
        assert!(bound.await.unwrap());
        assert!(client.is_closed());
    }

    #[tokio::test]
    async fn mailbox_permit_outliving_shutdown_still_reports_not_dispatched() {
        let (io, _peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let id = Uuid::new_v4();
        let (events, _rx) = mpsc::channel(1);
        inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let permit = sender.clone().reserve_owned().await.unwrap();
        let (abort, mut abort_rx) = mpsc::unbounded_channel();
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        abort.send(id).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        let (reply, response) = oneshot::channel();
        let (reply, dispatched) = DispatchReply::new(reply);
        permit.send(ClientRequest {
            data: ClientRequestData::Query {
                id: Uuid::new_v4(),
                cancelled: None,
                query: "INSERT INTO t VALUES (1)".into(),
                settings: IndexMap::new(),
                response: reply,
            },
        });
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                await_dispatch(response, &sender, &dispatched)
            )
            .await
            .unwrap(),
            Err(KlickhouseError::QueryNotDispatched)
        ));
    }

    #[tokio::test]
    async fn dropped_queued_bound_write_is_not_dispatched() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (events, _rx) = mpsc::channel(4);
        inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let write = tokio::spawn(async move {
            client
                .query_bound(
                    "INSERT INTO t SELECT {ntp_a:Int64}",
                    &[("ntp_a", NativeParameter::Int64(1))],
                )
                .await
                .is_err()
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let (reply, receive) = oneshot::channel();
                sender
                    .send(ClientRequest {
                        data: ClientRequestData::InspectPending(reply),
                    })
                    .await
                    .unwrap();
                if receive.await.unwrap() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        write.abort();
        let _ = write.await;
        let (started, wait) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(ServerPacket::EndOfStream, started),
            })
            .await
            .unwrap();
        wait.await.unwrap();
        let (reply, receive) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InspectPending(reply),
            })
            .await
            .unwrap();
        assert_eq!(receive.await.unwrap(), 0);
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), peer.read(&mut byte))
                .await
                .is_err(),
            "cancelled queued write must not produce wire bytes"
        );
        runner.abort();
    }

    #[tokio::test]
    async fn dropped_queued_unbound_write_is_not_dispatched() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (events, _rx) = mpsc::channel(4);
        inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let write = tokio::spawn(async move {
            client
                .query_events("INSERT INTO t VALUES (1)", None)
                .await
                .is_err()
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let (reply, receive) = oneshot::channel();
                sender
                    .send(ClientRequest {
                        data: ClientRequestData::InspectPending(reply),
                    })
                    .await
                    .unwrap();
                if receive.await.unwrap() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        write.abort();
        let _ = write.await;
        let (started, wait) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(ServerPacket::EndOfStream, started),
            })
            .await
            .unwrap();
        wait.await.unwrap();
        let (reply, receive) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InspectPending(reply),
            })
            .await
            .unwrap();
        assert_eq!(receive.await.unwrap(), 0);
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), peer.read(&mut byte))
                .await
                .is_err(),
            "cancelled queued write must not produce wire bytes"
        );
        runner.abort();
    }

    #[tokio::test]
    async fn timed_out_queued_unbound_write_is_not_dispatched() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let id = Uuid::new_v4();
        let (events, _rx) = mpsc::channel(4);
        inner.executing_query = Some((id, events, Arc::new(Mutex::new(None))));
        *inner.active.lock().unwrap() = Some(id);
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                client.query_events("INSERT INTO t VALUES (1)", None),
            )
            .await
            .is_err()
        );
        let (started, wait) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(ServerPacket::EndOfStream, started),
            })
            .await
            .unwrap();
        wait.await.unwrap();
        let (reply, receive) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InspectPending(reply),
            })
            .await
            .unwrap();
        assert_eq!(receive.await.unwrap(), 0);
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
        runner.abort();
    }

    #[tokio::test]
    async fn dropping_unfinished_unbound_stream_poisons_connection() {
        let (io, _peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let stream = client
            .query_events("INSERT INTO t VALUES (1)", None)
            .await
            .unwrap();
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(client.query_events("SELECT 1", None).await.is_err());
    }

    #[tokio::test]
    async fn dropping_full_unbound_stream_interrupts_delivery_and_closes() {
        let (io, _peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender: sender.clone(),
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let events = client
            .query_events("INSERT INTO t VALUES (1)", None)
            .await
            .unwrap();
        for _ in 0..33 {
            let (started, wait) = oneshot::channel();
            sender
                .send(ClientRequest {
                    data: ClientRequestData::InjectPacket(
                        ServerPacket::Data(protocol::ServerData {
                            table_name: String::new(),
                            block: empty_block(),
                        }),
                        started,
                    ),
                })
                .await
                .unwrap();
            wait.await.unwrap();
        }
        drop(events);
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(client.query_events("SELECT 1", None).await.is_err());
    }

    #[tokio::test]
    async fn timed_out_unbound_dispatch_poisons_connection() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(1);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54459;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let writer = client.clone();
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let timed = tokio::spawn(async move {
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                writer.query_events("INSERT INTO t VALUES (1)", None),
            )
            .await
            .is_err()
        });
        let mut byte = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer.read_exact(&mut byte),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), timed)
                .await
                .unwrap()
                .unwrap()
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert!(client.query_events("SELECT 1", None).await.is_err());
    }

    #[tokio::test]
    async fn postdispatch_response_loss_is_unknown_for_events_and_raw() {
        use tokio::io::AsyncReadExt;
        for mode in 0..3 {
            let (io, mut peer) = tokio::io::duplex(4096);
            let (read, write) = tokio::io::split(io);
            let mut inner = InnerClient::new(read, write, ClientOptions::default());
            inner.output.server_hello.revision_version = 54459;
            let (sender, requests) = mpsc::channel(8);
            let (aborts, mut abort_rx) = mpsc::unbounded_channel();
            let (progress, _) = broadcast::channel(1);
            let client = Client {
                sender,
                aborts,
                progress,
                default_settings: IndexMap::new(),
            };
            let runner =
                tokio::spawn(
                    async move { inner.run_controlled(requests, &mut abort_rx, false).await },
                );
            if mode == 2 {
                let mut events = client
                    .query_raw("INSERT INTO t VALUES (1)", None)
                    .await
                    .unwrap();
                let mut byte = [0u8; 1];
                peer.read_exact(&mut byte).await.unwrap(); // Query has reached the peer.
                drop(peer);
                assert!(matches!(
                    tokio::time::timeout(std::time::Duration::from_secs(1), events.next())
                        .await
                        .unwrap(),
                    Some(Err(KlickhouseError::QueryOutcomeUnknown(_)))
                ));
            } else {
                let mut events = if mode == 0 {
                    client
                        .query_events("INSERT INTO t VALUES (1)", None)
                        .await
                        .unwrap()
                } else {
                    client
                        .query_bound(
                            "INSERT INTO t SELECT {ntp_a:Int64}",
                            &[("ntp_a", NativeParameter::Int64(1))],
                        )
                        .await
                        .unwrap()
                };
                let mut byte = [0u8; 1];
                peer.read_exact(&mut byte).await.unwrap();
                drop(peer);
                assert!(matches!(
                    tokio::time::timeout(std::time::Duration::from_secs(1), events.next())
                        .await
                        .unwrap(),
                    Some(Err(KlickhouseError::QueryOutcomeUnknown(_)))
                ));
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), runner)
                .await
                .unwrap()
                .unwrap();
            assert!(client.query_events("SELECT 1", None).await.is_err());
        }
    }

    #[tokio::test]
    async fn bound_write_dispatch_loss_is_unknown_and_cancellation_poisons() {
        use tokio::io::AsyncReadExt;
        for cancel in [false, true] {
            let (io, mut peer) = tokio::io::duplex(1);
            let (read, write) = tokio::io::split(io);
            let mut inner = InnerClient::new(read, write, ClientOptions::default());
            inner.output.server_hello.revision_version = 54459;
            let (sender, requests) = mpsc::channel(8);
            let (aborts, mut abort_rx) = mpsc::unbounded_channel();
            let (progress, _) = broadcast::channel(1);
            let client = Client {
                sender,
                aborts,
                progress,
                default_settings: IndexMap::new(),
            };
            let writer = client.clone();
            let runner =
                tokio::spawn(
                    async move { inner.run_controlled(requests, &mut abort_rx, false).await },
                );
            let query = tokio::spawn(async move {
                matches!(
                    writer
                        .query_bound(
                            "INSERT INTO t SELECT {ntp_a:Int64}",
                            &[("ntp_a", NativeParameter::Int64(1))]
                        )
                        .await,
                    Err(KlickhouseError::QueryOutcomeUnknown(_))
                )
            });
            let mut byte = [0u8; 1];
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                peer.read_exact(&mut byte),
            )
            .await
            .unwrap()
            .unwrap();
            if cancel {
                query.abort();
                let _ = query.await;
            } else {
                drop(peer);
                assert!(
                    tokio::time::timeout(std::time::Duration::from_secs(1), query)
                        .await
                        .unwrap()
                        .unwrap()
                );
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), runner)
                .await
                .unwrap()
                .unwrap();
            assert!(
                client.query_events("SELECT 1", None).await.is_err(),
                "poisoned connection reused"
            );
        }
    }

    #[tokio::test]
    async fn eof_after_data_is_an_error_not_success() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(QueryEvent::Data(empty_block()))).await.unwrap();
        drop(tx);
        let mut blocks = terminal_blocks(QueryEvents::new(rx, Arc::new(Mutex::new(None))));
        assert!(blocks.next().await.unwrap().is_ok());
        assert!(matches!(
            blocks.next().await.unwrap(),
            Err(KlickhouseError::QueryOutcomeUnknown(source))
                if matches!(*source, KlickhouseError::ProtocolError(_))
        ));
        assert!(blocks.next().await.is_none());
    }

    #[tokio::test]
    async fn native_binding_fails_before_dispatch_for_every_candidate_type() {
        let (sender, mut requests) = mpsc::channel(4);
        let (aborts, _abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let values = [
            ("a", NativeParameter::Int64(-1)),
            ("b", NativeParameter::UInt64(1)),
            ("c", NativeParameter::Text("'); DROP TABLE t; --")),
            ("d", NativeParameter::Bytes(b"\0'\\\xff")),
            ("e", NativeParameter::Null),
        ];
        assert!(
            client
                .query_bound("SELECT {a:Int64}", &values)
                .await
                .is_err()
        );
        for unsupported in [
            NativeParameter::Text("'); DROP TABLE t; --"),
            NativeParameter::Bytes(b"\0'\\\xff"),
            NativeParameter::Null,
        ] {
            assert!(
                client
                    .query_bound("SELECT {ntp_a:Int64}", &[("ntp_a", unsupported)])
                    .await
                    .is_err()
            );
        }
        assert!(
            client
                .query_bound(
                    "SELECT {ntp_a:Nullable(Int64)}",
                    &[("ntp_a", NativeParameter::Null)]
                )
                .await
                .is_err()
        );
        let oversize = vec![0u8; 8193];
        assert!(
            client
                .query_bound(
                    "SELECT {ntp_a:String}",
                    &[("ntp_a", NativeParameter::Bytes(&oversize))]
                )
                .await
                .is_err()
        );
        for unsupported in [
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::NAN,
        ] {
            assert!(
                client
                    .query_bound(
                        "SELECT {ntp_f:Float64}",
                        &[("ntp_f", NativeParameter::Float64(unsupported))]
                    )
                    .await
                    .is_err()
            );
        }
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(protocol::DBMS_TCP_PROTOCOL_VERSION, 54459);
        assert_eq!(protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS, 54459);
    }

    #[test]
    fn native_parameter_names_arity_and_sql_subset_are_fail_closed() {
        assert_eq!(
            encode_bound(
                "SELECT {p1:Int64}, {p2:Int64}",
                &[
                    ("p1", NativeParameter::Int64(1)),
                    ("p2", NativeParameter::Int64(2)),
                ]
            )
            .unwrap(),
            vec![
                ("p1".to_string(), "'1'".to_string()),
                ("p2".to_string(), "'2'".to_string())
            ]
        );
        for invalid in ["p0", "p01", "p-1", "p1234567890"] {
            assert!(
                encode_bound(
                    &format!("SELECT {{{invalid}:Int64}}"),
                    &[(invalid, NativeParameter::Int64(1))]
                )
                .is_err()
            );
        }
        assert_eq!(
            encode_bound(
                "SELECT {ntp_b:Int64}, {ntp_a:Int64}, {ntp_b:Int64}",
                &[
                    ("ntp_b", NativeParameter::Int64(i64::MIN)),
                    ("ntp_a", NativeParameter::Int64(i64::MAX))
                ]
            )
            .unwrap(),
            vec![
                ("ntp_a".to_string(), format!("'{}'", i64::MAX)),
                ("ntp_b".to_string(), format!("'{}'", i64::MIN))
            ]
        );
        for query in [
            "SELECT '{ntp_a:Int64}'",
            "SELECT {ntp_a:Int64} -- comment",
            "SELECT {ntp_a:Int64}; SELECT 2",
            "SELECT {ntp_a:UInt64}",
            "SELECT {ntp_other:Int64}",
            "SELECT {ntp_a:Int64} /* x */",
            "INSERT INTO t SELECT 1 // {ntp_a:Int64}",
            "SELECT $tag${ntp_a:Int64}$tag$",
            "SELECT $${ntp_a:Int64}$$",
        ] {
            assert!(
                encode_bound(query, &[("ntp_a", NativeParameter::Int64(1))]).is_err(),
                "{query}"
            );
        }
        assert!(encode_bound("SELECT {ntp_a:Int64}", &[]).is_err());
        assert!(
            encode_bound(
                "SELECT {ntp_a:Int64}",
                &[
                    ("ntp_a", NativeParameter::Int64(1)),
                    ("ntp_unused", NativeParameter::Int64(2))
                ]
            )
            .is_err()
        );
        assert!(
            encode_bound(
                "SELECT {ntp_a:Int64}",
                &[
                    ("ntp_a", NativeParameter::Int64(1)),
                    ("ntp_a", NativeParameter::Int64(2))
                ]
            )
            .is_err()
        );
        assert!(
            encode_bound(
                "SELECT {max_threads:Int64}",
                &[("max_threads", NativeParameter::Int64(1))]
            )
            .is_err()
        );
        for parameter in [
            NativeParameter::Text("'); DROP TABLE t; --"),
            NativeParameter::Bytes(b"\0'\\\xff"),
            NativeParameter::Null,
            NativeParameter::UInt64(1),
        ] {
            assert!(encode_bound("SELECT {ntp_a:Int64}", &[("ntp_a", parameter)]).is_err());
        }
    }

    #[test]
    fn bool_and_restricted_float_parameter_encodings_are_typed() {
        assert_eq!(
            encode_bound(
                "SELECT {ntp_b:Bool}, {ntp_f:Float64}",
                &[
                    ("ntp_b", NativeParameter::Bool(true)),
                    ("ntp_f", NativeParameter::Float64(-0.0))
                ],
            )
            .unwrap(),
            vec![
                ("ntp_b".into(), "'1'".into()),
                ("ntp_f".into(), "'-0'".into())
            ],
        );
        for value in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN_POSITIVE,
            1.5,
        ] {
            assert!(
                encode_bound(
                    "SELECT {ntp_f:Float64}",
                    &[("ntp_f", NativeParameter::Float64(value))]
                )
                .is_err()
            );
        }
        assert!(
            encode_bound(
                "SELECT {ntp_b:Int64}",
                &[("ntp_b", NativeParameter::Bool(true))]
            )
            .is_err()
        );
    }

    #[test]
    fn binary_parameter_dump_uses_two_escaped_layers() {
        assert_eq!(byte_parameter_dump(b"\0'A").unwrap(), r"'\\x00\\x27\\x41'");
        assert_eq!(byte_parameter_dump(b"").unwrap(), "''");
        assert!(byte_parameter_dump(&vec![0; 8193]).is_err());
    }

    #[tokio::test]
    async fn bound_query_revision_gate_rejects_without_wire_bytes_and_allows_reuse() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54458;
        let (sender, requests) = mpsc::channel(8);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let task =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        assert!(matches!(
            client.query_bound("SELECT {ntp_a:Int64}", &[("ntp_a", NativeParameter::Int64(1))]).await,
            Err(KlickhouseError::ProtocolError(ref reason)) if reason.contains("54459")
        ));
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
        let _events = client.query_events("SELECT 1", None).await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        task.abort();
    }

    #[tokio::test]
    async fn explicit_terminal_packet_allows_completion() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(QueryEvent::Data(empty_block()))).await.unwrap();
        tx.send(Ok(QueryEvent::EndOfStream)).await.unwrap();
        drop(tx);
        let mut blocks = terminal_blocks(QueryEvents::new(rx, Arc::new(Mutex::new(None))));
        assert!(blocks.next().await.unwrap().is_ok());
        assert!(blocks.next().await.is_none());
    }

    #[tokio::test]
    async fn server_exception_is_not_success() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Err(KlickhouseError::ProtocolError("injected".into())))
            .await
            .unwrap();
        drop(tx);
        let mut blocks = terminal_blocks(QueryEvents::new(rx, Arc::new(Mutex::new(None))));
        assert!(blocks.next().await.unwrap().is_err());
        assert!(blocks.next().await.is_none());
    }

    #[tokio::test]
    async fn stale_data_is_rejected_without_disturbing_active_query() {
        let (io, _peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let active = Uuid::new_v4();
        let (events, _receiver) = mpsc::channel(1);
        inner.executing_query = Some((active, events, Arc::new(Mutex::new(None))));
        let (reply, response) = oneshot::channel();
        inner
            .handle_request(ClientRequest {
                data: ClientRequestData::SendData {
                    id: Uuid::new_v4(),
                    block: empty_block(),
                    response: reply,
                },
            })
            .await
            .unwrap();
        assert!(response.await.unwrap().is_err());
        assert_eq!(inner.executing_query.as_ref().unwrap().0, active);
    }

    #[tokio::test]
    async fn abort_before_request_prevents_dispatch() {
        let (io, _peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let (reply, _response) = oneshot::channel();
        inner
            .handle_request(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: Some(Arc::new(AtomicBool::new(true))),
                    query: "SELECT 1".to_string(),
                    settings: IndexMap::new(),
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        assert!(inner.executing_query.is_none());
    }

    #[tokio::test]
    async fn old_revision_rejects_filtered_settings_without_poisoning_session() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54428;
        let (reply, response) = oneshot::channel();
        let mut settings = IndexMap::new();
        settings.insert("opentelemetry_traceparent".into(), "not-a-trace".into());
        inner
            .handle_request(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "SELECT 1".into(),
                    settings,
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            response.await.unwrap(),
            Err(KlickhouseError::ProtocolError(_))
        ));
        assert!(inner.executing_query.is_none());
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
        let (reply, response) = oneshot::channel();
        inner
            .handle_request(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "SELECT 1".into(),
                    settings: IndexMap::new(),
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(response.await, Ok(Ok(_))));
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn old_revision_insert_rejection_is_predispatch_for_all_entrypoints() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        inner.output.server_hello.revision_version = 54428;
        let (sender, requests) = mpsc::channel(16);
        let (aborts, mut abort_rx) = mpsc::unbounded_channel();
        let (progress, _) = broadcast::channel(1);
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings: IndexMap::new(),
        };
        let task =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, false).await });
        let mut settings = IndexMap::new();
        settings.insert("max_execution_time".into(), "1".into());
        let result = client
            .insert_native_raw(
                "INSERT INTO t FORMAT Native",
                Box::pin(stream::iter(vec![empty_block()])),
                Some(settings.clone()),
            )
            .await;
        assert!(
            matches!(result, Err(KlickhouseError::ProtocolError(ref message)) if message.contains("54429")),
            "raw insert must return the original pre-dispatch error"
        );
        let mut typed = client.clone();
        typed.default_settings = settings;
        let result = typed
            .insert_native::<RawRow>("INSERT INTO t FORMAT Native", Box::pin(stream::empty()))
            .await;
        assert!(
            matches!(result, Err(KlickhouseError::ProtocolError(ref message)) if message.contains("54429"))
        );
        let result = typed
            .insert_native_block::<RawRow>("INSERT INTO t FORMAT Native", vec![])
            .await;
        assert!(
            matches!(result, Err(KlickhouseError::ProtocolError(ref message)) if message.contains("54429"))
        );
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
        let _events = client.query_events("SELECT 1", None).await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            1
        );
        task.abort();
    }

    #[tokio::test]
    async fn interrupted_dispatch_remains_unknown_for_raw_and_typed_insert() {
        use tokio::io::AsyncReadExt;
        for raw in [true, false] {
            let (io, mut peer) = tokio::io::duplex(1);
            let (read, write) = tokio::io::split(io);
            let mut inner = InnerClient::new(read, write, ClientOptions::default());
            inner.output.server_hello.revision_version = 54428;
            let (sender, requests) = mpsc::channel(16);
            let (aborts, mut abort_rx) = mpsc::unbounded_channel();
            let (progress, _) = broadcast::channel(1);
            let client = Client {
                sender,
                aborts,
                progress,
                default_settings: IndexMap::new(),
            };
            let task =
                tokio::spawn(
                    async move { inner.run_controlled(requests, &mut abort_rx, false).await },
                );
            let insert = tokio::spawn(async move {
                if raw {
                    matches!(
                        client
                            .insert_native_raw(
                                "INSERT INTO t FORMAT Native",
                                Box::pin(stream::empty()),
                                None
                            )
                            .await,
                        Err(KlickhouseError::InsertOutcomeUnknown(_))
                    )
                } else {
                    matches!(
                        client
                            .insert_native::<RawRow>(
                                "INSERT INTO t FORMAT Native",
                                Box::pin(stream::empty())
                            )
                            .await,
                        Err(KlickhouseError::InsertOutcomeUnknown(_))
                    )
                }
            });
            let mut byte = [0u8; 1];
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                peer.read_exact(&mut byte),
            )
            .await
            .unwrap()
            .unwrap();
            drop(peer);
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(1), insert)
                    .await
                    .unwrap()
                    .unwrap(),
                "partial query dispatch must be unknown"
            );
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn invalid_or_unsupported_trace_settings_are_not_silently_dropped() {
        use tokio::io::AsyncReadExt;
        for (revision, key, value) in [
            (
                54441,
                "opentelemetry_traceparent",
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            ),
            (54459, "opentelemetry_traceparent", "invalid"),
            (54459, "opentelemetry_tracestate", "vendor=value"),
        ] {
            let (io, mut peer) = tokio::io::duplex(4096);
            let (read, write) = tokio::io::split(io);
            let mut inner = InnerClient::new(read, write, ClientOptions::default());
            inner.output.server_hello.revision_version = revision;
            let (reply, response) = oneshot::channel();
            let mut settings = IndexMap::new();
            settings.insert(key.into(), value.into());
            inner
                .handle_request(ClientRequest {
                    data: ClientRequestData::Query {
                        id: Uuid::new_v4(),
                        cancelled: None,
                        query: "SELECT 1".into(),
                        settings,
                        response: reply.into(),
                    },
                })
                .await
                .unwrap();
            assert!(matches!(
                response.await.unwrap(),
                Err(KlickhouseError::ProtocolError(_))
            ));
            let mut byte = [0u8; 1];
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut byte))
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn abort_interrupts_blocked_socket_write_and_drops_pending_requests() {
        use tokio::io::AsyncReadExt;
        let (io, mut peer) = tokio::io::duplex(1);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(1);
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (requests, request_rx) = mpsc::channel(4);
        let (abort, mut abort_rx) = mpsc::unbounded_channel();
        let task =
            tokio::spawn(
                async move { inner.run_controlled(request_rx, &mut abort_rx, false).await },
            );
        let (reply, mut response) = oneshot::channel();
        requests
            .send(ClientRequest {
                data: ClientRequestData::SendData {
                    id,
                    block: empty_block(),
                    response: reply,
                },
            })
            .await
            .unwrap();
        let mut byte = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer.read_exact(&mut byte),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        abort.send(Uuid::new_v4()).unwrap();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "stale abort must not poison active query"
        );
        let (pending_reply, pending) = oneshot::channel();
        requests
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "SELECT 1".into(),
                    settings: IndexMap::new(),
                    response: pending_reply.into(),
                },
            })
            .await
            .unwrap();
        abort.send(id).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(response.await.is_err());
        assert!(matches!(
            pending.await,
            Ok(Err(KlickhouseError::QueryNotDispatched))
        ));
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(matches!(
            events.next().await,
            Some(Err(KlickhouseError::QueryOutcomeUnknown(source)))
                if matches!(*source, KlickhouseError::ProtocolError(_))
        ));
        assert!(events.next().await.is_none());
        // Any partially written bytes are unusable; the transport must subsequently reach EOF.
        let mut rest = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer.read_to_end(&mut rest),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn abort_interrupts_saturated_event_delivery_without_losing_error() {
        let (io, _peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(io);
        let mut inner = InnerClient::new(read, write, ClientOptions::default());
        let id = Uuid::new_v4();
        let (event_tx, event_rx) = mpsc::channel(1);
        event_tx
            .send(Ok(QueryEvent::Data(empty_block())))
            .await
            .unwrap();
        let failure = Arc::new(Mutex::new(None));
        inner.executing_query = Some((id, event_tx, failure.clone()));
        *inner.active.lock().unwrap() = Some(id);
        let (requests, request_rx) = mpsc::channel(4);
        let (abort, mut abort_rx) = mpsc::unbounded_channel();
        let task =
            tokio::spawn(
                async move { inner.run_controlled(request_rx, &mut abort_rx, false).await },
            );
        let (started, waiting) = oneshot::channel();
        requests
            .send(ClientRequest {
                data: ClientRequestData::InjectPacket(
                    ServerPacket::Data(protocol::ServerData {
                        table_name: String::new(),
                        block: empty_block(),
                    }),
                    started,
                ),
            })
            .await
            .unwrap();
        waiting.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        abort.send(Uuid::new_v4()).unwrap();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "stale abort must not interrupt event delivery"
        );
        abort.send(id).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        let mut events = QueryEvents::new(event_rx, failure);
        assert!(matches!(events.next().await, Some(Ok(QueryEvent::Data(_)))));
        assert!(matches!(
            events.next().await,
            Some(Err(KlickhouseError::QueryOutcomeUnknown(source)))
                if matches!(*source, KlickhouseError::ProtocolError(_))
        ));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn cancelling_connect_closes_a_proven_blocked_initial_set() {
        use tokio::io::AsyncReadExt;
        let (client, peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(client);
        let (mut peer_read, mut peer_write) = tokio::io::split(peer);
        let blocked = Arc::new(Notify::new());
        let write = BlockedWrite {
            inner: write,
            blocked: blocked.clone(),
        };
        let mut options = ClientOptions::default();
        options
            .settings
            .insert("large".into(), "x".repeat(1024 * 1024));
        let connect = tokio::spawn(Client::connect_stream(read, write, options));

        // The hello fits in the duplex buffer; drain it before sending the server hello.
        let mut hello = [0; 1024];
        assert!(peer_read.read(&mut hello).await.unwrap() > 0);
        peer_write.write_var_uint(0).await.unwrap();
        peer_write.write_string("test").await.unwrap();
        peer_write.write_var_uint(1).await.unwrap();
        peer_write.write_var_uint(2).await.unwrap();
        peer_write.write_var_uint(54459).await.unwrap();
        peer_write.write_string("UTC").await.unwrap();
        peer_write.write_string("display").await.unwrap();
        peer_write.write_var_uint(9).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), blocked.notified())
            .await
            .expect("initial SET must actually block in poll_write");
        connect.abort();
        assert!(matches!(connect.await, Err(error) if error.is_cancelled()));
        let mut rest = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer_read.read_to_end(&mut rest),
        )
        .await
        .expect("cancelled connect must close the stream")
        .unwrap();
        assert!(
            rest.len() < 100_000,
            "SET continued writing after cancellation"
        );
    }

    #[tokio::test]
    async fn initialization_guard_aborts_buffered_dispatch_without_query_abort() {
        use tokio::io::AsyncReadExt;
        let (client, peer) = tokio::io::duplex(1024);
        let (read, write) = tokio::io::split(client);
        let (mut peer_read, mut peer_write) = tokio::io::split(peer);
        let blocked = Arc::new(Notify::new());
        let inner = InnerClient::new(
            read,
            BlockedWrite {
                inner: write,
                blocked: blocked.clone(),
            },
            ClientOptions::default(),
        );
        let (sender, requests) = mpsc::channel(1);
        let (_abort_sender, aborts) = mpsc::unbounded_channel();
        let (reply, _response) = oneshot::channel();
        let mut settings = IndexMap::new();
        settings.insert("large".into(), "x".repeat(1024 * 1024));
        sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id: Uuid::new_v4(),
                    cancelled: None,
                    query: "SET date_time_input_format='best_effort'".into(),
                    settings,
                    response: reply.into(),
                },
            })
            .await
            .unwrap();
        let guard = InitializationTask(Some(tokio::spawn(inner.run(requests, aborts))));
        let mut hello = [0; 1024];
        assert!(peer_read.read(&mut hello).await.unwrap() > 0);
        // The one-slot mailbox becoming free proves initialization took the request
        // before the server hello was delivered.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while sender.capacity() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial SET was not buffered");
        peer_write.write_var_uint(0).await.unwrap();
        peer_write.write_string("test").await.unwrap();
        peer_write.write_var_uint(1).await.unwrap();
        peer_write.write_var_uint(2).await.unwrap();
        peer_write.write_var_uint(54459).await.unwrap();
        peer_write.write_string("UTC").await.unwrap();
        peer_write.write_string("display").await.unwrap();
        peer_write.write_var_uint(9).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), blocked.notified())
            .await
            .expect("buffered SET did not block");
        drop(guard);
        let mut rest = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            peer_read.read_to_end(&mut rest),
        )
        .await
        .expect("guard did not close blocked transport")
        .unwrap();
        assert!(
            rest.len() < 100_000,
            "blocked dispatch continued after cancellation"
        );
        drop(sender);
    }

    #[tokio::test]
    async fn fragmented_hello_survives_a_mailbox_request_during_initialization() {
        let (client_io, peer_io) = tokio::io::duplex(4096);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (mut peer_read, mut peer_write) = tokio::io::split(peer_io);
        // Drain the client's hello/addendum so its writes cannot block.
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut peer_read, &mut tokio::io::sink()).await;
        });

        let count = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let reader = TrackingRead {
            inner: client_read,
            count: count.clone(),
            notify: notify.clone(),
        };
        let inner = InnerClient::new(reader, client_write, ClientOptions::default());
        let (sender, requests) = mpsc::channel(4);
        let (_aborts, mut abort_rx) = mpsc::unbounded_channel();
        let runner =
            tokio::spawn(async move { inner.run_controlled(requests, &mut abort_rx, true).await });

        let (first_written, wrote_first) = oneshot::channel();
        let (release, release_rest) = oneshot::channel();
        let peer = tokio::spawn(async move {
            peer_write.write_var_uint(0).await.unwrap();
            first_written.send(()).unwrap();
            release_rest.await.unwrap();
            peer_write.write_string("test").await.unwrap();
            peer_write.write_var_uint(1).await.unwrap();
            peer_write.write_var_uint(2).await.unwrap();
            peer_write.write_var_uint(54459).await.unwrap();
            peer_write.write_string("UTC").await.unwrap();
            peer_write.write_string("display").await.unwrap();
            peer_write.write_var_uint(9).await.unwrap();
        });

        wrote_first.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while count.load(Ordering::SeqCst) == 0 {
                notify.notified().await;
            }
        })
        .await
        .expect("client must consume the first hello byte");

        // The hello parser is now mid-packet. Deliver a mailbox event before the rest of
        // the hello. A recreated `receive_hello` future would restart at the string length
        // and fail; the pinned initialization future resumes in place.
        let (reply, inspect) = oneshot::channel();
        sender
            .send(ClientRequest {
                data: ClientRequestData::InspectPending(reply),
            })
            .await
            .unwrap();
        release.send(()).unwrap();

        assert_eq!(inspect.await.unwrap(), 0);
        drop(sender);
        tokio::time::timeout(std::time::Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), peer)
            .await
            .unwrap()
            .unwrap();
    }
}

struct InnerClient<R: ClickhouseRead, W: ClickhouseWrite> {
    input: Option<InternalClientIn<R>>,
    output: InternalClientOut<W>,
    options: ClientOptions,
    pending_queries: VecDeque<PendingQuery>,
    dispatch_response: Option<DispatchReply>,
    executing_query: Option<(Uuid, mpsc::Sender<Result<QueryEvent>>, FailureSlot)>,
    active: Arc<Mutex<Option<Uuid>>>,
    progress: broadcast::Sender<(Uuid, Progress)>,
}

struct PendingQuery {
    id: Uuid,
    cancelled: Option<Arc<AtomicBool>>,
    query: String,
    settings: IndexMap<String, String>,
    parameters: Vec<(String, String)>,
    response: DispatchReply,
}

/// Carries the dispatch boundary with the request, including while it is still
/// in a sender's reserved mailbox slot. Dropping any unsent request yields an
/// authoritative not-dispatched acknowledgement.
struct DispatchReply {
    sender: Option<oneshot::Sender<Result<QueryEvents>>>,
    dispatched: Arc<AtomicBool>,
}

impl From<oneshot::Sender<Result<QueryEvents>>> for DispatchReply {
    fn from(sender: oneshot::Sender<Result<QueryEvents>>) -> Self {
        Self::new(sender).0
    }
}

impl DispatchReply {
    fn new(sender: oneshot::Sender<Result<QueryEvents>>) -> (Self, Arc<AtomicBool>) {
        let dispatched = Arc::new(AtomicBool::new(false));
        (
            Self {
                sender: Some(sender),
                dispatched: dispatched.clone(),
            },
            dispatched,
        )
    }

    fn send(mut self, result: Result<QueryEvents>) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

impl Drop for DispatchReply {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let error = if self.dispatched.load(Ordering::Acquire) {
                KlickhouseError::QueryOutcomeUnknown(Box::new(KlickhouseError::ProtocolError(
                    "query dispatch was interrupted".to_string(),
                )))
            } else {
                KlickhouseError::QueryNotDispatched
            };
            let _ = sender.send(Err(error));
        }
    }
}

impl<R: ClickhouseRead + 'static, W: ClickhouseWrite> InnerClient<R, W> {
    pub fn new(reader: R, writer: W, options: ClientOptions) -> Self {
        Self {
            input: Some(InternalClientIn::new(reader)),
            output: InternalClientOut::new(writer),
            options,
            pending_queries: VecDeque::new(),
            dispatch_response: None,
            executing_query: None,
            active: Arc::new(Mutex::new(None)),
            progress: broadcast::channel(PROGRESS_CAPACITY).0,
        }
    }

    async fn dispatch_query(&mut self, query: PendingQuery) -> Result<()> {
        if query
            .cancelled
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Ok(());
        }
        let revision = self.output.server_hello.revision_version;
        if !query.parameters.is_empty()
            && revision < protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
        {
            query.response.send(Err(KlickhouseError::ProtocolError(
                "native parameters require protocol revision 54459".to_string(),
            )));
            return Ok(());
        }
        let traceparent = query.settings.get("opentelemetry_traceparent");
        let tracestate = query.settings.get("opentelemetry_tracestate");
        let checked =
            check_settings_revision(revision, !query.settings.is_empty()).and_then(|()| {
                if (traceparent.is_some() || tracestate.is_some())
                    && revision < protocol::DBMS_MIN_REVISION_WITH_OPENTELEMETRY
                {
                    return Err(KlickhouseError::ProtocolError(
                        "OpenTelemetry requires protocol revision 54442".to_string(),
                    ));
                }
                match (traceparent, tracestate) {
                    (None, Some(_)) => Err(KlickhouseError::ProtocolError(
                        "OpenTelemetry tracestate requires traceparent".to_string(),
                    )),
                    (Some(parent), state) => {
                        OpenTelemetry::from_traceparent(parent, state.map_or("", String::as_str))
                            .map(Some)
                            .map_err(|_| {
                                KlickhouseError::ProtocolError(
                                    "invalid OpenTelemetry trace context".to_string(),
                                )
                            })
                    }
                    (None, None) => Ok(None),
                }
            });
        let open_telemetry = match checked {
            Ok(telemetry) => telemetry,
            Err(error) => {
                query.response.send(Err(error));
                return Ok(());
            }
        };
        let id = query.id;
        let response = query.response;
        response.dispatched.store(true, Ordering::Release);
        self.dispatch_response = Some(response);
        *self.active.lock().unwrap_or_else(|e| e.into_inner()) = Some(id);

        let settings_vec: Vec<(String, String)> = query
            .settings
            .into_iter()
            .filter(|(k, _)| k != "opentelemetry_traceparent" && k != "opentelemetry_tracestate")
            .collect();
        self.output
            .send_query(Query {
                id: &id.to_string(),
                info: ClientInfo {
                    kind: QueryKind::InitialQuery,
                    initial_user: "",
                    initial_query_id: "",
                    initial_address: "0.0.0.0:0",
                    os_user: "",
                    client_hostname: "localhost",
                    client_name: CLIENT_NAME,
                    client_version_major: crate::VERSION_MAJOR,
                    client_version_minor: crate::VERSION_MINOR,
                    client_tcp_protocol_version: protocol::DBMS_TCP_PROTOCOL_VERSION,
                    quota_key: "",
                    distributed_depth: 1,
                    client_version_patch: 1,
                    open_telemetry,
                },
                settings: &settings_vec,
                parameters: &query.parameters,
                stage: QueryProcessingStage::Complete,
                compression: CompressionMethod::None,
                query: &query.query,
            })
            .await?;

        let (sender, receiver) = mpsc::channel(32);
        let failure = Arc::new(Mutex::new(None));
        if let Some(response) = self.dispatch_response.take() {
            response.send(Ok(QueryEvents::new(receiver, failure.clone())));
        }
        self.executing_query = Some((id, sender, failure));
        self.output
            .send_data(
                Block {
                    info: BlockInfo::default(),
                    rows: 0,
                    column_types: IndexMap::new(),
                    column_data: IndexMap::new(),
                },
                CompressionMethod::None,
                "",
                false,
            )
            .await?;
        Ok(())
    }

    async fn dispatch_pending(&mut self) -> Result<()> {
        while let Some(query) = self.pending_queries.pop_front() {
            if query
                .cancelled
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire))
            {
                continue;
            }
            self.dispatch_query(query).await?;
            if self.executing_query.is_some() {
                break;
            }
        }
        Ok(())
    }

    async fn handle_request(&mut self, request: ClientRequest) -> Result<()> {
        match request.data {
            ClientRequestData::Query {
                id,
                cancelled,
                query,
                settings,
                response,
            } => {
                if cancelled
                    .as_ref()
                    .is_some_and(|flag| flag.load(Ordering::Acquire))
                {
                    return Ok(());
                }
                let query = PendingQuery {
                    id,
                    cancelled,
                    query,
                    settings,
                    parameters: Vec::new(),
                    response,
                };
                if self.pending_queries.is_empty() && self.executing_query.is_none() {
                    self.dispatch_query(query).await?;
                } else {
                    self.pending_queries.push_back(query);
                }
            }
            ClientRequestData::BoundQuery {
                id,
                cancelled,
                query,
                settings,
                parameters,
                response,
            } => {
                let query = PendingQuery {
                    id,
                    cancelled: Some(cancelled),
                    query,
                    settings,
                    parameters,
                    response,
                };
                if self.pending_queries.is_empty() && self.executing_query.is_none() {
                    self.dispatch_query(query).await?;
                } else {
                    self.pending_queries.push_back(query);
                }
            }
            ClientRequestData::SendData {
                id,
                block,
                response,
            } => {
                if !self
                    .executing_query
                    .as_ref()
                    .is_some_and(|(active, _, _)| *active == id)
                {
                    let _ = response.send(Err(KlickhouseError::ProtocolError(
                        "data belongs to a stale query".to_string(),
                    )));
                    return Ok(());
                }
                self.output
                    .send_data(block, CompressionMethod::None, "", false)
                    .await?;
                response.send(Ok(())).ok();
            }
            #[cfg(test)]
            ClientRequestData::InjectPacket(packet, started) => {
                let _ = started.send(());
                self.receive_packet(packet).await?;
            }
            #[cfg(test)]
            ClientRequestData::InspectPending(reply) => {
                let _ = reply.send(self.pending_queries.len());
            }
        }
        Ok(())
    }

    async fn receive_packet(&mut self, packet: ServerPacket) -> Result<()> {
        match packet {
            ServerPacket::Hello(_) => {
                return Err(KlickhouseError::ProtocolError(
                    "unexpected retransmission of server hello".to_string(),
                ));
            }
            ServerPacket::Data(block) => {
                if let Some((_, current, _)) = self.executing_query.as_ref() {
                    current.send(Ok(QueryEvent::Data(block.block))).await.ok();
                } else {
                    return Err(KlickhouseError::ProtocolError(
                        "received data block, but no pending queries".to_string(),
                    ));
                }
            }
            ServerPacket::Exception(e) => {
                // A failed query can have effects. Do not dispatch queued work on this
                // connection after an exception; the controller closes and reports it.
                return Err(e.emit());
            }
            ServerPacket::Progress(progress) => {
                if let Some((id, _, _)) = &self.executing_query {
                    let _ = self.progress.send((*id, progress));
                }
            }
            ServerPacket::Pong => {}
            ServerPacket::EndOfStream => {
                let Some((_, current, _)) = self.executing_query.as_ref() else {
                    return Err(KlickhouseError::ProtocolError(
                        "received end of stream, but no executing query".to_string(),
                    ));
                };
                current.send(Ok(QueryEvent::EndOfStream)).await.ok();
                self.executing_query.take();
                *self.active.lock().unwrap_or_else(|e| e.into_inner()) = None;
                self.dispatch_pending().await?;
            }
            ServerPacket::ProfileInfo(_) => {}
            ServerPacket::Totals(_) | ServerPacket::Extremes(_) => {
                return Err(KlickhouseError::ProtocolError(
                    "additional native result sets are unsupported".to_string(),
                ));
            }
            ServerPacket::TablesStatusResponse(_) => {}
            ServerPacket::Log(_) => {}
            ServerPacket::TableColumns(_) => {}
            ServerPacket::PartUUIDs(_) => {}
            ServerPacket::ReadTaskRequest => {}
            ServerPacket::ProfileEvents(_) => {}
        }
        Ok(())
    }

    async fn run_inner(
        &mut self,
        input: &mut Receiver<ClientRequest>,
        initialize: bool,
    ) -> Result<()> {
        let mut incoming = self.input.take().ok_or_else(|| {
            KlickhouseError::ProtocolError("native input unavailable".to_string())
        })?;
        if initialize {
            let mut buffered = VecDeque::new();
            {
                let output = &mut self.output;
                let options = &self.options;
                let client_hello = ClientHello {
                    default_database: &options.default_database,
                    username: &options.username,
                    password: &options.password,
                };
                // Keep one pinned initialization future across mailbox events. Recreating it
                // would discard a partially received hello, and selecting only around
                // `receive_hello` would leave a blocked hello/addendum write uninterruptible.
                let initialization = async {
                    output.send_hello(client_hello).await?;
                    let hello = incoming.receive_hello().await?;
                    incoming.server_hello = hello.clone();
                    output.server_hello = hello.clone();
                    output.send_addendum().await?;
                    Ok::<_, KlickhouseError>(())
                };
                tokio::pin!(initialization);
                loop {
                    tokio::select! {
                        result = &mut initialization => break result?,
                        request = input.recv() => match request {
                            Some(request) => buffered.push_back(request),
                            None => return Ok(()),
                        },
                    }
                }
            }
            for request in buffered {
                self.handle_request(request).await?;
            }
        }

        loop {
            // Do not recreate a partially read packet when a request arrives. Its parser
            // retains consumed bytes until it completes or the entire connection is aborted.
            let packet = incoming.receive_packet();
            tokio::pin!(packet);
            let packet = loop {
                select! {
                    request = input.recv() => {
                        let Some(request) = request else { return Ok(()); };
                        self.handle_request(request).await?;
                    },
                    packet = &mut packet => break packet?,
                }
            };
            self.receive_packet(packet).await?;
        }
    }

    pub async fn run(
        self,
        input: Receiver<ClientRequest>,
        mut aborts: mpsc::UnboundedReceiver<Uuid>,
    ) {
        self.run_controlled(input, &mut aborts, true).await;
    }

    async fn run_controlled(
        mut self,
        mut input: Receiver<ClientRequest>,
        aborts: &mut mpsc::UnboundedReceiver<Uuid>,
        initialize: bool,
    ) {
        let active = self.active.clone();
        let outcome = {
            let running = self.run_inner(&mut input, initialize);
            tokio::pin!(running);
            loop {
                select! {
                    biased;
                    Some(id) = aborts.recv() => {
                        if *active.lock().unwrap_or_else(|e| e.into_inner()) == Some(id) {
                            break Err(KlickhouseError::ProtocolError(
                                "insert aborted; connection poisoned; write outcome unknown".to_string()
                            ));
                        }
                    }
                    result = &mut running => break result,
                }
            }
        };
        let mut error = Some(outcome.err().unwrap_or_else(unexpected_eof));
        if let Some(response) = self.dispatch_response.take() {
            let cause = error.take().unwrap_or_else(unexpected_eof);
            response.send(Err(KlickhouseError::QueryOutcomeUnknown(Box::new(cause))));
        }
        input.close(); // No new reservations; outstanding permits may still exist.
        for pending in self.pending_queries.drain(..) {
            pending
                .response
                .send(Err(KlickhouseError::QueryNotDispatched));
        }
        while let Ok(request) = input.try_recv() {
            match request.data {
                ClientRequestData::Query { response, .. }
                | ClientRequestData::BoundQuery { response, .. } => {
                    response.send(Err(KlickhouseError::QueryNotDispatched));
                }
                ClientRequestData::SendData { response, .. } => {
                    let _ = response.send(Err(KlickhouseError::ProtocolError(
                        "connection closed before data dispatch".to_string(),
                    )));
                }
                #[cfg(test)]
                ClientRequestData::InjectPacket(_, _) | ClientRequestData::InspectPending(_) => {}
            }
        }
        let running = self.executing_query.take();
        drop(self); // Close both transport halves before reporting the active-query failure.
        if let Some((_, sender, failure)) = running {
            let cause = error.unwrap_or_else(|| {
                KlickhouseError::ProtocolError(
                    "inconsistent concurrent dispatch and active query".to_string(),
                )
            });
            *failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(cause);
            drop(sender);
        }
    }
}

enum ClientRequestData {
    Query {
        id: Uuid,
        cancelled: Option<Arc<AtomicBool>>,
        query: String,
        settings: IndexMap<String, String>,
        response: DispatchReply,
    },
    BoundQuery {
        id: Uuid,
        cancelled: Arc<AtomicBool>,
        query: String,
        settings: IndexMap<String, String>,
        parameters: Vec<(String, String)>,
        response: DispatchReply,
    },
    SendData {
        id: Uuid,
        block: Block,
        response: oneshot::Sender<Result<()>>,
    },
    #[cfg(test)]
    InjectPacket(ServerPacket, oneshot::Sender<()>),
    #[cfg(test)]
    InspectPending(oneshot::Sender<usize>),
}

struct ClientRequest {
    data: ClientRequestData,
}

async fn await_dispatch(
    mut response: oneshot::Receiver<Result<QueryEvents>>,
    sender: &mpsc::Sender<ClientRequest>,
    dispatched: &AtomicBool,
) -> Result<QueryEvents> {
    let lost = || {
        if dispatched.load(Ordering::Acquire) {
            KlickhouseError::QueryOutcomeUnknown(Box::new(KlickhouseError::ProtocolError(
                "query dispatch acknowledgement lost".to_string(),
            )))
        } else {
            KlickhouseError::QueryNotDispatched
        }
    };
    select! {
        biased;
        result = &mut response => result.map_err(|_| lost())?,
        () = sender.closed() => {
            match response.try_recv() {
                Ok(result) => result,
                Err(_) => Err(lost()),
            }
        }
    }
}

/// Client handle for a Clickhouse connection, has internal reference to connection, and can be freely cloned and sent across threads.
#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<ClientRequest>,
    aborts: mpsc::UnboundedSender<Uuid>,
    progress: broadcast::Sender<(Uuid, Progress)>,
    default_settings: IndexMap<String, String>,
}

struct InsertGuard {
    id: Uuid,
    aborts: mpsc::UnboundedSender<Uuid>,
    armed: bool,
    cancelled: Arc<AtomicBool>,
}

// Until initialization succeeds the controller is private to `start`. Dropping the
// connect future must abort it even if it is blocked dispatching the buffered SET.
struct InitializationTask(Option<tokio::task::JoinHandle<()>>);

impl Drop for InitializationTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

impl Drop for InsertGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancelled.store(true, Ordering::Release);
            let _ = self.aborts.send(self.id);
        }
    }
}

/// Options set for a Clickhouse connection.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub username: String,
    pub password: String,
    pub default_database: String,
    pub tcp_nodelay: bool,
    /// Default query-level settings applied to every query.
    /// OpenTelemetry trace context can be set via `opentelemetry_traceparent`
    /// and `opentelemetry_tracestate` keys.
    pub settings: IndexMap<String, String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            username: "default".to_string(),
            password: String::new(),
            default_database: String::new(),
            tcp_nodelay: true,
            settings: IndexMap::new(),
        }
    }
}

impl Client {
    /// Sends Bool, Int64, Float64 values 0 or ±1, text and bytes without rewriting SQL.
    /// This experimental interface accepts `p1`..`p999999999` or names prefixed `ntp_`, exact typed
    /// `{name:Bool}`, `{name:Int64}`, `{name:Float64}`, or `{name:String}`
    /// placeholders, and SQL without quoted text, comments, dollar signs, or semicolons.
    /// Dropping this future after dispatch can leave server effects unknown. Once
    /// the event stream is returned, its data remains provisional until EndOfStream.
    pub async fn query_bound(
        &self,
        query: &str,
        parameters: &[(&str, NativeParameter<'_>)],
    ) -> Result<QueryEvents> {
        let parameters = encode_bound(query, parameters)?;
        self.send_bound(query, parameters).await
    }

    /// Sends a query whose native parameters the caller has already validated.
    ///
    /// Unlike [`Client::query_bound`], this does not scan or rewrite the SQL. The caller is
    /// responsible for matching every `{name:Type}` placeholder to an encoded `param_` value
    /// with the same declared type. The server remains authoritative for placeholder syntax.
    /// Dropping this future after dispatch can leave server effects unknown. Once the event
    /// stream is returned, its data remains provisional until `EndOfStream`.
    pub async fn query_bound_validated(
        &self,
        query: &str,
        parameters: Vec<(String, String)>,
    ) -> Result<QueryEvents> {
        self.send_bound(query, parameters).await
    }

    async fn send_bound(
        &self,
        query: &str,
        parameters: Vec<(String, String)>,
    ) -> Result<QueryEvents> {
        let (response, receiver) = oneshot::channel();
        let (response, dispatched) = DispatchReply::new(response);
        let id = Uuid::new_v4();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = InsertGuard {
            id,
            aborts: self.aborts.clone(),
            armed: true,
            cancelled: cancelled.clone(),
        };
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::BoundQuery {
                    id,
                    cancelled,
                    query: query.to_string(),
                    settings: self.default_settings.clone(),
                    parameters,
                    response,
                },
            })
            .await
            .map_err(|_| KlickhouseError::QueryNotDispatched)?;
        let result = await_dispatch(receiver, &self.sender, &dispatched).await;
        match result {
            Ok(mut events) => {
                events.guard = Some(guard);
                Ok(events)
            }
            Err(error) => {
                guard.armed = false; // Explicit pre-dispatch rejection.
                Err(error)
            }
        }
    }

    /// Consumes a reader and writer to connect to Klickhouse. To be used for exotic setups or TLS. Generally prefer [`Client::connect()`]
    pub async fn connect_stream(
        read: impl AsyncRead + Unpin + Send + Sync + 'static,
        writer: impl AsyncWrite + Unpin + Send + Sync + 'static,
        options: ClientOptions,
    ) -> Result<Self> {
        Self::start(InnerClient::new(
            BufReader::new(read),
            BufWriter::new(writer),
            options,
        ))
        .await
    }

    /// Connects to a specific socket address over plaintext TCP for Clickhouse.
    pub async fn connect<A: ToSocketAddrs>(destination: A, options: ClientOptions) -> Result<Self> {
        let stream = TcpStream::connect(destination).await?;
        stream.set_nodelay(options.tcp_nodelay)?;
        let (read, writer) = stream.into_split();
        Self::connect_stream(read, writer, options).await
    }

    /// Connects to a specific socket address over TLS (rustls) for Clickhouse.
    /// The caller supplies the connector and is responsible for its verification policy.
    #[cfg(feature = "tls")]
    pub async fn connect_tls<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: rustls_pki_types::ServerName<'static>,
        connector: &tokio_rustls::TlsConnector,
    ) -> Result<Self> {
        let stream = TcpStream::connect(destination).await?;
        stream.set_nodelay(options.tcp_nodelay)?;
        let tls_stream = connector.connect(name, stream).await?;
        let (read, writer) = tokio::io::split(tls_stream);
        Self::connect_stream(read, writer, options).await
    }

    /// Connects using certificate-chain and server-name verification with the supplied roots.
    #[cfg(feature = "tls")]
    pub async fn connect_tls_verified<A: ToSocketAddrs>(
        destination: A,
        options: ClientOptions,
        name: rustls_pki_types::ServerName<'static>,
        roots: tokio_rustls::rustls::RootCertStore,
    ) -> Result<Self> {
        let provider =
            std::sync::Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
        let config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| KlickhouseError::ProtocolError(format!("invalid TLS configuration: {e}")))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Self::connect_tls(
            destination,
            options,
            name,
            &tokio_rustls::TlsConnector::from(std::sync::Arc::new(config)),
        )
        .await
    }

    async fn start<R: ClickhouseRead + 'static, W: ClickhouseWrite>(
        inner: InnerClient<R, W>,
    ) -> Result<Self> {
        let progress = inner.progress.clone();
        let default_settings = inner.options.settings.clone();
        let (sender, receiver) = mpsc::channel(1024);
        let (aborts, abort_rx) = mpsc::unbounded_channel();

        let mut initialization =
            InitializationTask(Some(tokio::spawn(inner.run(receiver, abort_rx))));
        let client = Client {
            sender,
            aborts,
            progress,
            default_settings,
        };
        client
            .execute("SET date_time_input_format='best_effort'")
            .await?;
        // Dropping a JoinHandle detaches the now-established controller.
        drop(initialization.0.take());
        Ok(client)
    }

    /// Sends a query and exposes the explicit native terminal packet.
    ///
    /// Channel closure without `EndOfStream` is not success. Data events must be
    /// treated as provisional until the terminal event has been received.
    pub async fn query_events(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        query_settings: Option<IndexMap<String, String>>,
    ) -> Result<QueryEvents> {
        let mut settings = self.default_settings.clone();
        if let Some(qs) = query_settings {
            settings.extend(qs);
        }
        let (sender, receiver) = oneshot::channel();
        let (reply, dispatched) = DispatchReply::new(sender);
        let id = Uuid::new_v4();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = InsertGuard {
            id,
            aborts: self.aborts.clone(),
            armed: true,
            cancelled: cancelled.clone(),
        };
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id,
                    cancelled: Some(cancelled),
                    query: query.try_into()?.0,
                    settings,
                    response: reply,
                },
            })
            .await
            .map_err(|_| KlickhouseError::QueryNotDispatched)?;
        let result = await_dispatch(receiver, &self.sender, &dispatched).await;
        match result {
            Ok(mut events) => {
                events.guard = Some(guard);
                Ok(events)
            }
            Err(error) => {
                guard.armed = false; // Explicit pre-dispatch rejection.
                Err(error)
            }
        }
    }

    /// Sends a query and returns blocks. Unexpected transport EOF is an error.
    pub async fn query_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        query_settings: Option<IndexMap<String, String>>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Block>> + Send>>> {
        let events = self.query_events(query, query_settings).await?;
        Ok(terminal_blocks(events))
    }

    async fn send_data(&self, id: Uuid, block: Block) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::SendData {
                    id,
                    block,
                    response: sender,
                },
            })
            .await
            .map_err(|e| KlickhouseError::ProtocolError(format!("failed to send block: {e}")))?;
        receiver.await.map_err(|e| {
            KlickhouseError::ProtocolError(format!("failed to receive blocks from upstream: {e}"))
        })?
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks from Clickhouse are read.
    /// You probably want [`Client::insert_native`].
    pub async fn insert_native_raw(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        mut blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
        query_settings: Option<IndexMap<String, String>>,
    ) -> Result<impl Stream<Item = Result<Block>>> {
        let mut settings = self.default_settings.clone();
        if let Some(qs) = query_settings {
            settings.extend(qs);
        }
        let (sender, receiver) = oneshot::channel();
        let (reply, dispatched) = DispatchReply::new(sender);
        let id = Uuid::new_v4();
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = InsertGuard {
            id,
            aborts: self.aborts.clone(),
            armed: true,
            cancelled: cancelled.clone(),
        };
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id,
                    cancelled: Some(cancelled),
                    query: query.try_into()?.0,
                    settings,
                    response: reply,
                },
            })
            .await
            .map_err(|_| KlickhouseError::QueryNotDispatched)?;
        let receiver = await_dispatch(receiver, &self.sender, &dispatched)
            .await
            .map_err(insert_dispatch_error)?;

        while let Some(block) = blocks.next().await {
            self.send_data(id, block).await.map_err(unknown_insert)?;
        }
        self.send_data(
            id,
            Block {
                info: BlockInfo::default(),
                rows: 0,
                column_types: IndexMap::new(),
                column_data: IndexMap::new(),
            },
        )
        .await
        .map_err(unknown_insert)?;

        Ok(terminal_blocks_guarded(receiver, Some(guard))
            .map(|result| result.map_err(unknown_insert)))
    }

    /// Sends a query string with streaming associated data (i.e. insert) over native protocol.
    /// Once all outgoing blocks are written (EOF of `blocks` stream), then any response blocks
    /// from Clickhouse are read and DISCARDED, but server-side exceptions are returned.
    /// Make sure any query you send native data with has a `format native` suffix.
    pub async fn insert_native<T: Row + Send + Sync + 'static>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        mut blocks: impl Stream<Item = Vec<T>> + Send + Sync + Unpin + 'static,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let (reply, dispatched) = DispatchReply::new(sender);
        let id = Uuid::new_v4();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut guard = InsertGuard {
            id,
            aborts: self.aborts.clone(),
            armed: true,
            cancelled: cancelled.clone(),
        };
        self.sender
            .send(ClientRequest {
                data: ClientRequestData::Query {
                    id,
                    cancelled: Some(cancelled),
                    query: query.try_into()?.0.trim().to_string(),
                    settings: self.default_settings.clone(),
                    response: reply,
                },
            })
            .await
            .map_err(|_| KlickhouseError::QueryNotDispatched)?;
        let mut receiver = await_dispatch(receiver, &self.sender, &dispatched)
            .await
            .map_err(insert_dispatch_error)?;
        let first_event = receiver
            .recv()
            .await
            .ok_or_else(|| {
                unknown_insert(KlickhouseError::ProtocolError(
                    "missing header block from server".to_string(),
                ))
            })?
            .map_err(unknown_insert)?;
        let QueryEvent::Data(first_block) = first_event else {
            return Err(unknown_insert(KlickhouseError::ProtocolError(
                "missing header block from server".to_string(),
            )));
        };
        while let Some(rows) = blocks.next().await {
            if rows.is_empty() {
                continue;
            }
            let mut block = Block {
                info: BlockInfo::default(),
                rows: rows.len() as u64,
                column_types: first_block.column_types.clone(),
                column_data: IndexMap::new(),
            };
            rows.into_iter()
                .map(|x| x.serialize_row(&first_block.column_types))
                .try_for_each(|x| -> Result<()> {
                    let x = x?;
                    for (key, value) in x {
                        let type_ = first_block.column_types.get(&*key).ok_or_else(|| {
                            KlickhouseError::ProtocolError(format!(
                                "missing type for data, column: {key}"
                            ))
                        })?;
                        type_.validate_value(&value)?;
                        if let Some(column) = block.column_data.get_mut(&*key) {
                            column.push(value);
                        } else {
                            block.column_data.insert(key.into_owned(), vec![value]);
                        }
                    }
                    Ok(())
                })
                .map_err(|error| KlickhouseError::InsertOutcomeUnknown(Box::new(error)))?;
            self.send_data(id, block).await.map_err(unknown_insert)?;
        }
        self.send_data(
            id,
            Block {
                info: BlockInfo::default(),
                rows: 0,
                column_types: IndexMap::new(),
                column_data: IndexMap::new(),
            },
        )
        .await
        .map_err(unknown_insert)?;

        loop {
            match receiver.recv().await {
                Some(Ok(QueryEvent::Data(_))) => {}
                Some(Ok(QueryEvent::EndOfStream)) => {
                    guard.armed = false;
                    return Ok(());
                }
                Some(Err(error)) => return Err(unknown_insert(error)),
                None => return Err(unknown_insert(unexpected_eof())),
            }
        }
    }

    /// Wrapper over [`Client::insert_native`] to send a single block.
    /// Make sure any query you send native data with has a `format native` suffix.
    pub async fn insert_native_block<T: Row + Send + Sync + 'static>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
        blocks: Vec<T>,
    ) -> Result<()> {
        let blocks = Box::pin(async move { blocks });
        let stream = futures_util::stream::once(blocks);
        self.insert_native(query, stream).await
    }

    /// Runs a query against Clickhouse, returning a stream of deserialized rows.
    /// Note that no rows are returned until Clickhouse sends a full block (but it usually sends more than one block).
    pub async fn query<T: Row, I: TryInto<ParsedQuery, Error = KlickhouseError>>(
        &self,
        query: I,
    ) -> Result<impl Stream<Item = Result<T>> + use<T, I>> {
        let raw = self.query_raw(query, None).await?;
        Ok(raw.flat_map(|block| match block {
            Ok(mut block) => stream::iter(
                block
                    .take_iter_rows()
                    .filter(|x| !x.is_empty())
                    .map(|m| T::deserialize_row(m))
                    .collect::<Vec<_>>(),
            ),
            Err(e) => stream::iter(vec![Err(e)]),
        }))
    }

    /// Same as `query`, but collects all rows into a `Vec`
    pub async fn query_collect<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Vec<T>> {
        let mut out = vec![];
        let mut stream = self.query::<T, _>(query).await?;
        while let Some(next) = stream.next().await {
            out.push(next?);
        }
        Ok(out)
    }

    /// Returns the first row only after the entire query completes successfully.
    pub async fn query_one<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<T> {
        self.query_opt(query)
            .await?
            .ok_or(KlickhouseError::MissingRow)
    }

    /// Returns the first row, if any, only after the entire query completes successfully.
    pub async fn query_opt<T: Row>(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<Option<T>> {
        let mut stream = self.query(query).await?;
        let mut first = None;
        while let Some(row) = stream.next().await {
            let row = row?;
            if first.is_none() {
                first = Some(row);
            }
        }
        Ok(first)
    }

    /// Same as `query`, but discards all returns blocks. Waits until the first block returns from the server to check for errors.
    /// Waiting for the first response block or EOS also prevents the server from aborting the query potentially due to client disconnection.
    pub async fn execute(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let mut stream = self.query::<RawRow, _>(query).await?;
        while let Some(next) = stream.next().await {
            next?;
        }
        Ok(())
    }

    /// Same as `execute`, but doesn't wait for a server response. The query could get aborted if the connection is closed quickly.
    pub async fn execute_now(
        &self,
        query: impl TryInto<ParsedQuery, Error = KlickhouseError>,
    ) -> Result<()> {
        let _ = self.query::<RawRow, _>(query).await?;
        Ok(())
    }

    /// true if the Client is closed
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Receive progress on the queries as they execute.
    ///
    /// TODO: There is currently no way to retrieve the ID of a query launched
    ///       with `query` or `execute.`
    ///       The signature of these functions should be modified to also return
    ///       an ID (and possibly directly the streaming broadcast).
    pub fn subscribe_progress(&self) -> broadcast::Receiver<(Uuid, Progress)> {
        self.progress.subscribe()
    }
}
