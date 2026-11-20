//! Private diagnostic branching/lifecycle tests; no database, SQL execution, or credentials.
use super::*;
use neotales_dbapi::{
    BufferedReader, DbError, DbFuture, ErrorKind, Reader, Row, TimeoutSetting, Transaction,
    TxOptions,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;

const IDENTITY: &str = "synthetic-owned-probe";
const PRIVATE_MARKER: &str = "synthetic-marker-that-must-not-be-emitted";

#[derive(Default)]
struct State {
    lookups: AtomicUsize,
    drops: AtomicUsize,
    cancellations: Arc<AtomicUsize>,
    entered: tokio::sync::Notify,
}

struct CancelProbe {
    cancellations: Arc<AtomicUsize>,
    completed: bool,
}
impl Drop for CancelProbe {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellations.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct FakeSession {
    state: Arc<State>,
    stack: bool,
    pending: bool,
}
impl Drop for FakeSession {
    fn drop(&mut self) {
        self.state.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl Session for FakeSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
    fn fetch_async<'a>(
        &'a mut self,
        query: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            assert!(
                query.contains("WHERE query={p1:String}"),
                "lookup must use exact equality"
            );
            assert_eq!(options.parameters.len(), 1);
            assert!(
                matches!(options.timeout, TimeoutSetting::Set(value) if value == OBSERVATION_BUDGET)
            );
            let Value::Text(identity) = options.parameters[0].value() else {
                panic!("lookup must bind the synthetic identity");
            };
            assert_eq!(identity, IDENTITY);
            // An otherwise identical neighboring operation must not enter the result.
            let matching = [IDENTITY, "synthetic-owned-probe-neighbor"]
                .into_iter()
                .filter(|candidate| *candidate == identity)
                .count();
            assert_eq!(matching, 1);
            self.state.lookups.fetch_add(1, Ordering::SeqCst);
            self.state.entered.notify_one();
            if self.pending {
                let mut cancel = CancelProbe {
                    cancellations: self.state.cancellations.clone(),
                    completed: false,
                };
                // Finite secondary ceiling: even a mutation removing the observer's
                // timeout must fail assertions and clean up, rather than hang a thread.
                tokio::time::sleep(OBSERVATION_BUDGET * 2).await;
                cancel.completed = true;
            }
            let fields = if self.stack {
                vec![
                    ("thread".into(), Value::I64(17)),
                    (
                        "frames".into(),
                        Value::Bytes(format!("DB::fsyncFile\n{PRIVATE_MARKER}").into_bytes()),
                    ),
                ]
            } else {
                vec![
                    (
                        "query_id".into(),
                        Value::Bytes(PRIVATE_MARKER.as_bytes().to_vec()),
                    ),
                    ("elapsed_ms".into(), Value::I64(1000)),
                    ("cancelled".into(), Value::I64(0)),
                    ("threads".into(), Value::I64(1)),
                ]
            };
            Ok(Box::new(BufferedReader::new(vec![Row::new(fields).unwrap()])) as Box<dyn Reader>)
        })
    }
}

fn sessions(state: &Arc<State>, pending_process: bool) -> Sessions {
    (
        Box::new(FakeSession {
            state: state.clone(),
            stack: false,
            pending: pending_process,
        }),
        Box::new(FakeSession {
            state: state.clone(),
            stack: true,
            pending: false,
        }),
    )
}

fn numeric_field(record: &str, name: &str) -> u128 {
    record
        .split_whitespace()
        .find_map(|field| field.strip_prefix(name))
        .unwrap()
        .parse()
        .unwrap()
}

// Private trigger, deadline, serialization, and cleanup branches require a deliberately
// delayed synthetic operation; no real engine needs to stall to exercise them.
#[tokio::test]
async fn delayed_probe_exercises_trigger_deadlines_socket_selection_and_cleanup() {
    tokio::time::timeout(Duration::from_secs(8), controlled_slow_probe())
        .await
        .expect("controlled diagnostic must finish within its overall test deadline");
}

#[allow(clippy::too_many_lines)] // One fixture owns the independent observer and both socket endpoints.
async fn controlled_slow_probe() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let target = listener.local_addr().unwrap();
    let (port, connection_slot, owned_proxy) = proxy(target).await;
    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let (mut server, outbound) = listener.accept().await.unwrap();
    let connection = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(connection) = *connection_slot.lock().unwrap() {
                break connection;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let client_address = client.local_addr().unwrap();
    let proxy_address = client.peer_addr().unwrap();
    assert_eq!(connection.client, client_address);
    assert_eq!(connection.listener, proxy_address);
    assert_eq!(connection.outbound, outbound);
    assert_eq!(connection.target, target);
    let expected_filters: Vec<_> = [
        (client_address, proxy_address),
        (proxy_address, client_address),
        (outbound, target),
    ]
    .into_iter()
    .map(|(source, destination)| {
        format!(
            "( src {} and sport = :{} and dst {} and dport = :{} )",
            source.ip(),
            source.port(),
            destination.ip(),
            destination.port()
        )
    })
    .collect();
    let state = Arc::new(State::default());
    let setup_state = state.clone();
    let command_count = Arc::new(AtomicUsize::new(0));
    let kernel_commands = command_count.clone();
    let (stamped, writer_stamp) = mpsc::sync_channel(1);
    let writer_stamp = Mutex::new(writer_stamp);
    let mut observer = observer_with_setup(
        move || async move { Ok(sessions(&setup_state, true)) },
        move |connection, started, records| {
            let mut index = 0;
            socket_sample_with(connection, started, records, |program, arguments| {
                assert_eq!(program, "ss");
                assert_eq!(arguments[0], "-tinH");
                assert_eq!(arguments[1], expected_filters[index]);
                index += 1;
                kernel_commands.fetch_add(1, Ordering::SeqCst);
                Ok(format!("ESTAB 7 9 withheld-endpoint withheld-endpoint\n bytes_sent:111 bytes_acked:101 bytes_received:212 unknown:{PRIVATE_MARKER} bytes_sent:{PRIVATE_MARKER}\n"))
            });
            // Causal handshake, not a narrow timing comparison: cleanup cannot finish
            // until the independent writer has stamped completion and acknowledged it.
            let stamped_us: u128 = writer_stamp.lock().unwrap().recv_timeout(Duration::from_secs(3)).unwrap();
            records.push(format!("kernel-after-writer writer-stamped-us={stamped_us} completed-us={}", started.elapsed().as_micros()));
        },
    ).await;
    let started = Instant::now();
    observer
        .sender
        .send(Message::Arm(Probe {
            sequence: 0,
            started,
            sql: IDENTITY.into(),
            connection,
        }))
        .unwrap();
    // This synthetic operation is deliberately pending until the real slow-trigger
    // observer executes its exact-query lookup. Removing the trigger must fail here.
    let operation = tokio::time::timeout(Duration::from_secs(2), state.entered.notified()).await;
    let completion = writer_completed(&observer, 0, started, None);
    stamped.send(completion.1).unwrap();
    let records = finish_observer(&mut observer).await;
    owned_proxy.shutdown().await;
    let mut byte = [0];
    assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    assert_eq!(server.read(&mut byte).await.unwrap(), 0);
    assert!(tokio::net::TcpStream::connect(proxy_address).await.is_err());
    assert!(
        operation.is_ok(),
        "slow trigger must execute before the independent two-second operation deadline"
    );
    assert_eq!(state.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(command_count.load(Ordering::SeqCst), 3);
    assert_eq!(
        state.cancellations.load(Ordering::SeqCst),
        1,
        "observer deadline must cancel the pending fake fetch"
    );
    assert_eq!(
        state.drops.load(Ordering::SeqCst),
        2,
        "both observer sessions must be dropped before joining"
    );
    let report = records.join("\n");
    assert!(
        !report.contains(PRIVATE_MARKER),
        "raw synthetic content must not escape"
    );
    assert!(
        !report.contains(IDENTITY),
        "bound operation identity must not be emitted"
    );
    assert!(report.contains("sample sequence=0"));
    assert!(report.contains("unavailable-kind=Timeout"));
    assert!(report.contains("wait-category=filesystem-sync"));
    assert!(report.contains("recv-send-queues=[7, 9]"));
    assert!(report.contains("bytes_sent=111"));
    let after_writer = records
        .iter()
        .find(|record| record.starts_with("kernel-after-writer"))
        .unwrap();
    assert_eq!(
        numeric_field(after_writer, "writer-stamped-us="),
        completion.1
    );
    assert!(numeric_field(after_writer, "completed-us=") >= completion.1);
}

#[tokio::test]
async fn synthetic_server_rows_redact_identifiers_and_stack_bodies() {
    let state = Arc::new(State::default());
    let (mut process, mut stack) = sessions(&state, false);
    let address: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let probe = Probe {
        sequence: 0,
        started: Instant::now(),
        sql: IDENTITY.into(),
        connection: Connection {
            client: address,
            listener: address,
            outbound: address,
            target: address,
        },
    };
    let mut records = Vec::new();
    server_sample(process.as_mut(), stack.as_mut(), &probe, &mut records).await;
    let report = records.join("\n");
    assert!(report.contains("exact-matches=1"));
    assert!(report.contains("query-id=non-uuid-withheld"));
    assert!(report.contains("wait-category=filesystem-sync"));
    assert!(
        !report.contains(PRIVATE_MARKER),
        "raw fake server data must not be emitted"
    );
    assert!(
        !report.contains(IDENTITY),
        "bound identity must not be emitted"
    );
    assert_eq!(state.lookups.load(Ordering::SeqCst), 2);
}
