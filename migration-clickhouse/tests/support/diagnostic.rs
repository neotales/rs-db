//! Opt-in, bounded diagnostic. No production hooks, packet payloads, or raw logs.
use super::{bridge_address, connect, disposable_password, start_clickhouse};
use neotales_dbapi::{DbType, Parameter, QueryOptions, RowLimit, Session, Value};
use neotales_dbapi_clickhouse::ClickHousePoolProvider;
use std::{
    io::Read,
    net::SocketAddr,
    process::{Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

const TRIGGER_AFTER: Duration = Duration::from_secs(1);
const OBSERVATION_BUDGET: Duration = Duration::from_millis(600);
type Sessions = (Box<dyn Session>, Box<dyn Session>);

#[derive(Clone, Copy)]
struct Connection {
    client: SocketAddr,
    listener: SocketAddr,
    outbound: SocketAddr,
    target: SocketAddr,
}

struct Proxy {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Proxy {
    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn proxy(target: SocketAddr) -> (u16, Arc<Mutex<Option<Connection>>>, Proxy) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let connection = Arc::new(Mutex::new(None));
    let observed = connection.clone();
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        // Aborting the listener also drops and aborts every owned connection worker.
        let mut workers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                accepted = listener.accept() => {
                    let Ok((mut inbound, client)) = accepted else { break };
                    let observed = observed.clone();
                    workers.spawn(async move {
                        inbound.set_nodelay(true).unwrap();
                        let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await else { return };
                        outbound.set_nodelay(true).unwrap();
                        *observed.lock().unwrap() = Some(Connection {
                            client, listener: address,
                            outbound: outbound.local_addr().unwrap(), target,
                        });
                        // Deliberately unchanged forwarding, no wrappers/per-packet accounting.
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    });
                }
                _ = workers.join_next(), if !workers.is_empty() => {}
            }
        }
        workers.shutdown().await;
    });
    (
        address.port(),
        connection,
        Proxy {
            stop: Some(stop),
            task: Some(task),
        },
    )
}

struct Probe {
    sequence: usize,
    started: Instant,
    sql: String,
    connection: Connection,
}
enum Message {
    Arm(Probe),
    Done(usize),
    Stop,
}

struct Observer {
    sender: mpsc::SyncSender<Message>,
    thread: Option<std::thread::JoinHandle<Vec<String>>>,
}

fn writer_completed(
    observer: &Observer,
    sequence: usize,
    started: Instant,
    error: Option<neotales_dbapi::ErrorKind>,
) -> (usize, u128, Option<neotales_dbapi::ErrorKind>) {
    let completed_us = started.elapsed().as_micros();
    observer.sender.send(Message::Done(sequence)).unwrap();
    (sequence, completed_us, error)
}

async fn finish_observer(observer: &mut Observer) -> Vec<String> {
    observer.sender.send(Message::Stop).unwrap();
    let thread = observer.thread.take().unwrap();
    // Keep proxy tasks polled while the observer drains.
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap()
}

impl Drop for Observer {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

// No child may outlive the sampler; stdout is limited to 16 KiB and stderr discarded.
fn bounded_command(program: &str, args: &[String]) -> Result<String, &'static str> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "spawn-unavailable")?;
    let deadline = Instant::now() + Duration::from_millis(400);
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let mut output = String::new();
                child
                    .stdout
                    .take()
                    .unwrap()
                    .take(16_384)
                    .read_to_string(&mut output)
                    .map_err(|_| "stdout-unavailable")?;
                return Ok(output);
            }
            Ok(Some(_)) => return Err("command-failed"),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err("command-deadline-or-wait-failure");
            }
        }
    }
}

fn socket_sample(connection: Connection, started: Instant, records: &mut Vec<String>) {
    socket_sample_with(connection, started, records, bounded_command);
}

fn socket_sample_with(
    connection: Connection,
    started: Instant,
    records: &mut Vec<String>,
    mut command: impl FnMut(&str, &[String]) -> Result<String, &'static str>,
) {
    for (leg, source, destination) in [
        ("client", connection.client, connection.listener),
        ("proxy-in", connection.listener, connection.client),
        ("proxy-out", connection.outbound, connection.target),
    ] {
        let filter = format!(
            "( src {} and sport = :{} and dst {} and dport = :{} )",
            source.ip(),
            source.port(),
            destination.ip(),
            destination.port()
        );
        let begun = started.elapsed().as_micros();
        let result = command("ss", &["-tinH".into(), filter]);
        let stamp = started.elapsed().as_micros();
        match result {
            Ok(output) => {
                let mut lines = output.lines();
                if let Some(line) = lines.next() {
                    let fields: Vec<_> = line.split_whitespace().collect();
                    let state = fields
                        .first()
                        .copied()
                        .filter(|state| {
                            matches!(
                                *state,
                                "ESTAB"
                                    | "CLOSE-WAIT"
                                    | "FIN-WAIT-1"
                                    | "FIN-WAIT-2"
                                    | "TIME-WAIT"
                                    | "LAST-ACK"
                                    | "CLOSING"
                                    | "CLOSE"
                                    | "SYN-SENT"
                                    | "SYN-RECV"
                            )
                        })
                        .unwrap_or("unclassified");
                    // Never emit raw tool output or endpoint strings.
                    let queues = fields.get(1..3).unwrap_or_default();
                    let queues: Vec<_> = queues
                        .iter()
                        .filter_map(|x| x.parse::<u64>().ok())
                        .collect();
                    let counters: Vec<_> = lines
                        .flat_map(str::split_whitespace)
                        .filter_map(|field| {
                            let (key, value) = field.split_once(':')?;
                            if ![
                                "bytes_sent",
                                "bytes_acked",
                                "bytes_received",
                                "segs_out",
                                "segs_in",
                                "unacked",
                            ]
                            .contains(&key)
                            {
                                return None;
                            }
                            value
                                .parse::<u64>()
                                .ok()
                                .map(|value| format!("{key}={value}"))
                        })
                        .collect();
                    records.push(format!(
                        "socket leg={leg} begun-us={begun} completed-us={stamp} state={state} recv-send-queues={queues:?} {}",
                        counters.join(" ")
                    ));
                } else {
                    records.push(format!(
                        "socket leg={leg} elapsed-us={stamp} no-matching-socket"
                    ));
                }
            }
            Err(category) => records.push(format!(
                "socket leg={leg} elapsed-us={stamp} unavailable={category}"
            )),
        }
    }
}

fn pressure_sample(started: Instant, records: &mut Vec<String>) {
    for resource in ["cpu", "io"] {
        match std::fs::read_to_string(format!("/proc/pressure/{resource}")) {
            Ok(text) => {
                for line in text.lines().take(2) {
                    let mut fields = line.split_whitespace();
                    let category = match fields.next() {
                        Some("some") => "some",
                        Some("full") => "full",
                        _ => continue,
                    };
                    let fields: Vec<_> = fields.collect();
                    let total = fields
                        .iter()
                        .find_map(|x| x.strip_prefix("total="))
                        .and_then(|x| x.parse::<u64>().ok());
                    let avg10 = fields
                        .iter()
                        .find_map(|x| x.strip_prefix("avg10="))
                        .and_then(|x| x.parse::<f64>().ok());
                    records.push(format!("pressure resource={resource} category={category} total-us={total:?} avg10={avg10:?} elapsed-us={}", started.elapsed().as_micros()));
                }
            }
            Err(_) => records.push(format!("pressure resource={resource} unavailable")),
        }
    }
    match std::fs::read_to_string("/proc/thread-self/schedstat") {
        Ok(text) => {
            let stats: Vec<_> = text
                .split_whitespace()
                .take(3)
                .filter_map(|x| x.parse::<u64>().ok())
                .collect();
            records.push(format!(
                "scheduler actor=kernel-sampler runtime-ns_wait-ns_slices={stats:?} elapsed-us={}",
                started.elapsed().as_micros()
            ));
        }
        Err(_) => records.push("scheduler actor=kernel-sampler unavailable".into()),
    }
}

fn wait_category(frames: &str) -> &'static str {
    // Allowlisted categories only; symbols/addresses and raw stack bodies never leave memory.
    let frames = frames.to_ascii_lowercase();
    if frames.contains("fsync") || frames.contains("fdatasync") {
        "filesystem-sync"
    } else if frames.contains("writebufferfromfile") || frames.contains("readbufferfromfile") {
        "filesystem-io"
    } else if frames.contains("ddlguard") || frames.contains("lockexclusiv") {
        "ddl-lock"
    } else if frames.contains("futex") || frames.contains("pthread_mutex") {
        "mutex-or-condition"
    } else if frames.contains("receivepacket") || frames.contains("readbufferfrompoco") {
        "socket-read"
    } else if frames.contains("senddata") || frames.contains("writebufferfrompoco") {
        "socket-write"
    } else if frames.contains("sleep") || frames.contains("poll") {
        "sleep-or-poll"
    } else {
        "unclassified"
    }
}

async fn server_sample(
    process: &mut dyn Session,
    stack: &mut dyn Session,
    probe: &Probe,
    records: &mut Vec<String>,
) {
    records.push(format!(
        "server-observation dispatched-us={}",
        probe.started.elapsed().as_micros()
    ));
    let parameter = || Parameter::new(DbType::Text, Value::Text(probe.sql.clone())).unwrap();
    let options = || {
        QueryOptions::new()
            .bind(parameter())
            .timeout(OBSERVATION_BUDGET)
            .limit(RowLimit::new(4, 16_384).unwrap())
    };
    // Exact equality matches this diagnostic's synthetic query only, not other workloads.
    let process_sql = "SELECT query_id, toInt64(elapsed*1000) AS elapsed_ms, \
        toInt64(is_cancelled) AS cancelled, toInt64(length(thread_ids)) AS threads \
        FROM system.processes WHERE query={p1:String} LIMIT 1";
    let stack_sql = "SELECT toInt64(thread_id) AS thread, \
        arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\\n') AS frames \
        FROM system.stack_trace WHERE thread_id IN \
        (SELECT arrayJoin(thread_ids) FROM system.processes WHERE query={p1:String}) \
        LIMIT 4 SETTINGS allow_introspection_functions=1";
    let process_work = async {
        let result = tokio::time::timeout(
            OBSERVATION_BUDGET,
            process.fetch_all_async(process_sql, options()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(neotales_dbapi::DbError::new(
                neotales_dbapi::ErrorKind::Timeout,
            ))
        });
        (probe.started.elapsed().as_micros(), result)
    };
    let stack_work = async {
        let result = tokio::time::timeout(
            OBSERVATION_BUDGET,
            stack.fetch_all_async(stack_sql, options()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(neotales_dbapi::DbError::new(
                neotales_dbapi::ErrorKind::Timeout,
            ))
        });
        (probe.started.elapsed().as_micros(), result)
    };
    let ((at, processes), (stack_at, stacks)) = tokio::join!(process_work, stack_work);
    match processes {
        Ok(rows) => {
            records.push(format!(
                "server-process elapsed-us={at} exact-matches={}",
                rows.len()
            ));
            for row in rows {
                let id = row.get::<Vec<u8>>("query_id").unwrap_or_default();
                let id = if id.len() == 36 && id.iter().all(|x| x.is_ascii_hexdigit() || *x == b'-')
                {
                    String::from_utf8(id).unwrap()
                } else {
                    "non-uuid-withheld".into()
                };
                records.push(format!("server-process query-id={id} server-elapsed-ms={:?} cancelled={:?} threads={:?}",
                    row.get::<i64>("elapsed_ms"), row.get::<i64>("cancelled"), row.get::<i64>("threads")));
            }
        }
        Err(error) => records.push(format!(
            "server-process elapsed-us={at} unavailable-kind={:?}",
            error.kind()
        )),
    }
    match stacks {
        Ok(rows) => {
            records.push(format!(
                "server-stacks elapsed-us={stack_at} rows={}",
                rows.len()
            ));
            for row in rows {
                let frames = row.get::<Vec<u8>>("frames").unwrap_or_default();
                let category = std::str::from_utf8(&frames).map_or("invalid-utf8", wait_category);
                records.push(format!(
                    "server-thread id={:?} wait-category={category}",
                    row.get::<i64>("thread")
                ));
            }
        }
        Err(error) => records.push(format!(
            "server-stacks elapsed-us={stack_at} unavailable-kind={:?}",
            error.kind()
        )),
    }
}

async fn observer(provider: ClickHousePoolProvider) -> Observer {
    observer_with_setup(
        move || async move {
            let process = provider.acquire_clickhouse().await?;
            let stack = provider.acquire_clickhouse().await?;
            Ok((
                Box::new(process) as Box<dyn Session>,
                Box::new(stack) as Box<dyn Session>,
            ))
        },
        |connection, started, records| {
            pressure_sample(started, records);
            socket_sample(connection, started, records);
        },
    )
    .await
}

async fn observer_with_setup<F, Fut, K>(setup: F, kernel_sample: K) -> Observer
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Sessions, neotales_dbapi::DbError>>,
    K: Fn(Connection, Instant, &mut Vec<String>) + Send + Sync + 'static,
{
    // At most 1026 arm/done/stop messages for 512 writes, below capacity even
    // if the observer is delayed; sending cannot block the measured writer.
    let (sender, receiver) = mpsc::sync_channel(2048);
    let (ready, waiting) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let sessions =
            runtime.block_on(async { tokio::time::timeout(Duration::from_secs(5), setup()).await });
        let Ok(Ok((mut process, mut stack))) = sessions else {
            ready.send(false).unwrap();
            return vec!["observer pre-acquisition unavailable".into()];
        };
        ready.send(true).unwrap();
        let mut records = Vec::new();
        while let Ok(message) = receiver.recv() {
            match message {
                Message::Arm(probe) => {
                    let wait =
                        (probe.started + TRIGGER_AFTER).saturating_duration_since(Instant::now());
                    match receiver.recv_timeout(wait) {
                        Ok(Message::Done(sequence)) if sequence == probe.sequence => {}
                        Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Bounded output, including while subsequent operations finish.
                            if records.len() >= 96 {
                                continue;
                            }
                            records.push(format!(
                                "sample sequence={} triggered-us={} client-port={} proxy-port={} server-facing-port={}",
                                probe.sequence,
                                probe.started.elapsed().as_micros(),
                                probe.connection.client.port(),
                                probe.connection.listener.port(),
                                probe.connection.outbound.port()
                            ));
                            // The caller's runtime is independent of all these observations.
                            std::thread::scope(|scope| {
                                let kernel = scope.spawn(|| {
                                    let mut kernel_records = Vec::new();
                                    kernel_sample(
                                        probe.connection,
                                        probe.started,
                                        &mut kernel_records,
                                    );
                                    kernel_records
                                });
                                runtime.block_on(server_sample(
                                    process.as_mut(),
                                    stack.as_mut(),
                                    &probe,
                                    &mut records,
                                ));
                                records.extend(kernel.join().unwrap());
                            });
                        }
                    }
                }
                Message::Done(_) => {}
                Message::Stop => break,
            }
        }
        records
    });
    let observer = Observer {
        sender,
        thread: Some(thread),
    };
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(6), waiting).await,
            Ok(Ok(true))
        ),
        "independent observer sessions unavailable"
    );
    observer
}

#[allow(clippy::too_many_lines)] // Keep bounded writer/observer ownership and completion chronology together.
pub(super) async fn run() {
    let password = disposable_password();
    let container = start_clickhouse(&password).await;
    super::assert_no_published_ports(&container);
    let target = bridge_address(container.get_bridge_ip_address().await.unwrap());
    let (port, connection, owned_proxy) = proxy(target).await;
    let provider = connect(port, &password).await;
    let mut observer = observer(provider.clone()).await;
    let mut failure = None;
    let mut acquisition_kind = None;
    let mut stop_reason = "operation-count-bound";
    let experiment_started = Instant::now();
    let mut completions = Vec::with_capacity(512);
    for iteration in 0..256 {
        if experiment_started.elapsed() >= Duration::from_secs(60) {
            stop_reason = "elapsed-bound";
            break;
        }
        let mut session =
            match tokio::time::timeout(Duration::from_secs(5), provider.acquire_clickhouse()).await
            {
                Ok(Ok(session)) => session,
                Ok(Err(error)) => {
                    stop_reason = "acquisition-error";
                    acquisition_kind = Some(error.kind());
                    break;
                }
                Err(_) => {
                    stop_reason = "acquisition-deadline";
                    break;
                }
            };
        let connection = connection.lock().unwrap().unwrap();
        for (direction, statement) in [
            (
                "create",
                "CREATE TABLE nt_lifecycle_lock (owner String) ENGINE=Memory",
            ),
            ("drop", "DROP TABLE nt_lifecycle_lock"),
        ] {
            let sequence = completions.len();
            let sql = format!("{statement} /*nt-diagnostic-{iteration}-{direction}*/");
            let started = Instant::now();
            observer
                .sender
                .send(Message::Arm(Probe {
                    sequence,
                    started,
                    sql: sql.clone(),
                    connection,
                }))
                .unwrap();
            let result = session
                .execute_no_count(&sql, QueryOptions::new().timeout(Duration::from_secs(2)))
                .await;
            // Capture before waiting for the observer or emitting any output.
            completions.push(writer_completed(
                &observer,
                sequence,
                started,
                result.as_ref().err().map(|error| error.kind()),
            ));
            if let Err(error) = result {
                failure = Some(error.kind());
                stop_reason = "write-error";
                break;
            }
        }
        if failure.is_some() {
            break;
        }
    }
    let records = finish_observer(&mut observer).await;
    owned_proxy.shutdown().await;
    // No diagnostic output while any measured write is outstanding.
    for record in records {
        eprintln!("bounded-observer {record}");
    }
    for (sequence, elapsed, error) in &completions {
        if *elapsed >= 1_000_000 || error.is_some() {
            eprintln!("bounded-writer sequence={sequence} completed-us={elapsed} error={error:?}");
        }
    }
    let peak = completions
        .iter()
        .map(|(_, elapsed, _)| *elapsed)
        .max()
        .unwrap_or(0);
    eprintln!(
        "bounded-summary operations={} peak-us={peak} failure={failure:?} stop={stop_reason} acquisition-kind={acquisition_kind:?}; local flush is not server receipt",
        completions.len()
    );
    assert!(
        failure.is_none(),
        "bounded diagnostic observed {failure:?}; no retry or stability inference"
    );
    assert!(
        !stop_reason.starts_with("acquisition-"),
        "diagnostic capability unavailable: {stop_reason} {acquisition_kind:?}"
    );
}

#[cfg(test)]
#[path = "diagnostic_tests.rs"]
mod controlled_tests;

#[cfg(test)]
mod tests {
    use super::wait_category;

    // Private redaction/classification branching: only a fixed category may escape,
    // never an arbitrary symbol, address, or server-supplied string.
    #[test]
    fn wait_classification_emits_only_fixed_categories() {
        for (input, expected) in [
            ("unknown symbol with arbitrary content", "unclassified"),
            ("DB::fsyncFile()", "filesystem-sync"),
            ("fdatasync", "filesystem-sync"),
            ("DB::WriteBufferFromFile", "filesystem-io"),
            ("DB::DDLGuard", "ddl-lock"),
            ("pthread_mutex_lock", "mutex-or-condition"),
            ("DB::ReadBufferFromPocoSocket", "socket-read"),
            ("DB::WriteBufferFromPocoSocket", "socket-write"),
            ("poll", "sleep-or-poll"),
        ] {
            assert_eq!(wait_category(input), expected);
        }
    }
}
