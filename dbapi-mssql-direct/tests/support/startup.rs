//! Opt-in diagnostic runner. Never used by the TLS assertions or production.
use std::{collections::HashMap, path::Path, time::Duration};

use bollard::{
    Docker,
    errors::Error,
    models::{ContainerCreateBody, HostConfig},
    query_parameters::{
        CreateContainerOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
        StatsOptionsBuilder, UploadToContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use tokio::{sync::oneshot, time::Instant};

const LABEL: &str = "neotales.mssql-start-probe";
const REQUEST_BOUND: Duration = Duration::from_secs(210);
const READ_BOUND: Duration = Duration::from_secs(3);
const OBSERVATION_BOUND: Duration = Duration::from_secs(180);
const CLEANUP_BOUND: Duration = Duration::from_secs(30);
const LOG_BYTE_BOUND: usize = 512 * 1024;

#[derive(Clone, Copy)]
struct Schedule {
    boundary: Duration,
    observation: Duration,
    poll: Duration,
}

const SCHEDULE: Schedule = Schedule {
    boundary: super::TLS_FIXTURE_STARTUP_DEADLINE,
    observation: OBSERVATION_BOUND,
    poll: Duration::from_secs(1),
};

fn identity() -> String {
    let mut nonce = [0; 12];
    getrandom::fill(&mut nonce).expect("probe identity entropy");
    let mut name = format!("mssql-start-probe-{}-", std::process::id());
    for byte in nonce {
        use std::fmt::Write as _;
        write!(name, "{byte:02x}").unwrap();
    }
    name
}

fn config(name: &str, password: &str) -> ContainerCreateBody {
    ContainerCreateBody {
        image: Some(format!("{}:{}", super::TLS_IMAGE, super::TLS_IMAGE_TAG)),
        user: Some("0".into()),
        entrypoint: Some(vec!["/bin/bash".into()]),
        cmd: Some(vec!["-c".into(), super::TLS_STARTUP_COMMAND.into()]),
        env: Some(vec![
            "ACCEPT_EULA=Y".into(),
            format!("MSSQL_SA_PASSWORD={password}"),
            "MSSQL_PID=Developer".into(),
        ]),
        labels: Some(HashMap::from([(LABEL.into(), name.into())])),
        host_config: Some(HostConfig {
            publish_all_ports: Some(false),
            port_bindings: Some(HashMap::new()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn archive(directory: &Path) -> Result<Vec<u8>, &'static str> {
    let mut tar = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    header.set_mode(0o755);
    header.set_cksum();
    tar.append_data(&mut header, "fixture/", std::io::empty())
        .map_err(|_| "fixture archive directory")?;
    for (file, mode) in [("server.pem", 0o644), ("server.key", 0o600)] {
        let bytes = std::fs::read(directory.join(file)).map_err(|_| "fixture file read")?;
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(mode);
        header.set_cksum();
        tar.append_data(&mut header, format!("fixture/{file}"), bytes.as_slice())
            .map_err(|_| "fixture archive")?;
    }
    tar.into_inner().map_err(|_| "fixture archive finish")
}

// The task, not its caller, owns all mutations. Dropping the caller requests
// cancellation but never aborts a create/upload/start HTTP request. The enclosing
// synchronous test keeps the runtime alive until this tracked supervisor exits.
struct Supervisor {
    cancel: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), &'static str>>,
}

impl Supervisor {
    fn start(docker: Docker, name: String, config: ContainerCreateBody, files: Vec<u8>) -> Self {
        Self::scheduled(docker, name, config, files, SCHEDULE)
    }

    fn scheduled(
        docker: Docker,
        name: String,
        config: ContainerCreateBody,
        files: Vec<u8>,
        schedule: Schedule,
    ) -> Self {
        let (cancel, receiver) = oneshot::channel();
        let count = super::child_supervisors().start();
        let task = tokio::spawn(async move {
            let _count = count;
            let result = supervise(docker, &name, config, files, receiver, schedule).await;
            if let Err(category) = result {
                // All errors are fixed categories, never Docker response bodies,
                // certificate data, environment values, or SQL Server logs.
                eprintln!("startup probe name={name} failure={category}");
            }
            result
        });
        Self {
            cancel: Some(cancel),
            task,
        }
    }

    async fn wait(mut self) -> Result<(), &'static str> {
        let result = (&mut self.task)
            .await
            .map_err(|_| "supervisor task failed")?;
        self.cancel.take();
        result
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

fn absent(error: &Error) -> bool {
    matches!(
        error,
        Error::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

async fn supervise(
    mut docker: Docker,
    name: &str,
    config: ContainerCreateBody,
    files: Vec<u8>,
    mut cancel: oneshot::Receiver<()>,
    schedule: Schedule,
) -> Result<(), &'static str> {
    docker.set_timeout(Duration::from_secs(30));
    // Know the name and label before dispatch, even if the response containing
    // the ID is lost. Never retry create under a new name.
    let created = docker
        .create_container(
            Some(CreateContainerOptionsBuilder::default().name(name).build()),
            config,
        )
        .await;
    let (mut result, id) = match created {
        Ok(created) => (Ok(()), Some(created.id)),
        Err(_) => (Err("create failed; daemon outcome uncertain"), None),
    };
    let target = id.as_deref().unwrap_or(name);
    if result.is_ok() && !cancel_requested(&mut cancel) {
        result = docker
            .upload_to_container(
                target,
                Some(UploadToContainerOptionsBuilder::default().path("/").build()),
                bollard::body_full(files.into()),
            )
            .await
            .map_err(|_| "fixture upload failed");
        if result.is_ok() && !cancel_requested(&mut cancel) {
            result = observe_start(&docker, target, &mut cancel, schedule).await;
        }
    }
    if result.is_ok() && cancel_requested(&mut cancel) {
        result = Err("cancelled");
    }
    // Even a lost create response gets label-checked reconciliation. A 404
    // after a lost response does NOT prove a late create cannot still happen.
    let cleanup = tokio::time::timeout(CLEANUP_BOUND, cleanup(&docker, target, name, id.is_some()))
        .await
        .unwrap_or(Err("cleanup deadline; manual reconciliation required"));
    if let Err(category) = cleanup {
        eprintln!("startup probe name={name} cleanup={category}");
    }
    result.and(cleanup)
}

fn cancel_requested(cancel: &mut oneshot::Receiver<()>) -> bool {
    !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

async fn cleanup(
    docker: &Docker,
    target: &str,
    name: &str,
    acknowledged: bool,
) -> Result<(), &'static str> {
    let inspect = match docker.inspect_container(target, None).await {
        Ok(inspect) => inspect,
        Err(error) if absent(&error) && acknowledged => return Ok(()),
        Err(error) if absent(&error) => return Err("create unacknowledged; absence inconclusive"),
        Err(_) => return Err("inspect failed; manual reconciliation required"),
    };
    let owned = inspect
        .config
        .as_ref()
        .and_then(|config| config.labels.as_ref())
        .and_then(|labels| labels.get(LABEL))
        .is_some_and(|value| value == name);
    if !owned {
        return Err("ownership mismatch; refusing removal");
    }
    // Delete by inspected ID, not reusable name; force removal also stops a
    // running container. Include anonymous volumes, never the pinned image.
    let id = inspect.id.ok_or("missing owned container ID")?;
    docker
        .remove_container(
            &id,
            Some(
                RemoveContainerOptionsBuilder::default()
                    .force(true)
                    .v(true)
                    .build(),
            ),
        )
        .await
        .map_err(|_| "remove failed; manual reconciliation required")?;
    match docker.inspect_container(&id, None).await {
        Err(error) if absent(&error) => {
            eprintln!("startup probe name={name} removal_verified=true");
            Ok(())
        }
        _ => Err("removal not verified; manual reconciliation required"),
    }
}

async fn observe_start(
    docker: &Docker,
    name: &str,
    cancel: &mut oneshot::Receiver<()>,
    schedule: Schedule,
) -> Result<(), &'static str> {
    let mut start_client = docker.clone();
    start_client.set_timeout(REQUEST_BOUND);
    let name_owned = name.to_owned();
    let (dispatch_tx, dispatch_rx) = oneshot::channel();
    let mut start = tokio::spawn(async move {
        // Same monotonic clock, immediately around the actual API call.
        let dispatch = Instant::now();
        let _ = dispatch_tx.send(dispatch);
        let response = start_client.start_container(&name_owned, None).await;
        (Instant::now(), response.is_ok())
    });
    let dispatch = dispatch_rx
        .await
        .map_err(|_| "start task did not dispatch")?;
    let mut response = None;
    let (ready, cancelled) = {
        let mut ready = false;
        let mut at_boundary = false;
        let mut cancelled = false;
        // Persistent sibling futures: neither log reads nor snapshot reads
        // occupy a select branch body. A stalled log response cannot postpone
        // dispatch of the boundary inspection (or vice versa).
        let boundary = async {
            tokio::time::sleep_until(dispatch + schedule.boundary).await;
            eprintln!("startup probe boundary_woke_at={:?}", dispatch.elapsed());
            snapshot(docker, name, dispatch, "90s").await;
        };
        let polling = poll_marker(docker, name, dispatch, schedule);
        tokio::pin!(boundary, polling);
        loop {
            tokio::select! {
                result = &mut start, if response.is_none() => {
                    let (when, succeeded) = result.map_err(|_| "start task failed")?;
                    eprintln!("startup probe start_response elapsed={:?} succeeded={succeeded}", when - dispatch);
                    response = Some(succeeded);
                }
                _ = &mut *cancel => { cancelled = true; break; }
                () = &mut boundary, if !at_boundary => {
                    at_boundary = true;
                }
                observed = &mut polling, if !ready => {
                    let Some((observed_at, previous_poll)) = observed else { break };
                    ready = true;
                    eprintln!("startup probe marker=true observed_at={observed_at:?} previous_poll={previous_poll:?}; interval is observation uncertainty, not server timestamp");
                }
                () = tokio::time::sleep_until(dispatch + schedule.observation) => { break; }
            }
            if ready && at_boundary {
                break;
            }
        }
        // Drop all read-only observation futures before draining the owned
        // start mutation, including on cancellation. No detached read tasks.
        (ready, cancelled)
    };
    if !cancelled {
        snapshot(docker, name, dispatch, "final").await;
    }
    // Join even on cancellation/deadline: dropping the HTTP start future is
    // not rollback. Its separate transport bound limits this drain to 210s.
    if response.is_none() {
        let (when, succeeded) = start.await.map_err(|_| "start task failed")?;
        eprintln!(
            "startup probe start_response elapsed={:?} succeeded={succeeded}",
            when - dispatch
        );
        response = Some(succeeded);
    }
    if cancelled {
        Err("cancelled")
    } else if response != Some(true) {
        Err("start failed; daemon outcome uncertain")
    } else if !ready {
        Err("marker not observed within diagnostic deadline")
    } else {
        Ok(())
    }
}

async fn poll_marker(
    docker: &Docker,
    name: &str,
    dispatch: Instant,
    schedule: Schedule,
) -> Option<(Duration, Duration)> {
    tokio::time::timeout_at(dispatch + schedule.observation, async {
        let mut previous_poll = Duration::ZERO;
        let mut tick = tokio::time::interval(schedule.poll);
        loop {
            tick.tick().await;
            let requested_at = dispatch.elapsed();
            match tokio::time::timeout(READ_BOUND, marker(docker, name)).await {
                Ok(Ok(true)) => return (dispatch.elapsed(), previous_poll),
                // Only dispatch of a successful negative read establishes the
                // lower bound; completion of that response is not a timestamp
                // of the server's last log entry.
                Ok(Ok(false)) => previous_poll = requested_at,
                _ => {}
            }
        }
    })
    .await
    .ok()
}

async fn marker(docker: &Docker, name: &str) -> Result<bool, ()> {
    let options = LogsOptionsBuilder::default()
        .stdout(true)
        .stderr(true)
        .follow(false)
        .build();
    let mut logs = docker.logs(name, Some(options));
    let needle = super::TLS_READY_MARKER.as_bytes();
    let mut tail = Vec::new();
    let mut total = 0;
    while let Some(chunk) = logs.next().await {
        let chunk = chunk.map_err(|_| ())?;
        let bytes = chunk.as_ref();
        total += bytes.len();
        if total > LOG_BYTE_BOUND {
            return Err(());
        }
        tail.extend_from_slice(bytes);
        if tail.windows(needle.len()).any(|window| window == needle) {
            return Ok(true);
        }
        let keep = tail.len().saturating_sub(needle.len() - 1);
        tail.drain(..keep);
    }
    Ok(false)
}

async fn snapshot(docker: &Docker, name: &str, dispatch: Instant, phase: &str) {
    let read = async {
        let requested_at = dispatch.elapsed();
        let inspect = docker.inspect_container(name, None).await.map_err(|_| ())?;
        let received_at = dispatch.elapsed();
        if let Some(state) = inspect.state {
            eprintln!(
                "startup probe phase={phase} inspect_requested_at={requested_at:?} inspect_received_at={received_at:?} status={:?} running={:?} exit={:?} oom={:?} health={:?}",
                state.status,
                state.running,
                state.exit_code,
                state.oom_killed,
                state.health.and_then(|health| health.status),
            );
        }
        let ip = inspect
            .network_settings
            .and_then(|network| network.networks)
            .and_then(|networks| networks.get("bridge").and_then(|n| n.ip_address.clone()))
            .and_then(|ip| ip.parse::<std::net::Ipv4Addr>().ok());
        let listener = if let Some(ip) = ip {
            Some(
                tokio::time::timeout(
                    Duration::from_millis(250),
                    tokio::net::TcpStream::connect((ip, 1433)),
                )
                .await
                .is_ok_and(|r| r.is_ok()),
            )
        } else {
            None
        };
        eprintln!("startup probe phase={phase} listener={listener:?}");
        let options = StatsOptionsBuilder::default()
            .stream(false)
            .one_shot(true)
            .build();
        if let Some(Ok(stats)) = docker.stats(name, Some(options)).next().await {
            let memory = stats.memory_stats.and_then(|m| m.usage);
            let cpu = stats
                .cpu_stats
                .and_then(|c| c.cpu_usage)
                .and_then(|c| c.total_usage);
            let io = stats
                .blkio_stats
                .and_then(|b| b.io_service_bytes_recursive)
                .map(|entries| {
                    entries
                        .iter()
                        .filter(|e| {
                            e.op.as_deref().is_some_and(|op| {
                                op.eq_ignore_ascii_case("read") || op.eq_ignore_ascii_case("write")
                            })
                        })
                        .filter_map(|e| e.value)
                        .fold(0_u64, u64::saturating_add)
                });
            eprintln!(
                "startup probe phase={phase} memory_bytes={memory:?} cpu_ns={cpu:?} io_bytes={io:?}"
            );
        } else {
            eprintln!("startup probe phase={phase} resources_unavailable=true");
        }
        Ok::<_, ()>(())
    };
    if !matches!(tokio::time::timeout(READ_BOUND, read).await, Ok(Ok(()))) {
        eprintln!(
            "startup probe phase={phase} snapshot_incomplete=true elapsed={:?}",
            dispatch.elapsed()
        );
    }
}

#[test]
#[ignore = "requires exclusive Docker lane, OpenSSL, and locally available pinned SQL Server image"]
fn expired_fixture_start_boundary() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            let name = identity();
            let directory = super::TestDirectory(std::env::temp_dir().join(&name));
            super::make_tls_material(&directory.0, true).await;
            let files = archive(&directory.0).expect("diagnostic fixture archive");
            // Fresh disposable credential, never an external secret.
            let password = format!("T!a{}", identity());
            #[cfg(unix)]
            if std::env::var_os("DOCKER_HOST")
                .is_some_and(|value| !value.to_string_lossy().starts_with("unix://"))
            {
                panic!("startup probe requires a local Unix Docker endpoint; refusing fallback");
            }
            let docker = Docker::connect_with_local_defaults()
                .unwrap_or_else(|_| panic!("local Docker transport unavailable"));
            eprintln!("startup probe name={name}; image must already be present; no pull/retry");
            Supervisor::start(docker, name.clone(), config(&name, &password), files)
                .wait()
                .await
                .expect("startup probe failed (see sanitized category)");
        });
    }));
    // Covers unwinding/cancellation too. Maximum supervisor budget: create 30s
    // + upload 30s + start 210s + cleanup 30s; do not shut down its runtime early.
    let cleanup = runtime.block_on(super::wait_for_child_supervisors(Duration::from_secs(310)));
    assert!(
        cleanup.is_ok(),
        "startup supervisor did not drain; reconcile printed name"
    );
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

// These exercise otherwise-private cancellation/ownership branches through a
// real Bollard HTTP transport. A fake Engine is necessary: safely reproducing
// a lost daemon response or delayed create on a shared Docker lane is unreasonable.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn diagnostic_config_matches_tls_fixture_without_starting_docker() {
        let request = super::super::tls_container_request(Path::new("unused-fixture"), "synthetic");
        let diagnostic = config("probe-fixture", "synthetic");
        assert_eq!(
            diagnostic.image.as_deref(),
            Some(request.descriptor().as_str())
        );
        assert_eq!(diagnostic.user.as_deref(), request.user());
        assert_eq!(
            diagnostic.entrypoint,
            Some(vec![request.entrypoint().unwrap().into()])
        );
        assert_eq!(
            diagnostic.cmd,
            Some(request.cmd().map(std::borrow::Cow::into_owned).collect())
        );
        let expected: std::collections::BTreeSet<_> = request
            .env_vars()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        assert_eq!(
            diagnostic
                .env
                .unwrap()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        let mut expected_host = HostConfig::default();
        request.host_config_modifier().unwrap()(&mut expected_host);
        let actual_host = diagnostic.host_config.unwrap();
        assert_eq!(
            actual_host.publish_all_ports,
            expected_host.publish_all_ports
        );
        assert_eq!(actual_host.port_bindings, expected_host.port_bindings);
    }

    #[test]
    fn fixture_archive_preserves_paths_modes_and_synthetic_bytes() {
        let directory = super::super::TestDirectory(std::env::temp_dir().join(identity()));
        std::fs::create_dir(&directory.0).unwrap();
        for file in ["server.pem", "server.key"] {
            std::fs::write(directory.0.join(file), file).unwrap();
        }
        let bytes = archive(&directory.0).unwrap();
        let mut tar = tar::Archive::new(bytes.as_slice());
        let mut entries = tar.entries().unwrap();
        assert!(
            entries
                .next()
                .unwrap()
                .unwrap()
                .header()
                .entry_type()
                .is_dir()
        );
        for (file, mode) in [("server.pem", 0o644), ("server.key", 0o600)] {
            let mut entry = entries.next().unwrap().unwrap();
            assert_eq!(
                entry.path().unwrap().as_ref(),
                Path::new(&format!("fixture/{file}"))
            );
            assert_eq!(entry.header().mode().unwrap(), mode);
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content).unwrap();
            assert_eq!(content, file);
        }
        assert!(entries.next().is_none());
    }

    #[derive(Default)]
    #[allow(
        clippy::struct_excessive_bools,
        reason = "independent fake Engine fault switches, not production state"
    )]
    struct EngineState {
        exists: bool,
        wrong_label: bool,
        lose_create_response: bool,
        delay_create: bool,
        delay_start: bool,
        delay_logs: bool,
        ready: bool,
        first_inspection: Option<Instant>,
        first_log: Option<Instant>,
        events: Vec<&'static str>,
    }

    struct Engine {
        docker: Docker,
        state: Arc<Mutex<EngineState>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Engine {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Engine {
        async fn new(settings: EngineState) -> Self {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            let docker = Docker::connect_with_http(
                &format!("http://{address}"),
                5,
                bollard::API_DEFAULT_VERSION,
            )
            .unwrap();
            let state = Arc::new(Mutex::new(settings));
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let task = {
                let state = state.clone();
                let entered = entered.clone();
                let release = release.clone();
                tokio::spawn(async move {
                    let mut requests = tokio::task::JoinSet::new();
                    loop {
                        tokio::select! {
                            accepted = listener.accept() => {
                                let (socket, _) = accepted.unwrap();
                                requests.spawn(serve(socket, state.clone(), entered.clone(), release.clone()));
                            }
                            _ = requests.join_next(), if !requests.is_empty() => {}
                        }
                    }
                })
            };
            Self {
                docker,
                state,
                entered,
                release,
                task,
            }
        }

        fn supervisor(&self) -> Supervisor {
            self.supervisor_with_schedule(Schedule {
                boundary: Duration::from_millis(30),
                observation: Duration::from_millis(80),
                poll: Duration::from_millis(5),
            })
        }

        fn supervisor_with_schedule(&self, schedule: Schedule) -> Supervisor {
            Supervisor::scheduled(
                self.docker.clone(),
                "probe-fixture".into(),
                config("probe-fixture", "synthetic-not-a-secret"),
                vec![],
                schedule,
            )
        }

        async fn entered(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.entered.notified())
                .await
                .unwrap();
        }

        fn events(&self) -> Vec<&'static str> {
            self.state.lock().unwrap().events.clone()
        }

        async fn removed(&self) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let events = self.events();
                    if events.last() == Some(&"inspect") && events.contains(&"remove") {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("owned container removal must be verified");
            assert!(!self.state.lock().unwrap().exists);
            super::super::wait_for_child_supervisors(Duration::from_secs(2))
                .await
                .expect("cancelled supervisor must finish before its runtime exits");
        }
    }

    async fn request_line(socket: &mut tokio::net::TcpStream) -> Option<String> {
        let mut request = Vec::new();
        let header_end = loop {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            if n == 0 {
                return None;
            }
            request.extend_from_slice(&bytes[..n]);
            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            assert!(request.len() < 64 * 1024);
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let first = headers.lines().next().unwrap().to_owned();
        while request.len() < header_end + length {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            if n == 0 {
                return None;
            }
            request.extend_from_slice(&bytes[..n]);
        }
        Some(first)
    }

    async fn serve(
        mut socket: tokio::net::TcpStream,
        state: Arc<Mutex<EngineState>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    ) {
        let Some(first) = request_line(&mut socket).await else {
            return;
        };
        // No request body or error body is ever printed, even by this fake.
        let (status, body, content_type) = if first.contains("/containers/create") {
            let delayed = state.lock().unwrap().delay_create;
            if delayed {
                entered.notify_one();
                release.notified().await;
            }
            let mut state = state.lock().unwrap();
            state.exists = true;
            state.events.push("create-ack");
            if state.lose_create_response {
                return;
            }
            (
                201,
                r#"{"Id":"owned-id","Warnings":[]}"#.to_owned(),
                "application/json",
            )
        } else if first.contains("/archive") {
            state.lock().unwrap().events.push("upload");
            (200, String::new(), "application/json")
        } else if first.contains("/start") {
            state.lock().unwrap().events.push("start-dispatch");
            if state.lock().unwrap().delay_start {
                entered.notify_one();
                release.notified().await;
            }
            state.lock().unwrap().events.push("start-response");
            (204, String::new(), "application/json")
        } else if first.contains("/json") {
            let mut state = state.lock().unwrap();
            state.events.push("inspect");
            state.first_inspection.get_or_insert_with(Instant::now);
            if state.exists {
                let label = if state.wrong_label {
                    "someone-else"
                } else {
                    "probe-fixture"
                };
                (
                    200,
                    format!(
                        r#"{{"Id":"owned-id","Config":{{"Labels":{{"{LABEL}":"{label}"}}}},"State":{{"Running":true,"ExitCode":0,"OOMKilled":false}}}}"#
                    ),
                    "application/json",
                )
            } else {
                (404, r#"{"message":"absent"}"#.into(), "application/json")
            }
        } else if first.contains("/logs") {
            let delayed = {
                let mut state = state.lock().unwrap();
                state.events.push("logs");
                state.first_log.get_or_insert_with(Instant::now);
                state.delay_logs
            };
            if delayed {
                entered.notify_one();
                release.notified().await;
            }
            let mut state = state.lock().unwrap();
            state.events.push("logs-response");
            let mut body = String::new();
            if state.ready {
                // Two multiplexed frames force the classifier to handle a
                // marker crossing Docker log chunk boundaries.
                let (left, right) = super::super::TLS_READY_MARKER.split_at(12);
                for part in [left, right] {
                    body.push_str("\x01\0\0\0\0\0\0");
                    body.push(char::from(u8::try_from(part.len()).unwrap()));
                    body.push_str(part);
                }
            }
            (200, body, "application/vnd.docker.raw-stream")
        } else if first.contains("/stats") {
            state.lock().unwrap().events.push("stats");
            (
                200,
                r#"{"memory_stats":{"usage":4096}}"#.into(),
                "application/json",
            )
        } else if first.starts_with("DELETE ") && first.contains("/containers/owned-id") {
            let mut state = state.lock().unwrap();
            state.events.push("remove");
            state.exists = false;
            (204, String::new(), "application/json")
        } else {
            panic!("unexpected fake Engine route");
        };
        let response = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );
        let _ = socket.write_all(response.as_bytes()).await;
    }

    #[tokio::test]
    async fn cancelled_create_is_drained_before_label_checked_removal() {
        let engine = Engine::new(EngineState {
            delay_create: true,
            ..Default::default()
        })
        .await;
        let supervisor = engine.supervisor();
        engine.entered().await;
        drop(supervisor);
        assert!(!engine.events().contains(&"remove"));
        engine.release.notify_one();
        engine.removed().await;
        assert_eq!(
            engine.events(),
            ["create-ack", "inspect", "remove", "inspect"]
        );
    }

    #[tokio::test]
    async fn cancelled_start_is_joined_before_removal() {
        let engine = Engine::new(EngineState {
            delay_start: true,
            ..Default::default()
        })
        .await;
        let supervisor = engine.supervisor();
        engine.entered().await;
        drop(supervisor);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!engine.events().contains(&"remove"));
        engine.release.notify_one();
        engine.removed().await;
        let events = engine.events();
        assert!(
            events.iter().position(|e| *e == "start-response").unwrap()
                < events.iter().position(|e| *e == "remove").unwrap()
        );
    }

    #[tokio::test]
    async fn boundary_snapshot_does_not_wait_for_start_response() {
        let engine = Engine::new(EngineState {
            delay_start: true,
            ..Default::default()
        })
        .await;
        let supervisor = engine.supervisor();
        engine.entered().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !engine.events().contains(&"stats") {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert!(!engine.events().contains(&"start-response"));
        engine.release.notify_one();
        assert!(supervisor.wait().await.is_err()); // No marker, not a silent pass.
        engine.removed().await;
    }

    #[tokio::test]
    async fn boundary_snapshot_is_dispatched_while_log_response_is_held() {
        let engine = Engine::new(EngineState {
            delay_logs: true,
            ready: true,
            ..Default::default()
        })
        .await;
        let boundary = Duration::from_millis(30);
        let before = Instant::now();
        let supervisor = engine.supervisor_with_schedule(Schedule {
            boundary,
            // Longer than READ_BOUND: the old serial poll would inspect only
            // after its three-second log timeout. This test requires the
            // boundary inspection within one second, before releasing logs.
            observation: Duration::from_secs(5),
            poll: Duration::from_millis(5),
        });
        engine.entered().await;
        let snapshot_seen = tokio::time::timeout(Duration::from_secs(1), async {
            while !engine.events().contains(&"stats") {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        let (first_inspection, first_log, held) = {
            let state = engine.state.lock().unwrap();
            (
                state.first_inspection,
                state.first_log,
                !state.events.contains(&"logs-response"),
            )
        };
        engine.release.notify_one();
        let result = supervisor.wait().await;
        engine.removed().await;
        assert!(
            snapshot_seen.is_ok(),
            "boundary snapshot must not wait for the log timeout"
        );
        assert!(
            held,
            "log response must still be held when the snapshot is observed"
        );
        let inspected_at = first_inspection.expect("boundary inspection timestamp");
        assert!(
            inspected_at >= before + boundary,
            "snapshot must not run early"
        );
        assert!(inspected_at < before + Duration::from_secs(1));
        assert!(
            first_log.unwrap() < inspected_at,
            "log request must precede boundary"
        );
        assert!(result.is_ok(), "released marker must still be observed");
        let events = engine.events();
        assert!(
            events.iter().position(|e| *e == "inspect").unwrap()
                < events.iter().position(|e| *e == "logs-response").unwrap()
        );
    }

    #[tokio::test]
    async fn early_marker_is_classified_across_frames_and_retained_through_boundary() {
        let engine = Engine::new(EngineState {
            ready: true,
            ..Default::default()
        })
        .await;
        let before = Instant::now();
        assert!(engine.supervisor().wait().await.is_ok());
        assert!(before.elapsed() >= Duration::from_millis(30));
        engine.removed().await;
        assert!(engine.events().contains(&"stats"));
    }

    #[tokio::test]
    async fn lost_create_response_reconciles_labeled_container_but_fails_probe() {
        let engine = Engine::new(EngineState {
            lose_create_response: true,
            ..Default::default()
        })
        .await;
        assert!(engine.supervisor().wait().await.is_err());
        engine.removed().await;
        assert!(!engine.events().contains(&"start-dispatch"));
    }

    #[tokio::test]
    async fn cleanup_refuses_foreign_label() {
        let engine = Engine::new(EngineState {
            exists: true,
            wrong_label: true,
            ..Default::default()
        })
        .await;
        assert!(
            cleanup(&engine.docker, "probe-fixture", "probe-fixture", true)
                .await
                .is_err()
        );
        assert!(!engine.events().contains(&"remove"));
        assert!(engine.state.lock().unwrap().exists);
    }

    #[tokio::test]
    async fn absent_after_unacknowledged_create_is_not_verified_cleanup() {
        let engine = Engine::new(EngineState::default()).await;
        assert!(
            cleanup(&engine.docker, "probe-fixture", "probe-fixture", false)
                .await
                .is_err()
        );
        assert!(!engine.events().contains(&"remove"));
    }
}
