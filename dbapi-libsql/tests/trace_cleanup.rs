//! A deterministic Hrana peer exercises the actual spawned cleanup paths.
use neotales_dbapi::{DbType, Parameter, QueryOptions, RowLimit, Session, Value};
use neotales_dbapi_libsql::{LibsqlConfig, LibsqlPoolProvider};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const BATON: &str = "known-server-baton";
const TOKEN: &str = "known-bearer-token";
const SQL: &str = "known-sql-marker";
const ARG: &str = "known-bound-marker";
const DONE: &str = "transaction rollback cleanup finished";
const PROBE_BATON: &str = "probe-stream-baton";

struct Capture {
    messages: Arc<Mutex<String>>,
    done: Mutex<Option<oneshot::Sender<()>>>,
}
struct Visitor(String);
impl tracing::field::Visit for Visitor {
    fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{value:?}");
    }
}
impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains(DONE)
            && let Some(done) = self.done.lock().unwrap().take()
        {
            let _ = done.send(());
        }
        self.messages.lock().unwrap().push_str(&visitor.0);
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> (String, serde_json::Value) {
    let mut bytes = Vec::new();
    let (header_end, length) = loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() < 128 * 1024);
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let end = index + 4;
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end, length);
        }
    };
    while bytes.len() < header_end + length {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() < 128 * 1024);
    }
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let body = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
    (headers, body)
}

fn response(body: &serde_json::Value) -> serde_json::Value {
    let requests = body["requests"].as_array().unwrap();
    let rollback = requests.iter().any(|req| req["stmt"]["sql"] == "ROLLBACK");
    let results: Vec<_> = requests.iter().map(|req| {
        let result = serde_json::json!({"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null});
        let response = match req["type"].as_str().unwrap() {
            "batch" => serde_json::json!({"type":"batch","result":{"step_results":[result],"step_errors":[null]}}),
            "execute" => serde_json::json!({"type":"execute","result":result}),
            "get_autocommit" => serde_json::json!({"type":"get_autocommit","is_autocommit":rollback}),
            "close" => serde_json::json!({"type":"close"}),
            other => panic!("unexpected request type {other}"),
        };
        serde_json::json!({"type":"ok","response":response})
    }).collect();
    let closed = requests.iter().any(|req| req["type"] == "close");
    serde_json::json!({"baton":if closed { None } else { Some(BATON) },"base_url":null,"results":results})
}

async fn exercise_cleanup(transaction: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (processed_tx, processed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (headers, body) = read_request(&mut socket).await;
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains(&format!("authorization: bearer {TOKEN}").to_ascii_lowercase())
            );
            if index > 0 {
                assert_eq!(body["baton"], BATON);
            }
            if index == 1 {
                let text = body.to_string();
                assert!(text.contains(SQL) && text.contains(ARG));
            }
            if index == 2 {
                assert_eq!(body.to_string().contains("ROLLBACK"), transaction);
                assert!(
                    body["requests"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|v| v["type"] == "close")
                );
            }
            let response = response(&body).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            seen.push(body);
        }
        processed_tx.send(()).unwrap();
        seen
    });
    let logs = Arc::new(Mutex::new(String::new()));
    let (done_tx, done_rx) = oneshot::channel();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::TRACE)
        .with(Capture {
            messages: logs.clone(),
            done: Mutex::new(Some(done_tx)),
        });
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let db = libsql::Builder::new_remote(endpoint, TOKEN.to_owned())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    if transaction {
        let tx = conn.transaction().await.unwrap();
        tx.execute("SELECT ?1 || 'known-sql-marker'", [ARG])
            .await
            .unwrap();
        drop(tx);
    } else {
        conn.execute("BEGIN", ()).await.unwrap();
        conn.execute("SELECT ?1 || 'known-sql-marker'", [ARG])
            .await
            .unwrap();
    }
    drop(conn);
    drop(db);

    tokio::time::timeout(Duration::from_secs(5), processed_rx)
        .await
        .unwrap()
        .unwrap();
    if transaction {
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
    }
    let seen = server.await.unwrap();
    assert_eq!(seen.len(), 3);
    let captured = logs.lock().unwrap();
    assert!(captured.contains("client stream"));
    if transaction {
        assert!(captured.contains(DONE));
    } else {
        assert!(captured.contains("closing client stream"));
    }
    for secret in [BATON, TOKEN, SQL, ARG] {
        assert!(!captured.contains(secret), "trace exposed a secret");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_transaction_completes_rollback_without_logging_secrets() {
    exercise_cleanup(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_connection_sends_close_without_logging_secrets() {
    exercise_cleanup(false).await;
}

async fn provider_mock(
    listener: TcpListener,
    drop_started: Arc<AtomicBool>,
    closed_tx: oneshot::Sender<()>,
) {
    let mut session_sql_seen = false;
    let mut session_cursor_seen = false;
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (headers, body) = read_request(&mut socket).await;
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {TOKEN}").to_ascii_lowercase())
        );
        let is_cursor = headers.starts_with("POST /v3/cursor ");
        let is_session_close;
        let reply = if is_cursor {
            let baton = body["baton"].as_str().unwrap();
            assert!(baton == BATON || baton == PROBE_BATON);
            if baton == BATON {
                let request = body.to_string();
                assert!(request.contains(SQL) && request.contains(ARG));
                assert!(session_sql_seen);
                session_cursor_seen = true;
            }
            is_session_close = false;
            format!(
                "{}\n{}\n{}\n",
                serde_json::json!({"baton":baton,"base_url":null}),
                serde_json::json!({"type":"step_begin","step":0,"cols":[]}),
                serde_json::json!({"type":"step_end","affected_row_count":0,"last_inserted_rowid":null})
            )
        } else {
            let requests = body["requests"].as_array().unwrap();
            if requests[0]["type"] == "describe" {
                assert!(body["baton"].is_null());
                let sql = requests[0]["sql"].as_str().unwrap();
                let baton = if sql.contains(SQL) {
                    session_sql_seen = true;
                    BATON
                } else {
                    assert_eq!(sql, "SELECT 1");
                    PROBE_BATON
                };
                is_session_close = false;
                serde_json::json!({
                    "baton":baton,
                    "base_url":null,
                    "results":[{"type":"ok","response":{
                        "type":"describe",
                        "result":{"params":[],"cols":[],"is_explain":false,"is_readonly":true}
                    }}]
                })
                .to_string()
            } else {
                assert_eq!(requests[0]["type"], "close");
                let baton = body["baton"].as_str().unwrap();
                assert!(baton == BATON || baton == PROBE_BATON);
                is_session_close = baton == BATON;
                if is_session_close {
                    assert!(session_sql_seen && session_cursor_seen);
                    assert!(drop_started.load(Ordering::SeqCst));
                }
                serde_json::json!({
                    "baton":null,"base_url":null,
                    "results":[{"type":"ok","response":{"type":"close"}}]
                })
                .to_string()
            }
        };
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{reply}",
                    reply.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        if is_session_close {
            closed_tx.send(()).unwrap();
            break;
        }
    }
}

async fn exercise_provider_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let drop_started = Arc::new(AtomicBool::new(false));
    let (closed_tx, mut closed_rx) = oneshot::channel();
    let server = tokio::spawn(provider_mock(listener, drop_started.clone(), closed_tx));
    let logs = Arc::new(Mutex::new(String::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::TRACE)
        .with(Capture {
            messages: logs.clone(),
            done: Mutex::new(None),
        });
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let provider =
        LibsqlPoolProvider::connect(LibsqlConfig::plaintext_loopback(endpoint, TOKEN).unwrap())
            .await
            .unwrap();
    let mut session = provider.acquire_libsql_session().await.unwrap();
    let parameters = [Parameter::new(DbType::Text, Value::Text(ARG.into())).unwrap()];
    let rows = session
        .fetch_all_async(
            "SELECT ?1 || 'known-sql-marker'",
            QueryOptions::new()
                .bind_all(&parameters)
                .limit(RowLimit::new(1, 1024).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
    assert!(matches!(
        closed_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let before_drop = logs.lock().unwrap().len();
    drop_started.store(true, Ordering::SeqCst);
    drop(session);
    tokio::time::timeout(Duration::from_secs(5), closed_rx)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    drop(provider);
    let captured = logs.lock().unwrap();
    assert!(captured[before_drop..].contains("closing client stream"));
    for secret in [BATON, PROBE_BATON, TOKEN, SQL, ARG] {
        assert!(!captured.contains(secret), "trace exposed a secret");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_provider_session_closes_known_stream_without_logging_secrets() {
    tokio::time::timeout(Duration::from_secs(10), exercise_provider_session())
        .await
        .expect("provider session cleanup did not finish");
}
