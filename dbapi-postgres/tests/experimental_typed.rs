//! Typed operations use the compiled quiet pool and a real disposable PostgreSQL.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi::{DbType, ErrorKind, Parameter, RowLimit, Value};
use neotales_dbapi_postgres::experimental::{Config, Error, Lease, Pool, Profile};

fn pool() -> Pool {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(55433)
        .user("postgres")
        .dbname("postgres");
    Pool::loopback_plaintext_for_tests(
        config,
        1,
        Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
    )
    .unwrap()
}

fn parameter(kind: DbType, value: Value) -> Parameter {
    Parameter::new(kind, value).unwrap()
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).unwrap()
}

async fn fresh() -> Lease {
    pool().acquire().await.unwrap()
}

#[tokio::test]
async fn polymorphic_inference_probes_preserve_fully_typed_execute_and_fetch() {
    let mut lease = fresh().await;
    for (sql, expected) in [
        ("SELECT to_json($1)::text AS v", "7"),
        ("SELECT to_jsonb($1)::text AS v", "7"),
        ("SELECT pg_typeof($1)::text AS v", "bigint"),
        (
            "SELECT v::text AS v FROM (SELECT $1 AS v, $1::bigint AS typed) AS s",
            "7",
        ),
    ] {
        let parameters = [parameter(DbType::I64, Value::I64(7))];
        assert_eq!(lease.execute(sql, &parameters).await.unwrap(), 1);
        assert_eq!(
            lease.fetch(sql, &parameters, limit(1, 100)).await.unwrap()[0]
                .get::<String>("v")
                .unwrap(),
            expected
        );
    }
    for (kind, type_name) in [
        (DbType::Bool, "boolean"),
        (DbType::I64, "bigint"),
        (DbType::F64, "double precision"),
        (DbType::Text, "text"),
        (DbType::Bytes, "bytea"),
    ] {
        let parameters = [parameter(kind, Value::Null(kind))];
        for (sql, expected) in [
            ("SELECT to_json($1)::text AS v", None),
            ("SELECT to_jsonb($1)::text AS v", None),
            ("SELECT pg_typeof($1)::text AS v", Some(type_name)),
        ] {
            assert_eq!(lease.execute(sql, &parameters).await.unwrap(), 1);
            assert_eq!(
                lease.fetch(sql, &parameters, limit(1, 100)).await.unwrap()[0]
                    .get::<Option<String>>("v")
                    .unwrap()
                    .as_deref(),
                expected
            );
        }
    }
    assert_eq!(lease.execute("SELECT 1", &[]).await.unwrap(), 1);
    lease.recycle().await.unwrap();
}

#[tokio::test]
async fn probe_allowlist_never_accepts_extra_bindings_or_full_prepare_type_errors() {
    for sql in [
        "SELECT to_json($1)::text AS v",
        "SELECT to_jsonb($1)::text AS v",
        "SELECT pg_typeof($1)::text AS v",
        // This fails the *full* typed Parse with 42804, not only the probe.
        "SELECT COALESCE($1, 1::bigint)::text AS v",
    ] {
        let full_type_error = sql.contains("COALESCE");
        let parameters = if full_type_error {
            vec![parameter(DbType::Text, Value::Null(DbType::Text))]
        } else {
            vec![
                parameter(DbType::I64, Value::Null(DbType::I64)),
                parameter(DbType::Bool, Value::Bool(false)),
            ]
        };
        for fetch in [false, true] {
            let mut lease = fresh().await;
            let error = if fetch {
                lease
                    .fetch(sql, &parameters, limit(1, 100))
                    .await
                    .unwrap_err()
            } else {
                lease.execute(sql, &parameters).await.unwrap_err()
            };
            assert_eq!(
                error.kind(),
                if full_type_error {
                    ErrorKind::Decode
                } else {
                    ErrorKind::InvalidArgument
                }
            );
            assert!(std::error::Error::source(&error).is_none());
            assert_eq!(
                lease.execute("SELECT 1", &[]).await.unwrap_err().kind(),
                ErrorKind::Connection
            );
            assert_eq!(lease.recycle().await, Err(Error::Poisoned));
        }
    }
}

#[tokio::test]
async fn exactly_one_statement_rejects_empty_comments_and_semicolons() {
    for sql in [
        "",
        " \t\r\n",
        "-- nothing",
        "-- line\r\n",
        "/* nothing */",
        "/* outer /* nested */ outer */",
        "; ; -- comment\n /* block */ ;",
    ] {
        for fetch in [false, true] {
            let mut lease = fresh().await;
            let error = if fetch {
                lease.fetch(sql, &[], limit(1, 100)).await.unwrap_err()
            } else {
                lease.execute(sql, &[]).await.unwrap_err()
            };
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert_eq!(
                lease.execute("SELECT 1", &[]).await.unwrap_err().kind(),
                ErrorKind::Connection
            );
            assert_eq!(lease.recycle().await, Err(Error::Poisoned));
        }
    }
    let mut lease = fresh().await;
    assert_eq!(
        lease
            .execute(
                "/* outer /* nested */ */ ; -- line\n SELECT 1; /* tail */",
                &[]
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        lease
            .fetch(
                "-- prefix\r SELECT '; /* literal */'::text AS v; -- suffix",
                &[],
                limit(1, 100),
            )
            .await
            .unwrap()[0]
            .get::<String>("v")
            .unwrap(),
        "; /* literal */"
    );
    lease.recycle().await.unwrap();
}

#[derive(Clone, Copy)]
enum ProbeResponse {
    InconsistentType,
    Fatal,
    MissingSeverity,
    Disconnect,
}

fn rewrite_probe_error(body: &[u8], response: ProbeResponse) -> Vec<u8> {
    let mut rewritten = Vec::new();
    let mut required = [false; 2];
    for field in body
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
    {
        match field[0] {
            b'C' => {
                assert_eq!(&field[1..], b"42804");
                rewritten.extend_from_slice(
                    if matches!(response, ProbeResponse::InconsistentType) {
                        b"C42P08\0"
                    } else {
                        b"C42804\0"
                    },
                );
                required[0] = true;
            }
            b'V' => {
                assert_eq!(&field[1..], b"ERROR");
                match response {
                    ProbeResponse::Fatal => rewritten.extend_from_slice(b"VFATAL\0"),
                    ProbeResponse::MissingSeverity => {}
                    _ => rewritten.extend_from_slice(b"VERROR\0"),
                }
                required[1] = true;
            }
            b'M' => rewritten.extend_from_slice(b"Mprobe_fault_server_canary\0"),
            _ => {
                rewritten.extend_from_slice(field);
                rewritten.push(0);
            }
        }
    }
    assert!(required.into_iter().all(|field| field));
    rewritten.push(0);
    rewritten
}

#[allow(clippy::too_many_lines)] // One wire fault and client lifecycle share the same ordering.
async fn probe_response(response: ProbeResponse, fetch: bool) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        let (incoming, _) = listener.accept().await.unwrap();
        let upstream = tokio::net::TcpStream::connect(("127.0.0.1", 55433))
            .await
            .unwrap();
        let (mut client_read, mut client_write) = incoming.into_split();
        let (mut server_read, mut server_write) = upstream.into_split();
        let outbound = async {
            let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
            let _ = server_write.shutdown().await;
        };
        let inbound = async {
            let mut observed = false;
            while let Ok(tag) = server_read.read_u8().await {
                let len = server_read.read_u32().await.unwrap();
                assert!((4..=1_048_576).contains(&len));
                let mut body = vec![0; usize::try_from(len - 4).unwrap()];
                server_read.read_exact(&mut body).await.unwrap();
                if tag == b'E' {
                    assert!(!observed);
                    body = rewrite_probe_error(&body, response);
                    observed = true;
                    if matches!(response, ProbeResponse::Disconnect) {
                        break;
                    }
                }
                let mut frame = vec![tag];
                frame.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
                frame.extend_from_slice(&body);
                if client_write.write_all(&frame).await.is_err() {
                    break;
                }
            }
            let _ = client_write.shutdown().await;
            assert!(
                observed,
                "shortened live Parse did not produce the expected error"
            );
        };
        tokio::join!(outbound, inbound);
    });
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    let pool = Pool::loopback_plaintext_for_tests(
        config,
        1,
        Profile::normal(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
    )
    .unwrap();
    let mut lease = pool.acquire().await.unwrap();
    let parameters = [parameter(DbType::I64, Value::I64(7))];
    let result = if fetch {
        lease
            .fetch("SELECT to_json($1)::text AS v", &parameters, limit(1, 100))
            .await
            .map(|rows| {
                assert_eq!(rows[0].get::<String>("v").unwrap(), "7");
            })
    } else {
        lease
            .execute("SELECT to_json($1)::text AS v", &parameters)
            .await
            .map(|count| {
                assert_eq!(count, 1);
            })
    };
    if matches!(response, ProbeResponse::InconsistentType) {
        result.unwrap();
        lease.recycle().await.unwrap();
    } else {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Connection);
        assert!(!format!("{error:?} {error}").contains("probe_fault_server_canary"));
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(
            lease.execute("SELECT 1", &[]).await.unwrap_err().kind(),
            ErrorKind::Connection
        );
        assert_eq!(lease.recycle().await, Err(Error::Poisoned));
    }
    pool.close();
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.free_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed arity probe leaked capacity");
}

#[tokio::test]
async fn shortened_probe_requires_nonfatal_known_severity_and_valid_transport() {
    for response in [
        ProbeResponse::InconsistentType,
        ProbeResponse::Fatal,
        ProbeResponse::MissingSeverity,
        ProbeResponse::Disconnect,
    ] {
        for fetch in [false, true] {
            tokio::time::timeout(Duration::from_secs(5), probe_response(response, fetch))
                .await
                .expect("probe fault did not settle");
        }
    }
}

#[tokio::test]
async fn types_nulls_native_parameter_order_and_detached_duplicate_names() {
    let pool = pool();
    let mut lease = pool.acquire().await.unwrap();
    let parameters = [
        parameter(DbType::Bool, Value::Bool(true)),
        parameter(DbType::I64, Value::I64(i64::MIN)),
        parameter(DbType::F64, Value::F64(1.25)),
        parameter(DbType::Text, Value::Text("héllo $1 ? ' ; --".into())),
        parameter(DbType::Bytes, Value::Bytes(vec![0, 1, 255])),
    ];
    let rows = lease
        .fetch(
            "SELECT $1 AS b, $2 AS i, $3 AS f, $4 AS t, $5 AS x",
            &parameters,
            limit(1, 200),
        )
        .await
        .unwrap();
    assert!(rows[0].get::<bool>("b").unwrap());
    assert_eq!(rows[0].get::<i64>("i").unwrap(), i64::MIN);
    assert_eq!(
        rows[0].get::<f64>("f").unwrap().to_bits(),
        1.25_f64.to_bits()
    );
    assert_eq!(rows[0].get::<String>("t").unwrap(), "héllo $1 ? ' ; --");
    assert_eq!(rows[0].get::<Vec<u8>>("x").unwrap(), vec![0, 1, 255]);
    let nulls: Vec<_> = [
        DbType::Bool,
        DbType::I64,
        DbType::F64,
        DbType::Text,
        DbType::Bytes,
    ]
    .into_iter()
    .map(|kind| parameter(kind, Value::Null(kind)))
    .collect();
    let null_rows = lease
        .fetch(
            "SELECT $1 AS b, $2 AS i, $3 AS f, $4 AS t, $5 AS x",
            &nulls,
            limit(1, 5),
        )
        .await
        .unwrap();
    assert_eq!(null_rows[0].get::<Option<bool>>("b").unwrap(), None);
    assert_eq!(null_rows[0].get::<Option<i64>>("i").unwrap(), None);
    assert_eq!(null_rows[0].get::<Option<f64>>("f").unwrap(), None);
    assert_eq!(null_rows[0].get::<Option<String>>("t").unwrap(), None);
    assert_eq!(null_rows[0].get::<Option<Vec<u8>>>("x").unwrap(), None);
    assert_eq!(
        null_rows[0].get::<bool>("b").unwrap_err().kind(),
        ErrorKind::Null
    );
    let ordered = [
        parameter(DbType::I64, Value::I64(7)),
        parameter(DbType::Text, Value::Text("second".into())),
    ];
    let repeated = lease.fetch(
        "SELECT $2 AS dup, $1 AS dup, $1 AS again, '$3'::text AS literal, $$ $4 $$::text AS quoted /* $5 */ -- $6\n",
        &ordered, limit(1, 100),
    ).await.unwrap();
    assert_eq!(
        repeated[0].get::<i64>("dup").unwrap_err().kind(),
        ErrorKind::Ambiguous
    );
    assert_eq!(repeated[0].get_ordinal::<String>(0).unwrap(), "second");
    assert_eq!(repeated[0].get_ordinal::<i64>(1).unwrap(), 7);
    assert_eq!(repeated[0].get::<i64>("again").unwrap(), 7);
    assert_eq!(repeated[0].get::<String>("literal").unwrap(), "$3");
    assert_eq!(
        repeated[0].get::<String>("missing").unwrap_err().kind(),
        ErrorKind::Missing
    );
    assert_eq!(
        repeated[0].get::<String>("again").unwrap_err().kind(),
        ErrorKind::WrongType
    );
    let varchar = lease
        .fetch("SELECT 'ok'::varchar AS v", &[], limit(1, 3))
        .await
        .unwrap();
    assert_eq!(varchar[0].get::<String>("v").unwrap(), "ok");
    lease.recycle().await.unwrap();
    // No driver rows/statement handles escape; old rows survive DISCARD and reuse.
    let mut reused = pool.acquire().await.unwrap();
    reused.execute("SELECT 1", &[]).await.unwrap();
    reused.recycle().await.unwrap();
    pool.close();
    assert_eq!(rows[0].get::<i64>("i").unwrap(), i64::MIN);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One independent observer verifies every pre-dispatch rejection.
async fn arity_validation_and_statement_script_separation_prevent_dispatch() {
    let observer = pool();
    let mut observer = observer.acquire().await.unwrap();
    observer
        .execute_trusted_script("CREATE TABLE quiet_typed_arity(v bigint)")
        .await
        .unwrap();
    for (sql, parameters) in [
        (
            "INSERT INTO quiet_typed_arity(v) VALUES (1)",
            vec![parameter(DbType::I64, Value::I64(9))],
        ),
        ("INSERT INTO quiet_typed_arity(v) VALUES ($1)", vec![]),
        (
            "INSERT INTO quiet_typed_arity(v) VALUES ($2)",
            vec![parameter(DbType::I64, Value::I64(9))],
        ),
        (
            "INSERT INTO quiet_typed_arity(v) VALUES ($1)",
            vec![
                parameter(DbType::I64, Value::I64(9)),
                parameter(DbType::I64, Value::I64(10)),
            ],
        ),
        (
            "INSERT INTO quiet_typed_arity(v) VALUES (1); INSERT INTO quiet_typed_arity(v) VALUES (2)",
            vec![],
        ),
    ] {
        let mut lease = fresh().await;
        assert!(lease.execute(sql, &parameters).await.is_err());
        assert_eq!(lease.recycle().await, Err(Error::Poisoned));
        // An independent connection proves no write committed, even after discard.
        assert_eq!(
            observer
                .fetch(
                    "SELECT count(*) AS n FROM quiet_typed_arity",
                    &[],
                    limit(1, 9),
                )
                .await
                .unwrap()[0]
                .get::<i64>("n")
                .unwrap(),
            0
        );
    }
    let mut lease = fresh().await;
    assert_eq!(
        lease
            .fetch(
                "INSERT INTO quiet_typed_arity VALUES (1) RETURNING v::int4",
                &[],
                limit(1, 100),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WrongType
    );
    assert_eq!(lease.recycle().await, Err(Error::Poisoned));
    assert_eq!(
        observer
            .fetch(
                "SELECT count(*) AS n FROM quiet_typed_arity",
                &[],
                limit(1, 9),
            )
            .await
            .unwrap()[0]
            .get::<i64>("n")
            .unwrap(),
        0
    );
    let mut lease = fresh().await;
    lease.execute_trusted_script(
        "CREATE TEMP TABLE typed_script(v bigint); INSERT INTO typed_script VALUES (1); INSERT INTO typed_script VALUES (2)",
    ).await.unwrap();
    assert_eq!(
        lease
            .execute(
                "INSERT INTO typed_script VALUES ($1)",
                &[parameter(DbType::I64, Value::I64(3))],
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        lease
            .fetch("SELECT count(*) AS n FROM typed_script", &[], limit(1, 9))
            .await
            .unwrap()[0]
            .get::<i64>("n")
            .unwrap(),
        3
    );
    lease.recycle().await.unwrap();
    observer
        .execute("DROP TABLE quiet_typed_arity", &[])
        .await
        .unwrap();
    observer.recycle().await.unwrap();
}

#[tokio::test]
async fn aggregate_limits_exact_boundaries_and_strict_result_types() {
    // Two rows: name "é" is 2 UTF-8 bytes, text "界" is 3 per row.
    let mut lease = fresh().await;
    assert_eq!(
        lease
            .fetch(
                "SELECT '界'::text AS \"é\" FROM generate_series(1,2)",
                &[],
                limit(2, 10),
            )
            .await
            .unwrap()
            .len(),
        2
    );
    // Bool + int8 + float8 + bytea = 1+8+8+2 payload, 4 bytes of names.
    assert_eq!(
        lease
            .fetch(
                "SELECT true AS b, 1::bigint AS i, 1.25::float8 AS f, '\\x00ff'::bytea AS x",
                &[],
                limit(1, 23),
            )
            .await
            .unwrap()
            .len(),
        1
    );
    lease.recycle().await.unwrap();
    for (sql, limit) in [
        (
            "SELECT '界'::text AS \"é\" FROM generate_series(1,2)",
            limit(2, 9),
        ),
        (
            "SELECT 'x'::text AS t FROM generate_series(1,2)",
            limit(1, 100),
        ),
        ("SELECT repeat('x',10000) AS t", limit(1, 100)),
        ("SELECT 'NaN'::float8 AS n", limit(1, 100)),
        ("SELECT 'Infinity'::float8 AS n", limit(1, 100)),
    ] {
        let mut lease = fresh().await;
        let error = lease.fetch(sql, &[], limit).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
        assert_eq!(lease.recycle().await, Err(Error::Poisoned));
    }
    for sql in [
        "SELECT 1::int4 AS x",
        "SELECT NULL::numeric AS x",
        "SELECT now() AS x",
    ] {
        let mut lease = fresh().await;
        assert_eq!(
            lease
                .fetch(sql, &[], limit(1, 100))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::WrongType
        );
        assert_eq!(lease.recycle().await, Err(Error::Poisoned));
    }
    assert_eq!(
        Parameter::new(DbType::F64, Value::F64(f64::NAN))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        Parameter::new(DbType::Bool, Value::I64(1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
}

#[tokio::test]
async fn sanitized_sqlstate_categories_and_fetch_cancellation() {
    for (sql, expected) in [
        (
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='typed_error_canary'; END $$",
            ErrorKind::Constraint,
        ),
        (
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='22003', MESSAGE='typed_error_canary'; END $$",
            ErrorKind::Overflow,
        ),
        (
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='typed_error_canary'; END $$",
            ErrorKind::Busy,
        ),
        (
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='57014', MESSAGE='typed_error_canary'; END $$",
            ErrorKind::Cancelled,
        ),
        (
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE='40003', MESSAGE='typed_error_canary'; END $$",
            ErrorKind::WriteOutcomeUnknown,
        ),
    ] {
        let mut lease = fresh().await;
        let error = lease.execute_trusted_script(sql).await.unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(std::error::Error::source(&error).is_none());
        assert!(!format!("{error:?} {error}").contains("typed_error_canary"));
    }
    let mut lease = fresh().await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            lease.fetch("SELECT 1::bigint AS n FROM pg_sleep(1)", &[], limit(1, 9)),
        )
        .await
        .is_err()
    );
    assert_eq!(lease.recycle().await, Err(Error::Poisoned));
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Logger(Mutex<Vec<String>>);
impl log::Log for Logger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }
    fn flush(&self) {}
}
static LOGS: Logger = Logger(Mutex::new(Vec::new()));

#[tokio::test]
async fn typed_sql_parameters_notices_and_errors_do_not_log() {
    log::set_logger(&LOGS).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let capture = Capture::default();
    let writer = capture.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();
    let mut lease = fresh().await;
    lease
        .execute_trusted_script("DO $$ BEGIN RAISE WARNING 'typed_notice_canary'; END $$")
        .await
        .unwrap();
    let rows = lease
        .fetch(
            "SELECT $1 AS v /* typed_sql_canary */",
            &[parameter(
                DbType::Text,
                Value::Text("typed_bind_canary".into()),
            )],
            limit(1, 100),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("v").unwrap(), "typed_bind_canary");
    lease
        .execute(
            "SELECT $1 /* typed_execute_canary */",
            &[parameter(
                DbType::Bytes,
                Value::Bytes(b"typed_bytes_canary".to_vec()),
            )],
        )
        .await
        .unwrap();
    let error = lease
        .execute_trusted_script("DO $$ BEGIN RAISE EXCEPTION 'typed_server_canary'; END $$")
        .await
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("typed_server_canary"));
    tracing::info!("typed_caller_trace_sentinel");
    log::info!("typed_caller_log_sentinel");
    let trace = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let logs = LOGS.0.lock().unwrap().join("\n");
    assert!(trace.contains("typed_caller_trace_sentinel"));
    assert!(logs.contains("typed_caller_log_sentinel"));
    for canary in [
        "typed_notice_canary",
        "typed_sql_canary",
        "typed_bind_canary",
        "typed_execute_canary",
        "typed_bytes_canary",
        "typed_server_canary",
    ] {
        assert!(!trace.contains(canary));
        assert!(!logs.contains(canary));
    }
}
