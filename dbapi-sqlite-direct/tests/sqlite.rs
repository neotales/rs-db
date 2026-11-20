//! Real-file contract tests for the direct rusqlite SQLite provider.

use std::time::Duration;

use neotales_dbapi::{
    DbEvent, DbType, Delay, ErrorKind, Parameter, Pool, PoolPolicy, QueryOptions, RetryPolicy,
    RowLimit, Session, SqlitePolicyPool, SqlitePool, TelemetryOutcome, TxOptions, Value,
    sync::Session as SyncSession,
};
use neotales_dbapi_sqlite_direct::{SqliteConfig, SqlitePoolProvider};

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).expect("valid limit")
}

fn text(value: &str) -> Parameter {
    Parameter::new(DbType::Text, Value::Text(value.to_owned())).expect("valid text parameter")
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).expect("valid integer parameter")
}

async fn provider(path: &std::path::Path) -> SqlitePoolProvider {
    SqlitePoolProvider::connect(
        SqliteConfig::new(path)
            .busy_timeout(Duration::from_millis(150))
            .max_connections(4),
    )
    .await
    .expect("provider")
}

fn inspector(path: &std::path::Path) -> rusqlite::Connection {
    let connection = rusqlite::Connection::open(path).expect("inspector");
    connection
        .busy_timeout(Duration::from_millis(500))
        .expect("busy timeout");
    connection
}

async fn assert_standalone_rejected(session: &mut dyn Session, sql: &str, expected: ErrorKind) {
    let error = session
        .execute_async(sql, QueryOptions::new())
        .await
        .expect_err("standalone statement must be rejected");
    assert_eq!(error.kind(), expected, "{sql:?}");
}

#[tokio::test]
async fn standalone_statement_surfaces_reject_transaction_control_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("guarded.sqlite");
    let provider = provider(&path).await;
    let inspection = inspector(&path);
    inspection
        .execute_batch("CREATE TABLE guarded (value INTEGER)")
        .unwrap();

    let mut session = provider.acquire_async().await.unwrap();
    for sql in [
        "\u{feff}BEGIN",
        "\u{000b}BEGIN",
        "/* comment */ \u{feff}COMMIT",
        "BEGIN",
        " /* leading comment */ cOmMiT",
        "-- comment\nROLLBACK",
        "END",
        "SAVEPOINT guarded_point",
        "RELEASE guarded_point",
        "\"COMMIT\"",
        "`ROLLBACK`",
        "[SAVEPOINT]",
        "'BEGIN'",
    ] {
        assert_standalone_rejected(session.as_mut(), sql, ErrorKind::InvalidArgument).await;
    }
    for prefix in [
        "\u{feff}",
        "\u{000b}",
        "/* comment */\u{feff}",
        "-- comment\n\u{000b}",
        "\u{feff}\u{000b}/* comment */",
        " \t\r\n\u{000c}/* comment */-- comment\n",
    ] {
        for command in [
            "BEGIN",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT point",
            "RELEASE point",
        ] {
            assert_standalone_rejected(
                session.as_mut(),
                &format!("{prefix}{command}"),
                ErrorKind::InvalidArgument,
            )
            .await;
        }
    }
    // The documented prefix scope is deliberately narrower than SQLite's, even for reads.
    assert_standalone_rejected(
        session.as_mut(),
        "\u{feff}SELECT 1",
        ErrorKind::InvalidArgument,
    )
    .await;
    assert_standalone_rejected(
        session.as_mut(),
        "\u{000b}SELECT 1",
        ErrorKind::InvalidArgument,
    )
    .await;

    let count: i64 = inspection
        .query_row("SELECT count(*) FROM guarded", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);

    // The supported transaction API remains usable after rejected standalone commands.
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async("INSERT INTO guarded(value) VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    let count: i64 = inspection
        .query_row("SELECT count(*) FROM guarded", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn placeholders_are_positional_and_mismatches_fail_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("placeholders.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();

    let rows = session
        .fetch_all_async(
            "SELECT ? AS a, ? AS b",
            QueryOptions::new()
                .bind(text("one"))
                .bind(integer(2))
                .limit(limit(1, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("a").unwrap(), "one");
    assert_eq!(rows[0].get::<i64>("b").unwrap(), 2);

    let rows = session
        .fetch_all_async(
            "SELECT ?2 AS second, ?1 AS first",
            QueryOptions::new()
                .bind(text("first"))
                .bind(text("second"))
                .limit(limit(1, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("first").unwrap(), "first");
    assert_eq!(rows[0].get::<String>("second").unwrap(), "second");

    for (sql, parameters, expected) in [
        ("SELECT ?", vec![], ErrorKind::InvalidArgument),
        (
            "SELECT ?",
            vec![integer(1), integer(2)],
            ErrorKind::InvalidArgument,
        ),
        ("SELECT ?1, ?", vec![integer(1)], ErrorKind::InvalidArgument),
        ("SELECT ?0", vec![], ErrorKind::InvalidArgument),
        ("SELECT :name", vec![], ErrorKind::Unsupported),
        ("SELECT @name", vec![], ErrorKind::Unsupported),
        ("SELECT $name", vec![], ErrorKind::Unsupported),
    ] {
        let mut options = QueryOptions::new();
        for parameter in parameters {
            options = options.bind(parameter);
        }
        let error = session
            .fetch_all_async(sql, options)
            .await
            .expect_err("placeholder mismatch must fail");
        assert_eq!(error.kind(), expected, "{sql}");
    }

    // A validation failure leaves the session live.
    let quote = session
        .fetch_all_async(
            "SELECT '?' AS literal, 1 AS one",
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .unwrap();
    assert_eq!(quote[0].get::<String>("literal").unwrap(), "?");
}

#[tokio::test]
async fn typed_values_and_null_affinity_are_exact() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("types.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE typed (i INTEGER, r REAL, t TEXT, b BLOB, flag BOOLEAN)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async(
            "INSERT INTO typed VALUES (?, ?, ?, ?, ?)",
            QueryOptions::new()
                .bind(integer(7))
                .bind(Parameter::new(DbType::F64, Value::F64(1.5)).expect("valid float parameter"))
                .bind(text("hello"))
                .bind(Parameter::new(DbType::Bytes, Value::Bytes(vec![0, 1, 2])).unwrap())
                .bind_null(DbType::Bool),
        )
        .await
        .unwrap();

    let row = session
        .fetch_one_async(
            "SELECT i, r, t, b, flag FROM typed",
            QueryOptions::new().limit(limit(1, 256)),
        )
        .await
        .unwrap();
    assert_eq!(row.read::<i64>("i").unwrap(), 7);
    assert!((row.read::<f64>("r").unwrap() - 1.5).abs() < f64::EPSILON);
    assert_eq!(row.read::<String>("t").unwrap(), "hello");
    assert_eq!(row.read::<Vec<u8>>("b").unwrap(), vec![0, 1, 2]);
    assert_eq!(row.read::<Option<bool>>("flag").unwrap(), None);
    assert!(matches!(
        row.value_at(4).unwrap(),
        Value::Null(DbType::Bool)
    ));

    // An expression without a declared type falls back to a typed Text null.
    let row = session
        .fetch_one_async(
            "SELECT NULL AS untyped",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(row.value_at(0).unwrap(), Value::Null(DbType::Text));

    // SQLite has no boolean storage class: bool binds as integer and reads back as I64.
    let row = session
        .fetch_one_async(
            "SELECT typeof(?) AS kind, ? AS value",
            QueryOptions::new()
                .bind(Parameter::new(DbType::Bool, Value::Bool(true)).unwrap())
                .bind(Parameter::new(DbType::Bool, Value::Bool(true)).unwrap())
                .limit(limit(1, 128)),
        )
        .await
        .unwrap();
    assert_eq!(row.read::<String>("kind").unwrap(), "integer");
    assert_eq!(row.read::<i64>("value").unwrap(), 1);
}

#[tokio::test]
async fn row_and_byte_limits_fail_without_returning_partial_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("limits.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async("CREATE TABLE items (name TEXT)", QueryOptions::new())
        .await
        .unwrap();
    session
        .execute_trusted_script_async(
            "INSERT INTO items VALUES ('a'), ('b'), ('c')",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let error = session
        .fetch_all_async(
            "SELECT name FROM items ORDER BY name",
            QueryOptions::new().limit(limit(2, 4096)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow);
    // The worker was discarded, so the next use reports the poisoned session.
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );

    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT name FROM items ORDER BY name",
            QueryOptions::new().limit(limit(10, 5)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow);
    drop(session);

    // A larger limit returns every row.
    let rows = provider
        .fetch_all_async(
            "SELECT name FROM items ORDER BY name",
            QueryOptions::new().limit(limit(10, 4096)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn oversized_text_and_blob_are_rejected_before_materialization() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("oversized.sqlite")).await;
    let mut session = provider.acquire_async().await.unwrap();

    for sql in [
        "SELECT printf('%01000000d', 1) AS payload",
        "SELECT zeroblob(1000000) AS payload",
    ] {
        let error = session
            .fetch_all_async(sql, QueryOptions::new().limit(limit(1, 64)))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Overflow, "{sql}");
        drop(session);
        session = provider.acquire_async().await.unwrap();
    }
}

#[tokio::test]
async fn async_session_fetch_accounts_each_rows_names_and_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("row-byte-accounting.sqlite")).await;

    let mut session = provider.acquire_async().await.unwrap();
    let Err(error) = session
        .fetch_async(
            "SELECT 'a' AS name UNION ALL SELECT 'b' AS name",
            QueryOptions::new().limit(limit(2, 6)),
        )
        .await
    else {
        panic!("direct Session::fetch_async must reject a six-byte aggregate limit");
    };
    assert_eq!(error.kind(), ErrorKind::Overflow);
    drop(session);

    let mut session = provider.acquire_async().await.unwrap();
    let mut reader = session
        .fetch_async(
            "SELECT 'a' AS name UNION ALL SELECT 'b' AS name",
            QueryOptions::new().limit(limit(2, 10)),
        )
        .await
        .unwrap();
    let rows = reader.read_all_async().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        10
    );

    drop(reader);
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    let mut reader = session
        .fetch_async(
            "SELECT 'x' AS a_very_long_column_name WHERE 0",
            QueryOptions::new().limit(limit(1, 1)),
        )
        .await
        .unwrap();
    assert!(
        reader.read_all_async().await.unwrap().is_empty(),
        "an empty result has no row-byte cost"
    );
    drop(reader);
    drop(session);

    // Duplicate names are charged per cell per row, while NULL contributes no payload bytes.
    let mut session = provider.acquire_async().await.unwrap();
    let Err(error) = session
        .fetch_async(
            "SELECT NULL AS n, 'x' AS n UNION ALL SELECT NULL, 'y'",
            QueryOptions::new().limit(limit(2, 5)),
        )
        .await
    else {
        panic!("duplicate column names must be charged for both rows");
    };
    assert_eq!(error.kind(), ErrorKind::Overflow);
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    let mut reader = session
        .fetch_async(
            "SELECT NULL AS n, 'x' AS n UNION ALL SELECT NULL, 'y'",
            QueryOptions::new().limit(limit(2, 6)),
        )
        .await
        .unwrap();
    let rows = reader.read_all_async().await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        6
    );
}

#[tokio::test]
async fn sqlite_policy_pool_preserves_immediate_transactions_and_sanitized_events() {
    use std::sync::{Arc, Mutex};

    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("sqlite-policy.sqlite")).await;
    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let observed = Arc::clone(&events);
    let policy = PoolPolicy {
        telemetry_enabled: true,
        retry: RetryPolicy::new(3, Delay::Fixed(Duration::ZERO), false).unwrap(),
        observer: Some(Arc::new(move |event| {
            observed.lock().unwrap().push(event.clone());
        })),
        ..PoolPolicy::default()
    };
    let pool = Arc::new(SqlitePolicyPool::new(provider.clone(), policy));

    let mut session = pool.acquire_sqlite_async().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async(
            "CREATE TABLE policy_events (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();

    let mut session = pool.acquire_sqlite_async().await.unwrap();
    session
        .execute_async(
            "INSERT INTO policy_events VALUES (?)",
            QueryOptions::new().bind(Parameter::new(DbType::I64, Value::I64(7)).unwrap()),
        )
        .await
        .unwrap();
    let error = session
        .execute_async("INSERT INTO missing_table VALUES (1)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    let event_snapshot = events.lock().unwrap().clone();
    assert!(event_snapshot.len() >= 3);
    assert!(event_snapshot.iter().all(|event| event.sql.is_none()));
    assert!(
        event_snapshot
            .iter()
            .any(|event| event.outcome == TelemetryOutcome::Success)
    );
    assert!(
        event_snapshot.iter().any(|event| {
            event.outcome == TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
        })
    );
    assert_eq!(
        event_snapshot
            .iter()
            .filter(|event| {
                event.outcome == TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
            })
            .count(),
        1,
        "a dispatched unknown write outcome must not be retried"
    );
    let mut session = pool.acquire_sqlite_async().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    let transaction_error = transaction
        .execute_async("INSERT INTO missing_table VALUES (2)", QueryOptions::new())
        .await
        .unwrap_err();
    assert_eq!(transaction_error.kind(), ErrorKind::WriteOutcomeUnknown);
    let _ = transaction.rollback_async().await;
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                event.outcome == TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
            })
            .count(),
        2,
        "a transaction statement with enabled pool retries must execute only once"
    );
    assert_cancellation_event(Arc::clone(&pool), Arc::clone(&events)).await;
    assert_sql_capture(provider).await;
}

#[tokio::test]
async fn sqlite_policy_defaults_leave_telemetry_and_capture_off() {
    use std::sync::{Arc, Mutex};

    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("sqlite-policy-defaults.sqlite")).await;
    let events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let observed = Arc::clone(&events);
    let pool = SqlitePolicyPool::new(
        provider,
        PoolPolicy {
            observer: Some(Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let rows = pool
        .fetch_all_async("SELECT 1 AS value", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(events.lock().unwrap().is_empty());
}

async fn assert_cancellation_event(
    pool: std::sync::Arc<SqlitePolicyPool>,
    events: std::sync::Arc<std::sync::Mutex<Vec<DbEvent>>>,
) {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let pending = tokio::spawn(async move {
        let mut session = pool.acquire_sqlite_async().await.unwrap();
        let _ = started_tx.send(());
        let _ = session
            .fetch_async(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n",
                QueryOptions::new(),
            )
            .await;
    });
    started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    pending.abort();
    let _ = pending.await;
    let after_cancel = events.lock().unwrap().clone();
    assert_eq!(
        after_cancel
            .iter()
            .filter(|event| {
                event.outcome == TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
            })
            .count(),
        3,
        "dropping an in-flight query records an unknown outcome after dispatch"
    );
}

async fn assert_sql_capture(provider: SqlitePoolProvider) {
    use std::sync::{Arc, Mutex};

    let captured_events = Arc::new(Mutex::new(Vec::<DbEvent>::new()));
    let observed = Arc::clone(&captured_events);
    let capture_pool = SqlitePolicyPool::new(
        provider,
        PoolPolicy {
            telemetry_enabled: true,
            sql_capture_enabled: true,
            observer: Some(Arc::new(move |event| {
                observed.lock().unwrap().push(event.clone());
            })),
            ..PoolPolicy::default()
        },
    );
    let mut session = capture_pool.acquire_sqlite_async().await.unwrap();
    let _ = session
        .execute_async("INSERT INTO absent VALUES (1)", QueryOptions::new())
        .await;
    let captured = captured_events.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].sql.as_deref(),
        Some("INSERT INTO absent VALUES (1)")
    );
    assert_eq!(
        captured[0].outcome,
        TelemetryOutcome::Error(ErrorKind::WriteOutcomeUnknown)
    );
}

#[test]
fn sync_session_fetch_accounts_each_rows_names_and_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let provider = SqlitePoolProvider::connect_sync(SqliteConfig::new(
        directory.path().join("row-bytes-sync.sqlite"),
    ))
    .unwrap();

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let Err(error) = SyncSession::fetch(
        &mut session,
        "SELECT 'a' AS name UNION ALL SELECT 'b' AS name",
        QueryOptions::new().limit(limit(2, 6)),
    ) else {
        panic!("direct sync Session::fetch must reject a six-byte aggregate limit");
    };
    assert_eq!(error.kind(), ErrorKind::Overflow);
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let mut reader = SyncSession::fetch(
        &mut session,
        "SELECT 'a' AS name UNION ALL SELECT 'b' AS name",
        QueryOptions::new().limit(limit(2, 10)),
    )
    .unwrap();
    let rows = reader.read_all().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        10
    );
    drop(reader);
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let mut reader = SyncSession::fetch(
        &mut session,
        "SELECT 'x' AS a_very_long_column_name WHERE 0",
        QueryOptions::new().limit(limit(1, 1)),
    )
    .unwrap();
    assert!(
        reader.read_all().unwrap().is_empty(),
        "an empty result has no row-byte cost"
    );
    drop(reader);
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let Err(error) = SyncSession::fetch(
        &mut session,
        "SELECT NULL AS n, 'x' AS n UNION ALL SELECT NULL, 'y'",
        QueryOptions::new().limit(limit(2, 5)),
    ) else {
        panic!("duplicate column names must be charged for both rows");
    };
    assert_eq!(error.kind(), ErrorKind::Overflow);
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let mut reader = SyncSession::fetch(
        &mut session,
        "SELECT NULL AS n, 'x' AS n UNION ALL SELECT NULL, 'y'",
        QueryOptions::new().limit(limit(2, 6)),
    )
    .unwrap();
    let rows = reader.read_all().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.encoded_bytes().unwrap())
            .sum::<usize>(),
        6
    );
}

#[tokio::test]
async fn constraint_errors_are_sanitized_and_discard_the_worker() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("constraints.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE unique_rows (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    session
        .execute_async(
            "INSERT INTO unique_rows (name) VALUES (?)",
            QueryOptions::new().bind(text("same")),
        )
        .await
        .unwrap();

    let error = session
        .execute_async(
            "INSERT INTO unique_rows (name) VALUES (?)",
            QueryOptions::new().bind(text("same")),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    assert!(format!("{error:?}").find("same").is_none());
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );

    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .execute_async(
            "INSERT INTO unique_rows (id, name) VALUES (2, NULL)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);

    let count: i64 = inspector(&path)
        .query_row("SELECT count(*) FROM unique_rows", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn execute_with_a_result_set_reports_an_unknown_write_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("returning.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_async(
            "CREATE TABLE returning_rows (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    // `execute` is not the fetch API. SQLite steps the statement before rusqlite reports that it
    // returned a row, so the provider cannot claim that no write happened.
    let error = session
        .execute_async(
            "INSERT INTO returning_rows VALUES (1) RETURNING value",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    assert_eq!(
        session
            .execute_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);
    let rows = provider
        .fetch_all_async(
            "SELECT value FROM returning_rows",
            QueryOptions::new().limit(limit(10, 256)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn foreign_keys_and_busy_are_observable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("keys.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    let pragma = session
        .fetch_scalar_async(
            "PRAGMA foreign_keys",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(pragma, Value::I64(1));

    session
        .execute_async(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parent(id))",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    session
        .execute_trusted_script_async(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY);
             CREATE TABLE child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parent(id));",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let error = session
        .execute_async(
            "INSERT INTO child (id, parent) VALUES (1, 99)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);

    // Hold the write lock on one connection, then ask another to begin immediately.
    let mut first = provider.acquire_sqlite().await.unwrap();
    let transaction = first.begin_immediate_async().await.unwrap();
    let mut second = provider.acquire_sqlite().await.unwrap();
    let Err(error) = second.begin_immediate_async().await else {
        panic!("a second immediate writer must fail while the lock is held");
    };
    assert_eq!(error.kind(), ErrorKind::Busy);
    drop(transaction);
    drop(first);
    drop(second);
}

#[tokio::test]
async fn transactions_commit_rollback_and_immediate_begin() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("transactions.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_sqlite().await.unwrap();
    session
        .execute_async("CREATE TABLE rows (value INTEGER)", QueryOptions::new())
        .await
        .unwrap();

    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async("INSERT INTO rows VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();

    let transaction = session.begin_immediate_async().await.unwrap();
    transaction.rollback_async().await.unwrap();

    session
        .execute_async("INSERT INTO rows VALUES (2)", QueryOptions::new())
        .await
        .unwrap();
    let values = session
        .fetch_all_async(
            "SELECT value FROM rows ORDER BY value",
            QueryOptions::new().limit(limit(10, 256)),
        )
        .await
        .unwrap();
    assert_eq!(values.len(), 2);

    // Dropping an open transaction rolls back rather than leaving it reusable.
    let transaction = session.begin_async(TxOptions::default()).await.unwrap();
    let mut transaction = transaction;
    transaction
        .execute_async("INSERT INTO rows VALUES (3)", QueryOptions::new())
        .await
        .unwrap();
    drop(transaction);
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Connection
    );
    drop(session);

    let count: i64 = inspector(&path)
        .query_row("SELECT count(*) FROM rows", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn trusted_scripts_execute_multiple_statements() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("scripts.sqlite");
    let provider = provider(&path).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE a (x INTEGER);
             CREATE TABLE b (y INTEGER);
             INSERT INTO a VALUES (1);
             INSERT INTO b VALUES (2);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let row = session
        .fetch_one_async(
            "SELECT (SELECT count(*) FROM a) AS a, (SELECT count(*) FROM b) AS b",
            QueryOptions::new().limit(limit(1, 128)),
        )
        .await
        .unwrap();
    assert_eq!(row.read::<i64>("a").unwrap(), 1);
    assert_eq!(row.read::<i64>("b").unwrap(), 1);

    // Ordinary statement calls reject a multi-statement script before dispatch.
    let error = session
        .execute_async(
            "CREATE TABLE c (z INTEGER); CREATE TABLE d (w INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn script_boundaries_preserve_sqlite_trigger_parsing_nulls_and_transaction_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider(&directory.path().join("script-boundaries.sqlite")).await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE input(value TEXT);
         CREATE TABLE output(value TEXT);
         CREATE TRIGGER copy AFTER INSERT ON input BEGIN
           INSERT INTO output VALUES ('semicolon; inside literal');
           INSERT INTO output VALUES (NEW.value);
         END;
         PRAGMA user_version = 3;
         SELECT 1;
         INSERT INTO input VALUES (?);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let values = session
        .fetch_all_async(
            "SELECT value FROM output ORDER BY rowid",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(
        values[0].read::<String>("value").unwrap(),
        "semicolon; inside literal"
    );
    assert_eq!(values[1].value_at(0).unwrap(), Value::Null(DbType::Text));
    let mut tx = session.begin_async(TxOptions::default()).await.unwrap();
    tx.execute_trusted_script_async(
        "INSERT INTO input VALUES ('rolled back'); SELECT 2;",
        QueryOptions::new(),
    )
    .await
    .unwrap();
    tx.rollback_async().await.unwrap();
    let count = session
        .fetch_scalar_async("SELECT count(*) FROM input", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(count, Value::I64(1));
}

#[tokio::test]
async fn trusted_script_partial_failure_is_unknown_after_prior_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("partial-script-async.sqlite");
    let provider = provider(&path).await;
    provider
        .execute_async(
            "CREATE TABLE script_writes (value INTEGER)",
            QueryOptions::new(),
        )
        .await
        .unwrap();

    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .execute_trusted_script_async(
            "INSERT INTO script_writes VALUES (1); INSERT INTO missing_script_table VALUES (2);",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    drop(session);

    let rows = provider
        .fetch_all_async(
            "SELECT value FROM script_writes",
            QueryOptions::new().limit(limit(4, 128)),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].read::<i64>("value").unwrap(), 1);
}

#[test]
fn trusted_script_partial_failure_is_unknown_on_sync_surface() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("partial-script-sync.sqlite");
    let provider = SqlitePoolProvider::connect_sync(SqliteConfig::new(&path)).unwrap();
    let mut session = provider.acquire_sync_sqlite().unwrap();
    SyncSession::execute(
        &mut session,
        "CREATE TABLE script_writes (value INTEGER)",
        QueryOptions::new(),
    )
    .unwrap();

    let error = SyncSession::execute_trusted_script(
        &mut session,
        "INSERT INTO script_writes VALUES (1); INSERT INTO missing_script_table VALUES (2);",
        QueryOptions::new(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::WriteOutcomeUnknown);
    drop(session);

    let mut session = provider.acquire_sync_sqlite().unwrap();
    let rows = SyncSession::fetch_all(
        &mut session,
        "SELECT value FROM script_writes",
        QueryOptions::new().limit(limit(4, 128)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].read::<i64>("value").unwrap(), 1);
}

#[test]
fn in_memory_databases_require_one_connection() {
    let mut session =
        SqlitePoolProvider::connect_sync(SqliteConfig::new(":memory:").max_connections(1))
            .expect("in-memory provider")
            .acquire_sync_sqlite()
            .expect("session");
    SyncSession::execute(
        &mut session,
        "CREATE TABLE memory_only (value INTEGER)",
        QueryOptions::new(),
    )
    .unwrap();
    SyncSession::execute(
        &mut session,
        "INSERT INTO memory_only VALUES (9)",
        QueryOptions::new(),
    )
    .unwrap();
    let value = SyncSession::fetch_scalar(
        &mut session,
        "SELECT value FROM memory_only",
        QueryOptions::new().limit(limit(1, 64)),
    )
    .unwrap();
    assert_eq!(value, Value::I64(9));

    let error = SqlitePoolProvider::connect_sync(SqliteConfig::new(":memory:"))
        .expect_err("multi-connection in-memory must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn synchronous_trait_family_matches_the_async_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sync.sqlite");
    let provider = SqlitePoolProvider::connect_sync(SqliteConfig::new(&path).max_connections(2))
        .expect("provider");
    let mut session = provider.acquire_sync_sqlite().expect("session");
    SyncSession::execute(
        &mut session,
        "CREATE TABLE sync_rows (id INTEGER PRIMARY KEY, name TEXT)",
        QueryOptions::new(),
    )
    .unwrap();
    assert_eq!(
        SyncSession::execute(
            &mut session,
            "INSERT INTO sync_rows (name) VALUES (?)",
            QueryOptions::new().bind(text("sync")),
        )
        .unwrap(),
        1
    );
    let row = SyncSession::fetch_one(
        &mut session,
        "SELECT id, name FROM sync_rows",
        QueryOptions::new().limit(limit(1, 128)),
    )
    .unwrap();
    assert_eq!(row.read::<i64>("id").unwrap(), 1);
    assert_eq!(row.read::<String>("name").unwrap(), "sync");

    let mut transaction = session.begin_immediate().unwrap();
    transaction
        .execute(
            "INSERT INTO sync_rows (name) VALUES (?)",
            QueryOptions::new().bind(text("immediate")),
        )
        .unwrap();
    transaction.commit().unwrap();
    let count = SyncSession::fetch_scalar(
        &mut session,
        "SELECT count(*) FROM sync_rows",
        QueryOptions::new().limit(limit(1, 64)),
    )
    .unwrap();
    assert_eq!(count, Value::I64(2));

    let error = SyncSession::execute(
        &mut session,
        "SELECT 1",
        QueryOptions::new().timeout(Duration::from_millis(5)),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    drop(session);
    provider.close();
}
