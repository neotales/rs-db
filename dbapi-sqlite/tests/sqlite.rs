//! Real-file integration tests for the SQLite provider contract.

use std::time::Duration;

use neotales_dbapi::{
    DbType, ErrorKind, Parameter, Pool, QueryOptions, RowLimit, Session, TxOptions, Value,
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tempfile::TempDir;
use tracing::{
    Event as TraceEvent, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    layer::{Context, Layer},
    prelude::*,
};

async fn database() -> (TempDir, SqlitePool, SqlitePoolProvider) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("provider.sqlite");
    let provider = SqlitePoolProvider::connect(
        SqliteConfig::new(path.clone())
            .busy_timeout(Duration::from_millis(100))
            .max_connections(3),
    )
    .await
    .unwrap();
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .busy_timeout(Duration::from_millis(100));
    let pool = SqlitePoolOptions::new()
        .max_connections(3)
        .connect_with(options)
        .await
        .unwrap();
    (directory, pool, provider)
}

#[tokio::test]
async fn standalone_statement_surfaces_reject_transaction_control_before_dispatch() {
    let (_directory, inspection, provider) = database().await;
    sqlx::query("CREATE TABLE guarded (value INTEGER)")
        .execute(&inspection)
        .await
        .unwrap();

    let mut session = provider.acquire_async().await.unwrap();
    let statements = [
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
    ];
    for sql in statements {
        assert_standalone_rejected(&provider, session.as_mut(), sql).await;
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
            assert_standalone_rejected(&provider, session.as_mut(), &format!("{prefix}{command}"))
                .await;
        }
    }
    // The documented prefix scope is deliberately narrower than SQLite's, even for reads.
    assert_standalone_rejected(&provider, session.as_mut(), "\u{feff}SELECT 1").await;
    assert_standalone_rejected(&provider, session.as_mut(), "\u{000b}SELECT 1").await;

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM guarded")
        .fetch_one(&inspection)
        .await
        .unwrap();
    assert_eq!(count, 0);

    // The supported transaction API remains usable after rejected standalone commands.
    let mut transaction = session.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_async("INSERT INTO guarded(value) VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    transaction.commit_async().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM guarded")
        .fetch_one(&inspection)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

async fn assert_standalone_rejected(
    provider: &SqlitePoolProvider,
    session: &mut dyn Session,
    sql: &str,
) {
    assert_eq!(
        provider
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument,
        "{sql}"
    );
    assert_eq!(
        provider
            .fetch_all_async(sql, QueryOptions::new().limit(limit(1, 128)))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument,
        "{sql}"
    );
    assert_eq!(
        session
            .execute_async(sql, QueryOptions::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument,
        "{sql}"
    );
    assert_eq!(
        session
            .fetch_all_async(sql, QueryOptions::new().limit(limit(1, 128)))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument,
        "{sql}"
    );
}

fn parameter(kind: DbType, value: Value) -> Parameter {
    Parameter::new(kind, value).unwrap()
}

fn limit(rows: usize, bytes: usize) -> RowLimit {
    RowLimit::new(rows, bytes).unwrap()
}

#[derive(Clone)]
struct TraceCapture(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

struct TraceFields(String);

impl Visit for TraceFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value:?};", field.name());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value};", field.name());
    }
}

impl<S: Subscriber> Layer<S> for TraceCapture {
    fn on_event(&self, event: &TraceEvent<'_>, _context: Context<'_, S>) {
        let mut fields = TraceFields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

#[test]
fn sqlx_statement_and_slow_statement_tracing_are_disabled() {
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber =
        tracing_subscriber::registry().with(TraceCapture(std::sync::Arc::clone(&events)));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    runtime.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace.sqlite");
        let trace_options = SqliteConnectOptions::new()
            .filename(path.clone())
            .create_if_missing(true)
            .log_statements(log::LevelFilter::Trace)
            .log_slow_statements(log::LevelFilter::Trace, Duration::ZERO);
        let trace_pool = SqlitePoolOptions::new()
            .connect_with(trace_options)
            .await
            .unwrap();
        sqlx::query("SELECT 'TRACE_CONTROL_SENTINEL'")
            .fetch_one(&trace_pool)
            .await
            .unwrap();
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.contains("TRACE_CONTROL_SENTINEL")),
            "{:?}",
            events.lock().unwrap()
        );
        events.lock().unwrap().clear();
        drop(trace_pool);

        let provider = SqlitePoolProvider::connect(SqliteConfig::new(path))
            .await
            .unwrap();
        let mut session = provider.acquire_sqlite().await.unwrap();
        session
            .fetch_all_async(
                "SELECT 'PROVIDER_TRACE_SENTINEL'",
                QueryOptions::new().limit(limit(1, 128)),
            )
            .await
            .unwrap();
    });
    let captured = events.lock().unwrap().join("\n");
    assert!(!captured.contains("PROVIDER_TRACE_SENTINEL"), "{captured}");
}

#[tokio::test]
async fn typed_values_nulls_and_detached_duplicate_columns() {
    let (_directory, _pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let params = [
        parameter(DbType::Bool, Value::Bool(true)),
        parameter(DbType::I64, Value::I64(i64::MAX)),
        parameter(DbType::F64, Value::F64(1.25)),
        parameter(DbType::Text, Value::Text("utf8-π".into())),
        parameter(DbType::Bytes, Value::Bytes(vec![0, 1, 255])),
        parameter(DbType::Text, Value::Null(DbType::Text)),
    ];
    let rows = session.fetch_all_async(
        "SELECT ? AS flag, ? AS big, ? AS real, ? AS text, ? AS blob, ? AS nullable, 9 AS n, 10 AS n",
        QueryOptions::new().bind_all(&params).limit(limit(1, 100)),
    ).await.unwrap();
    let row = rows.into_iter().next().unwrap();
    assert_eq!(row.get_ordinal::<i64>(0).unwrap(), 1); // SQLite booleans use INTEGER.
    assert_eq!(row.get::<i64>("big").unwrap(), i64::MAX);
    assert_eq!(
        row.get_ordinal::<i32>(1).unwrap_err().kind(),
        ErrorKind::Overflow
    );
    assert!((row.get::<f64>("real").unwrap() - 1.25).abs() < f64::EPSILON);
    assert_eq!(row.get::<String>("text").unwrap(), "utf8-π");
    assert_eq!(row.get::<Vec<u8>>("blob").unwrap(), [0, 1, 255]);
    assert_eq!(row.get::<Option<String>>("nullable").unwrap(), None);
    assert_eq!(
        row.get::<Option<i64>>("nullable").unwrap_err().kind(),
        ErrorKind::WrongType
    );
    assert_eq!(
        row.get::<i64>("n").unwrap_err().kind(),
        ErrorKind::Ambiguous
    );
    assert_eq!(
        row.get_ordinal::<i64>(8).unwrap_err().kind(),
        ErrorKind::Missing
    );
    session
        .execute_trusted_script_async(
            "CREATE TABLE typed_null (value INTEGER); INSERT INTO typed_null VALUES (NULL);",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let null_row = session
        .fetch_all_async(
            "SELECT value FROM typed_null",
            QueryOptions::new().limit(limit(1, 32)),
        )
        .await
        .unwrap();
    assert_eq!(null_row[0].get::<Option<i64>>("value").unwrap(), None);
}

#[tokio::test]
async fn trusted_scripts_and_detached_rows_survive_session_release() {
    let (_directory, _pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(
            "CREATE TABLE script_rows (value TEXT); INSERT INTO script_rows VALUES ('saved');",
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let rows = session
        .fetch_all_async(
            "SELECT value FROM script_rows",
            QueryOptions::new().limit(limit(1, 32)),
        )
        .await
        .unwrap();
    drop(session);
    assert_eq!(rows[0].get::<String>("value").unwrap(), "saved");
}

#[tokio::test]
async fn unsupported_transaction_options_do_not_begin() {
    let (_directory, _pool, provider) = database().await;
    assert!(provider.capabilities().transactions);
    assert!(!provider.capabilities().isolation_level);
    let mut session = provider.acquire_async().await.unwrap();
    let options = neotales_dbapi::TxOptions {
        isolation: None,
        read_only: Some(true),
    };
    assert_eq!(
        session.begin_async(options).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    let transaction = session
        .begin_async(neotales_dbapi::TxOptions {
            isolation: None,
            read_only: Some(false),
        })
        .await
        .unwrap();
    transaction.rollback_async().await.unwrap();
    let rows = session
        .fetch_all_async("SELECT 1 AS value", QueryOptions::new().limit(limit(1, 32)))
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 1);
}

#[tokio::test]
async fn indexed_placeholder_arity_and_bad_bind_rejected() {
    let (_directory, _pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let params = [
        parameter(DbType::I64, Value::I64(11)),
        parameter(DbType::I64, Value::I64(22)),
    ];
    let rows = session
        .fetch_all_async(
            "SELECT ?2 AS value",
            QueryOptions::new().bind_all(&params).limit(limit(1, 32)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 22);
    let error = session
        .fetch_all_async(
            "SELECT ?2 AS value",
            QueryOptions::new()
                .bind_all(&params[..1])
                .limit(limit(1, 32)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = session
        .fetch_all_async("SELECT ? AS value", QueryOptions::new().limit(limit(1, 32)))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let three_params = [
        params[0].clone(),
        params[1].clone(),
        parameter(DbType::I64, Value::I64(33)),
    ];
    for mixed in ["SELECT ?1, ?", "SELECT ?2, ?"] {
        let bind_slice = if mixed.contains("?2") {
            &three_params[..]
        } else {
            &params[..]
        };
        assert_eq!(
            session
                .fetch_all_async(
                    mixed,
                    QueryOptions::new().bind_all(bind_slice).limit(limit(1, 32))
                )
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidArgument,
        );
    }
    let repeated = session
        .fetch_all_async(
            "SELECT ?1 AS first, ?1 AS second",
            QueryOptions::new()
                .bind_all(&params[..1])
                .limit(limit(1, 32)),
        )
        .await
        .unwrap();
    assert_eq!(repeated[0].get::<i64>("first").unwrap(), 11);
    assert_eq!(repeated[0].get::<i64>("second").unwrap(), 11);
    let quoted = session
        .fetch_all_async(
            "SELECT '?1, ?' AS literal /* ?2, ? */",
            QueryOptions::new().limit(limit(1, 64)),
        )
        .await
        .unwrap();
    assert_eq!(quoted[0].get::<String>("literal").unwrap(), "?1, ?");
}

#[tokio::test]
async fn statements_reject_extra_executable_sql_before_dispatch() {
    let (_directory, pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let error = session.execute_async(
            "CREATE TABLE should_not_exist (value INTEGER); INSERT INTO should_not_exist VALUES (1)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let exists: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='should_not_exist'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(exists, 0);
    let rows = session
        .fetch_all_async(
            "SELECT ';' AS value; -- trailing comment",
            QueryOptions::new().limit(limit(1, 32)),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>("value").unwrap(), ";");
    let error = session
        .fetch_all_async(
            "SELECT 1; SELECT 2",
            QueryOptions::new().limit(limit(2, 32)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn invalid_utf8_is_reported_as_decode_error() {
    let (_directory, _pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT CAST(x'80' AS TEXT) AS invalid_text",
            QueryOptions::new().limit(limit(1, 32)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[tokio::test]
async fn hard_fetch_bounds_fail_without_returning_partial_rows() {
    let (_directory, _pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT '12345' AS payload",
            QueryOptions::new().limit(limit(1, 5)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow); // Includes column-name bytes.
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT '12345' AS payload",
            QueryOptions::new().limit(limit(1, 11)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow); // 7 name bytes + 5 text bytes.
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    assert_eq!(
        session
            .fetch_all_async(
                "SELECT '12345' AS payload",
                QueryOptions::new().limit(limit(1, 12)),
            )
            .await
            .unwrap()
            .len(),
        1
    );
    drop(session);
    let mut session = provider.acquire_async().await.unwrap();
    let error = session
        .fetch_all_async(
            "SELECT 1 UNION ALL SELECT 2",
            QueryOptions::new().limit(limit(1, 100)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[tokio::test]
async fn immediate_transaction_rolls_back_on_explicit_rollback_and_drop() {
    let (_directory, pool, provider) = database().await;
    sqlx::query("CREATE TABLE entries (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut tx = session.begin_immediate_async().await.unwrap();
    tx.execute_async("INSERT INTO entries VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    tx.rollback_async().await.unwrap();
    drop(session);

    let mut session = provider.acquire_sqlite().await.unwrap();
    {
        let mut tx = session.begin_immediate_async().await.unwrap();
        tx.execute_async("INSERT INTO entries VALUES (2)", QueryOptions::new())
            .await
            .unwrap();
    }
    drop(session);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn implicit_sqlite_rollback_discards_transaction_connection() {
    let (_directory, pool, provider) = database().await;
    sqlx::query("CREATE TABLE entries (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut tx = session.begin_immediate_async().await.unwrap();
    tx.execute_async("INSERT INTO entries VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    let error = tx
        .execute_async(
            "INSERT OR ROLLBACK INTO entries VALUES (1)",
            QueryOptions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Constraint);
    drop(tx);
    drop(session);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn cancellation_discards_session_and_pool_remains_usable() {
    let (_directory, pool, provider) = database().await;
    let mut session = provider.acquire_async().await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_millis(10), session.fetch_all_async(
        "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM counter WHERE x < 100000000) SELECT sum(x) AS total FROM counter",
        QueryOptions::new().limit(limit(1, 100)),
    )).await;
    assert!(
        outcome.is_err(),
        "long-running SQLite query should be cancelled by timeout"
    );
    assert_eq!(
        session
            .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit(1, 10)))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled,
    );
    drop(session);
    let value: i64 = sqlx::query_scalar("SELECT 42")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(value, 42);
}

#[tokio::test]
async fn immediate_begin_observes_writer_contention() {
    let (_directory, _pool, provider) = database().await;
    let mut first = provider.acquire_sqlite().await.unwrap();
    let _transaction = first.begin_immediate_async().await.unwrap();
    let mut second = provider.acquire_sqlite().await.unwrap();
    let result = second.begin_immediate_async().await;
    assert!(matches!(result, Err(error) if error.kind() == ErrorKind::Busy));
}

#[tokio::test]
async fn dispatched_commit_failure_reports_unknown_outcome() {
    let (_directory, pool, provider) = database().await;
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
        .execute(&pool).await.unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO child VALUES (7)", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(
        transaction.commit_async().await.unwrap_err().kind(),
        ErrorKind::CommitOutcomeUnknown
    );
}

#[tokio::test]
async fn failed_transaction_statements_cannot_be_committed() {
    let (_directory, pool, provider) = database().await;
    sqlx::query("CREATE TABLE unique_rows (value INTEGER UNIQUE)")
        .execute(&pool)
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO unique_rows VALUES (1)", QueryOptions::new())
        .await
        .unwrap();
    assert_eq!(
        transaction
            .execute_async("INSERT INTO unique_rows VALUES (1)", QueryOptions::new(),)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Constraint,
    );
    assert_eq!(
        transaction.commit_async().await.unwrap_err().kind(),
        ErrorKind::Connection,
    );
    drop(session);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM unique_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn cancelled_transaction_cannot_commit_and_writer_lock_is_released() {
    let (_directory, pool, provider) = database().await;
    sqlx::query("CREATE TABLE pending_rows (value INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    let mut session = provider.acquire_sqlite().await.unwrap();
    let mut transaction = session.begin_immediate_async().await.unwrap();
    transaction
        .execute_async("INSERT INTO pending_rows VALUES (9)", QueryOptions::new())
        .await
        .unwrap();
    let long_fetch = transaction.fetch_all_async(
        "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM counter WHERE x < 100000000) SELECT sum(x) FROM counter",
        QueryOptions::new().limit(limit(1, 64)),
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), long_fetch)
            .await
            .is_err()
    );
    assert_eq!(
        transaction.commit_async().await.unwrap_err().kind(),
        ErrorKind::Cancelled,
    );
    drop(session);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pending_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    let mut independent = provider.acquire_sqlite().await.unwrap();
    independent
        .begin_immediate_async()
        .await
        .unwrap()
        .rollback_async()
        .await
        .unwrap();
}

#[tokio::test]
async fn progress_handler_survives_commit_and_rollback_for_session_reuse() {
    let (_directory, _pool, provider) = database().await;
    for commit in [true, false] {
        let mut session = provider.acquire_sqlite().await.unwrap();
        let transaction = session.begin_immediate_async().await.unwrap();
        if commit {
            transaction.commit_async().await.unwrap();
        } else {
            transaction.rollback_async().await.unwrap();
        }
        let long_fetch = session.fetch_all_async(
            "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM counter WHERE x < 100000000) SELECT sum(x) FROM counter",
            QueryOptions::new().limit(limit(1, 64)),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), long_fetch)
                .await
                .is_err()
        );
        assert_eq!(
            session
                .fetch_all_async("SELECT 1", QueryOptions::new().limit(limit(1, 16)))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Cancelled,
        );
        drop(session);
        let mut independent = provider.acquire_sqlite().await.unwrap();
        independent
            .begin_immediate_async()
            .await
            .unwrap()
            .rollback_async()
            .await
            .unwrap();
    }
}
