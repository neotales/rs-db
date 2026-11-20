//! Connection-affine quiet transactions against disposable PostgreSQL.
use std::time::Duration;

use neotales_dbapi::{DbType, ErrorKind, IsolationLevel, Parameter, RowLimit, TxOptions, Value};
use neotales_dbapi_postgres::experimental::{Config, Lease, Pool, Profile};

#[path = "support/transaction_proxy.rs"]
mod proxy;

fn pool() -> Pool {
    make_pool(55433, Duration::from_secs(3))
}

fn make_pool(port: u16, ceiling: Duration) -> Pool {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("postgres");
    Pool::loopback_plaintext_for_tests(
        config,
        1,
        Profile::normal(ceiling, Duration::from_millis(100)).unwrap(),
    )
    .unwrap()
}

fn integer(value: i64) -> Parameter {
    Parameter::new(DbType::I64, Value::I64(value)).unwrap()
}

fn limit() -> RowLimit {
    RowLimit::new(10, 1000).unwrap()
}

async fn pid(lease: &mut Lease) -> i64 {
    lease
        .fetch("SELECT pg_backend_pid()::bigint AS pid", &[], limit())
        .await
        .unwrap()[0]
        .get::<i64>("pid")
        .unwrap()
}

async fn count(lease: &mut Lease, table: &str) -> i64 {
    lease
        .fetch(&format!("SELECT count(*) AS n FROM {table}"), &[], limit())
        .await
        .unwrap()[0]
        .get::<i64>("n")
        .unwrap()
}

async fn capacity(pool: &Pool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.free_slots() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("socket-owning transaction worker did not release its permit");
}

#[tokio::test]
async fn begin_honors_options_and_xact_locks_are_connection_affine() {
    let pool = pool();
    let mut observer = make_pool(55433, Duration::from_secs(3))
        .acquire()
        .await
        .unwrap();
    for (isolation, expected) in [
        (None, "read committed"),
        (Some(IsolationLevel::ReadCommitted), "read committed"),
        (Some(IsolationLevel::RepeatableRead), "repeatable read"),
        (Some(IsolationLevel::Serializable), "serializable"),
    ] {
        for read_only in [false, true] {
            let mut lease = pool.acquire().await.unwrap();
            let original = pid(&mut lease).await;
            let mut tx = lease
                .begin(TxOptions {
                    isolation,
                    read_only: Some(read_only),
                })
                .await
                .unwrap();
            let rows = tx.fetch(
                "SELECT pg_backend_pid()::bigint AS pid, current_setting('transaction_isolation') AS isolation, current_setting('transaction_read_only') = 'on' AS readonly",
                &[], limit(),
            ).await.unwrap();
            assert_eq!(rows[0].get::<i64>("pid").unwrap(), original);
            assert_eq!(rows[0].get::<String>("isolation").unwrap(), expected);
            assert_eq!(rows[0].get::<bool>("readonly").unwrap(), read_only);
            tx.execute("SELECT pg_advisory_xact_lock($1)", &[integer(75_823_382)])
                .await
                .unwrap();
            assert!(
                !observer
                    .fetch(
                        "SELECT pg_try_advisory_xact_lock($1) AS free",
                        &[integer(75_823_382)],
                        limit()
                    )
                    .await
                    .unwrap()[0]
                    .get::<bool>("free")
                    .unwrap()
            );
            let mut lease = tx.commit().await.unwrap();
            assert_eq!(pid(&mut lease).await, original);
            assert!(
                observer
                    .fetch(
                        "SELECT pg_try_advisory_xact_lock($1) AS free",
                        &[integer(75_823_382)],
                        limit()
                    )
                    .await
                    .unwrap()[0]
                    .get::<bool>("free")
                    .unwrap()
            );
            lease.recycle().await.unwrap();
        }
    }
    let lease = pool.acquire().await.unwrap();
    assert_eq!(
        lease
            .begin(TxOptions {
                isolation: Some(IsolationLevel::ReadUncommitted),
                read_only: None
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    capacity(&pool).await;
    observer.recycle().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn arity_savepoints_preserve_prior_writes_and_support_values_nulls_and_scripts() {
    let pool = pool();
    let lease = pool.acquire().await.unwrap();
    let mut tx = lease.begin(TxOptions::default()).await.unwrap();
    tx.execute_trusted_script(
        "CREATE TEMP TABLE quiet_tx_values(v bigint); INSERT INTO quiet_tx_values VALUES (1)",
    )
    .await
    .unwrap();
    for (sql, expected, null_result) in [
        ("SELECT to_json($1)::text AS v", "7", None),
        ("SELECT to_jsonb($1)::text AS v", "7", None),
        ("SELECT pg_typeof($1)::text AS v", "bigint", Some("bigint")),
    ] {
        assert_eq!(tx.execute(sql, &[integer(7)]).await.unwrap(), 1);
        assert_eq!(
            tx.fetch(sql, &[integer(7)], limit()).await.unwrap()[0]
                .get::<String>("v")
                .unwrap(),
            expected
        );
        let null = Parameter::new(DbType::I64, Value::Null(DbType::I64)).unwrap();
        assert_eq!(
            tx.fetch(sql, &[null], limit()).await.unwrap()[0]
                .get::<Option<String>>("v")
                .unwrap(),
            null_result.map(str::to_owned)
        );
    }
    for fetch in [false, true] {
        let parameters = [integer(7), integer(8)];
        let error = if fetch {
            tx.fetch(
                "INSERT INTO quiet_tx_values VALUES ($1) RETURNING v",
                &parameters,
                limit(),
            )
            .await
            .unwrap_err()
        } else {
            tx.execute("INSERT INTO quiet_tx_values VALUES ($1)", &parameters)
                .await
                .unwrap_err()
        };
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
    assert_eq!(
        tx.execute("/* no statement */", &[])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        tx.fetch("SELECT now()", &[], limit())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::WrongType
    );
    assert_eq!(
        tx.execute("SELECT broken_unknown_identifier", &[])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
    assert_eq!(
        tx.fetch("SELECT count(*) AS n FROM quiet_tx_values", &[], limit())
            .await
            .unwrap()[0]
            .get::<i64>("n")
            .unwrap(),
        1
    );
    tx.execute_trusted_script(
        "INSERT INTO quiet_tx_values VALUES (2); INSERT INTO quiet_tx_values VALUES (3)",
    )
    .await
    .unwrap();
    let mut lease = tx.commit().await.unwrap();
    assert_eq!(count(&mut lease, "quiet_tx_values").await, 3);
    lease.recycle().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn aborted_commit_returns_cancelled_not_success_and_rollback_can_reuse() {
    let pool = pool();
    let mut observer = make_pool(55433, Duration::from_secs(3))
        .acquire()
        .await
        .unwrap();
    observer
        .execute("CREATE TABLE quiet_tx_constraint(v bigint UNIQUE)", &[])
        .await
        .unwrap();
    for commit in [true, false] {
        let mut lease = pool.acquire().await.unwrap();
        let original = pid(&mut lease).await;
        let mut tx = lease.begin(TxOptions::default()).await.unwrap();
        tx.execute("INSERT INTO quiet_tx_constraint VALUES (1)", &[])
            .await
            .unwrap();
        assert_eq!(
            tx.execute("INSERT INTO quiet_tx_constraint VALUES (1)", &[])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Constraint
        );
        if commit {
            assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Cancelled);
            capacity(&pool).await;
            let mut replacement = pool.acquire().await.unwrap();
            assert_ne!(pid(&mut replacement).await, original);
            replacement.recycle().await.unwrap();
        } else {
            let mut lease = tx.rollback().await.unwrap();
            assert_eq!(pid(&mut lease).await, original);
            lease.recycle().await.unwrap();
        }
        assert_eq!(count(&mut observer, "quiet_tx_constraint").await, 0);
    }
    observer
        .execute("DROP TABLE quiet_tx_constraint", &[])
        .await
        .unwrap();
    observer.recycle().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn deferred_commit_error_is_authoritative_and_transaction_drop_discards() {
    let pool = pool();
    let mut lease = pool.acquire().await.unwrap();
    let original = pid(&mut lease).await;
    let mut tx = lease.begin(TxOptions::default()).await.unwrap();
    tx.execute_trusted_script("CREATE TEMP TABLE quiet_tx_deferred(v bigint UNIQUE DEFERRABLE INITIALLY DEFERRED); INSERT INTO quiet_tx_deferred VALUES(1),(1)")
        .await.unwrap();
    assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Constraint);
    capacity(&pool).await;
    let mut lease = pool.acquire().await.unwrap();
    assert_ne!(pid(&mut lease).await, original);
    let before_drop = pid(&mut lease).await;
    let mut tx = lease.begin(TxOptions::default()).await.unwrap();
    tx.execute("SELECT pg_advisory_xact_lock(75823383)", &[])
        .await
        .unwrap();
    drop(tx);
    capacity(&pool).await;
    let mut lease = pool.acquire().await.unwrap();
    assert_ne!(pid(&mut lease).await, before_drop);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if lease
                .fetch(
                    "SELECT pg_try_advisory_xact_lock(75823383) AS free",
                    &[],
                    limit(),
                )
                .await
                .unwrap()[0]
                .get::<bool>("free")
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    lease.recycle().await.unwrap();
    pool.close();
}

#[tokio::test]
async fn read_only_and_user_transaction_control_never_claim_success() {
    let lease = pool().acquire().await.unwrap();
    let mut tx = lease
        .begin(TxOptions {
            isolation: None,
            read_only: Some(true),
        })
        .await
        .unwrap();
    assert_eq!(
        tx.execute(
            "CREATE TABLE quiet_tx_readonly_must_not_exist(v bigint)",
            &[]
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Decode
    );
    tx.rollback().await.unwrap().recycle().await.unwrap();
    for script in [
        "/* fixture */ COMMIT",
        "COMMIT; BEGIN",
        "COMMIT; SELECT 1 / 0",
    ] {
        let mut tx = pool()
            .acquire()
            .await
            .unwrap()
            .begin(TxOptions::default())
            .await
            .unwrap();
        assert_eq!(
            tx.execute_trusted_script(script).await.unwrap_err().kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Connection);
    }
    let mut lease = pool().acquire().await.unwrap();
    lease.batch("BEGIN").await.unwrap();
    assert_eq!(
        lease.begin(TxOptions::default()).await.unwrap_err().kind(),
        ErrorKind::Connection
    );
}

#[tokio::test]
async fn transaction_row_limit_failure_discards_and_cannot_commit() {
    let pool = pool();
    let mut tx = pool
        .acquire()
        .await
        .unwrap()
        .begin(TxOptions::default())
        .await
        .unwrap();
    assert_eq!(
        tx.fetch(
            "SELECT 1::bigint AS n FROM generate_series(1,2)",
            &[],
            RowLimit::new(1, 9).unwrap()
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::WriteOutcomeUnknown
    );
    assert_eq!(tx.commit().await.unwrap_err().kind(), ErrorKind::Connection);
    capacity(&pool).await;
    pool.close();
}

#[tokio::test]
async fn cancelling_begin_query_commit_and_rollback_discards_the_socket_owner() {
    for stage in [
        proxy::Stage::Begin,
        proxy::Stage::Query,
        proxy::Stage::Commit,
        proxy::Stage::Rollback,
    ] {
        proxy::cancelled(stage).await;
    }
}

#[tokio::test]
async fn withheld_commit_response_is_unknown_despite_observed_commit() {
    proxy::lost_commit().await;
}

#[tokio::test]
async fn deadlines_distinguish_preflight_from_dispatched_write_and_commit() {
    for stage in [
        proxy::Stage::Begin,
        proxy::Stage::Preflight,
        proxy::Stage::Query,
        proxy::Stage::Commit,
        proxy::Stage::Rollback,
    ] {
        proxy::deadline(stage).await;
    }
}

#[tokio::test]
async fn dropping_only_a_borrowed_query_discards_even_with_transaction_owner_retained() {
    proxy::retained_owner_cancellation().await;
}
