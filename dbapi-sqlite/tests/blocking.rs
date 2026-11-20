//! Live blocking-adapter coverage against the SQLite provider.
use std::{sync::Arc, thread};

use neotales_dbapi::{
    DbType, ErrorKind, Parameter, PolicyPool, PoolPolicy, QueryOptions, RowLimit, TxOptions, Value,
    blocking::BlockingPool, sync::Pool as SyncPool,
};
use neotales_dbapi_sqlite::{SqliteConfig, SqlitePoolProvider};
use tempfile::TempDir;
use tokio::runtime::Runtime;

struct Fixture {
    _directory: TempDir,
    _runtime: Runtime,
    pool: BlockingPool,
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().expect("temporary directory");
    let config = SqliteConfig::new(directory.path().join("blocking.sqlite")).max_connections(8);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let provider = runtime
        .block_on(SqlitePoolProvider::connect(config))
        .expect("SQLite provider");
    Fixture {
        _directory: directory,
        _runtime: runtime,
        pool: BlockingPool::new(provider),
    }
}

fn limit() -> RowLimit {
    RowLimit::new(16, 16_384).expect("valid limit")
}

fn one_limit() -> RowLimit {
    RowLimit::new(1, 1024).expect("valid limit")
}

fn insert(id: i64, name: &str) -> [Parameter; 2] {
    [
        Parameter::new(DbType::I64, Value::I64(id)).expect("id"),
        Parameter::new(DbType::Text, Value::Text(name.to_owned())).expect("name"),
    ]
}

fn create_table(pool: &BlockingPool) {
    SyncPool::execute(
        pool,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        QueryOptions::new(),
    )
    .expect("create table");
}

#[test]
fn reads_rows_and_advances_the_reader() {
    let fixture = fixture();
    create_table(&fixture.pool);
    for (id, name) in [(1, "ada"), (2, "grace")] {
        SyncPool::execute(
            &fixture.pool,
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&insert(id, name)),
        )
        .expect("insert");
    }

    let mut session = SyncPool::acquire(&fixture.pool).expect("session");
    let mut reader = session
        .fetch(
            "SELECT id, name FROM users ORDER BY id",
            QueryOptions::new().limit(limit()),
        )
        .expect("reader");
    let first = reader.next_row().expect("row").expect("first");
    assert_eq!(first.read_at::<i64>(0).expect("id"), 1);
    assert_eq!(first.read::<String>("name").expect("name"), "ada");
    let second = reader.next_row().expect("row").expect("second");
    assert_eq!(second.read_at::<i64>(0).expect("id"), 2);
    assert!(reader.next_row().expect("end").is_none());
    assert!(!reader.has_next_set().expect("single set"));
    reader.finish().expect("finish");
}

#[test]
fn convenience_calls_and_cardinality() {
    let fixture = fixture();
    create_table(&fixture.pool);
    SyncPool::execute(
        &fixture.pool,
        "INSERT INTO users (id, name) VALUES (?, ?)",
        QueryOptions::new().bind_all(&insert(1, "ada")),
    )
    .expect("insert");

    assert_eq!(
        SyncPool::fetch_scalar(
            &fixture.pool,
            "SELECT count(*) FROM users",
            QueryOptions::new().limit(one_limit()),
        )
        .expect("scalar"),
        Value::I64(1)
    );
    assert_eq!(
        SyncPool::fetch_one(
            &fixture.pool,
            "SELECT name FROM users WHERE id = ?",
            QueryOptions::new()
                .bind_all(&insert(1, "unused")[..1])
                .limit(limit()),
        )
        .expect("one")
        .read::<String>("name")
        .expect("name"),
        "ada"
    );
    assert_eq!(
        SyncPool::fetch_first(
            &fixture.pool,
            "SELECT name FROM users",
            QueryOptions::new().limit(limit())
        )
        .expect("first")
        .expect("row")
        .read::<String>("name")
        .expect("name"),
        "ada"
    );
}

#[test]
fn commit_rollback_and_implicit_rollback() {
    let fixture = fixture();
    create_table(&fixture.pool);
    let mut session = SyncPool::acquire(&fixture.pool).expect("session");

    let mut transaction = session.begin(TxOptions::default()).expect("transaction");
    transaction
        .execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&insert(1, "ada")),
        )
        .expect("insert");
    transaction.commit().expect("commit");

    let mut transaction = session.begin(TxOptions::default()).expect("transaction");
    transaction
        .execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&insert(2, "grace")),
        )
        .expect("insert");
    transaction.rollback().expect("rollback");

    let transaction = session.begin(TxOptions::default()).expect("transaction");
    drop(transaction);

    assert_eq!(
        SyncPool::fetch_scalar(
            &fixture.pool,
            "SELECT count(*) FROM users",
            QueryOptions::new().limit(one_limit()),
        )
        .expect("count"),
        Value::I64(1)
    );
}

#[test]
fn dropping_an_unfinished_reader_keeps_the_session_usable() {
    let fixture = fixture();
    create_table(&fixture.pool);
    for id in 1..=3 {
        SyncPool::execute(
            &fixture.pool,
            "INSERT INTO users (id, name) VALUES (?, ?)",
            QueryOptions::new().bind_all(&insert(id, "user")),
        )
        .expect("insert");
    }

    let mut session = SyncPool::acquire(&fixture.pool).expect("session");
    let mut reader = session
        .fetch(
            "SELECT id FROM users ORDER BY id",
            QueryOptions::new().limit(limit()),
        )
        .expect("reader");
    assert!(reader.next_row().expect("row").is_some());
    drop(reader);
    assert_eq!(
        session
            .fetch_scalar(
                "SELECT count(*) FROM users",
                QueryOptions::new().limit(one_limit()),
            )
            .expect("session reusable"),
        Value::I64(3)
    );
}

#[test]
fn concurrent_blocking_callers_share_the_pool() {
    let fixture = fixture();
    create_table(&fixture.pool);
    let pool = Arc::new(fixture.pool.clone());
    let mut workers = Vec::new();
    for _ in 0..8 {
        let pool = Arc::clone(&pool);
        workers.push(thread::spawn(move || {
            for _ in 0..4 {
                let mut session = SyncPool::acquire(pool.as_ref()).expect("session");
                let value = session
                    .fetch_scalar("SELECT 1", QueryOptions::new().limit(one_limit()))
                    .expect("scalar");
                assert_eq!(value, Value::I64(1));
            }
        }));
    }
    for worker in workers {
        worker.join().expect("worker");
    }
}

#[test]
fn blocking_session_and_transaction_preserve_inherited_and_overridden_limits() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let provider = runtime
        .block_on(SqlitePoolProvider::connect(SqliteConfig::new(
            directory.path().join("policy-blocking.sqlite"),
        )))
        .unwrap();
    let policy = PolicyPool::new(
        provider,
        PoolPolicy {
            default_limit: RowLimit::new(1, 1024).unwrap(),
            ..PoolPolicy::default()
        },
    );
    let pool = BlockingPool::new(policy);
    let sql = "SELECT 1 UNION ALL SELECT 2";
    let mut session = SyncPool::acquire(&pool).unwrap();
    assert_eq!(
        session
            .fetch_all(sql, QueryOptions::new())
            .unwrap_err()
            .kind(),
        ErrorKind::Overflow
    );
    drop(session);
    let mut session = SyncPool::acquire(&pool).unwrap();
    let override_limit = RowLimit::new(2, 1024).unwrap();
    assert_eq!(
        session
            .fetch_all(sql, QueryOptions::new().limit(override_limit))
            .unwrap()
            .len(),
        2
    );
    let mut tx = session.begin(TxOptions::default()).unwrap();
    assert_eq!(
        tx.fetch_all(sql, QueryOptions::new().limit(override_limit))
            .unwrap()
            .len(),
        2
    );
    tx.rollback().unwrap();
}
