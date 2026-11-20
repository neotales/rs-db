//! Explicit live lane: tests/live.sh creates and removes the only server used here.
use neotales_dbapi::{
    DbType, Parameter, Pool, QueryOptions, Row, RowLimit, Transaction, TxOptions, Value,
};
use neotales_dbapi_postgres::{Config, PoolOptions, PostgresPool, Profile};
use neotales_migration_postgres::{Error, HistoryReason, PostgresBackend};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn pool() -> Arc<PostgresPool> {
    pool_at(55435)
}

fn pool_at(port: u16) -> Arc<PostgresPool> {
    assert_eq!(
        std::env::var("NEOTALES_MIGRATION_POSTGRES_LIVE").as_deref(),
        Ok("disposable-17"),
        "run tests/live.sh, not an existing server"
    );
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("migrator_test")
        .dbname("migrator_test");
    Arc::new(
        PostgresPool::loopback_plaintext_for_tests(
            config,
            PoolOptions::new(
                8,
                Profile::normal(Duration::from_secs(3), Duration::from_secs(1)).unwrap(),
                Duration::from_secs(15),
            )
            .unwrap(),
        )
        .unwrap(),
    )
}

#[path = "execution/mod.rs"]
mod execution;

fn schema(suffix: &str) -> String {
    format!("migration_{}_{suffix}", std::process::id())
}

fn backend(schema: &str) -> PostgresBackend {
    PostgresBackend::new(schema, "history", Duration::from_secs(2)).unwrap()
}

async fn script(pool: &PostgresPool, sql: &str) {
    let mut session = pool.acquire_async().await.unwrap();
    session
        .execute_trusted_script_async(sql, QueryOptions::new())
        .await
        .unwrap();
    drop(session);
    pool.wait_for_cleanup().await.unwrap();
}

async fn query(pool: &PostgresPool, sql: &str) -> Vec<Row> {
    pool.fetch_all_async(
        sql,
        QueryOptions::new().limit(RowLimit::new(100, 16_384).unwrap()),
    )
    .await
    .unwrap()
}

async fn finish(pool: &PostgresPool) {
    pool.wait_for_cleanup().await.unwrap();
    pool.close();
}

async fn try_lock(transaction: &mut (dyn Transaction<'_> + '_), key: i64) -> bool {
    let rows = transaction
        .fetch_all_async(
            "SELECT pg_catalog.pg_try_advisory_xact_lock($1) AS locked",
            QueryOptions::new()
                .bind(Parameter::new(DbType::I64, Value::I64(key)).unwrap())
                .limit(RowLimit::new(1, 64).unwrap()),
        )
        .await
        .unwrap();
    rows[0].get("locked").unwrap()
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn same_and_different_modules_and_ledgers_bootstrap_concurrently() {
    let pool = pool();
    let schema = schema("concurrent");
    let backend = backend(&schema);
    let other = PostgresBackend::new(&schema, "other", Duration::from_secs(2)).unwrap();
    let (same_a, same_b, distinct, other_ledger) = tokio::join!(
        backend.initialize_module(pool.as_ref(), "same", "fp-v1", &[]),
        backend.initialize_module(pool.as_ref(), "same", "fp-v1", &[]),
        backend.initialize_module(pool.as_ref(), "other", "fp-v1", &[]),
        other.initialize_module(pool.as_ref(), "same", "fp-v1", &[]),
    );
    let state = same_a.unwrap();
    assert_eq!(same_b.unwrap(), state);
    assert_eq!(state.module(), "same");
    assert_eq!(state.fingerprint(), "fp-v1");
    assert_eq!(state.applied_count(), 0);
    assert_eq!(state.head(), None);
    assert_eq!(distinct.unwrap().module(), "other");
    assert_eq!(other_ledger.unwrap().module(), "same");
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".history_modules"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 2);
    let (first, second) = tokio::join!(
        backend.initialize_module(pool.as_ref(), "competing", "first", &[]),
        backend.initialize_module(pool.as_ref(), "competing", "second", &[]),
    );
    assert!(
        matches!(
            (&first, &second),
            (Ok(_), Err(Error::History(HistoryReason::BindingMismatch)))
                | (Err(Error::History(HistoryReason::BindingMismatch)), Ok(_))
        ),
        "conflicting first registrations must not both succeed: {first:?}, {second:?}"
    );
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "same", "changed", &[])
            .await,
        Err(Error::History(HistoryReason::BindingMismatch))
    ));
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn schema_qualified_history_names_and_opaque_values_round_trip_at_limits() {
    let pool = pool();
    let schema = "s".repeat(63);
    let table = "h".repeat(55);
    let backend = PostgresBackend::new(&schema, &table, Duration::from_secs(2)).unwrap();
    let module = format!("{}'", "m".repeat(254));
    let fingerprint = format!("{}'", "é".repeat(127));
    let first = backend
        .initialize_module(pool.as_ref(), &module, &fingerprint, &[])
        .await
        .unwrap();
    assert_eq!(
        first,
        backend
            .initialize_module(pool.as_ref(), &module, &fingerprint, &[])
            .await
            .unwrap()
    );
    let reserved = PostgresBackend::new(&schema, "select", Duration::from_secs(2)).unwrap();
    reserved
        .initialize_module(pool.as_ref(), "app", "fp", &[])
        .await
        .unwrap();
    let rows = query(&pool, &format!(
        "SELECT count(*) AS n FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='{schema}' AND c.relname='{table}_modules'"
    )).await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 1);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn bytewise_identity_and_actual_c_collations_are_verified() {
    let pool = pool();
    let schema = schema("collation");
    let backend = backend(&schema);
    backend
        .initialize_module(pool.as_ref(), "Case", "FP", &[])
        .await
        .unwrap();
    backend
        .initialize_module(pool.as_ref(), "case", "fp", &[])
        .await
        .unwrap();
    let rows = query(
        &pool,
        &format!(
            "SELECT count(*) AS n,
            bool_and(a.attcollation = 'pg_catalog.\"C\"'::regcollation::oid
                     AND co.collisdeterministic) AS valid
         FROM pg_catalog.pg_attribute a
         JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
         JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
         JOIN pg_catalog.pg_collation co ON co.oid=a.attcollation
         WHERE n.nspname='{schema}' AND c.relname IN ('history','history_modules')
           AND a.attnum > 0 AND a.atttypid='pg_catalog.text'::regtype"
        ),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 6);
    assert!(rows[0].get::<bool>("valid").unwrap());
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".history_modules"),
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        2,
        "case-distinct modules must remain distinct"
    );

    script(
        &pool,
        &format!(
            "INSERT INTO \"{schema}\".history VALUES ('Case',1,repeat('é',127)||'x','FP');
         UPDATE \"{schema}\".history_modules SET applied_count=1, head_version=1,
             head_checksum=repeat('é',127)||'x' WHERE module='Case'"
        ),
    )
    .await;
    let state = backend
        .initialize_module(pool.as_ref(), "Case", "FP", &[])
        .await
        .unwrap();
    assert_eq!(
        state.head().unwrap().1.len(),
        255,
        "checksum limit is bytes, not characters"
    );
    script(
        &pool,
        &format!(
            "UPDATE \"{schema}\".history SET checksum='Head';
         UPDATE \"{schema}\".history_modules SET head_checksum='head' WHERE module='Case'"
        ),
    )
    .await;
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "Case", "FP", &[])
            .await,
        Err(Error::History(HistoryReason::InconsistentRows))
    ));
    script(
        &pool,
        &format!(
            "UPDATE \"{schema}\".history_modules SET head_checksum='Head' WHERE module='Case';
         UPDATE \"{schema}\".history SET fingerprint='fp'"
        ),
    )
    .await;
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "Case", "FP", &[])
            .await,
        Err(Error::History(HistoryReason::InconsistentRows))
    ));
    script(
        &pool,
        &format!("UPDATE \"{schema}\".history SET fingerprint='FP'"),
    )
    .await;
    backend
        .initialize_module(pool.as_ref(), "Case", "FP", &[])
        .await
        .unwrap();
    reject_nondeterministic_collation(&pool, &schema, &backend).await;
    finish(&pool).await;
}

async fn reject_nondeterministic_collation(
    pool: &PostgresPool,
    schema: &str,
    backend: &PostgresBackend,
) {
    let rows = query(
        pool,
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_collation WHERE collprovider='i') AS icu",
    )
    .await;
    if rows[0].get::<bool>("icu").unwrap() {
        script(pool, &format!(
            "CREATE COLLATION \"{schema}\".caseless (provider=icu, locale='und-u-ks-level2', deterministic=false)"
        )).await;
        let rows = query(
            pool,
            &format!("SELECT 'Head' COLLATE \"{schema}\".caseless = 'head' AS aliases"),
        )
        .await;
        assert!(
            rows[0].get::<bool>("aliases").unwrap(),
            "ICU fixture must actually alias these values"
        );
        script(pool, &format!(
            "ALTER TABLE \"{schema}\".history ALTER COLUMN checksum TYPE text COLLATE \"{schema}\".caseless"
        )).await;
        assert!(matches!(
            backend.initialize_module(pool, "Case", "FP", &[]).await,
            Err(Error::History(HistoryReason::IncompatibleLedger))
        ));
        let rows = query(pool, &format!("SELECT checksum FROM \"{schema}\".history")).await;
        assert_eq!(
            rows[0].get::<String>("checksum").unwrap(),
            "Head",
            "rejection must not rewrite records"
        );
    }
    // The calling test's catalog assertions always run, including without ICU.
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn unregistered_application_objects_are_not_adopted_and_bootstrap_rolls_back() {
    let pool = pool();
    let schema = schema("managed");
    script(
        &pool,
        &format!(
            "CREATE SCHEMA \"{schema}\";
        CREATE TABLE \"{schema}\".kept (value integer);
        INSERT INTO \"{schema}\".kept VALUES (7);
        CREATE VIEW \"{schema}\".kept_view AS SELECT * FROM \"{schema}\".kept;
        CREATE SEQUENCE \"{schema}\".kept_sequence"
        ),
    )
    .await;
    let backend = backend(&schema);
    for name in ["kept", "kept_view", "kept_sequence"] {
        assert!(matches!(
            backend
                .initialize_module(pool.as_ref(), "app", "fp", &[(&schema, name)])
                .await,
            Err(Error::UntrackedObjects)
        ));
    }
    let rows = query(
        &pool,
        &format!("SELECT value::bigint AS value FROM \"{schema}\".kept"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 7);
    let rows = query(
        &pool,
        &format!(
            "SELECT count(*) AS n FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='{schema}' AND c.relname LIKE 'history%'"
        ),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn unowned_lookalike_and_partial_ledgers_are_rejected_without_writes() {
    let pool = pool();
    let schema = schema("unowned");
    let backend = backend(&schema);
    backend
        .initialize_module(pool.as_ref(), "app", "fp", &[])
        .await
        .unwrap();
    script(&pool, &format!("COMMENT ON TABLE \"{schema}\".history IS NULL;
        INSERT INTO \"{schema}\".history VALUES ('app', 1, 'kept', 'fp');
        UPDATE \"{schema}\".history_modules SET applied_count=1, head_version=1, head_checksum='kept'")).await;
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "app", "fp", &[])
            .await,
        Err(Error::History(HistoryReason::UnownedLedger))
    ));
    let rows = query(&pool, &format!("SELECT checksum FROM \"{schema}\".history")).await;
    assert_eq!(rows[0].get::<String>("checksum").unwrap(), "kept");
    script(&pool, &format!("DROP TABLE \"{schema}\".history_modules")).await;
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "new", "fp", &[])
            .await,
        Err(Error::History(HistoryReason::PartialLedger))
    ));
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".history"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 1);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn owned_but_incompatible_ledger_shapes_are_rejected() {
    let pool = pool();
    let mutations = [
        "ALTER TABLE SCHEMA.history ADD COLUMN extra text",
        "ALTER TABLE SCHEMA.history ALTER COLUMN checksum SET DEFAULT 'hidden'",
        "ALTER TABLE SCHEMA.history ALTER COLUMN checksum DROP NOT NULL",
        "ALTER TABLE SCHEMA.history DROP CONSTRAINT history_version_check",
        "ALTER TABLE SCHEMA.history ENABLE ROW LEVEL SECURITY",
        "CREATE INDEX extra ON SCHEMA.history(checksum)",
        "ALTER TABLE SCHEMA.history ALTER COLUMN checksum TYPE varchar",
        "ALTER TABLE SCHEMA.history ALTER COLUMN fingerprint TYPE text COLLATE \"default\"",
        "ALTER TABLE SCHEMA.history_modules ADD CONSTRAINT extra CHECK (module <> '')",
        "COMMENT ON TABLE SCHEMA.history_modules IS 'neotales-migration-postgres/ledger/v99/modules'",
        "CREATE RULE changed AS ON INSERT TO SCHEMA.history DO INSTEAD NOTHING",
        "CREATE FUNCTION SCHEMA.changed() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RETURN NEW; END $$;
         CREATE TRIGGER changed BEFORE INSERT ON SCHEMA.history FOR EACH ROW EXECUTE FUNCTION SCHEMA.changed()",
        "DROP TABLE SCHEMA.history; CREATE VIEW SCHEMA.history AS SELECT 1 AS unrelated",
    ];
    for (index, mutation) in mutations.iter().enumerate() {
        let schema = schema(&format!("shape_{index}"));
        let backend = backend(&schema);
        backend
            .initialize_module(pool.as_ref(), "app", "fp", &[])
            .await
            .unwrap();
        script(&pool, &mutation.replace("SCHEMA", &format!("\"{schema}\""))).await;
        assert!(
            matches!(
                backend
                    .initialize_module(pool.as_ref(), "new", "fp", &[])
                    .await,
                Err(Error::History(HistoryReason::IncompatibleLedger))
            ),
            "{mutation}"
        );
        let rows = query(
            &pool,
            &format!("SELECT count(*) AS n FROM \"{schema}\".history_modules"),
        )
        .await;
        assert_eq!(rows[0].get::<i64>("n").unwrap(), 1);
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn populated_history_validates_heads_fingerprints_missing_rows_and_all_orphans() {
    let pool = pool();
    let mutations = [
        "DELETE FROM SCHEMA.history WHERE version=1",
        "DELETE FROM SCHEMA.history",
        "UPDATE SCHEMA.history SET checksum='' WHERE version=1",
        "UPDATE SCHEMA.history SET checksum=repeat('x',256) WHERE version=1",
        "UPDATE SCHEMA.history SET checksum=repeat('é',128) WHERE version=1",
        "UPDATE SCHEMA.history SET checksum=repeat('x',8192) WHERE version=3;
         UPDATE SCHEMA.history_modules SET head_checksum=repeat('x',8192)",
        "UPDATE SCHEMA.history_modules SET head_checksum=repeat('x',8192)",
        "UPDATE SCHEMA.history SET fingerprint='wrong' WHERE version=1",
        "UPDATE SCHEMA.history_modules SET head_checksum='wrong'",
        "INSERT INTO SCHEMA.history VALUES ('orphan-other-module', 1, 'untouched', 'fp')",
        "INSERT INTO SCHEMA.history_modules VALUES ('broken-other-module', 'fp', 1, 1, 'missing')",
    ];
    for (index, mutation) in mutations.iter().enumerate() {
        let schema = schema(&format!("rows_{index}"));
        let backend = backend(&schema);
        backend
            .initialize_module(pool.as_ref(), "app", "fp", &[])
            .await
            .unwrap();
        script(&pool, &format!(
            "INSERT INTO \"{schema}\".history VALUES ('app', 1, 'first', 'fp'), ('app', 3, 'last', 'fp');
             UPDATE \"{schema}\".history_modules SET applied_count=2, head_version=3, head_checksum='last'"
        )).await;
        let state = backend
            .initialize_module(pool.as_ref(), "app", "fp", &[])
            .await
            .unwrap();
        assert_eq!(state.applied_count(), 2);
        assert_eq!(state.head(), Some((3, "last")));
        script(&pool, &mutation.replace("SCHEMA", &format!("\"{schema}\""))).await;
        let before = query(
            &pool,
            &format!("SELECT * FROM \"{schema}\".history ORDER BY module, version"),
        )
        .await;
        assert!(
            matches!(
                backend
                    .initialize_module(pool.as_ref(), "app", "fp", &[])
                    .await,
                Err(Error::History(HistoryReason::InconsistentRows))
            ),
            "{mutation}"
        );
        let after = query(
            &pool,
            &format!("SELECT * FROM \"{schema}\".history ORDER BY module, version"),
        )
        .await;
        assert_eq!(before, after, "rejection must preserve data");
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn advisory_timeout_precedes_bootstrap_and_releases_waiter_transaction() {
    let pool = pool();
    let schema = schema("timeout");
    let backend = PostgresBackend::new(&schema, "history", Duration::from_millis(80)).unwrap();
    let mut holder = pool.acquire_async().await.unwrap();
    let mut transaction = holder.begin_async(TxOptions::default()).await.unwrap();
    assert!(try_lock(&mut *transaction, backend.advisory_lock_key()).await);
    let started = Instant::now();
    assert!(matches!(
        backend
            .initialize_module(pool.as_ref(), "app", "fp", &[])
            .await,
        Err(Error::LockTimeout)
    ));
    assert!(started.elapsed() >= Duration::from_millis(60));
    assert!(started.elapsed() < Duration::from_secs(2));
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM pg_catalog.pg_namespace WHERE nspname='{schema}'"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);
    transaction.rollback_async().await.unwrap();
    drop(holder);
    backend
        .initialize_module(pool.as_ref(), "app", "fp", &[])
        .await
        .unwrap();
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn cancelled_lock_waiter_cannot_bootstrap_or_poison_reuse() {
    let pool = pool();
    let schema = schema("cancel_waiter");
    let backend = Arc::new(backend(&schema));
    let mut holder = pool.acquire_async().await.unwrap();
    let mut transaction = holder.begin_async(TxOptions::default()).await.unwrap();
    assert!(try_lock(&mut *transaction, backend.advisory_lock_key()).await);
    let worker_pool = Arc::clone(&pool);
    let worker_backend = Arc::clone(&backend);
    let task = tokio::spawn(async move {
        worker_backend
            .initialize_module(worker_pool.as_ref(), "app", "fp", &[])
            .await
    });
    // Observe the worker's open transaction, not just scheduler elapsed time.
    wait_for(
        &pool,
        "SELECT count(*) >= 2 AS ready FROM pg_catalog.pg_stat_activity
        WHERE datname='migrator_test' AND pid <> pg_backend_pid()
          AND state='idle in transaction' AND query LIKE '%pg_try_advisory_xact_lock%'",
    )
    .await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM pg_catalog.pg_namespace WHERE nspname='{schema}'"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);
    transaction.rollback_async().await.unwrap();
    drop(holder);
    backend
        .initialize_module(pool.as_ref(), "app", "fp", &[])
        .await
        .unwrap();
    finish(&pool).await;
}

async fn wait_for(pool: &PostgresPool, sql: &str) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if query(pool, sql).await[0].get::<bool>("ready").unwrap() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("observable database state was not reached");
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn cancellation_after_lock_acquisition_eventually_releases_lock_and_rolls_back() {
    let pool = pool();
    let schema = schema("cancel_owner");
    let backend = Arc::new(backend(&schema));
    backend
        .initialize_module(pool.as_ref(), "existing", "fp", &[])
        .await
        .unwrap();
    let mut blocker = pool.acquire_async().await.unwrap();
    let mut transaction = blocker.begin_async(TxOptions::default()).await.unwrap();
    transaction
        .execute_trusted_script_async(
            &format!("LOCK TABLE \"{schema}\".history IN ACCESS EXCLUSIVE MODE"),
            QueryOptions::new(),
        )
        .await
        .unwrap();
    let worker_pool = Arc::clone(&pool);
    let worker_backend = Arc::clone(&backend);
    let task = tokio::spawn(async move {
        worker_backend
            .initialize_module(worker_pool.as_ref(), "cancelled", "fp", &[])
            .await
    });
    wait_for(
        &pool,
        &format!(
            "SELECT count(*) > 0 AS ready FROM pg_catalog.pg_stat_activity
        WHERE datname='migrator_test' AND wait_event_type='Lock'
          AND query LIKE 'LOCK TABLE \"{schema}\".%'"
        ),
    )
    .await;
    let mut observer = pool.acquire_async().await.unwrap();
    let mut observer_tx = observer.begin_async(TxOptions::default()).await.unwrap();
    assert!(!try_lock(&mut *observer_tx, backend.advisory_lock_key()).await);
    observer_tx.rollback_async().await.unwrap();
    drop(observer);
    // Another schema can initialize while this schema is held.
    initialize_independent(&pool).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    transaction.rollback_async().await.unwrap();
    drop(blocker);
    wait_for(
        &pool,
        &format!(
            "SELECT pg_catalog.pg_try_advisory_xact_lock({}) AS ready",
            backend.advisory_lock_key()
        ),
    )
    .await;
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".history_modules WHERE module='cancelled'"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);
    backend
        .initialize_module(pool.as_ref(), "cancelled", "fp", &[])
        .await
        .unwrap();
    finish(&pool).await;
}

async fn initialize_independent(pool: &PostgresPool) {
    backend(&schema("independent"))
        .initialize_module(pool, "app", "fp", &[])
        .await
        .unwrap();
}
