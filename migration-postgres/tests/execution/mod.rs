use super::{backend, finish, pool, pool_at, query, schema, script, wait_for};
use neotales_dbapi::ErrorKind;
use neotales_migration::{Dialect, Identifier, Migration, Migrations, bind_identifiers};
use neotales_migration_postgres::{Error, HistoryReason};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

mod commit_proxy;
mod flatten;

fn definitions(schema: &str) -> (Migrations, String) {
    let bindings = BTreeMap::from([
        ("schema".to_owned(), Identifier::new(schema).unwrap()),
        ("table".to_owned(), Identifier::new("payload").unwrap()),
    ]);
    let mut fingerprint = None;
    let mut definitions = Vec::new();
    // Read actual caller-supplied UTF-8 SQL files before opening any transaction.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
    for version in 1..=2 {
        let mut sql = Vec::new();
        for direction in ["up", "down"] {
            let source =
                std::fs::read_to_string(root.join(format!("{version:03}.{direction}.sql")))
                    .unwrap();
            let (bound, binding) =
                bind_identifiers(&source, &bindings, Dialect::PostgreSql).unwrap();
            if let Some(previous) = &fingerprint {
                assert_eq!(previous, &binding);
            }
            fingerprint = Some(binding);
            sql.push(bound);
        }
        definitions.push(
            Migration::new(
                version,
                format!("migration_{version}"),
                &sql[0],
                Some(sql[1].clone()),
            )
            .unwrap(),
        );
    }
    (Migrations::new(definitions).unwrap(), fingerprint.unwrap())
}

async fn assert_two(pool: &neotales_dbapi_postgres::PostgresPool, schema: &str) {
    let rows = query(
        pool,
        &format!("SELECT count(*) AS n, bool_and(active) AS active FROM \"{schema}\".payload"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 2);
    assert!(rows[0].get::<bool>("active").unwrap());
    let rows = query(
        pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".history"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 2);
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn sql_files_up_repeat_and_two_step_down_are_atomic_and_checksummed() {
    let pool = pool();
    let schema = schema("execute");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    let objects = [(&*schema, "payload")];
    let first = backend
        .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
        .await
        .unwrap();
    assert_eq!(first.applied_count(), 2);
    assert_eq!(first.head(), Some((2, migrations.as_slice()[1].checksum())));
    assert_eq!(
        first,
        backend
            .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
            .await
            .unwrap()
    );
    assert_two(&pool, &schema).await;
    let rows = query(
        &pool,
        &format!("SELECT version,checksum FROM \"{schema}\".history ORDER BY version"),
    )
    .await;
    for (row, definition) in rows.iter().zip(migrations.as_slice()) {
        assert_eq!(
            row.get::<i64>("version").unwrap(),
            i64::try_from(definition.version()).unwrap()
        );
        assert_eq!(
            row.get::<String>("checksum").unwrap(),
            definition.checksum()
        );
    }
    let next = backend
        .down(pool.as_ref(), "app", &migrations, &fingerprint, 1, &objects)
        .await
        .unwrap();
    assert_eq!(next.applied_count(), 1);
    let rows = query(&pool, &format!("SELECT value FROM \"{schema}\".payload")).await;
    assert_eq!(rows[0].get::<String>("value").unwrap(), "kept");
    let empty = backend
        .down(pool.as_ref(), "app", &migrations, &fingerprint, 1, &objects)
        .await
        .unwrap();
    assert_eq!(empty.applied_count(), 0);
    assert_eq!(empty.head(), None);
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &migrations, &fingerprint, 1, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    backend
        .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
        .await
        .unwrap();
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn missing_down_and_baseline_barrier_preflight_every_step() {
    let pool = pool();
    for baseline in [false, true] {
        let schema = schema(if baseline { "barrier" } else { "missing_down" });
        let backend = backend(&schema);
        let (original, fingerprint) = definitions(&schema);
        let first = &original.as_slice()[0];
        let first = if baseline {
            first.clone().into_baseline()
        } else {
            Migration::new(first.version(), first.name(), first.up(), None).unwrap()
        };
        let migrations = Migrations::new(vec![first, original.as_slice()[1].clone()]).unwrap();
        let objects = [(&*schema, "payload")];
        let before = backend
            .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
            .await
            .unwrap();
        let error = backend
            .down(pool.as_ref(), "app", &migrations, &fingerprint, 2, &objects)
            .await
            .unwrap_err();
        let expected = if baseline {
            neotales_migration::Error::Baseline
        } else {
            neotales_migration::Error::MissingDown
        };
        assert!(matches!(error, Error::Migration(reason) if reason == expected));
        assert_eq!(
            before,
            backend
                .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
                .await
                .unwrap()
        );
        assert_two(&pool, &schema).await; // The first selected down would delete row 2/drop active.
        if baseline {
            backend
                .down(pool.as_ref(), "app", &migrations, &fingerprint, 1, &objects)
                .await
                .unwrap();
            assert!(matches!(
                backend
                    .down(pool.as_ref(), "app", &original, &fingerprint, 1, &objects)
                    .await,
                Err(Error::Migration(neotales_migration::Error::Modified))
            ));
            assert!(matches!(
                backend
                    .down(pool.as_ref(), "app", &migrations, &fingerprint, 1, &objects)
                    .await,
                Err(Error::Migration(neotales_migration::Error::Baseline))
            ));
        }
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn syntax_failure_rolls_back_prior_scripts_and_bootstrap_preserving_unrelated_data() {
    let pool = pool();
    let schema = schema("syntax");
    let backend = backend(&schema);
    script(&pool, &format!("CREATE SCHEMA \"{schema}\"; CREATE TABLE \"{schema}\".unrelated (value bigint); INSERT INTO \"{schema}\".unrelated VALUES(7)")).await;
    let (migrations, fingerprint) = definitions(&schema);
    let second = &migrations.as_slice()[1];
    let broken = Migration::new(
        2,
        second.name(),
        format!("{}; SELECT (;", second.up()),
        second.down().map(str::to_owned),
    )
    .unwrap();
    let broken = Migrations::new(vec![migrations.as_slice()[0].clone(), broken]).unwrap();
    assert!(matches!(
        backend
            .up(
                pool.as_ref(),
                "app",
                &broken,
                &fingerprint,
                &[(&schema, "payload")]
            )
            .await,
        Err(Error::Database(_))
    ));
    wait_for(
        &pool,
        &format!(
            "SELECT pg_catalog.pg_try_advisory_xact_lock({}) AS ready",
            backend.advisory_lock_key()
        ),
    )
    .await;
    let rows = query(&pool, &format!("SELECT count(*) AS n FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema}'")).await;
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        1,
        "only the unrelated table may remain"
    );
    let rows = query(&pool, &format!("SELECT value FROM \"{schema}\".unrelated")).await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 7);
    backend
        .up(
            pool.as_ref(),
            "app",
            &migrations,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn concurrent_runners_execute_each_migration_once() {
    let pool = pool();
    let schema = schema("two_runners");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    let objects = [(&*schema, "payload")];
    let (first, second) = tokio::join!(
        backend.up(pool.as_ref(), "app", &migrations, &fingerprint, &objects),
        backend.up(pool.as_ref(), "app", &migrations, &fingerprint, &objects),
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn down_script_failure_rolls_back_preceding_down_step() {
    let pool = pool();
    let schema = schema("down_syntax");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    let first = &migrations.as_slice()[0];
    let broken_down = Migration::new(
        1,
        first.name(),
        first.up(),
        Some(format!("{}; SELECT (;", first.down().unwrap())),
    )
    .unwrap();
    let migrations = Migrations::new(vec![broken_down, migrations.as_slice()[1].clone()]).unwrap();
    let objects = [(&*schema, "payload")];
    let before = backend
        .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &migrations, &fingerprint, 2, &objects)
            .await,
        Err(Error::Database(_))
    ));
    wait_for(
        &pool,
        &format!(
            "SELECT pg_catalog.pg_try_advisory_xact_lock({}) AS ready",
            backend.advisory_lock_key()
        ),
    )
    .await;
    assert_eq!(
        before,
        backend
            .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
            .await
            .unwrap()
    );
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn definition_and_binding_drift_fail_before_up_or_down_effects() {
    let pool = pool();
    let schema = schema("drift");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    let objects = [(&*schema, "payload")];
    let before = backend
        .up(pool.as_ref(), "app", &migrations, &fingerprint, &objects)
        .await
        .unwrap();
    let first = &migrations.as_slice()[0];
    let changed = Migration::new(
        1,
        first.name(),
        format!("{} -- changed", first.up()),
        first.down().map(str::to_owned),
    )
    .unwrap();
    let changed = Migrations::new(vec![changed, migrations.as_slice()[1].clone()]).unwrap();
    assert!(matches!(
        backend
            .up(pool.as_ref(), "app", &changed, &fingerprint, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::Modified))
    ));
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &changed, &fingerprint, 1, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::Modified))
    ));
    assert!(matches!(
        backend
            .up(pool.as_ref(), "app", &migrations, "different", &objects)
            .await,
        Err(Error::History(HistoryReason::BindingMismatch))
    ));
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &migrations, "different", 1, &objects)
            .await,
        Err(Error::History(HistoryReason::BindingMismatch))
    ));
    assert_eq!(
        before,
        backend
            .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
            .await
            .unwrap()
    );
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn empty_registration_never_authorizes_untracked_relation_adoption() {
    let pool = pool();
    let schema = schema("no_adoption");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    let objects = [(&*schema, "payload")];
    backend
        .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
        .await
        .unwrap();
    script(&pool, &format!("CREATE TABLE \"{schema}\".payload (value bigint); INSERT INTO \"{schema}\".payload VALUES(99)")).await;
    for module in ["app", "never_registered"] {
        assert!(matches!(
            backend
                .up(pool.as_ref(), module, &migrations, &fingerprint, &objects)
                .await,
            Err(Error::UntrackedObjects)
        ));
    }
    let rows = query(&pool, &format!("SELECT value FROM \"{schema}\".payload")).await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 99);
    let rows = query(
        &pool,
        &format!("SELECT applied_count FROM \"{schema}\".history_modules"),
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>("applied_count").unwrap(), 0);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn cancellation_mid_script_releases_locks_and_rolls_back_earlier_effects() {
    let pool = pool();
    let schema = schema("cancel_scripts");
    let backend = Arc::new(backend(&schema));
    let (migrations, fingerprint) = definitions(&schema);
    backend
        .initialize_module(pool.as_ref(), "app", &fingerprint, &[(&schema, "payload")])
        .await
        .unwrap();
    let second = &migrations.as_slice()[1];
    let slow = Migration::new(
        2,
        second.name(),
        format!("{}; SELECT pg_catalog.pg_sleep(2)", second.up()),
        second.down().map(str::to_owned),
    )
    .unwrap();
    let slow = Migrations::new(vec![migrations.as_slice()[0].clone(), slow]).unwrap();
    let worker_pool = Arc::clone(&pool);
    let worker_backend = Arc::clone(&backend);
    let worker_schema = schema.clone();
    let worker_fingerprint = fingerprint.clone();
    let task = tokio::spawn(async move {
        worker_backend
            .up(
                worker_pool.as_ref(),
                "app",
                &slow,
                &worker_fingerprint,
                &[(&worker_schema, "payload")],
            )
            .await
    });
    wait_for(
        &pool,
        &format!(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE wait_event='PgSleep'
         AND query LIKE '%\"{schema}\".%') AS ready"
        ),
    )
    .await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    wait_for(
        &pool,
        &format!(
            "SELECT pg_catalog.pg_try_advisory_xact_lock({}) AS ready",
            backend.advisory_lock_key()
        ),
    )
    .await;
    let state = backend
        .initialize_module(pool.as_ref(), "app", &fingerprint, &[(&schema, "payload")])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 0);
    let rows = query(
        &pool,
        &format!("SELECT pg_catalog.to_regclass('\"{schema}\".payload') IS NULL AS absent"),
    )
    .await;
    assert!(rows[0].get::<bool>("absent").unwrap());
    backend
        .up(
            pool.as_ref(),
            "app",
            &migrations,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    assert_two(&pool, &schema).await;
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn lost_commit_completion_is_never_success_or_automatically_retried() {
    let observer = pool();
    for down in [false, true] {
        let schema = schema(if down { "unknown_down" } else { "unknown_up" });
        let backend = Arc::new(backend(&schema));
        let (migrations, fingerprint) = definitions(&schema);
        if down {
            backend
                .up(
                    observer.as_ref(),
                    "app",
                    &migrations,
                    &fingerprint,
                    &[(&schema, "payload")],
                )
                .await
                .unwrap();
        }
        let mut proxy = commit_proxy::Proxy::start().await;
        let pool = pool_at(proxy.port);
        let runner_pool = Arc::clone(&pool);
        let runner_backend = Arc::clone(&backend);
        let runner_schema = schema.clone();
        let runner_migrations = migrations.clone();
        let runner_fingerprint = fingerprint.clone();
        let runner = tokio::spawn(async move {
            let objects = [(&*runner_schema, "payload")];
            if down {
                runner_backend
                    .down(
                        runner_pool.as_ref(),
                        "app",
                        &runner_migrations,
                        &runner_fingerprint,
                        1,
                        &objects,
                    )
                    .await
            } else {
                runner_backend
                    .up(
                        runner_pool.as_ref(),
                        "app",
                        &runner_migrations,
                        &runner_fingerprint,
                        &objects,
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), &mut proxy.observed)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !runner.is_finished(),
            "unconfirmed COMMIT must not be accepted"
        );
        let rows = query(
            &observer,
            &format!("SELECT applied_count FROM \"{schema}\".history_modules"),
        )
        .await;
        assert_eq!(
            rows[0].get::<i64>("applied_count").unwrap(),
            if down { 1 } else { 2 }
        );
        proxy.stop().await; // Lose the actual provider's COMMIT completion, not a fake trait error.
        assert!(
            matches!(runner.await.unwrap(), Err(Error::Database(error)) if error.kind() == ErrorKind::CommitOutcomeUnknown)
        );
        let rows = query(
            &observer,
            &format!("SELECT count(*) AS n FROM \"{schema}\".payload"),
        )
        .await;
        assert_eq!(rows[0].get::<i64>("n").unwrap(), if down { 1 } else { 2 });
        backend
            .up(
                observer.as_ref(),
                "app",
                &migrations,
                &fingerprint,
                &[(&schema, "payload")],
            )
            .await
            .unwrap();
        assert_two(&observer, &schema).await;
        finish(&pool).await;
    }
    finish(&observer).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn down_of_unregistered_module_has_no_bootstrap_or_application_effects() {
    let pool = pool();
    let schema = schema("fresh_down");
    let backend = backend(&schema);
    let (migrations, fingerprint) = definitions(&schema);
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &migrations, &fingerprint, 0, &[])
            .await,
        Err(Error::History(HistoryReason::UnregisteredModule))
    ));
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM pg_catalog.pg_namespace WHERE nspname='{schema}'"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("n").unwrap(), 0);
    finish(&pool).await;
}
