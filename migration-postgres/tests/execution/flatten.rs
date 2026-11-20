use super::{
    backend, commit_proxy, definitions, finish, pool, pool_at, query, schema, script, wait_for,
};
use neotales_dbapi::{ErrorKind, Row};
use neotales_dbapi_postgres::PostgresPool;
use neotales_migration::{FlattenManifest, Migration, Migrations};
use neotales_migration_postgres::{Error, HistoryReason, ModuleState, PostgresBackend};
use std::{sync::Arc, time::Duration};

fn manifest(legacy: &Migrations) -> FlattenManifest {
    // Correct fresh-schema SQL, but CREATE TABLE would fail against an existing
    // application. Successful flatten therefore proves this SQL was not executed.
    let up = legacy
        .as_slice()
        .iter()
        .map(Migration::up)
        .collect::<Vec<_>>()
        .join("\n");
    let baseline = Migration::new(
        3,
        "baseline",
        up,
        legacy.as_slice()[0].down().map(str::to_owned),
    )
    .unwrap()
    .into_baseline();
    FlattenManifest::new(legacy.as_slice().to_vec(), baseline).unwrap()
}

async fn application(pool: &PostgresPool, schema: &str) -> Vec<Row> {
    query(
        pool,
        &format!("SELECT id,value,active FROM \"{schema}\".payload ORDER BY id"),
    )
    .await
}

async fn history(pool: &PostgresPool, schema: &str) -> Vec<Row> {
    query(
        pool,
        &format!("SELECT * FROM \"{schema}\".history ORDER BY module,version"),
    )
    .await
}

async fn metadata(pool: &PostgresPool, schema: &str) -> Vec<Row> {
    query(
        pool,
        &format!("SELECT * FROM \"{schema}\".history_modules ORDER BY module"),
    )
    .await
}

async fn assert_baseline(
    pool: &PostgresPool,
    schema: &str,
    manifest: &FlattenManifest,
    fingerprint: &str,
) {
    let rows = history(pool, schema).await;
    let app: Vec<_> = rows
        .iter()
        .filter(|row| row.get::<String>("module").unwrap() == "app")
        .collect();
    assert_eq!(app.len(), 1);
    assert_eq!(app[0].get::<i64>("version").unwrap(), 3);
    assert_eq!(
        app[0].get::<String>("checksum").unwrap(),
        manifest.baseline().checksum()
    );
    assert_eq!(app[0].get::<String>("fingerprint").unwrap(), fingerprint);
    let state = backend(schema)
        .initialize_module(pool, "app", fingerprint, &[(schema, "payload")])
        .await
        .unwrap();
    assert_eq!(state.applied_count(), 1);
    assert_eq!(state.head(), Some((3, manifest.baseline().checksum())));
}

async fn create_other(pool: &PostgresPool, schema: &str) -> ModuleState {
    let definitions = Migrations::new(vec![Migration::new(1, "other",
        format!("CREATE TABLE \"{schema}\".other_payload (value bigint); INSERT INTO \"{schema}\".other_payload VALUES(42)"),
        None).unwrap()]).unwrap();
    backend(schema)
        .up(
            pool,
            "other",
            &definitions,
            "other-binding",
            &[(schema, "other_payload")],
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn exact_legacy_flattens_without_baseline_sql_or_application_changes() {
    let pool = pool();
    let schema = schema("flatten_exact");
    let backend = backend(&schema);
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    backend
        .up(
            pool.as_ref(),
            "app",
            &legacy,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    let other = create_other(&pool, &schema).await;
    let before = application(&pool, &schema).await;
    let oid = query(
        &pool,
        &format!("SELECT '\"{schema}\".payload'::regclass::oid::bigint AS oid"),
    )
    .await;
    let next = backend
        .flatten(
            pool.as_ref(),
            "app",
            &manifest,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    assert_eq!(next.applied_count(), 1);
    assert_baseline(&pool, &schema, &manifest, &fingerprint).await;
    assert_eq!(application(&pool, &schema).await, before);
    assert_eq!(
        query(
            &pool,
            &format!("SELECT '\"{schema}\".payload'::regclass::oid::bigint AS oid")
        )
        .await,
        oid
    );
    assert_eq!(
        backend
            .initialize_module(pool.as_ref(), "other", "other-binding", &[])
            .await
            .unwrap(),
        other
    );
    let rows = query(
        &pool,
        &format!("SELECT value FROM \"{schema}\".other_payload"),
    )
    .await;
    assert_eq!(rows[0].get::<i64>("value").unwrap(), 42);
    assert!(matches!(
        backend
            .flatten(
                pool.as_ref(),
                "app",
                &manifest,
                &fingerprint,
                &[(&schema, "payload")]
            )
            .await,
        Err(Error::Migration(neotales_migration::Error::HistoryDrift))
    ));
    let baseline_only = Migrations::new(vec![manifest.baseline().clone()]).unwrap();
    assert_eq!(
        next,
        backend
            .up(
                pool.as_ref(),
                "app",
                &baseline_only,
                &fingerprint,
                &[(&schema, "payload")]
            )
            .await
            .unwrap()
    );
    assert_eq!(application(&pool, &schema).await, before);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn flattened_barrier_blocks_complete_rollback_before_any_down_script() {
    let pool = pool();
    let schema = schema("flatten_barrier");
    let backend = backend(&schema);
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    let objects = [(&*schema, "payload")];
    backend
        .up(pool.as_ref(), "app", &legacy, &fingerprint, &objects)
        .await
        .unwrap();
    backend
        .flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects)
        .await
        .unwrap();
    let later = Migration::new(
        4,
        "later",
        format!("ALTER TABLE \"{schema}\".payload ADD COLUMN marker bigint"),
        Some(format!(
            "ALTER TABLE \"{schema}\".payload DROP COLUMN marker"
        )),
    )
    .unwrap();
    let current = Migrations::new(vec![manifest.baseline().clone(), later]).unwrap();
    let before = backend
        .up(pool.as_ref(), "app", &current, &fingerprint, &objects)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &current, &fingerprint, 2, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM \"{schema}\".payload WHERE marker IS NULL"),
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        2,
        "later down must not run before barrier rejection"
    );
    assert_eq!(
        before,
        backend
            .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
            .await
            .unwrap()
    );
    backend
        .down(pool.as_ref(), "app", &current, &fingerprint, 1, &objects)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .down(pool.as_ref(), "app", &current, &fingerprint, 1, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::Baseline))
    ));
    assert_baseline(&pool, &schema, &manifest, &fingerprint).await;
    assert_eq!(application(&pool, &schema).await.len(), 2);
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn partial_modified_unknown_and_binding_drift_never_replace_history() {
    let pool = pool();
    for scenario in ["partial", "modified", "version", "unknown", "binding"] {
        let schema = schema(&format!("flatten_drift_{scenario}"));
        let backend = backend(&schema);
        let (legacy, fingerprint) = definitions(&schema);
        let manifest = manifest(&legacy);
        let applied = if scenario == "partial" {
            Migrations::new(vec![legacy.as_slice()[0].clone()]).unwrap()
        } else {
            legacy.clone()
        };
        backend
            .up(
                pool.as_ref(),
                "app",
                &applied,
                &fingerprint,
                &[(&schema, "payload")],
            )
            .await
            .unwrap();
        if scenario == "modified" {
            script(
                &pool,
                &format!("UPDATE \"{schema}\".history SET checksum='modified' WHERE version=1"),
            )
            .await;
        } else if scenario == "version" {
            script(&pool, &format!("UPDATE \"{schema}\".history SET version=7 WHERE version=1;
                UPDATE \"{schema}\".history_modules SET head_version=7,
                    head_checksum=(SELECT checksum FROM \"{schema}\".history WHERE version=7) WHERE module='app'")).await;
        } else if scenario == "unknown" {
            script(&pool, &format!("INSERT INTO \"{schema}\".history VALUES('app',99,'unknown','{fingerprint}');
                UPDATE \"{schema}\".history_modules SET applied_count=3,head_version=99,head_checksum='unknown' WHERE module='app'")).await;
        }
        let before_history = history(&pool, &schema).await;
        let before_metadata = metadata(&pool, &schema).await;
        let before_rows = query(
            &pool,
            &format!("SELECT id,value FROM \"{schema}\".payload ORDER BY id"),
        )
        .await;
        let binding = if scenario == "binding" {
            "wrong-binding"
        } else {
            &fingerprint
        };
        let error = backend
            .flatten(
                pool.as_ref(),
                "app",
                &manifest,
                binding,
                &[(&schema, "payload")],
            )
            .await
            .unwrap_err();
        match scenario {
            "partial" => assert!(matches!(
                error,
                Error::Migration(neotales_migration::Error::FlattenDrift)
            )),
            "modified" => assert!(matches!(
                error,
                Error::Migration(neotales_migration::Error::Modified)
            )),
            "version" => assert!(matches!(
                error,
                Error::Migration(neotales_migration::Error::HistoryDrift)
            )),
            "unknown" => assert!(matches!(
                error,
                Error::Migration(neotales_migration::Error::UnknownApplied)
            )),
            "binding" => assert!(matches!(
                error,
                Error::History(HistoryReason::BindingMismatch)
            )),
            _ => unreachable!(),
        }
        assert_eq!(history(&pool, &schema).await, before_history);
        assert_eq!(metadata(&pool, &schema).await, before_metadata);
        assert_eq!(
            query(
                &pool,
                &format!("SELECT id,value FROM \"{schema}\".payload ORDER BY id")
            )
            .await,
            before_rows
        );
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn partial_or_unowned_ledger_is_never_adopted_by_flatten() {
    let pool = pool();
    for partial in [false, true] {
        let schema = schema(if partial {
            "flatten_pair"
        } else {
            "flatten_unowned"
        });
        let backend = backend(&schema);
        let (legacy, fingerprint) = definitions(&schema);
        let manifest = manifest(&legacy);
        backend
            .up(
                pool.as_ref(),
                "app",
                &legacy,
                &fingerprint,
                &[(&schema, "payload")],
            )
            .await
            .unwrap();
        let before = application(&pool, &schema).await;
        let before_history = history(&pool, &schema).await;
        script(
            &pool,
            &if partial {
                format!("DROP TABLE \"{schema}\".history_modules")
            } else {
                format!("COMMENT ON TABLE \"{schema}\".history IS NULL")
            },
        )
        .await;
        let error = backend
            .flatten(
                pool.as_ref(),
                "app",
                &manifest,
                &fingerprint,
                &[(&schema, "payload")],
            )
            .await
            .unwrap_err();
        let expected = if partial {
            HistoryReason::PartialLedger
        } else {
            HistoryReason::UnownedLedger
        };
        assert!(matches!(error, Error::History(reason) if reason == expected));
        assert_eq!(application(&pool, &schema).await, before);
        assert_eq!(history(&pool, &schema).await, before_history);
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn fresh_or_empty_flatten_never_records_unexecuted_baseline() {
    let pool = pool();
    let schema = schema("flatten_fresh");
    let backend = backend(&schema);
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    let objects = [(&*schema, "payload")];
    assert!(matches!(
        backend
            .flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects)
            .await,
        Err(Error::History(HistoryReason::UnregisteredModule))
    ));
    let rows = query(
        &pool,
        &format!("SELECT count(*) AS n FROM pg_catalog.pg_namespace WHERE nspname='{schema}'"),
    )
    .await;
    assert_eq!(
        rows[0].get::<i64>("n").unwrap(),
        0,
        "fresh rejection must roll back bootstrap"
    );
    backend
        .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
        .await
        .unwrap();
    let before = metadata(&pool, &schema).await;
    assert!(matches!(
        backend
            .flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::FlattenDrift))
    ));
    assert_eq!(metadata(&pool, &schema).await, before);
    assert!(history(&pool, &schema).await.is_empty());
    let fresh = Migrations::new(vec![manifest.baseline().clone()]).unwrap();
    backend
        .up(pool.as_ref(), "app", &fresh, &fingerprint, &objects)
        .await
        .unwrap();
    assert_baseline(&pool, &schema, &manifest, &fingerprint).await;
    assert_eq!(
        application(&pool, &schema).await.len(),
        2,
        "only up may execute fresh baseline SQL"
    );
    assert!(matches!(
        backend
            .flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects)
            .await,
        Err(Error::Migration(neotales_migration::Error::HistoryDrift))
    ));
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn untracked_application_state_is_not_adopted_for_fresh_or_empty_flatten() {
    let pool = pool();
    for registered in [false, true] {
        let schema = schema(if registered {
            "flatten_empty"
        } else {
            "flatten_untracked"
        });
        let backend = backend(&schema);
        let (legacy, fingerprint) = definitions(&schema);
        let manifest = manifest(&legacy);
        let objects = [(&*schema, "payload")];
        if registered {
            backend
                .initialize_module(pool.as_ref(), "app", &fingerprint, &objects)
                .await
                .unwrap();
        } else {
            script(&pool, &format!("CREATE SCHEMA \"{schema}\"")).await;
        }
        script(&pool, &format!("CREATE TABLE \"{schema}\".payload (value bigint); INSERT INTO \"{schema}\".payload VALUES(77)")).await;
        assert!(matches!(
            backend
                .flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects)
                .await,
            Err(Error::UntrackedObjects)
        ));
        let rows = query(&pool, &format!("SELECT value FROM \"{schema}\".payload")).await;
        assert_eq!(rows[0].get::<i64>("value").unwrap(), 77);
        if registered {
            assert!(history(&pool, &schema).await.is_empty());
            let state = backend
                .initialize_module(pool.as_ref(), "app", &fingerprint, &[])
                .await
                .unwrap();
            assert_eq!(state.applied_count(), 0);
        } else {
            let rows = query(
                &pool,
                &format!("SELECT pg_catalog.to_regclass('\"{schema}\".history') IS NULL AS absent"),
            )
            .await;
            assert!(rows[0].get::<bool>("absent").unwrap());
        }
    }
    finish(&pool).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn concurrent_flatten_has_one_winner_and_never_reflattens_the_baseline() {
    let pool = pool();
    let schema = schema("flatten_concurrent");
    let backend = backend(&schema);
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    let objects = [(&*schema, "payload")];
    backend
        .up(pool.as_ref(), "app", &legacy, &fingerprint, &objects)
        .await
        .unwrap();
    let before = application(&pool, &schema).await;
    let (first, second) = tokio::join!(
        backend.flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects),
        backend.flatten(pool.as_ref(), "app", &manifest, &fingerprint, &objects),
    );
    assert!(matches!(
        (&first, &second),
        (
            Ok(_),
            Err(Error::Migration(neotales_migration::Error::HistoryDrift))
        ) | (
            Err(Error::Migration(neotales_migration::Error::HistoryDrift)),
            Ok(_)
        )
    ));
    assert_baseline(&pool, &schema, &manifest, &fingerprint).await;
    assert_eq!(application(&pool, &schema).await, before);
    finish(&pool).await;
}

fn start_runner(
    pool: Arc<PostgresPool>,
    backend: Arc<PostgresBackend>,
    schema: String,
    manifest: FlattenManifest,
    fingerprint: String,
) -> tokio::task::JoinHandle<Result<ModuleState, Error>> {
    tokio::spawn(async move {
        backend
            .flatten(
                pool.as_ref(),
                "app",
                &manifest,
                &fingerprint,
                &[(&schema, "payload")],
            )
            .await
    })
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn cancellation_during_replacement_rolls_back_deleted_legacy_rows() {
    let observer = pool();
    let schema = schema("flatten_cancel");
    let backend = Arc::new(backend(&schema));
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    backend
        .up(
            observer.as_ref(),
            "app",
            &legacy,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    let before_history = history(&observer, &schema).await;
    let before_metadata = metadata(&observer, &schema).await;
    let before_rows = application(&observer, &schema).await;
    let mut proxy = commit_proxy::Proxy::hold_delete().await;
    let pool = pool_at(proxy.port);
    let runner = start_runner(
        Arc::clone(&pool),
        Arc::clone(&backend),
        schema.clone(),
        manifest.clone(),
        fingerprint.clone(),
    );
    tokio::time::timeout(Duration::from_secs(2), &mut proxy.observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!runner.is_finished());
    assert_eq!(
        history(&observer, &schema).await,
        before_history,
        "an uncommitted DELETE must not leak"
    );
    runner.abort();
    assert!(runner.await.unwrap_err().is_cancelled());
    proxy.stop().await;
    wait_for(
        &observer,
        &format!(
            "SELECT pg_catalog.pg_try_advisory_xact_lock({}) AS ready",
            backend.advisory_lock_key()
        ),
    )
    .await;
    assert_eq!(history(&observer, &schema).await, before_history);
    assert_eq!(metadata(&observer, &schema).await, before_metadata);
    assert_eq!(application(&observer, &schema).await, before_rows);
    backend
        .flatten(
            observer.as_ref(),
            "app",
            &manifest,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    assert_baseline(&observer, &schema, &manifest, &fingerprint).await;
    assert_eq!(application(&observer, &schema).await, before_rows);
    finish(&pool).await;
    finish(&observer).await;
}

#[tokio::test]
#[ignore = "requires tests/live.sh disposable PostgreSQL 17"]
async fn lost_flatten_commit_is_unknown_even_when_observer_sees_baseline() {
    let observer = pool();
    let schema = schema("flatten_unknown");
    let backend = Arc::new(backend(&schema));
    let (legacy, fingerprint) = definitions(&schema);
    let manifest = manifest(&legacy);
    backend
        .up(
            observer.as_ref(),
            "app",
            &legacy,
            &fingerprint,
            &[(&schema, "payload")],
        )
        .await
        .unwrap();
    let before = application(&observer, &schema).await;
    let mut proxy = commit_proxy::Proxy::start().await;
    let pool = pool_at(proxy.port);
    let runner = start_runner(
        Arc::clone(&pool),
        Arc::clone(&backend),
        schema.clone(),
        manifest.clone(),
        fingerprint.clone(),
    );
    tokio::time::timeout(Duration::from_secs(2), &mut proxy.observed)
        .await
        .unwrap()
        .unwrap();
    assert!(!runner.is_finished());
    assert_baseline(&observer, &schema, &manifest, &fingerprint).await;
    proxy.stop().await;
    assert!(
        matches!(runner.await.unwrap(), Err(Error::Database(error)) if error.kind() == ErrorKind::CommitOutcomeUnknown)
    );
    assert_eq!(application(&observer, &schema).await, before);
    assert!(matches!(
        backend
            .flatten(
                observer.as_ref(),
                "app",
                &manifest,
                &fingerprint,
                &[(&schema, "payload")]
            )
            .await,
        Err(Error::Migration(neotales_migration::Error::HistoryDrift))
    ));
    assert_baseline(&observer, &schema, &manifest, &fingerprint).await;
    finish(&pool).await;
    finish(&observer).await;
}
