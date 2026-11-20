//! Immutable public manifest views used to bound backend reads and apply fresh baselines.
use neotales_migration::{FlattenManifest, Migration, Migrations};

#[test]
fn manifest_views_preserve_the_complete_legacy_and_marked_baseline_identities() {
    let old = vec![
        Migration::new(1, "one", "UP ONE", Some("DOWN ONE".to_owned())).unwrap(),
        Migration::new(2, "two", "UP TWO", None).unwrap(),
    ];
    let baseline = Migration::new(3, "baseline", "BASELINE", None)
        .unwrap()
        .into_baseline();
    let manifest = FlattenManifest::new(old.clone(), baseline.clone()).unwrap();
    assert_eq!(manifest.legacy(), &Migrations::new(old).unwrap());
    assert_eq!(manifest.baseline(), &baseline);
}
