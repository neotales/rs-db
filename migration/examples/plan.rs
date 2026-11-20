//! Build a migration plan without connecting to a database.
use neotales_migration::{Dialect, Identifier, Migration, Migrations, bind_identifiers};
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bindings = BTreeMap::from([("table".to_owned(), Identifier::new("events")?)]);
    let (up, fingerprint) = bind_identifiers(
        "CREATE TABLE {{table}} (id INTEGER); INSERT INTO {{table}} VALUES (1)",
        &bindings,
        Dialect::Sqlite,
    )?;
    let migration = Migration::new(1, "events", up, Some("DROP TABLE \"events\"".into()))?;
    let migrations = Migrations::new(vec![migration])?;
    let history = migrations.validate_state(vec![], fingerprint.clone(), None)?;
    let plan = migrations.plan_up(&history, &fingerprint)?;
    assert_eq!(plan.len(), 1);
    println!("{}", plan[0].sql);
    Ok(())
}
