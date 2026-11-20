use crate::{Error, HistoryReason, ModuleState, ledger::Ledger};
use neotales_dbapi::Transaction;
use neotales_migration::{FlattenManifest, PersistedHistory};

pub(super) async fn apply(
    transaction: &mut (dyn Transaction<'_> + '_),
    ledger: &Ledger<'_>,
    module: &str,
    manifest: &FlattenManifest,
    fingerprint: &str,
    managed_objects: &[(&str, &str)],
) -> Result<ModuleState, Error> {
    let snapshot = ledger
        .module_state(transaction, module, fingerprint)
        .await?;
    let Some(snapshot) = snapshot else {
        ledger
            .reject_untracked(transaction, managed_objects)
            .await?;
        return Err(Error::History(HistoryReason::UnregisteredModule));
    };
    if snapshot.applied_count() == 0 {
        ledger
            .reject_untracked(transaction, managed_objects)
            .await?;
        return Err(Error::Migration(neotales_migration::Error::FlattenDrift));
    }
    let applied = ledger
        .read_applied(
            transaction,
            module,
            snapshot.applied_count(),
            manifest.legacy().as_slice().len(),
        )
        .await?;
    // Manifest construction forbids legacy baselines. Its assessment validates
    // the complete legacy prefix, exact count and checksum identity, and binding.
    // Never use assess(None): that decision does not prove fresh SQL was applied.
    let persisted = PersistedHistory {
        applied,
        binding_fingerprint: snapshot.fingerprint().to_owned(),
        barrier: None,
    };
    let assessment = manifest
        .assess(Some(&persisted), fingerprint.to_owned())
        .map_err(Error::Migration)?;
    // Only ledger rows are mutated. No script executor is called, even if the
    // baseline SQL would fail against this already-initialized application.
    for applied in &persisted.applied {
        ledger.remove(transaction, module, applied.version).await?;
    }
    ledger
        .append(transaction, module, fingerprint, manifest.baseline())
        .await?;
    ledger
        .write_head(transaction, module, assessment.state.applied())
        .await?;
    ledger.verify_tables(transaction).await?;
    ledger.verify_history(transaction).await?;
    let next = ledger
        .module_state(transaction, module, fingerprint)
        .await?
        .ok_or(Error::History(HistoryReason::InconsistentRows))?;
    let rows = ledger
        .read_applied(transaction, module, next.applied_count(), 1)
        .await?;
    let baseline = neotales_migration::Migrations::new(vec![manifest.baseline().clone()])
        .map_err(Error::Migration)?;
    crate::execution::validated_state(&baseline, rows, fingerprint)?;
    Ok(next)
}
