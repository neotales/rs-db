use crate::{Error, HistoryReason, ModuleState, ledger::Ledger};
use neotales_dbapi::{QueryOptions, Transaction};
use neotales_migration::{Applied, FlattenManifest, HistoryState, Migration, Migrations};

#[derive(Clone, Copy)]
pub(super) enum Operation<'a> {
    Initialize,
    Up(&'a Migrations),
    Down(&'a Migrations, usize),
    Flatten(&'a FlattenManifest),
}

impl Operation<'_> {
    pub(super) fn validate(self) -> Result<(), Error> {
        match self {
            Self::Initialize => Ok(()),
            Self::Up(migrations) | Self::Down(migrations, _) => validate_migrations(migrations),
            Self::Flatten(manifest) => {
                validate_migrations(manifest.legacy())?;
                validate_definition(manifest.baseline())
            }
        }
    }

    pub(super) async fn apply(
        self,
        transaction: &mut (dyn Transaction<'_> + '_),
        ledger: &Ledger<'_>,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        if let Self::Flatten(manifest) = self {
            return crate::flatten::apply(
                transaction,
                ledger,
                module,
                manifest,
                fingerprint,
                managed_objects,
            )
            .await;
        }
        self.apply_migrations(transaction, ledger, module, fingerprint, managed_objects)
            .await
    }

    async fn apply_migrations(
        self,
        transaction: &mut (dyn Transaction<'_> + '_),
        ledger: &Ledger<'_>,
        module: &str,
        fingerprint: &str,
        managed_objects: &[(&str, &str)],
    ) -> Result<ModuleState, Error> {
        let snapshot = match self {
            Self::Initialize | Self::Up(_) => {
                ledger
                    .register(transaction, module, fingerprint, managed_objects)
                    .await?
            }
            Self::Down(_, _) => ledger
                .module_state(transaction, module, fingerprint)
                .await?
                .ok_or(Error::History(HistoryReason::UnregisteredModule))?,
            Self::Flatten(_) => unreachable!("handled by apply"),
        };
        let (Self::Up(migrations) | Self::Down(migrations, _)) = self else {
            return Ok(snapshot);
        };
        // A registered-but-empty module still has no tracked application objects.
        // Do not let CREATE IF NOT EXISTS in a first script adopt external state.
        if matches!(self, Self::Up(_)) && snapshot.applied_count() == 0 {
            ledger
                .reject_untracked(transaction, managed_objects)
                .await?;
        }
        let applied = ledger
            .read_applied(
                transaction,
                module,
                snapshot.applied_count(),
                migrations.as_slice().len(),
            )
            .await?;
        let history = validated_state(migrations, applied, fingerprint)?;
        let (plans, remaining) = match self {
            Self::Up(_) => (
                migrations.plan_up(&history, fingerprint),
                migrations.as_slice().len(),
            ),
            Self::Down(_, count) => (
                migrations.plan_down(&history, fingerprint, count),
                history.applied().len().saturating_sub(count),
            ),
            Self::Initialize | Self::Flatten(_) => unreachable!("handled above"),
        };
        // Planning is complete before the first SQL effect, including every down
        // script/barrier check. No scripts are replayed on any failure.
        let plans = plans.map_err(Error::Migration)?;
        for (index, plan) in plans.iter().enumerate() {
            transaction
                .execute_trusted_script_async(&plan.sql, QueryOptions::new())
                .await
                .map_err(Error::Database)?;
            match self {
                Self::Up(_) => {
                    let definition = &migrations.as_slice()[history.applied().len() + index];
                    ledger
                        .append(transaction, module, fingerprint, definition)
                        .await?;
                }
                Self::Down(_, _) => ledger.remove(transaction, module, plan.version).await?,
                Self::Initialize | Self::Flatten(_) => unreachable!("handled above"),
            }
        }
        let next: Vec<_> = migrations.as_slice()[..remaining]
            .iter()
            .map(|migration| Applied {
                version: migration.version(),
                checksum: migration.checksum().to_owned(),
            })
            .collect();
        ledger.verify_tables(transaction).await?;
        ledger.write_head(transaction, module, &next).await?;
        ledger.verify_history(transaction).await?;
        let snapshot = ledger
            .module_state(transaction, module, fingerprint)
            .await?
            .ok_or(Error::History(HistoryReason::InconsistentRows))?;
        let actual = ledger
            .read_applied(
                transaction,
                module,
                snapshot.applied_count(),
                migrations.as_slice().len(),
            )
            .await?;
        validated_state(migrations, actual, fingerprint)?;
        Ok(snapshot)
    }
}

fn validate_migrations(migrations: &Migrations) -> Result<(), Error> {
    migrations
        .as_slice()
        .iter()
        .try_for_each(validate_definition)
}

fn validate_definition(migration: &Migration) -> Result<(), Error> {
    if migration.version() == 0
        || i64::try_from(migration.version()).is_err()
        || migration.up().contains('\0')
        || migration.down().is_some_and(|sql| sql.contains('\0'))
    {
        return Err(Error::InvalidDefinitions);
    }
    Ok(())
}

pub(super) fn validated_state(
    migrations: &Migrations,
    applied: Vec<Applied>,
    fingerprint: &str,
) -> Result<HistoryState, Error> {
    // V1 has no independent mutable barrier column. The core's baseline marker is
    // part of the definition checksum, so a validated first applied baseline pins
    // the barrier. Removing the marker changes the checksum and fails validation
    // *before* this fallback; no ledger schema change or implicit adoption occurs.
    match migrations.validate_state(applied.clone(), fingerprint.to_owned(), None) {
        Err(neotales_migration::Error::InvalidBaseline) => {
            let barrier = applied.first().cloned();
            migrations
                .validate_state(applied, fingerprint.to_owned(), barrier)
                .map_err(Error::Migration)
        }
        result => result.map_err(Error::Migration),
    }
}
