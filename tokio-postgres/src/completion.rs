//! Coarse, non-sensitive command and transaction completion metadata.

use crate::Error;

/// Transaction status from an authoritative ReadyForQuery message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionStatus {
    /// No transaction is active.
    Idle,
    /// A transaction is active.
    Active,
    /// A transaction is active but aborted.
    Failed,
}

impl TransactionStatus {
    pub(crate) fn parse(value: u8) -> Result<Self, Error> {
        match value {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::Active),
            b'E' => Ok(Self::Failed),
            _ => Err(Error::unexpected_message()),
        }
    }
}

/// Classified server CommandComplete tag; no original server string is retained.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CommandTag {
    /// BEGIN / START TRANSACTION completed.
    Begin,
    /// COMMIT completed (not an aborted transaction's ROLLBACK).
    Commit,
    /// ROLLBACK or ROLLBACK TO SAVEPOINT completed.
    Rollback,
    /// SAVEPOINT completed.
    Savepoint,
    /// RELEASE SAVEPOINT completed.
    Release,
    /// PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED.
    TwoPhase,
    /// Any non-transaction-control command, or no command yet.
    #[default]
    Other,
}

impl CommandTag {
    pub(crate) fn parse(value: &str) -> Self {
        match value {
            "BEGIN" => Self::Begin,
            "COMMIT" => Self::Commit,
            "ROLLBACK" => Self::Rollback,
            "SAVEPOINT" => Self::Savepoint,
            "RELEASE" => Self::Release,
            "PREPARE TRANSACTION" | "COMMIT PREPARED" | "ROLLBACK PREPARED" => Self::TwoPhase,
            _ => Self::Other,
        }
    }

    /// Whether this command manages transaction ownership or savepoints.
    pub fn is_transaction_control(self) -> bool {
        self != Self::Other
    }
}

/// Bounded summary produced only after a simple-query script reaches ReadyForQuery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchCompletion {
    /// Number of actual CommandComplete messages (empty queries count as zero).
    pub commands: u64,
    /// The last command's classified tag.
    pub tag: CommandTag,
    /// Final transaction status.
    pub status: TransactionStatus,
    /// Whether any command in the script managed transaction ownership/savepoints.
    pub transaction_control: bool,
}
