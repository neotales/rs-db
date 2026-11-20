//! Proves that the vendored upstream tests compile against the quiet fork.
//!
//! Cargo's dependency rename makes the fork available under the upstream name,
//! so the upstream test sources stay byte-for-byte unchanged. This test closes
//! the gap where a future edit could point that rename at stock
//! `tokio-postgres` and silently turn the lane into upstream-vs-upstream.
//!
//! These public types exist only in the fork's `completion` module. Stock
//! `tokio-postgres` 0.7.18 has no equivalents, so a successful compile is the
//! assertion. The test body also exercises their `Debug` and equality surface.

use tokio_postgres::{BatchCompletion, CommandTag, TransactionStatus};

#[test]
fn dependency_is_the_quiet_fork() {
    let completion = BatchCompletion {
        commands: 1,
        tag: CommandTag::Commit,
        status: TransactionStatus::Idle,
        transaction_control: true,
    };

    assert_eq!(completion.tag, CommandTag::Commit);
    assert_eq!(completion.status, TransactionStatus::Idle);
    assert!(completion.transaction_control);
    assert!(format!("{completion:?}").contains("Commit"));
}
