//! Experimental Cloudflare D1 adapter over [`neotales_dbapi::local`].
//!
//! **Experimental.** All `local::Pool` database operations fail closed before dispatch because
//! isolation after cancellation or an uncertain outcome is not established. D1 also materializes
//! raw results before Rust can enforce DBAPI hard bounds, so bounded fetch is unavailable.
//!
//! The D1-specific `execute_uncertain_async` and `batch_uncertain_async` are explicitly outside
//! the DBAPI session contract. Dropping a dispatched future does not cancel its promise; rejection
//! or dropping it leaves the outcome unknown. No binding-wide coordination or exclusivity is
//! provided, including across clones. Callers must independently reconcile and control retries.
//! Cloudflare documents batch as atomic, but that is not an interactive transaction. A local
//! Miniflare diagnostic does not establish hosted behavior.
//!
//! [`paging`] is a separate, explicitly experimental D1-only requery paging capability. One fixed,
//! caller-supplied statement is re-executed with caller-supplied bindings and a caller-asserted
//! read-only and replay-safe contract. Row, page, and byte budgets are enforced after D1 materializes
//! a page, before it is published; they are publication bounds, not pre-materialization allocation
//! limits. It does not implement a DBAPI reader, session, transaction, or `PageSource`, does not make
//! the fail-closed contract succeed, and makes no cancellation claim. See its module documentation.
//!
//! Parameters use positional `?` or `?NNN`. Integers outside JavaScript's safe range and integral
//! floating-point parameters (including signed zero) are rejected. JavaScript cannot distinguish
//! an integral `f64` from an `i64`, and local D1 testing also observed integer parameters stored
//! as SQLite REAL in a column without affinity. Consequently the uncertain API does not promise
//! an exact SQL numeric storage class. [`numeric_to_f64`] converts values obtained separately,
//! but cannot recover type or precision already lost before Rust sees them.
//!
//! # Compile check
//!
//! ```text
//! cargo check --target wasm32-unknown-unknown -p neotales-dbapi-d1
//! ```
//!
//! No D1 database, binding, credential, or live request is needed. The D1 binding dispatcher is
//! absent on a non-wasm target so the rest of the workspace can keep building normally. The value
//! and placeholder helpers and the [`paging`] continuation and bound types remain available on the
//! host, so shared keyset logic stays testable without a binding.

mod codec;

pub mod paging;

#[cfg(target_arch = "wasm32")]
mod d1;
#[cfg(target_arch = "wasm32")]
pub use d1::{D1BatchStatement, D1Pager, D1Pool};

/// Converts a D1 numeric value to `f64` without introducing integer conversion rounding.
///
/// This does not distinguish SQL REAL from INTEGER, or detect precision already lost before the
/// value reached Rust. Integral values outside JavaScript's safe integer range are rejected.
///
/// # Errors
/// Returns `Unsupported` for unsafe integral values and `WrongType` for non-numeric values.
#[allow(clippy::cast_precision_loss)] // Safe-integer bound checked before conversion.
pub fn numeric_to_f64(value: &neotales_dbapi::Value) -> Result<f64, neotales_dbapi::DbError> {
    use neotales_dbapi::{DbError, ErrorKind, Value};
    const MAX: i64 = 9_007_199_254_740_991;
    match value {
        Value::I64(n) if (-MAX..=MAX).contains(n) => Ok(*n as f64),
        Value::I64(_) => Err(DbError::new(ErrorKind::Unsupported)),
        Value::F64(n) if !n.is_finite() || (n.fract() == 0.0 && n.abs() > MAX as f64) => {
            Err(DbError::new(ErrorKind::Unsupported))
        }
        Value::F64(n) => Ok(*n),
        _ => Err(DbError::new(ErrorKind::WrongType)),
    }
}

#[cfg(test)]
mod numeric_tests {
    use super::numeric_to_f64;
    use neotales_dbapi::Value;

    #[test]
    fn explicit_numeric_conversion_rejects_unsafe_integers() {
        assert_eq!(
            numeric_to_f64(&Value::I64(1)).unwrap().to_bits(),
            1.0f64.to_bits()
        );
        assert_eq!(
            numeric_to_f64(&Value::F64(1.0)).unwrap().to_bits(),
            1.0f64.to_bits()
        );
        assert!(numeric_to_f64(&Value::I64(9_007_199_254_740_992)).is_err());
        assert!(numeric_to_f64(&Value::F64(9_007_199_254_740_992.0)).is_err());
    }
}
