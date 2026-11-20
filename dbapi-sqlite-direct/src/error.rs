//! Sanitized translation of rusqlite and SQLite failures into [`DbError`] categories.
//!
//! Driver messages, SQL text, bound values, paths, and VFS names are never retained. Each arm
//! returns a stable category only.

use neotales_dbapi::{DbError, ErrorKind};
use rusqlite::{Error as SqliteError, ffi::ErrorCode};

/// Maps a rusqlite error without retaining any driver text.
pub(crate) fn map_sqlite_error(error: &SqliteError) -> DbError {
    if let Some(sqlite) = error.sqlite_error() {
        return map_sqlite_code(sqlite.code, sqlite.extended_code);
    }
    match error {
        SqliteError::FromSqlConversionFailure(..)
        | SqliteError::IntegralValueOutOfRange(..)
        | SqliteError::Utf8Error(..)
        | SqliteError::InvalidColumnIndex(_)
        | SqliteError::InvalidColumnName(_)
        | SqliteError::InvalidColumnType(..)
        | SqliteError::StatementChangedRows(_) => DbError::new(ErrorKind::Decode),
        SqliteError::InvalidParameterName(_)
        | SqliteError::InvalidParameterCount(..)
        | SqliteError::ExecuteReturnedResults
        | SqliteError::NulError(_)
        | SqliteError::InvalidPath(_)
        | SqliteError::ToSqlConversionFailure(_)
        | SqliteError::InvalidQuery
        | SqliteError::MultipleStatement
        | SqliteError::SqliteSingleThreadedMode => DbError::new(ErrorKind::InvalidArgument),
        SqliteError::QueryReturnedNoRows => DbError::new(ErrorKind::NoRows),
        SqliteError::QueryReturnedMoreThanOneRow => DbError::new(ErrorKind::TooManyRows),
        _ => DbError::new(ErrorKind::Connection),
    }
}

fn map_sqlite_code(code: ErrorCode, extended_code: i32) -> DbError {
    // Every constraint failure has primary result code 19, including the extended subcodes.
    if extended_code & 0xff == 19 {
        return DbError::new(ErrorKind::Constraint);
    }
    let kind = match code {
        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => ErrorKind::Busy,
        ErrorCode::OperationInterrupted => ErrorKind::Cancelled,
        ErrorCode::ConstraintViolation => ErrorKind::Constraint,
        ErrorCode::TooBig => ErrorKind::Overflow,
        ErrorCode::TypeMismatch => ErrorKind::Decode,
        ErrorCode::ApiMisuse | ErrorCode::ParameterOutOfRange => ErrorKind::InvalidArgument,
        ErrorCode::AuthorizationForStatementDenied | ErrorCode::NoLargeFileSupport => {
            ErrorKind::Unsupported
        }
        _ => ErrorKind::Connection,
    };
    DbError::new(kind)
}

/// Maps a dispatched statement failure conservatively for a possible write.
///
/// A result the provider cannot establish must not become a retryable transport error. Busy and
/// constraint failures are known non-applications and keep their category; interrupted, connection,
/// timeout, and decode failures become [`ErrorKind::WriteOutcomeUnknown`].
pub(crate) fn map_write_error(error: &SqliteError) -> DbError {
    let mapped = map_sqlite_error(error);
    // `execute` on a statement with a result set (for example `INSERT ... RETURNING`) steps the
    // statement before reporting `ExecuteReturnedResults`, so the write may already have applied.
    if matches!(error, SqliteError::ExecuteReturnedResults)
        || matches!(
            mapped.kind(),
            ErrorKind::Connection
                | ErrorKind::Timeout
                | ErrorKind::Cancelled
                | ErrorKind::Decode
                | ErrorKind::Overflow
        )
    {
        DbError::new(ErrorKind::WriteOutcomeUnknown)
    } else {
        mapped
    }
}
