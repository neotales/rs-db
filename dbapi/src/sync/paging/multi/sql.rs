//! Native blocking caller-authored SQL sources.
//!
//! This mirrors [`crate::paging::multi::sql`] for the synchronous traits. A source owns an
//! exclusive blocking [`Session`], authors one provider-specific statement per
//! request, and enforces the same hard row/byte bound, single-result-set completion, lookahead, and
//! no-retry rules. No runtime, `block_on`, task, prefetch, or replay is created.
//!
//! The plan trait is the root [`SqlPagePlan`] because the synchronous pager reuses the root
//! [`Continuation`] and [`SourcePage`]. The root retry rule applies: an explicit enabled
//! `RetrySetting::Use(...)` or retry runtime is rejected, `RetrySetting::Off` is accepted, and the
//! request is normalized to `Off`.

use std::fmt;

use super::{Continuation, PageRequest, PageSource, SourcePage, read_single_set};
use crate::sync::Session;
use crate::{
    DbError, ErrorKind, LimitSetting, QueryOptions, RetryPolicy, RetrySetting, Row, RowLimit,
};

pub use crate::paging::multi::sql::{SqlPageAssertion, SqlPagePlan};

/// A synchronous [`PageSource`] over a caller-authored SQL statement and an exclusive session.
///
/// The session and plan may borrow caller state for `'session`; the source does not require
/// `'static`.
pub struct SqlPageSource<'session, S> {
    session: Box<dyn Session + 'session>,
    plan: S,
    sql: String,
    source_bytes: usize,
}

impl<'session, S: SqlPagePlan> SqlPageSource<'session, S> {
    /// Creates a blocking source over `session` and a caller's plan.
    ///
    /// `source_bytes` is the non-zero portable encoded-byte allowance for the rows accepted by one
    /// request, including the lookahead row. It is not a provider allocation or buffering bound.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when `source_bytes` is zero.
    pub fn new(
        session: Box<dyn Session + 'session>,
        plan: S,
        source_bytes: usize,
        _assertion: SqlPageAssertion,
    ) -> Result<Self, DbError> {
        if source_bytes == 0 {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        let sql = plan.sql().to_owned();
        Ok(Self {
            session,
            plan,
            sql,
            source_bytes,
        })
    }
}

impl<S> fmt::Debug for SqlPageSource<'_, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlPageSource")
            .field("source_bytes", &self.source_bytes)
            .finish_non_exhaustive()
    }
}

impl<S: SqlPagePlan> PageSource for SqlPageSource<'_, S> {
    fn next_page(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> Result<SourcePage, DbError> {
        let effective = request.page_size().get().min(request.remaining_rows());
        if effective == 0 {
            return Err(DbError::new(ErrorKind::Overflow));
        }
        let lookahead = effective
            .checked_add(1)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;

        let options = self.plan.options(request, lookahead, continuation)?;
        let options = prepare_options(options, lookahead, self.source_bytes)?;

        let reader = self.session.fetch(&self.sql, options)?;
        let mut rows = read_single_set(reader)?;

        if rows.len() > lookahead {
            return Err(DbError::new(ErrorKind::Overflow));
        }
        if encoded_bytes(&rows)? > self.source_bytes {
            return Err(DbError::new(ErrorKind::Overflow));
        }

        let has_lookahead = rows.len() > effective;
        if has_lookahead {
            rows.truncate(effective);
        }
        if encoded_bytes(&rows)? > request.remaining_bytes() {
            return Err(DbError::new(ErrorKind::Overflow));
        }

        let next = self.plan.next_continuation(&rows, has_lookahead)?;
        if has_lookahead != next.is_some() {
            return Err(DbError::new(ErrorKind::Decode));
        }
        Ok(SourcePage::new(rows, next))
    }
}

fn prepare_options(
    mut options: QueryOptions,
    lookahead: usize,
    source_bytes: usize,
) -> Result<QueryOptions, DbError> {
    if !matches!(options.limit, LimitSetting::Inherit) {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    if matches!(
        options.retry,
        RetrySetting::Use(RetryPolicy::Enabled { .. })
    ) || options.retry_runtime.is_some()
    {
        return Err(DbError::new(ErrorKind::Unsupported));
    }
    options.limit = LimitSetting::Set(RowLimit::new(lookahead, source_bytes)?);
    options.retry = RetrySetting::Off;
    Ok(options)
}

fn encoded_bytes(rows: &[Row]) -> Result<usize, DbError> {
    rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))
    })
}
