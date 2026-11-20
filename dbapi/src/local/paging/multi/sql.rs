//! Local-future caller-authored SQL sources.
//!
//! This mirrors [`crate::paging::multi::sql`] for `!Send` providers. The plan and its continuation
//! may hold `Rc` or another single-threaded value; no `Send` assertion is made. Provider SQL,
//! typed parameters, continuation scope, bounds, completion, and no-retry behavior are the same.
//! D1 or another local provider still owns its own live support and fails closed where the
//! provider contract has no safe local operation; this module adds no D1 claim.
//!
//! The [root module documentation](crate::paging::multi::sql) describes the plan contract and the
//! provider-specific placeholder examples. This module only substitutes local futures, a local
//! [`Session`], and a local [`Continuation`]. The root retry rule applies: an explicit enabled
//! `RetrySetting::Use(...)` or retry runtime is rejected, `RetrySetting::Off` is accepted, and the
//! request is normalized to `Off`.

use std::fmt;

use super::{Continuation, PageRequest, PageSource, SourcePage, read_single_set};
use crate::local::{DbFuture, Session};
use crate::{
    DbError, ErrorKind, LimitSetting, QueryOptions, RetryPolicy, RetrySetting, Row, RowLimit,
};

pub use crate::paging::multi::sql::SqlPageAssertion;

/// A local caller-owned plan for one provider-specific SQL page request.
///
/// The first call receives `None`; later calls receive the token returned by the previous page. The
/// plan must author the native statement, typed parameters, keyset/offset logic, and continuation
/// scope. It is not required to be [`Send`]. The statement must be a single-result-set statement:
/// the reader contract reports a no-row batch as one empty set, so the adapter cannot prove that the
/// provider sent zero sets.
///
/// [`options`](SqlPagePlan::options) receives the effective lookahead count and must bind it as the
/// statement's row limit. The adapter owns the hard [`RowLimit`] and rejects an explicit enabled
/// retry. [`next_continuation`](SqlPagePlan::next_continuation) must return `Some` exactly when the
/// lookahead row exists.
pub trait SqlPagePlan {
    /// Returns the complete provider-specific statement.
    fn sql(&self) -> &str;

    /// Builds the typed parameters and options for one request.
    ///
    /// # Errors
    /// Returns a sanitized error when a continuation is invalid or the request cannot be planned.
    fn options(
        &mut self,
        request: PageRequest,
        lookahead: usize,
        continuation: Option<Continuation>,
    ) -> Result<QueryOptions, DbError>;

    /// Derives the continuation for the next request from the published page.
    ///
    /// # Errors
    /// Returns a sanitized error when no valid continuation can be derived.
    fn next_continuation(
        &mut self,
        rows: &[Row],
        has_lookahead: bool,
    ) -> Result<Option<Continuation>, DbError>;
}

/// A local [`PageSource`] over a caller-authored SQL statement and an exclusive session.
///
/// The session and plan may borrow caller state for `'session`; the source does not require
/// `'static` or `Send`.
pub struct SqlPageSource<'session, S> {
    session: Box<dyn Session + 'session>,
    plan: S,
    sql: String,
    source_bytes: usize,
}

impl<'session, S: SqlPagePlan> SqlPageSource<'session, S> {
    /// Creates a source over `session` and a caller's plan.
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
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage> {
        Box::pin(async move {
            let effective = request.page_size().get().min(request.remaining_rows());
            if effective == 0 {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            let lookahead = effective
                .checked_add(1)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;

            let options = self.plan.options(request, lookahead, continuation)?;
            let options = prepare_options(options, lookahead, self.source_bytes)?;

            let reader = self.session.fetch_async(&self.sql, options).await?;
            let mut rows = read_single_set(reader).await?;

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
        })
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
