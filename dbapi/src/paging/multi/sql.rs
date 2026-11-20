//! Caller-authored SQL sources for the explicit multi-request pager.
//!
//! A [`SqlPageSource`] owns an exclusive DBAPI [`Session`] and a caller's [`SqlPagePlan`]. It
//! requests one page at a time, applies the plan's typed parameters, and publishes rows only after
//! the complete single-result-set request has finished. It does not rewrite SQL, classify a
//! statement as read-only, infer a continuation, retry, replay, prefetch, or create a task.
//!
//! The caller supplies:
//!
//! - the complete provider-specific statement, with its native placeholders;
//! - the typed parameters for each request, including the requested row limit;
//! - the continuation token and its scope, tenant binding, expiry, and validation;
//! - the [`SqlPageAssertion`] that the statement is read-only, replay-safe, and authorized.
//!
//! The adapter computes an effective page size of `min(page_size, remaining_rows)`, asks for exactly
//! one extra lookahead row, applies a hard [`RowLimit`] covering those rows and a caller-supplied
//! portable byte allowance, removes the lookahead before publication, and rejects a page whose
//! continuation state disagrees with the lookahead. The pager still enforces the aggregate
//! [`PageBudget`](super::PageBudget) and page size.
//!
//! # Provider SQL
//!
//! Nothing here rewrites the statement. The plan must author the provider's own form, for example:
//!
//! ```sql
//! -- SQLite and libSQL
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//!
//! -- PostgreSQL
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > ($1, $2) ORDER BY tenant, seq LIMIT $3;
//!
//! -- MySQL and MariaDB
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//!
//! -- SQL Server (row-value comparison is not available; expand the keyset predicate)
//! SELECT TOP (@P3) tenant, seq, body FROM events
//! WHERE tenant > @P1 OR (tenant = @P1 AND seq > @P2) ORDER BY tenant, seq;
//!
//! -- ClickHouse 24.8
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > ({p1:Int64}, {p2:Int64})
//! ORDER BY tenant, seq LIMIT {p3:Int64};
//! ```
//!
//! These are placeholder-shape examples, not live conformance claims. Only the SQLite form is
//! exercised against a real server in this tree; the other providers may have different ordering,
//! limit, or type requirements that the caller must confirm for its schema.
//!
//! # Bounds, completion, and replay
//!
//! The plan must return at most `lookahead` rows in a total order. The adapter passes the same
//! count to the plan so it can bind a provider `LIMIT`/`TOP`/native limit parameter. `source_bytes`
//! is a separate allowance for the portable [`Row::encoded_bytes`] total of the rows accepted for
//! one request, including the lookahead row. It is not a provider allocation, buffering, or
//! driver-overhead bound; a provider that does not enforce `RowLimit` may materialize more before
//! the adapter's post-check.
//!
//! The adapter rejects an additional result set, a later statement error, and a continuation that
//! disagrees with the lookahead before publishing the page. The `Reader` contract has no observable
//! "zero result sets" state: a no-row batch is one empty set. The caller must dispatch a
//! single-result-set statement; `read_single_set` cannot prove that a provider sent zero sets. It
//! does not promise a snapshot or distributed consistency across requests.
//!
//! # Retry and cancellation
//!
//! Direct [`Session::fetch_async`] operations do not retry. The adapter rejects an explicit enabled
//! `RetrySetting::Use(RetryPolicy::Enabled { .. })` override or a retry runtime in the plan's
//! options with [`ErrorKind::Unsupported`]. `RetrySetting::Off` is accepted, and every request is
//! set to `RetrySetting::Off`; a disabled `Use` override is also accepted and normalized to `Off`.
//! A page request is terminal on error. Dropping the page future or the containing stream is not
//! proof that a statement did not write; the assertion is the caller's, and the adapter cannot
//! verify it.

use std::fmt;

use super::{Continuation, PageRequest, PageSource, SourcePage, read_single_set};
use crate::{
    DbError, DbFuture, ErrorKind, LimitSetting, QueryOptions, RetryPolicy, RetrySetting, Row,
    RowLimit, Session,
};

/// The caller's explicit paging-safety assertion.
///
/// Constructing this value is the assertion; the adapter cannot verify it. The caller asserts that
/// every request the plan can issue is one caller-authored read-only statement, that repeating it is
/// safe, and that the caller is authorized for every tenant or row scope the statement can reach.
/// Binding an authorization predicate separately from the cursor remains the plan's responsibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlPageAssertion {
    _private: (),
}

impl SqlPageAssertion {
    /// Asserts that the plan's requests are read-only, replay-safe, and caller-authorized.
    #[must_use]
    pub const fn read_only_replay_safe_and_authorized() -> Self {
        Self { _private: () }
    }
}

/// A caller-owned plan for one provider-specific SQL page request.
///
/// The plan owns SQL, parameters, keyset or offset logic, and continuation validation. The adapter
/// calls it only when a page is requested and never retries or replays a failed call.
///
/// # Statement
///
/// [`SqlPagePlan::sql`] is captured once when the source is constructed. It must be the complete
/// native statement; the adapter never appends, rewrites, or classifies it. It must be a
/// single-result-set statement: the `Reader` contract reports a no-row batch as one empty set, so
/// the adapter cannot prove that a provider sent zero sets.
///
/// # Bounds
///
/// [`SqlPagePlan::options`] receives the effective lookahead count
/// (`min(page_size, remaining_rows) + 1`). The statement must return no more than that many rows in a
/// total order. The adapter replaces the fetch limit with its own hard [`RowLimit`], so a plan that
/// sets a limit is rejected rather than silently overridden.
///
/// # Continuation
///
/// [`SqlPagePlan::next_continuation`] must return `Some` exactly when the lookahead row exists. A
/// continuation belongs to the plan: it must reject tokens it did not issue, a token for another
/// query, and a cursor outside its authorized tenant. The adapter passes the token back unchanged
/// and never prints it.
pub trait SqlPagePlan: Send {
    /// Returns the complete provider-specific statement.
    fn sql(&self) -> &str;

    /// Builds the typed parameters and options for one request.
    ///
    /// The plan must bind exactly the statement's native placeholders, including `lookahead` as the
    /// row-limit parameter in the caller's chosen position. `request` carries the remaining
    /// aggregate row, byte, and page allowances; `remaining_bytes()` describes published rows only.
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
    /// `has_lookahead` is true when the statement returned the extra row. Return `Some` exactly in
    /// that case; the adapter fails the page when the two disagree. An empty page without a
    /// lookahead returns `None`.
    ///
    /// # Errors
    /// Returns a sanitized error when no valid continuation can be derived.
    fn next_continuation(
        &mut self,
        rows: &[Row],
        has_lookahead: bool,
    ) -> Result<Option<Continuation>, DbError>;
}

/// A [`PageSource`] over a caller-authored SQL statement and an exclusive session.
///
/// The session may borrow caller state for `'session`; the source does not require `'static`.
/// See the [module documentation](self) for the plan obligations, bounds, and no-retry behavior.
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
    /// The assertion is required even though the adapter cannot verify it; the plan owns the
    /// read-only, replay-safe, and authorization decision.
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
