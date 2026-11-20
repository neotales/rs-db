//! Explicit multi-request paging over a [`PageSource`].
//!
//! This module is additive to the single-execution views in [`crate::paging`]. A [`PageSource`]
//! chooses its own page request, continuation token, keyset/offset logic, SQL, and parameters. The
//! pager never rewrites SQL, classifies a statement as read-only, retries, replays, prefetches, or
//! spawns a task. It requests one page only when the stream is polled for one, and it is terminal
//! after an error or a dropped in-flight operation.
//!
//! # Bounds
//!
//! [`PageBudget`] bounds the whole page sequence: delivered rows, portable decoded bytes, and page
//! count. Empty pages count toward the page bound, so a source that keeps returning empty pages with
//! a continuation cannot run forever. [`PageSize`] is enforced separately: a source that returns
//! more rows than the requested page size is rejected with `Decode`. Aggregate row and byte
//! arithmetic is checked; exceeding a bound fails with `Overflow`, never truncates.
//!
//! A page stream is a `futures_core::Stream` of `Result<PagedPage, DbError>`, and the row stream is
//! a `Stream` of `Result<PagedRow, DbError>`. Both work with `TryStreamExt::try_collect` for a
//! bounded one-await collection. The row stream wakes cooperatively and yields `Pending` after an
//! empty page, so one poll performs at most one source request instead of spinning through a chain
//! of ready empty pages.
//!
//! # Exact bounds and continuations
//!
//! When the rows, bytes, or page count reach the budget exactly and the source still supplies a
//! continuation token, the pager fails closed with `Overflow` on the next poll. It does not request
//! the next page. If the source supplies no continuation at the exact bound, the stream completes
//! normally.
//!
//! # Source obligations
//!
//! The pager enforces the bounds, page shape, continuation handoff, and terminal/no-retry behavior.
//! It cannot enforce the following; they are the source author's responsibility:
//!
//! - **Read-only and replay safety.** Re-requesting or continuing a statement can apply a write more
//!   than once. The source must use only read-only, replay-safe requests for its data. Cancellation,
//!   a dropped stream, or a lost response does not prove a request did not write.
//! - **SQL, ordering, consistency, and parameters.** The source authors the request, its parameters,
//!   its ordering, and its consistency model. The pager does not rewrite or interpret SQL and makes
//!   no distributed-consistency claim.
//! - **Single-result-set completion.** Use [`read_single_set`] (or equivalent) to drain the whole
//!   reader, reject additional result sets, and surface late statement errors before publishing a
//!   page. A source that returns rows before draining hides later errors and extra sets; the pager
//!   cannot detect that. The default reader reports a no-row batch as one empty set, so zero-set
//!   presence is not observable; the caller must issue a single-result-set statement.
//! - **Token scope and validity.** The pager passes [`Continuation`] back unchanged. The source owns
//!   the token's scope, expiry, tenant/query binding, and validation.
//! - **Provider limits.** The source maps the request to its provider's limits and owns any
//!   materialization or cancellation behavior.
//!
//! This crate also ships a generic, caller-authored SQL source in [`sql`]. It still builds no
//! statement of its own: the caller supplies the complete provider-specific SQL, typed parameters,
//! continuation scope, and an explicit read-only/replay-safe/authorized assertion. A tested SQLite
//! example is compiled by the `neotales-dbapi-sqlite` package (`examples/keyset_paging.rs`, with the
//! reusable source in `examples/support/keyset_source.rs` and tests in `tests/keyset_paging.rs`).
//! PostgreSQL uses `$1`-style placeholders in the same statement; the caller authors them, nothing
//! here rewrites SQL, and no live PostgreSQL test is claimed.
//!
//! The root pager requires a `Send` source and `Send` database futures. It is not a D1 local-binding
//! adapter: a `!Send` D1 Workers future must fail closed rather than be asserted into `Send`.
//! [`crate::local::paging::multi`] mirrors this pager without `Send`, and
//! [`crate::sync::paging::multi`] provides native blocking iterators. Neither adds a provider
//! dispatch path or a D1 claim.
//!
//! # Example
//!
//! A source constructs its own request and continuation. This one always returns one empty page and
//! no continuation:
//!
//! ```
//! use neotales_dbapi::{
//!     DbFuture,
//!     paging::{
//!         PageSize,
//!         multi::{Continuation, PageRequest, PageSource, SourcePage},
//!     },
//! };
//!
//! struct EmptySource;
//!
//! impl PageSource for EmptySource {
//!     fn next_page_async(
//!         &mut self,
//!         _request: PageRequest,
//!         _continuation: Option<Continuation>,
//!     ) -> DbFuture<'_, SourcePage> {
//!         Box::pin(async { Ok(SourcePage::new(Vec::new(), None)) })
//!     }
//! }
//! # let _ = EmptySource;
//! # let _ = PageSize::try_from(1).expect("nonzero");
//! ```
//!
//! A page stream collects under the budget in one await:
//!
//! ```no_run
//! use futures_util::TryStreamExt;
//! use neotales_dbapi::{
//!     DbError,
//!     paging::{
//!         PageSize,
//!         multi::{PageBudget, PageSource, PageSourceStream, PagedPage},
//!     },
//! };
//!
//! async fn collect(source: Box<dyn PageSource>) -> Result<Vec<PagedPage>, DbError> {
//!     PageSourceStream::new(
//!         source,
//!         PageSize::try_from(50)?,
//!         PageBudget::new(1_000, 65_536, 100)?,
//!     )
//!     .try_collect()
//!     .await
//! }
//! ```

pub mod sql;

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;

use super::PageSize;
use crate::{DbError, DbFuture, ErrorKind, Reader, Row};

/// An opaque, redacted continuation token owned by a [`PageSource`].
///
/// The pager stores the token and passes it back to the same source unchanged. It never interprets,
/// compares, logs, or includes it in an error. [`fmt::Debug`] prints only `Continuation(redacted)`,
/// so a source's cursor, tenant binding, or provider token is not exposed through the pager's debug
/// output.
///
/// The token must be owned (`Any` implies `'static`). Its scope, expiry, encoding, and validation
/// belong to the source. The source should bind it to the query and tenant it came from and should
/// reject tokens it did not issue.
pub struct Continuation(Box<dyn Any + Send>);

impl Continuation {
    /// Wraps a source-owned token.
    #[must_use]
    pub fn new<T: Any + Send>(token: T) -> Self {
        Self(Box::new(token))
    }

    /// Returns whether the token has type `T` without consuming it.
    #[must_use]
    pub fn is<T: Any + Send>(&self) -> bool {
        self.0.is::<T>()
    }

    /// Consumes and returns the token when it has type `T`.
    ///
    /// Returns the opaque token unchanged when it has another type.
    ///
    /// # Errors
    /// Returns `self` when the stored token is not `T`.
    pub fn downcast<T: Any + Send>(self) -> Result<T, Self> {
        self.0.downcast::<T>().map(|token| *token).map_err(Self)
    }
}

impl fmt::Debug for Continuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Continuation(redacted)")
    }
}

/// One page returned by a [`PageSource`].
///
/// `rows` is the page the source chose to return. `continuation` is `Some` only when the source
/// expects another request to be useful. The pager validates the page against the request and
/// aggregate budget before yielding it.
#[derive(Debug)]
pub struct SourcePage {
    rows: Vec<Row>,
    continuation: Option<Continuation>,
}

impl SourcePage {
    /// Creates a source page from its rows and optional continuation.
    #[must_use]
    pub fn new(rows: Vec<Row>, continuation: Option<Continuation>) -> Self {
        Self { rows, continuation }
    }

    /// Returns the page's rows in source order.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns whether the source supplied a continuation.
    #[must_use]
    pub fn has_continuation(&self) -> bool {
        self.continuation.is_some()
    }

    /// Consumes the page and returns its rows and continuation.
    #[must_use]
    pub fn into_parts(self) -> (Vec<Row>, Option<Continuation>) {
        (self.rows, self.continuation)
    }
}

/// The bound for a whole multi-request page sequence.
///
/// Rows and bytes are aggregate across every delivered page. `max_pages` also counts empty pages.
/// Providers and this pager fail rather than truncate when a bound is exceeded. The pager enforces
/// these bounds; a source must also map the request to its provider's limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageBudget {
    rows: usize,
    bytes: usize,
    max_pages: usize,
}

impl PageBudget {
    /// Creates a budget with non-zero row, byte, and page bounds.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if any bound is zero.
    pub fn new(rows: usize, bytes: usize, max_pages: usize) -> Result<Self, DbError> {
        if rows == 0 || bytes == 0 || max_pages == 0 {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(Self {
            rows,
            bytes,
            max_pages,
        })
    }

    /// Returns the maximum number of delivered rows across all pages.
    #[must_use]
    pub const fn rows(self) -> usize {
        self.rows
    }

    /// Returns the maximum decoded byte count across all pages.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }

    /// Returns the maximum number of pages, including empty pages.
    #[must_use]
    pub const fn max_pages(self) -> usize {
        self.max_pages
    }
}

impl Default for PageBudget {
    /// A bounded default: 1,000 rows, 1 MiB, and 100 pages.
    fn default() -> Self {
        Self {
            rows: 1_000,
            bytes: 1_048_576,
            max_pages: 100,
        }
    }
}

/// The request passed to one [`PageSource`] call.
///
/// `remaining_*` values describe what the aggregate [`PageBudget`] still allows. They are not a
/// promise that the source will return that many rows; the source chooses the request and may return
/// fewer. The pager separately enforces the page size and the aggregate bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageRequest {
    page_size: PageSize,
    remaining_rows: usize,
    remaining_bytes: usize,
    remaining_pages: usize,
}

impl PageRequest {
    pub(crate) fn new(
        page_size: PageSize,
        remaining_rows: usize,
        remaining_bytes: usize,
        remaining_pages: usize,
    ) -> Self {
        Self {
            page_size,
            remaining_rows,
            remaining_bytes,
            remaining_pages,
        }
    }

    /// Returns the maximum number of rows this page may contain.
    #[must_use]
    pub const fn page_size(self) -> PageSize {
        self.page_size
    }

    /// Returns the rows still allowed by the aggregate budget.
    #[must_use]
    pub const fn remaining_rows(self) -> usize {
        self.remaining_rows
    }

    /// Returns the decoded bytes still allowed by the aggregate budget.
    #[must_use]
    pub const fn remaining_bytes(self) -> usize {
        self.remaining_bytes
    }

    /// Returns the pages still allowed by the aggregate page bound.
    #[must_use]
    pub const fn remaining_pages(self) -> usize {
        self.remaining_pages
    }
}

/// A source of explicitly requested pages.
///
/// The source owns the page request and the continuation. It decides whether it is using keyset,
/// offset, or a provider token; the pager does not infer or rewrite the mechanism. It is called only
/// when a stream is polled for another page, and never after an error.
///
/// # Request and continuation
///
/// The first call receives `None`. Later calls receive the token returned by the previous
/// [`SourcePage`]. The pager does not inspect the token; see [`Continuation`].
///
/// # Read-only and replay safety
///
/// The pager does not classify SQL as read-only and does not retry or replay. A source must use only
/// read-only, replay-safe requests for its data. A write can be applied again when a page is
/// requested again; cancellation or a lost response does not prove a write did not happen.
///
/// # Completion
///
/// A source must finish each single-result-set request before returning its page. Use
/// [`read_single_set`] (or equivalent) so unread rows, extra result sets, and later statement errors
/// are handled before publication. The pager cannot verify that a source did this.
///
/// # Errors
///
/// Return a sanitized [`DbError`]. The pager makes the stream terminal and does not retry, replay, or
/// request another page.
pub trait PageSource: Send {
    /// Fetches one page using the aggregate request and the previous continuation.
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage>;
}

/// Drains one reader, rejects more than one result set, and surfaces later statement errors.
///
/// This is the helper a [`PageSource`] should use before publishing a page. It reads every row and
/// set, rejects a second set (including an empty one), and propagates the first later error. It has
/// no page-size or aggregate-budget logic; the pager enforces those.
///
/// The default [`Reader::read_sets_async`] always reports at least one set, so a reader that
/// produced no rows yields one empty set. Absence of a result set is not observable through that
/// default method, and this helper cannot prove that a provider sent zero sets. A source must issue
/// a single-result-set statement; that is the caller's precondition. A reader that overrides
/// `read_sets_async` can report zero, which this helper also rejects, but that override is not a
/// general presence signal.
///
/// # Errors
/// Returns the reader's sanitized error, or `Decode` when the reader reports more than one result
/// set (or an overriding reader reports zero).
pub async fn read_single_set(reader: Box<dyn Reader + '_>) -> Result<Vec<Row>, DbError> {
    let mut reader = reader;
    let mut sets = reader.read_sets_async().await?;
    if sets.len() != 1 {
        return Err(DbError::new(ErrorKind::Decode));
    }
    Ok(sets.remove(0))
}

/// One page emitted by a [`PageSourceStream`].
///
/// `page_index` is the zero-based request/page position in this stream, not a row offset, result-set
/// index, or database identity. Empty pages are emitted and counted.
#[derive(Clone, Debug, PartialEq)]
pub struct PagedPage {
    page_index: usize,
    rows: Vec<Row>,
}

impl PagedPage {
    pub(crate) fn new(page_index: usize, rows: Vec<Row>) -> Self {
        Self { page_index, rows }
    }

    /// Returns the zero-based page/request position.
    #[must_use]
    pub const fn page_index(&self) -> usize {
        self.page_index
    }

    /// Returns the page's rows in source order.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the number of rows in this page.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns whether this page is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Consumes the page and returns its rows.
    #[must_use]
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }
}

/// One row emitted by a [`PageSourceRowStream`], with its page/request position.
#[derive(Clone, Debug, PartialEq)]
pub struct PagedRow {
    page_index: usize,
    row: Row,
}

impl PagedRow {
    pub(crate) fn new(page_index: usize, row: Row) -> Self {
        Self { page_index, row }
    }

    /// Returns the zero-based page/request position.
    #[must_use]
    pub const fn page_index(&self) -> usize {
        self.page_index
    }

    /// Returns the row.
    #[must_use]
    pub fn row(&self) -> &Row {
        &self.row
    }

    /// Consumes the item and returns its row.
    #[must_use]
    pub fn into_row(self) -> Row {
        self.row
    }
}

struct SourceState<'a> {
    source: Box<dyn PageSource + 'a>,
    page_size: PageSize,
    budget: PageBudget,
    continuation: Option<Continuation>,
    started: bool,
    pages: usize,
    rows: usize,
    bytes: usize,
    finished: bool,
}

impl<'a> SourceState<'a> {
    fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self {
            source,
            page_size,
            budget,
            continuation: None,
            started: false,
            pages: 0,
            rows: 0,
            bytes: 0,
            finished: false,
        }
    }

    fn fail(mut self, error: DbError) -> (Self, Option<Result<PagedPage, DbError>>) {
        self.finished = true;
        (self, Some(Err(error)))
    }

    async fn next_page(mut self) -> (Self, Option<Result<PagedPage, DbError>>) {
        if self.finished {
            return (self, None);
        }

        if self.started && self.continuation.is_none() {
            self.finished = true;
            return (self, None);
        }

        if self.pages >= self.budget.max_pages() {
            if self.continuation.is_some() {
                return self.fail(DbError::new(ErrorKind::Overflow));
            }
            self.finished = true;
            return (self, None);
        }

        if self.rows >= self.budget.rows() || self.bytes >= self.budget.bytes() {
            if self.continuation.is_some() {
                return self.fail(DbError::new(ErrorKind::Overflow));
            }
            self.finished = true;
            return (self, None);
        }

        let request = PageRequest::new(
            self.page_size,
            self.budget.rows() - self.rows,
            self.budget.bytes() - self.bytes,
            self.budget.max_pages() - self.pages,
        );
        let page = match self
            .source
            .next_page_async(request, self.continuation.take())
            .await
        {
            Ok(page) => page,
            Err(error) => return self.fail(error),
        };

        let (page_rows, continuation) = page.into_parts();
        if page_rows.len() > self.page_size.get() {
            return self.fail(DbError::new(ErrorKind::Decode));
        }

        let next_rows = match self.rows.checked_add(page_rows.len()) {
            Some(rows) if rows <= self.budget.rows() => rows,
            _ => return self.fail(DbError::new(ErrorKind::Overflow)),
        };
        let page_bytes = match encoded_page_bytes(&page_rows) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        let next_bytes = match self.bytes.checked_add(page_bytes) {
            Some(bytes) if bytes <= self.budget.bytes() => bytes,
            _ => return self.fail(DbError::new(ErrorKind::Overflow)),
        };
        let next_pages = match self.pages.checked_add(1) {
            Some(pages) if pages <= self.budget.max_pages() => pages,
            _ => return self.fail(DbError::new(ErrorKind::Overflow)),
        };

        let page_index = self.pages;
        self.pages = next_pages;
        self.rows = next_rows;
        self.bytes = next_bytes;
        self.continuation = continuation;
        self.started = true;
        (self, Some(Ok(PagedPage::new(page_index, page_rows))))
    }
}

pub(crate) fn encoded_page_bytes(rows: &[Row]) -> Result<usize, DbError> {
    rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))
    })
}

type StepFuture<'a> = Pin<
    Box<dyn Future<Output = (SourceState<'a>, Option<Result<PagedPage, DbError>>)> + Send + 'a>,
>;

/// A fallible stream of pages from a [`PageSource`].
///
/// Each poll requests at most one page. The stream does not prefetch, dispatch after an error, retry,
/// or spawn a task. Dropping the stream while a request is pending drops the in-flight future and
/// the source; no further request is made.
///
/// The pager enforces [`PageBudget`], [`PageSize`], continuation handoff, and terminal error
/// behavior. It does not enforce the source obligations described on [`PageSource`].
#[must_use = "streams do nothing unless polled"]
pub struct PageSourceStream<'a> {
    state: Option<SourceState<'a>>,
    step: Option<StepFuture<'a>>,
    done: bool,
}

impl<'a> PageSourceStream<'a> {
    /// Creates a page stream from an explicitly implemented source, page size, and aggregate budget.
    ///
    /// The source decides the actual request, SQL, parameters, keyset/offset logic, and
    /// continuation. No SQL is constructed or rewritten here.
    pub fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self {
            state: Some(SourceState::new(source, page_size, budget)),
            step: None,
            done: false,
        }
    }

    /// Converts this page stream into a row stream over the same source and state.
    ///
    /// Empty pages still consume a request and count toward the budget, but produce no row item.
    pub fn into_rows(self) -> PageSourceRowStream<'a> {
        PageSourceRowStream::from_pages(self)
    }
}

impl fmt::Debug for PageSourceStream<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageSourceStream")
            .field("page_index", &self.state.as_ref().map(|state| state.pages))
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl Stream for PageSourceStream<'_> {
    type Item = Result<PagedPage, DbError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }

        if this.step.is_none() {
            let Some(state) = this.state.take() else {
                this.done = true;
                return Poll::Ready(None);
            };
            this.step = Some(Box::pin(async move { state.next_page().await }));
        }

        let Some(step) = this.step.as_mut() else {
            this.done = true;
            return Poll::Ready(None);
        };

        match step.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((state, item)) => {
                this.step = None;
                match item {
                    Some(Ok(page)) => {
                        this.state = Some(state);
                        Poll::Ready(Some(Ok(page)))
                    }
                    Some(Err(error)) => {
                        this.done = true;
                        Poll::Ready(Some(Err(error)))
                    }
                    None => {
                        this.done = true;
                        Poll::Ready(None)
                    }
                }
            }
        }
    }
}

/// A fallible stream of rows from a [`PageSource`].
///
/// Each item carries its [`PagedRow::page_index`]. Empty pages produce no row item; after an empty
/// page the stream wakes cooperatively and returns `Pending`, so one poll performs at most one
/// source request instead of spinning through a chain of ready empty pages. Use [`PageSourceStream`]
/// when empty pages must be observed as items.
///
/// The underlying page stream still enforces the page budget, page size, continuation, terminal, and
/// no-prefetch behavior.
#[must_use = "streams do nothing unless polled"]
pub struct PageSourceRowStream<'a> {
    pages: PageSourceStream<'a>,
    current: VecDeque<Row>,
    current_page: usize,
}

impl<'a> PageSourceRowStream<'a> {
    /// Creates a row stream from an explicitly implemented source, page size, and aggregate budget.
    ///
    /// The source decides the actual request, SQL, parameters, keyset/offset logic, and
    /// continuation. No SQL is constructed or rewritten here.
    pub fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self::from_pages(PageSourceStream::new(source, page_size, budget))
    }

    fn from_pages(pages: PageSourceStream<'a>) -> Self {
        Self {
            pages,
            current: VecDeque::new(),
            current_page: 0,
        }
    }
}

impl fmt::Debug for PageSourceRowStream<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageSourceRowStream")
            .field("current_page", &self.current_page)
            .field("buffered_rows", &self.current.len())
            .finish_non_exhaustive()
    }
}

impl Stream for PageSourceRowStream<'_> {
    type Item = Result<PagedRow, DbError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(row) = this.current.pop_front() {
                return Poll::Ready(Some(Ok(PagedRow::new(this.current_page, row))));
            }

            match Pin::new(&mut this.pages).poll_next(context) {
                Poll::Ready(Some(Ok(page))) => {
                    this.current_page = page.page_index();
                    this.current = page.into_rows().into();
                    if this.current.is_empty() {
                        // An empty page is a completed request, not a reason to dispatch
                        // another one in the same poll. Wake so a cooperative executor
                        // can poll again without a busy loop.
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
