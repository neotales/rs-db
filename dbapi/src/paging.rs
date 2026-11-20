//! Paged views over one existing [`Reader`] execution.
//!
//! This module does not dispatch SQL, acquire a connection, or retry a statement. It wraps the
//! reader returned by one `fetch_async` call, so parameters, timeout, telemetry, retry policy, and
//! the provider's `RowLimit` are exactly those of that call. [`PageStream`] and [`RowStream`] are
//! ordinary [`futures_core::Stream`] values whose items are `Result<_, DbError>`, so the standard
//! [`TryStreamExt`](https://docs.rs/futures-util/latest/futures_util/traits/trait.TryStreamExt.html)
//! methods apply, including a single-await `try_collect`.
//!
//! The [`multi`] submodule is the separate multi-request pager: a caller-implemented
//! [`multi::PageSource`] owns each request, keyset/offset logic, SQL, parameters, and continuation.
//! It does not change the single-reader views in this module.
//!
//! # Page boundaries
//!
//! A [`Page`] never crosses a result-set boundary. Each result set, including an empty one,
//! contributes at least one page: an empty result set is one page with no rows. The page's
//! [`Page::set_index`] labels that set within the view, and empty sets count. Use the page view when
//! set identity matters; the row view deliberately says nothing about an empty set because it has no
//! row to carry it.
//!
//! Set indices are **view-relative labels**, not database identities. [`PageStream::new`] and
//! [`RowStream::new`] label the first set they observe as `0`. A [`Reader`] can already have been
//! advanced before it is wrapped, and the trait does not expose its current result-set position.
//! When that happens, use [`PageStream::at_set`] or [`RowStream::at_set`] to supply the current
//! set's index; the stream increments it for later sets. The stream cannot verify the supplied
//! label. If the current label is `usize::MAX` and a later set exists, the stream yields
//! [`ErrorKind::Overflow`] instead of reusing a label; a reader that ends in its current set
//! completes normally.
//!
//! [`PageSize`] is a view chunk, not a query limit. It does not relax or replace the provider's
//! [`RowLimit`](crate::RowLimit). A page size larger than the remaining rows simply produces a
//! shorter page, and a page size smaller than the `RowLimit` is still bounded by that limit.
//!
//! # Completion, errors, and cleanup
//!
//! A stream drains to the end of the batch and calls [`Reader::finish_async`] before it completes,
//! so a later statement error is surfaced as an error item. No item follows an error. Dropping a
//! stream early drops the reader; it does not drain, finish, or report a later error. The reader's
//! existing contract applies: a provider with unread results attached must discard its connection
//! when the reader is dropped, while a provider that already materialized the bounded batch has
//! nothing outstanding.
//!
//! A reader is not proof of a read. `INSERT ... RETURNING` can write, and paging neither changes
//! nor retries that statement. Cancelling, dropping, or losing a response does not prove the write
//! did not happen; a write outcome the provider cannot establish is `WriteOutcomeUnknown`, not a
//! retryable transport error.
//!
//! # Materialization and scope
//!
//! This is a view over one reader. It makes no claim that paging reduces the provider's
//! materialization, memory use, or buffering; the provider's `RowLimit` remains the hard aggregate
//! bound, and a provider that materializes the batch before returning still does so.
//!
//! The slice deliberately covers single-execution reader chunking only. Offset, keyset, and
//! continuation-token paging that re-executes a statement are separate mechanisms with their own
//! snapshot and write semantics, and are not implemented here. [`crate::local::paging`] mirrors the
//! views for `!Send` local futures, and [`crate::sync::paging`] provides native blocking iterators;
//! all three reuse [`Page`] and [`PageSize`].
//!
//! # Example
//!
//! One fetch, then a bounded collection in one await:
//!
//! ```no_run
//! use futures_util::TryStreamExt;
//! use neotales_dbapi::{
//!     DbError, QueryOptions, RowLimit, Session,
//!     paging::{Page, PageSize, PageStream},
//! };
//!
//! async fn names(session: &mut dyn Session) -> Result<Vec<Page>, DbError> {
//!     let reader = session
//!         .fetch_async(
//!             "SELECT name FROM users ORDER BY name",
//!             QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
//!         )
//!         .await?;
//!
//!     // `PageSize` chunks the view; `RowLimit` above is still the aggregate bound.
//!     PageStream::new(reader, PageSize::try_from(25)?)
//!         .try_collect()
//!         .await
//! }
//! ```
//!
//! Use [`RowStream`] when rows are all that matter. Its [`SetRow`] keeps the result-set index so a
//! multi-set stream does not silently flatten identity:
//!
//! ```no_run
//! use futures_util::TryStreamExt;
//! use neotales_dbapi::{
//!     DbError, QueryOptions, RowLimit, Session, paging::{RowStream, SetRow},
//! };
//!
//! async fn values(session: &mut dyn Session) -> Result<Vec<SetRow>, DbError> {
//!     let reader = session
//!         .fetch_async(
//!             "SELECT value FROM items ORDER BY value",
//!             QueryOptions::new().limit(RowLimit::new(100, 65_536)?),
//!         )
//!         .await?;
//!     RowStream::new(reader).try_collect().await
//! }
//! ```

pub mod multi;

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;

use crate::{DbError, ErrorKind, Reader, Row};

/// A positive maximum number of rows in one page.
///
/// This is a view chunk, not a query limit. It never changes the provider's
/// [`RowLimit`](crate::RowLimit), and a page stream remains bounded by the limit supplied to the
/// fetch that produced its reader.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageSize(NonZeroUsize);

impl PageSize {
    /// Creates a page size from an already nonzero count.
    #[must_use]
    pub const fn new(rows: NonZeroUsize) -> Self {
        Self(rows)
    }

    /// Returns the maximum number of rows in one page.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl TryFrom<usize> for PageSize {
    type Error = DbError;

    /// Converts a nonzero row count.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when `rows` is zero.
    fn try_from(rows: usize) -> Result<Self, Self::Error> {
        NonZeroUsize::new(rows)
            .map(Self)
            .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))
    }
}

/// One page of rows from a single result set.
///
/// This value is shared by the root [`PageStream`], the local
/// [`local::paging::PageStream`](crate::local::paging::PageStream), and the synchronous
/// [`sync::paging::PageIter`](crate::sync::paging::PageIter).
///
/// Pages are produced in stream order. A page never crosses a result-set boundary; an empty result
/// set is represented by exactly one page whose [`Page::rows`] is empty.
///
/// [`Page::set_index`] is a view-relative label, not a stable database identity. It starts at `0`
/// for [`PageStream::new`] and at the caller-supplied index for [`PageStream::at_set`], then counts
/// every later set, including empty sets.
#[derive(Clone, Debug, PartialEq)]
pub struct Page {
    set_index: usize,
    rows: Vec<Row>,
}

impl Page {
    pub(crate) fn new(set_index: usize, rows: Vec<Row>) -> Self {
        Self { set_index, rows }
    }

    /// Returns the view-relative result-set label, counting empty sets.
    ///
    /// Use [`PageStream::at_set`] when the reader was already advanced and the label must match the
    /// reader's current set. Otherwise the first set observed by this view is `0`.
    #[must_use]
    pub const fn set_index(&self) -> usize {
        self.set_index
    }

    /// Returns the page's rows in stream order.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the number of rows in this page.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns whether this page is open only because its result set was empty.
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

/// One row and the result set it came from.
///
/// This value is shared by the root [`RowStream`], the local
/// [`local::paging::RowStream`](crate::local::paging::RowStream), and the synchronous
/// [`sync::paging::RowIter`](crate::sync::paging::RowIter).
///
/// This is the row view's item. [`SetRow::set_index`] has the same view-relative meaning as
/// [`Page::set_index`]. Empty result sets have no row that could carry their identity, so use
/// [`PageStream`] when empty sets must be observed.
#[derive(Clone, Debug, PartialEq)]
pub struct SetRow {
    set_index: usize,
    row: Row,
}

impl SetRow {
    pub(crate) fn new(set_index: usize, row: Row) -> Self {
        Self { set_index, row }
    }

    /// Returns the view-relative result-set label, counting empty sets.
    ///
    /// Use [`RowStream::at_set`] when the reader was already advanced and the label must match the
    /// reader's current set. Otherwise the first set observed by this view is `0`.
    #[must_use]
    pub const fn set_index(&self) -> usize {
        self.set_index
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

struct PagingState<'a> {
    reader: Box<dyn Reader + 'a>,
    page_size: PageSize,
    set_index: usize,
    set_end_seen: bool,
    set_has_emitted: bool,
    finished: bool,
}

impl<'a> PagingState<'a> {
    fn new(reader: Box<dyn Reader + 'a>, page_size: PageSize) -> Self {
        Self::at_set(reader, page_size, 0)
    }

    fn at_set(reader: Box<dyn Reader + 'a>, page_size: PageSize, set_index: usize) -> Self {
        Self {
            reader,
            page_size,
            set_index,
            set_end_seen: false,
            set_has_emitted: false,
            finished: false,
        }
    }

    async fn next_page(mut self) -> (Self, Option<Result<Page, DbError>>) {
        if self.finished {
            return (self, None);
        }

        loop {
            if self.set_end_seen {
                match self.reader.has_next_set_async().await {
                    Ok(true) => {
                        let Some(next_set_index) = self.set_index.checked_add(1) else {
                            self.finished = true;
                            return (self, Some(Err(DbError::new(ErrorKind::Overflow))));
                        };
                        self.set_index = next_set_index;
                        self.set_end_seen = false;
                        self.set_has_emitted = false;
                        continue;
                    }
                    Ok(false) => {
                        self.finished = true;
                        let result = self.reader.finish_async().await;
                        return (self, result.err().map(Err));
                    }
                    Err(error) => {
                        self.finished = true;
                        return (self, Some(Err(error)));
                    }
                }
            }

            let mut rows = Vec::new();
            while rows.len() < self.page_size.get() {
                match self.reader.next_row_async().await {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {
                        self.set_end_seen = true;
                        if rows.is_empty() && self.set_has_emitted {
                            // A later page of this set would be empty; advance instead.
                            break;
                        }
                        self.set_has_emitted = true;
                        let set_index = self.set_index;
                        return (self, Some(Ok(Page::new(set_index, rows))));
                    }
                    Err(error) => {
                        self.finished = true;
                        return (self, Some(Err(error)));
                    }
                }
            }

            if rows.is_empty() {
                // Only reachable after breaking on a set that already emitted a page.
                continue;
            }

            self.set_has_emitted = true;
            let set_index = self.set_index;
            return (self, Some(Ok(Page::new(set_index, rows))));
        }
    }
}

type StepFuture<'a> =
    Pin<Box<dyn Future<Output = (PagingState<'a>, Option<Result<Page, DbError>>)> + Send + 'a>>;

/// A fallible stream of pages over one existing reader.
///
/// The stream owns the reader until it completes or is dropped. It never dispatches another query.
/// Pages never cross result-set boundaries, and every result set, including an empty one, yields at
/// least one page. On normal completion the stream finishes the reader so a later statement error
/// becomes an error item; dropping the stream early does not finish it.
///
/// The item type is `Result<Page, DbError>`, which is a `TryStream`, so a whole bounded batch can be
/// collected in one await with `TryStreamExt::try_collect`. The stream does not add a limit: the
/// provider's [`RowLimit`](crate::RowLimit) still bounds the reader.
#[must_use = "streams do nothing unless polled"]
pub struct PageStream<'a> {
    state: Option<PagingState<'a>>,
    step: Option<StepFuture<'a>>,
    done: bool,
}

impl<'a> PageStream<'a> {
    /// Wraps one existing reader with a page view whose set labels start at `0`.
    ///
    /// Use this for a reader at the start of its batch. The view labels the first result set it
    /// observes as `0` and increments for later sets. If the reader has already advanced, use
    /// [`PageStream::at_set`] so the labels match the reader's actual current set.
    ///
    /// No SQL is taken or dispatched here. The reader's options, parameters, telemetry, and bounded
    /// `RowLimit` stay exactly as supplied to the single fetch that created it.
    pub fn new(reader: Box<dyn Reader + 'a>, page_size: PageSize) -> Self {
        Self {
            state: Some(PagingState::new(reader, page_size)),
            step: None,
            done: false,
        }
    }

    /// Wraps one existing reader with a page view whose current set is labelled `set_index`.
    ///
    /// Use this after advancing a reader to another result set. The reader contract does not expose
    /// its current set position, so the caller supplies the label; the stream cannot verify it. The
    /// label counts empty sets and increments for every later set.
    ///
    /// A current set labelled `usize::MAX` is valid when it is the last set. If another set exists,
    /// the stream cannot label it without colliding and yields
    /// [`ErrorKind::Overflow`] as a terminal error item.
    ///
    /// No SQL is taken or dispatched here. The reader's options, parameters, telemetry, and bounded
    /// `RowLimit` stay exactly as supplied to the single fetch that created it.
    pub fn at_set(reader: Box<dyn Reader + 'a>, page_size: PageSize, set_index: usize) -> Self {
        Self {
            state: Some(PagingState::at_set(reader, page_size, set_index)),
            step: None,
            done: false,
        }
    }

    /// Converts this page stream into a row stream over the same reader.
    ///
    /// The existing reader and any pending page step are reused; this does not fetch again, re-read
    /// a result set, or produce an extra page just to recover rows. Empty result sets produce no row
    /// item; use the page view to observe them.
    pub fn into_rows(self) -> RowStream<'a> {
        RowStream::from_pages(self)
    }
}

impl fmt::Debug for PageStream<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageStream")
            .field(
                "set_index",
                &self.state.as_ref().map(|state| state.set_index),
            )
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl Stream for PageStream<'_> {
    type Item = Result<Page, DbError>;

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

/// A fallible stream of rows over one existing reader.
///
/// Each item carries its [`SetRow::set_index`], so a multi-set stream preserves identity for every
/// row. Empty result sets produce no item because they have no row; after an empty set the stream
/// wakes cooperatively and returns `Pending`, so one poll advances at most one set instead of
/// spinning through a chain of ready empty sets. Use [`PageStream`] when empty sets must be observed
/// as items. The stream has the same completion, error, drop, and write rules as [`PageStream`], and
/// it adds no query or limit of its own.
#[must_use = "streams do nothing unless polled"]
pub struct RowStream<'a> {
    pages: PageStream<'a>,
    current: VecDeque<Row>,
    current_set: usize,
}

impl<'a> RowStream<'a> {
    /// Wraps one existing reader with a row view whose set labels start at `0`.
    ///
    /// Use this for a reader at the start of its batch. If the reader has already advanced, use
    /// [`RowStream::at_set`] so the labels match the reader's actual current set.
    ///
    /// No SQL is taken or dispatched here. The reader's options, parameters, telemetry, and bounded
    /// `RowLimit` stay exactly as supplied to the single fetch that created it.
    pub fn new(reader: Box<dyn Reader + 'a>) -> Self {
        Self::from_pages(PageStream::new(reader, PageSize::new(NonZeroUsize::MIN)))
    }

    /// Wraps one existing reader with a row view whose current set is labelled `set_index`.
    ///
    /// Use this after advancing a reader to another result set. The reader contract does not expose
    /// its current set position, so the caller supplies the label; the stream cannot verify it. The
    /// label counts empty sets and increments for every later set.
    ///
    /// A current set labelled `usize::MAX` is valid when it is the last set. If another set exists,
    /// the stream cannot label it without colliding and yields
    /// [`ErrorKind::Overflow`] as a terminal error item.
    ///
    /// No SQL is taken or dispatched here. The reader's options, parameters, telemetry, and bounded
    /// `RowLimit` stay exactly as supplied to the single fetch that created it.
    pub fn at_set(reader: Box<dyn Reader + 'a>, set_index: usize) -> Self {
        Self::from_pages(PageStream::at_set(
            reader,
            PageSize::new(NonZeroUsize::MIN),
            set_index,
        ))
    }

    fn from_pages(pages: PageStream<'a>) -> Self {
        Self {
            pages,
            current: VecDeque::new(),
            current_set: 0,
        }
    }
}

impl fmt::Debug for RowStream<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RowStream")
            .field("current_set", &self.current_set)
            .field("buffered_rows", &self.current.len())
            .finish_non_exhaustive()
    }
}

impl Stream for RowStream<'_> {
    type Item = Result<SetRow, DbError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(row) = this.current.pop_front() {
                return Poll::Ready(Some(Ok(SetRow::new(this.current_set, row))));
            }

            match Pin::new(&mut this.pages).poll_next(context) {
                Poll::Ready(Some(Ok(page))) => {
                    this.current_set = page.set_index();
                    this.current = page.into_rows().into();
                    if this.current.is_empty() {
                        // An empty result set is one completed page, not a reason to
                        // spin through more ready sets in the same poll.
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
