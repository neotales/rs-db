//! Local-future multi-request paging.
//!
//! This mirrors [`crate::paging::multi`] for `!Send` providers. The pager still owns the request
//! and continuation handoff, bounds, page-size enforcement, terminal behavior, and no-retry/no-
//! prefetch behavior. The source owns SQL, parameters, keyset/offset logic, token scope, and
//! read-only/replay safety. Local tokens may hold `Rc` or other `!Send` state; no `Send` assertion is
//! made.
//!
//! The pager dispatches no SQL, retries, or tasks. D1 or another local provider must implement the
//! source itself; this module does not add a provider dispatch path or claim a D1 bound. The
//! [`sql`] submodule is a generic caller-authored SQL source for a provider that has a safe local
//! session; it adds no D1 support either.
//!
//! The stream is not `Send` even for a `Send` source, because its step future is local:
//!
//! ```compile_fail
//! use neotales_dbapi::{
//!     local,
//!     local::paging::multi::{Continuation, PageSource, PageSourceStream, SourcePage},
//!     paging::{PageSize, multi::{PageBudget, PageRequest}},
//! };
//!
//! struct Source;
//!
//! impl PageSource for Source {
//!     fn next_page_async(
//!         &mut self,
//!         _: PageRequest,
//!         _: Option<Continuation>,
//!     ) -> local::DbFuture<'_, SourcePage> {
//!         Box::pin(async { Ok(SourcePage::new(Vec::new(), None)) })
//!     }
//! }
//!
//! fn require_send<T: Send>(_: T) {}
//! let stream = PageSourceStream::new(
//!     Box::new(Source),
//!     PageSize::try_from(1).expect("nonzero"),
//!     PageBudget::new(1, 64, 1).expect("valid"),
//! );
//! require_send(stream);
//! ```

pub mod sql;

pub use crate::paging::multi::{PageBudget, PageRequest, PagedPage, PagedRow};

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;

use crate::local::{DbFuture, Reader};
use crate::paging::PageSize;
use crate::{DbError, ErrorKind, Row};

/// An opaque, redacted local continuation token.
///
/// Unlike the root token, this one carries no `Send` bound, so it can hold an `Rc` or a
/// single-threaded handle. It must be owned (`Any` implies `'static`). Its scope, expiry, and
/// validation belong to the source.
pub struct Continuation(Box<dyn Any>);

impl Continuation {
    /// Wraps a source-owned local token.
    #[must_use]
    pub fn new<T: Any>(token: T) -> Self {
        Self(Box::new(token))
    }

    /// Returns whether the token has type `T` without consuming it.
    #[must_use]
    pub fn is<T: Any>(&self) -> bool {
        self.0.is::<T>()
    }

    /// Consumes and returns the token when it has type `T`.
    ///
    /// # Errors
    /// Returns `self` when the stored token is not `T`.
    pub fn downcast<T: Any>(self) -> Result<T, Self> {
        self.0.downcast::<T>().map(|token| *token).map_err(Self)
    }
}

impl fmt::Debug for Continuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Continuation(redacted)")
    }
}

/// One page returned by a local [`PageSource`].
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

    /// Returns the page's rows.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns whether a continuation was supplied.
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

/// A local source of explicitly requested pages.
///
/// The first call receives `None`; later calls receive the previous page's token. The source must
/// use read-only, replay-safe requests for its data, finish each single-result-set request before
/// publishing rows, and reject extra sets. `read_single_set` is the local completion helper. The
/// pager does not classify SQL, retry, replay, prefetch, or spawn a task; source errors are terminal.
pub trait PageSource {
    /// Fetches one page using the aggregate request and previous continuation.
    ///
    /// # Errors
    /// Returns a sanitized source error; the pager makes the stream terminal.
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage>;
}

/// Drains one local reader, rejects more than one result set, and surfaces later errors.
///
/// The default [`Reader::read_sets_async`] always reports at least one set, so a no-row batch is
/// one empty set; zero-set presence is not observable through the default method. A source must
/// issue a single-result-set statement.
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
        let page_bytes = match crate::paging::multi::encoded_page_bytes(&page_rows) {
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

type StepFuture<'a> =
    Pin<Box<dyn Future<Output = (SourceState<'a>, Option<Result<PagedPage, DbError>>)> + 'a>>;

/// A local fallible stream of pages from a [`PageSource`].
///
/// The stream is `!Send`; poll it on the thread that created it. One poll requests at most one page;
/// empty pages are yielded, errors are terminal, and dropping the stream drops the source. No
/// prefetch, retry, or task is created.
#[must_use = "streams do nothing unless polled"]
pub struct PageSourceStream<'a> {
    state: Option<SourceState<'a>>,
    step: Option<StepFuture<'a>>,
    done: bool,
}

impl<'a> PageSourceStream<'a> {
    /// Creates a local page stream from a source, page size, and aggregate budget.
    pub fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self {
            state: Some(SourceState::new(source, page_size, budget)),
            step: None,
            done: false,
        }
    }

    /// Converts this page stream into a local row stream over the same source.
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

/// A local fallible stream of rows from a [`PageSource`].
///
/// Empty pages produce no row item; after an empty page the stream wakes cooperatively and returns
/// `Pending`, so one poll performs at most one source request. The stream is `!Send`.
#[must_use = "streams do nothing unless polled"]
pub struct PageSourceRowStream<'a> {
    pages: PageSourceStream<'a>,
    current: VecDeque<Row>,
    current_page: usize,
}

impl<'a> PageSourceRowStream<'a> {
    /// Creates a local row stream from a source, page size, and aggregate budget.
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
