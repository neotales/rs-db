//! Local-future paged views over one existing [`Reader`].
//!
//! This is the `!Send` counterpart of [`crate::paging`]. It uses the same [`Page`], [`PageSize`],
//! and [`SetRow`] values and the same set/page boundary rules, but it holds a [`local::Reader`] and
//! polls local futures. It is not a thread-transfer grant: keep the stream on the thread that
//! created it.
//!
//! The single-reader and [`multi`] views preserve the local contract: no `Send` assertion, no
//! runtime, no thread spawning, and no SQL construction or retry. A local stream is not usable where
//! `Send` is required:
//!
//! ```compile_fail
//! use neotales_dbapi::{local, local::paging::PageStream, paging::PageSize};
//!
//! fn require_send<T: Send>(_: T) {}
//!
//! let reader = Box::new(local::BufferedReader::new(Vec::new()));
//! let stream = PageStream::new(reader, PageSize::try_from(1).expect("nonzero"));
//! require_send(stream);
//! ```
//!
//! [`local::Reader`]: super::Reader

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

use super::Reader;
use crate::{DbError, ErrorKind, Row};

pub use crate::paging::{Page, PageSize, SetRow};

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
                // The set ended after an earlier page; advance on the next iteration.
                continue;
            }

            self.set_has_emitted = true;
            let set_index = self.set_index;
            return (self, Some(Ok(Page::new(set_index, rows))));
        }
    }
}

type StepFuture<'a> =
    Pin<Box<dyn Future<Output = (PagingState<'a>, Option<Result<Page, DbError>>)> + 'a>>;

/// A local fallible stream of pages over one existing reader.
///
/// The stream is `!Send`; poll it on the thread that created it. It never dispatches SQL, retries,
/// or spawns a task. Pages never cross result-set boundaries, and every result set, including an
/// empty one, yields at least one page. On normal completion the stream finishes the reader so a
/// later statement error becomes an error item; dropping early does not finish it.
#[must_use = "streams do nothing unless polled"]
pub struct PageStream<'a> {
    state: Option<PagingState<'a>>,
    step: Option<StepFuture<'a>>,
    done: bool,
}

impl<'a> PageStream<'a> {
    /// Wraps one existing local reader with a page view whose set labels start at `0`.
    ///
    /// Use this for a reader at the start of its batch. If the reader has already advanced, use
    /// [`PageStream::at_set`].
    pub fn new(reader: Box<dyn Reader + 'a>, page_size: PageSize) -> Self {
        Self {
            state: Some(PagingState::new(reader, page_size)),
            step: None,
            done: false,
        }
    }

    /// Wraps one existing local reader with a page view whose current set is labelled `set_index`.
    ///
    /// The reader contract does not expose its current set position, so the caller supplies the
    /// label; the stream cannot verify it. A current set at `usize::MAX` completes normally when it
    /// is last; a later set yields a terminal `Overflow`.
    pub fn at_set(reader: Box<dyn Reader + 'a>, page_size: PageSize, set_index: usize) -> Self {
        Self {
            state: Some(PagingState::at_set(reader, page_size, set_index)),
            step: None,
            done: false,
        }
    }

    /// Converts this page stream into a row stream over the same reader.
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

/// A local fallible stream of rows over one existing reader.
///
/// Each item carries its [`SetRow::set_index`]. Empty result sets produce no row item; after an empty
/// set the stream wakes cooperatively and returns `Pending`, so one poll advances at most one set.
/// Use [`PageStream`] when empty-set identity matters. The stream is `!Send` and follows the same
/// completion, error, drop, and no-retry rules as [`PageStream`].
#[must_use = "streams do nothing unless polled"]
pub struct RowStream<'a> {
    pages: PageStream<'a>,
    current: VecDeque<Row>,
    current_set: usize,
}

impl<'a> RowStream<'a> {
    /// Wraps one existing local reader with a row view whose set labels start at `0`.
    pub fn new(reader: Box<dyn Reader + 'a>) -> Self {
        Self::from_pages(PageStream::new(reader, PageSize::new(NonZeroUsize::MIN)))
    }

    /// Wraps one existing local reader with a row view whose current set is `set_index`.
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
