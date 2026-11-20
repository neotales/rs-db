//! Native synchronous multi-request paging.
//!
//! This mirrors [`crate::paging::multi`] for blocking sources. The source authors SQL, parameters,
//! keyset/offset logic, and continuation; the iterator enforces aggregate row/byte/page bounds,
//! page size, exact-bound continuation failure, and terminal errors. No runtime, `block_on`, retry,
//! prefetch, or task is created. Read-only/replay safety and single-result-set completion remain
//! source obligations; `read_single_set` is the synchronous completion helper. The [`sql`]
//! submodule adds a generic caller-authored SQL source over the root plan contract.

pub mod sql;

pub use crate::paging::multi::{
    Continuation, PageBudget, PageRequest, PagedPage, PagedRow, SourcePage,
};

use super::PageSize;
use crate::sync::Reader;
use crate::{DbError, ErrorKind, Row};

/// A native synchronous source of explicitly requested pages.
///
/// The first call receives `None`; later calls receive the previous page's token. The source must
/// use read-only, replay-safe requests for its data, finish each single-result-set request before
/// publishing rows, and reject extra sets. The iterator does not classify SQL, retry, replay,
/// prefetch, or spawn a task. Source errors are terminal.
pub trait PageSource: Send {
    /// Fetches one page using the aggregate request and previous continuation.
    ///
    /// # Errors
    /// Returns a sanitized source error; the iterator makes the sequence terminal.
    fn next_page(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> Result<SourcePage, DbError>;
}

/// Drains one synchronous reader, rejects more than one result set, and surfaces later errors.
///
/// The default [`Reader::read_sets`] always reports at least one set, so a no-row batch is one
/// empty set; zero-set presence is not observable through the default method. A source must issue a
/// single-result-set statement.
///
/// # Errors
/// Returns the reader's sanitized error, or `Decode` when the reader reports more than one result
/// set (or an overriding reader reports zero).
pub fn read_single_set(reader: Box<dyn Reader + '_>) -> Result<Vec<Row>, DbError> {
    let mut reader = reader;
    let mut sets = reader.read_sets()?;
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

    fn fail(&mut self, error: DbError) -> Result<PagedPage, DbError> {
        self.finished = true;
        Err(error)
    }

    fn next_page(&mut self) -> Option<Result<PagedPage, DbError>> {
        if self.finished {
            return None;
        }

        if self.started && self.continuation.is_none() {
            self.finished = true;
            return None;
        }
        if self.pages >= self.budget.max_pages() {
            if self.continuation.is_some() {
                return Some(self.fail(DbError::new(ErrorKind::Overflow)));
            }
            self.finished = true;
            return None;
        }
        if self.rows >= self.budget.rows() || self.bytes >= self.budget.bytes() {
            if self.continuation.is_some() {
                return Some(self.fail(DbError::new(ErrorKind::Overflow)));
            }
            self.finished = true;
            return None;
        }

        let request = PageRequest::new(
            self.page_size,
            self.budget.rows() - self.rows,
            self.budget.bytes() - self.bytes,
            self.budget.max_pages() - self.pages,
        );
        let page = match self.source.next_page(request, self.continuation.take()) {
            Ok(page) => page,
            Err(error) => return Some(self.fail(error)),
        };

        let (page_rows, continuation) = page.into_parts();
        if page_rows.len() > self.page_size.get() {
            return Some(self.fail(DbError::new(ErrorKind::Decode)));
        }
        let next_rows = match self.rows.checked_add(page_rows.len()) {
            Some(rows) if rows <= self.budget.rows() => rows,
            _ => return Some(self.fail(DbError::new(ErrorKind::Overflow))),
        };
        let page_bytes = match crate::paging::multi::encoded_page_bytes(&page_rows) {
            Ok(bytes) => bytes,
            Err(error) => return Some(self.fail(error)),
        };
        let next_bytes = match self.bytes.checked_add(page_bytes) {
            Some(bytes) if bytes <= self.budget.bytes() => bytes,
            _ => return Some(self.fail(DbError::new(ErrorKind::Overflow))),
        };
        let next_pages = match self.pages.checked_add(1) {
            Some(pages) if pages <= self.budget.max_pages() => pages,
            _ => return Some(self.fail(DbError::new(ErrorKind::Overflow))),
        };

        let page_index = self.pages;
        self.pages = next_pages;
        self.rows = next_rows;
        self.bytes = next_bytes;
        self.continuation = continuation;
        self.started = true;
        Some(Ok(PagedPage::new(page_index, page_rows)))
    }
}

/// A native synchronous iterator of pages from a [`PageSource`].
///
/// Each `next` requests at most one page. Empty pages are yielded and count toward the page bound.
/// Errors are terminal and do not trigger a retry. Dropping the iterator drops the source.
#[must_use = "iterators do nothing unless advanced"]
pub struct PageSourceIter<'a> {
    state: SourceState<'a>,
}

impl<'a> PageSourceIter<'a> {
    /// Creates a synchronous page iterator from a source, page size, and aggregate budget.
    pub fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self {
            state: SourceState::new(source, page_size, budget),
        }
    }

    /// Converts this page iterator into a row iterator over the same source.
    pub fn into_rows(self) -> PageSourceRowIter<'a> {
        PageSourceRowIter::from_pages(self)
    }
}

impl Iterator for PageSourceIter<'_> {
    type Item = Result<PagedPage, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.next_page()
    }
}

impl std::fmt::Debug for PageSourceIter<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PageSourceIter")
            .field("page_index", &self.state.pages)
            .field("done", &self.state.finished)
            .finish_non_exhaustive()
    }
}

/// A native synchronous iterator of rows from a [`PageSource`].
///
/// Empty pages produce no row item; the iterator may consume them within one `next` call, bounded
/// by the page budget. Each row carries its page index. Errors are terminal and the iterator does
/// not retry.
#[must_use = "iterators do nothing unless advanced"]
pub struct PageSourceRowIter<'a> {
    pages: PageSourceIter<'a>,
    current: std::collections::VecDeque<Row>,
    current_page: usize,
}

impl<'a> PageSourceRowIter<'a> {
    /// Creates a synchronous row iterator from a source, page size, and aggregate budget.
    pub fn new(source: Box<dyn PageSource + 'a>, page_size: PageSize, budget: PageBudget) -> Self {
        Self::from_pages(PageSourceIter::new(source, page_size, budget))
    }

    fn from_pages(pages: PageSourceIter<'a>) -> Self {
        Self {
            pages,
            current: std::collections::VecDeque::new(),
            current_page: 0,
        }
    }
}

impl Iterator for PageSourceRowIter<'_> {
    type Item = Result<PagedRow, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.pop_front() {
                return Some(Ok(PagedRow::new(self.current_page, row)));
            }
            match self.pages.next() {
                Some(Ok(page)) => {
                    self.current_page = page.page_index();
                    self.current = page.into_rows().into();
                }
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        }
    }
}

impl std::fmt::Debug for PageSourceRowIter<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PageSourceRowIter")
            .field("current_page", &self.current_page)
            .field("buffered_rows", &self.current.len())
            .finish_non_exhaustive()
    }
}
