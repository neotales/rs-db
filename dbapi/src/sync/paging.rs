//! Native synchronous paged views over one existing [`Reader`].
//!
//! These are blocking iterators, not futures. They own one [`Reader`] and preserve its result-set
//! boundaries, empty sets, hard limits, terminal errors, and cleanup rules. No runtime or
//! `block_on` is used. A `crate::blocking::BlockingReader` implements [`crate::sync::Reader`], so
//! it works here naturally; the blocking adapter keeps its own worker/drain lifecycle.
//!
//! The iterators use the same [`Page`], [`PageSize`], and [`SetRow`] values as the asynchronous
//! views. Normal completion drains and calls [`Reader::finish`]; dropping early drops the reader
//! without finishing it. An error is terminal.

pub mod multi;

use std::{collections::VecDeque, fmt, num::NonZeroUsize};

use super::Reader;
use crate::{DbError, ErrorKind, Row};

pub use crate::paging::{Page, PageSize, SetRow};

/// A synchronous iterator of pages from one reader.
///
/// Each result set, including an empty one, yields at least one page. Pages never cross result-set
/// boundaries. `set_index` is a view-relative label starting at `0` for [`PageIter::new`] or at the
/// supplied value for [`PageIter::at_set`]. A later set that cannot be labelled yields `Overflow`.
#[must_use = "iterators do nothing unless advanced"]
pub struct PageIter<'a> {
    reader: Option<Box<dyn Reader + 'a>>,
    page_size: PageSize,
    set_index: usize,
    set_end_seen: bool,
    set_has_emitted: bool,
    done: bool,
}

impl<'a> PageIter<'a> {
    /// Wraps one existing reader with a page view whose labels start at `0`.
    pub fn new(reader: Box<dyn Reader + 'a>, page_size: PageSize) -> Self {
        Self::at_set(reader, page_size, 0)
    }

    /// Wraps one existing reader with the current set labelled `set_index`.
    ///
    /// A current set at `usize::MAX` completes normally when it is last; a later set yields a
    /// terminal `Overflow`.
    pub fn at_set(reader: Box<dyn Reader + 'a>, page_size: PageSize, set_index: usize) -> Self {
        Self {
            reader: Some(reader),
            page_size,
            set_index,
            set_end_seen: false,
            set_has_emitted: false,
            done: false,
        }
    }

    /// Converts this page iterator into a row iterator over the same reader.
    pub fn into_rows(self) -> RowIter<'a> {
        RowIter::from_pages(self)
    }

    fn fail(&mut self, error: DbError) -> Result<Page, DbError> {
        self.done = true;
        self.reader = None;
        Err(error)
    }

    fn finish(&mut self) -> Option<Result<Page, DbError>> {
        self.done = true;
        let mut reader = self.reader.take()?;
        match reader.finish() {
            Ok(()) => None,
            Err(error) => Some(Err(error)),
        }
    }

    fn next_page(&mut self) -> Option<Result<Page, DbError>> {
        if self.done {
            return None;
        }

        loop {
            if self.set_end_seen {
                let next_set = {
                    let Some(reader) = self.reader.as_mut() else {
                        self.done = true;
                        return None;
                    };
                    reader.has_next_set()
                };
                match next_set {
                    Ok(true) => {
                        let Some(next_set_index) = self.set_index.checked_add(1) else {
                            return Some(self.fail(DbError::new(ErrorKind::Overflow)));
                        };
                        self.set_index = next_set_index;
                        self.set_end_seen = false;
                        self.set_has_emitted = false;
                    }
                    Ok(false) => return self.finish(),
                    Err(error) => return Some(self.fail(error)),
                }
            }

            let mut rows = Vec::new();
            while rows.len() < self.page_size.get() {
                let next_row = {
                    let Some(reader) = self.reader.as_mut() else {
                        self.done = true;
                        return None;
                    };
                    reader.next_row()
                };
                match next_row {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => {
                        self.set_end_seen = true;
                        if rows.is_empty() && self.set_has_emitted {
                            break;
                        }
                        self.set_has_emitted = true;
                        return Some(Ok(Page::new(self.set_index, rows)));
                    }
                    Err(error) => return Some(self.fail(error)),
                }
            }

            if rows.is_empty() {
                // The set ended after an earlier page; advance on the next iteration.
                continue;
            }

            self.set_has_emitted = true;
            return Some(Ok(Page::new(self.set_index, rows)));
        }
    }
}

impl Iterator for PageIter<'_> {
    type Item = Result<Page, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_page()
    }
}

impl fmt::Debug for PageIter<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageIter")
            .field("set_index", &self.set_index)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

/// A synchronous iterator of rows from one reader.
///
/// Empty pages produce no row item. Each [`SetRow`] carries its view-relative set index. The
/// iterator is blocking and owns the reader; dropping it early follows the reader's normal drop
/// cleanup.
#[must_use = "iterators do nothing unless advanced"]
pub struct RowIter<'a> {
    pages: PageIter<'a>,
    current: VecDeque<Row>,
    current_set: usize,
}

impl<'a> RowIter<'a> {
    /// Wraps one existing reader with a row view whose labels start at `0`.
    pub fn new(reader: Box<dyn Reader + 'a>) -> Self {
        Self::from_pages(PageIter::new(reader, PageSize::new(NonZeroUsize::MIN)))
    }

    /// Wraps one existing reader with the current set labelled `set_index`.
    pub fn at_set(reader: Box<dyn Reader + 'a>, set_index: usize) -> Self {
        Self::from_pages(PageIter::at_set(
            reader,
            PageSize::new(NonZeroUsize::MIN),
            set_index,
        ))
    }

    fn from_pages(pages: PageIter<'a>) -> Self {
        Self {
            pages,
            current: VecDeque::new(),
            current_set: 0,
        }
    }
}

impl Iterator for RowIter<'_> {
    type Item = Result<SetRow, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.pop_front() {
                return Some(Ok(SetRow::new(self.current_set, row)));
            }
            match self.pages.next() {
                Some(Ok(page)) => {
                    self.current_set = page.set_index();
                    self.current = page.into_rows().into();
                }
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        }
    }
}

impl fmt::Debug for RowIter<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RowIter")
            .field("current_set", &self.current_set)
            .field("buffered_rows", &self.current.len())
            .finish_non_exhaustive()
    }
}
