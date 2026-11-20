//! Contract tests for native synchronous page/row iterators and multi-request sources.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use neotales_dbapi::{
    DbError, ErrorKind, Row, Value,
    sync::{
        self,
        paging::{
            Page, PageIter, PageSize, RowIter, SetRow,
            multi::{
                Continuation, PageBudget, PageRequest, PageSource, PageSourceIter,
                PageSourceRowIter, PagedPage, PagedRow, SourcePage, read_single_set,
            },
        },
    },
};

fn row(value: i64) -> Row {
    Row::new(vec![("value".to_owned(), Value::I64(value))]).expect("valid row")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn budget(rows: usize, bytes: usize, max_pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, max_pages).expect("valid budget")
}

fn page_values(page: &Page) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

fn set_row_values(rows: &[SetRow]) -> Vec<(usize, i64)> {
    rows.iter()
        .map(|item| {
            (
                item.set_index(),
                item.row().read::<i64>("value").expect("integer value"),
            )
        })
        .collect()
}

fn paged_values(page: &PagedPage) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

#[test]
fn sync_page_and_row_iterators_preserve_sets_and_empty_sets() {
    let sets = vec![vec![row(1), row(2)], Vec::new(), vec![row(3)]];
    let pages: Vec<Page> = PageIter::new(
        Box::new(sync::BufferedReader::from_sets(sets.clone())),
        page_size(2),
    )
    .collect::<Result<_, DbError>>()
    .expect("page iterator");
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        pages.iter().map(Page::len).collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert!(pages[1].is_empty());
    assert_eq!(page_values(&pages[2]), vec![3]);

    let rows: Vec<SetRow> = RowIter::new(Box::new(sync::BufferedReader::from_sets(sets)))
        .collect::<Result<_, DbError>>()
        .expect("row iterator");
    assert_eq!(set_row_values(&rows), vec![(0, 1), (0, 2), (2, 3)]);
}

struct FinishErrorReader {
    inner: sync::BufferedReader,
}

impl sync::Reader for FinishErrorReader {
    fn next_row(&mut self) -> Result<Option<Row>, DbError> {
        self.inner.next_row()
    }

    fn has_next_set(&mut self) -> Result<bool, DbError> {
        self.inner.has_next_set()
    }

    fn finish(&mut self) -> Result<(), DbError> {
        self.inner.finish()?;
        Err(DbError::new(ErrorKind::Busy))
    }
}

#[test]
fn sync_finish_error_is_terminal() {
    let reader = Box::new(FinishErrorReader {
        inner: sync::BufferedReader::new(vec![row(4)]),
    });
    let mut pages = PageIter::new(reader, page_size(1));
    let first = pages.next().expect("first item").expect("first page");
    assert_eq!(page_values(&first), vec![4]);
    assert_eq!(
        pages
            .next()
            .expect("finish error item")
            .expect_err("late finish error")
            .kind(),
        ErrorKind::Busy
    );
    assert!(pages.next().is_none());
}

#[test]
fn sync_page_iterator_labels_and_overflow_are_view_relative() {
    let reader = sync::BufferedReader::from_sets(vec![vec![row(40)], vec![row(41)]]);
    let pages: Vec<Page> = PageIter::at_set(Box::new(reader), page_size(1), 5)
        .collect::<Result<_, DbError>>()
        .expect("labelled pages");
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![5, 6]
    );

    let reader = sync::BufferedReader::from_sets(vec![vec![row(42)], vec![row(43)]]);
    let mut pages = PageIter::at_set(Box::new(reader), page_size(1), usize::MAX);
    let first = pages.next().expect("first item").expect("first page");
    assert_eq!(first.set_index(), usize::MAX);
    assert_eq!(
        pages
            .next()
            .expect("overflow item")
            .expect_err("a later set cannot reuse the label")
            .kind(),
        ErrorKind::Overflow
    );
    assert!(pages.next().is_none());
}

#[test]
fn sync_read_single_set_rejects_extra_sets_and_late_errors() {
    let reader = Box::new(sync::BufferedReader::from_sets(vec![
        vec![row(30)],
        vec![row(31)],
    ]));
    assert_eq!(
        read_single_set(reader)
            .expect_err("two sets must be rejected")
            .kind(),
        ErrorKind::Decode
    );

    let reader = Box::new(FinishErrorReader {
        inner: sync::BufferedReader::new(vec![row(32)]),
    });
    assert_eq!(
        read_single_set(reader)
            .expect_err("the late finish error must win")
            .kind(),
        ErrorKind::Busy
    );
}

struct DropReader {
    inner: sync::BufferedReader,
    drops: Arc<AtomicUsize>,
    finishes: Arc<AtomicUsize>,
}

impl sync::Reader for DropReader {
    fn next_row(&mut self) -> Result<Option<Row>, DbError> {
        self.inner.next_row()
    }

    fn has_next_set(&mut self) -> Result<bool, DbError> {
        self.inner.has_next_set()
    }

    fn finish(&mut self) -> Result<(), DbError> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        self.inner.finish()
    }
}

impl Drop for DropReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn dropping_a_sync_page_iterator_drops_the_reader_without_finishing() {
    let drops = Arc::new(AtomicUsize::new(0));
    let finishes = Arc::new(AtomicUsize::new(0));
    let reader = DropReader {
        inner: sync::BufferedReader::new(vec![row(5), row(6)]),
        drops: Arc::clone(&drops),
        finishes: Arc::clone(&finishes),
    };
    let mut pages = PageIter::new(Box::new(reader), page_size(1));
    let first = pages.next().expect("first item").expect("first page");
    assert_eq!(page_values(&first), vec![5]);
    drop(pages);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}

struct ScriptedPage {
    rows: Vec<Row>,
    continuation: Option<Continuation>,
}

fn page(rows: Vec<Row>, next: Option<usize>) -> ScriptedPage {
    ScriptedPage {
        rows,
        continuation: next.map(Continuation::new),
    }
}

struct ScriptedSource {
    pages: VecDeque<ScriptedPage>,
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl PageSource for ScriptedSource {
    fn next_page(
        &mut self,
        _request: PageRequest,
        continuation: Option<Continuation>,
    ) -> Result<SourcePage, DbError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let seen = match continuation {
            Some(token) => Some(
                token
                    .downcast::<usize>()
                    .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?,
            ),
            None => None,
        };
        self.seen.lock().unwrap().push(seen);
        let page = self
            .pages
            .pop_front()
            .ok_or_else(|| DbError::new(ErrorKind::Decode))?;
        Ok(SourcePage::new(page.rows, page.continuation))
    }
}

fn sync_source(pages: Vec<ScriptedPage>) -> ScriptedSource {
    ScriptedSource {
        pages: VecDeque::from(pages),
        calls: Arc::new(AtomicUsize::new(0)),
        seen: Arc::new(Mutex::new(Vec::new())),
    }
}

#[test]
fn sync_page_source_preserves_empty_pages_and_tokens() {
    let source = sync_source(vec![
        page(vec![row(7), row(8)], Some(1)),
        page(Vec::new(), Some(2)),
        page(vec![row(9)], None),
    ]);
    let calls = Arc::clone(&source.calls);
    let seen = Arc::clone(&source.seen);
    let pages: Vec<PagedPage> =
        PageSourceIter::new(Box::new(source), page_size(2), budget(10, 4096, 10))
            .collect::<Result<_, DbError>>()
            .expect("page source iterator");
    assert_eq!(
        pages.iter().map(PagedPage::page_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        pages.iter().map(PagedPage::len).collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    assert_eq!(paged_values(&pages[0]), vec![7, 8]);
    assert!(pages[1].is_empty());
    assert_eq!(paged_values(&pages[2]), vec![9]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(seen.lock().unwrap().clone(), vec![None, Some(1), Some(2)]);

    let rows: Vec<PagedRow> = PageSourceRowIter::new(
        Box::new(sync_source(vec![
            page(vec![row(10)], Some(1)),
            page(Vec::new(), Some(2)),
            page(vec![row(11)], None),
        ])),
        page_size(1),
        budget(10, 4096, 10),
    )
    .collect::<Result<_, DbError>>()
    .expect("row source iterator");
    assert_eq!(
        rows.iter()
            .map(|item| {
                (
                    item.page_index(),
                    item.row().read::<i64>("value").expect("integer value"),
                )
            })
            .collect::<Vec<_>>(),
        vec![(0, 10), (2, 11)]
    );
}

#[test]
fn sync_source_exact_bound_fails_without_redispatch() {
    let source = sync_source(vec![
        page(vec![row(12), row(13)], Some(1)),
        page(vec![row(14)], None),
    ]);
    let calls = Arc::clone(&source.calls);
    let mut pages = PageSourceIter::new(Box::new(source), page_size(2), budget(2, 4096, 10));
    let first = pages.next().expect("first item").expect("first page");
    assert_eq!(paged_values(&first), vec![12, 13]);
    assert_eq!(
        pages
            .next()
            .expect("continuation at exact bound")
            .expect_err("must fail closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert!(pages.next().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn sync_source_empty_pages_and_byte_bound_are_enforced() {
    let source = sync_source(vec![
        page(Vec::new(), Some(1)),
        page(Vec::new(), Some(2)),
        page(Vec::new(), None),
    ]);
    let calls = Arc::clone(&source.calls);
    let result: Result<Vec<PagedPage>, DbError> =
        PageSourceIter::new(Box::new(source), page_size(1), budget(10, 4096, 2)).collect();
    assert_eq!(
        result
            .expect_err("empty pages count against the page bound")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let source = sync_source(vec![
        page(vec![row(33)], Some(1)),
        page(vec![row(34)], None),
    ]);
    let calls = Arc::clone(&source.calls);
    let result: Result<Vec<PagedPage>, DbError> =
        PageSourceIter::new(Box::new(source), page_size(1), budget(10, 13, 10)).collect();
    assert_eq!(
        result
            .expect_err("the exact byte bound with a token fails closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct ErrorSource {
    calls: Arc<AtomicUsize>,
}

impl PageSource for ErrorSource {
    fn next_page(
        &mut self,
        _request: PageRequest,
        _continuation: Option<Continuation>,
    ) -> Result<SourcePage, DbError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(DbError::new(ErrorKind::Busy))
    }
}

#[test]
fn sync_source_error_is_terminal_and_not_retried() {
    let calls = Arc::new(AtomicUsize::new(0));
    let source = ErrorSource {
        calls: Arc::clone(&calls),
    };
    let mut pages = PageSourceIter::new(Box::new(source), page_size(1), budget(10, 4096, 10));
    assert_eq!(
        pages
            .next()
            .expect("error item")
            .expect_err("source error")
            .kind(),
        ErrorKind::Busy
    );
    assert!(pages.next().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
