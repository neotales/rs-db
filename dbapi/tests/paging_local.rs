//! Contract tests for local `!Send` paged views and multi-request sources.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    DbError, ErrorKind, Row, Value, local,
    local::paging::{
        Page, PageSize, PageStream, RowStream, SetRow,
        multi::{
            Continuation, PageBudget, PageRequest, PageSource, PageSourceRowStream,
            PageSourceStream, PagedPage, SourcePage, read_single_set,
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

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..10_000 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
    panic!("local future did not resolve");
}

struct CountingWake {
    count: Arc<AtomicUsize>,
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_local_row_next(
    stream: &mut RowStream<'_>,
    waker: &Waker,
) -> Poll<Option<Result<SetRow, DbError>>> {
    let mut next = Box::pin(stream.next());
    let mut context = Context::from_waker(waker);
    next.as_mut().poll(&mut context)
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

struct LocalReader {
    inner: local::BufferedReader,
    finish_error: bool,
    _not_send: Rc<()>,
}

impl LocalReader {
    fn new(sets: Vec<Vec<Row>>, finish_error: bool) -> Self {
        Self {
            inner: local::BufferedReader::from_sets(sets),
            finish_error,
            _not_send: Rc::new(()),
        }
    }
}

impl local::Reader for LocalReader {
    fn next_row_async(&mut self) -> local::DbFuture<'_, Option<Row>> {
        self.inner.next_row_async()
    }

    fn has_next_set_async(&mut self) -> local::DbFuture<'_, bool> {
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> local::DbFuture<'_, ()> {
        if self.finish_error {
            return Box::pin(async { Err(DbError::new(ErrorKind::Busy)) });
        }
        self.inner.finish_async()
    }
}

struct CountingLocalReader {
    inner: local::BufferedReader,
    advances: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    _not_send: Rc<()>,
}

impl CountingLocalReader {
    fn new(sets: Vec<Vec<Row>>, advances: Rc<Cell<usize>>, drops: Rc<Cell<usize>>) -> Self {
        Self {
            inner: local::BufferedReader::from_sets(sets),
            advances,
            drops,
            _not_send: Rc::new(()),
        }
    }
}

impl local::Reader for CountingLocalReader {
    fn next_row_async(&mut self) -> local::DbFuture<'_, Option<Row>> {
        self.inner.next_row_async()
    }

    fn has_next_set_async(&mut self) -> local::DbFuture<'_, bool> {
        self.advances.set(self.advances.get() + 1);
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> local::DbFuture<'_, ()> {
        self.inner.finish_async()
    }
}

impl Drop for CountingLocalReader {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn local_page_and_row_views_preserve_sets_and_empty_sets() {
    let reader = Box::new(LocalReader::new(
        vec![vec![row(1), row(2)], Vec::new(), vec![row(3)]],
        false,
    ));
    let pages: Vec<Page> = block_on(PageStream::new(reader, page_size(2)).try_collect())
        .expect("local page collection");
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

    let reader = Box::new(LocalReader::new(
        vec![vec![row(4), row(5)], Vec::new(), vec![row(6)]],
        false,
    ));
    let rows: Vec<SetRow> =
        block_on(RowStream::new(reader).try_collect()).expect("local row collection");
    assert_eq!(set_row_values(&rows), vec![(0, 4), (0, 5), (2, 6)]);
}

#[test]
fn local_row_stream_wakes_after_each_empty_set_and_preserves_progress() {
    let advances = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let reader = CountingLocalReader::new(
        vec![Vec::new(), Vec::new(), vec![row(32)]],
        Rc::clone(&advances),
        Rc::clone(&drops),
    );
    let mut stream = RowStream::new(Box::new(reader));
    let wake_count = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountingWake {
        count: Arc::clone(&wake_count),
    }));

    assert!(matches!(
        poll_local_row_next(&mut stream, &waker),
        Poll::Pending
    ));
    assert_eq!(advances.get(), 0);
    assert!(wake_count.load(Ordering::SeqCst) > 0);

    assert!(matches!(
        poll_local_row_next(&mut stream, &waker),
        Poll::Pending
    ));
    assert_eq!(advances.get(), 1);

    let Poll::Ready(Some(Ok(item))) = poll_local_row_next(&mut stream, &waker) else {
        panic!("the third poll must yield the first row");
    };
    assert_eq!(item.set_index(), 2);
    assert_eq!(item.row().read::<i64>("value").unwrap(), 32);
    assert_eq!(advances.get(), 2);

    assert!(matches!(
        poll_local_row_next(&mut stream, &waker),
        Poll::Ready(None)
    ));
    assert_eq!(advances.get(), 3);
    assert_eq!(drops.get(), 1);
}

#[test]
fn dropping_local_row_stream_after_an_empty_set_stops_immediately() {
    let advances = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let reader = CountingLocalReader::new(
        vec![Vec::new(), Vec::new(), vec![row(33)]],
        Rc::clone(&advances),
        Rc::clone(&drops),
    );
    let mut stream = RowStream::new(Box::new(reader));
    let waker = Waker::noop();

    assert!(matches!(
        poll_local_row_next(&mut stream, waker),
        Poll::Pending
    ));
    drop(stream);
    assert_eq!(advances.get(), 0);
    assert_eq!(drops.get(), 1);
}

#[test]
fn local_finish_error_is_terminal() {
    let reader = Box::new(LocalReader::new(vec![vec![row(7)]], true));
    let mut stream = PageStream::new(reader, page_size(1));
    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![7]);
    let error = block_on(stream.next())
        .expect("finish error item")
        .expect_err("late finish error");
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert!(block_on(stream.next()).is_none());
}

#[test]
fn local_page_stream_labels_and_overflow_are_view_relative() {
    let reader = Box::new(LocalReader::new(vec![vec![row(17)], vec![row(18)]], false));
    let pages: Vec<Page> = block_on(PageStream::at_set(reader, page_size(1), 5).try_collect())
        .expect("labelled local pages");
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![5, 6]
    );

    let reader = Box::new(LocalReader::new(vec![vec![row(19)], vec![row(20)]], false));
    let mut stream = PageStream::at_set(reader, page_size(1), usize::MAX);
    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(first.set_index(), usize::MAX);
    assert_eq!(
        block_on(stream.next())
            .expect("overflow item")
            .expect_err("a later set cannot reuse the label")
            .kind(),
        ErrorKind::Overflow
    );
    assert!(block_on(stream.next()).is_none());
}

#[test]
fn local_read_single_set_rejects_extra_sets_and_late_errors() {
    let reader = Box::new(LocalReader::new(vec![vec![row(20)], vec![row(21)]], false));
    assert_eq!(
        block_on(read_single_set(reader))
            .expect_err("two sets must be rejected")
            .kind(),
        ErrorKind::Decode
    );

    let reader = Box::new(LocalReader::new(vec![vec![row(22)]], true));
    assert_eq!(
        block_on(read_single_set(reader))
            .expect_err("the late finish error must win")
            .kind(),
        ErrorKind::Busy
    );
}

type LocalToken = Rc<String>;

struct ScriptedPage {
    rows: Vec<Row>,
    continuation: Option<Continuation>,
}

fn page(rows: Vec<Row>, next: Option<&str>) -> ScriptedPage {
    ScriptedPage {
        rows,
        continuation: next.map(|value| Continuation::new(Rc::new(value.to_owned()))),
    }
}

struct LocalSource {
    pages: VecDeque<ScriptedPage>,
    seen: Rc<RefCell<Vec<Option<String>>>>,
    calls: Rc<Cell<usize>>,
    _not_send: Rc<()>,
}

impl LocalSource {
    fn new(pages: Vec<ScriptedPage>) -> Self {
        Self {
            pages: VecDeque::from(pages),
            seen: Rc::new(RefCell::new(Vec::new())),
            calls: Rc::new(Cell::new(0)),
            _not_send: Rc::new(()),
        }
    }
}

impl PageSource for LocalSource {
    fn next_page_async(
        &mut self,
        _request: PageRequest,
        continuation: Option<Continuation>,
    ) -> local::DbFuture<'_, SourcePage> {
        self.calls.set(self.calls.get() + 1);
        Box::pin(async move {
            match continuation {
                Some(token) => {
                    let token = token
                        .downcast::<LocalToken>()
                        .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
                    self.seen.borrow_mut().push(Some((*token).clone()));
                }
                None => self.seen.borrow_mut().push(None),
            }
            let page = self
                .pages
                .pop_front()
                .ok_or_else(|| DbError::new(ErrorKind::Decode))?;
            Ok(SourcePage::new(page.rows, page.continuation))
        })
    }
}

fn page_values_for_source(page: &PagedPage) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

#[test]
fn local_page_source_preserves_empty_pages_and_rc_tokens() {
    let source = LocalSource::new(vec![
        page(vec![row(8), row(9)], Some("first")),
        page(Vec::new(), Some("second")),
        page(vec![row(10)], None),
    ]);
    let seen = Rc::clone(&source.seen);
    let calls = Rc::clone(&source.calls);
    let pages: Vec<PagedPage> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 4096, 10)).try_collect(),
    )
    .expect("local source collection");
    assert_eq!(
        pages.iter().map(PagedPage::page_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        pages.iter().map(PagedPage::len).collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    assert_eq!(page_values_for_source(&pages[0]), vec![8, 9]);
    assert!(pages[1].is_empty());
    assert_eq!(page_values_for_source(&pages[2]), vec![10]);
    assert_eq!(
        seen.borrow().clone(),
        vec![None, Some("first".to_owned()), Some("second".to_owned())]
    );
    assert_eq!(calls.get(), 3);

    let source = LocalSource::new(vec![
        page(vec![row(11), row(12)], Some("first")),
        page(Vec::new(), Some("second")),
        page(vec![row(13)], None),
    ]);
    let rows: Vec<neotales_dbapi::paging::multi::PagedRow> = block_on(
        PageSourceRowStream::new(Box::new(source), page_size(2), budget(10, 4096, 10))
            .try_collect(),
    )
    .expect("local source row collection");
    assert_eq!(
        rows.iter()
            .map(|item| {
                (
                    item.page_index(),
                    item.row().read::<i64>("value").expect("integer value"),
                )
            })
            .collect::<Vec<_>>(),
        vec![(0, 11), (0, 12), (2, 13)]
    );
}

#[test]
fn local_source_exact_bound_fails_without_redispatch() {
    let source = LocalSource::new(vec![
        page(vec![row(14), row(15)], Some("first")),
        page(vec![row(16)], None),
    ]);
    let calls = Rc::clone(&source.calls);
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(2, 4096, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("the exact bound with a token must fail closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn local_source_empty_pages_and_byte_bound_are_enforced() {
    let source = LocalSource::new(vec![
        page(Vec::new(), Some("first")),
        page(Vec::new(), Some("second")),
        page(Vec::new(), None),
    ]);
    let calls = Rc::clone(&source.calls);
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 2)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("empty pages count against the page bound")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.get(), 2);

    let source = LocalSource::new(vec![
        page(vec![row(23)], Some("first")),
        page(vec![row(24)], None),
    ]);
    let calls = Rc::clone(&source.calls);
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 13, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("the exact byte bound with a token fails closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(calls.get(), 1);
}

struct PendingLocalPage {
    polled: bool,
    drops: Rc<Cell<usize>>,
}

impl Future for PendingLocalPage {
    type Output = Result<SourcePage, DbError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.polled {
            Poll::Ready(Ok(SourcePage::new(Vec::new(), None)))
        } else {
            this.polled = true;
            Poll::Pending
        }
    }
}

impl Drop for PendingLocalPage {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

struct PendingLocalSource {
    drops: Rc<Cell<usize>>,
    future_drops: Rc<Cell<usize>>,
    _not_send: Rc<()>,
}

impl PageSource for PendingLocalSource {
    fn next_page_async(
        &mut self,
        _request: PageRequest,
        _continuation: Option<Continuation>,
    ) -> local::DbFuture<'_, SourcePage> {
        Box::pin(PendingLocalPage {
            polled: false,
            drops: Rc::clone(&self.future_drops),
        })
    }
}

impl Drop for PendingLocalSource {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn local_pending_source_drop_cancels_with_cleanup() {
    let drops = Rc::new(Cell::new(0));
    let future_drops = Rc::new(Cell::new(0));
    let source = PendingLocalSource {
        drops: Rc::clone(&drops),
        future_drops: Rc::clone(&future_drops),
        _not_send: Rc::new(()),
    };
    let mut stream = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));
    {
        let mut next = Box::pin(stream.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(next.as_mut().poll(&mut context), Poll::Pending));
    }
    drop(stream);
    assert_eq!(drops.get(), 1);
    assert_eq!(future_drops.get(), 1);
}
