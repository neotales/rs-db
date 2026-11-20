//! Contract tests for explicit multi-request `PageSource` paging.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    BufferedReader, DbError, DbFuture, ErrorKind, Reader, Row, Value,
    paging::{
        PageSize,
        multi::{
            Continuation, PageBudget, PageRequest, PageSource, PageSourceRowStream,
            PageSourceStream, PagedPage, PagedRow, SourcePage, read_single_set,
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
    PageBudget::new(rows, bytes, max_pages).expect("valid page budget")
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
    panic!("scripted future did not resolve");
}

fn page_values(page: &PagedPage) -> Vec<i64> {
    page.rows()
        .iter()
        .map(|row| row.read::<i64>("value").expect("integer value"))
        .collect()
}

fn row_values(rows: &[PagedRow]) -> Vec<(usize, i64)> {
    rows.iter()
        .map(|item| {
            (
                item.page_index(),
                item.row().read::<i64>("value").expect("integer value"),
            )
        })
        .collect()
}

fn assert_send<T: Send>(_: &T) {}
fn assert_unpin<T: Unpin>(_: &T) {}

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

fn poll_row_next(
    stream: &mut PageSourceRowStream<'_>,
    waker: &Waker,
) -> Poll<Option<Result<PagedRow, DbError>>> {
    let mut next = Box::pin(stream.next());
    let mut context = Context::from_waker(waker);
    next.as_mut().poll(&mut context)
}

#[derive(Default)]
struct Counters {
    calls: AtomicUsize,
    drops: AtomicUsize,
    future_drops: AtomicUsize,
    seen: Mutex<Vec<Option<usize>>>,
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
    counters: Arc<Counters>,
    pending_once: bool,
    error: Option<ErrorKind>,
}

impl ScriptedSource {
    fn new(pages: Vec<ScriptedPage>) -> Self {
        Self {
            pages: VecDeque::from(pages),
            counters: Arc::new(Counters::default()),
            pending_once: false,
            error: None,
        }
    }

    fn pending_once(mut self) -> Self {
        self.pending_once = true;
        self
    }

    fn error(mut self, kind: ErrorKind) -> Self {
        self.error = Some(kind);
        self
    }

    fn counters(&self) -> Arc<Counters> {
        Arc::clone(&self.counters)
    }
}

impl Drop for ScriptedSource {
    fn drop(&mut self) {
        self.counters.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingSourcePage {
    page: Option<ScriptedPage>,
    polled: bool,
    counters: Arc<Counters>,
}

impl Future for PendingSourcePage {
    type Output = Result<SourcePage, DbError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.polled {
            let page = this.page.take().expect("pending source page");
            Poll::Ready(Ok(SourcePage::new(page.rows, page.continuation)))
        } else {
            this.polled = true;
            Poll::Pending
        }
    }
}

impl Drop for PendingSourcePage {
    fn drop(&mut self) {
        self.counters.future_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl PageSource for ScriptedSource {
    fn next_page_async(
        &mut self,
        _request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage> {
        self.counters.calls.fetch_add(1, Ordering::SeqCst);
        let seen = match continuation {
            Some(token) => match token.downcast::<usize>() {
                Ok(value) => Some(value),
                Err(_) => {
                    return Box::pin(async { Err(DbError::new(ErrorKind::InvalidArgument)) });
                }
            },
            None => None,
        };
        self.counters.seen.lock().unwrap().push(seen);

        if let Some(kind) = self.error.take() {
            return Box::pin(async move { Err(DbError::new(kind)) });
        }

        let Some(page) = self.pages.pop_front() else {
            return Box::pin(async { Err(DbError::new(ErrorKind::Decode)) });
        };
        if self.pending_once {
            self.pending_once = false;
            return Box::pin(PendingSourcePage {
                page: Some(page),
                polled: false,
                counters: Arc::clone(&self.counters),
            });
        }

        let ScriptedPage { rows, continuation } = page;
        Box::pin(async move { Ok(SourcePage::new(rows, continuation)) })
    }
}

fn scripted_pages() -> Vec<ScriptedPage> {
    vec![
        page(vec![row(1), row(2)], Some(7)),
        page(Vec::new(), Some(8)),
        page(vec![row(3)], None),
    ]
}

#[test]
fn page_and_row_views_preserve_page_indices_and_continuations() {
    let source = ScriptedSource::new(scripted_pages());
    let counters = source.counters();
    let pages: Vec<PagedPage> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(10, 4096, 10)).try_collect(),
    )
    .expect("page collection");
    assert_eq!(
        pages.iter().map(PagedPage::page_index).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        pages.iter().map(PagedPage::len).collect::<Vec<_>>(),
        vec![2, 0, 1]
    );
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert!(pages[1].is_empty());
    assert_eq!(page_values(&pages[2]), vec![3]);
    assert_eq!(counters.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        counters.seen.lock().unwrap().clone(),
        vec![None, Some(7), Some(8)]
    );

    let source = ScriptedSource::new(scripted_pages());
    let rows: Vec<PagedRow> = block_on(
        PageSourceRowStream::new(Box::new(source), page_size(2), budget(10, 4096, 10))
            .try_collect(),
    )
    .expect("row collection");
    assert_eq!(row_values(&rows), vec![(0, 1), (0, 2), (2, 3)]);
}

#[test]
fn row_stream_makes_one_request_per_poll_across_empty_pages() {
    let source = ScriptedSource::new(vec![
        page(Vec::new(), Some(0)),
        page(Vec::new(), Some(1)),
        page(vec![row(1)], Some(2)),
        page(vec![row(2)], None),
    ]);
    let counters = source.counters();
    let mut stream = PageSourceRowStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));
    let wake_count = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountingWake {
        count: Arc::clone(&wake_count),
    }));

    assert!(matches!(poll_row_next(&mut stream, &waker), Poll::Pending));
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
    assert!(wake_count.load(Ordering::SeqCst) > 0);

    assert!(matches!(poll_row_next(&mut stream, &waker), Poll::Pending));
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);

    let Poll::Ready(Some(Ok(first))) = poll_row_next(&mut stream, &waker) else {
        panic!("the third poll must yield the first row");
    };
    assert_eq!(first.page_index(), 2);
    assert_eq!(first.row().read::<i64>("value").unwrap(), 1);
    assert_eq!(counters.calls.load(Ordering::SeqCst), 3);

    let Poll::Ready(Some(Ok(second))) = poll_row_next(&mut stream, &waker) else {
        panic!("the fourth poll must yield the second row");
    };
    assert_eq!(second.page_index(), 3);
    assert_eq!(second.row().read::<i64>("value").unwrap(), 2);
    assert_eq!(counters.calls.load(Ordering::SeqCst), 4);

    assert!(matches!(
        poll_row_next(&mut stream, &waker),
        Poll::Ready(None)
    ));
    assert_eq!(counters.calls.load(Ordering::SeqCst), 4);
}

#[test]
fn dropping_row_stream_after_an_empty_page_stops_requests() {
    let source = ScriptedSource::new(vec![page(Vec::new(), Some(0)), page(vec![row(1)], None)]);
    let counters = source.counters();
    let mut stream = PageSourceRowStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));
    let waker = Waker::noop();

    assert!(matches!(poll_row_next(&mut stream, waker), Poll::Pending));
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
    drop(stream);

    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counters.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn pages_are_requested_lazily_without_prefetch() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], Some(0)), page(vec![row(2)], None)]);
    let counters = source.counters();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));

    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![1]);
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);

    let second = block_on(stream.next())
        .expect("second item")
        .expect("second page");
    assert_eq!(page_values(&second), vec![2]);
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);

    assert!(block_on(stream.next()).is_none());
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn page_size_is_enforced_and_terminal() {
    let source = ScriptedSource::new(vec![page(vec![row(1), row(2), row(3)], Some(0))]);
    let counters = source.counters();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(2), budget(10, 4096, 10));

    let error = block_on(stream.next())
        .expect("error item")
        .expect_err("a source page larger than the requested page size is a shape error");
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(block_on(stream.next()).is_none());
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn aggregate_row_bound_is_checked_across_pages() {
    let source = ScriptedSource::new(vec![
        page(vec![row(1), row(2)], Some(0)),
        page(vec![row(3), row(4)], None),
    ]);
    let counters = source.counters();
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(3, 4096, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("the second page exceeds the row bound")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn exact_row_bound_with_continuation_fails_without_dispatch() {
    let source = ScriptedSource::new(vec![
        page(vec![row(1), row(2)], Some(0)),
        page(vec![row(3)], None),
    ]);
    let counters = source.counters();
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(2), budget(2, 4096, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("a continuation at the exact row bound must fail closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn exact_byte_bound_with_continuation_fails_without_dispatch() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], Some(0)), page(vec![row(2)], None)]);
    let counters = source.counters();
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 13, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("a continuation at the exact byte bound must fail closed")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn aggregate_byte_bound_is_checked_across_pages() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], Some(0)), page(vec![row(2)], None)]);
    let counters = source.counters();
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 20, 10)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("the second page exceeds the byte bound")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn empty_pages_count_against_the_page_bound() {
    let source = ScriptedSource::new(vec![
        page(Vec::new(), Some(0)),
        page(Vec::new(), Some(1)),
        page(Vec::new(), None),
    ]);
    let counters = source.counters();
    let result: Result<Vec<PagedPage>, DbError> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 2)).try_collect(),
    );
    assert_eq!(
        result
            .expect_err("empty pages still consume the page bound")
            .kind(),
        ErrorKind::Overflow
    );
    assert_eq!(counters.calls.load(Ordering::SeqCst), 2);

    let source = ScriptedSource::new(vec![page(Vec::new(), None)]);
    let pages: Vec<PagedPage> = block_on(
        PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 1)).try_collect(),
    )
    .expect("a final empty page completes without a false failure");
    assert_eq!(pages.len(), 1);
    assert!(pages[0].is_empty());
    assert_eq!(pages[0].page_index(), 0);
}

#[test]
fn source_error_is_terminal_and_not_retried() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], None)]).error(ErrorKind::Busy);
    let counters = source.counters();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));

    let error = block_on(stream.next())
        .expect("error item")
        .expect_err("source error");
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert!(block_on(stream.next()).is_none());
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_a_pending_source_stream_cancels_without_dispatch() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], None)]).pending_once();
    let counters = source.counters();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));

    {
        let mut next = Box::pin(stream.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(
            matches!(next.as_mut().poll(&mut context), Poll::Pending),
            "the controlled source future is pending"
        );
    }
    drop(stream);

    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counters.drops.load(Ordering::SeqCst), 1);
    assert_eq!(counters.future_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_a_pending_consumer_next_future_resumes_without_loss() {
    let source = ScriptedSource::new(vec![page(vec![row(1)], None)]).pending_once();
    let counters = source.counters();
    let mut stream = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 10));

    {
        let mut next = Box::pin(stream.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(next.as_mut().poll(&mut context), Poll::Pending));
    }

    let page = block_on(stream.next())
        .expect("page item")
        .expect("page after resume");
    assert_eq!(page_values(&page), vec![1]);
    assert!(block_on(stream.next()).is_none());
    assert_eq!(counters.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counters.future_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn continuation_is_redacted_and_round_trips_by_type() {
    let token = Continuation::new(String::from("secret-cursor"));
    let rendered = format!("{token:?}");
    assert_eq!(rendered, "Continuation(redacted)");
    assert!(!rendered.contains("secret"));
    assert!(token.is::<String>());
    assert_eq!(
        token.downcast::<String>().expect("string token"),
        "secret-cursor"
    );

    let token = Continuation::new(7usize);
    let token = token.downcast::<String>().expect_err("wrong token type");
    assert!(token.is::<usize>());

    let page = SourcePage::new(
        vec![row(1)],
        Some(Continuation::new(String::from("secret-cursor"))),
    );
    let rendered = format!("{page:?}");
    assert!(!rendered.contains("secret-cursor"));
}

struct FinishErrorReader {
    inner: BufferedReader,
}

impl Reader for FinishErrorReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        self.inner.next_row_async()
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Busy)) })
    }
}

#[test]
fn read_single_set_rejects_extra_sets_and_surfaces_late_errors() {
    let rows =
        block_on(read_single_set(Box::new(BufferedReader::new(vec![row(1)])))).expect("single set");
    assert_eq!(rows.len(), 1);

    let reader = Box::new(BufferedReader::from_sets(vec![vec![row(1)], vec![row(2)]]));
    assert_eq!(
        block_on(read_single_set(reader))
            .expect_err("two sets must be rejected")
            .kind(),
        ErrorKind::Decode
    );

    let reader = Box::new(FinishErrorReader {
        inner: BufferedReader::new(vec![row(3)]),
    });
    assert_eq!(
        block_on(read_single_set(reader))
            .expect_err("the late finish error must win")
            .kind(),
        ErrorKind::Busy
    );
}

#[test]
fn budget_is_nonzero_and_streams_are_send_and_unpin() {
    assert_eq!(
        PageBudget::new(0, 1, 1).expect_err("zero rows").kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(PageBudget::default().max_pages(), 100);

    let source = ScriptedSource::new(vec![page(Vec::new(), None)]);
    let pages = PageSourceStream::new(Box::new(source), page_size(1), budget(10, 4096, 1));
    assert_send(&pages);
    assert_unpin(&pages);

    let source = ScriptedSource::new(vec![page(Vec::new(), None)]);
    let rows = PageSourceRowStream::new(Box::new(source), page_size(1), budget(10, 4096, 1));
    assert_send(&rows);
    assert_unpin(&rows);
}
