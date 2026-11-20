//! Contract tests for paged reader views over a scripted, non-materializing reader.

use std::{
    collections::VecDeque,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use futures_util::{StreamExt, TryStreamExt};
use neotales_dbapi::{
    DbError, DbFuture, ErrorKind, Reader, Row, Value,
    paging::{Page, PageSize, PageStream, RowStream, SetRow},
};

fn row(value: i64) -> Row {
    Row::new(vec![("value".to_owned(), Value::I64(value))]).expect("valid row")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
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

#[derive(Debug)]
struct FixtureReader {
    sets: VecDeque<VecDeque<Row>>,
    next_row_error: Option<ErrorKind>,
    advance_error: Option<ErrorKind>,
    finish_error: Option<ErrorKind>,
    next_row_calls: Arc<AtomicUsize>,
    advance_calls: Arc<AtomicUsize>,
    finish_calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl FixtureReader {
    fn new(sets: Vec<Vec<Row>>) -> Self {
        Self::with_counters(
            sets,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    fn with_counters(
        sets: Vec<Vec<Row>>,
        next_row_calls: Arc<AtomicUsize>,
        advance_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            sets: sets.into_iter().map(VecDeque::from).collect(),
            next_row_error: None,
            advance_error: None,
            finish_error: None,
            next_row_calls,
            advance_calls,
            finish_calls,
            drops,
        }
    }

    fn next_row_error(mut self, kind: ErrorKind) -> Self {
        self.next_row_error = Some(kind);
        self
    }

    fn advance_error(mut self, kind: ErrorKind) -> Self {
        self.advance_error = Some(kind);
        self
    }

    fn finish_error(mut self, kind: ErrorKind) -> Self {
        self.finish_error = Some(kind);
        self
    }
}

impl Drop for FixtureReader {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Reader for FixtureReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        self.next_row_calls.fetch_add(1, Ordering::SeqCst);
        let row = self.sets.front_mut().and_then(VecDeque::pop_front);
        if row.is_none()
            && let Some(kind) = self.next_row_error.take()
        {
            return Box::pin(async move { Err(DbError::new(kind)) });
        }
        Box::pin(async move { Ok(row) })
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.advance_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = self.advance_error.take() {
            return Box::pin(async move { Err(DbError::new(kind)) });
        }
        self.sets.pop_front();
        let more = !self.sets.is_empty();
        Box::pin(async move { Ok(more) })
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.finish_calls.fetch_add(1, Ordering::SeqCst);
        self.sets.clear();
        if let Some(kind) = self.finish_error.take() {
            return Box::pin(async move { Err(DbError::new(kind)) });
        }
        Box::pin(async { Ok(()) })
    }
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
    stream: &mut RowStream<'_>,
    waker: &Waker,
) -> Poll<Option<Result<SetRow, DbError>>> {
    let mut next = Box::pin(stream.next());
    let mut context = Context::from_waker(waker);
    next.as_mut().poll(&mut context)
}

struct PendingRow {
    row: Option<Row>,
    polled: bool,
    drops: Arc<AtomicUsize>,
}

impl Future for PendingRow {
    type Output = Result<Option<Row>, DbError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.polled {
            Poll::Ready(Ok(this.row.take()))
        } else {
            this.polled = true;
            Poll::Pending
        }
    }
}

impl Drop for PendingRow {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingReader {
    rows: VecDeque<Row>,
    pending_once: bool,
    next_row_calls: Arc<AtomicUsize>,
    finish_calls: Arc<AtomicUsize>,
    reader_drops: Arc<AtomicUsize>,
    future_drops: Arc<AtomicUsize>,
}

impl PendingReader {
    fn with_counters(
        rows: Vec<Row>,
        next_row_calls: Arc<AtomicUsize>,
        finish_calls: Arc<AtomicUsize>,
        reader_drops: Arc<AtomicUsize>,
        future_drops: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            rows: VecDeque::from(rows),
            pending_once: true,
            next_row_calls,
            finish_calls,
            reader_drops,
            future_drops,
        }
    }
}

impl Drop for PendingReader {
    fn drop(&mut self) {
        self.reader_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl Reader for PendingReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        self.next_row_calls.fetch_add(1, Ordering::SeqCst);
        let Some(row) = self.rows.pop_front() else {
            return Box::pin(async { Ok(None) });
        };
        if std::mem::take(&mut self.pending_once) {
            Box::pin(PendingRow {
                row: Some(row),
                polled: false,
                drops: Arc::clone(&self.future_drops),
            })
        } else {
            Box::pin(async move { Ok(Some(row)) })
        }
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.finish_calls.fetch_add(1, Ordering::SeqCst);
        self.rows.clear();
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn page_view_preserves_set_boundaries_and_empty_sets() {
    let reader = FixtureReader::new(vec![vec![row(1), row(2), row(3)], Vec::new(), vec![row(4)]]);
    let pages: Vec<Page> = block_on(PageStream::new(Box::new(reader), page_size(2)).try_collect())
        .expect("scripted page collection");

    assert_eq!(pages.len(), 4);
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![0, 0, 1, 2]
    );
    assert_eq!(
        pages.iter().map(Page::len).collect::<Vec<_>>(),
        vec![2, 1, 0, 1]
    );
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert_eq!(page_values(&pages[1]), vec![3]);
    assert!(pages[2].is_empty());
    assert_eq!(page_values(&pages[3]), vec![4]);
}

#[test]
fn row_view_keeps_set_identity_and_skips_empty_sets() {
    let reader = FixtureReader::new(vec![vec![row(1), row(2), row(3)], Vec::new(), vec![row(4)]]);
    let rows: Vec<SetRow> =
        block_on(RowStream::new(Box::new(reader)).try_collect()).expect("scripted row collection");

    assert_eq!(set_row_values(&rows), vec![(0, 1), (0, 2), (0, 3), (2, 4)]);

    let reader = FixtureReader::new(vec![vec![row(5)], Vec::new(), vec![row(6)]]);
    let rows: Vec<SetRow> = block_on(
        PageStream::new(Box::new(reader), page_size(2))
            .into_rows()
            .try_collect(),
    )
    .expect("scripted page-to-row collection");
    assert_eq!(set_row_values(&rows), vec![(0, 5), (2, 6)]);
}

#[test]
fn row_stream_wakes_after_each_empty_set_and_preserves_progress() {
    let advances = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![Vec::new(), Vec::new(), vec![row(30)]],
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&advances),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&drops),
    );
    let mut stream = RowStream::new(Box::new(reader));
    let wake_count = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountingWake {
        count: Arc::clone(&wake_count),
    }));

    assert!(matches!(poll_row_next(&mut stream, &waker), Poll::Pending));
    assert_eq!(advances.load(Ordering::SeqCst), 0);
    assert!(wake_count.load(Ordering::SeqCst) > 0);

    assert!(matches!(poll_row_next(&mut stream, &waker), Poll::Pending));
    assert_eq!(advances.load(Ordering::SeqCst), 1);

    let Poll::Ready(Some(Ok(item))) = poll_row_next(&mut stream, &waker) else {
        panic!("the third poll must yield the first row");
    };
    assert_eq!(item.set_index(), 2);
    assert_eq!(item.row().read::<i64>("value").unwrap(), 30);
    assert_eq!(advances.load(Ordering::SeqCst), 2);

    assert!(matches!(
        poll_row_next(&mut stream, &waker),
        Poll::Ready(None)
    ));
    assert_eq!(advances.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_row_stream_after_an_empty_set_stops_immediately() {
    let advances = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![Vec::new(), Vec::new(), vec![row(31)]],
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&advances),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&drops),
    );
    let mut stream = RowStream::new(Box::new(reader));
    let waker = Waker::noop();

    assert!(matches!(poll_row_next(&mut stream, waker), Poll::Pending));
    drop(stream);
    assert_eq!(advances.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn already_advanced_reader_can_supply_its_current_set_index() {
    let mut reader = FixtureReader::new(vec![vec![row(20)], vec![row(21)], vec![row(22)]]);
    let first = block_on(reader.next_row_async())
        .expect("read first set")
        .expect("first row");
    assert_eq!(first.read::<i64>("value").expect("integer value"), 20);
    assert!(
        block_on(reader.has_next_set_async()).expect("advance to the second set"),
        "the fixture has another set"
    );

    let pages: Vec<Page> =
        block_on(PageStream::at_set(Box::new(reader), page_size(2), 1).try_collect())
            .expect("advanced page collection");
    assert_eq!(
        pages.iter().map(Page::set_index).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(page_values(&pages[0]), vec![21]);
    assert_eq!(page_values(&pages[1]), vec![22]);
}

#[test]
fn new_labels_an_advanced_reader_relative_to_the_wrap() {
    let mut reader = FixtureReader::new(vec![vec![row(23)], vec![row(24)]]);
    let _ = block_on(reader.next_row_async())
        .expect("read first set")
        .expect("first row");
    assert!(
        block_on(reader.has_next_set_async()).expect("advance to the second set"),
        "the fixture has another set"
    );

    let pages: Vec<Page> = block_on(PageStream::new(Box::new(reader), page_size(1)).try_collect())
        .expect("view-relative page collection");
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].set_index(), 0);
    assert_eq!(page_values(&pages[0]), vec![24]);
}

#[test]
fn advanced_row_view_can_supply_its_current_set_index() {
    let mut reader = FixtureReader::new(vec![vec![row(25)], vec![row(26)], vec![row(27)]]);
    let _ = block_on(reader.next_row_async())
        .expect("read first set")
        .expect("first row");
    assert!(
        block_on(reader.has_next_set_async()).expect("advance to the second set"),
        "the fixture has another set"
    );

    let rows: Vec<SetRow> = block_on(RowStream::at_set(Box::new(reader), 1).try_collect())
        .expect("advanced row collection");
    assert_eq!(set_row_values(&rows), vec![(1, 26), (2, 27)]);
}

#[test]
fn max_set_label_is_valid_when_it_is_the_last_set() {
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![vec![row(28)]],
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&finish_calls),
        Arc::clone(&drops),
    );
    let pages: Vec<Page> =
        block_on(PageStream::at_set(Box::new(reader), page_size(1), usize::MAX).try_collect())
            .expect("the last set can use the maximum label");
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].set_index(), usize::MAX);
    assert_eq!(page_values(&pages[0]), vec![28]);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    // The empty-set path also ends without needing a later label.
    let pages: Vec<Page> = block_on(
        PageStream::at_set(
            Box::new(FixtureReader::new(vec![Vec::new()])),
            page_size(1),
            usize::MAX,
        )
        .try_collect(),
    )
    .expect("an empty last set can use the maximum label");
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].set_index(), usize::MAX);
    assert!(pages[0].is_empty());
}

#[test]
fn max_set_label_overflows_only_when_a_later_set_exists() {
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![vec![row(29)], vec![row(30)]],
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&finish_calls),
        Arc::clone(&drops),
    );
    let mut stream = PageStream::at_set(Box::new(reader), page_size(1), usize::MAX);

    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(first.set_index(), usize::MAX);
    assert_eq!(page_values(&first), vec![29]);

    let error = block_on(stream.next())
        .expect("overflow item")
        .expect_err("a later set cannot reuse the maximum label");
    assert_eq!(error.kind(), ErrorKind::Overflow);
    assert!(block_on(stream.next()).is_none());

    // Terminal error behavior: the reader is dropped for cleanup and finish_async is not
    // called after the unrepresentable advance.
    assert_eq!(finish_calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn max_set_label_row_view_matches_the_page_boundary() {
    let rows: Vec<SetRow> = block_on(
        RowStream::at_set(
            Box::new(FixtureReader::new(vec![vec![row(31)]])),
            usize::MAX,
        )
        .try_collect(),
    )
    .expect("the last row set can use the maximum label");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].set_index(), usize::MAX);

    let rows: Result<Vec<SetRow>, DbError> = block_on(
        RowStream::at_set(
            Box::new(FixtureReader::new(vec![vec![row(32)], vec![row(33)]])),
            usize::MAX,
        )
        .try_collect(),
    );
    assert_eq!(
        rows.expect_err("a later row set cannot reuse the maximum label")
            .kind(),
        ErrorKind::Overflow
    );
}

#[test]
fn exact_page_multiple_does_not_append_an_empty_trailing_page() {
    let pages: Vec<Page> = block_on(
        PageStream::new(
            Box::new(FixtureReader::new(vec![vec![
                row(1),
                row(2),
                row(3),
                row(4),
            ]])),
            page_size(2),
        )
        .try_collect(),
    )
    .expect("exact-multiple page collection");
    assert_eq!(pages.iter().map(Page::len).collect::<Vec<_>>(), vec![2, 2]);
    assert_eq!(page_values(&pages[0]), vec![1, 2]);
    assert_eq!(page_values(&pages[1]), vec![3, 4]);
}

#[test]
fn empty_result_set_is_one_empty_page_and_no_rows() {
    let pages: Vec<Page> = block_on(
        PageStream::new(Box::new(FixtureReader::new(vec![Vec::new()])), page_size(4)).try_collect(),
    )
    .expect("empty page collection");
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].set_index(), 0);
    assert!(pages[0].is_empty());

    let rows: Vec<SetRow> =
        block_on(RowStream::new(Box::new(FixtureReader::new(vec![Vec::new()]))).try_collect())
            .expect("empty row collection");
    assert_eq!(rows.len(), 0);
}

#[test]
fn later_row_error_becomes_one_error_item_and_ends_the_stream() {
    let reader = FixtureReader::new(vec![vec![row(7)]]).next_row_error(ErrorKind::Timeout);
    let mut stream = PageStream::new(Box::new(reader), page_size(1));

    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![7]);

    let error = block_on(stream.next())
        .expect("error item")
        .expect_err("later row error");
    assert_eq!(error.kind(), ErrorKind::Timeout);

    assert!(block_on(stream.next()).is_none());
}

#[test]
fn advance_and_finish_errors_are_surfaced_after_the_last_page() {
    let reader = FixtureReader::new(vec![vec![row(8)]]).advance_error(ErrorKind::Busy);
    let mut stream = PageStream::new(Box::new(reader), page_size(1));
    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![8]);
    assert_eq!(
        block_on(stream.next())
            .expect("advance error item")
            .expect_err("advance error")
            .kind(),
        ErrorKind::Busy
    );

    let reader = FixtureReader::new(vec![vec![row(9)]]).finish_error(ErrorKind::Connection);
    let mut stream = PageStream::new(Box::new(reader), page_size(1));
    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![9]);
    assert_eq!(
        block_on(stream.next())
            .expect("finish error item")
            .expect_err("finish error")
            .kind(),
        ErrorKind::Connection
    );
    assert!(block_on(stream.next()).is_none());
}

#[test]
fn dropping_a_stream_drops_the_reader_without_finishing_it() {
    let drops = Arc::new(AtomicUsize::new(0));
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![vec![row(10), row(11)]],
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&finish_calls),
        Arc::clone(&drops),
    );
    let mut stream = PageStream::new(Box::new(reader), page_size(1));
    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![10]);
    drop(stream);

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn dropping_a_pending_next_future_resumes_without_duplication_or_loss() {
    let next_row_calls = Arc::new(AtomicUsize::new(0));
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let reader_drops = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::new(AtomicUsize::new(0));
    let reader = PendingReader::with_counters(
        vec![row(30), row(31)],
        Arc::clone(&next_row_calls),
        Arc::clone(&finish_calls),
        Arc::clone(&reader_drops),
        Arc::clone(&future_drops),
    );
    let mut stream = PageStream::new(Box::new(reader), page_size(1));

    {
        let mut next = Box::pin(stream.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(
            matches!(next.as_mut().poll(&mut context), Poll::Pending),
            "the controlled reader's first row future is pending"
        );
    }

    let first = block_on(stream.next())
        .expect("first item")
        .expect("first page");
    assert_eq!(page_values(&first), vec![30]);
    let second = block_on(stream.next())
        .expect("second item")
        .expect("second page");
    assert_eq!(page_values(&second), vec![31]);
    assert!(block_on(stream.next()).is_none());

    // Two rows plus one end-of-set observation; the dropped consumer future did not
    // cause a second `next_row_async` call or discard the pending row.
    assert_eq!(next_row_calls.load(Ordering::SeqCst), 3);
    assert_eq!(future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_a_stream_with_a_pending_operation_cleans_up_the_reader() {
    let next_row_calls = Arc::new(AtomicUsize::new(0));
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let reader_drops = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::new(AtomicUsize::new(0));
    let reader = PendingReader::with_counters(
        vec![row(40)],
        Arc::clone(&next_row_calls),
        Arc::clone(&finish_calls),
        Arc::clone(&reader_drops),
        Arc::clone(&future_drops),
    );
    let mut stream = PageStream::new(Box::new(reader), page_size(1));

    {
        let mut next = Box::pin(stream.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(
            matches!(next.as_mut().poll(&mut context), Poll::Pending),
            "the controlled reader's row future is pending"
        );
    }
    drop(stream);

    assert_eq!(next_row_calls.load(Ordering::SeqCst), 1);
    assert_eq!(future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(reader_drops.load(Ordering::SeqCst), 1);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn normal_completion_finishes_once_and_does_not_read_rows_twice() {
    let drops = Arc::new(AtomicUsize::new(0));
    let next_row_calls = Arc::new(AtomicUsize::new(0));
    let finish_calls = Arc::new(AtomicUsize::new(0));
    let reader = FixtureReader::with_counters(
        vec![vec![row(12), row(13), row(14)]],
        Arc::clone(&next_row_calls),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&finish_calls),
        Arc::clone(&drops),
    );
    let pages: Vec<Page> = block_on(PageStream::new(Box::new(reader), page_size(2)).try_collect())
        .expect("page collection");

    assert_eq!(pages.len(), 2);
    // Three rows plus one end-of-set observation; no second dispatch or re-read.
    assert_eq!(next_row_calls.load(Ordering::SeqCst), 4);
    assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn page_size_is_nonzero_and_streams_are_send_and_unpin() {
    assert_eq!(
        PageSize::try_from(0).expect_err("zero page size").kind(),
        ErrorKind::InvalidArgument
    );
    assert_eq!(PageSize::try_from(1).expect("nonzero page size").get(), 1);
    assert_eq!(PageSize::new(NonZeroUsize::MIN).get(), 1);

    let pages = PageStream::new(Box::new(FixtureReader::new(vec![])), page_size(2));
    assert_send(&pages);
    assert_unpin(&pages);

    let rows = RowStream::new(Box::new(FixtureReader::new(vec![])));
    assert_send(&rows);
    assert_unpin(&rows);
}
