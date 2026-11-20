//! Experimental D1-only materialized requery paging.
//!
//! **Experimental, and specific to Cloudflare D1.** This is not
//! [`neotales_dbapi::paging`], [`neotales_dbapi::paging::multi`], or
//! [`neotales_dbapi::local::paging`]: it does not implement a reader, session, transaction, or
//! `PageSource`, and it does not make D1's fail-closed [`neotales_dbapi::local::Pool`] contract
//! succeed. It is a separate capability for one narrow case: reading a bounded sequence of pages by
//! re-executing one caller-supplied, caller-asserted read-only statement with new caller-supplied
//! bindings.
//!
//! # What it does
//!
//! A pager fixes one SQL string. For each page it sends exactly one request: the fixed SQL prepared
//! with the bindings the caller's [`D1PageContinuation`] returned. It never rewrites the SQL,
//! appends or removes a `LIMIT`, inspects a statement prefix, executes more than one statement per
//! request, retries, replays, prefetches, or speaks for cancellation. The stream asks for a page
//! only when it is polled, and it is terminal after an error.
//!
//! # Materialization, not a memory bound
//!
//! D1 materializes a whole page in the JavaScript isolate before Rust receives it. [`PageSize`] and
//! [`PageBudget`] are enforced after materialization and before a page is published, so they bound
//! what the caller receives and what Rust retains; they are **not** a pre-materialization allocation
//! limit and they cannot stop D1 from building a larger intermediate result. Exceeding a bound fails
//! with [`ErrorKind::Overflow`] and publishes nothing; exceeding [`PageSize`] fails with
//! [`ErrorKind::Decode`]. No page is ever silently truncated.
//!
//! The aggregate counters cover every delivered page, including empty pages, so a continuation that
//! keeps returning bindings cannot run forever. When a bound is already exact and the caller's
//! continuation still wants another page, the next poll fails closed with `Overflow` instead of
//! dispatching it.
//!
//! # The caller's assertion
//!
//! [`D1ReadOnlyReplaySafe::assert`] is an explicit assertion by the caller that the fixed statement
//! is read-only and safe to execute again. The pager does not parse, classify, or verify the SQL, and
//! it deliberately does not check a statement prefix or keyword. The caller also owns SQL selection
//! and correctness across pages, snapshot and ordering consistency between requests, and the scope,
//! sensitivity, and validation of anything its continuation keeps. Continuations are never printed
//! or included in an error.
//!
//! # Unknown write outcome
//!
//! Because the assertion cannot be verified, a request that is dispatched and does not produce an
//! authoritative result is reported as [`ErrorKind::WriteOutcomeUnknown`], never as a retryable
//! transport failure and never as proof that nothing was written. Dropping the stream or its
//! in-flight future does not cancel the dispatched promise; no further request is made, and the
//! outcome of that request remains unknown. There is no cancellation claim to rely on.
//!
//! # Local futures
//!
//! Pages are delivered as a `!Send` [`futures_core::Stream`] of `Result<D1PagedPage, DbError>`, and
//! every operation uses [`neotales_dbapi::local::DbFuture`]. Poll it on the thread that created it; a
//! D1 Workers promise is not `Send`.
//!
//! ```compile_fail
//! use neotales_dbapi::paging::{PageSize, multi::PageBudget};
//! use neotales_dbapi_d1::paging::{D1PageContinuation, D1PageStream, D1PagedPage};
//!
//! fn require_send<T: Send>(_: T) {}
//!
//! struct Stop;
//! impl D1PageContinuation for Stop {
//!     fn next_parameters(
//!         &mut self,
//!         _: Option<&D1PagedPage>,
//!     ) -> Result<Option<Vec<neotales_dbapi::Parameter>>, neotales_dbapi::DbError> {
//!         Ok(None)
//!     }
//! }
//!
//! // A host-only type cannot be constructed; the assertion is that the stream's type is not `Send`
//! // even when its continuation is.
//! fn check<C: D1PageContinuation>(stream: D1PageStream<'static, C>) {
//!     require_send(stream);
//! }
//! ```
//!
//! # Example
//!
//! The continuation, bounds, and assertion types compile on every target, so a caller can keep its
//! keyset logic in a shared module and test it without a binding:
//!
//! ```no_run
//! use neotales_dbapi::{
//!     DbError, DbType, Parameter, Value,
//!     paging::{PageSize, multi::PageBudget},
//! };
//! use neotales_dbapi_d1::paging::{D1PageContinuation, D1PagedPage, D1ReadOnlyReplaySafe};
//!
//! struct Keyset {
//!     after: i64,
//!     page_rows: i64,
//! }
//!
//! impl D1PageContinuation for Keyset {
//!     fn next_parameters(
//!         &mut self,
//!         previous: Option<&D1PagedPage>,
//!     ) -> Result<Option<Vec<Parameter>>, DbError> {
//!         if let Some(page) = previous {
//!             if page.is_empty() {
//!                 return Ok(None);
//!             }
//!             if let Some(row) = page.rows().last() {
//!                 self.after = row.get::<i64>("id")?;
//!             }
//!         }
//!         Ok(Some(vec![
//!             Parameter::new(DbType::I64, Value::I64(self.after))?,
//!             Parameter::new(DbType::I64, Value::I64(self.page_rows))?,
//!         ]))
//!     }
//! }
//!
//! fn bounds() -> Result<(PageSize, PageBudget), DbError> {
//!     Ok((PageSize::try_from(25)?, PageBudget::new(100, 65_536, 10)?))
//! }
//!
//! # let _ = D1ReadOnlyReplaySafe::assert();
//! ```
//!
//! An empty page ends the sequence; without that check a keyset continuation would keep requesting
//! pages for the same key. Ending on a short page avoids the terminating empty request **only when the
//! final page is actually short**. If the data ends on an exact full-page boundary, the page length
//! cannot tell the continuation that there are no more rows: it must either know completion
//! independently, or leave headroom for one more request. That probe is still a request, and the
//! pager checks the aggregate bounds before dispatching it, so `max_pages` must allow one more page
//! and the row and byte budgets must be strictly greater than the delivered data; an exact row or
//! byte total also fails closed with `Overflow` before the probe.
//!
//! On `wasm32`, a pager is created from the binding and polled as a page stream. This is a sketch,
//! not a doctest: the D1 dispatcher is absent from a host build, so it cannot be compiled here.
//!
//! ```text
//! let pager = pool.pager(
//!     "SELECT id, name FROM users WHERE id > ? ORDER BY id LIMIT ?",
//!     D1ReadOnlyReplaySafe::assert(),
//!     PageSize::try_from(25)?,
//!     PageBudget::new(100, 65_536, 10)?,
//! );
//! let pages: Vec<D1PagedPage> = pager
//!     .pages(Keyset { after: 0, page_rows: 25 })
//!     .try_collect()
//!     .await?;
//! ```

// The dispatcher is wasm-only; a host build keeps this state machine for its unit tests.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use neotales_dbapi::{
    DbError, ErrorKind, Parameter, Row, local,
    paging::{PageSize, multi::PageBudget},
};

/// The caller's explicit read-only and replay-safe assertion for one fixed statement.
///
/// Constructing this value states that the statement it is paired with in `D1Pool::pager` is
/// read-only and safe to execute more than once for its data. The library does not verify that: it
/// does not parse SQL, inspect a prefix, or classify a statement. Passing a statement that can write
/// is a contract violation.
///
/// There is no `Clone` or `Copy`: an assertion is consumed by the pager it authorizes.
#[derive(Debug)]
pub struct D1ReadOnlyReplaySafe(());

impl D1ReadOnlyReplaySafe {
    /// Asserts that the paired statement is read-only and replay-safe.
    ///
    /// This is the caller's assertion, not a check. See the [module documentation](self).
    #[must_use]
    pub const fn assert() -> Self {
        Self(())
    }
}

/// Produces the positional bindings for each page request.
///
/// The pager calls this once before the first request with `None`, and once after each materialized
/// page with that page. `Some(bindings)` dispatches or continues; `None` ends the sequence after the
/// page before it, or with no request at all for the first call.
///
/// The continuation owns the continuation logic: offset arithmetic, keyset state, or any caller
/// token. It must produce bindings whose placeholder count matches the fixed SQL; the pager validates
/// that before dispatch but cannot infer whether the resulting SQL selection is correct. It must not
/// depend on a request being repeated: the pager never retries.
///
/// It is called for a materialized page **before** that page is published, so a page is never
/// retained or cloned and no request is made early. If it fails, that materialized page is discarded
/// and the stream is terminal. A continuation must not perform I/O or dispatch of its own.
///
/// A continuation must make progress towards termination. A keyset continuation that reuses the same
/// key when a page is empty repeats that request until the page budget fails with `Overflow`; end the
/// sequence when the page is empty, or when it is shorter than the requested page size. A short page
/// avoids the extra empty probe only when the data does not end exactly on a page boundary; a full
/// final page needs independent completion knowledge or budget headroom for one more request.
///
/// # Errors
///
/// A returned error is propagated as the stream's terminal error.
pub trait D1PageContinuation {
    /// Returns the bindings for the next request, or `None` to stop.
    ///
    /// # Errors
    /// Returns a sanitized error that terminates the sequence.
    fn next_parameters(
        &mut self,
        previous: Option<&D1PagedPage>,
    ) -> Result<Option<Vec<Parameter>>, DbError>;
}

impl<F> D1PageContinuation for F
where
    F: FnMut(Option<&D1PagedPage>) -> Result<Option<Vec<Parameter>>, DbError>,
{
    fn next_parameters(
        &mut self,
        previous: Option<&D1PagedPage>,
    ) -> Result<Option<Vec<Parameter>>, DbError> {
        self(previous)
    }
}

/// One materialized page delivered by a [`D1PageStream`].
///
/// [`D1PagedPage::page_index`] is the zero-based request position in this stream, not a row offset,
/// a database identity, or a snapshot identifier. Empty pages are delivered and counted.
#[derive(Clone, Debug, PartialEq)]
pub struct D1PagedPage {
    page_index: usize,
    rows: Vec<Row>,
}

impl D1PagedPage {
    fn new(page_index: usize, rows: Vec<Row>) -> Self {
        Self { page_index, rows }
    }

    /// Returns the zero-based page/request position.
    #[must_use]
    pub const fn page_index(&self) -> usize {
        self.page_index
    }

    /// Returns the page's rows in result order.
    #[must_use]
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the number of rows in this page.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns whether this page has no rows.
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

/// The one-request dispatch used by a pager.
///
/// It is separated from the stream so the bound and continuation state machine is testable without a
/// D1 binding. An implementation must make exactly one request per call.
pub(crate) trait Materialize {
    fn materialize_async(&self, parameters: Vec<Parameter>) -> local::DbFuture<'_, Vec<Row>>;
}

struct PageState<'a, C> {
    materializer: Box<dyn Materialize + 'a>,
    continuation: C,
    page_size: PageSize,
    budget: PageBudget,
    /// Bindings for the next request, or `None` once the continuation has ended the sequence.
    request: Option<Vec<Parameter>>,
    started: bool,
    pages: usize,
    rows: usize,
    bytes: usize,
    finished: bool,
}

impl<'a, C: D1PageContinuation + 'a> PageState<'a, C> {
    fn new(
        materializer: Box<dyn Materialize + 'a>,
        continuation: C,
        page_size: PageSize,
        budget: PageBudget,
    ) -> Self {
        Self {
            materializer,
            continuation,
            page_size,
            budget,
            request: None,
            started: false,
            pages: 0,
            rows: 0,
            bytes: 0,
            finished: false,
        }
    }

    fn fail(mut self, error: DbError) -> (Self, Option<Result<D1PagedPage, DbError>>) {
        self.finished = true;
        (self, Some(Err(error)))
    }

    async fn next_page(mut self) -> (Self, Option<Result<D1PagedPage, DbError>>) {
        if self.finished {
            return (self, None);
        }

        if !self.started {
            match self.continuation.next_parameters(None) {
                Ok(Some(parameters)) => self.request = Some(parameters),
                Ok(None) => {
                    self.finished = true;
                    return (self, None);
                }
                Err(error) => return self.fail(error),
            }
            self.started = true;
        }

        let Some(parameters) = self.request.take() else {
            self.finished = true;
            return (self, None);
        };

        // A request is ready, so the caller's continuation wants another page. At an exact bound the
        // sequence fails closed instead of dispatching one page too many.
        if self.pages >= self.budget.max_pages()
            || self.rows >= self.budget.rows()
            || self.bytes >= self.budget.bytes()
        {
            return self.fail(DbError::new(ErrorKind::Overflow));
        }

        let page_rows = match self.materializer.materialize_async(parameters).await {
            Ok(rows) => rows,
            Err(error) => return self.fail(error),
        };

        if page_rows.len() > self.page_size.get() {
            return self.fail(DbError::new(ErrorKind::Decode));
        }
        let page_bytes = match encoded_page_bytes(&page_rows) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        let Some(next_rows) = self.rows.checked_add(page_rows.len()) else {
            return self.fail(DbError::new(ErrorKind::Overflow));
        };
        if next_rows > self.budget.rows() {
            return self.fail(DbError::new(ErrorKind::Overflow));
        }
        let Some(next_bytes) = self.bytes.checked_add(page_bytes) else {
            return self.fail(DbError::new(ErrorKind::Overflow));
        };
        if next_bytes > self.budget.bytes() {
            return self.fail(DbError::new(ErrorKind::Overflow));
        }
        let Some(next_pages) = self.pages.checked_add(1) else {
            return self.fail(DbError::new(ErrorKind::Overflow));
        };
        if next_pages > self.budget.max_pages() {
            return self.fail(DbError::new(ErrorKind::Overflow));
        }

        let page_index = self.pages;
        let page = D1PagedPage::new(page_index, page_rows);
        match self.continuation.next_parameters(Some(&page)) {
            Ok(next) => self.request = next,
            Err(error) => return self.fail(error),
        }

        self.pages = next_pages;
        self.rows = next_rows;
        self.bytes = next_bytes;
        (self, Some(Ok(page)))
    }
}

fn encoded_page_bytes(rows: &[Row]) -> Result<usize, DbError> {
    rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))
    })
}

type StepFuture<'a, C> =
    Pin<Box<dyn Future<Output = (PageState<'a, C>, Option<Result<D1PagedPage, DbError>>)> + 'a>>;

/// A `!Send` fallible stream of materialized requery pages.
///
/// Each poll dispatches at most one request, and only when the caller's continuation supplied
/// bindings for it. The stream never retries, replays, or prefetches; it is terminal after an error.
/// Dropping it drops any in-flight local future, which does not cancel the dispatched D1 promise.
///
/// The stream publishes a page only after [`PageSize`] and every aggregate [`PageBudget`] bound has
/// been checked against the materialized rows. See the [module documentation](self) for why those are
/// publication bounds and not allocation bounds.
#[must_use = "streams do nothing unless polled"]
pub struct D1PageStream<'a, C: D1PageContinuation + 'a> {
    state: Option<PageState<'a, C>>,
    step: Option<StepFuture<'a, C>>,
    done: bool,
}

// The stream never pins its state: the state and each step future are owned values that are moved
// out with `Option::take`. Declaring `Unpin` is therefore sound, and it lets callers poll with
// `Pin::new` without requiring their continuation to be `Unpin`.
impl<'a, C: D1PageContinuation + 'a> Unpin for D1PageStream<'a, C> {}

impl<'a, C: D1PageContinuation + 'a> D1PageStream<'a, C> {
    pub(crate) fn new(
        materializer: Box<dyn Materialize + 'a>,
        continuation: C,
        page_size: PageSize,
        budget: PageBudget,
    ) -> Self {
        Self {
            state: Some(PageState::new(
                materializer,
                continuation,
                page_size,
                budget,
            )),
            step: None,
            done: false,
        }
    }
}

impl<'a, C: D1PageContinuation + 'a> fmt::Debug for D1PageStream<'a, C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("D1PageStream")
            .field("page_index", &self.state.as_ref().map(|state| state.pages))
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<'a, C: D1PageContinuation + 'a> Stream for D1PageStream<'a, C> {
    type Item = Result<D1PagedPage, DbError>;

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

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use futures_core::Stream;

    use neotales_dbapi::{
        DbError, DbType, ErrorKind, Parameter, Row, Value, local,
        paging::{PageSize, multi::PageBudget},
    };

    use super::{D1PageContinuation, D1PageStream, D1PagedPage, Materialize};

    type Responses = VecDeque<Result<Vec<Row>, DbError>>;

    /// Unit test, not reachable without a D1 binding: this fake stands in for exactly one request.
    struct FakeMaterializer {
        calls: Rc<RefCell<Vec<Vec<Parameter>>>>,
        responses: RefCell<Responses>,
    }

    impl Materialize for FakeMaterializer {
        fn materialize_async(&self, parameters: Vec<Parameter>) -> local::DbFuture<'_, Vec<Row>> {
            self.calls.borrow_mut().push(parameters);
            let response = self
                .responses
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| Ok(Vec::new()));
            Box::pin(async move { response })
        }
    }

    type Calls = Rc<RefCell<Vec<Vec<Parameter>>>>;

    fn fake(responses: Vec<Result<Vec<Row>, DbError>>) -> (Calls, Box<FakeMaterializer>) {
        let fake = Box::new(FakeMaterializer {
            calls: Rc::new(RefCell::new(Vec::new())),
            responses: RefCell::new(responses.into()),
        });
        (Rc::clone(&fake.calls), fake)
    }

    fn row(id: i64) -> Row {
        Row::new(vec![
            ("id".into(), Value::I64(id)),
            ("name".into(), Value::Text(format!("row-{id}"))),
        ])
        .expect("row")
    }

    fn int(value: i64) -> Parameter {
        Parameter::new(DbType::I64, Value::I64(value)).expect("parameter")
    }

    fn budget(rows: usize, bytes: usize, pages: usize) -> PageBudget {
        PageBudget::new(rows, bytes, pages).expect("bounds")
    }

    fn stream<'a, C: D1PageContinuation + 'a>(
        materializer: Box<dyn Materialize + 'a>,
        continuation: C,
        page_size: usize,
        budget: PageBudget,
    ) -> D1PageStream<'a, C> {
        D1PageStream::new(
            materializer,
            continuation,
            PageSize::try_from(page_size).expect("non-zero"),
            budget,
        )
    }

    fn poll_next<C: D1PageContinuation>(
        stream: &mut D1PageStream<'_, C>,
    ) -> Poll<Option<Result<D1PagedPage, DbError>>> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Pin::new(stream).poll_next(&mut context)
    }

    fn collect<C: D1PageContinuation>(
        mut stream: D1PageStream<'_, C>,
    ) -> Vec<Result<D1PagedPage, DbError>> {
        let mut items = Vec::new();
        loop {
            match poll_next(&mut stream) {
                Poll::Ready(Some(item)) => items.push(item),
                Poll::Ready(None) => return items,
                Poll::Pending => panic!("fake materializer must be ready"),
            }
        }
    }

    fn bindings(calls: &Calls) -> Vec<Vec<(DbType, Value)>> {
        calls
            .borrow()
            .iter()
            .map(|call| {
                call.iter()
                    .map(|parameter| (parameter.db_type(), parameter.value().clone()))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn requery_terminates_after_a_short_final_page_without_looping() {
        let (calls, fake) = fake(vec![
            Ok(vec![row(1), row(2)]),
            Ok(vec![row(3), row(4)]),
            Ok(vec![row(5)]),
            Ok(Vec::new()),
        ]);
        let invocations = Rc::new(Cell::new(0usize));
        let counted = Rc::clone(&invocations);
        let mut after = 0i64;
        let continuation = move |previous: Option<&D1PagedPage>| {
            counted.set(counted.get() + 1);
            if let Some(page) = previous {
                if page.is_empty() {
                    return Ok(None);
                }
                if let Some(row) = page.rows().last() {
                    after = row.get::<i64>("id").expect("id");
                }
            }
            Ok(Some(vec![int(after), int(2)]))
        };

        let pages = collect(stream(fake, continuation, 2, budget(10, 65_536, 10)));
        assert_eq!(pages.len(), 4);
        for (index, page) in pages.iter().enumerate() {
            let page = page.as_ref().expect("page");
            assert_eq!(page.page_index(), index);
        }
        assert_eq!(pages[2].as_ref().expect("short page").len(), 1);
        assert!(pages[3].as_ref().expect("terminal page").is_empty());
        assert_eq!(
            invocations.get(),
            5,
            "the initial call plus one after each of four pages"
        );
        assert_eq!(
            bindings(&calls),
            vec![
                vec![(DbType::I64, Value::I64(0)), (DbType::I64, Value::I64(2))],
                vec![(DbType::I64, Value::I64(2)), (DbType::I64, Value::I64(2))],
                vec![(DbType::I64, Value::I64(4)), (DbType::I64, Value::I64(2))],
                vec![(DbType::I64, Value::I64(5)), (DbType::I64, Value::I64(2))],
            ]
        );
    }

    #[test]
    fn a_continuation_that_stops_first_makes_no_request() {
        let (calls, fake) = fake(vec![]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok::<_, DbError>(None),
            1,
            budget(1, 1, 1),
        ));
        assert_eq!(pages.len(), 0);
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn an_empty_page_is_delivered_and_counted() {
        let (calls, fake) = fake(vec![Ok(Vec::new())]);
        let requests = Rc::new(Cell::new(0usize));
        let counted = Rc::clone(&requests);
        let continuation = move |previous: Option<&D1PagedPage>| {
            counted.set(counted.get() + 1);
            if previous.is_none() {
                return Ok(Some(vec![int(0)]));
            }
            Ok(None)
        };
        let pages = collect(stream(fake, continuation, 2, budget(2, 10, 2)));
        assert_eq!(pages.len(), 1);
        let page = pages[0].as_ref().expect("page");
        assert_eq!(page.len(), 0);
        assert_eq!(page.page_index(), 0);
        assert_eq!(calls.borrow().len(), 1);
        assert_eq!(
            requests.get(),
            2,
            "one call for the request and one after the page"
        );
    }

    #[test]
    fn a_page_larger_than_the_page_size_is_decoded_as_an_error_not_truncated() {
        let (calls, fake) = fake(vec![Ok(vec![row(1), row(2), row(3)])]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 10),
        ));
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].as_ref().expect_err("rejected").kind(),
            ErrorKind::Decode
        );
        assert_eq!(calls.borrow().len(), 1);
    }

    #[test]
    fn an_exact_row_budget_with_a_continuation_fails_closed_before_dispatch() {
        let (calls, fake) = fake(vec![Ok(vec![row(1), row(2)])]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(2, 100, 10),
        ));
        assert_eq!(pages.len(), 2);
        assert!(pages[0].is_ok());
        assert_eq!(
            pages[1].as_ref().expect_err("bounded").kind(),
            ErrorKind::Overflow
        );
        assert_eq!(calls.borrow().len(), 1, "no request past the exact bound");
    }

    #[test]
    fn stopping_on_a_short_page_avoids_the_budget_exhausting_empty_probe() {
        // The recommended keyset strategy: end on a short page, so an exact aggregate bound still
        // completes instead of failing closed before a terminating empty request.
        let (calls, fake) = fake(vec![Ok(vec![row(1)]), Ok(Vec::new())]);
        let pages = collect(stream(
            fake,
            |previous: Option<&D1PagedPage>| {
                if let Some(page) = previous
                    && (page.is_empty() || page.len() < 2)
                {
                    return Ok(None);
                }
                Ok(Some(vec![int(0)]))
            },
            2,
            budget(1, 100, 10),
        ));
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_ok());
        assert_eq!(
            calls.borrow().len(),
            1,
            "the exact budget still permits the only request"
        );
    }

    #[test]
    fn a_full_final_page_needs_headroom_for_the_empty_probe() {
        // Two full pages: page length cannot signal completion, so the sequence needs one more
        // request. Budget headroom (rows 5 > 4, pages 3 > 2) permits that empty probe.
        let (calls, fake) = fake(vec![
            Ok(vec![row(1), row(2)]),
            Ok(vec![row(3), row(4)]),
            Ok(Vec::new()),
        ]);
        let pages = collect(stream(
            fake,
            |previous: Option<&D1PagedPage>| {
                if previous.is_some_and(D1PagedPage::is_empty) {
                    return Ok(None);
                }
                Ok(Some(vec![int(0)]))
            },
            2,
            budget(5, 100, 3),
        ));
        assert_eq!(pages.len(), 3);
        assert!(pages[0].is_ok() && pages[1].is_ok());
        assert!(pages[2].as_ref().expect("probe").is_empty());
        assert_eq!(calls.borrow().len(), 3);
    }

    #[test]
    fn an_exact_page_budget_with_a_continuation_fails_closed_before_the_empty_probe() {
        let (calls, fake) = fake(vec![Ok(vec![row(1)])]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 1),
        ));
        assert_eq!(pages.len(), 2);
        assert!(pages[0].is_ok());
        assert_eq!(
            pages[1].as_ref().expect_err("bounded").kind(),
            ErrorKind::Overflow
        );
        assert_eq!(
            calls.borrow().len(),
            1,
            "the empty probe is not dispatched past an exact page bound"
        );
    }

    #[test]
    fn the_page_budget_counts_empty_pages() {
        let (calls, fake) = fake(vec![Ok(Vec::new())]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 1),
        ));
        assert_eq!(pages.len(), 2);
        assert!(pages[0].is_ok());
        assert_eq!(
            pages[1].as_ref().expect_err("bounded").kind(),
            ErrorKind::Overflow
        );
        assert_eq!(calls.borrow().len(), 1);
    }

    #[test]
    fn an_exact_byte_budget_with_a_continuation_fails_closed_before_dispatch() {
        // One row costs 2 + 8 for "id" and 4 + 5 for "name", so 19 bytes is exact.
        let (calls, fake) = fake(vec![Ok(vec![row(1)]), Ok(vec![row(2)])]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 19, 10),
        ));
        assert_eq!(pages.len(), 2);
        assert!(pages[0].is_ok());
        assert_eq!(
            pages[1].as_ref().expect_err("bounded").kind(),
            ErrorKind::Overflow
        );
        assert_eq!(calls.borrow().len(), 1, "no request past the exact bound");
    }

    #[test]
    fn a_page_over_the_byte_budget_is_not_published() {
        // One row costs 2 + 8 for "id" and 4 + 5 for "name", so this budget admits no row.
        let (_, fake) = fake(vec![Ok(vec![row(1)])]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 18, 10),
        ));
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].as_ref().expect_err("bounded").kind(),
            ErrorKind::Overflow
        );
    }

    #[test]
    fn a_materializer_error_is_terminal_and_not_retried() {
        let (calls, fake) = fake(vec![Err(DbError::new(ErrorKind::WriteOutcomeUnknown))]);
        let pages = collect(stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 10),
        ));
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].as_ref().expect_err("uncertain").kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        assert_eq!(calls.borrow().len(), 1);
    }

    #[test]
    fn a_continuation_error_discards_the_materialized_page() {
        let (calls, fake) = fake(vec![Ok(vec![row(1)])]);
        let seen = Rc::new(Cell::new(0usize));
        let counted = Rc::clone(&seen);
        let continuation = move |previous: Option<&D1PagedPage>| {
            counted.set(counted.get() + usize::from(previous.is_some()));
            if previous.is_some() {
                return Err(DbError::new(ErrorKind::InvalidArgument));
            }
            Ok(Some(vec![int(0)]))
        };
        let pages = collect(stream(fake, continuation, 2, budget(10, 100, 10)));
        assert_eq!(pages.len(), 1);
        assert_eq!(
            pages[0].as_ref().expect_err("continuation failed").kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(seen.get(), 1);
        assert_eq!(calls.borrow().len(), 1);
    }

    #[test]
    fn bindings_are_not_dispatched_until_the_next_poll() {
        let (calls, fake) = fake(vec![Ok(vec![row(1)]), Ok(vec![row(2)])]);
        let mut stream = stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 10),
        );
        assert!(matches!(poll_next(&mut stream), Poll::Ready(Some(Ok(_)))));
        assert_eq!(calls.borrow().len(), 1, "no prefetched request");
        assert!(matches!(poll_next(&mut stream), Poll::Ready(Some(Ok(_)))));
        assert_eq!(calls.borrow().len(), 2);
    }

    #[test]
    fn debug_output_does_not_include_bindings_or_continuation_state() {
        let (_, fake) = fake(vec![Ok(vec![row(1)])]);
        let sentinel = "sentinel-continuation-token";
        let continuation = move |_: Option<&D1PagedPage>| {
            Ok(Some(vec![
                Parameter::new(DbType::Text, Value::Text(sentinel.to_owned())).expect("parameter"),
            ]))
        };
        let mut stream = stream(fake, continuation, 2, budget(10, 100, 10));
        assert!(matches!(poll_next(&mut stream), Poll::Ready(Some(Ok(_)))));
        let debug = format!("{stream:?}");
        assert!(debug.contains("D1PageStream"));
        assert!(!debug.contains(sentinel));
        assert!(!debug.contains("row-1"));
    }

    #[test]
    fn dropping_an_unpolled_stream_makes_no_request() {
        let (calls, fake) = fake(vec![]);
        let stream = stream(
            fake,
            |_: Option<&D1PagedPage>| Ok(Some(vec![int(0)])),
            2,
            budget(10, 100, 10),
        );
        drop(stream);
        assert!(calls.borrow().is_empty());
    }
}
