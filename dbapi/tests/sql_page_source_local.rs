//! Local-future contract test for the caller-authored SQL source.
//!
//! The fake session, plan, and continuation deliberately hold `Rc` state. Compiling this file proves
//! the local adapter does not require `Send`; it does not claim any D1 operation.

use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_util::TryStreamExt;
use neotales_dbapi::{
    DbError, DbType, Delay, ErrorKind, LimitSetting, Parameter, QueryOptions, RetryPolicy,
    RetrySetting, Row, RowLimit, TxOptions, Value, local,
    local::paging::{
        PageSize,
        multi::{
            PageBudget, PageRequest, PagedPage,
            sql::{SqlPageAssertion, SqlPagePlan, SqlPageSource},
        },
    },
};

const SQL: &str = "SELECT tenant, seq FROM events \
                   WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";

#[derive(Debug)]
struct Recorded {
    sql: String,
    options: QueryOptions,
}

struct FakeSession {
    responses: RefCell<VecDeque<Vec<Vec<Row>>>>,
    recorded: Rc<RefCell<Vec<Recorded>>>,
    marker: Rc<()>,
}

impl local::Session for FakeSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> local::DbFuture<'a, u64> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Reader + 'a>> {
        self.recorded.borrow_mut().push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let sets = self
            .responses
            .borrow_mut()
            .pop_front()
            .expect("scripted response");
        let marker = Rc::clone(&self.marker);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(marker);
            Ok(Box::new(local::BufferedReader::from_sets(sets)) as Box<dyn local::Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn begin_async<'a>(
        &'a mut self,
        _: TxOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

struct BorrowedLocalSession<'a> {
    responses: &'a RefCell<VecDeque<Vec<Vec<Row>>>>,
    recorded: &'a RefCell<Vec<Recorded>>,
    marker: &'a Rc<()>,
}

impl local::Session for BorrowedLocalSession<'_> {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> local::DbFuture<'a, u64> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Reader + 'a>> {
        self.recorded.borrow_mut().push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let sets = self
            .responses
            .borrow_mut()
            .pop_front()
            .expect("scripted response");
        let marker = Rc::clone(self.marker);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(marker);
            Ok(Box::new(local::BufferedReader::from_sets(sets)) as Box<dyn local::Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, ()> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }

    fn begin_async<'a>(
        &'a mut self,
        _: TxOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

#[derive(Debug)]
struct LocalToken {
    source: u64,
    after: (i64, i64),
    marker: Rc<()>,
}

struct LocalPlan {
    source: u64,
    start: (i64, i64),
    marker: Rc<()>,
}

impl LocalPlan {
    fn resolve(
        &self,
        continuation: Option<local::paging::multi::Continuation>,
    ) -> Result<(i64, i64), DbError> {
        let Some(token) = continuation else {
            return Ok(self.start);
        };
        let token = token
            .downcast::<LocalToken>()
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
        if token.source != self.source {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if !Rc::ptr_eq(&token.marker, &self.marker) {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(token.after)
    }
}

impl SqlPagePlan for LocalPlan {
    fn sql(&self) -> &str {
        SQL
    }

    fn options(
        &mut self,
        _: PageRequest,
        lookahead: usize,
        continuation: Option<local::paging::multi::Continuation>,
    ) -> Result<QueryOptions, DbError> {
        let after = self.resolve(continuation)?;
        let parameters = [
            i64_param(after.0)?,
            i64_param(after.1)?,
            i64_param(i64::try_from(lookahead).map_err(|_| DbError::new(ErrorKind::Overflow))?)?,
        ];
        Ok(QueryOptions::new().bind_all(&parameters))
    }

    fn next_continuation(
        &mut self,
        rows: &[Row],
        has_lookahead: bool,
    ) -> Result<Option<local::paging::multi::Continuation>, DbError> {
        if !has_lookahead {
            return Ok(None);
        }
        let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
        Ok(Some(local::paging::multi::Continuation::new(LocalToken {
            source: self.source,
            after: (last.read("tenant")?, last.read("seq")?),
            marker: Rc::clone(&self.marker),
        })))
    }
}

fn i64_param(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}

fn row(tenant: i64, seq: i64) -> Row {
    Row::new(vec![
        ("tenant".to_owned(), Value::I64(tenant)),
        ("seq".to_owned(), Value::I64(seq)),
    ])
    .expect("valid row")
}

fn page_size(rows: usize) -> PageSize {
    PageSize::try_from(rows).expect("nonzero page size")
}

fn budget(rows: usize, bytes: usize, pages: usize) -> PageBudget {
    PageBudget::new(rows, bytes, pages).expect("valid page budget")
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
    panic!("scripted local future did not resolve");
}

fn values(page: &PagedPage) -> Vec<(i64, i64)> {
    page.rows()
        .iter()
        .map(|row| {
            (
                row.read::<i64>("tenant").expect("tenant"),
                row.read::<i64>("seq").expect("seq"),
            )
        })
        .collect()
}

#[test]
fn local_source_pages_without_send() {
    let marker = Rc::new(());
    let recorded = Rc::new(RefCell::new(Vec::new()));
    let session = FakeSession {
        responses: RefCell::new(VecDeque::from(vec![
            vec![vec![row(1, 1), row(1, 2), row(1, 3)]],
            vec![vec![row(1, 3)]],
        ])),
        recorded: Rc::clone(&recorded),
        marker: Rc::clone(&marker),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        LocalPlan {
            source: 1,
            start: (0, 0),
            marker: Rc::clone(&marker),
        },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");

    let pages: Vec<PagedPage> = block_on(
        local::paging::multi::PageSourceStream::new(
            Box::new(source),
            page_size(2),
            budget(10, 65_536, 10),
        )
        .try_collect(),
    )
    .expect("pages");

    assert_eq!(pages.len(), 2);
    assert_eq!(values(&pages[0]), vec![(1, 1), (1, 2)]);
    assert_eq!(values(&pages[1]), vec![(1, 3)]);

    let recorded = recorded.borrow();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].sql, SQL);
    match recorded[0].options.limit {
        LimitSetting::Set(limit) => {
            assert_eq!(limit.rows(), 3);
            assert_eq!(limit.bytes(), 4_096);
        }
        LimitSetting::Inherit => panic!("the adapter must set a hard limit"),
    }
    assert_eq!(recorded[0].options.retry, RetrySetting::Off);
}

#[test]
fn local_source_accepts_a_session_borrowing_caller_state() {
    let marker = Rc::new(());
    let responses = RefCell::new(VecDeque::from(vec![vec![vec![row(1, 1)]]]));
    let recorded = RefCell::new(Vec::new());
    let source = SqlPageSource::new(
        Box::new(BorrowedLocalSession {
            responses: &responses,
            recorded: &recorded,
            marker: &marker,
        }),
        LocalPlan {
            source: 1,
            start: (0, 0),
            marker: Rc::clone(&marker),
        },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");

    let pages: Vec<PagedPage> = block_on(
        local::paging::multi::PageSourceStream::new(
            Box::new(source),
            page_size(2),
            budget(10, 65_536, 10),
        )
        .try_collect(),
    )
    .expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(recorded.borrow().len(), 1);
}

#[test]
fn local_empty_page_returns_no_rows_and_no_continuation() {
    let marker = Rc::new(());
    let session = FakeSession {
        responses: RefCell::new(VecDeque::from(vec![vec![Vec::new()]])),
        recorded: Rc::new(RefCell::new(Vec::new())),
        marker: Rc::clone(&marker),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        LocalPlan {
            source: 1,
            start: (0, 0),
            marker,
        },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");

    let mut stream = local::paging::multi::PageSourceStream::new(
        Box::new(source),
        page_size(2),
        budget(10, 65_536, 10),
    );
    let page = block_on(stream.try_next()).expect("stream").expect("page");
    assert!(page.rows().is_empty());
    assert!(block_on(stream.try_next()).expect("terminal").is_none());
}

#[test]
fn local_plan_limit_override_is_rejected() {
    struct LimitPlan {
        marker: Rc<()>,
    }

    impl SqlPagePlan for LimitPlan {
        fn sql(&self) -> &str {
            SQL
        }

        fn options(
            &mut self,
            _: PageRequest,
            _: usize,
            _: Option<local::paging::multi::Continuation>,
        ) -> Result<QueryOptions, DbError> {
            let _ = &self.marker;
            Ok(QueryOptions::new().limit(RowLimit::new(9, 4_096)?))
        }

        fn next_continuation(
            &mut self,
            _: &[Row],
            _: bool,
        ) -> Result<Option<local::paging::multi::Continuation>, DbError> {
            Ok(None)
        }
    }

    let marker = Rc::new(());
    let session = FakeSession {
        responses: RefCell::new(VecDeque::new()),
        recorded: Rc::new(RefCell::new(Vec::new())),
        marker: Rc::clone(&marker),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        LimitPlan { marker },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");

    let mut stream = local::paging::multi::PageSourceStream::new(
        Box::new(source),
        page_size(2),
        budget(10, 65_536, 10),
    );
    let error = block_on(stream.try_next()).expect_err("plan limit");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

struct RetryPlan {
    retry: RetrySetting,
    marker: Rc<()>,
}

impl SqlPagePlan for RetryPlan {
    fn sql(&self) -> &str {
        SQL
    }

    fn options(
        &mut self,
        _: PageRequest,
        _: usize,
        _: Option<local::paging::multi::Continuation>,
    ) -> Result<QueryOptions, DbError> {
        let _ = &self.marker;
        Ok(QueryOptions::new().retry(self.retry))
    }

    fn next_continuation(
        &mut self,
        _: &[Row],
        _: bool,
    ) -> Result<Option<local::paging::multi::Continuation>, DbError> {
        Ok(None)
    }
}

fn retry_source(
    retry: RetrySetting,
    responses: Vec<Vec<Vec<Row>>>,
) -> (
    SqlPageSource<'static, RetryPlan>,
    Rc<RefCell<Vec<Recorded>>>,
) {
    let marker = Rc::new(());
    let recorded = Rc::new(RefCell::new(Vec::new()));
    let session = FakeSession {
        responses: RefCell::new(VecDeque::from(responses)),
        recorded: Rc::clone(&recorded),
        marker: Rc::clone(&marker),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        RetryPlan { retry, marker },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    (source, recorded)
}

#[test]
fn local_explicit_retry_off_is_accepted() {
    let (source, recorded) = retry_source(RetrySetting::Off, vec![vec![vec![row(1, 1)]]]);
    let pages: Vec<PagedPage> = block_on(
        local::paging::multi::PageSourceStream::new(
            Box::new(source),
            page_size(2),
            budget(10, 65_536, 10),
        )
        .try_collect(),
    )
    .expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(recorded.borrow()[0].options.retry, RetrySetting::Off);
}

#[test]
fn local_enabled_retry_override_is_rejected_before_dispatch() {
    let enabled = RetrySetting::Use(
        RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(1)), false).expect("retry policy"),
    );
    let (source, recorded) = retry_source(enabled, vec![vec![Vec::new()]]);
    let mut stream = local::paging::multi::PageSourceStream::new(
        Box::new(source),
        page_size(2),
        budget(10, 65_536, 10),
    );
    let error = block_on(stream.try_next()).expect_err("retry override");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(recorded.borrow().is_empty());
}
