//! Native blocking contract test for the caller-authored SQL source.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use neotales_dbapi::{
    DbError, DbType, Delay, ErrorKind, LimitSetting, Parameter, QueryOptions, RetryPolicy,
    RetrySetting, Row, TxOptions, Value, sync,
    sync::paging::{
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
    responses: VecDeque<Vec<Vec<Row>>>,
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl sync::Session for FakeSession {
    fn execute(&mut self, _: &str, _: QueryOptions) -> Result<u64, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }

    fn fetch(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        self.recorded.lock().expect("recordings").push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let sets = self.responses.pop_front().expect("scripted response");
        Ok(Box::new(sync::BufferedReader::from_sets(sets)))
    }

    fn execute_trusted_script(&mut self, _: &str, _: QueryOptions) -> Result<(), DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }

    fn begin(&mut self, _: TxOptions) -> Result<Box<dyn sync::Transaction<'_> + '_>, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

struct BorrowedSyncSession<'a> {
    responses: &'a Mutex<VecDeque<Vec<Vec<Row>>>>,
    recorded: &'a Mutex<Vec<Recorded>>,
}

impl sync::Session for BorrowedSyncSession<'_> {
    fn execute(&mut self, _: &str, _: QueryOptions) -> Result<u64, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }

    fn fetch(
        &mut self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        self.recorded.lock().expect("recordings").push(Recorded {
            sql: sql.to_owned(),
            options,
        });
        let sets = self
            .responses
            .lock()
            .expect("responses")
            .pop_front()
            .expect("scripted response");
        Ok(Box::new(sync::BufferedReader::from_sets(sets)))
    }

    fn execute_trusted_script(&mut self, _: &str, _: QueryOptions) -> Result<(), DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }

    fn begin(&mut self, _: TxOptions) -> Result<Box<dyn sync::Transaction<'_> + '_>, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

#[derive(Debug)]
struct Token {
    source: u64,
    after: (i64, i64),
}

struct Plan {
    source: u64,
    start: (i64, i64),
}

impl Plan {
    fn resolve(
        &self,
        continuation: Option<neotales_dbapi::paging::multi::Continuation>,
    ) -> Result<(i64, i64), DbError> {
        let Some(token) = continuation else {
            return Ok(self.start);
        };
        let token = token
            .downcast::<Token>()
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
        if token.source != self.source {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(token.after)
    }
}

impl SqlPagePlan for Plan {
    fn sql(&self) -> &str {
        SQL
    }

    fn options(
        &mut self,
        _: PageRequest,
        lookahead: usize,
        continuation: Option<neotales_dbapi::paging::multi::Continuation>,
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
    ) -> Result<Option<neotales_dbapi::paging::multi::Continuation>, DbError> {
        if !has_lookahead {
            return Ok(None);
        }
        let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
        Ok(Some(neotales_dbapi::paging::multi::Continuation::new(
            Token {
                source: self.source,
                after: (last.read("tenant")?, last.read("seq")?),
            },
        )))
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
fn blocking_source_accepts_a_session_borrowing_caller_state() {
    let responses = Mutex::new(VecDeque::from(vec![vec![vec![row(1, 1)]]]));
    let recorded = Mutex::new(Vec::new());
    let source = SqlPageSource::new(
        Box::new(BorrowedSyncSession {
            responses: &responses,
            recorded: &recorded,
        }),
        Plan {
            source: 1,
            start: (0, 0),
        },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    let iterator = sync::paging::multi::PageSourceIter::new(
        Box::new(source),
        PageSize::try_from(2).expect("page size"),
        PageBudget::new(10, 65_536, 10).expect("budget"),
    );

    let pages: Vec<PagedPage> = iterator.collect::<Result<_, _>>().expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(recorded.lock().expect("recordings").len(), 1);
}

#[test]
fn blocking_source_pages_with_lookahead() {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let session = FakeSession {
        responses: VecDeque::from(vec![
            vec![vec![row(1, 1), row(1, 2), row(1, 3)]],
            vec![vec![row(1, 3)]],
        ]),
        recorded: Arc::clone(&recorded),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        Plan {
            source: 1,
            start: (0, 0),
        },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    let iterator = sync::paging::multi::PageSourceIter::new(
        Box::new(source),
        PageSize::try_from(2).expect("page size"),
        PageBudget::new(10, 65_536, 10).expect("budget"),
    );

    let pages: Vec<PagedPage> = iterator.collect::<Result<_, _>>().expect("pages");
    assert_eq!(pages.len(), 2);
    assert_eq!(values(&pages[0]), vec![(1, 1), (1, 2)]);
    assert_eq!(values(&pages[1]), vec![(1, 3)]);

    let recorded = recorded.lock().expect("recordings");
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].sql, SQL);
    match recorded[0].options.limit {
        LimitSetting::Set(limit) => assert_eq!(limit.rows(), 3),
        LimitSetting::Inherit => panic!("the adapter must set a hard limit"),
    }
    assert_eq!(recorded[0].options.retry, RetrySetting::Off);
}

struct RetryPlan {
    retry: RetrySetting,
}

impl SqlPagePlan for RetryPlan {
    fn sql(&self) -> &str {
        SQL
    }

    fn options(
        &mut self,
        _: PageRequest,
        _: usize,
        _: Option<neotales_dbapi::paging::multi::Continuation>,
    ) -> Result<QueryOptions, DbError> {
        Ok(QueryOptions::new().retry(self.retry))
    }

    fn next_continuation(
        &mut self,
        _: &[Row],
        _: bool,
    ) -> Result<Option<neotales_dbapi::paging::multi::Continuation>, DbError> {
        Ok(None)
    }
}

fn retry_iterator(
    retry: RetrySetting,
    responses: Vec<Vec<Vec<Row>>>,
) -> (
    sync::paging::multi::PageSourceIter<'static>,
    Arc<Mutex<Vec<Recorded>>>,
) {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let session = FakeSession {
        responses: VecDeque::from(responses),
        recorded: Arc::clone(&recorded),
    };
    let source = SqlPageSource::new(
        Box::new(session),
        RetryPlan { retry },
        4_096,
        SqlPageAssertion::read_only_replay_safe_and_authorized(),
    )
    .expect("valid source");
    let iterator = sync::paging::multi::PageSourceIter::new(
        Box::new(source),
        PageSize::try_from(2).expect("page size"),
        PageBudget::new(10, 65_536, 10).expect("budget"),
    );
    (iterator, recorded)
}

#[test]
fn blocking_explicit_retry_off_is_accepted() {
    let (iterator, recorded) = retry_iterator(RetrySetting::Off, vec![vec![vec![row(1, 1)]]]);
    let pages: Vec<PagedPage> = iterator.collect::<Result<_, _>>().expect("pages");
    assert_eq!(pages.len(), 1);
    assert_eq!(
        recorded.lock().expect("recordings")[0].options.retry,
        RetrySetting::Off
    );
}

#[test]
fn blocking_enabled_retry_override_is_rejected_before_dispatch() {
    let enabled = RetrySetting::Use(
        RetryPolicy::new(2, Delay::Fixed(Duration::from_millis(1)), false).expect("retry policy"),
    );
    let (mut iterator, recorded) = retry_iterator(enabled, vec![vec![Vec::new()]]);
    let error = iterator.next().expect("item").expect_err("retry override");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(recorded.lock().expect("recordings").is_empty());
}
