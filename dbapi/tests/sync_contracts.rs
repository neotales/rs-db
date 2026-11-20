//! Contract tests for the native synchronous traits.
use neotales_dbapi::sync::{Pool as SyncPool, Reader as SyncReader};
use neotales_dbapi::{
    Capabilities, DbError, ErrorKind, QueryOptions, Row, RowLimit, TxOptions, Value, sync,
};

struct MockPool;

impl sync::Pool for MockPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            transactions: false,
            ..Capabilities::default()
        }
    }

    fn acquire(&self) -> Result<Box<dyn sync::Session>, DbError> {
        Ok(Box::new(MockSession))
    }
}

struct MockSession;

impl sync::Session for MockSession {
    fn execute(&mut self, _: &str, _: QueryOptions) -> Result<u64, DbError> {
        Ok(1)
    }

    fn fetch(&mut self, sql: &str, _: QueryOptions) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        let (sets, finish_error) = match sql {
            "one" => (vec![vec![row(1)]], false),
            "many" => (vec![vec![row(1), row(2)]], false),
            "sets" => (vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]], false),
            "finish-error" => (vec![vec![row(1), row(2)]], true),
            _ => (vec![Vec::new()], false),
        };
        let inner = sync::BufferedReader::from_sets(sets);
        if finish_error {
            Ok(Box::new(FinishErrorReader { inner }))
        } else {
            Ok(Box::new(inner))
        }
    }

    fn execute_trusted_script(&mut self, _: &str, _: QueryOptions) -> Result<(), DbError> {
        Ok(())
    }

    fn begin(&mut self, _: TxOptions) -> Result<Box<dyn sync::Transaction<'_> + '_>, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }
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
        Err(DbError::new(ErrorKind::Connection))
    }
}

fn row(value: i64) -> Row {
    Row::new(vec![("n".to_owned(), Value::I64(value))]).expect("valid row")
}

fn one(pool: &dyn sync::Pool, sql: &str) -> Result<Row, DbError> {
    pool.fetch_one(
        sql,
        QueryOptions::new().limit(RowLimit::new(10, 4096).expect("valid limit")),
    )
}

#[test]
fn buffered_reader_preserves_boundaries_and_advances() {
    let mut reader =
        sync::BufferedReader::from_sets(vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]]);
    assert_eq!(reader.next_row().expect("row"), Some(row(1)));
    assert_eq!(reader.next_row().expect("end"), None);
    assert!(reader.has_next_set().expect("next set"));
    assert_eq!(reader.next_row().expect("empty set"), None);
    assert!(reader.has_next_set().expect("next set"));
    assert_eq!(reader.next_row().expect("row"), Some(row(2)));
    assert_eq!(reader.next_row().expect("row"), Some(row(3)));
    assert_eq!(reader.next_row().expect("end"), None);
    assert!(!reader.has_next_set().expect("end"));
}

#[test]
fn buffered_reader_read_methods_flatten_or_group() {
    let mut reader =
        sync::BufferedReader::from_sets(vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]]);
    assert_eq!(
        reader.read_sets().expect("sets"),
        vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]]
    );

    let mut reader = sync::BufferedReader::from_sets(vec![vec![row(1)], vec![row(2), row(3)]]);
    assert_eq!(
        reader.read_all().expect("rows"),
        vec![row(1), row(2), row(3)]
    );
}

#[test]
fn convenience_calls_use_the_same_cardinality_rules() {
    let pool = MockPool;
    assert_eq!(one(&pool, "one").expect("one row"), row(1));
    assert_eq!(
        one(&pool, "many").expect_err("too many").kind(),
        ErrorKind::TooManyRows
    );
    assert_eq!(
        one(&pool, "empty").expect_err("no rows").kind(),
        ErrorKind::NoRows
    );
    assert_eq!(
        pool.fetch_first(
            "many",
            QueryOptions::new().limit(RowLimit::new(10, 4096).expect("valid limit"))
        )
        .expect("first")
        .expect("row"),
        row(1)
    );
    assert_eq!(
        pool.fetch_scalar(
            "one",
            QueryOptions::new().limit(RowLimit::new(10, 4096).expect("valid limit"))
        )
        .expect("scalar"),
        Value::I64(1)
    );
    assert_eq!(
        pool.fetch_all(
            "sets",
            QueryOptions::new().limit(RowLimit::with_max_sets(10, 4096, 3).expect("valid limit"))
        )
        .expect("all"),
        vec![row(1), row(2), row(3)]
    );
}

#[test]
fn a_later_finish_error_wins_over_cardinality() {
    let pool = MockPool;
    assert_eq!(
        one(&pool, "finish-error").expect_err("later error").kind(),
        ErrorKind::Connection
    );
}

#[test]
fn fetch_defaults_enforce_the_aggregate_limit() {
    let pool = MockPool;
    assert_eq!(
        pool.fetch_all(
            "sets",
            QueryOptions::new().limit(RowLimit::new(2, 4096).expect("valid limit"))
        )
        .expect_err("row limit"),
        DbError::new(ErrorKind::Overflow)
    );
    assert_eq!(
        pool.fetch_all(
            "sets",
            QueryOptions::new().limit(RowLimit::with_max_sets(10, 4096, 2).expect("valid limit"))
        )
        .expect_err("set limit"),
        DbError::new(ErrorKind::Overflow)
    );
}

#[test]
fn session_reader_reports_grouped_sets() {
    let pool = MockPool;
    let mut session = pool.acquire().expect("session");
    let mut reader = session
        .fetch(
            "sets",
            QueryOptions::new().limit(RowLimit::new(10, 4096).expect("valid limit")),
        )
        .expect("reader");
    assert_eq!(
        reader.read_sets().expect("sets"),
        vec![vec![row(1)], Vec::new(), vec![row(2), row(3)]]
    );
}

#[test]
fn traits_are_object_safe() {
    let pool: Box<dyn sync::Pool> = Box::new(MockPool);
    let mut session: Box<dyn sync::Session> = pool.acquire().expect("session");
    let mut reader: Box<dyn sync::Reader> = session
        .fetch(
            "one",
            QueryOptions::new().limit(RowLimit::new(10, 4096).expect("valid limit")),
        )
        .expect("reader");
    assert_eq!(reader.next_row().expect("row"), Some(row(1)));
}
