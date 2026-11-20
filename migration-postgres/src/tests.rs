use super::execution::Operation;
use super::{Error, PostgresBackend, acquire_lock, ensure_supported_version};
use neotales_dbapi::{
    BufferedReader, DbFuture, ErrorKind, QueryOptions, Reader, Row, Transaction, Value,
};
use std::{collections::VecDeque, time::Duration};

const LOCK_SQL: &str = "SELECT pg_catalog.pg_try_advisory_xact_lock($1) AS locked";
const VERSION_SQL: &str =
    "SELECT pg_catalog.current_setting('server_version_num')::bigint AS version";

struct ReplyTransaction(VecDeque<(&'static str, Vec<Row>)>);

impl<'session> Transaction<'session> for ReplyTransaction {
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        let (expected, rows) = self.0.pop_front().expect("unexpected query");
        assert_eq!(
            sql, expected,
            "query order is part of the lock/version gate"
        );
        Box::pin(async move { Ok(Box::new(BufferedReader::new(rows)) as Box<dyn Reader>) })
    }

    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        panic!("no writes are allowed before the version gate");
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        panic!("no scripts are allowed before the version gate");
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        panic!("private-helper tests do not commit");
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        panic!("private-helper tests do not own transaction closure");
    }
}

fn row(name: &str, value: Value) -> Row {
    Row::new(vec![(name.to_owned(), value)]).unwrap()
}

// Unit scope: a private helper with branching, including an impossible normal-server reply.
#[tokio::test]
async fn empty_advisory_result_is_lock_decode_error_not_history_corruption() {
    let mut transaction = ReplyTransaction(VecDeque::from([(LOCK_SQL, vec![])]));
    assert!(matches!(
        acquire_lock(&mut transaction, 1).await,
        Err(Error::Lock(error)) if error.kind() == ErrorKind::Decode
    ));
}

// Unit scope: the private orchestration branch must fail before any ledger operation.
#[tokio::test]
async fn unsupported_major_is_rejected_after_lock_before_inspection_or_ddl() {
    let backend = PostgresBackend::new("app", "history", Duration::from_secs(1)).unwrap();
    for version in [160_008, 180_001, 0, -1] {
        let mut transaction = ReplyTransaction(VecDeque::from([
            (LOCK_SQL, vec![row("locked", Value::Bool(true))]),
            (VERSION_SQL, vec![row("version", Value::I64(version))]),
        ]));
        assert!(matches!(
            backend
                .run_locked(&mut transaction, "app", "fp", &[], Operation::Initialize)
                .await,
            Err(Error::UnsupportedVersion)
        ));
        assert_eq!(transaction.0.len(), 0);
    }
}

// Unit scope: private major-version bounds check, independent of installed server minors.
#[tokio::test]
async fn postgres_17_minor_versions_are_supported() {
    for version in [170_000, 170_005, 179_999] {
        let mut transaction = ReplyTransaction(VecDeque::from([(
            VERSION_SQL,
            vec![row("version", Value::I64(version))],
        )]));
        ensure_supported_version(&mut transaction).await.unwrap();
    }
}

// Unit scope: private version decoding branch, not reachable with a healthy PostgreSQL reply.
#[tokio::test]
async fn empty_version_result_is_database_decode_error_not_history_corruption() {
    let mut transaction = ReplyTransaction(VecDeque::from([(VERSION_SQL, vec![])]));
    assert!(matches!(
        ensure_supported_version(&mut transaction).await,
        Err(Error::Database(error)) if error.kind() == ErrorKind::Decode
    ));
}
