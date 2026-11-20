//! Contract tests for the local-future traits.
//!
//! The mock provider deliberately holds `Rc` state, so it cannot implement the `Send`-bounded
//! traits. If this file compiles, the local traits really do accept non-`Send` providers and
//! futures.

use std::{
    cell::Cell,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use neotales_dbapi::{
    Capabilities, DbError, ErrorKind, QueryOptions, Row, RowLimit, TxOptions, Value, local,
    local::Pool as _,
};

fn row(value: i64) -> Row {
    Row::new(vec![("n".to_owned(), Value::I64(value))]).expect("valid row")
}

fn block_on<T>(future: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..1_000 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
    }
    panic!("mock local future did not resolve");
}

struct LocalReader {
    inner: local::BufferedReader,
    not_send: Rc<()>,
}

impl local::Reader for LocalReader {
    fn next_row_async(&mut self) -> local::DbFuture<'_, Option<Row>> {
        let not_send = Rc::clone(&self.not_send);
        let row = self.inner.next_row_async();
        Box::pin(async move {
            let result = row.await;
            drop(not_send);
            result
        })
    }

    fn has_next_set_async(&mut self) -> local::DbFuture<'_, bool> {
        self.inner.has_next_set_async()
    }

    fn finish_async(&mut self) -> local::DbFuture<'_, ()> {
        self.inner.finish_async()
    }
}

struct LocalTransaction {
    state: Rc<Cell<u8>>,
}

impl<'session> local::Transaction<'session> for LocalTransaction {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> local::DbFuture<'a, u64> {
        let state = Rc::clone(&self.state);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(state);
            Ok(1)
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Reader + 'a>> {
        let state = Rc::clone(&self.state);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(state);
            Ok(Box::new(local::BufferedReader::new(Vec::new())) as Box<dyn local::Reader>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, ()> {
        let state = Rc::clone(&self.state);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(state);
            Ok(())
        })
    }

    fn commit_async(self: Box<Self>) -> local::DbFuture<'session, ()> {
        self.state.set(1);
        Box::pin(async { Ok(()) })
    }

    fn rollback_async(self: Box<Self>) -> local::DbFuture<'session, ()> {
        self.state.set(2);
        Box::pin(async { Ok(()) })
    }
}

struct LocalSession {
    sets: Vec<Vec<Row>>,
    transaction_state: Rc<Cell<u8>>,
}

impl local::Session for LocalSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> local::DbFuture<'a, u64> {
        let state = Rc::clone(&self.transaction_state);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(state);
            Ok(1)
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Reader + 'a>> {
        let sets = self.sets.clone();
        let not_send = Rc::new(());
        Box::pin(async move {
            std::future::ready(()).await;
            drop(not_send);
            let reader = LocalReader {
                inner: local::BufferedReader::from_sets(sets),
                not_send: Rc::new(()),
            };
            Ok(Box::new(reader) as Box<dyn local::Reader + 'a>)
        })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> local::DbFuture<'a, ()> {
        let state = Rc::clone(&self.transaction_state);
        Box::pin(async move {
            std::future::ready(()).await;
            drop(state);
            Ok(())
        })
    }

    fn begin_async<'a>(
        &'a mut self,
        _: TxOptions,
    ) -> local::DbFuture<'a, Box<dyn local::Transaction<'a> + 'a>> {
        let state = Rc::clone(&self.transaction_state);
        Box::pin(async move {
            std::future::ready(()).await;
            Ok(Box::new(LocalTransaction { state }) as Box<dyn local::Transaction<'a> + 'a>)
        })
    }
}

struct LocalPool {
    sets: Vec<Vec<Row>>,
    transaction_state: Rc<Cell<u8>>,
}

impl LocalPool {
    fn new(sets: Vec<Vec<Row>>) -> Self {
        Self {
            sets,
            transaction_state: Rc::new(Cell::new(0)),
        }
    }
}

impl local::Pool for LocalPool {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> local::DbFuture<'_, Box<dyn local::Session>> {
        let sets = self.sets.clone();
        let transaction_state = Rc::clone(&self.transaction_state);
        Box::pin(async move {
            let not_send = Rc::new(());
            std::future::ready(()).await;
            drop(not_send);
            Ok(Box::new(LocalSession {
                sets,
                transaction_state,
            }) as Box<dyn local::Session>)
        })
    }
}

#[test]
fn traits_are_object_safe() {
    fn pool(_: &dyn local::Pool) {}
    fn session(_: &mut dyn local::Session) {}
    fn reader(_: &mut dyn local::Reader) {}
    fn transaction(_: &dyn local::Transaction<'_>) {}
    let _ = (pool, session, reader, transaction);
}

#[test]
fn local_pool_preserves_result_set_boundaries() {
    let pool: Box<dyn local::Pool> =
        Box::new(LocalPool::new(vec![vec![row(1), row(2)], vec![row(3)]]));
    let mut session = block_on(pool.acquire_async()).expect("acquire");
    let mut reader = block_on(session.fetch_async("SELECT n", QueryOptions::new())).expect("fetch");

    let sets = block_on(reader.read_sets_async()).expect("read sets");
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].len(), 2);
    assert_eq!(sets[1].len(), 1);
    assert_eq!(sets[0][1].read_at::<i64>(0).expect("second row"), 2);
    assert_eq!(sets[1][0].read_at::<i64>(0).expect("third row"), 3);
}

#[test]
fn local_pool_default_fetch_enforces_the_row_limit() {
    let pool = LocalPool::new(vec![vec![row(1), row(2), row(3)]]);
    let options = QueryOptions::new().limit(RowLimit::new(2, 4_096).expect("limit"));
    let error = block_on(pool.fetch_all_async("SELECT n", options)).expect_err("limit");
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[test]
fn local_pool_default_fetch_enforces_the_byte_limit() {
    let pool = LocalPool::new(vec![vec![row(1)]]);
    let options = QueryOptions::new().limit(RowLimit::new(1, 8).expect("limit"));
    let error = block_on(pool.fetch_all_async("SELECT n", options)).expect_err("limit");
    assert_eq!(error.kind(), ErrorKind::Overflow);
}

#[test]
fn local_transaction_is_object_safe_and_commits() {
    let pool = LocalPool::new(Vec::new());
    let mut session = block_on(pool.acquire_async()).expect("acquire");
    let mut transaction = block_on(session.begin_async(TxOptions::default())).expect("begin");
    assert_eq!(
        block_on(transaction.execute_async("UPDATE n SET n = 1", QueryOptions::new()))
            .expect("execute"),
        1
    );
    block_on(transaction.commit_async()).expect("commit");
    assert_eq!(pool.transaction_state.get(), 1);
}

#[test]
fn local_capabilities_can_declare_no_transactions() {
    let capabilities = local::Pool::capabilities(&LocalPool::new(Vec::new()));
    assert!(!capabilities.transactions);
    assert!(!capabilities.isolation_level);
    assert!(!capabilities.read_only_transactions);
    assert!(!capabilities.trusted_scripts);
}
