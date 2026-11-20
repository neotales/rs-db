//! Explicit fetch limits must fail before an execute or script reaches a provider.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use neotales_dbapi::{
    BufferedReader, Capabilities, DbError, DbFuture, ErrorKind, PolicyPool, Pool, PoolDefaults,
    PoolPolicy, QueryOptions, Reader, RowLimit, Session, Transaction, TxOptions, sync,
};

#[derive(Clone, Default)]
struct Spy(Arc<AtomicUsize>);

fn options() -> QueryOptions {
    QueryOptions::new().limit(RowLimit::new(1, 64).unwrap())
}

fn run<T>(future: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
    let mut future = Box::pin(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("spy future unexpectedly pending"),
    }
}

impl Pool for Spy {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>> {
        let spy = self.clone();
        Box::pin(async move { Ok(Box::new(spy) as Box<dyn Session>) })
    }
}

impl Session for Spy {
    fn execute_async<'a>(&'a mut self, _: &'a str, mut options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async { Ok(Box::new(BufferedReader::new(vec![])) as Box<dyn Reader>) })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.prepare_script_provider(PoolDefaults::default())?;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn begin_async<'a>(&'a mut self, _: TxOptions) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>> {
        let spy = self.clone();
        Box::pin(async move { Ok(Box::new(spy) as Box<dyn Transaction<'a> + 'a>) })
    }
}

impl<'session> Transaction<'session> for Spy {
    fn execute_async<'a>(&'a mut self, _: &'a str, mut options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.prepare_execute_provider(PoolDefaults::default())?;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async { Ok(Box::new(BufferedReader::new(vec![])) as Box<dyn Reader>) })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async move {
            options.prepare_script_provider(PoolDefaults::default())?;
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }

    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl sync::Pool for Spy {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire(&self) -> Result<Box<dyn sync::Session>, DbError> {
        Ok(Box::new(self.clone()))
    }
}

impl sync::Session for Spy {
    fn execute(&mut self, _: &str, mut options: QueryOptions) -> Result<u64, DbError> {
        options.prepare_execute_provider(PoolDefaults::default())?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }

    fn fetch(&mut self, _: &str, _: QueryOptions) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        Ok(Box::new(sync::BufferedReader::new(vec![])))
    }

    fn execute_trusted_script(
        &mut self,
        _: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        options.prepare_script_provider(PoolDefaults::default())?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn begin(&mut self, _: TxOptions) -> Result<Box<dyn sync::Transaction<'_> + '_>, DbError> {
        Ok(Box::new(self.clone()))
    }
}

impl sync::Transaction<'_> for Spy {
    fn execute(&mut self, _: &str, mut options: QueryOptions) -> Result<u64, DbError> {
        options.prepare_execute_provider(PoolDefaults::default())?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }

    fn fetch(&mut self, _: &str, _: QueryOptions) -> Result<Box<dyn sync::Reader + '_>, DbError> {
        Ok(Box::new(sync::BufferedReader::new(vec![])))
    }

    fn execute_trusted_script(
        &mut self,
        _: &str,
        mut options: QueryOptions,
    ) -> Result<(), DbError> {
        options.prepare_script_provider(PoolDefaults::default())?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<(), DbError> {
        Ok(())
    }

    fn rollback(self: Box<Self>) -> Result<(), DbError> {
        Ok(())
    }
}

fn invalid<T>(result: Result<T, DbError>) {
    assert_eq!(
        result.err().expect("must reject").kind(),
        ErrorKind::InvalidArgument
    );
}

#[test]
fn async_bare_and_policy_pool_session_transaction_and_script_do_not_dispatch() {
    let spy = Spy::default();
    let wrapped = PolicyPool::new(
        spy.clone(),
        PoolPolicy {
            default_limit: RowLimit::new(2, 128).unwrap(),
            ..PoolPolicy::default()
        },
    );
    for pool in [&spy as &dyn Pool, &wrapped as &dyn Pool] {
        invalid(run(pool.execute_async("write", options())));
        let mut session = run(pool.acquire_async()).unwrap();
        invalid(run(session.execute_async("write", options())));
        invalid(run(
            session.execute_trusted_script_async("script", options())
        ));
        let mut transaction = run(session.begin_async(TxOptions::default())).unwrap();
        invalid(run(transaction.execute_async("write", options())));
        invalid(run(
            transaction.execute_trusted_script_async("script", options())
        ));
        assert_eq!(spy.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            run(transaction.execute_async("write", QueryOptions::new())).unwrap(),
            1
        );
        run(transaction.rollback_async()).unwrap();
        assert_eq!(
            run(session.execute_async("write", QueryOptions::new())).unwrap(),
            1
        );
        run(session.execute_trusted_script_async("script", QueryOptions::new())).unwrap();
        assert_eq!(
            run(pool.execute_async("write", QueryOptions::new())).unwrap(),
            1
        );
        spy.0.store(0, Ordering::SeqCst);
    }
}

#[test]
fn sync_provider_pool_session_transaction_and_script_do_not_dispatch() {
    let spy = Spy::default();
    let pool: &dyn sync::Pool = &spy;
    invalid(pool.execute("write", options()));
    let mut session = pool.acquire().unwrap();
    invalid(session.execute("write", options()));
    invalid(session.execute_trusted_script("script", options()));
    let mut transaction = session.begin(TxOptions::default()).unwrap();
    invalid(transaction.execute("write", options()));
    invalid(transaction.execute_trusted_script("script", options()));
    assert_eq!(spy.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        transaction.execute("write", QueryOptions::new()).unwrap(),
        1
    );
    transaction.rollback().unwrap();
    assert_eq!(session.execute("write", QueryOptions::new()).unwrap(), 1);
}

#[cfg(feature = "blocking")]
#[test]
fn blocking_policy_pool_session_transaction_and_script_do_not_dispatch() {
    let spy = Spy::default();
    let policy = PolicyPool::new(
        spy.clone(),
        PoolPolicy {
            default_limit: RowLimit::new(2, 128).unwrap(),
            ..PoolPolicy::default()
        },
    );
    let pool = neotales_dbapi::blocking::BlockingPool::new(policy);
    invalid(sync::Pool::execute(&pool, "write", options()));
    let mut session = sync::Pool::acquire(&pool).unwrap();
    invalid(session.execute("write", options()));
    invalid(session.execute_trusted_script("script", options()));
    let mut transaction = session.begin(TxOptions::default()).unwrap();
    invalid(transaction.execute("write", options()));
    invalid(transaction.execute_trusted_script("script", options()));
    assert_eq!(spy.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        transaction.execute("write", QueryOptions::new()).unwrap(),
        1
    );
    transaction.rollback().unwrap();
    assert_eq!(session.execute("write", QueryOptions::new()).unwrap(), 1);
    assert_eq!(
        sync::Pool::execute(&pool, "write", QueryOptions::new()).unwrap(),
        1
    );
}
