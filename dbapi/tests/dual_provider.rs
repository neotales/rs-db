//! Compile-and-run proof that one provider type can implement both trait families.
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use neotales_dbapi::sync::{
    BufferedReader as SyncBufferedReader, Pool as SyncPool, Reader as SyncReader,
    Session as SyncSession,
};
use neotales_dbapi::{
    BufferedReader as AsyncBufferedReader, Capabilities, DbError, DbFuture, ErrorKind,
    Pool as AsyncPool, QueryOptions, Reader as AsyncReader, Row, RowLimit, Session as AsyncSession,
    TxOptions, Value,
};

fn row(value: i64) -> Row {
    Row::new(vec![("n".to_owned(), Value::I64(value))]).expect("valid row")
}

fn block_on<T, F: Future<Output = Result<T, DbError>>>(future: F) -> Result<T, DbError> {
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..1_000 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
    }
    panic!("mock future did not resolve");
}

struct DualReader {
    asynchronous: AsyncBufferedReader,
    synchronous: SyncBufferedReader,
}

impl DualReader {
    fn new() -> Self {
        Self {
            asynchronous: AsyncBufferedReader::new(vec![row(7)]),
            synchronous: SyncBufferedReader::new(vec![row(7)]),
        }
    }
}

impl AsyncReader for DualReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        self.asynchronous.next_row_async()
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        self.asynchronous.has_next_set_async()
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.asynchronous.finish_async()
    }
}

impl SyncReader for DualReader {
    fn next_row(&mut self) -> Result<Option<Row>, DbError> {
        self.synchronous.next_row()
    }

    fn has_next_set(&mut self) -> Result<bool, DbError> {
        self.synchronous.has_next_set()
    }

    fn finish(&mut self) -> Result<(), DbError> {
        self.synchronous.finish()
    }
}

struct DualSession;

impl AsyncSession for DualSession {
    fn execute_async<'a>(&'a mut self, _: &'a str, _: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async { Ok(1) })
    }

    fn fetch_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, Box<dyn AsyncReader + 'a>> {
        Box::pin(async { Ok(Box::new(DualReader::new()) as Box<dyn AsyncReader>) })
    }

    fn execute_trusted_script_async<'a>(
        &'a mut self,
        _: &'a str,
        _: QueryOptions,
    ) -> DbFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn begin_async<'a>(
        &'a mut self,
        _: TxOptions,
    ) -> DbFuture<'a, Box<dyn neotales_dbapi::Transaction<'a> + 'a>> {
        Box::pin(async { Err(DbError::new(ErrorKind::Unsupported)) })
    }
}

impl SyncSession for DualSession {
    fn execute(&mut self, _: &str, _: QueryOptions) -> Result<u64, DbError> {
        Ok(1)
    }

    fn fetch(&mut self, _: &str, _: QueryOptions) -> Result<Box<dyn SyncReader + '_>, DbError> {
        Ok(Box::new(DualReader::new()))
    }

    fn execute_trusted_script(&mut self, _: &str, _: QueryOptions) -> Result<(), DbError> {
        Ok(())
    }

    fn begin(
        &mut self,
        _: TxOptions,
    ) -> Result<Box<dyn neotales_dbapi::sync::Transaction<'_> + '_>, DbError> {
        Err(DbError::new(ErrorKind::Unsupported))
    }
}

struct DualProvider;

impl AsyncPool for DualProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire_async(&self) -> DbFuture<'_, Box<dyn AsyncSession>> {
        Box::pin(async { Ok(Box::new(DualSession) as Box<dyn AsyncSession>) })
    }
}

impl SyncPool for DualProvider {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    fn acquire(&self) -> Result<Box<dyn SyncSession>, DbError> {
        Ok(Box::new(DualSession))
    }
}

fn both_capabilities(provider: &(impl AsyncPool + SyncPool)) -> (Capabilities, Capabilities) {
    // `provider.capabilities()` would be ambiguous because both Pool traits define it.
    (
        AsyncPool::capabilities(provider),
        SyncPool::capabilities(provider),
    )
}

#[test]
fn one_type_implements_both_trait_families() {
    let provider = DualProvider;
    let limit = RowLimit::new(1, 64).unwrap();
    assert_eq!(both_capabilities(&provider).0, Capabilities::default());
    assert_eq!(both_capabilities(&provider).1, Capabilities::default());

    let mut session = block_on(AsyncPool::acquire_async(&provider)).unwrap();
    let mut reader = block_on(AsyncSession::fetch_async(
        session.as_mut(),
        "",
        QueryOptions::new().limit(limit),
    ))
    .unwrap();
    assert_eq!(
        block_on(AsyncReader::next_row_async(reader.as_mut()))
            .unwrap()
            .unwrap()
            .read::<i64>("n")
            .unwrap(),
        7
    );
    block_on(AsyncReader::finish_async(reader.as_mut())).unwrap();

    let mut session = SyncPool::acquire(&provider).unwrap();
    let mut reader =
        SyncSession::fetch(&mut *session, "", QueryOptions::new().limit(limit)).unwrap();
    assert_eq!(
        SyncReader::next_row(&mut *reader)
            .unwrap()
            .unwrap()
            .read::<i64>("n")
            .unwrap(),
        7
    );
    SyncReader::finish(&mut *reader).unwrap();
}
