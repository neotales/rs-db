use futures_core::Stream;
use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, QueryOptions, Value, local::Pool,
    paging::{PageSize, multi::PageBudget},
};
use neotales_dbapi_d1::{
    D1BatchStatement, D1Pool,
    paging::{D1PageContinuation, D1PageStream, D1PagedPage, D1ReadOnlyReplaySafe},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use wasm_bindgen::{JsCast, prelude::*};
use worker::Delay;

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn error(e: impl std::fmt::Debug) -> JsValue {
    JsValue::from_str(&format!("{e:?}"))
}
fn param(t: DbType, v: Value) -> Result<Parameter, JsValue> {
    Parameter::new(t, v).map_err(error)
}
async fn contract_disabled(pool: &D1Pool) -> Result<(), JsValue> {
    if !matches!(pool.acquire_async().await, Err(e) if e.kind() == ErrorKind::Unsupported) {
        return Err(JsValue::from_str("acquire must fail"));
    }
    if pool
        .execute_async(
            "INSERT INTO t VALUES (98, 'forbidden')",
            QueryOptions::new(),
        )
        .await
        .unwrap_err()
        .kind()
        != ErrorKind::Unsupported
    {
        return Err(JsValue::from_str("trait write must fail"));
    }
    if pool
        .fetch_all_async(
            "INSERT INTO t VALUES (99, 'forbidden') RETURNING id",
            QueryOptions::new(),
        )
        .await
        .unwrap_err()
        .kind()
        != ErrorKind::Unsupported
    {
        return Err(JsValue::from_str("trait fetch must fail"));
    }
    Ok(())
}
#[wasm_bindgen]
pub async fn probe(binding: JsValue, pending_binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(binding.unchecked_into());
    contract_disabled(&pool).await?;
    pool.execute_uncertain_async("CREATE TABLE t (id INTEGER UNIQUE, v TEXT)", &[])
        .await
        .map_err(error)?;
    let params = [
        param(DbType::I64, Value::I64(1))?,
        param(DbType::Text, Value::Text("hello".into()))?,
    ];
    if pool
        .execute_uncertain_async("INSERT INTO t VALUES (?, ?)", &params)
        .await
        .map_err(error)?
        != 1
    {
        return Err(JsValue::from_str("wrong changes"));
    }
    if pool
        .execute_uncertain_async("INSERT INTO t VALUES (1, 'duplicate')", &[])
        .await
        .unwrap_err()
        .kind()
        != ErrorKind::WriteOutcomeUnknown
    {
        return Err(JsValue::from_str("rejected write outcome not conservative"));
    }
    contract_disabled(&pool.clone()).await?;
    pool.execute_uncertain_async("CREATE TABLE types (i, f, b, n, payload)", &[])
        .await
        .map_err(error)?;
    let types = [
        param(DbType::I64, Value::I64(7))?,
        param(DbType::F64, Value::F64(1.25))?,
        param(DbType::Bool, Value::Bool(true))?,
        param(DbType::Text, Value::Null(DbType::Text))?,
        param(DbType::Bytes, Value::Bytes(vec![1, 2, 255]))?,
    ];
    pool.execute_uncertain_async("INSERT INTO types VALUES (?, ?, ?, ?, ?)", &types)
        .await
        .map_err(error)?;
    let neg = [
        param(DbType::I64, Value::I64(-7))?,
        param(DbType::F64, Value::F64(-1.25))?,
    ];
    pool.execute_uncertain_async("INSERT INTO types (i, f) VALUES (?, ?)", &neg)
        .await
        .map_err(error)?;
    for f in [1.0, -1.0, 0.0, -0.0, 9_007_199_254_740_992.0] {
        let p = [param(DbType::F64, Value::F64(f))?];
        if pool
            .execute_uncertain_async("INSERT INTO types (f) VALUES (?)", &p)
            .await
            .unwrap_err()
            .kind()
            != ErrorKind::Unsupported
        {
            return Err(JsValue::from_str("lossy float accepted"));
        }
        let b = vec![
            D1BatchStatement::new("INSERT INTO types (f) VALUES (?)", p.to_vec()).map_err(error)?,
        ];
        if pool.batch_uncertain_async(b).await.unwrap_err().kind() != ErrorKind::Unsupported {
            return Err(JsValue::from_str("lossy batch float accepted"));
        }
    }
    pool.execute_uncertain_async("CREATE TABLE edges (n)", &[])
        .await
        .map_err(error)?;
    let mut endpoints = Vec::new();
    for value in [-9_007_199_254_740_991, 9_007_199_254_740_991] {
        endpoints.push(
            D1BatchStatement::new(
                "INSERT INTO edges VALUES (?)",
                vec![param(DbType::I64, Value::I64(value))?],
            )
            .map_err(error)?,
        );
    }
    if pool.batch_uncertain_async(endpoints).await.map_err(error)? != [1, 1] {
        return Err(JsValue::from_str("successful batch changes mismatch"));
    }
    for value in [-9_007_199_254_740_992, 9_007_199_254_740_992] {
        let huge = [param(DbType::I64, Value::I64(value))?];
        if pool
            .execute_uncertain_async("INSERT INTO edges VALUES (?)", &huge)
            .await
            .unwrap_err()
            .kind()
            != ErrorKind::Unsupported
        {
            return Err(JsValue::from_str("unsafe integer accepted"));
        }
        let batch = vec![
            D1BatchStatement::new("INSERT INTO edges VALUES (42)", vec![]).map_err(error)?,
            D1BatchStatement::new("INSERT INTO edges VALUES (?)", huge.to_vec()).map_err(error)?,
        ];
        if pool.batch_uncertain_async(batch).await.unwrap_err().kind() != ErrorKind::Unsupported {
            return Err(JsValue::from_str("unsafe batch integer accepted"));
        }
    }
    let batch = vec![
        D1BatchStatement::new("INSERT INTO t VALUES (2, 'rollback')", vec![]).map_err(error)?,
        D1BatchStatement::new("INSERT INTO t VALUES (1, 'duplicate')", vec![]).map_err(error)?,
    ];
    if pool.batch_uncertain_async(batch).await.unwrap_err().kind() != ErrorKind::WriteOutcomeUnknown
    {
        return Err(JsValue::from_str("batch error not conservative"));
    }
    let pending = D1Pool::new(pending_binding.unchecked_into());
    let clone = pending.clone();
    let unpolled = pending.execute_uncertain_async("INSERT INTO t VALUES (3, 'unpolled')", &[]);
    drop(unpolled);
    contract_disabled(&pending).await?;
    let mut future = pending.execute_uncertain_async("INSERT INTO t VALUES (3, 'pending')", &[]);
    let waker = Waker::from(Arc::new(Noop));
    if !matches!(
        Future::poll(future.as_mut(), &mut Context::from_waker(&waker)),
        Poll::Pending
    ) {
        return Err(JsValue::from_str("dispatch was not pending"));
    }
    drop(future);
    contract_disabled(&pending).await?;
    contract_disabled(&clone).await?;
    if clone
        .execute_uncertain_async("INSERT INTO t VALUES (4, 'rejected')", &[])
        .await
        .unwrap_err()
        .kind()
        != ErrorKind::WriteOutcomeUnknown
    {
        return Err(JsValue::from_str("rejection must be uncertain"));
    }
    contract_disabled(&pending).await?;
    let batch =
        vec![D1BatchStatement::new("INSERT INTO t VALUES (5, 'rejected')", vec![]).map_err(error)?];
    if clone.batch_uncertain_async(batch).await.unwrap_err().kind()
        != ErrorKind::WriteOutcomeUnknown
    {
        return Err(JsValue::from_str("batch rejection must be uncertain"));
    }
    contract_disabled(&clone).await?;
    Ok("binding-exercised".into())
}

// ---------------------------------------------------------------------------
// Experimental D1 requery paging probes.
//
// Each probe uses an instrumented binding wrapper supplied by the harness so the
// JavaScript side can assert that no extra request was dispatched. One probe
// uses a wrapper whose promise never settles, which demonstrates that dropping
// the stream does not cancel it and does not dispatch again.
// ---------------------------------------------------------------------------

fn iparam(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}

fn text(value: &str) -> Result<Parameter, DbError> {
    Parameter::new(DbType::Text, Value::Text(value.to_owned()))
}

fn page_size(rows: usize) -> Result<PageSize, JsValue> {
    PageSize::try_from(rows).map_err(error)
}

fn page_budget(rows: usize, bytes: usize, pages: usize) -> Result<PageBudget, JsValue> {
    PageBudget::new(rows, bytes, pages).map_err(error)
}

fn check(condition: bool, message: &str) -> Result<(), JsValue> {
    if condition {
        Ok(())
    } else {
        Err(JsValue::from_str(message))
    }
}

/// Returns the bindings the caller wants for the next request, or `None` to stop.
struct Keyset {
    after: i64,
    page_rows: i64,
}

impl D1PageContinuation for Keyset {
    fn next_parameters(
        &mut self,
        previous: Option<&D1PagedPage>,
    ) -> Result<Option<Vec<Parameter>>, DbError> {
        if let Some(page) = previous {
            if page.is_empty() {
                return Ok(None);
            }
            if let Some(row) = page.rows().last() {
                self.after = row.get::<i64>("id")?;
            }
        }
        Ok(Some(vec![iparam(self.after)?, iparam(self.page_rows)?]))
    }
}

/// Always asks for another page with the same bindings.
struct Always(Vec<Parameter>);

impl D1PageContinuation for Always {
    fn next_parameters(
        &mut self,
        _: Option<&D1PagedPage>,
    ) -> Result<Option<Vec<Parameter>>, DbError> {
        Ok(Some(self.0.clone()))
    }
}

fn poll_page<C: D1PageContinuation>(
    stream: &mut D1PageStream<'_, C>,
) -> Poll<Option<Result<D1PagedPage, DbError>>> {
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    Pin::new(stream).poll_next(&mut context)
}

async fn collect_pages<C: D1PageContinuation>(
    mut stream: D1PageStream<'_, C>,
) -> Vec<Result<D1PagedPage, DbError>> {
    let mut items = Vec::new();
    loop {
        match poll_page(&mut stream) {
            Poll::Ready(Some(item)) => items.push(item),
            Poll::Ready(None) => return items,
            Poll::Pending => Delay::from(Duration::from_millis(0)).await,
        }
    }
}

async fn setup_pages(pool: &D1Pool) -> Result<(), JsValue> {
    pool.execute_uncertain_async(
        "CREATE TABLE pages (id INTEGER PRIMARY KEY, name TEXT, payload BLOB)",
        &[],
    )
    .await
    .map_err(error)?;
    let mut statements = Vec::new();
    for id in 1i64..=5 {
        statements.push(
            D1BatchStatement::new(
                "INSERT INTO pages VALUES (?, ?, ?)",
                vec![
                    iparam(id).map_err(error)?,
                    text(&format!("name-{id}")).map_err(error)?,
                    Parameter::new(
                        DbType::Bytes,
                        Value::Bytes(vec![u8::try_from(id).map_err(error)?, 0xff]),
                    )
                    .map_err(error)?,
                ],
            )
            .map_err(error)?,
        );
    }
    pool.batch_uncertain_async(statements).await.map_err(error)?;
    Ok(())
}

const SEQUENCE_SQL: &str =
    "SELECT id, name, payload FROM pages WHERE id > ? ORDER BY id LIMIT ?";

/// Happy path: keyset requery, typed bindings, page order, payload preservation.
#[wasm_bindgen]
pub async fn paging_probe_sequence(
    binding: JsValue,
    counted_binding: JsValue,
) -> Result<String, JsValue> {
    let setup = D1Pool::new(binding.unchecked_into());
    setup_pages(&setup).await?;

    let pool = D1Pool::new(counted_binding.unchecked_into());
    let pager = pool.pager(
        SEQUENCE_SQL,
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 65_536, 10)?,
    );
    let pages = collect_pages(pager.pages(Keyset {
        after: 0,
        page_rows: 2,
    }))
    .await;
    let pages = pages
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    // The final non-empty page is short, so the follow-up request is empty and ends the sequence.
    check(pages.len() == 4, "paging sequence page count")?;
    let expected = [vec![1i64, 2], vec![3, 4], vec![5]];
    for (index, page) in pages.iter().enumerate() {
        check(page.page_index() == index, "paging sequence page index")?;
    }
    for (index, ids) in expected.iter().enumerate() {
        let actual = pages[index]
            .rows()
            .iter()
            .map(|row| row.get::<i64>("id"))
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        check(&actual == ids, "paging sequence ids")?;
    }
    check(pages[3].is_empty(), "paging sequence short page ending")?;
    let first = &pages[0].rows()[0];
    check(
        first.read_at::<String>(1).map_err(error)? == "name-1",
        "paging sequence name",
    )?;
    check(
        first.value_at(2).map_err(error)? == Value::Bytes(vec![1, 0xff]),
        "paging sequence payload",
    )?;
    Ok("paging-sequence".into())
}

/// Bounds: PageSize, aggregate rows, aggregate bytes, and the page count.
#[wasm_bindgen]
pub async fn paging_probe_bounds(counted_binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(counted_binding.unchecked_into());

    let pager = pool.pager(
        "SELECT id FROM pages ORDER BY id LIMIT 3",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 65_536, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::Decode => {}
        _ => return Err(JsValue::from_str("paging page size bound")),
    }

    let pager = pool.pager(
        "SELECT id FROM pages ORDER BY id LIMIT 2",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(2, 65_536, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Ok(page), Err(error)] if page.len() == 2 && error.kind() == ErrorKind::Overflow => {}
        _ => return Err(JsValue::from_str("paging exact row budget")),
    }

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = 1",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 1, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::Overflow => {}
        _ => return Err(JsValue::from_str("paging byte budget")),
    }

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = 999",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 1)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Ok(page), Err(error)] if page.is_empty() && error.kind() == ErrorKind::Overflow => {}
        _ => return Err(JsValue::from_str("paging empty page budget")),
    }

    Ok("paging-bounds".into())
}

/// Errors: pre-dispatch validation, an uncertain request, and a rejected promise.
#[wasm_bindgen]
pub async fn paging_probe_errors(
    counted_binding: JsValue,
    reject_binding: JsValue,
) -> Result<String, JsValue> {
    let pool = D1Pool::new(counted_binding.unchecked_into());

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = ?",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::InvalidArgument => {}
        _ => return Err(JsValue::from_str("paging placeholder mismatch")),
    }

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = ?",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let pages = collect_pages(
        pager.pages(Always(vec![
            iparam(1).map_err(error)?,
            iparam(2).map_err(error)?,
        ])),
    )
    .await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::InvalidArgument => {}
        _ => return Err(JsValue::from_str("paging extra bindings")),
    }

    let pager = pool.pager(
        "SELECT ?",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(vec![
        Parameter::new(DbType::I64, Value::I64(9_007_199_254_740_992)).map_err(error)?,
    ])))
    .await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::Unsupported => {}
        _ => return Err(JsValue::from_str("paging unsafe parameter")),
    }

    let pager = pool.pager(
        "SELECT id FROM no_such_table",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::WriteOutcomeUnknown => {}
        _ => return Err(JsValue::from_str("paging missing table outcome")),
    }

    let reject = D1Pool::new(reject_binding.unchecked_into());
    let pager = reject.pager(
        "SELECT 1",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let pages = collect_pages(pager.pages(Always(Vec::new()))).await;
    match pages.as_slice() {
        [Err(error)] if error.kind() == ErrorKind::WriteOutcomeUnknown => {}
        _ => return Err(JsValue::from_str("paging rejected promise outcome")),
    }

    Ok("paging-errors".into())
}

/// A dispatched request whose promise never settles; dropping must not dispatch again.
#[wasm_bindgen]
pub async fn paging_probe_pending(pending_binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(pending_binding.unchecked_into());
    let pager = pool.pager(
        "SELECT id FROM pages",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let mut stream = pager.pages(Always(Vec::new()));
    match poll_page(&mut stream) {
        Poll::Pending => {}
        _ => return Err(JsValue::from_str("paging pending request settled")),
    }
    drop(stream);
    Ok("paging-pending".into())
}

/// Raw value decoding through the actual binding: position, bytes, and precision.
#[wasm_bindgen]
pub async fn paging_probe_values(binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(binding.unchecked_into());

    let pager = pool.pager(
        "SELECT id AS \"0\", name AS \"1\", payload, 1.25 AS f, 9007199254740991 AS max_int \
         FROM pages WHERE id = 1",
        D1ReadOnlyReplaySafe::assert(),
        page_size(8)?,
        page_budget(10, 65_536, 10)?,
    );
    let rows = pager.fetch_page_async(&[]).await.map_err(error)?;
    check(rows.len() == 1, "paging value row count")?;
    let row = &rows[0];
    check(
        row.read_at::<i64>(0).map_err(error)? == 1,
        "paging value ordinal 0",
    )?;
    check(
        row.read_at::<String>(1).map_err(error)? == "name-1",
        "paging value ordinal 1",
    )?;
    check(
        row.value_at(2).map_err(error)? == Value::Bytes(vec![1, 0xff]),
        "paging value payload",
    )?;
    check(
        row.read_at::<f64>(3).map_err(error)? == 1.25,
        "paging value float",
    )?;
    check(
        row.read_at::<i64>(4).map_err(error)? == 9_007_199_254_740_991,
        "paging value max safe integer",
    )?;

    let pager = pool.pager(
        "SELECT 9007199254740992.0 AS big",
        D1ReadOnlyReplaySafe::assert(),
        page_size(1)?,
        page_budget(10, 100, 10)?,
    );
    let failure = pager
        .fetch_page_async(&[])
        .await
        .expect_err("unsafe integer");
    check(
        failure.kind() == ErrorKind::Unsupported,
        "paging unsafe integer",
    )?;

    let pager = pool.pager(
        "SELECT id FROM pages ORDER BY id",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 65_536, 10)?,
    );
    let failure = pager.fetch_page_async(&[]).await.expect_err("page size");
    check(failure.kind() == ErrorKind::Decode, "paging one page size")?;

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = 1",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 1, 10)?,
    );
    let failure = pager.fetch_page_async(&[]).await.expect_err("byte budget");
    check(
        failure.kind() == ErrorKind::Overflow,
        "paging one page bytes",
    )?;

    // The pinned binding returns the column header even when a SELECT matches no rows, so this is
    // an empty page rather than a missing header.
    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = 999",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let rows = pager.fetch_page_async(&[]).await.map_err(error)?;
    check(rows.is_empty(), "paging empty select")?;

    let pager = pool.pager(
        "SELECT id FROM pages WHERE id = ?",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let rows = pager
        .fetch_page_async(&[iparam(2).map_err(error)?])
        .await
        .map_err(error)?;
    check(
        rows.len() == 1 && rows[0].read_at::<i64>(0).map_err(error)? == 2,
        "paging one page parameter",
    )?;

    Ok("paging-values".into())
}

/// A binding double's exact and unsafe integer cells, through the real wasm decoder.
#[wasm_bindgen]
pub async fn paging_probe_bigint(binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(binding.unchecked_into());

    let pager = pool.pager(
        "bigint_exact",
        D1ReadOnlyReplaySafe::assert(),
        page_size(1)?,
        page_budget(10, 100, 10)?,
    );
    let rows = pager.fetch_page_async(&[]).await.map_err(error)?;
    check(
        rows.len() == 1 && rows[0].read_at::<i64>(0).map_err(error)? == 9_007_199_254_740_992,
        "paging bigint exact",
    )?;

    let pager = pool.pager(
        "bigint_exact_plus_one",
        D1ReadOnlyReplaySafe::assert(),
        page_size(1)?,
        page_budget(10, 100, 10)?,
    );
    let rows = pager.fetch_page_async(&[]).await.map_err(error)?;
    check(
        rows.len() == 1 && rows[0].read_at::<i64>(0).map_err(error)? == 9_007_199_254_740_993,
        "paging bigint exact past the number boundary",
    )?;

    for sql in ["bigint_overflow", "number_unsafe", "number_rounded"] {
        let pager = pool.pager(
            sql,
            D1ReadOnlyReplaySafe::assert(),
            page_size(1)?,
            page_budget(10, 100, 10)?,
        );
        let failure = pager
            .fetch_page_async(&[])
            .await
            .expect_err("unrepresentable integer");
        check(
            failure.kind() == ErrorKind::Unsupported,
            "paging unrepresentable integer",
        )?;
    }

    Ok("paging-bigint".into())
}

/// A binding double's malformed raw results and an actual null cell.
#[wasm_bindgen]
pub async fn paging_probe_malformed(binding: JsValue) -> Result<String, JsValue> {
    let pool = D1Pool::new(binding.unchecked_into());

    for sql in [
        "missing_header",
        "empty_header",
        "not_array",
        "undefined_cell",
        "hole_cell",
    ] {
        let pager = pool.pager(
            sql,
            D1ReadOnlyReplaySafe::assert(),
            page_size(2)?,
            page_budget(10, 100, 10)?,
        );
        let failure = pager
            .fetch_page_async(&[])
            .await
            .expect_err("malformed raw result");
        check(
            failure.kind() == ErrorKind::Decode,
            "paging malformed result",
        )?;
    }

    let pager = pool.pager(
        "null_cell",
        D1ReadOnlyReplaySafe::assert(),
        page_size(2)?,
        page_budget(10, 100, 10)?,
    );
    let rows = pager.fetch_page_async(&[]).await.map_err(error)?;
    check(
        rows.len() == 1
            && rows[0].value_at(0).map_err(error)? == Value::Null(DbType::Text),
        "paging null cell",
    )?;

    Ok("paging-malformed".into())
}
