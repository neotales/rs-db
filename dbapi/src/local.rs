//! Local-future counterparts to the asynchronous traits.
//!
//! The root traits use boxed [`Send`] futures so a provider can be driven from any runtime. Some
//! hosts cannot meet that bound: a Cloudflare D1 binding in a Workers request is one example,
//! because the JavaScript promise future is not `Send`. This module mirrors [`crate::Pool`],
//! [`crate::Session`], [`crate::Transaction`], and [`crate::Reader`] with the same `_async` names,
//! the same [`QueryOptions`], rows, limits, errors, and convenience calls, but its futures carry no
//! `Send` requirement. The [`paging`] submodule mirrors the single-reader page/row views and the
//! explicit multi-request source pager in local futures; it makes no `Send` assertion.
//!
//! These traits are a local-future API, not a thread-transfer grant. A [`DbFuture`] here may hold
//! `Rc`, JavaScript handles, or another single-threaded value. Poll it on the thread that created
//! it, and do not wrap it in an unsafe `Send` assertion to move it to another thread. The root
//! [`crate::DbFuture`] remains `Send`; this module does not weaken, replace, or alias it.
//!
//! `capabilities` is synchronous here too, so qualify it as [`Pool::capabilities`] or
//! [`crate::Pool::capabilities`] when both families are in scope. A provider that implements both
//! families must keep their behavior consistent; implementing this module alone is enough for a
//! host whose futures are local.
//!
//! A pool, session, transaction, or reader from this module is object safe for the same reasons as
//! its root counterpart: methods take `&self`, `&mut self`, or `Box<Self>`, and every future is
//! boxed. The trait types are not `Send`; a `dyn Pool` is not a thread-safe value even when the
//! concrete implementation happens to be.
//!
//! ```
//! use neotales_dbapi::{Capabilities, local, PoolDefaults};
//!
//! struct LocalProvider;
//!
//! impl local::Pool for LocalProvider {
//!     fn capabilities(&self) -> Capabilities {
//!         Capabilities::default()
//!     }
//!
//!     fn defaults(&self) -> PoolDefaults {
//!         PoolDefaults::default()
//!     }
//!
//!     fn acquire_async(&self) -> local::DbFuture<'_, Box<dyn local::Session>> {
//!         Box::pin(async { Err(neotales_dbapi::DbError::new(neotales_dbapi::ErrorKind::Unsupported)) })
//!     }
//! }
//! ```
//!
//! A local future is deliberately not usable where `Send` is required:
//!
//! ```compile_fail
//! use neotales_dbapi::{local, QueryOptions};
//!
//! fn require_send<T: Send>(_: T) {}
//!
//! fn move_it(pool: &dyn local::Pool) {
//!     require_send(pool.fetch_all_async("SELECT 1", QueryOptions::new()));
//! }
//! ```
//!
//! The trait object is not `Send` either; a type can implement this module and still hold
//! single-threaded state:
//!
//! ```compile_fail
//! use neotales_dbapi::local;
//!
//! fn require_send<T: Send>(_: T) {}
//!
//! fn move_it(pool: Box<dyn local::Pool>) {
//!     require_send(pool);
//! }
//! ```

use std::{collections::VecDeque, future::Future, pin::Pin};

pub mod paging;

use super::{
    Capabilities, DbError, ErrorKind, PoolDefaults, QueryOptions, Row, RowLimit, TxOptions, Value,
};

/// A boxed future returning a sanitized database result without a `Send` requirement.
///
/// This is the local counterpart of [`crate::DbFuture`]. Poll it on the thread that created it;
/// moving it to another thread needs a separate safety proof that this type does not provide.
pub type DbFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DbError>> + 'a>>;

/// A statement's bounded result sets, consumed row by row on a local thread.
///
/// The lifecycle is identical to [`crate::Reader`]: [`Reader::next_row_async`] yields rows from the
/// current set, [`Reader::has_next_set_async`] advances and discards unread rows, and
/// [`Reader::finish_async`] drains the whole batch. A reader is not proof of a read: statements
/// such as `INSERT ... RETURNING` can write.
pub trait Reader {
    /// Returns the next row in the current result set, or `None` at the set's end.
    ///
    /// # Errors
    /// Returns a sanitized provider error. After an error the reader is finished; further calls
    /// return `None` or the same error.
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>>;

    /// Advances to the next result set, discarding any unread rows in the current one.
    ///
    /// Returns `true` when a next set is current, or `false` at the end of the batch. A single-set
    /// provider returns `false`; it must not fabricate result boundaries.
    ///
    /// # Errors
    /// Returns a sanitized provider error while draining or advancing.
    fn has_next_set_async(&mut self) -> DbFuture<'_, bool>;

    /// Drains and discards every remaining row and result set.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while draining.
    fn finish_async(&mut self) -> DbFuture<'_, ()>;

    /// Drains every result set and returns the owned rows, flattening set boundaries.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_all_async(&mut self) -> DbFuture<'_, Vec<Row>> {
        Box::pin(async move {
            Ok(self
                .read_sets_async()
                .await?
                .into_iter()
                .flatten()
                .collect())
        })
    }

    /// Drains every result set and returns the rows grouped by set, including empty sets.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_sets_async(&mut self) -> DbFuture<'_, Vec<Vec<Row>>> {
        Box::pin(async move {
            let mut sets = Vec::new();
            loop {
                let mut set = Vec::new();
                while let Some(row) = self.next_row_async().await? {
                    set.push(row);
                }
                sets.push(set);
                if !self.has_next_set_async().await? {
                    break;
                }
            }
            self.finish_async().await?;
            Ok(sets)
        })
    }
}

/// A local reader over an already-materialized batch, preserving result-set boundaries.
///
/// This is the local counterpart of [`crate::BufferedReader`]. It owns every row, so `finish_async`
/// and `has_next_set_async` only discard memory; no connection cleanup is outstanding.
#[derive(Debug)]
pub struct BufferedReader {
    sets: VecDeque<VecDeque<Row>>,
    done: bool,
}

impl BufferedReader {
    /// Wraps owned rows as one result set.
    #[must_use]
    pub fn new(rows: Vec<Row>) -> Self {
        Self::from_sets(vec![rows])
    }

    /// Wraps owned rows grouped by result set, including empty sets.
    #[must_use]
    pub fn from_sets(sets: Vec<Vec<Row>>) -> Self {
        Self {
            sets: sets.into_iter().map(VecDeque::from).collect(),
            done: false,
        }
    }
}

impl Reader for BufferedReader {
    fn next_row_async(&mut self) -> DbFuture<'_, Option<Row>> {
        let row = if self.done {
            None
        } else {
            self.sets.front_mut().and_then(VecDeque::pop_front)
        };
        Box::pin(async move { Ok(row) })
    }

    fn has_next_set_async(&mut self) -> DbFuture<'_, bool> {
        if self.done {
            return Box::pin(async { Ok(false) });
        }
        self.sets.pop_front();
        let more = !self.sets.is_empty();
        if !more {
            self.done = true;
        }
        Box::pin(async move { Ok(more) })
    }

    fn finish_async(&mut self) -> DbFuture<'_, ()> {
        self.sets.clear();
        self.done = true;
        Box::pin(async { Ok(()) })
    }
}

/// A local pool able to acquire exclusive sessions and report truthful capabilities.
///
/// This trait does not require [`Send`] or [`Sync`]. Use [`crate::Pool`] when the pool must cross
/// a thread or task boundary.
pub trait Pool {
    /// Returns capabilities for this provider/configuration.
    fn capabilities(&self) -> Capabilities;

    /// Returns defaults used for options inherited by this pool.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }

    /// Acquires one exclusive session; the caller must drop it to release it.
    fn acquire_async(&self) -> DbFuture<'_, Box<dyn Session>>;

    /// Executes one statement through a newly acquired session with per-query policy overrides.
    /// Overrides must reject an explicit fetch limit before dispatch.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_async<'a>(&'a self, sql: &'a str, mut options: QueryOptions) -> DbFuture<'a, u64> {
        Box::pin(async move {
            options.validate_execute()?;
            super::reject_unhandled_pool_options(&options)?;
            super::resolve_provider_timeout(&mut options, self.defaults().timeout)?;
            let mut session = self.acquire_async().await?;
            session.execute_async(sql, options).await
        })
    }

    /// Fetches one bounded batch through a newly acquired session with per-query overrides.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_async<'a>(
        &'a self,
        sql: &'a str,
        mut options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>> {
        Box::pin(async move {
            super::reject_unhandled_pool_options(&options)?;
            let limit = options.resolved_limit(self.defaults());
            options.limit = super::LimitSetting::Set(limit);
            super::resolve_provider_timeout(&mut options, self.defaults().timeout)?;
            let mut session = self.acquire_async().await?;
            let mut reader = session.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(Box::new(BufferedReader::from_sets(sets)) as Box<dyn Reader + 'a>)
        })
    }

    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        Box::pin(async move {
            let limit = options.resolved_limit(self.defaults());
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(super::row_cardinality_error(false)),
                Some(_) if more => Err(super::row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar_async<'a>(
        &'a self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }
}

/// An exclusively borrowed local database session.
///
/// The `Send`-free future contract does not add cancellation guarantees. If a statement or script
/// future is dropped after dispatch, the provider must roll back or discard the connection before
/// it can be reused. A write whose dispatched result cannot be established must return
/// [`ErrorKind::WriteOutcomeUnknown`], including `fetch_async` calls such as `INSERT ... RETURNING`;
/// fetch is not proof of a read. A commit whose dispatched result is unknown must return
/// [`ErrorKind::CommitOutcomeUnknown`]. These rules mirror [`crate::Session`] and
/// [`crate::Transaction`]. Providers should use [`QueryOptions::prepare_provider`],
/// [`QueryOptions::prepare_execute_provider`], and [`QueryOptions::prepare_script_provider`] at
/// their respective operation boundaries so unsupported settings and explicit fetch limits are
/// rejected before SQL dispatch.
pub trait Session {
    /// Defaults inherited by operations on this session.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }

    /// Executes one positional-parameter statement without rewriting SQL.
    ///
    /// Providers must reject an explicit fetch limit before dispatch. Dropping this future after
    /// dispatch does not cancel the write or prove its outcome; the provider must roll back or
    /// discard the connection before reuse and must return
    /// [`ErrorKind::WriteOutcomeUnknown`] when success cannot be established.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64>;

    /// Fetches one bounded batch as a reader.
    ///
    /// Fetch may execute a write, for example `INSERT ... RETURNING`. If this future is dropped
    /// after dispatch, the provider must clean up or discard the connection before reuse. If the
    /// outcome of a possibly writing statement cannot be established, return
    /// [`ErrorKind::WriteOutcomeUnknown`], not a retryable transport error.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including `WriteOutcomeUnknown` when a dispatched
    /// write's outcome cannot be established.
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>>;

    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        let limit = options.resolved_limit(self.defaults());
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(super::row_cardinality_error(false)),
                Some(_) if more => Err(super::row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing`, or a sanitized provider error.
    fn fetch_scalar_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }

    /// Executes a trusted multi-statement script that must not manage transactions.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()>;

    /// Begins an interactive transaction on this same session.
    ///
    /// If this future is dropped after begin dispatch, the provider must roll back or discard the
    /// connection before reuse; dropping a local transaction value alone does not prove rollback.
    ///
    /// # Errors
    /// Returns `Unsupported` before dispatch when the provider has no interactive transactions.
    fn begin_async<'a>(
        &'a mut self,
        options: TxOptions,
    ) -> DbFuture<'a, Box<dyn Transaction<'a> + 'a>>;
}

/// A local transaction that exclusively borrows its session until consumed or dropped.
///
/// The cleanup and unknown-outcome rules are identical to [`crate::Transaction`]. Dropping a
/// transaction or interrupting a begin/commit/rollback future does not prove rollback or closure;
/// the provider must complete rollback or discard the connection before reuse.
pub trait Transaction<'session> {
    /// Defaults inherited by operations on this transaction.
    fn defaults(&self) -> PoolDefaults {
        PoolDefaults::default()
    }

    /// Executes one statement with positional parameters in slice order.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, u64>;

    /// Fetches one bounded batch as a reader.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn fetch_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Box<dyn Reader + 'a>>;

    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Vec<Row>> {
        let limit = options.resolved_limit(self.defaults());
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let sets = collect_sets(reader.as_mut(), limit).await?;
            Ok(sets.into_iter().flatten().collect())
        })
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Option<Row>> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            reader.finish_async().await?;
            Ok(first)
        })
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one_async<'a>(&'a mut self, sql: &'a str, options: QueryOptions) -> DbFuture<'a, Row> {
        Box::pin(async move {
            let mut reader = self.fetch_async(sql, options).await?;
            let first = reader.next_row_async().await?;
            let more = reader.next_row_async().await?.is_some();
            reader.finish_async().await?;
            match first {
                None => Err(super::row_cardinality_error(false)),
                Some(_) if more => Err(super::row_cardinality_error(true)),
                Some(row) => Ok(row),
            }
        })
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing`, or a sanitized provider error.
    fn fetch_scalar_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, Value> {
        Box::pin(async move {
            let row = self.fetch_one_async(sql, options).await?;
            row.value_at(0)
        })
    }

    /// Executes a trusted multi-statement script that must not manage transactions.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_trusted_script_async<'a>(
        &'a mut self,
        sql: &'a str,
        options: QueryOptions,
    ) -> DbFuture<'a, ()>;

    /// Commits and consumes this transaction.
    ///
    /// If the commit future is dropped after dispatch, the outcome is still unknown; dropping it
    /// does not prove rollback. The provider must finish cleanup or discard the connection before
    /// reuse.
    ///
    /// # Errors
    /// Returns `CommitOutcomeUnknown` if commit was dispatched without an authoritative result.
    fn commit_async(self: Box<Self>) -> DbFuture<'session, ()>;

    /// Rolls back and consumes this transaction.
    ///
    /// If this future is dropped, the provider must finish rollback or discard the connection
    /// before reuse.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn rollback_async(self: Box<Self>) -> DbFuture<'session, ()>;
}

async fn collect_sets(reader: &mut dyn Reader, limit: RowLimit) -> Result<Vec<Vec<Row>>, DbError> {
    let mut sets = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    loop {
        let mut set = Vec::new();
        while let Some(row) = reader.next_row_async().await? {
            if rows == limit.rows() {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            let size = row.encoded_bytes()?;
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
            if bytes > limit.bytes() {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            rows = rows
                .checked_add(1)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
            set.push(row);
        }
        sets.push(set);
        if !reader.has_next_set_async().await? {
            break;
        }
        if sets.len() == limit.max_sets() {
            return Err(DbError::new(ErrorKind::Overflow));
        }
    }
    reader.finish_async().await?;
    Ok(sets)
}
