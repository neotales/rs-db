//! Native synchronous counterparts to the asynchronous traits.
//!
//! These traits reuse [`Row`], [`RowLimit`], [`DbError`], and every option type from the
//! asynchronous contract. They contain no runtime, executor, or worker thread. A provider that is
//! natively blocking can implement them directly; a provider whose driver is asynchronous should
//! be used through `crate::blocking::BlockingPool` (feature `blocking`) or another adapter that
//! owns an executor. The [`paging`] submodule adds blocking page/row iterators and the synchronous
//! multi-request source pager over these reader contracts.
//!
//! Synchronous methods deliberately keep their unsuffixed names (`execute`, `fetch`, `next_row`,
//! and so on). Asynchronous methods use the `_async` suffix where both traits need to coexist on
//! one provider type; that naming rule does not change this module's public names. `capabilities`
//! is synchronous in both pool traits, so qualify it as [`Pool::capabilities`] or
//! [`crate::Pool::capabilities`] when both traits are in scope.
//!
//! The lifecycle rules are the same as the asynchronous reader:
//!
//! - [`Reader::next_row`] yields rows from the current result set.
//! - [`Reader::has_next_set`] advances, discarding unread rows in the current set.
//! - [`Reader::finish`] drains the whole batch and surfaces a later statement error.
//! - Dropping an unfinished reader must discard the connection when unread provider results are
//!   still attached. A reader over an already-materialized batch has nothing to discard.
//!
//! A native synchronous provider may stream from the driver. An adapter over asynchronous traits
//! must not drive a future by blocking inside an async-runtime thread; see
//! `crate::blocking` for that rule and the dedicated-worker implementation.

use std::collections::VecDeque;

pub mod paging;

use super::{Capabilities, DbError, ErrorKind, QueryOptions, Row, RowLimit, TxOptions, Value};

/// A statement's bounded result sets, consumed synchronously row by row.
///
/// A reader is not proof of a read: statements such as `INSERT ... RETURNING` can write. See the
/// module documentation for the lifecycle rules shared with the asynchronous reader.
pub trait Reader: Send {
    /// Returns the next row in the current result set, or `None` at the set's end.
    ///
    /// # Errors
    /// Returns a sanitized provider error. After an error the reader is finished; further calls
    /// return `None` or the same error.
    fn next_row(&mut self) -> Result<Option<Row>, DbError>;

    /// Advances to the next result set, discarding any unread rows in the current one.
    ///
    /// Returns `true` when a next set is current, or `false` at the end of the batch. A single-set
    /// provider returns `false`; it must not fabricate result boundaries.
    ///
    /// # Errors
    /// Returns a sanitized provider error while draining or advancing.
    fn has_next_set(&mut self) -> Result<bool, DbError>;

    /// Drains and discards every remaining row and result set.
    ///
    /// The reader is finished afterwards. A provider with unread results must give dropping the
    /// reader the same cleanup effect on the connection, but dropping cannot report a later
    /// statement error.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while draining.
    fn finish(&mut self) -> Result<(), DbError>;

    /// Drains every result set and returns the owned rows, flattening set boundaries.
    ///
    /// This is a convenience for callers that want the whole bounded batch. It does not add a
    /// limit of its own; providers apply the [`RowLimit`] supplied to `fetch`. Use
    /// [`Reader::read_sets`] when boundaries matter.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_all(&mut self) -> Result<Vec<Row>, DbError> {
        Ok(self.read_sets()?.into_iter().flatten().collect())
    }

    /// Drains every result set and returns the rows grouped by set, including empty sets.
    ///
    /// This is a convenience for callers that want the whole bounded batch without losing
    /// boundaries. It does not add a limit of its own; providers apply the [`RowLimit`] supplied to
    /// `fetch`.
    ///
    /// # Errors
    /// Returns the first sanitized error encountered while reading or draining.
    fn read_sets(&mut self) -> Result<Vec<Vec<Row>>, DbError> {
        let mut sets = Vec::new();
        loop {
            let mut set = Vec::new();
            while let Some(row) = self.next_row()? {
                set.push(row);
            }
            sets.push(set);
            if !self.has_next_set()? {
                break;
            }
        }
        self.finish()?;
        Ok(sets)
    }
}

/// A synchronous reader over an already-materialized batch, preserving result-set boundaries.
///
/// Providers and wrappers that materialize the bounded batch before returning use this. It has no
/// unread wire results, so `finish` and `has_next_set` only discard the in-memory rows. Empty sets
/// remain representable and countable.
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
    fn next_row(&mut self) -> Result<Option<Row>, DbError> {
        if self.done {
            return Ok(None);
        }
        Ok(self.sets.front_mut().and_then(VecDeque::pop_front))
    }

    fn has_next_set(&mut self) -> Result<bool, DbError> {
        if self.done {
            return Ok(false);
        }
        self.sets.pop_front();
        let more = !self.sets.is_empty();
        if !more {
            self.done = true;
        }
        Ok(more)
    }

    fn finish(&mut self) -> Result<(), DbError> {
        self.sets.clear();
        self.done = true;
        Ok(())
    }
}

/// A pool able to acquire exclusive synchronous sessions and report truthful capabilities.
pub trait Pool: Send + Sync {
    /// Returns capabilities for this provider/configuration.
    fn capabilities(&self) -> Capabilities;
    /// Returns defaults used for inherited query settings.
    fn defaults(&self) -> super::PoolDefaults {
        super::PoolDefaults::default()
    }

    /// Acquires one exclusive session; the caller must drop it to release it.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn acquire(&self) -> Result<Box<dyn Session>, DbError>;

    /// Executes one statement through a newly acquired session with per-query policy overrides.
    /// Overrides must reject an explicit fetch limit before dispatch.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute(&self, sql: &str, mut options: QueryOptions) -> Result<u64, DbError> {
        options.validate_execute()?;
        super::reject_unhandled_pool_options(&options)?;
        super::resolve_provider_timeout(&mut options, self.defaults().timeout)?;
        let mut session = self.acquire()?;
        session.execute(sql, options)
    }

    /// Fetches one bounded batch through a newly acquired session using default options.
    ///
    /// The default implementation drains the batch and returns a reader over owned rows, because
    /// the acquired session cannot outlive this call. A provider may override it with a reader
    /// that owns its connection.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch(&self, sql: &str, mut options: QueryOptions) -> Result<Box<dyn Reader + '_>, DbError> {
        super::reject_unhandled_pool_options(&options)?;
        let limit = options.resolved_limit(self.defaults());
        options.limit = super::LimitSetting::Set(limit);
        super::resolve_provider_timeout(&mut options, self.defaults().timeout)?;
        let mut session = self.acquire()?;
        let mut reader = session.fetch(sql, options)?;
        let sets = collect_sets(reader.as_mut(), limit)?;
        Ok(Box::new(BufferedReader::from_sets(sets)))
    }

    /// Fetches and drains the whole bounded batch using default options.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all(&self, sql: &str, options: QueryOptions) -> Result<Vec<Row>, DbError> {
        let limit = options.resolved_limit(self.defaults());
        let mut reader = self.fetch(sql, options)?;
        let sets = collect_sets(reader.as_mut(), limit)?;
        Ok(sets.into_iter().flatten().collect())
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first(&self, sql: &str, options: QueryOptions) -> Result<Option<Row>, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        reader.finish()?;
        Ok(first)
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// Remaining rows and later result sets are drained before returning. A later error wins over
    /// the cardinality result.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one(&self, sql: &str, options: QueryOptions) -> Result<Row, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        let more = reader.next_row()?.is_some();
        reader.finish()?;
        match first {
            None => Err(row_cardinality_error(false)),
            Some(_) if more => Err(row_cardinality_error(true)),
            Some(row) => Ok(row),
        }
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar(&self, sql: &str, options: QueryOptions) -> Result<Value, DbError> {
        let row = self.fetch_one(sql, options)?;
        row.value_at(0)
    }
}

/// An exclusively borrowed synchronous database session.
pub trait Session: Send {
    /// Defaults inherited by this session.
    fn defaults(&self) -> super::PoolDefaults {
        super::PoolDefaults::default()
    }
    /// Executes one positional-parameter statement; parameters follow placeholder order, with no
    /// SQL rewriting. Providers reject placeholder/parameter count mismatches and explicit fetch
    /// limits before dispatch. Inherited fetch limits are permitted.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute(&mut self, sql: &str, options: QueryOptions) -> Result<u64, DbError>;

    /// Fetches one bounded batch as a reader.
    ///
    /// The reader may borrow this session until it is finished or dropped; a provider that
    /// materializes the batch may release the session earlier. The limit applies to the whole
    /// batch; providers that support multiple result sets count rows and bytes across sets.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn fetch(&mut self, sql: &str, options: QueryOptions) -> Result<Box<dyn Reader + '_>, DbError>;

    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all(&mut self, sql: &str, options: QueryOptions) -> Result<Vec<Row>, DbError> {
        let limit = options.resolved_limit(self.defaults());
        let mut reader = self.fetch(sql, options)?;
        let sets = collect_sets(reader.as_mut(), limit)?;
        Ok(sets.into_iter().flatten().collect())
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first(&mut self, sql: &str, options: QueryOptions) -> Result<Option<Row>, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        reader.finish()?;
        Ok(first)
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one(&mut self, sql: &str, options: QueryOptions) -> Result<Row, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        let more = reader.next_row()?.is_some();
        reader.finish()?;
        match first {
            None => Err(row_cardinality_error(false)),
            Some(_) if more => Err(row_cardinality_error(true)),
            Some(row) => Ok(row),
        }
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar(&mut self, sql: &str, options: QueryOptions) -> Result<Value, DbError> {
        let row = self.fetch_one(sql, options)?;
        row.value_at(0)
    }

    /// Executes a multi-statement unparameterized script. Input must be trusted; scripts must not
    /// manage transactions. An interrupted operation requires provider cleanup before session use.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_trusted_script(&mut self, sql: &str, options: QueryOptions) -> Result<(), DbError>;

    /// Begins an interactive transaction on this same session.
    ///
    /// Unsupported requested options fail before dispatch. If the transaction handle is dropped
    /// without commit or rollback, the provider must complete rollback or discard the connection
    /// before reuse.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn begin(&mut self, options: TxOptions) -> Result<Box<dyn Transaction<'_> + '_>, DbError>;
}

/// A synchronous transaction that exclusively borrows its session until consumed or dropped.
///
/// Dropping without commit or rollback must not leave a connection reusable while a transaction
/// may remain open. The provider must complete rollback or discard that connection before allowing
/// reuse. A dispatched commit with unconfirmed success is [`ErrorKind::CommitOutcomeUnknown`], not
/// a rollback.
pub trait Transaction<'session>: Send {
    /// Defaults inherited by this transaction.
    fn defaults(&self) -> super::PoolDefaults {
        super::PoolDefaults::default()
    }
    /// Executes one statement with positional parameters in slice order, without SQL rewriting.
    /// Providers reject an explicit fetch limit before dispatch; inherited limits are permitted.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute(&mut self, sql: &str, options: QueryOptions) -> Result<u64, DbError>;

    /// Fetches one bounded batch as a reader.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn fetch(&mut self, sql: &str, options: QueryOptions) -> Result<Box<dyn Reader + '_>, DbError>;

    /// Fetches and drains the whole bounded batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_all(&mut self, sql: &str, options: QueryOptions) -> Result<Vec<Row>, DbError> {
        let limit = options.resolved_limit(self.defaults());
        let mut reader = self.fetch(sql, options)?;
        let sets = collect_sets(reader.as_mut(), limit)?;
        Ok(sets.into_iter().flatten().collect())
    }

    /// Fetches at most the first row of the first result set, then drains the full batch.
    ///
    /// # Errors
    /// Returns a sanitized provider error, including one found after the first row.
    fn fetch_first(&mut self, sql: &str, options: QueryOptions) -> Result<Option<Row>, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        reader.finish()?;
        Ok(first)
    }

    /// Fetches a statement whose first result set must contain exactly one row.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, or a sanitized provider error found while reading.
    fn fetch_one(&mut self, sql: &str, options: QueryOptions) -> Result<Row, DbError> {
        let mut reader = self.fetch(sql, options)?;
        let first = reader.next_row()?;
        let more = reader.next_row()?.is_some();
        reader.finish()?;
        match first {
            None => Err(row_cardinality_error(false)),
            Some(_) if more => Err(row_cardinality_error(true)),
            Some(row) => Ok(row),
        }
    }

    /// Fetches exactly one row and returns its first column.
    ///
    /// # Errors
    /// Returns `NoRows`, `TooManyRows`, `Missing` when the row has no columns, or a sanitized
    /// provider error found while reading.
    fn fetch_scalar(&mut self, sql: &str, options: QueryOptions) -> Result<Value, DbError> {
        let row = self.fetch_one(sql, options)?;
        row.value_at(0)
    }

    /// Executes a trusted multi-statement script that must not manage transactions.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn execute_trusted_script(&mut self, sql: &str, options: QueryOptions) -> Result<(), DbError>;

    /// Commits and consumes this transaction.
    ///
    /// The provider must establish transaction closure and cleanup or discard the connection
    /// before reuse. If commit was dispatched but success is unknown, return
    /// [`ErrorKind::CommitOutcomeUnknown`]; this does not imply rollback.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn commit(self: Box<Self>) -> Result<(), DbError>;

    /// Rolls back and consumes this transaction.
    ///
    /// The provider must finish rollback or discard the connection before reuse; it must never
    /// leave a possibly active connection reusable.
    ///
    /// # Errors
    /// Returns a sanitized provider error.
    fn rollback(self: Box<Self>) -> Result<(), DbError>;
}

fn row_cardinality_error(more_than_one: bool) -> DbError {
    DbError::new(if more_than_one {
        ErrorKind::TooManyRows
    } else {
        ErrorKind::NoRows
    })
}

fn collect_sets(reader: &mut dyn Reader, limit: RowLimit) -> Result<Vec<Vec<Row>>, DbError> {
    let mut sets = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    loop {
        let mut set = Vec::new();
        while let Some(row) = reader.next_row()? {
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
        if !reader.has_next_set()? {
            break;
        }
        if sets.len() == limit.max_sets() {
            return Err(DbError::new(ErrorKind::Overflow));
        }
    }
    reader.finish()?;
    Ok(sets)
}
