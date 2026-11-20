//! Reference keyset [`PageSource`] over any DBAPI session.
//!
//! This is a worked example, not a built-in source: `neotales-dbapi` ships no SQL source, and the
//! caller authors the statement and its placeholders. It is compiled by both
//! `examples/keyset_paging.rs` and `tests/keyset_paging.rs`, so the documented source is the tested
//! one.
//!
//! The expected statement is one of these, chosen with [`TenantScope`]:
//!
//! ```sql
//! -- Cross-tenant administrative scan. The caller must be authorized for every tenant.
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//!
//! -- Tenant-scoped scan. The first `?` is an authorization predicate bound from
//! -- `TenantScope::Tenant`, separately from the cursor.
//! SELECT tenant, seq, body FROM events
//! WHERE tenant = ? AND (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//! ```
//!
//! Parameter order is `[after.tenant, after.seq, limit]` for the administrative scan and
//! `[authorized_tenant, after.tenant, after.seq, limit]` for the tenant-scoped scan. SQLite uses `?`
//! placeholders. PostgreSQL uses `$1`, `$2`, `$3` (and `$4`) in the same positions, for example
//! `WHERE tenant = $1 AND (tenant, seq) > ($2, $3) ORDER BY tenant, seq LIMIT $4`; the pager never
//! rewrites SQL, so the caller writes the provider's form. `(tenant, seq)` must be unique and
//! `NOT NULL` (the example table uses a primary key), or the ordering is not total and keyset paging
//! can duplicate or omit rows. This example is tested only against SQLite.
//!
//! # Request shape
//!
//! Each request publishes at most `min(request.page_size(), request.remaining_rows())` rows. It
//! fetches exactly one extra lookahead row, which decides whether a continuation exists and is then
//! dropped; the lookahead row is never published or retained.
//!
//! The caller supplies a separate `source_bytes` allowance. It bounds the portable
//! [`Row::encoded_bytes`] total of the rows this source accepts for one request (the published page
//! plus the lookahead row). It is passed as the provider's `RowLimit` byte bound and checked again
//! after the fetch. It is **not** a provider allocation, buffering, or driver-overhead bound; a
//! provider that does not enforce `RowLimit` may materialize more before the post-check. The pager's
//! `remaining_bytes` budget is a separate check against the **published** rows only, because the
//! lookahead row is not delivered.
//!
//! # Exact budgets
//!
//! The lookahead row makes an exact page boundary unambiguous: if the data ends exactly after a full
//! page, the lookahead fetch is short, no continuation is returned, and an exact budget completes
//! normally without an extra probe. If more data exists after a page that exactly exhausts the
//! pager's row, byte, or page budget, the source returns a continuation and the pager fails closed
//! with `Overflow` before making another request; it never truncates silently.
//!
//! # Continuations, replay, and consistency
//!
//! The token carries the last published composite key and the issuing source instance. A token from
//! another instance is rejected. The pager only passes back tokens this source issued; the check
//! keeps a copied or cached token from being accepted by a different source. The instance identity
//! is process-local and is not a security token: a persisted token must be scoped and validated by
//! the caller (tenant, query, expiry). A tenant-scoped source also rejects a token whose cursor is
//! outside its authorized tenant; the tenant is an authorization predicate, never the cursor.
//!
//! One read-only `SELECT` is executed per request. The source does not retry, replay, prefetch, or
//! promise a snapshot: rows changed between two requests may be observed according to the provider's
//! consistency. Read-only and replay safety are the caller's precondition.
//!
//! The `seed` and `create_table` helpers live in `events_fixture.rs` so the example, the generic
//! plan example, and their tests share the same data. The source itself uses only the
//! provider-neutral `Session`.

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use neotales_dbapi::{
    DbError, DbFuture, DbType, ErrorKind, Parameter, QueryOptions, Row, RowLimit, Session, Value,
    paging::multi::{Continuation, PageRequest, PageSource, SourcePage, read_single_set},
};

static NEXT_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// The authorization scope of one paging instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TenantScope {
    /// A cross-tenant administrative scan. The caller must be authorized for every tenant.
    AllTenants,
    /// One authorized tenant, bound as a separate SQL predicate.
    Tenant(i64),
}

/// A continuation scoped to one source instance and its fixed query.
#[derive(Debug)]
pub(crate) struct KeysetToken {
    pub(crate) source: u64,
    pub(crate) after: (i64, i64),
}

/// A one-row-lookahead keyset source over a fixed caller-authored statement.
pub(crate) struct KeysetSource {
    session: Box<dyn Session>,
    sql: String,
    scope: TenantScope,
    start: (i64, i64),
    source_bytes: usize,
    source_id: u64,
    requests: Arc<AtomicUsize>,
}

impl std::fmt::Debug for KeysetSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeysetSource")
            .field("scope", &self.scope)
            .field("start", &self.start)
            .field("source_bytes", &self.source_bytes)
            .field("requests", &self.requests.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl KeysetSource {
    /// Creates a source over `session`, its statement, and its authorization scope.
    ///
    /// `start` is the exclusive composite key before the first row and must be inside
    /// [`TenantScope::Tenant`] when that scope is used. `source_bytes` is the portable encoded-byte
    /// allowance for the rows accepted by one request, including the lookahead row; it is not a
    /// provider allocation or buffering bound. See the module documentation for the two matching
    /// statements and their parameter order.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when `source_bytes` is zero or `start` is outside the authorized
    /// tenant.
    pub(crate) fn new(
        session: Box<dyn Session>,
        sql: impl Into<String>,
        scope: TenantScope,
        start: (i64, i64),
        source_bytes: usize,
    ) -> Result<Self, DbError> {
        if source_bytes == 0 {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if let TenantScope::Tenant(tenant) = scope
            && start.0 != tenant
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(Self {
            session,
            sql: sql.into(),
            scope,
            start,
            source_bytes,
            source_id: NEXT_SOURCE_ID.fetch_add(1, Ordering::Relaxed),
            requests: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns a diagnostic request counter shared with the caller.
    pub(crate) fn requests(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.requests)
    }

    /// Issues a continuation for `after` from this source instance.
    ///
    /// `next_page_async` uses this for the last published row. It is also used by tests to obtain a
    /// real token from a distinct source.
    pub(crate) fn issue(&self, after: (i64, i64)) -> Continuation {
        Continuation::new(KeysetToken {
            source: self.source_id,
            after,
        })
    }

    /// Resolves a continuation to the exclusive key lower bound.
    ///
    /// The pager passes only tokens this source issued. A token from another instance is
    /// `InvalidArgument`, as is a token whose cursor is outside the authorized tenant.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for a foreign, wrongly typed, or out-of-scope token.
    pub(crate) fn resolve_after(
        &self,
        continuation: Option<Continuation>,
    ) -> Result<(i64, i64), DbError> {
        let Some(token) = continuation else {
            return Ok(self.start);
        };
        let token = token
            .downcast::<KeysetToken>()
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
        if token.source != self.source_id {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if let TenantScope::Tenant(tenant) = self.scope
            && token.after.0 != tenant
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(token.after)
    }
}

impl PageSource for KeysetSource {
    fn next_page_async(
        &mut self,
        request: PageRequest,
        continuation: Option<Continuation>,
    ) -> DbFuture<'_, SourcePage> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            let after = self.resolve_after(continuation)?;
            // Never ask for more rows than the aggregate budget still permits.
            let effective = request.page_size().get().min(request.remaining_rows());
            if effective == 0 {
                return Err(DbError::new(ErrorKind::Overflow));
            }
            let lookahead = effective
                .checked_add(1)
                .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
            // The provider byte bound covers the lookahead row as well as the published page.
            let limit = RowLimit::new(lookahead, self.source_bytes)?;

            let mut parameters = Vec::with_capacity(4);
            if let TenantScope::Tenant(tenant) = self.scope {
                parameters.push(i64_param(tenant)?);
            }
            parameters.push(i64_param(after.0)?);
            parameters.push(i64_param(after.1)?);
            parameters.push(i64_param(usize_i64(lookahead)?)?);

            let options = QueryOptions::new().bind_all(&parameters).limit(limit);
            let reader = self.session.fetch_async(&self.sql, options).await?;
            let mut rows = read_single_set(reader).await?;

            // Portable encoded bytes of the accepted rows, including the lookahead row. This is a
            // post-fetch check, not a provider allocation bound.
            if encoded_bytes(&rows)? > self.source_bytes {
                return Err(DbError::new(ErrorKind::Overflow));
            }

            let has_more = rows.len() > effective;
            if has_more {
                rows.truncate(effective);
            }
            if encoded_bytes(&rows)? > request.remaining_bytes() {
                return Err(DbError::new(ErrorKind::Overflow));
            }

            let next = if has_more {
                let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
                Some(self.issue((last.read::<i64>("tenant")?, last.read::<i64>("seq")?)))
            } else {
                None
            };
            Ok(SourcePage::new(rows, next))
        })
    }
}

fn i64_param(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}

fn usize_i64(value: usize) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_| DbError::new(ErrorKind::Overflow))
}

fn encoded_bytes(rows: &[Row]) -> Result<usize, DbError> {
    rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))
    })
}
