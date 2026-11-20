//! Reference [`SqlPagePlan`] over a caller-authored SQLite keyset statement.
//!
//! This file is compiled by `examples/sql_page_source.rs` and `tests/sql_page_source.rs`, so the
//! documented plan is the tested one. It is a plan for the generic
//! [`SqlPageSource`](neotales_dbapi::paging::multi::sql::SqlPageSource), not a provider-specific
//! source in the core crate.
//!
//! The statements are:
//!
//! ```sql
//! -- Cross-tenant administrative scan. The caller must be authorized for every tenant.
//! SELECT tenant, seq, body FROM events
//! WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//!
//! -- Tenant-scoped scan. The first `?` is an authorization predicate, separate from the cursor.
//! SELECT tenant, seq, body FROM events
//! WHERE tenant = ? AND (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?;
//! ```
//!
//! SQLite uses `?` placeholders. The plan never rewrites SQL; a PostgreSQL caller writes `$1`,
//! `$2`, and `$3` in the same positions. `(tenant, seq)` is a `NOT NULL` primary key, so the keyset
//! order is total. This is a plan for SQLite; no other provider is run here.

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, QueryOptions, Row, Value,
    paging::multi::{Continuation, PageRequest, sql::SqlPagePlan},
};

static NEXT_PLAN_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) const ADMIN_SQL: &str = "SELECT tenant, seq, body FROM events \
                                    WHERE (tenant, seq) > (?, ?) ORDER BY tenant, seq LIMIT ?";

pub(crate) const TENANT_SQL: &str = "SELECT tenant, seq, body FROM events \
                                     WHERE tenant = ? AND (tenant, seq) > (?, ?) \
                                     ORDER BY tenant, seq LIMIT ?";

/// The authorization scope of one plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scope {
    /// A cross-tenant administrative scan. The caller must be authorized for every tenant.
    AllTenants,
    /// One authorized tenant, bound as a separate SQL predicate and parameter.
    Tenant(i64),
}

/// A continuation scoped to one plan instance and its authorized tenant.
#[derive(Debug)]
pub(crate) struct KeysetToken {
    plan: u64,
    after: (i64, i64),
}

/// A one-row-lookahead keyset plan over a fixed caller-authored statement.
pub(crate) struct KeysetPlan {
    scope: Scope,
    start: (i64, i64),
    plan_id: u64,
    requests: Arc<AtomicUsize>,
}

impl std::fmt::Debug for KeysetPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeysetPlan")
            .field("scope", &self.scope)
            .field("start", &self.start)
            .field("requests", &self.requests.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl KeysetPlan {
    /// Creates a plan over `scope` and the exclusive key before its first row.
    ///
    /// `start` must be inside [`Scope::Tenant`] when that scope is used.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when `start` is outside the authorized tenant.
    pub(crate) fn new(scope: Scope, start: (i64, i64)) -> Result<Self, DbError> {
        if let Scope::Tenant(tenant) = scope
            && start.0 != tenant
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(Self {
            scope,
            start,
            plan_id: NEXT_PLAN_ID.fetch_add(1, Ordering::Relaxed),
            requests: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns a diagnostic request counter shared with the caller.
    pub(crate) fn requests(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.requests)
    }

    /// Resolves a continuation to the exclusive key lower bound.
    ///
    /// A token from another plan instance, a wrongly typed token, and a token whose cursor is
    /// outside the authorized tenant are all `InvalidArgument`.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for a foreign, wrongly typed, or out-of-scope token.
    pub(crate) fn resolve(
        &self,
        continuation: Option<Continuation>,
    ) -> Result<(i64, i64), DbError> {
        let Some(token) = continuation else {
            return Ok(self.start);
        };
        let token = token
            .downcast::<KeysetToken>()
            .map_err(|_| DbError::new(ErrorKind::InvalidArgument))?;
        if token.plan != self.plan_id {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        if let Scope::Tenant(tenant) = self.scope
            && token.after.0 != tenant
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(token.after)
    }

    fn issue(&self, after: (i64, i64)) -> Continuation {
        Continuation::new(KeysetToken {
            plan: self.plan_id,
            after,
        })
    }
}

impl SqlPagePlan for KeysetPlan {
    fn sql(&self) -> &str {
        match self.scope {
            Scope::AllTenants => ADMIN_SQL,
            Scope::Tenant(_) => TENANT_SQL,
        }
    }

    fn options(
        &mut self,
        _: PageRequest,
        lookahead: usize,
        continuation: Option<Continuation>,
    ) -> Result<QueryOptions, DbError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        let after = self.resolve(continuation)?;
        let mut parameters = Vec::with_capacity(4);
        if let Scope::Tenant(tenant) = self.scope {
            parameters.push(i64_param(tenant)?);
        }
        parameters.push(i64_param(after.0)?);
        parameters.push(i64_param(after.1)?);
        parameters.push(i64_param(
            i64::try_from(lookahead).map_err(|_| DbError::new(ErrorKind::Overflow))?,
        )?);
        Ok(QueryOptions::new().bind_all(&parameters))
    }

    fn next_continuation(
        &mut self,
        rows: &[Row],
        has_lookahead: bool,
    ) -> Result<Option<Continuation>, DbError> {
        if !has_lookahead {
            return Ok(None);
        }
        let last = rows.last().ok_or_else(|| DbError::new(ErrorKind::Decode))?;
        Ok(Some(self.issue((
            last.read::<i64>("tenant")?,
            last.read::<i64>("seq")?,
        ))))
    }
}

fn i64_param(value: i64) -> Result<Parameter, DbError> {
    Parameter::new(DbType::I64, Value::I64(value))
}
