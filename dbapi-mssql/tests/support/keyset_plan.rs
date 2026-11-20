//! Shared test-only keyset `SqlPagePlan` for provider live lanes.
//!
//! The caller supplies the complete provider-specific SQL; this module only builds positional
//! parameters, an instance-scoped continuation, and the tenant check. It is included by each
//! provider's ignored live test so the provider syntax differs while the plan contract does not.
//!
//! The expected statement shape is:
//!
//! ```sql
//! -- All tenants
//! SELECT tenant, seq, body FROM <table>
//! WHERE (tenant, seq) > (<cursor>, <cursor>) ORDER BY tenant, seq LIMIT <lookahead>;
//!
//! -- One authorized tenant
//! SELECT tenant, seq, body FROM <table>
//! WHERE tenant = <tenant> AND (tenant, seq) > (<cursor>, <cursor>)
//! ORDER BY tenant, seq LIMIT <lookahead>;
//! ```
//!
//! The plan binds `[after.tenant, after.seq, lookahead]` for `Scope::AllTenants` and
//! `[authorized_tenant, after.tenant, after.seq, lookahead]` for `Scope::Tenant`. Placeholder syntax
//! is the provider's own.

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use neotales_dbapi::{
    DbError, DbType, ErrorKind, Parameter, QueryOptions, Row, Value,
    paging::multi::{Continuation, PageRequest, sql::SqlPagePlan},
};

static NEXT_PLAN_ID: AtomicU64 = AtomicU64::new(1);

/// The authorization scope of one test plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scope {
    /// Cross-tenant scan; the caller must be authorized for every tenant.
    AllTenants,
    /// One authorized tenant, bound separately from the cursor.
    Tenant(i64),
}

#[derive(Debug)]
struct KeysetToken {
    plan: u64,
    after: (i64, i64),
}

/// A one-row-lookahead keyset plan over caller-authored SQL.
pub(crate) struct KeysetPlan {
    sql: &'static str,
    scope: Scope,
    start: (i64, i64),
    plan_id: u64,
    requests: Arc<AtomicUsize>,
}

impl KeysetPlan {
    /// Creates a plan over `sql`, `scope`, and the exclusive key before the first row.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when `start` is outside the authorized tenant.
    pub(crate) fn new(sql: &'static str, scope: Scope, start: (i64, i64)) -> Result<Self, DbError> {
        if let Scope::Tenant(tenant) = scope
            && start.0 != tenant
        {
            return Err(DbError::new(ErrorKind::InvalidArgument));
        }
        Ok(Self {
            sql,
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

    fn resolve(&self, continuation: Option<Continuation>) -> Result<(i64, i64), DbError> {
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
        self.sql
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
