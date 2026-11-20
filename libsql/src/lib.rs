//! Internal, remote-only libSQL fork. See `PROVENANCE.md` for source and scope.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    all(
        any(
            not(feature = "remote"),
            not(feature = "replication"),
            not(feature = "core")
        ),
        feature = "tls"
    ),
    allow(unused_imports)
)]
#![cfg_attr(
    all(
        any(
            not(feature = "remote"),
            not(feature = "replication"),
            not(feature = "core")
        ),
        feature = "tls"
    ),
    allow(dead_code)
)]

#[cfg(not(all(feature = "remote", feature = "tls")))]
compile_error!("the internal libsql fork requires the remote and tls features");

#[macro_use]
mod macros;

pub mod params;

mod util;

pub mod errors;
pub use errors::Error;

pub use params::params_from_iter;

mod auth;
mod connection;
mod database;
mod load_extension_guard;

cfg_parser! {
    mod parser;
}

mod rows;
mod statement;
mod transaction;
mod value;

#[cfg(feature = "serde")]
pub mod de;

pub use value::{Value, ValueRef, ValueType};

cfg_hrana! {
    mod hrana;
}

pub use self::{
    auth::{AuthAction, AuthContext, Authorization},
    connection::{AuthHook, BatchRows, Connection, Op},
    database::{Builder, Database},
    load_extension_guard::LoadExtensionGuard,
    rows::{Column, Row, Rows},
    statement::Statement,
    transaction::{Transaction, TransactionBehavior},
};

/// Convenient alias for `Result` using the `libsql::Error` type.
pub type Result<T> = std::result::Result<T, errors::Error>;
pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
