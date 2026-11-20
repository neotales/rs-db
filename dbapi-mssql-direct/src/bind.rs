//! Mapping between shared DBAPI values and native SQL Server RPC parameters.

use mssql_tds::{
    datatypes::{sql_string::SqlString, sqltypes::SqlType},
    message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
};
use neotales_dbapi::{DbType, Parameter, Value};

/// Map DBAPI values in slice order to native `@P1`, `@P2`, ... RPC parameters.
///
/// This does not inspect or rewrite SQL. `Parameter::new` already validates
/// declared types and rejects non-finite floats.
#[must_use]
pub fn bind(parameters: &[Parameter]) -> Vec<RpcParameter> {
    parameters
        .iter()
        .enumerate()
        .map(|(index, parameter)| {
            let value = match parameter.value() {
                Value::Null(DbType::Bool) => SqlType::Bit(None),
                Value::Null(DbType::I64) => SqlType::BigInt(None),
                Value::Null(DbType::F64) => SqlType::Float(None),
                Value::Null(DbType::Text) => SqlType::NVarcharMax(None),
                Value::Null(DbType::Bytes) => SqlType::VarBinaryMax(None),
                Value::Bool(value) => SqlType::Bit(Some(*value)),
                Value::I64(value) => SqlType::BigInt(Some(*value)),
                Value::F64(value) => SqlType::Float(Some(*value)),
                Value::Text(value) => {
                    SqlType::NVarcharMax(Some(SqlString::from_utf8_string(value.clone())))
                }
                Value::Bytes(value) => SqlType::VarBinaryMax(Some(value.clone())),
            };
            RpcParameter::new(Some(format!("@P{}", index + 1)), StatusFlags::NONE, value)
        })
        .collect()
}
