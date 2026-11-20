//! Native parameter validation and encoding for the provider's positional convention.
//!
//! Parameter `i` binds `{p{i+1}:Type}`. SQL is never rewritten. The provider validates the
//! declared type and value range before dispatch, then asks the vendored native client to
//! encode the exact ClickHouse `Settings` dump for each value.

use std::collections::BTreeMap;

use klickhouse::NativeParameter;
use neotales_dbapi::{DbError, DbType, ErrorKind, Parameter, Value};

use crate::sql::{Placeholder, Scan};

/// The provider's maximum encoded text/bytes parameter payload.
const MAX_STRING_PARAMETER: usize = 8192;

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

/// Validates placeholders against positional parameters and returns encoded native values.
///
/// # Errors
/// Returns `InvalidArgument` for a count/name/type/range mismatch and `Unsupported` for a
/// value variant or declared type the native path cannot represent exactly.
pub(crate) fn bind(
    scan: &Scan,
    parameters: &[Parameter],
) -> Result<Vec<(String, String)>, DbError> {
    if scan.placeholders.is_empty() {
        return if parameters.is_empty() {
            Ok(Vec::new())
        } else {
            Err(invalid())
        };
    }
    let by_name = unique_placeholders(&scan.placeholders)?;
    if by_name.len() != parameters.len() {
        return Err(invalid());
    }
    let names: Vec<String> = (1..=parameters.len())
        .map(|index| format!("p{index}"))
        .collect();
    for name in &names {
        by_name.get(name.as_str()).ok_or_else(invalid)?;
    }
    let mut values = Vec::with_capacity(parameters.len());
    for (name, parameter) in names.iter().zip(parameters) {
        let declared = by_name.get(name.as_str()).ok_or_else(invalid)?;
        validate_declared(declared, parameter)?;
        values.push((name.as_str(), native_parameter(parameter, declared)?));
    }
    klickhouse::encode_native_parameters(&values).map_err(|_| invalid())
}

fn unique_placeholders(placeholders: &[Placeholder]) -> Result<BTreeMap<&str, &str>, DbError> {
    let mut by_name = BTreeMap::new();
    for placeholder in placeholders {
        if let Some(previous) =
            by_name.insert(placeholder.name.as_str(), placeholder.declared.as_str())
            && previous != placeholder.declared
        {
            return Err(invalid());
        }
    }
    Ok(by_name)
}

fn native_parameter<'a>(
    parameter: &'a Parameter,
    declared: &str,
) -> Result<NativeParameter<'a>, DbError> {
    match parameter.value() {
        Value::Bool(value) => Ok(NativeParameter::Bool(*value)),
        Value::I64(value) => Ok(NativeParameter::Int64(*value)),
        Value::F64(value) if exact_float(*value) => Ok(NativeParameter::Float64(*value)),
        Value::Null(DbType::I64) if declared == "Nullable(UInt64)" => {
            Ok(NativeParameter::NullableUInt64Null)
        }
        Value::Null(DbType::I64) => Ok(NativeParameter::NullableInt64Null),
        Value::Null(DbType::Text | DbType::Bytes) => Ok(NativeParameter::NullableStringNull),
        Value::F64(_) | Value::Null(_) => Err(unsupported()),
        Value::Text(value) => Ok(NativeParameter::Text(value.as_str())),
        Value::Bytes(value) => Ok(NativeParameter::Bytes(value.as_slice())),
    }
}

// Exact equality is the contract: ClickHouse 24.8's text parser is only trusted to round-trip
// these specific encodings. An epsilon comparison would admit values that cannot be exact.
#[allow(clippy::float_cmp)]
fn exact_float(value: f64) -> bool {
    value == 0.0 || value == 1.0 || value == -1.0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Bool,
    Int { bits: u8, signed: bool },
    Float64,
    String,
    NullableInt64,
    NullableUInt64,
    NullableString,
}

#[allow(clippy::match_same_arms)] // Distinct reasons share the same sanitized category.
fn validate_declared(declared: &str, parameter: &Parameter) -> Result<(), DbError> {
    let kind = parse_declared(declared)?;
    match (kind, parameter.value()) {
        (Kind::Bool, Value::Bool(_)) => Ok(()),
        (Kind::Int { bits, signed }, Value::I64(value)) => {
            let (min, max) = int_range(bits, signed)?;
            if (*value >= min) && (*value <= max) {
                Ok(())
            } else {
                Err(invalid())
            }
        }
        (Kind::Float64, Value::F64(value)) if exact_float(*value) => Ok(()),
        (Kind::String, Value::Text(value)) if value.len() <= MAX_STRING_PARAMETER => Ok(()),
        (Kind::String, Value::Bytes(value)) if value.len() <= MAX_STRING_PARAMETER => Ok(()),
        (Kind::NullableInt64, Value::I64(_) | Value::Null(DbType::I64)) => Ok(()),
        (Kind::NullableUInt64, Value::I64(value)) if *value >= 0 => Ok(()),
        (Kind::NullableUInt64, Value::Null(DbType::I64)) => Ok(()),
        (Kind::NullableString, Value::Text(value)) if value.len() <= MAX_STRING_PARAMETER => Ok(()),
        (Kind::NullableString, Value::Bytes(value)) if value.len() <= MAX_STRING_PARAMETER => {
            Ok(())
        }
        (Kind::NullableString, Value::Null(DbType::Text | DbType::Bytes)) => Ok(()),
        (Kind::Float64, Value::F64(_)) => Err(unsupported()),
        (Kind::String, Value::Text(_) | Value::Bytes(_)) => Err(unsupported()),
        (Kind::NullableString, Value::Text(_) | Value::Bytes(_)) => Err(unsupported()),
        (_, Value::Null(_)) => Err(unsupported()),
        _ => Err(invalid()),
    }
}

fn parse_declared(declared: &str) -> Result<Kind, DbError> {
    let kind = match declared.trim() {
        "Bool" => Kind::Bool,
        "Int8" => Kind::Int {
            bits: 8,
            signed: true,
        },
        "Int16" => Kind::Int {
            bits: 16,
            signed: true,
        },
        "Int32" => Kind::Int {
            bits: 32,
            signed: true,
        },
        "Int64" => Kind::Int {
            bits: 64,
            signed: true,
        },
        "UInt8" => Kind::Int {
            bits: 8,
            signed: false,
        },
        "UInt16" => Kind::Int {
            bits: 16,
            signed: false,
        },
        "UInt32" => Kind::Int {
            bits: 32,
            signed: false,
        },
        "UInt64" => Kind::Int {
            bits: 64,
            signed: false,
        },
        "Float64" => Kind::Float64,
        "String" => Kind::String,
        "Nullable(Int64)" => Kind::NullableInt64,
        "Nullable(UInt64)" => Kind::NullableUInt64,
        "Nullable(String)" => Kind::NullableString,
        _ => return Err(unsupported()),
    };
    Ok(kind)
}

fn int_range(bits: u8, signed: bool) -> Result<(i64, i64), DbError> {
    if bits == 0 || bits > 64 {
        return Err(invalid());
    }
    if signed {
        if bits == 64 {
            Ok((i64::MIN, i64::MAX))
        } else {
            let bound = 1i64 << (bits - 1);
            Ok((-bound, bound - 1))
        }
    } else if bits == 64 {
        // DBAPI carries only signed 64-bit integers, so the representable ceiling is i64::MAX.
        Ok((0, i64::MAX))
    } else {
        Ok((0, (1i64 << bits) - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::{bind, validate_declared};
    use crate::sql::scan;
    use neotales_dbapi::{DbType, ErrorKind, Parameter, Value};

    fn parameter(db_type: DbType, value: Value) -> Parameter {
        Parameter::new(db_type, value).unwrap()
    }

    fn count(sql: &str, parameters: &[Parameter]) -> Result<usize, ErrorKind> {
        bind(&scan(sql).unwrap(), parameters)
            .map(|values| values.len())
            .map_err(neotales_dbapi::DbError::kind)
    }

    #[test]
    fn whitespace_surrounding_declared_type_is_normalized() {
        let parameters = [parameter(DbType::I64, Value::I64(4))];
        assert_eq!(
            count("SELECT {p1: Int64 }, {p1:\tInt64\r\n}", &parameters),
            Ok(1)
        );
        assert_eq!(
            count("SELECT {p1:Nullable( Int64 )}", &parameters),
            Err(ErrorKind::Unsupported)
        );
        for sql in ["SELECT { p1:Int64}", "SELECT {p1 :Int64}"] {
            assert_eq!(scan(sql).unwrap_err().kind(), ErrorKind::Unsupported);
        }
    }

    #[test]
    fn positional_names_and_repeats_are_exact() {
        let parameters = [
            parameter(DbType::I64, Value::I64(4)),
            parameter(DbType::Text, Value::Text("x".to_owned())),
        ];
        assert_eq!(
            count("SELECT {p2:String}, {p1:Int64}, {p2:String}", &parameters),
            Ok(2)
        );
        assert_eq!(count("SELECT {p1:Int64}", &parameters[..1]), Ok(1));
        assert_eq!(
            count("SELECT {p1:Int64}", &parameters),
            Err(ErrorKind::InvalidArgument)
        );
        assert_eq!(
            count("SELECT {p2:Int64}", &parameters[..1]),
            Err(ErrorKind::InvalidArgument)
        );
    }

    #[test]
    fn duplicate_names_with_different_types_are_rejected() {
        let parameters = [parameter(DbType::I64, Value::I64(1))];
        assert_eq!(
            count("SELECT {p1:Int64}, {p1:String}", &parameters),
            Err(ErrorKind::InvalidArgument)
        );
    }

    #[test]
    fn integer_ranges_are_checked_before_dispatch() {
        let too_large = parameter(DbType::I64, Value::I64(256));
        assert_eq!(
            count("SELECT {p1:UInt8}", &[too_large]),
            Err(ErrorKind::InvalidArgument)
        );
        let negative = parameter(DbType::I64, Value::I64(-1));
        assert_eq!(
            count("SELECT {p1:UInt8}", &[negative]),
            Err(ErrorKind::InvalidArgument)
        );
        let ok = parameter(DbType::I64, Value::I64(255));
        assert_eq!(count("SELECT {p1:UInt8}", &[ok]), Ok(1));
    }

    #[test]
    fn unsupported_parameter_types_are_rejected() {
        let null = parameter(DbType::Text, Value::Null(DbType::Text));
        assert_eq!(
            count("SELECT {p1:String}", &[null]),
            Err(ErrorKind::Unsupported)
        );
        let float = parameter(DbType::F64, Value::F64(1.5));
        assert_eq!(
            count("SELECT {p1:Float64}", &[float]),
            Err(ErrorKind::Unsupported)
        );
        let large = parameter(DbType::Text, Value::Text("x".repeat(8193)));
        assert_eq!(
            count("SELECT {p1:String}", &[large]),
            Err(ErrorKind::Unsupported)
        );
        assert_eq!(
            count(
                "SELECT {p1:Nullable(UInt32)}",
                &[parameter(DbType::I64, Value::I64(1))]
            ),
            Err(ErrorKind::Unsupported)
        );
    }

    #[test]
    fn nullable_parameters_have_explicit_typed_null_dumps() {
        let null_i = parameter(DbType::I64, Value::Null(DbType::I64));
        let null_s = parameter(DbType::Bytes, Value::Null(DbType::Bytes));
        let encoded = bind(
            &scan("SELECT {p1:Nullable(Int64)}, {p2:Nullable(String)}").unwrap(),
            &[null_i, null_s],
        )
        .unwrap();
        assert_eq!(
            encoded,
            vec![
                ("p1".to_owned(), r"'\\N'".to_owned()),
                ("p2".to_owned(), r"'\\N'".to_owned())
            ]
        );
    }

    #[test]
    fn nullable_string_limits_match_plain_string_before_dispatch() {
        for db_type in [DbType::Text, DbType::Bytes] {
            for len in [8192, 8193] {
                let value = if db_type == DbType::Text {
                    Value::Text("x".repeat(len))
                } else {
                    Value::Bytes(vec![b'x'; len])
                };
                let result = count("SELECT {p1:Nullable(String)}", &[parameter(db_type, value)]);
                assert_eq!(
                    result,
                    if len == 8192 {
                        Ok(1)
                    } else {
                        Err(ErrorKind::Unsupported)
                    }
                );
            }
        }
    }

    #[test]
    fn nullable_uint64_parameters_are_nonnegative_and_typed() {
        for value in [
            Value::Null(DbType::I64),
            Value::I64(0),
            Value::I64(i64::MAX),
        ] {
            assert_eq!(
                count(
                    "SELECT {p1:Nullable(UInt64)}",
                    &[parameter(DbType::I64, value)]
                ),
                Ok(1)
            );
        }
        assert_eq!(
            count(
                "SELECT {p1:Nullable(UInt64)}",
                &[parameter(DbType::I64, Value::I64(-1))]
            ),
            Err(ErrorKind::InvalidArgument)
        );
    }

    #[test]
    fn declared_types_must_match_value_variants() {
        assert_eq!(
            validate_declared(
                "Int64",
                &parameter(DbType::Text, Value::Text("x".to_owned()))
            )
            .map_err(neotales_dbapi::DbError::kind),
            Err(ErrorKind::InvalidArgument)
        );
        assert_eq!(
            validate_declared("Bool", &parameter(DbType::Bool, Value::Bool(true)))
                .map_err(neotales_dbapi::DbError::kind),
            Ok(())
        );
    }

    #[test]
    fn declared_integer_matrix_binds() {
        let sql = "SELECT toInt64({p1:Int8}) AS i8, toInt64({p2:UInt16}) AS u16, \
                   toInt64({p3:Int32}) AS i32, toInt64({p4:UInt64}) AS u64";
        let parameters = [
            parameter(DbType::I64, Value::I64(-128)),
            parameter(DbType::I64, Value::I64(65_535)),
            parameter(DbType::I64, Value::I64(-2_147_483_648)),
            parameter(DbType::I64, Value::I64(i64::MAX)),
        ];
        assert_eq!(count(sql, &parameters), Ok(4));
    }

    #[test]
    fn empty_values_and_exact_limit_are_accepted() {
        let empty_text = parameter(DbType::Text, Value::Text(String::new()));
        assert_eq!(count("SELECT {p1:String}", &[empty_text]), Ok(1));
        let empty_bytes = parameter(DbType::Bytes, Value::Bytes(Vec::new()));
        assert_eq!(count("SELECT {p1:String}", &[empty_bytes]), Ok(1));
        let exact = parameter(DbType::Bytes, Value::Bytes(vec![b'x'; 8192]));
        assert_eq!(count("SELECT {p1:String}", &[exact]), Ok(1));
    }
}
