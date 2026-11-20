//! Value decoding and placeholder validation that does not depend on the Workers runtime.

// Host builds keep these helpers only for their unit tests; the adapter is wasm-only.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

#[cfg(test)]
use neotales_dbapi::RowLimit;
use neotales_dbapi::{Capabilities, DbError, DbType, ErrorKind, Row, Value};

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

pub(crate) fn d1_capabilities() -> Capabilities {
    Capabilities {
        transactions: false,
        isolation_level: false,
        read_only_transactions: false,
        trusted_scripts: false,
    }
}

pub(crate) fn classify(message: Option<&str>) -> DbError {
    let Some(message) = message else {
        return DbError::new(ErrorKind::Connection);
    };
    if message.contains("UNIQUE constraint failed")
        || message.contains("FOREIGN KEY constraint failed")
        || message.contains("NOT NULL constraint failed")
        || message.contains("CHECK constraint failed")
    {
        return DbError::new(ErrorKind::Constraint);
    }
    if message.contains("database is locked") || message.contains("SQLITE_BUSY") {
        return DbError::new(ErrorKind::Busy);
    }
    if message.contains("interrupted") || message.contains("SQLITE_INTERRUPT") {
        return DbError::new(ErrorKind::Cancelled);
    }
    DbError::new(ErrorKind::Connection)
}

pub(crate) fn validate_result_count(expected: usize, actual: usize) -> Result<(), DbError> {
    if expected == actual {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::WriteOutcomeUnknown))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum D1Changes {
    MissingMetadata,
    MissingValue,
    Value(usize),
}

pub(crate) fn classify_result(
    success: bool,
    message: Option<&str>,
    changes: D1Changes,
) -> Result<usize, DbError> {
    if !success {
        return Err(message.map_or_else(
            || DbError::new(ErrorKind::WriteOutcomeUnknown),
            |message| classify(Some(message)),
        ));
    }
    match changes {
        D1Changes::MissingMetadata | D1Changes::MissingValue => {
            Err(DbError::new(ErrorKind::WriteOutcomeUnknown))
        }
        D1Changes::Value(value) => Ok(value),
    }
}

/// One raw D1 cell after JavaScript-aware decoding, before it becomes a portable [`Value`].
///
/// The variants keep a rejection reason separate from the decoded value so the eventual error is
/// accurate instead of a single "decode failed": a number outside JavaScript's exact integer range,
/// or a `BigInt` outside the `i64` range, is `Unsupported`, while a cell shape D1 cannot produce is
/// `Decode`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum D1Cell {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// A `BigInt` outside the `i64` range, or an integer that cannot be represented exactly.
    OutOfRange,
    /// A JavaScript shape that is not a D1 cell, such as an object or a non-byte sequence.
    Invalid,
}

impl D1Cell {
    /// Converts the cell to a portable value.
    ///
    /// An integral JavaScript number is an `I64` because the binding does not report the SQL storage
    /// class; signed zero is not preserved and arrives as `I64(0)`. A finite non-integral number is an
    /// `F64`. A finite integral number beyond JavaScript's exact range, and an infinite or NaN value,
    /// is rejected as `Unsupported` rather than rounded. A `BigInt` is exact and arrives as `I64`
    /// when it fits; one that does not fit is `Unsupported`. Local Miniflare testing returned SQL NULL
    /// for an infinite `REAL`, so a non-finite value can already be gone before this decoder sees it.
    ///
    /// # Errors
    /// Returns `Unsupported` for an unrepresentable number and `Decode` for an invalid cell shape.
    pub(crate) fn into_value(self) -> Result<Value, DbError> {
        match self {
            Self::Null => Ok(Value::Null(DbType::Text)),
            Self::Bool(value) => Ok(Value::Bool(value)),
            Self::I64(value) => Ok(Value::I64(value)),
            Self::F64(value) => {
                if !value.is_finite() || (value.fract() == 0.0 && value.abs() > MAX_SAFE_INTEGER) {
                    Err(DbError::new(ErrorKind::Unsupported))
                } else {
                    Ok(Value::F64(value))
                }
            }
            Self::Text(value) => Ok(Value::Text(value)),
            Self::Bytes(value) => Ok(Value::Bytes(value)),
            Self::OutOfRange => Err(DbError::new(ErrorKind::Unsupported)),
            Self::Invalid => Err(DbError::new(ErrorKind::Decode)),
        }
    }
}

/// Builds rows from positional raw cells and their column names.
///
/// D1 does not report how many values a row must have, so a row whose width differs from the header
/// is a `Decode` error. No value is dropped or padded.
///
/// # Errors
/// Returns `Decode` for a width mismatch, the cell's conversion error, or a row that cannot be
/// constructed.
pub(crate) fn decode_raw(columns: &[String], rows: Vec<Vec<D1Cell>>) -> Result<Vec<Row>, DbError> {
    let mut decoded = Vec::with_capacity(rows.len());
    for cells in rows {
        if cells.len() != columns.len() {
            return Err(DbError::new(ErrorKind::Decode));
        }
        let mut values = Vec::with_capacity(cells.len());
        for (name, cell) in columns.iter().zip(cells) {
            values.push((name.clone(), cell.into_value()?));
        }
        decoded.push(Row::new(values)?);
    }
    Ok(decoded)
}

/// Reads one prepared statement's raw rows and column names in a single request.
///
/// This uses the pinned binding's `raw({ columnNames: true })` form directly instead of
/// `serde_wasm_bindgen`, because serde reports a JavaScript `BigInt` in `i64` range as `i64` and does
/// not distinguish it from a rounded `Number`. Reading the JavaScript values keeps the distinction:
/// a `BigInt` is exact, a `Number` is exact only inside JavaScript's safe integer range.
///
/// The pinned binding returns the column-name header even when a `SELECT` matches no rows. A result
/// without a header array, or with an empty header, is malformed and is `Decode` rather than an empty
/// page with no columns.
///
/// A dispatch or promise-rejection failure is `WriteOutcomeUnknown`, because the caller's read-only
/// assertion cannot be verified here. A successful result with a shape D1 does not produce is
/// `Decode`.
#[cfg(target_arch = "wasm32")]
pub(crate) async fn raw_with_columns(
    statement: &worker::D1PreparedStatement,
) -> Result<(Vec<String>, Vec<Vec<D1Cell>>), DbError> {
    use worker::{
        js_sys::{Array, futures::JsFuture},
        wasm_bindgen::JsCast,
        worker_sys::types::D1PreparedStatementRawOptions,
    };

    let options = D1PreparedStatementRawOptions::new();
    options.set_column_names(true);
    let promise = statement
        .inner()
        .raw_with_options(&options)
        .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;
    let value = JsFuture::from(promise)
        .await
        .map_err(|_| DbError::new(ErrorKind::WriteOutcomeUnknown))?;

    let Some(result) = value.dyn_ref::<Array>() else {
        return Err(DbError::new(ErrorKind::Decode));
    };
    let mut values = result.iter();
    let Some(header) = values.next() else {
        return Err(DbError::new(ErrorKind::Decode));
    };
    let Some(header) = header.dyn_ref::<Array>() else {
        return Err(DbError::new(ErrorKind::Decode));
    };
    if header.length() == 0 {
        return Err(DbError::new(ErrorKind::Decode));
    }
    let mut columns = Vec::with_capacity(header.length() as usize);
    for name in header.iter() {
        columns.push(
            name.as_string()
                .ok_or_else(|| DbError::new(ErrorKind::Decode))?,
        );
    }

    let mut rows = Vec::new();
    for row in values {
        let Some(row) = row.dyn_ref::<Array>() else {
            return Err(DbError::new(ErrorKind::Decode));
        };
        let mut cells = Vec::with_capacity(row.length() as usize);
        for cell in row.iter() {
            cells.push(decode_js_cell(&cell));
        }
        rows.push(cells);
    }
    Ok((columns, rows))
}

/// Classifies one raw JavaScript cell without losing the distinction between a `Number` and a
/// `BigInt`.
///
/// A JavaScript `null` is a SQL null. `undefined` is not a cell: it is a missing value, and a hole in
/// a JavaScript array reads as `undefined`, so both become `Invalid` (`Decode`) instead of a null.
///
/// A safe-integer `Number` is an exact `I64`; any other finite `Number` is an `F64`, and
/// [`D1Cell::into_value`] rejects an integral one beyond the exact range instead of rounding it. A
/// `BigInt` is exact and is accepted while it fits `i64`; D1 does not return one, but a binding
/// double or an alternate binding can. A byte view or a plain number array is a BLOB, which is how
/// this binding reports it; any other sequence or an object is invalid.
#[cfg(target_arch = "wasm32")]
fn decode_js_cell(value: &worker::wasm_bindgen::JsValue) -> D1Cell {
    use worker::{
        js_sys::{Array, ArrayBuffer, Number, Uint8Array},
        wasm_bindgen::JsCast,
    };

    if value.is_null() {
        return D1Cell::Null;
    }
    if value.is_undefined() {
        return D1Cell::Invalid;
    }
    if let Some(boolean) = value.as_bool() {
        return D1Cell::Bool(boolean);
    }
    if let Some(text) = value.as_string() {
        return D1Cell::Text(text);
    }
    if value.is_bigint() {
        return i64::try_from(value.clone()).map_or(D1Cell::OutOfRange, D1Cell::I64);
    }
    if let Some(number) = value.as_f64() {
        if Number::is_safe_integer(value) {
            return D1Cell::I64(exact_i64(number));
        }
        return D1Cell::F64(number);
    }
    if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        return D1Cell::Bytes(bytes.to_vec());
    }
    if let Some(buffer) = value.dyn_ref::<ArrayBuffer>() {
        return D1Cell::Bytes(Uint8Array::new(buffer).to_vec());
    }
    if let Some(array) = value.dyn_ref::<Array>() {
        // Local Miniflare exposes a BLOB as a plain number array. A non-byte element makes the cell
        // invalid rather than a guess.
        let mut bytes = Vec::with_capacity(array.length() as usize);
        for item in array.iter() {
            if !Number::is_safe_integer(&item) {
                return D1Cell::Invalid;
            }
            let Some(number) = item.as_f64() else {
                return D1Cell::Invalid;
            };
            let Some(byte) = u8::try_from(exact_i64(number)).ok() else {
                return D1Cell::Invalid;
            };
            bytes.push(byte);
        }
        return D1Cell::Bytes(bytes);
    }
    D1Cell::Invalid
}

/// Converts a value already checked by `Number::is_safe_integer`.
#[cfg(target_arch = "wasm32")]
#[allow(clippy::cast_possible_truncation)] // Precondition: a safe integer.
fn exact_i64(number: f64) -> i64 {
    number as i64
}

#[cfg(test)]
#[allow(dead_code)] // Retained prototype decoder for its existing codec tests, not a bounded fetch.
pub(crate) fn decode_rows(
    columns: &[String],
    rows: Vec<Vec<serde_json::Value>>,
    limit: RowLimit,
) -> Result<Vec<Row>, DbError> {
    if rows.len() > limit.rows() {
        return Err(DbError::new(ErrorKind::Overflow));
    }
    let mut decoded = Vec::with_capacity(rows.len());
    let mut bytes = 0usize;
    for cells in rows {
        let row = decode_row(columns, cells)?;
        bytes = bytes
            .checked_add(row.encoded_bytes()?)
            .ok_or_else(|| DbError::new(ErrorKind::Overflow))?;
        if bytes > limit.bytes() {
            return Err(DbError::new(ErrorKind::Overflow));
        }
        decoded.push(row);
    }
    Ok(decoded)
}

#[cfg(test)]
fn decode_row(columns: &[String], cells: Vec<serde_json::Value>) -> Result<Row, DbError> {
    if columns.len() != cells.len() {
        return Err(DbError::new(ErrorKind::Decode));
    }
    let mut values = Vec::with_capacity(cells.len());
    for (name, cell) in columns.iter().zip(cells) {
        values.push((name.clone(), decode_cell(cell)?));
    }
    Row::new(values)
}

#[cfg(test)]
pub(crate) fn decode_cell(cell: serde_json::Value) -> Result<Value, DbError> {
    match cell {
        // D1 does not report the SQL type of a null cell. The SQLite provider uses text for an
        // untyped null too, so callers see the same portable fallback.
        serde_json::Value::Null => Ok(Value::Null(DbType::Text)),
        serde_json::Value::Bool(value) => Ok(Value::Bool(value)),
        serde_json::Value::Number(value) => {
            let number = value
                .as_f64()
                .ok_or_else(|| DbError::new(ErrorKind::Decode))?;
            if number.fract() == 0.0 && number.abs() > MAX_SAFE_INTEGER {
                return Err(DbError::new(ErrorKind::Unsupported));
            }
            if number.fract() == 0.0 {
                #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
                let integer = number as i64;
                Ok(Value::I64(integer))
            } else {
                Ok(Value::F64(number))
            }
        }
        serde_json::Value::String(value) => Ok(Value::Text(value)),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(DbError::new(ErrorKind::Decode))
        }
    }
}

// This is the same conservative lexical rule the SQLite provider applies to its positional binder:
// it recognizes SQLite quoting and comments, counts anonymous or indexed placeholders, rejects
// mixing the forms, and rejects named placeholders. It is not a SQL parser.
pub(crate) fn validate_placeholders(sql: &str, parameters: usize) -> Result<(), DbError> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Single,
        Double,
        Backtick,
        Bracket,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut index = 0usize;
    let mut maximum = 0usize;
    let mut indexed = false;
    let mut anonymous = false;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            State::Normal => match byte {
                b'\'' => state = State::Single,
                b'"' => state = State::Double,
                b'`' => state = State::Backtick,
                b'[' => state = State::Bracket,
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    state = State::LineComment;
                    index += 1;
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    state = State::BlockComment;
                    index += 1;
                }
                b'?' => {
                    index += 1;
                    let start = index;
                    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                        index += 1;
                    }
                    let number = if index > start {
                        indexed = true;
                        std::str::from_utf8(&bytes[start..index])
                            .ok()
                            .and_then(|digits| digits.parse::<usize>().ok())
                            .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))?
                    } else {
                        anonymous = true;
                        maximum
                            .checked_add(1)
                            .ok_or_else(|| DbError::new(ErrorKind::InvalidArgument))?
                    };
                    if number == 0 {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    maximum = maximum.max(number);
                    continue;
                }
                b':' | b'@' | b'$' => return Err(DbError::new(ErrorKind::Unsupported)),
                _ => {}
            },
            State::Single if byte == b'\'' => {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Double if byte == b'"' => {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Backtick if byte == b'`' => state = State::Normal,
            State::Bracket if byte == b']' => state = State::Normal,
            State::LineComment if byte == b'\n' => state = State::Normal,
            State::BlockComment if byte == b'*' && bytes.get(index + 1) == Some(&b'/') => {
                state = State::Normal;
                index += 1;
            }
            _ => {}
        }
        index += 1;
    }
    if indexed && anonymous {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    if maximum == parameters {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::InvalidArgument))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        D1Cell, D1Changes, classify, classify_result, decode_cell, decode_raw,
        validate_placeholders, validate_result_count,
    };
    use neotales_dbapi::{DbType, ErrorKind, Value};

    #[test]
    fn placeholder_validation_counts_anonymous_and_indexed_forms() {
        assert!(validate_placeholders("SELECT ?", 1).is_ok());
        assert!(validate_placeholders("SELECT ?2, ?1", 2).is_ok());
        assert!(validate_placeholders("SELECT '?'", 0).is_ok());
        assert!(validate_placeholders("SELECT ?", 0).is_err());
        assert!(validate_placeholders("SELECT ?1, ?", 2).is_err());
        assert!(validate_placeholders("SELECT $1", 1).is_err());
    }

    #[test]
    fn untyped_null_decodes_as_text() {
        let value = decode_cell(serde_json::Value::Null).expect("null");
        assert_eq!(value, Value::Null(DbType::Text));
    }

    #[test]
    fn error_classification_is_sanitized() {
        assert_eq!(
            classify(Some("D1_ERROR: UNIQUE constraint failed: users.id")).kind(),
            ErrorKind::Constraint
        );
        assert_eq!(
            classify(Some("D1_ERROR: database is locked")).kind(),
            ErrorKind::Busy
        );
        assert_eq!(classify(None).kind(), ErrorKind::Connection);
    }

    #[test]
    fn missing_d1_change_metadata_is_unknown_not_zero() {
        assert_eq!(
            classify_result(true, None, D1Changes::MissingMetadata)
                .expect_err("missing metadata")
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        assert_eq!(
            classify_result(true, None, D1Changes::MissingValue)
                .expect_err("missing changes")
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        assert_eq!(classify_result(true, None, D1Changes::Value(0)), Ok(0));
        assert_eq!(classify_result(true, None, D1Changes::Value(3)), Ok(3));
        assert_eq!(
            classify_result(false, None, D1Changes::MissingMetadata)
                .expect_err("missing error metadata")
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }

    #[test]
    fn d1_batch_result_count_must_match_statement_count() {
        assert!(validate_result_count(2, 2).is_ok());
        assert_eq!(
            validate_result_count(2, 1)
                .expect_err("truncated result vector")
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
        assert_eq!(
            validate_result_count(1, 2)
                .expect_err("extra result")
                .kind(),
            ErrorKind::WriteOutcomeUnknown
        );
    }

    #[test]
    fn d1_capabilities_do_not_claim_interactive_or_script_support() {
        let capabilities = super::d1_capabilities();
        assert!(!capabilities.transactions);
        assert!(!capabilities.isolation_level);
        assert!(!capabilities.read_only_transactions);
        assert!(!capabilities.trusted_scripts);
    }

    #[test]
    fn json_numbers_outside_javascript_safe_integer_range_are_rejected() {
        let beyond = serde_json::Number::from_f64(9_007_199_254_740_992.0).expect("finite");
        assert_eq!(
            decode_cell(serde_json::Value::Number(beyond))
                .expect_err("unsafe integer")
                .kind(),
            ErrorKind::Unsupported
        );
        let safe = serde_json::Number::from_f64(9_007_199_254_740_991.0).expect("finite");
        assert_eq!(
            decode_cell(serde_json::Value::Number(safe)).expect("safe integer"),
            Value::I64(9_007_199_254_740_991)
        );
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn actual_js_number_representation_is_checked_before_conversion() {
        use worker::{js_sys::Number, wasm_bindgen::JsValue};

        let safe = JsValue::from_f64(9_007_199_254_740_991.0);
        let unsafe_integer = JsValue::from_f64(9_007_199_254_740_992.0);
        let rounded_literal = JsValue::from_f64(9_007_199_254_740_993.0);

        assert!(Number::is_safe_integer(&safe));
        assert!(!Number::is_safe_integer(&unsafe_integer));
        assert_eq!(
            Number::from(rounded_literal).value_of().to_bits(),
            Number::from(unsafe_integer.clone()).value_of().to_bits()
        );
        let decoded_safe =
            serde_wasm_bindgen::from_value::<serde_json::Value>(safe).expect("safe JS number");
        assert_eq!(
            decode_cell(decoded_safe).expect("safe number"),
            Value::I64(9_007_199_254_740_991)
        );
        let decoded = serde_wasm_bindgen::from_value::<serde_json::Value>(unsafe_integer)
            .expect("actual JS number deserialization");
        assert_eq!(
            decode_cell(decoded).expect_err("unsafe integer").kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn d1_cell_conversion_keeps_number_outcomes_explicit() {
        assert_eq!(D1Cell::I64(1).into_value().expect("integer"), Value::I64(1));
        assert_eq!(
            D1Cell::F64(1.25).into_value().expect("float"),
            Value::F64(1.25)
        );
        assert_eq!(
            D1Cell::F64(-0.0).into_value().expect("negative zero"),
            Value::F64(-0.0)
        );
        for unsafe_cell in [
            D1Cell::F64(9_007_199_254_740_992.0),
            D1Cell::F64(f64::INFINITY),
            D1Cell::F64(f64::NAN),
            D1Cell::OutOfRange,
        ] {
            assert_eq!(
                unsafe_cell
                    .into_value()
                    .expect_err("unrepresentable")
                    .kind(),
                ErrorKind::Unsupported
            );
        }
        assert_eq!(
            D1Cell::Invalid.into_value().expect_err("shape").kind(),
            ErrorKind::Decode
        );
    }

    #[test]
    fn decode_raw_requires_one_value_per_column_and_keeps_bytes() {
        let columns = vec!["id".to_owned(), "payload".to_owned()];
        let rows = decode_raw(
            &columns,
            vec![vec![D1Cell::I64(1), D1Cell::Bytes(vec![1, 2, 255])]],
        )
        .expect("row");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].value_at(1).expect("payload"),
            Value::Bytes(vec![1, 2, 255])
        );
        assert_eq!(
            decode_raw(&columns, vec![vec![D1Cell::I64(1)]])
                .expect_err("width")
                .kind(),
            ErrorKind::Decode
        );
        assert_eq!(
            decode_raw(&columns, vec![vec![D1Cell::I64(1), D1Cell::Invalid]])
                .expect_err("missing cell")
                .kind(),
            ErrorKind::Decode
        );
        assert!(decode_raw(&[], Vec::new()).expect("empty").is_empty());
    }

    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn d1_cell_decodes_actual_javascript_values() {
        use super::decode_js_cell;
        use worker::{
            js_sys::{Array, ArrayBuffer, Number, Object, Uint8Array},
            wasm_bindgen::JsValue,
        };

        let cell = |value: JsValue| decode_js_cell(&value);

        assert_eq!(cell(JsValue::from_f64(1.0)), D1Cell::I64(1));
        assert_eq!(cell(JsValue::from_f64(1.25)), D1Cell::F64(1.25));
        // A signed zero is a safe integer to JavaScript and loses its sign here.
        assert_eq!(cell(JsValue::from_f64(-0.0)), D1Cell::I64(0));
        assert_eq!(cell(JsValue::from_str("text")), D1Cell::Text("text".into()));
        assert_eq!(cell(JsValue::from_bool(true)), D1Cell::Bool(true));
        // A real null is a SQL null; undefined and array holes are missing cells, not nulls.
        assert_eq!(cell(JsValue::NULL), D1Cell::Null);
        assert_eq!(
            cell(JsValue::UNDEFINED)
                .into_value()
                .expect_err("undefined")
                .kind(),
            ErrorKind::Decode
        );
        let sparse = Array::new();
        sparse.set(1, JsValue::from_f64(1.0));
        assert_eq!(cell(sparse.into()), D1Cell::Invalid);
        assert_eq!(
            cell(JsValue::from_f64(9_007_199_254_740_991.0)),
            D1Cell::I64(9_007_199_254_740_991)
        );

        // A Number past the safe boundary has already been rounded by JavaScript; it is rejected,
        // never silently accepted as the rounded integer.
        let rounded = JsValue::from_f64(9_007_199_254_740_993.0);
        assert_eq!(
            Number::from(rounded.clone()).value_of().to_bits(),
            9_007_199_254_740_992.0f64.to_bits()
        );
        assert_eq!(
            cell(rounded)
                .into_value()
                .expect_err("unsafe number")
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            cell(JsValue::from_f64(f64::INFINITY))
                .into_value()
                .expect_err("infinity")
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            cell(JsValue::from_f64(f64::NAN))
                .into_value()
                .expect_err("nan")
                .kind(),
            ErrorKind::Unsupported
        );

        // A BigInt is exact: accepted through the safe-Number boundary and up to the i64 edges.
        assert_eq!(
            cell(JsValue::bigint_from_str("9007199254740992")),
            D1Cell::I64(9_007_199_254_740_992)
        );
        assert_eq!(
            cell(JsValue::bigint_from_str("9007199254740993")),
            D1Cell::I64(9_007_199_254_740_993)
        );
        assert_eq!(
            cell(JsValue::bigint_from_str("9223372036854775807")),
            D1Cell::I64(i64::MAX)
        );
        assert_eq!(
            cell(JsValue::bigint_from_str("-9223372036854775808")),
            D1Cell::I64(i64::MIN)
        );
        assert_eq!(
            cell(JsValue::bigint_from_str("9223372036854775808")),
            D1Cell::OutOfRange
        );
        assert_eq!(
            cell(JsValue::bigint_from_str("9223372036854775808"))
                .into_value()
                .expect_err("bigint overflow")
                .kind(),
            ErrorKind::Unsupported
        );

        // BLOBs: a plain number array (local Miniflare), a Uint8Array, and an ArrayBuffer.
        let array = Array::new();
        for byte in [1u8, 2, 255] {
            array.push(&JsValue::from_f64(f64::from(byte)));
        }
        assert_eq!(cell(array.into()), D1Cell::Bytes(vec![1, 2, 255]));

        let bytes = Uint8Array::from([1u8, 2, 255].as_slice());
        assert_eq!(cell(bytes.into()), D1Cell::Bytes(vec![1, 2, 255]));

        let buffer = ArrayBuffer::new(3);
        Uint8Array::new(&buffer).copy_from(&[1, 2, 255]);
        assert_eq!(cell(JsValue::from(buffer)), D1Cell::Bytes(vec![1, 2, 255]));

        let out_of_range = Array::new();
        out_of_range.push(&JsValue::from_f64(256.0));
        assert_eq!(cell(out_of_range.into()), D1Cell::Invalid);

        let sparse_bytes = Array::new();
        sparse_bytes.set(0, JsValue::from_f64(1.0));
        sparse_bytes.set(2, JsValue::from_f64(2.0));
        assert_eq!(cell(sparse_bytes.into()), D1Cell::Invalid);

        let mixed = Array::new();
        mixed.push(&JsValue::from_str("not a byte"));
        assert_eq!(
            cell(mixed.into()).into_value().expect_err("mixed").kind(),
            ErrorKind::Decode
        );

        let nested = Array::new();
        nested.push(&JsValue::from(Array::new()));
        assert_eq!(cell(nested.into()), D1Cell::Invalid);

        assert_eq!(cell(JsValue::from(Object::new())), D1Cell::Invalid);
    }
}
