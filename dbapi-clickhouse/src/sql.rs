//! Conservative ClickHouse SQL scanning for statement boundaries, transaction control, and
//! named placeholders.
//!
//! This is not a SQL parser. It understands ClickHouse single/double/backtick quoting with
//! backslash escapes and doubled quote characters, `--` and `//` line comments ending at LF,
//! and nested block comments. A bare CR does not end either line comment. `#` is rejected in
//! code rather than relying on its comment semantics. Non-ASCII bytes are
//! allowed inside recognized strings, quoted identifiers, and comments; outside them they are
//! rejected because ClickHouse also accepts Unicode curly quotes as string delimiters and this
//! scanner does not model that syntax.
//!
//! A `{name:Type}` outside strings and comments is recorded as a placeholder; whitespace is
//! allowed around the type. Other unquoted opening braces are rejected: ClickHouse accepts
//! additional whitespace and comments in placeholders,
//! which this scanner deliberately does not parse, and they must not bypass parameter validation.
//! Unquoted dollar signs and `#` are rejected because the scanner does not model them; dollar
//! signs inside ordinary quoted strings, identifiers, and comments are allowed.

use neotales_dbapi::{DbError, ErrorKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Normal,
    Single,
    Double,
    Backtick,
    LineComment,
    BlockComment,
}

/// One named native parameter placeholder found in SQL code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Placeholder {
    /// Placeholder name without braces or declared type.
    pub(crate) name: String,
    /// Declared ClickHouse type text between the colon and closing brace.
    pub(crate) declared: String,
}

/// The parts of a statement the provider validates before dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Scan {
    /// Byte offset of the first code byte of each non-empty statement.
    pub(crate) starts: Vec<usize>,
    /// Placeholders found outside strings and comments, in source order.
    pub(crate) placeholders: Vec<Placeholder>,
    /// An unquoted SQL SETTINGS token, which can override native query settings.
    has_settings: bool,
}

fn invalid() -> DbError {
    DbError::new(ErrorKind::InvalidArgument)
}

fn unsupported() -> DbError {
    DbError::new(ErrorKind::Unsupported)
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

/// Scans SQL for statement boundaries and native named placeholders.
///
/// # Errors
/// Returns `InvalidArgument` for NUL bytes, no statement, an unterminated quote or block
/// comment, or a placeholder-shaped brace that cannot be parsed. Returns `Unsupported` for
/// unquoted dollar signs, hash signs, or non-ASCII code.
#[allow(clippy::too_many_lines)] // Lexer states are clearer as one ordered match.
pub(crate) fn scan(sql: &str) -> Result<Scan, DbError> {
    if sql.as_bytes().contains(&0) {
        return Err(invalid());
    }
    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut starts = Vec::new();
    let mut placeholders = Vec::new();
    let mut has_settings = false;
    let mut start: Option<usize> = None;
    let mut block_depth = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            State::Normal => {
                if matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | 0x0b | 0x0c) {
                    index += 1;
                    continue;
                }
                match byte {
                    b'#' | 0x80..=0xff => return Err(unsupported()),
                    b'-' if bytes.get(index + 1) == Some(&b'-') => {
                        state = State::LineComment;
                        index += 2;
                    }
                    b'/' if bytes.get(index + 1) == Some(&b'/') => {
                        state = State::LineComment;
                        index += 2;
                    }
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        state = State::BlockComment;
                        block_depth = 1;
                        index += 2;
                    }
                    b';' => {
                        if let Some(statement) = start.take() {
                            starts.push(statement);
                        }
                        index += 1;
                    }
                    b'\'' => {
                        start.get_or_insert(index);
                        state = State::Single;
                        index += 1;
                    }
                    b'"' => {
                        start.get_or_insert(index);
                        state = State::Double;
                        index += 1;
                    }
                    b'`' => {
                        start.get_or_insert(index);
                        state = State::Backtick;
                        index += 1;
                    }
                    b'{' => {
                        start.get_or_insert(index);
                        if let Some((placeholder, next)) = parse_placeholder(bytes, index)? {
                            placeholders.push(placeholder);
                            index = next;
                        } else {
                            return Err(unsupported());
                        }
                    }
                    _ => {
                        start.get_or_insert(index);
                        let token_start = index;
                        while bytes.get(index).is_some_and(|byte| is_word(*byte)) {
                            index += 1;
                        }
                        if bytes[token_start..index].contains(&b'$') {
                            return Err(unsupported());
                        }
                        if bytes[token_start..index].eq_ignore_ascii_case(b"SETTINGS") {
                            has_settings = true;
                        }
                        if index == token_start {
                            index += 1;
                        }
                    }
                }
            }
            State::Single => {
                if byte == b'\\' {
                    index = index.saturating_add(2);
                } else if byte == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        state = State::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::Double => {
                if byte == b'\\' {
                    index = index.saturating_add(2);
                } else if byte == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        index += 2;
                    } else {
                        state = State::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::Backtick => {
                if byte == b'\\' {
                    index = index.saturating_add(2);
                } else if byte == b'`' {
                    if bytes.get(index + 1) == Some(&b'`') {
                        index += 2;
                    } else {
                        state = State::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            State::LineComment => {
                if byte == b'\n' {
                    state = State::Normal;
                }
                index += 1;
            }
            State::BlockComment => {
                if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
                    block_depth = block_depth.checked_add(1).ok_or_else(invalid)?;
                    index += 2;
                } else if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                    block_depth = block_depth.checked_sub(1).ok_or_else(invalid)?;
                    if block_depth == 0 {
                        state = State::Normal;
                    }
                    index += 2;
                } else {
                    index += 1;
                }
            }
        }
    }
    if !matches!(state, State::Normal | State::LineComment) {
        return Err(invalid());
    }
    if let Some(statement) = start {
        starts.push(statement);
    }
    if starts.is_empty() {
        return Err(invalid());
    }
    Ok(Scan {
        starts,
        placeholders,
        has_settings,
    })
}

fn parse_placeholder(bytes: &[u8], start: usize) -> Result<Option<(Placeholder, usize)>, DbError> {
    let mut index = start + 1;
    let name_start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    if index == name_start || bytes.get(index) != Some(&b':') {
        return Ok(None);
    }
    let name = std::str::from_utf8(&bytes[name_start..index])
        .map_err(|_| invalid())?
        .to_owned();
    index += 1;
    let type_start = index;
    let mut depth = 0usize;
    while let Some(byte) = bytes.get(index) {
        match byte {
            0x80..=0xff => return Err(unsupported()),
            b'(' => {
                depth = depth.checked_add(1).ok_or_else(invalid)?;
                index += 1;
            }
            b')' => {
                depth = depth.checked_sub(1).ok_or_else(invalid)?;
                index += 1;
            }
            b'}' if depth == 0 => {
                if index == type_start {
                    return Err(invalid());
                }
                let declared = std::str::from_utf8(&bytes[type_start..index])
                    .map_err(|_| invalid())?
                    .trim()
                    .to_owned();
                return Ok(Some((Placeholder { name, declared }, index + 1)));
            }
            b'}' | b'{' => return Err(invalid()),
            b if b.is_ascii_alphanumeric()
                || matches!(b, b'_' | b',' | b' ' | b'\t' | b'\r' | b'\n') =>
            {
                index += 1;
            }
            _ => return Err(unsupported()),
        }
    }
    Err(invalid())
}

/// Returns the leading SQL word at `start`, if any.
pub(crate) fn leading_token(sql: &str, start: usize) -> Option<&[u8]> {
    let bytes = sql.as_bytes();
    if !bytes.get(start)?.is_ascii_alphabetic() {
        return None;
    }
    let mut end = start;
    while bytes
        .get(end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
    {
        end += 1;
    }
    Some(&bytes[start..end])
}

/// Rejects a fetch statement that is not exactly one statement, or that attempts transaction
/// control before anything is dispatched.
///
/// # Errors
/// Returns `InvalidArgument` for zero or multiple statements and `Unsupported` for
/// transaction-control statements.
pub(crate) fn check_fetch(sql: &str, scan: &Scan) -> Result<(), DbError> {
    if scan.starts.len() != 1 {
        return Err(invalid());
    }
    let start = scan.starts[0];
    let token = leading_token(sql, start);
    if token.is_some_and(|token| {
        matches!(
            token.to_ascii_uppercase().as_slice(),
            b"BEGIN" | b"COMMIT" | b"ROLLBACK" | b"ABORT" | b"START"
        )
    }) {
        return Err(unsupported());
    }
    if token.is_some_and(|token| token.eq_ignore_ascii_case(b"SET"))
        && sql[start..].to_ascii_uppercase().contains("TRANSACTION")
    {
        return Err(unsupported());
    }
    Ok(())
}

pub(crate) fn check_write(sql: &str, scan: &Scan) -> Result<(), DbError> {
    if scan.starts.len() != 1 {
        return Err(invalid());
    }
    // An INSERT SETTINGS clause takes precedence over client query settings. Reject all
    // unquoted SETTINGS tokens in writes rather than trying to parse ClickHouse SQL.
    if scan.has_settings {
        return Err(unsupported());
    }
    match leading_token(sql, scan.starts[0]) {
        Some(token)
            if [b"CREATE".as_slice(), b"DROP", b"INSERT"]
                .iter()
                .any(|allowed| token.eq_ignore_ascii_case(allowed)) =>
        {
            Ok(())
        }
        _ => Err(unsupported()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Scan, check_fetch, check_write, scan};
    use neotales_dbapi::ErrorKind;

    #[test]
    fn lexical_bypasses_fail_closed() {
        for sql in [
            "INSERT INTO t SELECT 1 // '\nSETTINGS async_insert=1 -- '",
            "INSERT INTO t SELECT ‘'’ SETTINGS async_insert=1 -- '",
            "INSERT INTO t SELECT {p:Strıng}",
            "INSERT INTO t SELECT 1 # comment",
        ] {
            assert!(
                scan(sql).and_then(|s| check_write(sql, &s)).is_err(),
                "{sql}"
            );
        }
        for marker in ["--", "//"] {
            let sql =
                format!("INSERT INTO t SELECT 1 {marker} ignored\r SETTINGS async_insert=1\n");
            assert!(check_write(&sql, &scan(&sql).unwrap()).is_ok());
            let sql =
                format!("INSERT INTO t SELECT 1 {marker} ignored\r\n SETTINGS async_insert=1");
            assert_eq!(
                check_write(&sql, &scan(&sql).unwrap()).unwrap_err().kind(),
                ErrorKind::Unsupported
            );
        }
        for sql in [
            "SELECT 'é'",
            "SELECT `é`",
            "SELECT 1 /* é */",
            "SELECT 1 // é\n",
        ] {
            assert!(scan(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn writes_require_one_supported_statement() {
        for sql in [
            "CREATE TABLE t (x Int64) ENGINE=Memory",
            "DROP TABLE t",
            "INSERT INTO t SELECT {p1:Int64}",
        ] {
            assert!(check_write(sql, &scan(sql).unwrap()).is_ok());
        }
        for sql in ["SELECT 1", "ALTER TABLE t DELETE WHERE 1", "BEGIN"] {
            assert_eq!(
                check_write(sql, &scan(sql).unwrap()).unwrap_err().kind(),
                ErrorKind::Unsupported
            );
        }
        let sql = "DROP TABLE t; DROP TABLE u";
        assert_eq!(
            check_write(sql, &scan(sql).unwrap()).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn backslash_escaped_backticks_do_not_end_quoted_identifiers() {
        let sql = r"SELECT `name\`;still_identifier`";
        let scanned = scan(sql).unwrap();
        assert_eq!(scanned.starts.len(), 1);
        assert!(check_fetch(sql, &scanned).is_ok());
        assert!(scan(r"SELECT `unterminated\`").is_err());
    }

    #[test]
    fn writes_reject_sql_settings_overrides_but_not_quoted_text() {
        let sql = "INSERT INTO t SETTINGS async_insert=1, wait_for_async_insert=0 SELECT 1";
        assert_eq!(
            check_write(sql, &scan(sql).unwrap()).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        let sql = "INSERT INTO t SELECT 'SETTINGS', `SETTINGS` /* SETTINGS */";
        assert!(check_write(sql, &scan(sql).unwrap()).is_ok());
    }

    #[test]
    fn settings_token_classification_is_exact_and_fail_closed() {
        // Bounded generated corpus: every bare SETTINGS spelling in code is rejected, wherever a
        // clause can begin, including after a line or block comment and on a new line.
        for keyword in ["SETTINGS", "settings", "SeTtInGs"] {
            for prefix in ["", "/* note */ ", "-- note\n", "// note\n"] {
                for suffix in [
                    " async_insert=1",
                    "/* gap */ async_insert=1",
                    "\nasync_insert=1",
                ] {
                    let sql = format!("INSERT INTO t SELECT 1 {prefix}{keyword}{suffix}");
                    let scanned = scan(&sql).unwrap();
                    assert!(scanned.has_settings, "{sql:?}");
                    assert_eq!(
                        check_write(&sql, &scanned).unwrap_err().kind(),
                        ErrorKind::Unsupported,
                        "{sql:?}"
                    );
                }
            }
        }
        // Only an unquoted token is a clause. Quoted text, comments, and a keyword-ish identifier
        // are accepted and dispatch unchanged.
        let sql = "INSERT INTO t SELECT 1 SETTINGS /* gap */ max_block_size=1";
        assert!(scan(sql).unwrap().has_settings);
        for sql in [
            "INSERT INTO t SELECT 'SETTINGS async_insert=1'",
            "INSERT INTO t SELECT 1 /* SETTINGS async_insert=1 */",
            "INSERT INTO t SELECT 1 /* outer /* SETTINGS async_insert=1 */ outer */",
            "INSERT INTO t SELECT 1 -- SETTINGS async_insert=1\n",
            "INSERT INTO t SELECT 1 // SETTINGS async_insert=1\n",
            "INSERT INTO t SELECT 1 AS `SETTINGS`",
            "INSERT INTO t SELECT 1 AS settings_extra",
            "INSERT INTO t SELECT 1 SET/*x*/TINGS async_insert=1",
        ] {
            let scanned = scan(sql).unwrap();
            assert!(!scanned.has_settings, "{sql:?}");
            assert!(check_write(sql, &scanned).is_ok(), "{sql:?}");
        }
        // A bare SETTINGS used as an unquoted alias is not a clause, but the conservative
        // scanner rejects the token rather than parse ClickHouse grammar.
        let sql = "INSERT INTO t SELECT 1 AS settings";
        assert_eq!(
            check_write(sql, &scan(sql).unwrap()).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        // A quoted semicolon cannot split the statement so that the following clause is missed.
        let sql = "INSERT INTO t SELECT ';' SETTINGS max_block_size=1";
        assert_eq!(scan(sql).unwrap().starts.len(), 1);
        assert_eq!(
            check_write(sql, &scan(sql).unwrap()).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn dollar_quoted_input_is_rejected_before_settings_can_be_hidden() {
        for sql in [
            "INSERT INTO t SELECT $$'$$ SETTINGS async_insert=1, wait_for_async_insert=0 -- '",
            "INSERT INTO t SELECT $tag$' ; -- $tag$ SETTINGS async_insert=1",
            "INSERT INTO t SELECT $_tag9$ ' /* ; */ $_tag9$ SETTINGS async_insert=1",
        ] {
            assert!(scan(sql).is_err(), "dollar quoting must fail closed");
        }
        for sql in [
            "INSERT INTO t SELECT '$tag$; --'",
            "INSERT INTO t SELECT 1 /* $tag$' ; */",
            "INSERT INTO t SELECT 1 -- $$'\n",
        ] {
            let scanned = scan(sql).unwrap();
            assert!(check_write(sql, &scanned).is_ok());
        }
    }

    #[test]
    fn placeholders_are_found_only_in_code() {
        let scan =
            scan("SELECT {p1:Int64} AS a, '{p2:Int64}' AS b -- {p3:Int64}\n/* {p4:Int64} */")
                .unwrap();
        assert_eq!(scan.placeholders.len(), 1);
        assert_eq!(scan.placeholders[0].name, "p1");
        assert_eq!(scan.placeholders[0].declared, "Int64");
        assert_eq!(scan.starts.len(), 1);
    }

    #[test]
    fn quotes_escapes_and_doubled_quotes_are_respected() {
        let scan = scan("SELECT 'it\\'s', \"a\"\"b\", `c``d`, {p1:String} -- tail").unwrap();
        assert_eq!(scan.placeholders.len(), 1);
        assert_eq!(scan.placeholders[0].name, "p1");
    }

    #[test]
    fn statement_boundaries_ignore_quoted_semicolons() {
        let scan = scan("SELECT ';'; SELECT 2").unwrap();
        assert_eq!(scan.starts.len(), 2);
        assert!(check_fetch("SELECT ';'; SELECT 2", &scan).is_err());
    }

    #[test]
    fn empty_and_unterminated_sql_is_rejected() {
        for sql in [
            "",
            "   \n",
            "-- only a comment",
            "SELECT 'unclosed",
            "SELECT /* open",
        ] {
            assert!(scan(sql).is_err(), "{sql:?}");
        }
    }

    #[test]
    fn nested_block_comments_are_depth_tracked() {
        let sql = "SELECT {p1:Int64} /* outer /* inner ; {p2:Int64} */ still outer */";
        let scan = scan(sql).unwrap();
        assert_eq!(scan.starts.len(), 1);
        assert_eq!(scan.placeholders.len(), 1);
        assert_eq!(scan.placeholders[0].name, "p1");
        check_fetch(sql, &scan).unwrap();
    }

    #[test]
    fn nested_comment_semicolons_do_not_split_statements() {
        let sql = "SELECT 1 /* outer ; /* inner ; */ ; */";
        let scan = scan(sql).unwrap();
        assert_eq!(scan.starts.len(), 1);
        check_fetch(sql, &scan).unwrap();
    }

    #[test]
    fn malformed_nested_comments_are_rejected() {
        for sql in [
            "SELECT 1 /* outer /* inner */",
            "SELECT 1 /* outer /* inner */ still",
            "SELECT 1 /*/",
            "SELECT 1 /*",
        ] {
            assert!(scan(sql).is_err(), "{sql:?}");
        }
        assert_eq!(
            scan("SELECT 1 /* outer /* inner */ */")
                .unwrap()
                .starts
                .len(),
            1
        );
    }

    #[test]
    fn unrecognized_braces_fail_closed() {
        assert_eq!(
            scan("SELECT {'a': 1} AS value").unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }

    #[test]
    fn placeholder_shaped_braces_must_parse() {
        assert!(scan("SELECT {p1:Int64").is_err());
        assert!(scan("SELECT {p1:}").is_err());
    }

    #[test]
    fn alternate_placeholder_spelling_cannot_escape_validation() {
        for sql in [
            "SELECT {p1:String} FROM { p1:Identifier }",
            "SELECT {p1:String} FROM {/*x*/p1:Identifier}",
            "SELECT {p1:String} FROM {p1 /*x*/ : Identifier}",
        ] {
            assert!(scan(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn transaction_control_is_rejected_before_dispatch() {
        for sql in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "ABORT",
            "START TRANSACTION",
            "SET TRANSACTION READ ONLY",
        ] {
            let scan = scan(sql).unwrap();
            assert_eq!(
                check_fetch(sql, &scan).unwrap_err().kind(),
                ErrorKind::Unsupported,
                "{sql:?}"
            );
        }
    }

    #[test]
    fn a_single_fetch_statement_is_accepted() {
        let sql = "SELECT {p1:Int64} AS value";
        let scan: Scan = scan(sql).unwrap();
        check_fetch(sql, &scan).unwrap();
    }
}
