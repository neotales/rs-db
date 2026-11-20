//! Conservative SQLite SQL screening shared by the session and transaction paths.
//!
//! This is lexical screening, not a SQL parser. It recognizes SQLite's quote and comment forms so
//! that a statement boundary, a placeholder, or a leading command word is not confused by text
//! inside a literal or comment. Ambiguous input is rejected before dispatch.

use neotales_dbapi::{DbError, ErrorKind, Parameter};

/// Rejects SQL containing an interior NUL, which SQLite cannot represent as a C string.
pub(crate) fn check_sql(sql: &str) -> Result<(), DbError> {
    if sql.as_bytes().contains(&0) {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

/// Rejects additional executable statements and malformed lexical constructs.
///
/// One statement is permitted, with optional trailing semicolons and comments. This scans only
/// SQLite quote/comment boundaries; it is not a general SQL parser. In particular, an ordinary
/// `CREATE TRIGGER` whose body contains a semicolon is rejected; use a trusted script for that.
pub(crate) fn check_one_statement(sql: &str) -> Result<(), DbError> {
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
    let mut index = 0;
    let mut has_code = false;
    let mut terminated = false;
    while index < bytes.len() {
        let byte = bytes[index];
        match state {
            State::Normal => match byte {
                b' ' | b'\t' | b'\r' | b'\n' | b'\x0c' => {}
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    state = State::LineComment;
                    index += 1;
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    state = State::BlockComment;
                    index += 1;
                }
                b';' => {
                    if !terminated {
                        if !has_code {
                            return Err(DbError::new(ErrorKind::InvalidArgument));
                        }
                        terminated = true;
                    }
                }
                b'\'' | b'"' | b'`' | b'[' => {
                    if terminated {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    has_code = true;
                    state = match byte {
                        b'\'' => State::Single,
                        b'"' => State::Double,
                        b'`' => State::Backtick,
                        _ => State::Bracket,
                    };
                }
                _ => {
                    if terminated {
                        return Err(DbError::new(ErrorKind::InvalidArgument));
                    }
                    has_code = true;
                }
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
            State::Backtick if byte == b'`' => {
                if bytes.get(index + 1) == Some(&b'`') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
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
    if !matches!(state, State::Normal | State::LineComment) || !has_code {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    Ok(())
}

/// Returns the first unquoted ASCII command token after a narrow set of whitespace/comments.
///
/// This is intentionally SQLite-specific lexical screening, not a general SQL parser. A leading
/// form it does not recognize fails closed with `InvalidArgument`.
fn leading_command(sql: &str) -> Result<&[u8], DbError> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    loop {
        while bytes
            .get(index)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b'\x0c'))
        {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"--") {
            index += 2;
            while bytes.get(index).is_some_and(|byte| *byte != b'\n') {
                index += 1;
            }
            continue;
        }
        if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && bytes.get(index..index + 2) != Some(b"*/") {
                index += 1;
            }
            if index + 1 >= bytes.len() {
                return Err(DbError::new(ErrorKind::InvalidArgument));
            }
            index += 2;
            continue;
        }
        break;
    }

    if !bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        return Err(DbError::new(ErrorKind::InvalidArgument));
    }
    let start = index;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    Ok(&bytes[start..index])
}

/// Rejects transaction-control commands on statement methods; callers must use transaction APIs.
pub(crate) fn check_not_transaction_control(sql: &str) -> Result<(), DbError> {
    transaction_control_word(leading_command(sql)?)
}

/// Rejects `ATTACH`/`DETACH` as a single statement on durable sessions.
///
/// Attached databases are outside the per-pool durability policy. A form the lexical lead-token
/// scan does not recognize is left to SQLite; this check adds only the `ATTACH`/`DETACH`
/// rejection and is not a general SQL parser.
pub(crate) fn check_durable_command(sql: &str) -> Result<(), DbError> {
    let Ok(token) = leading_command(sql) else {
        return Ok(());
    };
    if token.eq_ignore_ascii_case(b"ATTACH") || token.eq_ignore_ascii_case(b"DETACH") {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

/// Rejects durable trusted scripts that contain a policy-changing or cross-database keyword.
///
/// A trusted script is the only public statement call that can execute several statements before
/// returning. SQLite requires the literal ASCII keyword for a `PRAGMA`, `ATTACH`, `DETACH`, or
/// `load_extension` statement, so excluding those words anywhere in the script prevents such a
/// statement from running. This is a conservative byte scan, not a SQL parser: a script is also
/// rejected when a keyword appears in a comment or string literal.
pub(crate) fn check_durable_script(sql: &str) -> Result<(), DbError> {
    let bytes = sql.as_bytes();
    if DURABLE_SCRIPT_KEYWORDS
        .iter()
        .any(|keyword| contains_ascii_case_insensitive(bytes, keyword))
    {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

const DURABLE_SCRIPT_KEYWORDS: [&[u8]; 4] = [b"pragma", b"attach", b"detach", b"load_extension"];

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

fn transaction_control_word(token: &[u8]) -> Result<(), DbError> {
    if token.eq_ignore_ascii_case(b"BEGIN")
        || token.eq_ignore_ascii_case(b"COMMIT")
        || token.eq_ignore_ascii_case(b"END")
        || token.eq_ignore_ascii_case(b"ROLLBACK")
        || token.eq_ignore_ascii_case(b"SAVEPOINT")
        || token.eq_ignore_ascii_case(b"RELEASE")
    {
        Err(DbError::new(ErrorKind::InvalidArgument))
    } else {
        Ok(())
    }
}

/// Counts SQLite positional placeholders and rejects named forms and mixed anon/indexed forms.
///
/// Ordinary statements accept anonymous `?` placeholders or indexed `?NNN` placeholders, but not
/// both in one statement. The supplied parameters must match the highest placeholder index. SQL is
/// never rewritten.
pub(crate) fn check_arity(sql: &str, parameters: &[Parameter]) -> Result<(), DbError> {
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
    let mut index = 0;
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
    if maximum == parameters.len() {
        Ok(())
    } else {
        Err(DbError::new(ErrorKind::InvalidArgument))
    }
}

#[cfg(test)]
mod tests {
    use super::{check_arity, check_one_statement};
    use neotales_dbapi::{DbType, ErrorKind, Parameter, Value};

    fn parameter() -> Parameter {
        Parameter::new(DbType::I64, Value::I64(1)).expect("valid parameter")
    }

    #[test]
    fn arity_accepts_anonymous_and_indexed_placeholders() {
        assert!(check_arity("SELECT ?", &[parameter()]).is_ok());
        assert!(check_arity("SELECT ?1", &[parameter()]).is_ok());
        assert!(check_arity("SELECT ?1, ?1", &[parameter()]).is_ok());
        assert!(check_arity("SELECT ?2", &[parameter(), parameter()]).is_ok());
        assert!(check_arity("SELECT '?'", &[]).is_ok());
        assert!(check_arity("SELECT 1 -- ?\n", &[]).is_ok());
    }

    #[test]
    fn arity_rejects_mismatch_mixed_and_named_forms() {
        for (sql, count) in [
            ("SELECT ?", 0usize),
            ("SELECT ?", 2),
            ("SELECT ?1, ?2", 1),
            ("SELECT ?1, ?", 1),
            ("SELECT ?0", 0),
            ("SELECT :name", 0),
            ("SELECT @name", 0),
            ("SELECT $name", 0),
        ] {
            let parameters = vec![parameter(); count];
            let error = check_arity(sql, &parameters).expect_err("must reject");
            assert!(
                matches!(
                    error.kind(),
                    ErrorKind::InvalidArgument | ErrorKind::Unsupported
                ),
                "unexpected kind for {sql}: {:?}",
                error.kind()
            );
        }
    }

    #[test]
    fn one_statement_rejects_trailing_code_and_unterminated_quotes() {
        assert!(check_one_statement("SELECT 1").is_ok());
        assert!(check_one_statement("SELECT 1;").is_ok());
        assert!(check_one_statement("SELECT 1; -- done").is_ok());
        assert!(check_one_statement("SELECT 1; SELECT 2").is_err());
        assert!(check_one_statement("SELECT 'unterminated").is_err());
        assert!(check_one_statement("   ").is_err());
        assert_eq!(
            check_one_statement("SELECT 1; SELECT 2")
                .expect_err("must reject")
                .kind(),
            ErrorKind::InvalidArgument
        );
    }
}
