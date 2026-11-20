//! SQL Server client-side batch separation.

/// A script cannot be safely split using the supported `GO` syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchError {
    /// A quoted string, quoted identifier, or block comment was not closed.
    Unterminated,
    /// A `GO` directive had arguments. Repetition and SQLCMD directives are unsupported.
    UnsupportedDirective,
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unterminated => f.write_str(
                "SQL Server batch cannot be split: a quoted string, quoted identifier, \
                 or block comment is not closed",
            ),
            Self::UnsupportedDirective => f.write_str(
                "SQL Server batch cannot be split: a GO directive must stand alone on its line; \
                 repetition counts and SQLCMD directives are unsupported",
            ),
        }
    }
}

impl std::error::Error for BatchError {}

/// Splits trusted T-SQL at standalone, case-insensitive `GO` lines.
///
/// `GO` is a client directive and is never sent to the server. It is recognized
/// only outside strings, quoted identifiers, and nested block comments. A trailing
/// `--` comment is accepted. Lines may end in LF, CR, or CRLF. `GO n` and other
/// arguments are rejected instead of silently changing execution counts. Empty
/// batches are omitted. The entire script is checked before any batches are
/// returned.
///
/// A batch boundary is **not** a transaction boundary; the caller must manage
/// connection, transaction, failure and ledger semantics separately. This is not
/// a SQL validator or SQLCMD interpreter.
///
/// # Examples
///
/// Batches are returned in script order and can be executed one at a time. The
/// whole script is validated before the first batch is returned, so an error
/// leaves the caller with no partial batch list.
///
/// ```
/// use neotales_migration::{BatchError, split_sql_server_batches};
///
/// let script = "SELECT 1;\nGO\nSELECT 2;\nGO\n";
/// let batches = split_sql_server_batches(script)?;
/// assert_eq!(batches, ["SELECT 1;\n", "SELECT 2;\n"]);
///
/// // Execute `batches[0]`, then `batches[1]`, on the same session.
/// for batch in &batches {
///     assert!(!batch.trim().is_empty());
/// }
///
/// let error = split_sql_server_batches("SELECT 1\nGO 3\n").unwrap_err();
/// assert_eq!(error, BatchError::UnsupportedDirective);
/// assert!(error.to_string().contains("GO directive"));
///
/// # Ok::<(), BatchError>(())
/// ```
// SQL Server accepts CR, LF, and CRLF line endings, and a `GO` directive is line-based.
// End a line at either terminator and keep CRLF as one terminator so batch slices preserve it.
fn line_end(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if start >= bytes.len() {
        return None;
    }
    let mut index = start;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => return Some(index + 1),
            b'\r' => {
                return Some(
                    index
                        + if bytes.get(index + 1) == Some(&b'\n') {
                            2
                        } else {
                            1
                        },
                );
            }
            _ => index += 1,
        }
    }
    Some(bytes.len())
}

pub fn split_sql_server_batches(sql: &str) -> Result<Vec<&str>, BatchError> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Code,
        String,
        Quoted,
        Bracket,
        Block(usize),
    }

    let mut state = State::Code;
    let mut batches = Vec::new();
    let mut start = 0;
    let mut offset = 0;
    while let Some(end) = line_end(sql, offset) {
        let line = &sql[offset..end];
        if state == State::Code {
            let trimmed = line.trim();
            let directive = trimmed
                .split_once("--")
                .map_or(trimmed, |(before, _)| before)
                .trim();
            let mut words = directive.split_whitespace();
            if words
                .next()
                .is_some_and(|word| word.eq_ignore_ascii_case("go"))
            {
                if words.next().is_some() {
                    return Err(BatchError::UnsupportedDirective);
                }
                let batch = &sql[start..offset];
                if !batch.trim().is_empty() {
                    batches.push(batch);
                }
                start = end;
                offset = end;
                continue;
            }
        }
        let bytes = line.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let pair = bytes.get(i..i + 2);
            match state {
                State::Code if pair == Some(b"--") => break,
                State::Code if pair == Some(b"/*") => {
                    state = State::Block(1);
                    i += 2;
                }
                State::Code => {
                    state = match bytes[i] {
                        b'\'' => State::String,
                        b'"' => State::Quoted,
                        b'[' => State::Bracket,
                        _ => State::Code,
                    };
                    i += 1;
                }
                State::Block(depth) if pair == Some(b"/*") => {
                    state = State::Block(depth + 1);
                    i += 2;
                }
                State::Block(depth) if pair == Some(b"*/") => {
                    state = if depth == 1 {
                        State::Code
                    } else {
                        State::Block(depth - 1)
                    };
                    i += 2;
                }
                State::Block(_) => i += 1,
                quoted => {
                    let delimiter = match quoted {
                        State::String => b'\'',
                        State::Quoted => b'"',
                        State::Bracket => b']',
                        _ => unreachable!(),
                    };
                    if bytes[i] == delimiter {
                        if bytes.get(i + 1) == Some(&delimiter) {
                            i += 2;
                        } else {
                            state = State::Code;
                            i += 1;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
        }
        offset = end;
    }
    if state != State::Code {
        return Err(BatchError::Unterminated);
    }
    let batch = &sql[start..];
    if !batch.trim().is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_only_directives_and_preserves_batches() {
        let sql = "select 1\r\n gO -- next\r\nselect 2\nGO\n";
        assert_eq!(
            split_sql_server_batches(sql).unwrap(),
            vec!["select 1\r\n", "select 2\n"]
        );
        assert_eq!(
            split_sql_server_batches("GO\n go\n").unwrap(),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn splits_cr_lf_and_crlf_go_lines() {
        assert_eq!(
            split_sql_server_batches("select 1\rGO\rselect 2\r").unwrap(),
            vec!["select 1\r", "select 2\r"]
        );
        assert_eq!(
            split_sql_server_batches("select 1\r\nGO\r\nselect 2\r\n").unwrap(),
            vec!["select 1\r\n", "select 2\r\n"]
        );
        assert_eq!(
            split_sql_server_batches("select 1\nGO\nselect 2\n").unwrap(),
            vec!["select 1\n", "select 2\n"]
        );
        // A CR-terminated line comment does not swallow a following GO or code line.
        assert_eq!(
            split_sql_server_batches("select 1 -- c\rGO\rselect 2").unwrap(),
            vec!["select 1 -- c\r", "select 2"]
        );
        // Line splitting must not end a string or block comment.
        assert_eq!(
            split_sql_server_batches("select 'a\rGO\rb'\rGO\rselect 2").unwrap(),
            vec!["select 'a\rGO\rb'\r", "select 2"]
        );
    }

    #[test]
    fn ignores_go_in_lexical_contexts() {
        let sql = "select 'x''\nGO\nx', \"x\nGO\nx\", [x]]\nGO\nx]\n/* outer\n/* nested */\nGO\n*/\nGO\nselect 2";
        let batches = split_sql_server_batches(sql).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[1], "select 2");
    }

    #[test]
    fn rejects_unsupported_or_unclosed_before_returning() {
        assert_eq!(
            split_sql_server_batches("select 1\nGO 2\n"),
            Err(BatchError::UnsupportedDirective)
        );
        for sql in ["select 'x", "select [x", "/* x", "select 1\nGO\n\"x"] {
            assert_eq!(split_sql_server_batches(sql), Err(BatchError::Unterminated));
        }
    }

    #[test]
    fn errors_have_distinct_descriptive_messages() {
        let unterminated = BatchError::Unterminated.to_string();
        let directive = BatchError::UnsupportedDirective.to_string();
        assert!(unterminated.contains("not closed"), "{unterminated}");
        assert!(directive.contains("GO directive"), "{directive}");
        assert_ne!(unterminated, directive);
        assert!(!unterminated.contains("Unterminated"));
        assert!(!directive.contains("UnsupportedDirective"));
    }
}
