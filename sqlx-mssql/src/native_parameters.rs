//! Validation for native SQL Server RPC parameter references.

/// Validates positional `@P1`, `@P2`, ... references without changing SQL.
///
/// Repeated references are allowed. Every supplied parameter must be referenced, and local
/// variables, unknown parameter names and `?` markers are rejected. System variables beginning
/// with `@@` are allowed. Quoted regions and nested comments are ignored.
pub fn validate_native_parameters(sql: &str, count: usize) -> Result<(), &'static str> {
    let bytes = sql.as_bytes();
    let mut used = vec![false; count];
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'[' => {
                let close = if bytes[i] == b'[' { b']' } else { bytes[i] };
                i += 1;
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == close {
                        i += 1;
                        if bytes.get(i) == Some(&close) {
                            i += 1;
                        } else {
                            closed = true;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                if !closed {
                    return Err("unterminated quote");
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1usize;
                while i < bytes.len() && depth > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        depth = depth.checked_add(1).ok_or("comment nesting overflow")?;
                        i += 2;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                if depth != 0 {
                    return Err("unterminated comment");
                }
            }
            b'?' => return Err("use native @P1 parameters"),
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    i += 1;
                }
                if sql[start..i].eq_ignore_ascii_case("DECLARE") {
                    return Err("local variable declarations are unsupported");
                }
            }
            b'@' if i > 0
                && (bytes[i - 1].is_ascii_alphanumeric()
                    || bytes[i - 1] >= 128
                    || matches!(bytes[i - 1], b'_' | b'#' | b'$' | b'@')) =>
            {
                return Err("ambiguous variable boundary");
            }
            b'@' if bytes.get(i + 1) == Some(&b'@') => {
                i += 2;
                let start = i;
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    i += 1;
                }
                if i == start
                    || bytes
                        .get(i)
                        .is_some_and(|b| *b >= 128 || matches!(*b, b'@' | b'#' | b'$'))
                {
                    return Err("invalid system variable");
                }
            }
            b'@' => {
                i += 1;
                if bytes.get(i) != Some(&b'P') {
                    return Err("unsupported local variable");
                }
                i += 1;
                let start = i;
                while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                }
                if start == i
                    || bytes[start] == b'0'
                    || bytes.get(i).is_some_and(|b| {
                        b.is_ascii_alphanumeric()
                            || *b == b'_'
                            || *b >= 128
                            || matches!(*b, b'@' | b'#' | b'$')
                    })
                {
                    return Err("invalid parameter name");
                }
                let number: usize = sql[start..i]
                    .parse()
                    .map_err(|_| "invalid parameter index")?;
                let slot = used
                    .get_mut(number - 1)
                    .ok_or("parameter index out of range")?;
                *slot = true;
                let mut next = i;
                while bytes.get(next).is_some_and(u8::is_ascii_whitespace) {
                    next += 1;
                }
                if bytes.get(next).is_some_and(|b| {
                    matches!(
                        *b,
                        b'=' | b'+' | b'-' | b'*' | b'/' | b'%' | b'&' | b'|' | b'^'
                    )
                }) {
                    return Err("ambiguous parameter assignment");
                }
            }
            _ => i += 1,
        }
    }
    if used.iter().any(|used| !used) {
        return Err("unreferenced parameter");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_native_parameters as validate;

    #[test]
    fn native_references_and_quoted_text() {
        assert!(
            validate(
                "SELECT @P1, @P1, @P2, '@P3', [@P4], \"@P5\" -- @P6\n/* @P7 /* @P8 */ */",
                2
            )
            .is_ok()
        );
        for sql in [
            "SELECT @P2",
            "SELECT @P1, @P3",
            "SELECT @P01",
            "SELECT @P1foo",
            "SELECT @name",
            "SELECT ?",
            "SELECT @P1é",
            "SELECT @p1",
            "DECLARE @P1 INT",
            "SELECT @P1 = 2",
            "SELECT @P1 /* comment */ = 2",
            "SET @P1/**/=2",
            "SELECT @P1 /* outer /* nested */ outer */ = 2",
            "SELECT @P1-- comment\n=2",
            "SELECT @P1 /* comment */ += 2",
            "SELECT @P1 += 2",
            "SELECT @P1 -= 2",
            "SELECT @P1 *= 2",
            "SELECT @P1 /= 2",
            "SELECT @P1 %= 2",
            "SELECT @P1 &= 2",
            "SELECT @P1 |= 2",
            "SELECT @P1 ^= 2",
        ] {
            assert!(validate(sql, 1).is_err(), "{sql}");
        }
        assert!(validate("SELECT @@TRANCOUNT", 0).is_ok());
        assert!(validate("SELECT @P1", 2).is_err());
        // SQL Server line comments end at CR as well as LF.
        assert!(validate("SELECT @P1, -- comment\r@P2", 2).is_ok());
        assert!(validate("SELECT @P1, -- comment\r\n@P2", 2).is_ok());
        assert!(validate("SELECT @P1, -- comment\n@P2", 2).is_ok());
        assert!(validate("SELECT @P1, -- comment\r?", 1).is_err());
        assert!(validate("SELECT @P1, -- comment\r@P3", 2).is_err());
    }
}
