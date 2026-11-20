//! Driver-free SQL migration planning. This crate does not connect to databases or execute SQL.
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::missing_panics_doc
)]
#![allow(missing_docs)]
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};
mod sql_server_batches;
pub use sql_server_batches::{BatchError, split_sql_server_batches};
pub type Checksum = String;
fn hash(domain: &[u8], parts: &[&[u8]]) -> Checksum {
    let mut h = Sha256::new();
    h.update(domain);
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p);
    }
    format!("{:x}", h.finalize())
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    version: u64,
    name: String,
    up: String,
    down: Option<String>,
    checksum: Checksum,
    baseline: bool,
}
impl Migration {
    pub fn new(
        version: u64,
        name: impl Into<String>,
        up: impl Into<String>,
        down: Option<String>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let up = up.into();
        if name.trim().is_empty()
            || up.trim().is_empty()
            || down.as_deref().is_some_and(|s| s.trim().is_empty())
        {
            return Err(Error::InvalidDefinition);
        }
        let checksum = hash(
            b"neotales-migration-v1\0",
            &[
                &version.to_be_bytes(),
                name.as_bytes(),
                up.as_bytes(),
                down.as_deref().unwrap_or("").as_bytes(),
                &[u8::from(down.is_some())],
            ],
        );
        Ok(Self {
            version,
            name,
            up,
            down,
            checksum,
            baseline: false,
        })
    }
    /// Marks a definition as an irreversible flatten baseline; changes its identity.
    #[must_use]
    pub fn into_baseline(mut self) -> Self {
        if !self.baseline {
            self.checksum = hash(
                b"neotales-migration-baseline-v1\0",
                &[self.checksum.as_bytes()],
            );
            self.baseline = true;
        }
        self
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn up(&self) -> &str {
        &self.up
    }
    pub fn down(&self) -> Option<&str> {
        self.down.as_deref()
    }
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Applied {
    pub version: u64,
    pub checksum: Checksum,
}
/// Persistable migration ledger state, including binding fingerprint and permanent rollback barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedHistory {
    /// Ordered applied migration identities.
    pub applied: Vec<Applied>,
    /// Fingerprint returned by identifier binding for this migration set.
    pub binding_fingerprint: Checksum,
    /// Baseline that rollback may never cross.
    pub barrier: Option<Applied>,
}
fn applied(m: &Migration) -> Applied {
    Applied {
        version: m.version,
        checksum: m.checksum.clone(),
    }
}
/// Ordered migration collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migrations(Vec<Migration>);
impl Migrations {
    pub fn new(mut v: Vec<Migration>) -> Result<Self, Error> {
        v.sort_by_key(Migration::version);
        let mut names = BTreeSet::new();
        for (i, m) in v.iter().enumerate() {
            if !names.insert(m.name.clone()) || i > 0 && v[i - 1].version == m.version {
                return Err(Error::Duplicate);
            }
        }
        if v.iter().enumerate().any(|(i, m)| m.baseline && i != 0) {
            return Err(Error::InvalidBaseline);
        }
        Ok(Self(v))
    }
    pub fn as_slice(&self) -> &[Migration] {
        &self.0
    }
    fn validate_rows(&self, rows: &[Applied]) -> Result<(), Error> {
        if rows.len() > self.0.len() {
            return Err(Error::UnknownApplied);
        }
        for (i, r) in rows.iter().enumerate() {
            let m = self.0.get(i).ok_or(Error::HistoryDrift)?;
            if r.version != m.version {
                return Err(Error::HistoryDrift);
            }
            if r.checksum != m.checksum {
                return Err(Error::Modified);
            }
        }
        Ok(())
    }
    pub fn validate_state(
        &self,
        rows: Vec<Applied>,
        binding_fingerprint: Checksum,
        barrier: Option<Applied>,
    ) -> Result<HistoryState, Error> {
        self.validate_rows(&rows)?;
        let expected = self
            .0
            .first()
            .filter(|m| m.baseline && !rows.is_empty())
            .map(applied);
        if barrier != expected {
            return Err(Error::InvalidBaseline);
        }
        Ok(HistoryState {
            applied: rows,
            binding_fingerprint,
            barrier,
        })
    }
    /// Revalidates decoded persisted state against these definitions before planning.
    pub fn validate_persisted(&self, persisted: &PersistedHistory) -> Result<HistoryState, Error> {
        self.validate_state(
            persisted.applied.clone(),
            persisted.binding_fingerprint.clone(),
            persisted.barrier.clone(),
        )
    }
    pub fn plan_up(
        &self,
        state: &HistoryState,
        binding_fingerprint: &str,
    ) -> Result<Vec<Planned>, Error> {
        state.check_binding(binding_fingerprint)?;
        self.validate_state(
            state.applied.clone(),
            state.binding_fingerprint.clone(),
            state.barrier.clone(),
        )?;
        Ok(self
            .0
            .iter()
            .skip(state.applied.len())
            .map(|m| Planned {
                version: m.version,
                sql: m.up.clone(),
            })
            .collect())
    }
    pub fn plan_down(
        &self,
        state: &HistoryState,
        binding_fingerprint: &str,
        count: usize,
    ) -> Result<Vec<Planned>, Error> {
        state.check_binding(binding_fingerprint)?;
        self.validate_state(
            state.applied.clone(),
            state.binding_fingerprint.clone(),
            state.barrier.clone(),
        )?;
        if count > state.applied.len() {
            return Err(Error::Baseline);
        }
        let selected = &state.applied[state.applied.len() - count..];
        if selected.iter().any(|a| {
            state
                .barrier
                .as_ref()
                .is_some_and(|b| a.version <= b.version)
        }) {
            return Err(Error::Baseline);
        }
        let mut out = Vec::with_capacity(count);
        for r in selected.iter().rev() {
            let m = self
                .0
                .iter()
                .find(|m| m.version == r.version)
                .ok_or(Error::UnknownApplied)?;
            out.push(Planned {
                version: m.version,
                sql: m.down.clone().ok_or(Error::MissingDown)?,
            });
        }
        Ok(out)
    }
}
/// Validated history identity and persistent rollback barrier. Construct only by validation or flatten assessment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryState {
    applied: Vec<Applied>,
    binding_fingerprint: Checksum,
    barrier: Option<Applied>,
}
impl HistoryState {
    pub fn applied(&self) -> &[Applied] {
        &self.applied
    }
    pub fn binding_fingerprint(&self) -> &str {
        &self.binding_fingerprint
    }
    pub fn barrier(&self) -> Option<&Applied> {
        self.barrier.as_ref()
    }
    /// Produces the ledger payload a caller can persist and later validate again.
    pub fn persisted(&self) -> PersistedHistory {
        PersistedHistory {
            applied: self.applied.clone(),
            binding_fingerprint: self.binding_fingerprint.clone(),
            barrier: self.barrier.clone(),
        }
    }
    fn check_binding(&self, b: &str) -> Result<(), Error> {
        if self.binding_fingerprint == b {
            Ok(())
        } else {
            Err(Error::BindingDrift)
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Planned {
    pub version: u64,
    pub sql: String,
}
/// Validated replacement of an exact ordered legacy prefix with a newer baseline migration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlattenManifest {
    old: Migrations,
    baseline: Migration,
}
impl FlattenManifest {
    /// Returns the complete legacy definitions that an existing ledger must match.
    ///
    /// Backends can use this immutable view to bound history reads before assessment.
    #[must_use]
    pub fn legacy(&self) -> &Migrations {
        &self.old
    }

    /// Returns the checksum-pinned baseline definition, without executing its SQL.
    #[must_use]
    pub fn baseline(&self) -> &Migration {
        &self.baseline
    }

    pub fn new(old: Vec<Migration>, baseline: Migration) -> Result<Self, Error> {
        if old.is_empty()
            || old
                .windows(2)
                .any(|pair| pair[0].version >= pair[1].version)
        {
            return Err(Error::InvalidBaseline);
        }
        let old = Migrations::new(old)?;
        if !baseline.baseline
            || old.0.iter().any(|m| m.baseline)
            || old.0.last().is_some_and(|m| m.version >= baseline.version)
        {
            return Err(Error::InvalidBaseline);
        }
        Ok(Self { old, baseline })
    }
    pub fn assess(
        &self,
        history: Option<&PersistedHistory>,
        binding: Checksum,
    ) -> Result<FlattenAssessment, Error> {
        let exact: Vec<_> = self.old.0.iter().map(applied).collect();
        let decision = if let Some(history) = history {
            if history.binding_fingerprint != binding {
                return Err(Error::BindingDrift);
            }
            self.old.validate_persisted(history)?;
            if history.applied != exact {
                return Err(Error::FlattenDrift);
            }
            FlattenDecision::ReplaceExactHistory
        } else {
            FlattenDecision::FreshBaseline
        };
        Ok(FlattenAssessment {
            decision,
            state: HistoryState {
                applied: vec![applied(&self.baseline)],
                binding_fingerprint: binding,
                barrier: Some(applied(&self.baseline)),
            },
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlattenDecision {
    FreshBaseline,
    ReplaceExactHistory,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlattenAssessment {
    pub decision: FlattenDecision,
    pub state: HistoryState,
}
/// Fixed SQL lexical dialect; each dialect controls quoting and comment/string syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dialect {
    PostgreSql,
    Sqlite,
    MySql,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Identifier(String);
impl Identifier {
    pub fn new(v: impl Into<String>) -> Result<Self, Error> {
        let s = v.into();
        if s.is_empty()
            || s.len() > 63
            || !s.is_ascii()
            || s.bytes()
                .any(|b| !b.is_ascii_lowercase() && !b.is_ascii_digit() && b != b'_')
            || !s.as_bytes()[0].is_ascii_lowercase() && s.as_bytes()[0] != b'_'
        {
            return Err(Error::InvalidIdentifier);
        }
        Ok(Self(s))
    }
}
/// Binds identifier placeholders in SQL code only. Lexical scanner rejects unsupported/unclosed constructs and placeholders in all non-code contexts.
#[allow(clippy::many_single_char_names)]
pub fn bind_identifiers(
    sql: &str,
    bindings: &BTreeMap<String, Identifier>,
    dialect: Dialect,
) -> Result<(String, Checksum), Error> {
    let b = sql.as_bytes();
    let (mut i, mut out, mut used) = (0, String::new(), BTreeSet::new());
    let mut names = BTreeSet::new();
    if bindings.values().any(|id| !names.insert(&id.0)) {
        return Err(Error::IdentifierCollision);
    }
    let (mut block, mut state) = (0usize, 0u8);
    let mut dollar: Option<Vec<u8>> = None;
    while i < b.len() {
        if !sql.is_char_boundary(i) {
            return Err(Error::InvalidTemplate);
        }
        if state != 0 && b[i..].starts_with(b"{{") {
            return Err(Error::InvalidTemplate);
        }
        if state == 0 {
            if dialect == Dialect::Sqlite && b[i] == b'[' {
                return Err(Error::InvalidTemplate);
            }
            if dialect == Dialect::MySql && b[i..].starts_with(b"/*") {
                return Err(Error::InvalidTemplate);
            }
            if dialect == Dialect::MySql
                && b[i..].starts_with(b"--")
                && b.get(i + 2)
                    .is_some_and(|c| !c.is_ascii_whitespace() && !c.is_ascii_control())
            {
                return Err(Error::InvalidTemplate);
            }
            if dialect == Dialect::MySql && b[i] == b'"' {
                return Err(Error::InvalidTemplate);
            }
            if b[i..].starts_with(b"--") || dialect == Dialect::MySql && b[i] == b'#' {
                state = 1;
                if b[i] == b'#' {
                    out.push('#');
                    i += 1;
                } else {
                    out.push_str("--");
                    i += 2;
                }
                continue;
            }
            if b[i..].starts_with(b"/*") {
                state = 2;
                block = 1;
                out.push_str("/*");
                i += 2;
                continue;
            }
            if b[i] == b'\'' {
                state = 3;
                out.push('\'');
                i += 1;
                continue;
            }
            if b[i] == b'"' {
                state = 4;
                out.push('"');
                i += 1;
                continue;
            }
            if dialect == Dialect::MySql && b[i] == b'`' {
                state = 5;
                out.push('`');
                i += 1;
                continue;
            }
            if dialect == Dialect::PostgreSql && b[i] == b'$' {
                // A dollar in an unquoted PostgreSQL identifier is not a string
                // delimiter. Refuse ambiguous adjacency rather than guessing.
                if i > 0
                    && (b[i - 1].is_ascii_alphanumeric()
                        || matches!(b[i - 1], b'_' | b'$' | b'}')
                        || !b[i - 1].is_ascii())
                {
                    return Err(Error::InvalidTemplate);
                }
                // $1 is a parameter, not a dollar-quote tag.
                if b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    return Err(Error::InvalidTemplate);
                }
                if b.get(i + 1).is_some_and(|c| !c.is_ascii()) {
                    return Err(Error::InvalidTemplate);
                }
                let mut j = i + 1;
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
                if b.get(j).is_some_and(|c| !c.is_ascii()) {
                    return Err(Error::InvalidTemplate);
                }
                if j < b.len() && b[j] == b'$' {
                    let tag = b[i..=j].to_vec();
                    dollar = Some(tag.clone());
                    out.push_str(std::str::from_utf8(&tag).map_err(|_| Error::InvalidTemplate)?);
                    i = j + 1;
                    state = 6;
                    continue;
                }
            }
            if b[i..].starts_with(b"{{") {
                let tail = &sql[i + 2..];
                let end = tail.find("}}").ok_or(Error::InvalidTemplate)?;
                let key = &tail[..end];
                if key.is_empty() || !key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
                    return Err(Error::InvalidTemplate);
                }
                let id = bindings.get(key).ok_or(Error::InvalidTemplate)?;
                let quoted = match dialect {
                    Dialect::MySql => format!("`{}`", id.0),
                    _ => format!("\"{}\"", id.0),
                };
                out.push_str(&quoted);
                used.insert(key.to_owned());
                i += end + 4;
                continue;
            }
            if b[i..].starts_with(b"}}") {
                return Err(Error::InvalidTemplate);
            }
            if b[i] == b'`' && dialect != Dialect::MySql {
                return Err(Error::InvalidTemplate);
            }
        } else if state == 1 {
            if b[i] == b'\n' || b[i] == b'\r' && dialect != Dialect::Sqlite {
                state = 0;
            }
        } else if state == 2 {
            if b[i..].starts_with(b"/*") {
                if dialect != Dialect::PostgreSql {
                    return Err(Error::InvalidTemplate);
                }
                block += 1;
                out.push_str("/*");
                i += 2;
                continue;
            }
            if b[i..].starts_with(b"*/") {
                block -= 1;
                out.push_str("*/");
                i += 2;
                if block == 0 {
                    state = 0;
                }
                continue;
            }
        } else if state == 3 || state == 4 || state == 5 {
            if b[i] == b'\\' {
                return Err(Error::InvalidTemplate);
            }
            let delim = match state {
                3 => b'\'',
                4 => b'"',
                _ => b'`',
            };
            if b[i..].starts_with(b"{{") {
                return Err(Error::InvalidTemplate);
            }
            if b[i] == delim {
                if i + 1 < b.len() && b[i + 1] == delim {
                    out.push(char::from(delim));
                    out.push(char::from(delim));
                    i += 2;
                    continue;
                }
                state = 0;
            }
        } else if state == 6 {
            let tag = dollar.as_ref().ok_or(Error::InvalidTemplate)?;
            if b[i..].starts_with(tag) {
                out.push_str(std::str::from_utf8(tag).map_err(|_| Error::InvalidTemplate)?);
                i += tag.len();
                state = 0;
                dollar = None;
                continue;
            }
            if b[i..].starts_with(b"{{") {
                return Err(Error::InvalidTemplate);
            }
        }
        let ch = sql[i..].chars().next().ok_or(Error::InvalidTemplate)?;
        out.push(ch);
        i += ch.len_utf8();
    }
    if (state != 0 && state != 1) || used.len() != bindings.len() {
        return Err(Error::InvalidTemplate);
    }
    let mut parts = Vec::new();
    for (k, v) in bindings {
        parts.push(k.as_bytes());
        parts.push(v.0.as_bytes());
    }
    Ok((out, hash(b"neotales-bindings-v2\0", &parts)))
}
/// Bounded unordered polling with ordered results. Futures and outputs must be `Send`; no workers/runtime are created.
pub async fn run_bounded<T: Send, E: Send, F: Future<Output = Result<T, E>> + Send>(
    jobs: Vec<F>,
    limit: usize,
) -> Result<Vec<Result<T, E>>, Error> {
    if limit == 0 {
        return Err(Error::InvalidLimit);
    }
    let n = jobs.len();
    let mut indexed = stream::iter(
        jobs.into_iter()
            .enumerate()
            .map(|(i, f)| async move { (i, f.await) }),
    )
    .buffer_unordered(limit)
    .collect::<Vec<_>>()
    .await;
    indexed.sort_by_key(|x| x.0);
    if indexed.len() != n {
        return Err(Error::InvalidLimit);
    }
    Ok(indexed.into_iter().map(|x| x.1).collect())
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidDefinition,
    Duplicate,
    UnknownApplied,
    Modified,
    HistoryDrift,
    Baseline,
    MissingDown,
    InvalidIdentifier,
    InvalidTemplate,
    InvalidLimit,
    InvalidBaseline,
    FlattenDrift,
    BindingDrift,
    IdentifierCollision,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "migration error: {self:?}")
    }
}
impl std::error::Error for Error {}
#[cfg(test)]
mod tests {
    use super::*;
    fn m(v: u64, name: &str) -> Migration {
        Migration::new(v, name, "UP", Some("DOWN".into())).unwrap()
    }
    fn bind() -> Checksum {
        "fp".into()
    }
    #[test]
    fn identifier_restrictions() {
        assert!(Identifier::new("Upper").is_err());
        assert!(Identifier::new("a".repeat(64)).is_err());
        assert!(Identifier::new("a".repeat(63)).is_ok());
    }
    #[test]
    fn quote_and_dialect_contexts() {
        let mut b = BTreeMap::new();
        b.insert("t".into(), Identifier::new("safe_name").unwrap());
        assert_eq!(
            bind_identifiers("select * from {{t}}", &b, Dialect::PostgreSql)
                .unwrap()
                .0,
            "select * from \"safe_name\""
        );
        assert_eq!(
            bind_identifiers("select * from {{t}}", &b, Dialect::MySql)
                .unwrap()
                .0,
            "select * from `safe_name`"
        );
        assert!(bind_identifiers("select * from {{t}}", &b, Dialect::Sqlite).is_ok());
        for s in [
            "'{{t}}'",
            "\"{{t}}\"",
            "-- {{t}}",
            "/* {{t}} */",
            "$tag${{t}}$tag$",
            "/* unclosed",
            "'unclosed",
            "$tag$ unclosed",
        ] {
            assert!(bind_identifiers(s, &b, Dialect::PostgreSql).is_err(), "{s}");
        }
        assert!(bind_identifiers("-- line comment at EOF", &b, Dialect::PostgreSql).is_err());
        assert!(bind_identifiers("SELECT 1 -- EOF", &BTreeMap::new(), Dialect::PostgreSql).is_ok());
        assert!(
            bind_identifiers(
                "/* outer /* inner */ end */ SELECT 1",
                &BTreeMap::new(),
                Dialect::PostgreSql
            )
            .is_ok()
        );
        assert_eq!(
            bind_identifiers("{{t}}", &b, Dialect::Sqlite).unwrap().0,
            "\"safe_name\""
        );
    }
    #[test]
    fn identifiers_collision() {
        let mut b = BTreeMap::new();
        b.insert("a".into(), Identifier::new("same").unwrap());
        b.insert("b".into(), Identifier::new("same").unwrap());
        assert_eq!(
            bind_identifiers("{{a}} {{b}}", &b, Dialect::PostgreSql),
            Err(Error::IdentifierCollision)
        );
    }
    #[test]
    fn binding_state_gates_both_directions() {
        let migrations = Migrations::new(vec![m(1, "one"), m(2, "two")]).unwrap();
        let s = migrations
            .validate_state(vec![applied(&migrations.0[0])], bind(), None)
            .unwrap();
        assert_eq!(migrations.plan_up(&s, "other"), Err(Error::BindingDrift));
        assert_eq!(
            migrations.plan_down(&s, "other", 1),
            Err(Error::BindingDrift)
        );
        let persisted = s.persisted();
        assert_eq!(migrations.validate_persisted(&persisted).unwrap(), s);
        let mut drifted = persisted;
        drifted.binding_fingerprint = "wrong".into();
        let drifted = migrations.validate_persisted(&drifted).unwrap();
        assert_eq!(
            migrations.plan_up(&drifted, &bind()),
            Err(Error::BindingDrift)
        );
        assert_eq!(
            migrations.plan_down(&drifted, &bind(), 1),
            Err(Error::BindingDrift)
        );
    }
    #[test]
    fn flatten_validates_definitions_and_barrier() {
        let old = vec![m(1, "old")];
        let baseline = m(5, "base").into_baseline();
        let f = FlattenManifest::new(old.clone(), baseline.clone()).unwrap();
        let oldrows = vec![applied(&old[0])];
        let history = PersistedHistory {
            applied: oldrows.clone(),
            binding_fingerprint: bind(),
            barrier: None,
        };
        assert_eq!(
            f.assess(Some(&history), "wrong".into()),
            Err(Error::BindingDrift)
        );
        let a = f.assess(Some(&history), bind()).unwrap();
        assert_eq!(a.decision, FlattenDecision::ReplaceExactHistory);
        let all = Migrations::new(vec![baseline, m(6, "new")]).unwrap();
        assert_eq!(all.plan_down(&a.state, &bind(), 1), Err(Error::Baseline));
        assert!(FlattenManifest::new(vec![m(6, "late")], m(5, "early")).is_err());
        assert_eq!(
            f.assess(None, bind()).unwrap().decision,
            FlattenDecision::FreshBaseline
        );
    }
    #[test]
    fn regression_lexer_context_and_repeated_binding() {
        let mut b = BTreeMap::new();
        b.insert("t".into(), Identifier::new("safe").unwrap());
        assert!(
            bind_identifiers(
                "CREATE TABLE {{t}} (x int); INSERT INTO {{t}} VALUES (1)",
                &b,
                Dialect::Sqlite
            )
            .is_ok()
        );
        for (sql, dialect) in [
            ("{{t}} -- {{t}}", Dialect::PostgreSql),
            ("{{t}} /* outer /* nested */ {{t}} */", Dialect::PostgreSql),
            ("{{t}} $é${{t}}$é$", Dialect::PostgreSql),
            ("{{t}} $tagé${{t}}$tagé$", Dialect::PostgreSql),
            ("{{t}} [{{t}}]", Dialect::Sqlite),
            ("{{t}} [unterminated", Dialect::Sqlite),
        ] {
            assert!(bind_identifiers(sql, &b, dialect).is_err(), "{sql}");
        }
        assert!(bind_identifiers("-- comment\r{{t}}", &b, Dialect::PostgreSql).is_ok());
    }
    #[test]
    fn dollar_identifier_adjacency_and_comment_newlines() {
        let b = BTreeMap::from([("t".into(), Identifier::new("safe").unwrap())]);
        assert_eq!(
            bind_identifiers(
                "SELECT foo$tag$, $tag${{t}}$tag$, foo$tag$",
                &b,
                Dialect::PostgreSql
            ),
            Err(Error::InvalidTemplate)
        );
        for sql in [
            "SELECT foo$$, $${{t}}$$",
            "SELECT café$tag$, $tag${{t}}$tag$",
            "SELECT $1$, $1${{t}}$1$",
            "SELECT {{t}}$tag$",
            "SELECT $tag${{t}}$tag$",
        ] {
            assert_eq!(
                bind_identifiers(sql, &b, Dialect::PostgreSql),
                Err(Error::InvalidTemplate),
                "{sql}"
            );
        }
        assert!(bind_identifiers("SELECT $tag$safe$tag$, {{t}}", &b, Dialect::PostgreSql).is_ok());
    }
    #[test]
    fn sqlite_comment_newlines() {
        let b = BTreeMap::from([("t".into(), Identifier::new("safe").unwrap())]);
        assert_eq!(
            bind_identifiers("SELECT 1 -- comment\r{{t}}", &b, Dialect::Sqlite),
            Err(Error::InvalidTemplate)
        );
        assert_eq!(
            bind_identifiers("SELECT 1 -- comment\r{{t}}\n", &b, Dialect::Sqlite),
            Err(Error::InvalidTemplate)
        );
        assert!(bind_identifiers("SELECT 1 -- comment\r\n{{t}}", &b, Dialect::Sqlite).is_ok());
        assert!(bind_identifiers("SELECT 1 -- comment\r{{t}}", &b, Dialect::PostgreSql).is_ok());
    }
    #[test]
    fn regression_cleared_barrier() {
        let f = FlattenManifest::new(vec![m(1, "old")], m(5, "base").into_baseline()).unwrap();
        let history = PersistedHistory {
            applied: vec![applied(&m(1, "old"))],
            binding_fingerprint: bind(),
            barrier: None,
        };
        let state = f.assess(Some(&history), bind()).unwrap().state;
        let mut persisted = state.persisted();
        persisted.barrier = None;
        let all = Migrations::new(vec![m(5, "base").into_baseline()]).unwrap();
        assert_eq!(
            all.validate_persisted(&persisted),
            Err(Error::InvalidBaseline)
        );
        persisted.barrier = Some(Applied {
            version: 5,
            checksum: "tampered".into(),
        });
        assert_eq!(
            all.validate_persisted(&persisted),
            Err(Error::InvalidBaseline)
        );
    }
    #[test]
    fn rejects_blank_down() {
        assert!(Migration::new(1, "x", "UP", Some(" \n".into())).is_err());
    }
    #[tokio::test]
    async fn jobs_order_failures_and_bounded_concurrency() {
        let r = run_bounded(
            vec![
                std::future::ready(Ok::<_, u8>(1)),
                std::future::ready(Err::<u8, _>(2)),
            ],
            2,
        )
        .await
        .unwrap();
        assert_eq!(r, vec![Ok(1), Err(2)]);
        assert!(
            run_bounded(vec![std::future::ready(Ok::<(), ()>(()))], 0)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn jobs_keep_limit_full_after_early_completion() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let jobs = (0..8)
            .map(|_| {
                let active = active.clone();
                let peak = peak.clone();
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, ()>(now)
                }
            })
            .collect();
        let results = run_bounded(jobs, 3).await.unwrap();
        assert_eq!(results.len(), 8);
        assert_eq!(peak.load(Ordering::SeqCst), 3);
    }
}
