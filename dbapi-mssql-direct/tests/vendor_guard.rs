//! Exact reviewed-source inventory plus syntax-level emission tripwires.
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use syn::visit::{self, Visit};

#[derive(Default)]
struct Emissions(Vec<String>);
impl<'ast> Visit<'ast> for Emissions {
    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        let name = node.path.segments.last().unwrap().ident.to_string();
        if matches!(
            name.as_str(),
            "trace"
                | "debug"
                | "info"
                | "warn"
                | "error"
                | "event"
                | "span"
                | "trace_span"
                | "debug_span"
                | "info_span"
                | "warn_span"
                | "error_span"
                | "log"
                | "log_enabled"
                | "enabled"
                | "instrument"
        ) {
            self.0.push(name);
        }
        // `quiet!` intentionally does not recursively expand its token tree.
        visit::visit_macro(self, node);
    }

    fn visit_attribute(&mut self, node: &'ast syn::Attribute) {
        if node.path().segments.last().unwrap().ident == "instrument" {
            self.0.push("instrument attribute".into());
        }
        visit::visit_attribute(self, node);
    }

    fn visit_item_use(&mut self, node: &'ast syn::ItemUse) {
        fn alias(tree: &syn::UseTree) -> bool {
            match tree {
                syn::UseTree::Rename(_) | syn::UseTree::Glob(_) => true,
                syn::UseTree::Path(path) => alias(&path.tree),
                syn::UseTree::Group(group) => group.items.iter().any(alias),
                syn::UseTree::Name(_) => false,
            }
        }
        if let syn::UseTree::Path(path) = &node.tree
            && (path.ident == "tracing" || path.ident == "log")
            && alias(&path.tree)
        {
            self.0.push("diagnostic alias/glob".into());
        }
        visit::visit_item_use(self, node);
    }
}

fn scan(source: &str) -> Vec<String> {
    let parsed = syn::parse_file(source).expect("all source must parse, including other targets");
    let mut emissions = Emissions::default();
    emissions.visit_file(&parsed);
    emissions.0
}

fn files(path: &Path, found: &mut BTreeSet<PathBuf>) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        assert!(!entry.file_type().unwrap().is_symlink());
        if entry.file_type().unwrap().is_dir() {
            files(&entry.path(), found);
        } else {
            found.insert(entry.path());
        }
    }
}

#[test]
fn sealed_vendor_inventory_is_exact_and_has_no_diagnostic_invocations() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut expected = BTreeSet::new();
    for line in include_str!("../vendor/SHA256SUMS").lines() {
        let (hash, relative) = line.split_once("  ").unwrap();
        let path = root.join(relative);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), hash, "{relative}");
        expected.insert(path.clone());
        if path.extension().is_some_and(|ext| ext == "rs") {
            assert!(
                scan(std::str::from_utf8(&bytes).unwrap()).is_empty(),
                "{relative}"
            );
        }
    }
    let mut actual = BTreeSet::new();
    for package in [
        "mssql-tds-0.3.0",
        "native-tls-0.2.18",
        "tokio-native-tls-0.3.1",
    ] {
        files(&root.join("vendor").join(package), &mut actual);
    }
    actual.insert(root.join("Cargo.toml"));
    assert_eq!(actual, expected, "unreviewed additions or removals");
    let mut owned = BTreeSet::new();
    files(&root.join("src"), &mut owned);
    for path in owned {
        // The cfg(test)-only live harness intentionally emits a positive-control
        // event to verify unrelated application logging remains visible.
        if path.ends_with("src/live.rs") {
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            assert!(
                scan(&fs::read_to_string(&path).unwrap()).is_empty(),
                "{path:?}"
            );
        }
    }
}

#[test]
fn tripwire_rejects_qualified_unqualified_attributes_and_alias_mutations() {
    // Guard logic is otherwise not reachable through the provider's public API.
    for mutation in [
        "fn f() { tracing::trace!(\"secret\"); }",
        "fn f() { log::error!(\"secret\"); }",
        "fn f() { event!(tracing::Level::TRACE, \"secret\"); }",
        "#[tracing::instrument] fn f(secret: String) {}",
        "fn f() { let _ = tracing::info_span!(\"secret\"); }",
        "use tracing::trace as leak; fn f() { leak!(\"secret\"); }",
        "use log::*;",
    ] {
        assert!(
            !scan(mutation).is_empty(),
            "ineffective tripwire: {mutation}"
        );
    }
    assert!(scan("fn f() { quiet!(panic!(\"must not execute\")); }").is_empty());
}
