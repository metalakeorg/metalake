//! Embeds the protocol's SQL in the order it installs (ADR 0003 §3.6, `sources`):
//! - the tables of the specification, `spec/v0/schema/`, in file-name order;
//! - the files of the reference the protocol reuses, `sql/v0/reference-reused.txt`, before all of its own, or, for
//!   `name before file`, just before that file of its own;
//! - the protocol's own files, `sql/v0/`, in file-name order, which are rendered for a catalog's schema.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for dir in ["spec/v0/schema", "spec/v0/reference", "sql/v0"] {
        println!("cargo::rerun-if-changed={}", root.join(dir).display());
    }

    let mut out = String::from("/// The protocol's sources, in the order they install: (name, kind, text).\n");
    out += "pub(crate) const SOURCES: &[(&str, Kind, &str)] = &[\n";
    for path in sql_files(&root.join("spec/v0/schema")) {
        entry(&mut out, &root, &path, "Table");
    }
    let list = std::fs::read_to_string(root.join("sql/v0/reference-reused.txt")).expect("reference-reused.txt");
    let reused: Vec<(String, Option<String>)> = list
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| match l.split_once(" before ") {
            Some((name, before)) => (name.to_string(), Some(before.trim().to_string())),
            None => (l.to_string(), None),
        })
        .collect();
    let reference = |name: &str| root.join("spec/v0/reference").join(name);
    for (name, _) in reused.iter().filter(|(_, before)| before.is_none()) {
        entry(&mut out, &root, &reference(name), "Reference");
    }
    for path in sql_files(&root.join("sql/v0")) {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        for (reused, _) in reused.iter().filter(|(_, before)| before.as_deref() == Some(name)) {
            entry(&mut out, &root, &reference(reused), "Reference");
        }
        entry(&mut out, &root, &path, "Protocol");
    }
    out += "];\n";

    let target = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("sources.rs");
    std::fs::write(target, out).expect("sources.rs");
}

/// The `.sql` files of a directory, in file-name order (bytewise, as the runner of the specification loads them).
fn sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.expect("a directory entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    paths.sort();
    paths
}

fn entry(out: &mut String, root: &Path, path: &Path, kind: &str) {
    assert!(path.is_file(), "{} does not exist", path.display());
    let name = path
        .strip_prefix(root)
        .expect("under the root")
        .to_string_lossy()
        .replace('\\', "/");
    let full = path.to_string_lossy().replace('\\', "/");
    writeln!(out, "    ({name:?}, Kind::{kind}, include_str!({full:?})),").expect("writing to a string");
}
