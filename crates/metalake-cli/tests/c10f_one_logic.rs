//! A face translates and decides nothing: reaching a lake and what a verb does live in `metalake-client`, once.

use std::fs;
use std::path::Path;

const REACHES_BELOW: [&str; 3] = ["metalake-local", "metalake-datafusion", "metalake-pg"];
const DECIDES: [&str; 6] = [
    "CatalogConfig",
    "Cluster",
    "MergeReport",
    "Storage::new",
    ".maintenance()",
    ".transaction()",
];

fn sources(dir: &Path, found: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, found);
        } else {
            found.push((path.display().to_string(), fs::read_to_string(&path).unwrap()));
        }
    }
}

#[test]
fn the_command_line_reaches_nothing_but_the_client() {
    let manifest = include_str!("../Cargo.toml");
    for crate_below in REACHES_BELOW {
        let depends = manifest.lines().any(|l| l.starts_with(crate_below));
        assert!(!depends, "{crate_below} is the client's to reach");
    }
}

#[test]
fn the_command_line_decides_nothing_the_client_does() {
    let mut found = Vec::new();
    sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut found);
    assert!(found.len() > 5, "the sources were read");
    for (path, text) in found {
        for word in DECIDES {
            assert!(!text.contains(word), "{path} has {word}: that is the client's");
        }
    }
}
