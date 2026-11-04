//! C10c (ADR 0003 §7): operating a lake through the binary: upgrading its catalog and keeping it healthy.

mod common;

use std::path::Path;
use std::process::Command;

use common::{lake, ok, run, sql, text};
use serde_json::Value;

/// A statement in the catalog's Postgres, past the protocol, as an older or newer catalog would have it. The tests'
/// Postgres is in the container `sql/test.sh` starts, and the command line may not depend on the driver.
fn postgres(statement: &str) {
    let output = Command::new("docker")
        .args([
            "exec",
            "metalake-protocol-pg",
            "psql",
            "-U",
            "postgres",
            "-c",
            statement,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", text(&output.stderr));
}

fn rows(dir: &Path) -> String {
    sql(dir, "select k from t order by k", "csv")
}

#[test]
fn migrating_a_catalog_up_to_date_changes_nothing() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    let before: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();

    for _ in 0..2 {
        let migrated: Value = serde_json::from_str(&ok(&dir, &["migrate", "--format", "json"])).unwrap();
        assert!(migrated["format_migration"].is_null());
        assert_eq!(migrated["schema"], schema.as_str());
        assert_eq!(migrated["warehouse"], before["warehouse"]);
        assert_eq!(migrated["protocol_version"], before["protocol_version"]);
    }
    assert_eq!(rows(&dir), "k\n1\n");
}

#[test]
fn a_catalog_behind_this_binary_asks_for_migrate() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    postgres(&format!("delete from {schema}.meta where key = 'protocol_version'"));

    let refused = run(&dir, &["status"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(
        text(&refused.stderr).contains("run `metalake migrate`"),
        "{}",
        text(&refused.stderr)
    );

    ok(&dir, &["migrate"]);
    assert!(ok(&dir, &["status"]).contains("protocol   0"));
    assert_eq!(rows(&dir), "k\n1\n");
}

#[test]
fn a_catalog_ahead_of_this_binary_asks_for_an_upgrade() {
    let (dir, schema) = lake();
    sql(&dir, "create table t (k int); insert into t values (1)", "csv");
    postgres(&format!(
        "update {schema}.meta set value = '99' where key = 'protocol_version'"
    ));

    // `migrate` cannot help here, and the message must not send anyone to it.
    let refused = run(&dir, &["status"]);
    assert_eq!(refused.status.code(), Some(1));
    let message = text(&refused.stderr);
    assert!(
        message.contains("upgrade") && !message.contains("metalake migrate"),
        "{message}"
    );

    let refused = run(&dir, &["migrate"]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(text(&refused.stderr).contains("newer"), "{}", text(&refused.stderr));

    // It touched nothing.
    postgres(&format!(
        "update {schema}.meta set value = '0' where key = 'protocol_version'"
    ));
    assert_eq!(rows(&dir), "k\n1\n");
}
