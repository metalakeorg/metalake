//! C10n: `metalake register` through the binary. A Hive table's directory in the lake's warehouse, written with no
//! field ids and `dt` only in its directories, becomes a table: checked first, then registered, read back with SQL;
//! a run again adds only what is new; a file that is not Parquet makes the run fail, having committed nothing.

mod common;

use std::fs;
use std::path::Path;
use std::sync::Arc;

use common::{lake, ok, run, sql, text};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use serde_json::Value;

/// A Parquet file as Hive writes one, at `path`.
fn hive_file(path: &Path, keys: &[i64]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys.to_vec()))];
    let mut writer = ArrowWriter::try_new(fs::File::create(path).unwrap(), schema.clone(), None).unwrap();
    writer.write(&RecordBatch::try_new(schema, columns).unwrap()).unwrap();
    writer.close().unwrap();
}

fn report(dir: &Path, args: &[&str]) -> Value {
    serde_json::from_str(&ok(dir, &[&["register"], args, &["--format", "json"]].concat())).unwrap()
}

#[test]
fn c10n_register_a_hive_directory() {
    let (dir, _) = lake();
    let status: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();
    let warehouse = status["warehouse"].as_str().unwrap().trim_end_matches('/').to_string();
    let source = format!("{warehouse}/hive/ventas");
    let local = dir.join("data").join("hive").join("ventas");
    hive_file(&local.join("dt=2024-01-02").join("part-0.parquet"), &[1, 2]);
    hive_file(&local.join("dt=2024-01-03").join("part-0.parquet"), &[3]);
    fs::write(local.join("_SUCCESS"), b"").unwrap();
    let args = ["--as", "db.ventas", "--partition", "dt:date"];

    let checked = report(&dir, &[&[source.as_str()][..], &args, &["--check"]].concat());
    assert_eq!(
        (checked["registered"].as_u64(), checked["skipped_hidden"].as_u64()),
        (Some(2), Some(1)),
        "{checked}"
    );
    assert_eq!(checked["commits"], Value::Array(vec![]));

    let first = report(&dir, &[&[source.as_str()][..], &args].concat());
    assert_eq!(
        (first["created"].as_bool(), first["registered"].as_u64()),
        (Some(true), Some(2)),
        "{first}"
    );
    assert_eq!(
        sql(&dir, "select k, dt from db.ventas order by k", "csv"),
        "k,dt\n1,2024-01-02\n2,2024-01-02\n3,2024-01-03\n"
    );

    hive_file(&local.join("dt=2024-01-04").join("part-0.parquet"), &[4]);
    let second = report(&dir, &[&[source.as_str()][..], &args].concat());
    assert_eq!(
        (second["registered"].as_u64(), second["already"].as_u64()),
        (Some(1), Some(2)),
        "{second}"
    );
    assert_eq!(sql(&dir, "select count(*) as n from db.ventas", "csv"), "n\n4\n");

    fs::write(local.join("dt=2024-01-05").join("notes.txt"), b"x").unwrap_or_else(|_| {
        fs::create_dir_all(local.join("dt=2024-01-05")).unwrap();
        fs::write(local.join("dt=2024-01-05").join("notes.txt"), b"x").unwrap();
    });
    let refused = run(
        &dir,
        &[&["register", source.as_str()][..], &args, &["--format", "json"]].concat(),
    );
    assert_eq!(
        refused.status.code(),
        Some(2),
        "a usage error: {}",
        text(&refused.stderr)
    );
    let answer: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(answer["refused"][0]["rule"], "parquet", "{answer}");
    assert_eq!(
        sql(&dir, "select count(*) as n from db.ventas", "csv"),
        "n\n4\n",
        "nothing committed"
    );
}

/// The same verb in SQL, as a user writes it: a path in the store, no partition said, and again with no source.
#[test]
fn c10n_register_in_sql() {
    let (dir, _) = lake();
    let status: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();
    let warehouse = status["warehouse"].as_str().unwrap().trim_end_matches('/').to_string();
    // The store is the machine's files, `file://`: a path in it is what follows.
    let source = format!("{}/hive/pedidos", warehouse.trim_start_matches("file://"));
    let local = dir.join("data").join("hive").join("pedidos");
    hive_file(&local.join("dt=2024-01-02").join("part-0.parquet"), &[1, 2]);
    hive_file(&local.join("dt=2024-01-03").join("part-0.parquet"), &[3]);

    let created = sql(&dir, &format!("REGISTER TABLE db.pedidos FROM '{source}'"), "json");
    let created: Value = serde_json::from_str(created.lines().next().unwrap()).unwrap();
    assert_eq!(
        (
            created["created"].as_bool(),
            created["registered"].as_u64(),
            created["partition"].as_str()
        ),
        (Some(true), Some(2), Some("dt date")),
        "{created}"
    );
    assert_eq!(
        sql(&dir, "select k, dt from db.pedidos order by k", "csv"),
        "k,dt\n1,2024-01-02\n2,2024-01-02\n3,2024-01-03\n"
    );

    // Again, from the command line with no source: what is new, from where it was registered.
    hive_file(&local.join("dt=2024-01-04").join("part-0.parquet"), &[4]);
    let again = report(&dir, &["--as", "db.pedidos"]);
    assert_eq!(
        (again["registered"].as_u64(), again["already"].as_u64()),
        (Some(1), Some(2)),
        "{again}"
    );
    // And in SQL with nothing new: nothing to commit.
    let none = sql(&dir, "register table db.pedidos", "json");
    let none: Value = serde_json::from_str(none.lines().next().unwrap()).unwrap();
    assert_eq!(
        (none["registered"].as_u64(), none["already"].as_u64()),
        (Some(0), Some(3)),
        "{none}"
    );
    assert_eq!(sql(&dir, "select count(*) as n from db.pedidos", "csv"), "n\n4\n");
}

/// A Hive database's directory as a schema, in SQL and through `register-schema`: a row per table, its partition
/// found; checked, nothing committed.
#[test]
fn c10n_register_a_schema() {
    let (dir, _) = lake();
    let status: Value = serde_json::from_str(&ok(&dir, &["status", "--format", "json"])).unwrap();
    let warehouse = status["warehouse"].as_str().unwrap().trim_end_matches('/').to_string();
    let source = format!("{}/hive/curated.db", warehouse.trim_start_matches("file://"));
    let local = dir.join("data").join("hive").join("curated.db");
    hive_file(&local.join("ventas").join("dt=2024-01-02").join("000000_0"), &[1, 2]);
    hive_file(&local.join("clientes").join("000000_0"), &[7]);

    let checked = ok(
        &dir,
        &[
            "register-schema",
            &source,
            "--as",
            "copia",
            "--check",
            "--format",
            "json",
        ],
    );
    let checked: Value = serde_json::from_str(&checked).unwrap();
    assert_eq!(checked["tables"].as_array().unwrap().len(), 2, "{checked}");
    assert_eq!(
        sql(
            &dir,
            "select count(*) as n from information_schema.tables where table_schema = 'copia'",
            "csv"
        ),
        "n\n0\n",
        "a check commits nothing"
    );

    let rows = sql(&dir, &format!("REGISTER SCHEMA curated FROM '{source}'"), "json");
    let rows: Vec<Value> = rows.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    let tables: Vec<&str> = rows.iter().map(|r| r["table"].as_str().unwrap()).collect();
    assert_eq!(
        tables,
        ["default.curated.clientes", "default.curated.ventas"],
        "{rows:?}"
    );
    assert_eq!(rows[1]["partition"], "dt date");
    assert_eq!(sql(&dir, "select count(*) as n from curated.ventas", "csv"), "n\n2\n");
}
