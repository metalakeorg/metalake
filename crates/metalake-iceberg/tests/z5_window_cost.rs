//! Z5: what serving a table's history costs a load. One table with a commit per snapshot, each with the keys Spark leaves
//! in a summary, loaded with windows of several lengths: the first load, which generates the window; a load of what has
//! not changed; and a load after a commit. A measurement: run it with `--ignored`, and `--release` to believe it.

mod common;

use std::process::Command;

use common::{Failure, batch, create, fresh_schema, local_catalog, server};
use metalake::{TableWriter, WriteOptions};
use serde_json::{Value, json};

const SNAPSHOTS: i64 = 10_000;
const WINDOWS: [i32; 4] = [11, 100, 1_000, 10_000];
const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/load_cost.py");

/// What a client takes to load the table `times` times: milliseconds of each, and the bytes of the answer.
fn loads(uri: &str, times: u32) -> Result<Value, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON");
    let run = Command::new(python).args([SCRIPT, uri, &times.to_string()]).output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    Ok(serde_json::from_slice(&run.stdout)?)
}

fn median(said: &Value) -> f64 {
    let mut times: Vec<f64> = said["ms"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_f64)
        .collect();
    times.sort_by(f64::total_cmp);
    times[times.len() / 2]
}

/// The keys a streaming query of Spark leaves in a summary (CMT-12).
fn keys(epoch: i64) -> Vec<(String, String)> {
    [
        (
            "spark.sql.streaming.queryId",
            "9d1f4b5e-3a43-4f7e-8d55-0a5c7f6a2b11".to_string(),
        ),
        ("spark.sql.streaming.epochId", epoch.to_string()),
        ("spark.app.id", "local-1793989113887".to_string()),
        ("app-id", "local-1793989113887".to_string()),
        ("engine-name", "spark".to_string()),
        ("engine-version", "3.5.5".to_string()),
        ("iceberg-version", "Apache Iceberg 1.11.0".to_string()),
    ]
    .map(|(key, value)| (key.to_string(), value))
    .into()
}

#[tokio::test]
#[ignore = "a measurement"]
async fn rest5_load_cost_by_window() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "long", "0", vec![]).await?;

    let append = async |at: i64| -> Result<(), Failure> {
        let t = main.table(&["db"], "long").await?;
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(at..at + 1)).await?;
        let added = writer.finish().await?;
        main.transaction()
            .append(added)
            .summary(t.id(), keys(at))
            .commit()
            .await?;
        Ok(())
    };
    for at in 0..SNAPSHOTS {
        append(at).await?;
    }
    eprintln!("{SNAPSHOTS} commits written");

    let t = main.table(&["db"], "long").await?;
    let location = t.location().trim_start_matches("file:///").to_string();
    let (_server, uri) = server(&schema)?;
    println!("window | first load ms | unchanged ms | after a commit ms | answer KB");
    let mut at = SNAPSHOTS;
    for window in WINDOWS {
        let property = json!({"properties": {"iceberg.served-snapshots": window.to_string()}});
        main.transaction().define(t.id(), property).commit().await?;
        let _ = std::fs::remove_dir_all(format!("{location}/metalake-manifests"));

        let first = loads(&uri, 1)?;
        let unchanged = loads(&uri, 21)?;
        let mut after = Vec::new();
        for _ in 0..7 {
            at += 1;
            append(at).await?;
            after.push(loads(&uri, 1)?["ms"][0].clone());
        }
        println!(
            "{window} | {:.0} | {:.1} | {:.1} | {:.0}",
            median(&first),
            median(&unchanged),
            median(&json!({"ms": after})),
            unchanged["bytes"].as_f64().unwrap_or(0.0) / 1024.0,
        );
    }
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}
