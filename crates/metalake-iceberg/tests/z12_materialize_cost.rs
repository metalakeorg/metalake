//! Z12: what materializing on commit costs and gives. One table of a thousand commits, read by a client first unmarked,
//! as REST-6 generates it on its first load, then marked, once the worker has generated it; and then the lag of each new
//! commit, from the commit to its metadata file, with no client loading. A measurement: run it with `--ignored`, and
//! `--release` to believe it.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Failure, append, create, fresh_schema, local_catalog, server};
use metalake::{Catalog, Ref, Storage};
use serde_json::{Value, json};

const SNAPSHOTS: i64 = 1_000;
/// Commits whose lag is measured.
const LAGS: usize = 20;
const KEY: &str = "metalake.iceberg.metadata";
const SCRIPT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/load_cost.py");

/// What a client takes to load `db.long` once, in milliseconds.
fn load_ms(uri: &str) -> Result<f64, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON");
    let run = Command::new(python).args([SCRIPT, uri, "1"]).output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    let said: Value = serde_json::from_slice(&run.stdout)?;
    Ok(said["ms"][0].as_f64().unwrap_or(f64::NAN))
}

fn table_dir(directory: &Path) -> PathBuf {
    let found = std::fs::read_dir(directory.join("db"))
        .into_iter()
        .flatten()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("long-"));
    found.expect("the table's directory").path()
}

/// Whether commit `seq` is materialized: its list, and a metadata file that serves it.
fn materialized(table: &Path, seq: i64) -> bool {
    let listed = table
        .join("metalake-manifests")
        .join(format!("snap-{seq}.avro"))
        .exists();
    listed
        && std::fs::read_dir(table.join("metalake-metadata"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.strip_prefix('v')?.split('-').next()?.parse::<i64>().ok()
            })
            .any(|served| served >= seq)
}

/// Waits for commit `seq` to be materialized, polling, and answers how long it took.
async fn until_materialized(table: &Path, seq: i64) -> Duration {
    let started = Instant::now();
    while !materialized(table, seq) {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "commit {seq} not materialized"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    started.elapsed()
}

async fn commit(catalog: &Catalog, main: &Ref, storage: &Storage, k: i64) -> Result<i64, Failure> {
    append(catalog, main, storage, "long", k..k + 1).await?;
    Ok(main.table(&["db"], "long").await?.snapshot_id().expect("a snapshot"))
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[tokio::test]
#[ignore = "a measurement"]
async fn rest6_materialize_cost() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "long", "0", vec![]).await?;
    let mut last = 0;
    for k in 0..SNAPSHOTS {
        last = commit(&catalog, &main, &storage, k).await?;
    }
    let table = table_dir(&directory);

    // Not marked: the first load generates the history.
    let unmarked = {
        let (_server, uri) = server(&schema)?;
        load_ms(&uri)?
    };
    let _ = std::fs::remove_dir_all(table.join("metalake-manifests"));
    let _ = std::fs::remove_dir_all(table.join("metalake-metadata"));

    // Marked: the worker generates it when it starts, and a client's first load finds it there.
    let t = main.table(&["db"], "long").await?;
    main.transaction()
        .define(t.id(), json!({"properties": {KEY: "on-commit"}}))
        .commit()
        .await?;
    let marked = main.table(&["db"], "long").await?.snapshot_id().expect("a snapshot");
    let (_server, uri) = server(&schema)?;
    let backfill = until_materialized(&table, marked.max(last)).await;
    let marked_load = load_ms(&uri)?;

    // The lag of each new commit, with no client loading, and what the commit itself takes.
    let (mut lags, mut commits) = (Vec::new(), Vec::new());
    for k in 0..LAGS as i64 {
        let started = Instant::now();
        let seq = commit(&catalog, &main, &storage, SNAPSHOTS + k).await?;
        commits.push(started.elapsed().as_secs_f64() * 1000.0);
        lags.push(until_materialized(&table, seq).await.as_secs_f64() * 1000.0);
    }
    let _ = std::fs::remove_dir_all(&directory);

    println!("{SNAPSHOTS} snapshots, local disk");
    println!("  first load, not marked:        {unmarked:>8.0} ms");
    println!(
        "  generated by the worker in:    {:>8.0} ms, from its start",
        backfill.as_secs_f64() * 1000.0
    );
    println!("  first load, marked:            {marked_load:>8.0} ms");
    println!(
        "  lag of a commit, {LAGS} commits:   median {:>6.0} ms, max {:>6.0} ms",
        median(lags.clone()),
        lags.iter().copied().fold(0.0, f64::max)
    );
    println!("  the commit itself:             median {:>6.1} ms", median(commits));
    Ok(())
}
