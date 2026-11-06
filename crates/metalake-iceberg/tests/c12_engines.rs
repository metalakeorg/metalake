//! Engines that are not PyIceberg: Spark through Iceberg's Java library, whose reader is the strictest, and DuckDB with a
//! reader of its own. They need what `METALAKE_TEST_ENGINES` names, as `protocol.yml` installs, and a local path a Linux
//! client can read.

mod common;

use std::process::Command;

use common::{Failure, append, create, fresh_schema, local_catalog, server};
use futures_util::TryStreamExt;
use metalake::Scan;
use metalake::vectors::Deletes;
use serde_json::{Value, json};

fn wanted(engine: &str) -> bool {
    let wanted = std::env::var("METALAKE_TEST_ENGINES").unwrap_or_default();
    wanted.split(',').any(|name| name == engine)
}

fn run(engine: &str, uri: &str) -> Result<Value, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON");
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/engines.py");
    let run = Command::new(python).args([script, engine, uri]).output()?;
    let out = String::from_utf8_lossy(&run.stdout);
    assert!(run.status.success(), "{out}\n{}", String::from_utf8_lossy(&run.stderr));
    Ok(serde_json::from_str(out.lines().last().expect("a line"))?)
}

/// A partitioned table, a vector on `main`, and a branch with one more row.
async fn wide(schema: &str) -> Result<std::path::PathBuf, Failure> {
    let (catalog, storage, directory) = local_catalog(schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "wide", "0", vec![("k", "truncate[10]")]).await?;
    append(&catalog, &main, &storage, "wide", 0..5).await?;
    append(&catalog, &main, &storage, "wide", 10..13).await?;
    let t = main.table(&["db"], "wide").await?;
    let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let mut deletes = Deletes::new();
    deletes.insert(
        planned
            .iter()
            .find(|f| f.record_count == 5)
            .expect("the first file")
            .file_id,
        1,
    );
    main.transaction().delete(&t, &storage, deletes).commit().await?;
    let branch = main.create_branch("b").await?;
    append(&catalog, &branch, &storage, "wide", 20..21).await?;
    catalog.close();
    Ok(directory)
}

#[tokio::test]
async fn spark_reads_a_partition_a_vector_and_a_branch() -> Result<(), Failure> {
    if !wanted("spark") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("spark", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["main"], json!([0, 2, 3, 4, 10, 11, 12]));
    assert_eq!(said["branch"], json!([0, 2, 3, 4, 10, 11, 12, 20]));
    assert_eq!(said["pruned"], json!([10, 11, 12]));
    Ok(())
}

#[tokio::test]
async fn duckdb_reads_a_partition_and_a_vector() -> Result<(), Failure> {
    if !wanted("duckdb") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("duckdb", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["main"], json!([0, 2, 3, 4, 10, 11, 12]));
    assert_eq!(said["pruned"], json!([10, 11, 12]));
    Ok(())
}
