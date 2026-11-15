//! Engines that are not PyIceberg: Spark through Iceberg's Java library, whose reader is the strictest, and DuckDB with a
//! reader of its own. They need what `METALAKE_TEST_ENGINES` names, as `protocol.yml` installs, and a local path a Linux
//! client can read.

mod common;

use std::process::Command;
use std::sync::{Mutex, PoisonError};

use common::{Failure, append, create, fresh_schema, local_catalog, server, streamed};
use futures_util::TryStreamExt;
use metalake::Scan;
use metalake::vectors::Deletes;
use serde_json::{Value, json};

fn wanted(engine: &str) -> bool {
    let wanted = std::env::var("METALAKE_TEST_ENGINES").unwrap_or_default();
    wanted.split(',').any(|name| name == engine)
}

/// One engine at a time: two Sparks race for Ivy's cache as they download the same jar, and fail.
static ONE_ENGINE: Mutex<()> = Mutex::new(());

fn run(engine: &str, uri: &str) -> Result<Value, Failure> {
    let _alone = ONE_ENGINE.lock().unwrap_or_else(PoisonError::into_inner);
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
async fn spark_read_partition_vector_branch() -> Result<(), Failure> {
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
async fn spark_insert() -> Result<(), Failure> {
    if !wanted("spark") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("spark_insert", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["rows"], json!([0, 2, 3, 4, 10, 11, 12, 30, 31]));
    Ok(())
}

#[tokio::test]
async fn spark_lifecycle() -> Result<(), Failure> {
    if !wanted("spark") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("spark_lifecycle", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["inserted"], json!([1, 2, 3]));
    assert_eq!(said["deleted"], json!([1, 3]));
    assert_eq!(said["updated"], json!([1, 30]));
    assert_eq!(said["merged"], json!([1, 5, 30]));
    assert_eq!(said["merged_s"], json!(["z"]));
    assert_eq!(said["property"], json!(["data"]));
    assert_eq!(said["created_as"], json!([1, 5]));
    assert_eq!(said["renamed"], json!(["d", "t"]));
    assert_eq!(said["namespaces"], json!(["db"]));
    Ok(())
}

#[tokio::test]
async fn trino_lifecycle() -> Result<(), Failure> {
    if !wanted("trino") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("trino", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["main"], json!([0, 2, 3, 4, 10, 11, 12]));
    assert_eq!(said["branch"], json!([0, 2, 3, 4, 10, 11, 12, 20]));
    assert_eq!(said["pruned"], json!([10, 11, 12]));
    assert_eq!(said["inserted"], json!([0, 2, 3, 4, 10, 11, 12, 30, 31]));
    let refused = said["row_delete"].as_str().unwrap_or_default();
    assert!(refused.contains("copy-on-write"), "the limit is named: {refused}");
    assert_eq!(
        said["after_delete"], said["inserted"],
        "a refused delete changes nothing"
    );
    assert_eq!(said["created_as"], json!([1, 2]), "{said}");
    assert_eq!(said["renamed"], json!(["d", "t"]));
    assert_eq!(said["schemas"], json!(["db", "information_schema", "system"]));
    Ok(())
}

#[tokio::test]
async fn flink_lifecycle() -> Result<(), Failure> {
    if !wanted("flink") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("flink", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["main"], json!([0, 2, 3, 4, 10, 11, 12]));
    assert_eq!(said["branch"], json!([0, 2, 3, 4, 10, 11, 12, 20]));
    assert_eq!(said["pruned"], json!([10, 11, 12]));
    assert_eq!(said["inserted"], json!([0, 2, 3, 4, 10, 11, 12, 30, 31]));
    assert_eq!(said["overwritten"], json!([3]));
    assert_eq!(said["databases"], json!(["db"]));
    Ok(())
}

#[tokio::test]
async fn spark_recovery_repeats_no_batch() -> Result<(), Failure> {
    if !wanted("spark") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("spark_recovery", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["first_again"], json!([1, 2]), "{said}");
    assert_eq!(said["second_again"], json!([1, 2, 3]), "{said}");
    assert_eq!(said["epochs"], json!(["0", "1", "2"]), "an empty batch counts: {said}");
    assert_eq!(said["complete_again"], json!([2]), "{said}");
    assert_eq!(said["complete_next"], json!([3]), "{said}");
    assert_eq!(said["behind_others"], json!([1]), "{said}");
    assert_eq!(said["read_on"], said["source"], "{said}");
    let expired = said["expired"].as_str().unwrap_or_default();
    assert!(expired.contains("expired or removed"), "{said}");
    Ok(())
}

#[tokio::test]
async fn flink_recovery_repeats_no_checkpoint() -> Result<(), Failure> {
    if !wanted("flink") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("flink_recovery", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["rows"], said["expected"], "{said}");
    assert_eq!(said["distinct"], said["expected"], "{said}");
    assert_eq!(said["last"], said["last_row"], "{said}");
    assert_eq!(
        said["restored"], said["committed"],
        "a checkpoint already committed: {said}"
    );
    Ok(())
}

#[tokio::test]
async fn spark_stream_reads_merge_and_specs() -> Result<(), Failure> {
    if !wanted("spark") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = streamed(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("spark_stream", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["merged"], json!([0, 1, 10, 11, 20]), "{said}");
    assert_eq!(said["evolved"], json!([0, 1, 10, 11, 20, 21]), "{said}");
    assert_eq!(said["long"], json!((0..60).collect::<Vec<_>>()), "{said}");
    let refused = said["removed"].as_str().unwrap_or_default();
    assert!(
        refused.contains("overwrite snapshot"),
        "a merge that removes is no append: {said}"
    );
    Ok(())
}

#[tokio::test]
async fn flink_incremental_reads_merge_and_specs() -> Result<(), Failure> {
    if !wanted("flink") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = streamed(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("flink_incremental", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["merged"], json!([10, 11, 20]), "{said}");
    assert_eq!(said["evolved"], json!([10, 11, 20, 21]), "{said}");
    Ok(())
}

#[tokio::test]
async fn duckdb_read_partition_vector() -> Result<(), Failure> {
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

/// DuckDB's insert, which DuckDB 2 commits with `assert-current-schema-id` and its kin: held, not refused (REST-12).
#[tokio::test]
async fn duckdb_insert() -> Result<(), Failure> {
    if !wanted("duckdb") {
        return Ok(());
    }
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = run("duckdb_insert", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["rows"], json!([0, 2, 3, 4, 10, 11, 12, 40, 41]), "{said}");
    Ok(())
}

/// A Trino the lake names, as its faces send it statements (`metalake engine check`, `lake.engine(…)`): both links hold
/// once Trino runs with the lake as its catalog, its rows come back as Arrow, and a catalog it does not have is the
/// second link failing alone.
#[tokio::test]
// One engine at a time, as `run` holds it: the container is named, and the other Trino test takes its port.
#[allow(clippy::await_holding_lock)]
async fn trino_named_engine() -> Result<(), Failure> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use metalake_client::{EngineConfig, EngineKind, Remote};

    if !wanted("trino") {
        return Ok(());
    }
    let _alone = ONE_ENGINE.lock().unwrap_or_else(PoisonError::into_inner);
    let schema = fresh_schema();
    let directory = wide(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let catalog = std::env::temp_dir().join(format!("{schema}_trino"));
    std::fs::create_dir_all(&catalog)?;
    std::fs::write(
        catalog.join("lake.properties"),
        format!(
            "connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri={uri}
fs.hadoop.enabled=true
"
        ),
    )?;
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            "metalake-trino",
            "--network",
            "host",
            "--user",
            "root",
            "-v",
        ])
        .arg(format!("{}:/etc/trino/catalog", catalog.display()))
        .args(["-v", "/tmp:/tmp", "trinodb/trino:484"])
        .output()?;
    assert!(started.status.success(), "{}", String::from_utf8_lossy(&started.stderr));

    let config = |catalog: &str| EngineConfig {
        kind: EngineKind::Trino,
        url: "http://127.0.0.1:8080".into(),
        user: Some("metalake".into()),
        token_env: None,
        password_env: None,
        catalog: Some(catalog.into()),
    };
    let trino = Remote::new("trino_test", &config("lake"))?;
    let outcome = async {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        let mut checked = trino.check().await;
        while !checked.holds() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            checked = trino.check().await;
        }
        assert!(checked.holds(), "{checked:?}");

        let (_, batches) = trino.sql("select k from db.wide order by k").await?;
        let keys: Vec<i32> = batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int32Type>().values().to_vec())
            .collect();
        assert_eq!(keys, [0, 2, 3, 4, 10, 11, 12]);

        let elsewhere = Remote::new("trino_test", &config("elsewhere"))?.check().await;
        assert!(elsewhere.engine.is_ok(), "{elsewhere:?}");
        assert!(elsewhere.lake.is_err(), "{elsewhere:?}");
        Ok::<_, Failure>(())
    }
    .await;
    let _ = Command::new("docker").args(["rm", "-f", "metalake-trino"]).output();
    let _ = std::fs::remove_dir_all(&catalog);
    let _ = std::fs::remove_dir_all(&directory);
    outcome
}
