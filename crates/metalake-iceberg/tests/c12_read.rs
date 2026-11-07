//! Tables read through the server, by a real client, and what it was answered held to the reference: `iceberg_metadata` as
//! a whole (ICE-1), `iceberg_bound` value for value (ICE-3).

mod common;

use std::process::Command;

use common::{Failure, append, create, fresh_schema, local_catalog, server, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Scan};
use metalake_iceberg::project::bounds::{from_iceberg, to_iceberg};
use metalake_pg::{Commit, Pool, Protocol};
use serde_json::{Value, json};

fn client(step: &str, uri: &str) -> Result<Value, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON: a Python with pyiceberg");
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/client.py");
    let run = Command::new(python).args([script, step, uri]).output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    Ok(serde_json::from_slice(&run.stdout)?)
}

/// Each snapshot's `manifest-list` is the server's to name, as the reference leaves it to who writes the files, and the
/// write modes are the server's to serve (REST-4).
fn as_the_reference_has_it(mut metadata: Value) -> Value {
    for snapshot in metadata["snapshots"].as_array_mut().unwrap() {
        snapshot.as_object_mut().unwrap().remove("manifest-list");
    }
    for mode in ["write.delete.mode", "write.update.mode", "write.merge.mode"] {
        let served = metadata["properties"].as_object_mut().unwrap().remove(mode);
        assert_eq!(served, Some(json!("copy-on-write")));
    }
    metadata
}

#[tokio::test]
async fn ice1_pyiceberg_load_matches_reference() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0", vec![]).await?;
    create(&main, "small", "1000000", vec![]).await?;
    append(&catalog, &main, &storage, "plain", 0..5).await?;
    append(&catalog, &main, &storage, "plain", 100..105).await?;
    append(&catalog, &main, &storage, "small", 0..3).await?;

    let (_server, uri) = server(&schema)?;
    let said = client("read", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["rows"], json!([0, 1, 2, 3, 4, 100, 101, 102, 103, 104]));
    assert_eq!(said["planned"], 2);
    assert_eq!(said["pruned"], 1, "the bounds of the manifest prune");
    assert_eq!(said["small"], 3, "an inline file is materialized");
    assert!(said["nowhere"].as_str().unwrap().contains("nowhere"), "{said}");
    let (current, older) = (&said["entries"][0], &said["entries"][1]);
    assert_eq!(said["entries"].as_array().unwrap().len(), 2);
    assert_eq!(current, &json!(["ADDED", said["current"]]), "what the snapshot added");
    assert_eq!(older[0], "EXISTING");
    assert!(older[1].as_i64() < said["current"].as_i64());

    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let reference = include_str!("../../../spec/v0/reference/16-iceberg.sql");
    session
        .batch_execute(&format!("set search_path = {schema}; {reference}"))
        .await?;
    let t = main.table(&["db"], "plain").await?;
    let expected: Value = session
        .query_one("select iceberg_metadata($1)", &[&t.id()])
        .await?
        .get(0);
    assert_eq!(as_the_reference_has_it(said["metadata"].clone()), expected);
    Ok(())
}

#[tokio::test]
async fn pyiceberg_read_partitions_vectors_branch_schemas() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
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

    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let protocol = Protocol::new(&schema);
    let id = protocol.next_id(&session, t.id(), "field").await?;
    let added = json!({"fields": [{"field_id": id, "new": true, "parent_field_id": null, "position": 2, "name": "v",
                                   "type": "long", "required": false}]});
    let change = Commit {
        ref_id: main.id(),
        operation: "evolve".into(),
        changes: Some(json!({t.id().to_string(): added})),
        ..Default::default()
    };
    protocol.commit(&session, &change).await?;

    let (_server, uri) = server(&schema)?;
    let said = client("wide", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["main"], json!([0, 2, 3, 4, 10, 11, 12]), "the vector applies");
    assert_eq!(said["branch"], json!([0, 2, 3, 4, 10, 11, 12, 20]));
    assert_eq!(said["files"], 2);
    assert_eq!(said["pruned"], 1, "the partition or the bounds prune");
    assert_eq!(said["deletes"], json!([0, 1]));
    assert_eq!(said["schemas"], 2);
    assert_eq!(said["columns"], json!(["k", "v"]));

    let reference = include_str!("../../../spec/v0/reference/16-iceberg.sql");
    session
        .batch_execute(&format!("set search_path = {schema}; {reference}"))
        .await?;
    let expected: Value = session
        .query_one("select iceberg_metadata($1)", &[&t.id()])
        .await?
        .get(0);
    assert_eq!(as_the_reference_has_it(said["metadata"].clone()), expected);
    Ok(())
}

#[tokio::test]
async fn ice6_pyiceberg_append() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0", vec![]).await?;
    append(&catalog, &main, &storage, "plain", 0..3).await?;

    let (_server, uri) = server(&schema)?;
    let said = client("append", &uri)?;
    let t = main.table(&["db"], "plain").await?;
    let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["rows"], json!([0, 1, 2, 7, 8]));
    assert_eq!(
        said["after"],
        t.snapshot_id().map_or(json!(null), |id| json!(id)),
        "MetaLake's id"
    );
    assert_eq!(said["reread"], said["after"]);
    assert_eq!(said["kept"], json!({"job": "7", "total-records": null}), "CMT-12");
    assert_eq!(said["metadata_exists"], true, "an answer names only files that exist");
    assert!(said["after"].as_i64() > said["before"].as_i64());
    assert_eq!(planned.iter().map(|f| f.record_count).sum::<i64>(), 5);
    Ok(())
}

#[tokio::test]
async fn rest3_conflict_rebase_and_repeat() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0", vec![]).await?;
    append(&catalog, &main, &storage, "plain", 0..3).await?;

    let (_server, uri) = server(&schema)?;
    let said = client("retry", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["statuses"], json!([409, 200]), "the client rebased: {said}");
    assert_eq!(said["same_id"], true, "under the snapshot id it proposed");
    assert_eq!(said["repeated"], 200, "{said}");
    assert_eq!(said["unchanged"], true, "a repeat commits nothing");
    assert_eq!(said["stale"], 200, "the key answers before the expected snapshot");
    assert_eq!(said["stale_unchanged"], true);
    assert_eq!(said["conflicts"], json!([409, 409]), "a refused commit leaves no key");
    assert_eq!(said["rows"], json!([0, 1, 2, 10, 11, 12]));
    Ok(())
}

#[tokio::test]
async fn pyiceberg_lifecycle() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, _storage, directory) = local_catalog(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = client("lifecycle", &uri)?;
    catalog.close();
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(said["namespace_exists"], true);
    assert_eq!(said["namespace_missing"], false);
    assert_eq!(said["namespace_summary"], json!([["owner"], ["team"], []]));
    assert_eq!(said["namespace_properties"], json!({"team": "b"}));
    assert_eq!(said["namespace_again"], "refused");
    assert_eq!(said["table_exists"], true);
    assert_eq!(said["table_again"], "refused");
    assert_eq!(said["appended"], json!([1, 2, 3]));
    assert_eq!(said["overwritten"], json!([1, 3, 7, 8]));
    assert_eq!(said["deleted"], json!([1, 7, 8]));
    assert_eq!(said["replaced"], json!([9]));
    assert_eq!(said["properties"], json!({"b": "2"}));
    assert_eq!(said["staged"], json!([5]));
    assert_eq!(said["renamed"], json!(["moved", "t"]));
    assert_eq!(said["moved"], json!([5]));
    assert_eq!(said["transaction"], 204);
    assert_eq!(said["both"], json!(["yes", "yes"]));
    assert_eq!(said["not_empty"], "refused");
    assert_eq!(said["dropped"], false);
    assert_eq!(said["namespace_dropped"], false);
    Ok(())
}

#[tokio::test]
async fn ice3_bounds_match_reference() -> Result<(), Failure> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let reference = include_str!("../../../spec/v0/reference/16-iceberg.sql");
    session
        .batch_execute(&format!("set search_path = {schema}; {reference}"))
        .await?;
    for (ty, value) in [
        ("boolean", "true"),
        ("boolean", "false"),
        ("int", "0"),
        ("int", "-5"),
        ("int", "2147483647"),
        ("long", "-9000000000"),
        ("long", "9223372036854775807"),
        ("date", "2024-02-29"),
        ("date", "1969-12-31"),
        ("float", "1.5"),
        ("float", "-0.25"),
        ("double", "-1e300"),
        ("double", "3.25"),
        ("decimal(10,2)", "-123.45"),
        ("decimal(10,2)", "1.28"),
        ("decimal(38,0)", "12345678901234567890123456789012345678"),
        ("string", "héllo"),
        ("uuid", "f79c3e09-677c-4bbd-a479-3f349cb785e7"),
        ("time", "12:30:00"),
        ("timestamp", "2024-02-29T12:30:00"),
        ("timestamp", "1969-12-31T23:59:59.999999"),
        ("timestamptz", "2024-02-29T12:30:00+00:00"),
        ("decimal(38,0)", "-12345678901234567890123456789012345678"),
        ("double", "-0.0"),
        ("long", "-9223372036854775808"),
    ] {
        let row = session
            .query_one(
                "select b, iceberg_bound($1, b), iceberg_bound_in($1, iceberg_bound($1, b)) from encode_bound($1, $2) b",
                &[&ty, &value],
            )
            .await?;
        let (stored, served, received): (Vec<u8>, Vec<u8>, Vec<u8>) = (row.get(0), row.get(1), row.get(2));
        assert_eq!(to_iceberg(ty, &stored).as_ref(), Some(&served), "{ty} {value}");
        assert_eq!(from_iceberg(ty, &served), Some(received), "{ty} {value}");
        assert_eq!(from_iceberg(ty, &served), Some(stored), "{ty} {value}");
    }
    catalog.close();
    Ok(())
}
