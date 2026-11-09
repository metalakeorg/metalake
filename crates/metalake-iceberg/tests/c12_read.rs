//! Tables read through the server, by a real client, and what it was answered held to the reference: `iceberg_metadata` as
//! a whole (ICE-1), `iceberg_bound` value for value (ICE-3).

mod common;

use std::process::Command;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use common::{Failure, append, create, fresh_schema, local_catalog, server, streamed, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Scan, TableWriter, WriteOptions};
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
    let (older, current) = (&said["entries"][0], &said["entries"][1]);
    assert_eq!(said["entries"].as_array().unwrap().len(), 2);
    assert_eq!(current, &json!(["ADDED", said["current"]]), "what the snapshot added");
    assert_eq!(older[0], "ADDED", "as the snapshot that added it listed it");
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

/// The fields a manifest states as its schema, and its schema's id: what its key-value metadata holds.
fn manifest_schema(path: &std::path::Path) -> Result<(Vec<String>, String), Failure> {
    let reader = apache_avro::Reader::new(std::fs::File::open(path)?)?;
    let meta = reader.user_metadata();
    let schema: Value = serde_json::from_slice(&meta["schema"])?;
    let names = schema["fields"].as_array().unwrap().iter();
    let names = names.map(|f| f["name"].as_str().unwrap().to_string()).collect();
    Ok((names, String::from_utf8(meta["schema-id"].clone())?))
}

#[tokio::test]
async fn rest6_manifest_schema_matches_snapshot() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "grown", "0", vec![]).await?;
    append(&catalog, &main, &storage, "grown", 0..2).await?;
    let before = main.table(&["db"], "grown").await?.snapshot_id().unwrap();

    let t = main.table(&["db"], "grown").await?;
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
    let t = main.table(&["db"], "grown").await?;
    let fields = vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![10, 11])),
        Arc::new(Int64Array::from(vec![Some(1), None])),
    ];
    let grown = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&grown).await?;
    main.transaction().append(writer.finish().await?).commit().await?;
    let after = main.table(&["db"], "grown").await?.snapshot_id().unwrap();

    // one load generates both, the first on the schema it was written with
    let (_server, uri) = server(&schema)?;
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON");
    let load = format!("import urllib.request; urllib.request.urlopen('{uri}/v1/namespaces/db/tables/grown').read()");
    assert!(Command::new(python).args(["-c", &load]).status()?.success());
    let generated = directory
        .join("db")
        .read_dir()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("grown"));
    let generated = generated
        .expect("the table's directory")
        .path()
        .join("metalake-manifests");
    let (first, first_id) = manifest_schema(&generated.join(format!("a-{before}-0.avro")))?;
    let (last, last_id) = manifest_schema(&generated.join(format!("a-{after}-0.avro")))?;
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(first, ["k"]);
    assert_eq!(last, ["k", "v"]);
    assert_ne!(first_id, last_id);
    Ok(())
}

#[tokio::test]
async fn ice2_added_files_manifest_apart() -> Result<(), Failure> {
    let schema = fresh_schema();
    let directory = streamed(&schema).await?;
    let (_server, uri) = server(&schema)?;
    let said = client("manifests", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    let expected = json!([[0, 1, 0, false], [1, 1, 0, false], [1, 1, 0, true]]);
    assert_eq!(said["manifests"], expected, "{said}");
    assert_eq!(said["merged"], json!([0, 1, 10, 11, 20]), "{said}");
    assert_eq!(
        said["removed"],
        json!([]),
        "a merge that removes is written whole: {said}"
    );
    assert_eq!(said["evolved"], json!([0, 1, 10, 11, 20, 21]), "{said}");
    Ok(())
}

#[tokio::test]
async fn ice2_manifests_roll_up() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, _storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "rolled", "0", vec![]).await?;

    let (_server, uri) = server(&schema)?;
    let said = client("rolled", &uri)?;
    let _ = std::fs::remove_dir_all(&directory);
    let mut counts = vec![json!([0, 50]), json!([0, 50])];
    counts.splice(0..0, std::iter::repeat_n(json!([1, 0]), 5));
    counts.sort_by_key(ToString::to_string);
    assert_eq!(said["counts"], json!(counts), "the last five, and two of fifty: {said}");
    assert_eq!(said["statuses"], json!(["EXISTING"]), "{said}");
    assert_eq!(
        said["sequences"], 100,
        "each entry keeps the sequence that added it: {said}"
    );
    assert_eq!(said["rows"], json!((0..105).collect::<Vec<_>>()));
    assert_eq!(
        said["before"],
        json!((0..99).collect::<Vec<_>>()),
        "a list stays as it was"
    );
    Ok(())
}

/// The keys of `t` as MetaLake reads it, sorted.
async fn keys_of(catalog: &Catalog, storage: &metalake::Storage, t: &metalake::Table) -> Result<Vec<i64>, Failure> {
    let scan = Scan::new(catalog, t);
    let read: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let column = |b: &RecordBatch| {
        b.column_by_name("k")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone()
    };
    let mut keys: Vec<i64> = read.iter().flat_map(|b| column(b).values().to_vec()).collect();
    keys.sort_unstable();
    Ok(keys)
}

#[tokio::test]
async fn tt3_pyiceberg_as_of_time_matches_metalake() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0", vec![]).await?;
    for keys in [0..5, 100..103] {
        // Commits a millisecond apart or more, so that each has a time of its own in the log.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        append(&catalog, &main, &storage, "plain", keys).await?;
    }

    let (_server, uri) = server(&schema)?;
    let said = client("as_of", &uri)?;
    let said = said.as_array().unwrap();
    assert_eq!(
        said.len(),
        6,
        "the creation and two appends, each at its time and just before: {said:?}"
    );
    assert_eq!(said[0]["snapshot"], Value::Null, "before the table, no snapshot");
    for at in said {
        let time = std::time::UNIX_EPOCH + std::time::Duration::from_millis(at["at"].as_u64().unwrap());
        let then = match main.as_of(time).table(&["db"], "plain").await {
            Ok(t) => Some((t.snapshot_id(), keys_of(&catalog, &storage, &t).await?)),
            Err(metalake::Error::NotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };
        let resolved = at["snapshot"]
            .as_i64()
            .map(|id| (Some(id), serde_json::from_value(at["rows"].clone()).unwrap()));
        assert_eq!(resolved, then, "as of {}", at["at"]);
    }
    let _ = std::fs::remove_dir_all(&directory);
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
    // The commit read no file of the client's, so its footer's size is not known (STA-1); MetaLake reads it all the
    // same, and the server keeps its footer once it has answered (FTR-1), which leaves maintenance nothing.
    let unknown: Vec<_> = planned.iter().filter(|f| f.footer_size == 0).collect();
    assert_eq!(unknown.len(), 1, "the client's file");
    let scan = Scan::new(&catalog, &t);
    let read: Vec<_> = scan.read(scan.plan().await?, &storage).try_collect().await?;
    assert_eq!(read.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
    let mut kept = false;
    for _ in 0..100 {
        let now: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
        kept = now.iter().all(|f| f.footer.is_some() || f.content.is_some());
        if kept {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        kept,
        "every file reads with no footer request, within seconds of the commit"
    );
    assert_eq!(catalog.maintenance().footers(&t, &storage).await?, 0);
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

/// The objects a table's generation wrote that an overwrite need not touch, each with when it was last written.
fn derived(directory: &std::path::Path) -> std::collections::BTreeMap<String, std::time::SystemTime> {
    fn walk(dir: &std::path::Path, found: &mut std::collections::BTreeMap<String, std::time::SystemTime>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else {
                let name = path.display().to_string().replace('\\', "/");
                if name.contains("/metalake-inline/") || name.contains("/metalake-deletes/") {
                    found.insert(name, entry.metadata().and_then(|m| m.modified()).unwrap());
                }
            }
        }
    }
    let mut found = std::collections::BTreeMap::new();
    walk(directory, &mut found);
    found
}

/// Deletes the row of key `k`, as an engine does: by its file and position.
async fn delete_key(catalog: &Catalog, on: &metalake::Ref, storage: &metalake::Storage, k: i64) -> Result<(), Failure> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use metalake::vectors::{FILE_ID, POSITION};

    let t = on.table(&["db"], "plain").await?;
    let scan = Scan::new(catalog, &t).with_positions();
    let batches: Vec<arrow_array::RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut deletes = Deletes::new();
    for b in &batches {
        let column = |name: &str| b.column_by_name(name).unwrap().as_primitive::<Int64Type>().clone();
        let (keys, files, positions) = (column("k"), column(FILE_ID), column(POSITION));
        for i in (0..b.num_rows()).filter(|i| keys.value(*i) == k) {
            deletes.insert(files.value(i), positions.value(i) as u64);
        }
    }
    on.transaction().delete(&t, storage, deletes).commit().await?;
    Ok(())
}

#[tokio::test]
async fn ice4_overwrite_writes_only_new_objects() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "65536", vec![]).await?;
    // four inline files, and rows deleted in two of them
    for keys in [0..3, 3..6, 6..9, 9..12] {
        append(&catalog, &main, &storage, "plain", keys).await?;
    }
    delete_key(&catalog, &main, &storage, 1).await?;
    delete_key(&catalog, &main, &storage, 4).await?;

    let (_server, uri) = server(&schema)?;
    let first = client("rows", &uri)?;
    let before = derived(&directory);

    // one more row deleted, in a third file: only its delete file is new, and nothing else is written again
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    delete_key(&catalog, &main, &storage, 7).await?;
    let second = client("rows", &uri)?;
    let after = derived(&directory);
    let _ = std::fs::remove_dir_all(&directory);

    assert_eq!(first["rows"], json!([0, 2, 3, 5, 6, 7, 8, 9, 10, 11]));
    assert_eq!(second["rows"], json!([0, 2, 3, 5, 6, 8, 9, 10, 11]));
    let deletes = |objects: &std::collections::BTreeMap<String, std::time::SystemTime>| {
        objects.keys().filter(|k| k.contains("/metalake-deletes/")).count()
    };
    assert_eq!(
        (deletes(&before), before.len() - deletes(&before)),
        (2, 4),
        "two vectors, four inline files"
    );
    assert_eq!(deletes(&after), 3, "the new vector's delete file");
    let rewritten: Vec<&String> = before
        .iter()
        .filter(|(k, at)| after.get(*k) != Some(at))
        .map(|(k, _)| k)
        .collect();
    assert!(rewritten.is_empty(), "written again: {rewritten:?}");
    Ok(())
}

/// A `loadTable`, asked as any client asks it.
fn load_table(uri: &str, name: &str) -> Result<(), Failure> {
    use std::io::{Read, Write};
    let address = uri.trim_start_matches("http://");
    let mut stream = std::net::TcpStream::connect(address)?;
    let request =
        format!("GET /v1/namespaces/db/tables/{name} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    assert!(
        answer.starts_with("HTTP/1.1 200"),
        "{}",
        &answer[..answer.len().min(300)]
    );
    Ok(())
}

/// What a table's manifests and lists hold, by name: their key-value metadata and their records. An Avro file's sync
/// marker is drawn anew each time it is written, so their bytes are not what they hold.
fn manifests_of(directory: &std::path::Path) -> Result<std::collections::BTreeMap<String, String>, Failure> {
    let table = directory
        .join("db")
        .read_dir()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("plain"));
    let mut found = std::collections::BTreeMap::new();
    for entry in table
        .expect("the table's directory")
        .path()
        .join("metalake-manifests")
        .read_dir()?
        .flatten()
    {
        let reader = apache_avro::Reader::new(std::fs::File::open(entry.path())?)?;
        let meta: std::collections::BTreeMap<_, _> = reader.user_metadata().clone().into_iter().collect();
        let records = reader.collect::<Result<Vec<_>, _>>()?;
        found.insert(
            entry.file_name().to_string_lossy().into_owned(),
            format!("{meta:?} {records:?}"),
        );
    }
    Ok(found)
}

/// A history generated at once writes what it writes generated a snapshot at a time, byte for byte: appends past a roll-up,
/// inline files, a delete, and a branch with its own chain.
#[tokio::test]
async fn rest6_batch_generation_matches_incremental() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "65536", vec![]).await?;
    let (_server, uri) = server(&schema)?;
    load_table(&uri, "plain")?;
    for k in 0..100 {
        append(&catalog, &main, &storage, "plain", k..k + 1).await?;
        load_table(&uri, "plain")?;
        if k == 30 {
            delete_key(&catalog, &main, &storage, 30).await?;
            load_table(&uri, "plain")?;
        }
        if k == 40 {
            let branch = main.create_branch("b").await?;
            for j in 100..103 {
                append(&catalog, &branch, &storage, "plain", j..j + 1).await?;
                load_table(&uri, "plain")?;
            }
        }
    }
    let one_at_a_time = manifests_of(&directory)?;

    let table = directory
        .join("db")
        .read_dir()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("plain"));
    let table = table.expect("the table's directory").path();
    std::fs::remove_dir_all(table.join("metalake-manifests"))?;
    std::fs::remove_dir_all(table.join("metalake-metadata"))?;
    load_table(&uri, "plain")?;
    let at_once = manifests_of(&directory)?;
    let _ = std::fs::remove_dir_all(&directory);

    assert!(at_once.keys().any(|name| name.starts_with('r')), "a roll-up among them");
    assert_eq!(
        one_at_a_time.keys().collect::<Vec<_>>(),
        at_once.keys().collect::<Vec<_>>()
    );
    let differ: Vec<_> = at_once
        .iter()
        .filter(|(name, bytes)| one_at_a_time[*name] != **bytes)
        .map(|(n, _)| n)
        .collect();
    assert!(differ.is_empty(), "written otherwise: {differ:?}");
    Ok(())
}
