//! C8a (ADR 0003 §7): what maintenance takes away. A branch keeps what it reads; expired objects wait for the grace,
//! then leave storage; a store that fails leaves in the queue what it did not delete, and sweeping again finishes; an
//! object already gone is no error; a Puffin file in the queue is no orphan; an abandoned upload is deleted and a
//! committed file is not; and every task, in batches smaller than its work, comes to nothing left.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{Counting, fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Retention, Scan, Storage, Table, TableWriter, WriteOptions};
use metalake_pg::records::FileAdd;
use metalake_pg::records::VectorSet;
use metalake_pg::{Commit, ErrorKind, Merge, Pool, Protocol};
use object_store::ObjectStoreExt;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// A catalog, its table on a store that counts and can refuse, every file and vector of it stored.
async fn lake(store: Arc<Counting>) -> Result<(Catalog, Table, Storage, Protocol), Error> {
    lake_in(store, &fresh_schema()).await
}

async fn lake_in(store: Arc<Counting>, schema: &str) -> Result<(Catalog, Table, Storage, Protocol), Error> {
    let catalog = Catalog::init(CatalogConfig::new(url(), schema).warehouse("memory:///wh"), None).await?;
    let storage = Storage::from_store("memory:///wh", store)?;
    let main = catalog.branch("main").await?;
    let shape = batch(0..1).schema();
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    Ok((catalog, t, storage, Protocol::new(schema)))
}

async fn files(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    keys: std::ops::Range<i64>,
    options: WriteOptions,
) -> Result<Vec<FileAdd>, Error> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), options)?;
    writer.write(&batch(keys)).await?;
    writer.finish().await
}

async fn append(catalog: &Catalog, t: &Table, storage: &Storage, keys: std::ops::Range<i64>) -> Result<(), Error> {
    let added = files(catalog, t, storage, keys, WriteOptions::default()).await?;
    let on = catalog.branch("main").await?;
    on.transaction().append(added).commit().await?;
    Ok(())
}

async fn keys(catalog: &Catalog, t: &Table, storage: &Storage) -> Result<Vec<i64>, Error> {
    let scan = Scan::new(catalog, t);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

async fn objects(store: &Counting) -> Vec<String> {
    use object_store::ObjectStore;
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    let mut names: Vec<String> = listed.into_iter().map(|m| m.location.to_string()).collect();
    names.sort();
    names
}

async fn file_ids(catalog: &Catalog, t: &Table) -> Result<Vec<i64>, Error> {
    let tasks: Vec<_> = Scan::new(catalog, t).plan().await?.try_collect().await?;
    Ok(tasks.iter().map(|f| f.file_id).collect())
}

fn keep_latest() -> Retention {
    Retention {
        min_snapshots: Some(1),
        ..Default::default()
    }
}

#[tokio::test]
async fn expired_objects_removed_after_grace() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, t, storage, _) = lake(store.clone()).await?;
    let main = catalog.branch("main").await?;
    append(&catalog, &t, &storage, 0..10).await?;
    append(&catalog, &t, &storage, 10..20).await?;
    let t = main.table(&["db"], "t").await?;
    let old = file_ids(&catalog, &t).await?;
    assert_eq!(old.len(), 2);
    let two = objects(&store).await;

    // A branch, then main drops both files for a third.
    let branch = main.create_branch("b").await?;
    let added = files(&catalog, &t, &storage, 100..101, WriteOptions::default()).await?;
    main.transaction()
        .remove(&t, old.clone())
        .append(added)
        .commit()
        .await?;
    let m = main.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &m, &storage).await?, [100]);
    main.set_retention(&keep_latest()).await?;

    // The branch still reads them: expiring leaves their objects out of the queue.
    let maintain = catalog.maintenance().batch(1);
    maintain.expire().await?;
    catalog.set_collect_grace(Duration::ZERO).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(maintain.sweep(&storage).await?, 0);
    let on_branch = branch.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &on_branch, &storage).await?, (0..20).collect::<Vec<_>>());

    // Without the branch they are collectable, in batches smaller than the work; within the grace they stay.
    catalog.set_collect_grace(Duration::from_secs(3600)).await?;
    branch.delete().await?;
    assert!(maintain.expire().await? >= 2, "more rows than a batch");
    assert_eq!(maintain.expire().await?, 0);
    assert_eq!(maintain.sweep(&storage).await?, 0);
    assert_eq!(objects(&store).await.len(), 3);

    // Past the grace, they leave storage; what main reads stays, and a second sweep finds nothing.
    catalog.set_collect_grace(Duration::ZERO).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(maintain.sweep(&storage).await?, 2);
    assert_eq!(maintain.sweep(&storage).await?, 0);
    let left = objects(&store).await;
    assert_eq!(left.len(), 1);
    assert!(!two.contains(&left[0]));
    assert_eq!(keys(&catalog, &m, &storage).await?, [100]);
    Ok(())
}

#[tokio::test]
async fn sweep_retries_failed_ignores_missing() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, t, storage, _) = lake(store.clone()).await?;
    let main = catalog.branch("main").await?;
    for at in 0..4 {
        append(&catalog, &t, &storage, at..at + 1).await?;
    }
    let t = main.table(&["db"], "t").await?;
    let old = file_ids(&catalog, &t).await?;
    main.transaction().remove(&t, old).commit().await?;
    main.set_retention(&keep_latest()).await?;
    let maintain = catalog.maintenance();
    maintain.expire().await?;
    catalog.set_collect_grace(Duration::ZERO).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // One object the store refuses to delete, and one somebody else already deleted.
    let paths: Vec<object_store::path::Path> = objects(&store).await.iter().map(|o| o.as_str().into()).collect();
    assert_eq!(paths.len(), 4);
    store.delete(&paths[0]).await?;
    store.report_missing(vec![paths[0].clone()]);
    store.refuse_deletes(vec![paths[1].clone()]);
    let failed = maintain.sweep(&storage).await;
    assert!(matches!(failed, Err(Error::Storage(_))), "{failed:?}");
    let left = objects(&store).await;
    assert!(left.contains(&paths[1].to_string()), "{left:?}");

    // The store is back: only what was not deleted is left, and it goes.
    store.refuse_deletes(vec![]);
    assert!(maintain.sweep(&storage).await? >= 1);
    assert!(objects(&store).await.is_empty());
    assert_eq!(maintain.sweep(&storage).await?, 0);
    Ok(())
}

#[tokio::test]
async fn orphan_upload_removed() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, t, storage, p) = lake(store.clone()).await?;
    let main = catalog.branch("main").await?;
    let short = || WriteOptions {
        intent_ttl: Duration::from_millis(1),
        ..Default::default()
    };
    append(&catalog, &t, &storage, 0..5).await?;
    let committed = objects(&store).await;
    // Two uploads no commit took, and one whose intent has not expired: its writer may still commit it.
    files(&catalog, &t, &storage, 5..6, short()).await?;
    files(&catalog, &t, &storage, 6..7, short()).await?;
    let waiting = files(&catalog, &t, &storage, 7..8, WriteOptions::default()).await?;
    assert_eq!(objects(&store).await.len(), 4);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // One the store refuses: the other goes, this one is not forgotten, and the next run finishes.
    let before = objects(&store).await;
    let refused: Vec<_> = before
        .iter()
        .filter(|o| !committed.contains(o))
        .map(|o| o.as_str().into())
        .collect();
    store.refuse_deletes(refused);
    let failed = catalog.maintenance().batch(1).orphans(&storage).await;
    assert!(matches!(failed, Err(Error::Storage(_))), "{failed:?}");
    assert_eq!(objects(&store).await.len(), 4);
    store.refuse_deletes(vec![]);

    assert_eq!(catalog.maintenance().batch(1).orphans(&storage).await?, 2);
    assert_eq!(objects(&store).await.len(), 2);
    assert!(objects(&store).await.contains(&committed[0]));
    assert_eq!(catalog.maintenance().orphans(&storage).await?, 0);
    main.transaction().append(waiting).commit().await?;
    let t = main.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &t, &storage).await?, [0, 1, 2, 3, 4, 7]);

    // A vector in Puffin, committed, its file then removed and expired: its object is in the queue, and however its
    // intent stands it waits for the grace, not an orphan until the sweep is done with it.
    let scan = Scan::new(&catalog, &t);
    let mut deletes = Deletes::new();
    let planned: Vec<_> = scan.plan().await?.try_collect().await?;
    let file = planned.iter().find(|f| f.record_count == 5).unwrap().file_id;
    deletes.insert(file, 1);
    main.transaction().delete(&t, &storage, deletes).commit().await?;
    let t = main.table(&["db"], "t").await?;
    main.transaction().remove(&t, [file]).commit().await?;
    main.set_retention(&keep_latest()).await?;
    catalog.maintenance().expire().await?;
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let later = SystemTime::now() + Duration::from_secs(7200);
    assert!(p.orphans(&session, Some(later)).await?.is_empty());
    catalog.set_collect_grace(Duration::ZERO).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        catalog.maintenance().sweep(&storage).await?,
        2,
        "the file and its Puffin"
    );
    assert!(p.orphans(&session, Some(later)).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fill_commit_times_batched() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, t, storage, _) = lake(Arc::new(Counting::default())).await?;
    append(&catalog, &t, &storage, 0..1).await?;
    append(&catalog, &t, &storage, 1..2).await?;
    let maintain = catalog.maintenance().batch(1);
    assert!(maintain.fill_commit_times().await? >= 2, "more commits than a batch");
    assert_eq!(maintain.fill_commit_times().await?, 0);
    Ok(())
}

#[tokio::test]
async fn cmt9_expired_puffin_intent_refused() -> Result<(), Box<dyn std::error::Error>> {
    let (catalog, t, storage, p) = lake(Arc::default()).await?;
    let main = catalog.branch("main").await?;
    append(&catalog, &t, &storage, 0..5).await?;
    let branch = main.create_branch("b").await?;
    let on_branch = branch.table(&["db"], "t").await?;
    let added = files(&catalog, &on_branch, &storage, 5..6, WriteOptions::default()).await?;
    branch.transaction().append(added).commit().await?;
    let t = main.table(&["db"], "t").await?;
    let file = file_ids(&catalog, &t).await?[0];

    let path = "memory:///wh/late.puffin".to_string();
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    p.register_intents(&session, main.id(), t.id(), std::slice::from_ref(&path), Duration::ZERO)
        .await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let vector = VectorSet {
        table_id: t.id(),
        file_id: file,
        base_seq: None,
        path: Some(path),
        content_offset: Some(4),
        content_size: 40,
        cardinality: 1,
        inline: None,
    };
    let refused = |e: &metalake_pg::Error| e.kind() == ErrorKind::Invalid && e.rule() == Some("RET-7");

    let commit = Commit {
        ref_id: main.id(),
        operation: "delete".into(),
        vectors: vec![vector.clone()],
        ..Default::default()
    };
    let answer = p.commit(&session, &commit).await;
    assert!(answer.as_ref().is_err_and(refused), "{answer:?}");
    let merge = Merge {
        vectors: vec![vector],
        ..Merge::of(branch.id())
    };
    let answer = p.merge(&session, &merge).await;
    assert!(answer.as_ref().is_err_and(refused), "{answer:?}");
    Ok(())
}

/// RET-7b: what a client left under a table's location goes through intents; a live file, a file the service
/// generated, a newer object and an intent at work stay; and a writer that commits a path adopted meanwhile is refused.
#[tokio::test]
async fn ret7b_adopt_orphans() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, t, storage, _) = lake(store.clone()).await?;
    let main = catalog.branch("main").await?;
    append(&catalog, &t, &storage, 0..5).await?;
    let t = main.table(&["db"], "t").await?;
    let live = objects(&store).await;
    assert_eq!(live.len(), 1);

    let location = t.location().trim_end_matches('/').to_string();
    let stray = format!("{location}/data/left-by-a-dead-spark.parquet");
    let generated = format!("{location}/metalake-manifests/m-1.avro");
    storage.put(&stray, bytes::Bytes::from_static(b"x")).await?;
    storage.put(&generated, bytes::Bytes::from_static(b"x")).await?;
    let waiting = files(&catalog, &t, &storage, 7..8, WriteOptions::default()).await?;

    // A day's age keeps everything that is new.
    let maintain = catalog.maintenance();
    assert_eq!(maintain.strays(&t, &storage, Duration::from_secs(86400)).await?, 0);

    assert_eq!(
        maintain.strays(&t, &storage, Duration::ZERO).await?,
        1,
        "the stray only"
    );
    assert_eq!(maintain.orphans(&storage).await?, 1);
    let left = objects(&store).await;
    assert_eq!(left.len(), 3, "{left:?}");
    assert!(left.contains(&live[0]));
    assert!(left.iter().any(|o| o.as_str().ends_with("m-1.avro")));
    assert!(
        !left
            .iter()
            .any(|o| o.as_str().ends_with("left-by-a-dead-spark.parquet"))
    );

    // The upload with a live intent was not touched, and its writer commits it.
    main.transaction().append(waiting).commit().await?;
    let t = main.table(&["db"], "t").await?;
    assert_eq!(keys(&catalog, &t, &storage).await?, [0, 1, 2, 3, 4, 7]);

    // A slow writer, whose file was adopted before its commit, is refused, and no row points to nothing.
    let late = files(
        &catalog,
        &t,
        &storage,
        9..10,
        WriteOptions {
            intent_ttl: Duration::ZERO,
            ..Default::default()
        },
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(main.transaction().append(late).commit().await.is_err());
    Ok(())
}

/// A store that spells a path otherwise than the catalog would make a live file look unknown: nothing is adopted.
#[tokio::test]
async fn ret7b_unlisted_live_file_refused() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let (catalog, t, storage, _) = lake(store.clone()).await?;
    let main = catalog.branch("main").await?;
    append(&catalog, &t, &storage, 0..5).await?;
    let t = main.table(&["db"], "t").await?;
    let path = objects(&store).await[0].clone();
    store.delete(&path.as_str().into()).await?;
    let refused = catalog.maintenance().strays(&t, &storage, Duration::ZERO).await;
    assert!(matches!(refused, Err(Error::Location(_))), "{refused:?}");
    Ok(())
}

/// RET-3: the key of an Iceberg commit goes below the floor, with its commit; a native key stays, and keeps its commit.
#[tokio::test]
async fn ret3_iceberg_keys_expire_at_floor() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Counting::default());
    let schema = fresh_schema();
    let (catalog, t, storage, _) = lake_in(store.clone(), &schema).await?;
    let main = catalog.branch("main").await?;
    let added = files(&catalog, &t, &storage, 0..1, WriteOptions::default()).await?;
    main.transaction()
        .append(added)
        .idempotent("iceberg:t:7431", 1)
        .commit()
        .await?;
    let added = files(&catalog, &t, &storage, 1..2, WriteOptions::default()).await?;
    main.transaction()
        .append(added)
        .idempotent("a-native-writer", 1)
        .commit()
        .await?;
    let added = files(&catalog, &t, &storage, 2..3, WriteOptions::default()).await?;
    main.transaction()
        .append(added)
        .idempotent("iceberg:t:7432", 1)
        .commit()
        .await?;

    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let query = format!("select array(select app_id from {schema}.idempotency order by app_id)");
    catalog.maintenance().expire().await?;
    let left: Vec<String> = session.query_one(&query, &[]).await?.get(0);
    assert_eq!(left.len(), 3, "no retention, no floor");

    main.set_retention(&keep_latest()).await?;
    catalog.maintenance().expire().await?;
    let left: Vec<String> = session.query_one(&query, &[]).await?.get(0);
    assert_eq!(left, ["a-native-writer", "iceberg:t:7432"]);
    Ok(())
}
