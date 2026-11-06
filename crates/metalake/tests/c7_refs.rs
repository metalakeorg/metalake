//! C7a (ADR 0003 §7): refs and time in the core. A history (a table written, renamed, a second created, one commit
//! writing both, the second dropped) is read as of a time between each step: the tables then, by their names then,
//! with their rows then, and the commit that wrote both in both or in neither. A tag names a past state and takes no
//! commit; a branch from the past starts from it; a read in the past takes no commit; and a deleted ref is gone.

mod common;

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, Storage, Table, TableWriter, WriteOptions};
use metalake_pg::records::FileAdd;
use metalake_pg::{Commit, ErrorKind, Pool, Protocol};
use serde_json::json;

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.map(|k| format!("s{k}")))),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn new_table<'a>(name: &'a str, schema: &'a Schema) -> NewTable<'a> {
    NewTable {
        namespace: &["db"],
        name,
        schema,
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    }
}

async fn files(
    catalog: &Catalog,
    t: &Table,
    storage: &Storage,
    keys: std::ops::Range<i64>,
) -> Result<Vec<FileAdd>, Error> {
    let mut writer = TableWriter::new(catalog, t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    writer.finish().await
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

async fn names(on: &Ref) -> Result<Vec<String>, Error> {
    Ok(on.tables().await?.into_iter().map(|t| t.name).collect())
}

#[tokio::test]
async fn time_travel_by_time_tag_branch() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let storage = Storage::in_memory();
    let pool = Pool::new(&url(), 1)?;
    let session = pool.get().await?;
    let p = Protocol::new(&schema);
    // Each time is a commit's own, as Postgres stamped it: a read as of it sees that commit and not the next.
    let time = |seq: i64| p.commit_time(&session, seq);
    let shape = batch(0..1).schema();

    // a, with 10 rows.
    let (tx, a_id) = main
        .transaction()
        .create_namespace(&["db"])
        .create_table(new_table("a", &shape))?;
    tx.commit().await?;
    let a = main.table(&["db"], "a").await?;
    let first = main
        .transaction()
        .append(files(&catalog, &a, &storage, 0..10).await?)
        .commit()
        .await?
        .seq();
    let t1 = time(first).await?;

    // a renamed b; c created; one commit writing both.
    p.commit(
        &session,
        &Commit {
            ref_id: main.id(),
            operation: "update".into(),
            changes: Some(json!({ a_id.to_string(): {"rename": {"namespace": ["db"], "name": "b"}} })),
            ..Default::default()
        },
    )
    .await?;
    let (tx, c_id) = main.transaction().create_table(new_table("c", &shape))?;
    tx.commit().await?;
    let (b, c) = (main.table(&["db"], "b").await?, main.table(&["db"], "c").await?);
    let both = [
        files(&catalog, &b, &storage, 10..20).await?,
        files(&catalog, &c, &storage, 100..105).await?,
    ]
    .concat();
    let wrote_both = main.transaction().append(both).commit().await?.seq();
    let t2 = time(wrote_both).await?;

    // c dropped.
    let dropped = p
        .commit(
            &session,
            &Commit {
                ref_id: main.id(),
                operation: "update".into(),
                changes: Some(json!({ c_id.to_string(): {"drop": true} })),
                ..Default::default()
            },
        )
        .await?;
    let t3 = time(dropped.commit_seq).await?;
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // As of each time: the tables then, by their names then, and their rows then.
    let then = main.as_of(t1);
    assert_eq!(names(&then).await?, ["a"]);
    assert_eq!(
        keys(&catalog, &then.table(&["db"], "a").await?, &storage).await?,
        (0..10).collect::<Vec<_>>()
    );
    assert!(matches!(then.table(&["db"], "b").await, Err(Error::NotFound(_))));
    let then = main.as_of(t2);
    assert_eq!(names(&then).await?, ["b", "c"]);
    let (b2, c2) = (then.table(&["db"], "b").await?, then.table(&["db"], "c").await?);
    assert_eq!(keys(&catalog, &b2, &storage).await?, (0..20).collect::<Vec<_>>());
    assert_eq!(keys(&catalog, &c2, &storage).await?, (100..105).collect::<Vec<_>>());
    assert_eq!(p.as_of_time(&session, main.id(), a_id, t2).await?, Some(b2.seq()));
    assert_eq!(names(&main.as_of(t3)).await?, ["b"]);
    assert!(matches!(then.read_point().await, Err(Error::Invalid(_))));
    // Now is less than a second ago (TT-3).
    let refused = main
        .as_of(SystemTime::now() + std::time::Duration::from_secs(60))
        .tables()
        .await;
    assert!(
        matches!(&refused, Err(Error::Protocol(e)) if e.kind() == ErrorKind::Invalid),
        "{refused:?}"
    );

    // A tag of the state after the first write: a and its 10 rows, for good; it takes no commit.
    let v1 = main.at(first).create_tag("v1").await?;
    assert_eq!(v1.kind(), "tag");
    let tagged = v1.table(&["db"], "a").await?;
    assert_eq!(keys(&catalog, &tagged, &storage).await?, (0..10).collect::<Vec<_>>());
    let refused = v1
        .transaction()
        .append(files(&catalog, &tagged, &storage, 50..51).await?)
        .commit()
        .await;
    assert!(matches!(refused, Err(Error::Invalid(m)) if m.contains("tag")));
    // Nor does a read in the past, nor can a time be a ref's start.
    let refused = main.at(first).transaction().append(vec![]).commit().await;
    assert!(matches!(refused, Err(Error::Invalid(m)) if m.contains("past")));
    assert!(matches!(
        main.as_of(t1).create_branch("x").await,
        Err(Error::Invalid(_))
    ));

    // A branch from the past: it starts with a as it was, and writes it there only.
    let old = main.at(first).create_branch("old").await?;
    let on_old = old.table(&["db"], "a").await?;
    old.transaction()
        .append(files(&catalog, &on_old, &storage, 1000..1001).await?)
        .commit()
        .await?;
    let on_old = old.table(&["db"], "a").await?;
    let mut want: Vec<i64> = (0..10).collect();
    want.push(1000);
    assert_eq!(keys(&catalog, &on_old, &storage).await?, want);
    assert_eq!(
        keys(&catalog, &main.table(&["db"], "b").await?, &storage).await?,
        (0..20).collect::<Vec<_>>()
    );

    // The branch renames its a: as of that time, the branch's own name, not its parent's.
    let renamed = p
        .commit(
            &session,
            &Commit {
                ref_id: old.id(),
                operation: "update".into(),
                changes: Some(json!({ a_id.to_string(): {"rename": {"namespace": ["db"], "name": "a_old"}} })),
                ..Default::default()
            },
        )
        .await?;
    let t4 = time(renamed.commit_seq).await?;
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert_eq!(names(&old.as_of(t4)).await?, ["a_old"]);

    // A deleted ref is gone.
    let mut refs: Vec<String> = catalog
        .refs()
        .await?
        .into_iter()
        .map(|r| r.name().to_string())
        .collect();
    refs.sort();
    assert_eq!(refs, ["main", "old", "v1"]);
    old.delete().await?;
    assert!(matches!(catalog.branch("old").await, Err(Error::NotFound(_))));
    Ok(())
}
