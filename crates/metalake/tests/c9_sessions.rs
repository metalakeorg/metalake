//! Sessions of the pool, one at a time: no operation of the core holds a connection while it asks the pool for another,
//! or as many of it at once as the pool has connections each hold one and wait for ever. Each operation is run six at
//! once on a pool of two, alone, so that one that waits names itself; then all at once.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use futures_util::future::{BoxFuture, join_all};
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref, Scan, Storage, Table, TableWriter, WriteOptions};
use object_store::memory::InMemory;

const COPIES: usize = 6;
const STUCK: Duration = Duration::from_secs(10);

fn batch(at: i64) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![at]))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

struct Lake {
    catalog: Catalog,
    main: Ref,
    storage: Storage,
    t: Table,
}

/// A catalog on a pool of two connections, with a table of two commits of a file each.
async fn lake() -> Result<Lake, Error> {
    let mut config = CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh");
    config.connections = 2;
    let catalog = Catalog::init(config, None).await?;
    let storage = Storage::from_store("memory:///wh", Arc::new(InMemory::new()))?;
    let main = catalog.branch("main").await?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0).schema(),
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", "0")],
    })?;
    tx.commit().await?;
    let lake = Lake {
        t: main.table(&["db"], "t").await?,
        catalog,
        main,
        storage,
    };
    for at in 0..2 {
        append(&lake, at).await?;
    }
    Ok(Lake {
        t: lake.main.table(&["db"], "t").await?,
        ..lake
    })
}

async fn append(lake: &Lake, at: i64) -> Result<(), Error> {
    let mut writer = TableWriter::new(&lake.catalog, &lake.t, lake.storage.clone(), WriteOptions::default())?;
    writer.write(&batch(at)).await?;
    lake.main.transaction().append(writer.finish().await?).commit().await?;
    Ok(())
}

type Operation = for<'a> fn(&'a Lake, SystemTime) -> BoxFuture<'a, Result<(), Error>>;

/// Every operation of the core that reaches the pool, by its name.
fn operations() -> Vec<(&'static str, Operation)> {
    vec![
        ("Ref::table", |l, _| {
            Box::pin(async { l.main.table(&["db"], "t").await.map(drop) })
        }),
        ("Ref::table as of a time", |l, then| {
            Box::pin(async move { l.main.as_of(then).table(&["db"], "t").await.map(drop) })
        }),
        ("Ref::table at a sequence", |l, _| {
            Box::pin(async { l.main.at(l.t.seq()).table(&["db"], "t").await.map(drop) })
        }),
        ("Ref::table_with_id", |l, _| {
            Box::pin(async { l.main.table_with_id(l.t.id()).await.map(drop) })
        }),
        ("Ref::table_at", |l, _| {
            Box::pin(async { l.main.table_at(&l.t, l.t.seq()).await.map(drop) })
        }),
        ("Ref::tables", |l, _| {
            Box::pin(async { l.main.tables().await.map(drop) })
        }),
        ("Ref::tables as of a time", |l, then| {
            Box::pin(async move { l.main.as_of(then).tables().await.map(drop) })
        }),
        ("Ref::read_point", |l, _| {
            Box::pin(async { l.main.read_point().await.map(drop) })
        }),
        ("Ref::definition", |l, _| {
            Box::pin(async { l.main.definition(&l.t, None).await.map(drop) })
        }),
        ("Ref::added_files", |l, _| {
            Box::pin(async { l.main.added_files(&l.t, l.t.seq()).await.map(drop) })
        }),
        ("Ref::added_files_of", |l, _| {
            Box::pin(async { l.main.added_files_of(&l.t, &[l.t.seq()]).await.map(drop) })
        }),
        ("Ref::files_at_paths", |l, _| {
            Box::pin(async { l.main.files_at_paths(&l.t, &[], &[]).await.map(drop) })
        }),
        ("Ref::commit_of_key", |l, _| {
            Box::pin(async { l.main.commit_of_key("none").await.map(drop) })
        }),
        ("Ref::namespaces", |l, _| {
            Box::pin(async { l.main.namespaces().await.map(drop) })
        }),
        ("Ref::history", |l, _| {
            Box::pin(async { l.main.history(10).await.map(drop) })
        }),
        ("Ref::snapshots", |l, _| {
            Box::pin(async { l.main.snapshots(&l.t, 10).await.map(drop) })
        }),
        ("Catalog::branch", |l, _| {
            Box::pin(async { l.catalog.branch("main").await.map(drop) })
        }),
        ("Catalog::refs", |l, _| {
            Box::pin(async { l.catalog.refs().await.map(drop) })
        }),
        ("Catalog::describe_refs", |l, _| {
            Box::pin(async { l.catalog.describe_refs().await.map(drop) })
        }),
        ("Catalog::latest", |l, _| {
            Box::pin(async { l.catalog.latest(&[l.t.ref_id()]).await.map(drop) })
        }),
        ("Catalog::table_snapshots", |l, _| {
            Box::pin(async { l.catalog.table_snapshots(&l.t).await.map(drop) })
        }),
        ("Catalog::ancestors", |l, _| {
            Box::pin(async {
                let heads: Vec<i64> = l.t.snapshot_id().into_iter().collect();
                l.catalog.ancestors(&l.t, &heads, 5, None).await.map(drop)
            })
        }),
        ("Catalog::commit_tables", |l, _| {
            Box::pin(async { l.catalog.commit_tables(&[l.t.seq()]).await.map(drop) })
        }),
        ("Catalog::tables_with_property", |l, _| {
            Box::pin(async { l.catalog.tables_with_property("k", "v").await.map(drop) })
        }),
        ("Catalog::collect_grace", |l, _| {
            Box::pin(async { l.catalog.collect_grace().await.map(drop) })
        }),
        ("Catalog::feed", |l, _| {
            Box::pin(async {
                let start = l.catalog.feed_start().await?;
                l.catalog.feed(&start, 10).await.map(drop)
            })
        }),
        ("Scan::plan and read", |l, _| {
            Box::pin(async {
                let scan = Scan::new(&l.catalog, &l.t);
                let read: Vec<RecordBatch> = scan.read(scan.plan().await?, &l.storage).try_collect().await?;
                assert!(!read.is_empty());
                Ok(())
            })
        }),
        ("Scan::list", |l, _| {
            Box::pin(async {
                let listed: Vec<_> = Scan::new(&l.catalog, &l.t).list().await?.try_collect().await?;
                assert!(!listed.is_empty());
                Ok(())
            })
        }),
        ("a write and its commit", |l, _| Box::pin(append(l, 100))),
        ("Maintenance::footers", |l, _| {
            Box::pin(async { l.catalog.maintenance().footers(&l.t, &l.storage).await.map(drop) })
        }),
        ("Maintenance::expire", |l, _| {
            Box::pin(async { l.catalog.maintenance().expire().await.map(drop) })
        }),
        ("Maintenance::sweep", |l, _| {
            Box::pin(async { l.catalog.maintenance().sweep(&l.storage).await.map(drop) })
        }),
        ("Maintenance::orphans", |l, _| {
            Box::pin(async { l.catalog.maintenance().orphans(&l.storage).await.map(drop) })
        }),
        ("Maintenance::fill_commit_times", |l, _| {
            Box::pin(async { l.catalog.maintenance().fill_commit_times().await.map(drop) })
        }),
    ]
}

#[tokio::test]
async fn operations_beyond_pool_resolves() -> Result<(), Error> {
    let lake = lake().await?;
    let then = SystemTime::now();
    // A time is read once it is a second past (TT-3).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut stuck = Vec::new();
    for (name, operation) in operations() {
        let copies = join_all((0..COPIES).map(|_| operation(&lake, then)));
        match tokio::time::timeout(STUCK, copies).await {
            Ok(done) => {
                if let Some(Err(e)) = done.into_iter().find(Result::is_err) {
                    panic!("{name}: {e}");
                }
            }
            Err(_) => stuck.push(name),
        }
    }
    assert!(stuck.is_empty(), "wait on the pool for ever: {stuck:?}");

    let all = join_all(operations().into_iter().map(|(_, operation)| operation(&lake, then)));
    let done = tokio::time::timeout(STUCK * 3, all)
        .await
        .expect("all at once, none waits for ever");
    assert!(done.iter().all(Result::is_ok), "{done:?}");
    Ok(())
}
