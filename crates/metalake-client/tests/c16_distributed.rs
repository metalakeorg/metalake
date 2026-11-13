//! A lake that names a Ballista scheduler: its sessions carry the router, which sends a statement to the cluster or
//! runs it here. The cluster is Ballista's in this process; the lake reaches it by its address, as one elsewhere.
#![cfg(feature = "distributed")]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use futures_util::TryStreamExt;
use metalake::NewTable;
use metalake_client::{Lake, Session};
use metalake_datafusion::MetaLake;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

/// A lake on the tests' Postgres, in a directory of its own, with the table `db.t` and a few rows.
async fn lake() -> (Lake, PathBuf) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!("dist_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let dir = std::env::temp_dir().join(&schema);
    let warehouse = dir.join("data");
    let lake = Lake::create(&url(), Some(&warehouse.to_string_lossy()), Some(&schema), Some(&dir), Some(4))
        .await
        .expect("a lake");
    let main = lake.catalog.branch("main").await.unwrap();
    main.transaction().create_namespace(&["db"]).commit().await.unwrap();
    let shape = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]));
    let (tx, _) = main
        .transaction()
        .create_table(NewTable {
            namespace: &["db"],
            name: "t",
            schema: &shape,
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![("inline.max-bytes", "0")],
        })
        .unwrap();
    tx.commit().await.unwrap();
    let here = Session::new(&lake).await.unwrap();
    rows(&here, "insert into db.t values (1, 'a'), (2, 'b'), (3, 'c')").await.unwrap();
    (lake, dir)
}

async fn rows(session: &Session, text: &str) -> Result<String, Box<dyn std::error::Error>> {
    let prepared = session.run(text).await?;
    let batches: Vec<RecordBatch> = prepared.read()?.try_collect().await?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

/// A port nothing listens on: one bound and let go.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// A lake that names the scheduler: forced to the cluster, its session's statements run there, with the rows a local
/// session reads. A lake that names a scheduler that does not answer: forced, a read fails, which a session without the
/// router would not; in `auto`, it runs here.
#[tokio::test(flavor = "multi_thread")]
async fn session_reaches_the_cluster() -> Result<(), Box<dyn std::error::Error>> {
    let (lake, dir) = lake().await;
    let sql = "select k, v from db.t order by k";
    let here = rows(&Session::new(&lake).await?, sql).await?;
    assert!(here.contains("| 3 | c |"), "{here}");

    let storage = lake.storage()?;
    let client = MetaLake::new(lake.catalog.clone(), storage.clone()).await?;
    let scheduler = MetaLake::new(lake.catalog.clone(), storage.clone()).await?;
    let cluster = metalake_ballista::standalone(&client, scheduler, storage, Some(lake.catalog.clone())).await?;
    let there = Session::new(&lake.clone().with_scheduler(cluster.scheduler())).await?;
    rows(&there, "set metalake.engine = 'cluster'").await?;
    assert_eq!(rows(&there, sql).await?, here);

    let nowhere = lake.clone().with_scheduler(format!("df://127.0.0.1:{}", closed_port()));
    let nowhere = Session::new(&nowhere).await?;
    rows(&nowhere, "set metalake.engine = 'cluster'").await?;
    assert!(rows(&nowhere, sql).await.is_err());
    rows(&nowhere, "set metalake.engine = 'auto'").await?;
    rows(&nowhere, "set metalake.engine.threshold = 0").await?;
    assert_eq!(rows(&nowhere, sql).await?, here);

    drop(cluster);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
