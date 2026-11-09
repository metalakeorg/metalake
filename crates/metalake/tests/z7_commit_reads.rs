//! What a commit of files another engine wrote pays to storage, with a store that answers each GET in 30 ms, as an
//! object store in the same region does. Before revision 0.3 the commit read each file's tail, 16 at once, for its
//! footer's size; now it reads nothing, and the footers are kept after it, by the Iceberg service's worker or by
//! maintenance. A measurement, not a test:
//!
//!   CRATE=metalake bash sql/test.sh --release --test z7_commit_reads -- --ignored --nocapture

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::{StreamExt, TryStreamExt, stream};
use metalake::{Catalog, CatalogConfig, NewTable, Scan, Storage, TableWriter, WriteOptions};
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};

/// A GET's latency, as an object store answers in the same region.
const GET: Duration = Duration::from_millis(30);
/// Tails the commit read at once, before.
const AT_ONCE: usize = 16;

fn batch(k: i64) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![k]))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

#[tokio::test]
#[ignore = "a measurement: run with --release --ignored --nocapture"]
async fn z7_commit_reads() -> Result<(), Box<dyn std::error::Error>> {
    let n: usize = std::env::var("Z7_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None).await?;
    let main = catalog.branch("main").await?;
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &batch(0).schema(),
        location: None,
        cluster_key: vec!["k"],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    let config = ThrottleConfig {
        wait_get_per_call: GET,
        ..Default::default()
    };
    let store = Arc::new(ThrottledStore::new(InMemory::new(), config));
    let storage = Storage::from_store("memory:///wh", store)?;

    // n stored files, one row each, as another engine wrote them: no footer, and a size the commit is not told.
    let options = WriteOptions {
        stored: true,
        target_file_size: Some(1),
        ..Default::default()
    };
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), options)?;
    for k in 0..n as i64 {
        writer.write(&batch(k)).await?;
    }
    let mut files = writer.finish().await?;
    for file in &mut files {
        file.footer_size = 0;
        file.footer = None;
    }
    assert_eq!(files.len(), n);
    let sized: Vec<(String, u64)> = files
        .iter()
        .map(|f| (f.path.clone().unwrap(), f.file_size as u64))
        .collect();

    // Before: the commit read each tail first, 16 at once.
    let started = Instant::now();
    let tails: Vec<_> = stream::iter(sized.clone())
        .map(|(path, size)| {
            let storage = storage.clone();
            async move { storage.tail(&path, size, 8).await }
        })
        .buffered(AT_ONCE)
        .try_collect()
        .await?;
    let read_before = started.elapsed();
    assert_eq!(tails.len(), n);

    // Now: the commit itself, which reads nothing.
    let started = Instant::now();
    main.transaction().append(files).commit().await?;
    let commit = started.elapsed();

    // After it, off the commit's path: the footers kept.
    let t = main.table(&["db"], "t").await?;
    let started = Instant::now();
    let kept = catalog.maintenance().keep_footers(t.id(), sized, &storage).await?;
    let footers = started.elapsed();
    assert_eq!(kept as usize, n);
    let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    assert!(planned.iter().all(|f| f.footer.is_some()));

    println!("{n} files, a GET of {} ms", GET.as_millis());
    println!(
        "  before: the commit waited {:>7.2} s for {n} tails",
        read_before.as_secs_f64()
    );
    println!(
        "  now:    the commit takes    {:>7.2} s, reading none",
        commit.as_secs_f64()
    );
    println!(
        "  after:  footers kept in     {:>7.2} s, while no client waits",
        footers.as_secs_f64()
    );
    Ok(())
}
