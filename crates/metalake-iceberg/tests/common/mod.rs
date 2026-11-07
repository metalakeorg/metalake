//! What the tests that run the server share: a catalog with tables in a directory of its own, and the server as a process.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Scan, Storage, TableWriter, WriteOptions};
use serde_json::json;

pub type Failure = Box<dyn std::error::Error>;

pub fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-iceberg bash sql/test.sh")
}

pub fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "iceberg_read_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// A catalog in a directory of its own, whose files a client reads.
pub async fn local_catalog(schema: &str) -> Result<(Catalog, Storage, PathBuf), Failure> {
    let directory = std::env::temp_dir().join(schema);
    std::fs::create_dir_all(&directory)?;
    let path = directory.display().to_string().replace('\\', "/");
    let warehouse = format!("file:///{}", path.trim_start_matches('/'));
    let catalog = Catalog::init(CatalogConfig::new(url(), schema).warehouse(&warehouse), None).await?;
    let storage = Storage::new(&warehouse, Vec::<(String, String)>::new())?;
    Ok((catalog, storage, directory))
}

pub async fn create(on: &Ref, name: &str, inline_max: &str, partitioning: Vec<(&str, &str)>) -> Result<(), Failure> {
    let shape = batch(0..1).schema();
    let (tx, _) = on.transaction().create_table(NewTable {
        namespace: &["db"],
        name,
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning,
        properties: vec![("inline.max-bytes", inline_max)],
    })?;
    tx.commit().await?;
    Ok(())
}

pub async fn append(
    catalog: &Catalog,
    on: &Ref,
    storage: &Storage,
    name: &str,
    keys: std::ops::Range<i64>,
) -> Result<(), Failure> {
    let t = on.table(&["db"], name).await?;
    let mut writer = TableWriter::new(catalog, &t, storage.clone(), WriteOptions::default())?;
    writer.write(&batch(keys)).await?;
    let added = writer.finish().await?;
    on.transaction().append(added).commit().await?;
    Ok(())
}

/// What a reader of a stream meets, each in a table of `db`: `merged`, a merge of added files between appends; `removed`,
/// a merge that removes a file; `evolved`, a second spec between appends; `long`, sixty commits, whose first list
/// rolls up.
pub async fn streamed(schema: &str) -> Result<std::path::PathBuf, Failure> {
    let (catalog, storage, directory) = local_catalog(schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;

    create(&main, "merged", "0", vec![]).await?;
    append(&catalog, &main, &storage, "merged", 0..2).await?;
    let branch = main.create_branch("b").await?;
    append(&catalog, &branch, &storage, "merged", 10..12).await?;
    branch.merge().commit().await?;
    append(&catalog, &main, &storage, "merged", 20..21).await?;

    create(&main, "removed", "0", vec![]).await?;
    append(&catalog, &main, &storage, "removed", 0..2).await?;
    let branch = main.create_branch("c").await?;
    let t = branch.table(&["db"], "removed").await?;
    let planned: Vec<_> = Scan::new(&catalog, &t).plan().await?.try_collect().await?;
    let ids = planned.iter().map(|f| f.file_id);
    branch.transaction().remove(&t, ids).commit().await?;
    branch.merge().commit().await?;

    create(&main, "evolved", "0", vec![]).await?;
    append(&catalog, &main, &storage, "evolved", 0..2).await?;
    let t = main.table(&["db"], "evolved").await?;
    let field = json!({"source_ids": [1], "field_id": 1000, "name": "k_trunc", "transform": "truncate[10]"});
    let spec = json!({"partition_spec": [field]});
    main.transaction().define(t.id(), spec).commit().await?;
    append(&catalog, &main, &storage, "evolved", 10..12).await?;
    append(&catalog, &main, &storage, "evolved", 20..22).await?;

    create(&main, "long", "0", vec![]).await?;
    for key in 0..60 {
        append(&catalog, &main, &storage, "long", key..key + 1).await?;
    }
    catalog.close();
    Ok(directory)
}

pub struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub fn server(schema: &str) -> Result<(Running, String), Failure> {
    use std::io::{BufRead, BufReader};
    let mut running = Running(
        Command::new(env!("CARGO_BIN_EXE_metalake-iceberg"))
            .args(["--schema", schema, "--listen", "127.0.0.1:0"])
            .env("METALAKE_POSTGRES", url())
            .stdout(Stdio::piped())
            .spawn()?,
    );
    let mut line = String::new();
    BufReader::new(running.0.stdout.take().expect("piped")).read_line(&mut line)?;
    let address = line
        .trim()
        .strip_prefix("listening on ")
        .expect("the first line says where");
    let address = address.to_string();
    Ok((running, format!("http://{address}")))
}
