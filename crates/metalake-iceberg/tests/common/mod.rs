//! What the tests that run the server share: a catalog with tables in a directory of its own, and the server as a process.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Storage, TableWriter, WriteOptions};

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
