//! A table read through the server, by a real client, and what it was answered held to the reference: `iceberg_metadata`
//! as a whole (ICE-1), `iceberg_bound` value for value (ICE-3).

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Storage, TableWriter, WriteOptions};
use metalake_iceberg::project::bounds::to_iceberg;
use metalake_pg::Pool;
use serde_json::{Value, json};

type Failure = Box<dyn std::error::Error>;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-iceberg bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "iceberg_read_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn create(on: &Ref, name: &str, inline_max: &str) -> Result<(), Failure> {
    let shape = batch(0..1).schema();
    let (tx, _) = on.transaction().create_table(NewTable {
        namespace: &["db"],
        name,
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![("inline.max-bytes", inline_max)],
    })?;
    tx.commit().await?;
    Ok(())
}

async fn append(
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

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn server(schema: &str) -> Result<(Running, String), Failure> {
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
        .expect("the first line says where")
        .to_string();
    Ok((running, format!("http://{address}")))
}

fn client(step: &str, uri: &str) -> Result<Value, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON: a Python with pyiceberg");
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/client.py");
    let run = Command::new(python).args([script, step, uri]).output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    Ok(serde_json::from_slice(&run.stdout)?)
}

/// Each snapshot's `manifest-list` is the server's to name; the reference leaves it to who writes the files.
fn without_lists(mut metadata: Value) -> Value {
    for snapshot in metadata["snapshots"].as_array_mut().unwrap() {
        snapshot.as_object_mut().unwrap().remove("manifest-list");
    }
    metadata
}

#[tokio::test]
async fn pyiceberg_reads_a_table_and_what_it_is_told_is_what_the_reference_says() -> Result<(), Failure> {
    let schema = fresh_schema();
    let directory = std::env::temp_dir().join(&schema);
    std::fs::create_dir_all(&directory)?;
    let warehouse = format!(
        "file:///{}",
        directory
            .display()
            .to_string()
            .replace('\\', "/")
            .trim_start_matches('/')
    );
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse(&warehouse), None).await?;
    let storage = Storage::new(&warehouse, Vec::<(String, String)>::new())?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0").await?;
    create(&main, "small", "1000000").await?;
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
    assert_eq!(without_lists(said["metadata"].clone()), expected);
    Ok(())
}

#[tokio::test]
async fn a_bound_is_served_as_the_reference_serves_it() -> Result<(), Failure> {
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
    ] {
        let row = session
            .query_one(
                "select b, iceberg_bound($1, b) from encode_bound($1, $2) b",
                &[&ty, &value],
            )
            .await?;
        let (stored, served): (Vec<u8>, Vec<u8>) = (row.get(0), row.get(1));
        assert_eq!(to_iceberg(ty, &stored), Some(served), "{ty} {value}");
    }
    catalog.close();
    Ok(())
}
