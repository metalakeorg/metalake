//! C17 (ADR 0006 milestone 6): a cluster of processes of its own. The binaries `metalake-scheduler` and two
//! `metalake-executor`s, started as a deployment would, by the environment alone, over a warehouse on disk; a client
//! that knows the scheduler's address and nothing else reads through them what a local session reads.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use metalake::environment::storage_options;
use metalake::{Catalog, CatalogConfig, NewTable, Storage, TableWriter, WriteOptions};
use metalake_datafusion::MetaLake;

/// The processes of a cluster, stopped when this goes.
struct Processes(Vec<Child>);

impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Waits until `port` takes a connection, or fails.
fn listening(port: u16, what: &str) {
    let until = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < until, "{what} did not listen on {port}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `dir` as a warehouse's URL.
fn file_url(dir: &Path) -> String {
    let path = dir.to_string_lossy().replace('\\', "/");
    format!("file:///{}/", path.trim_start_matches('/'))
}

fn batch(keys: std::ops::Range<i64>) -> RecordBatch {
    let shape = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(keys.clone())),
        Arc::new(StringArray::from_iter_values(keys.map(|k| format!("s{k}")))),
    ];
    RecordBatch::try_new(shape, columns).unwrap()
}

async fn rows(context: &SessionContext, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(pretty_format_batches(&context.sql(sql).await?.collect().await?)?.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn binaries_answer_as_a_local_session() -> Result<(), Box<dyn std::error::Error>> {
    // A lake on disk: `main.db.t`, four files of 25 rows.
    let schema = fresh_schema();
    let dir = std::env::temp_dir().join(&schema);
    std::fs::create_dir_all(&dir)?;
    let warehouse = file_url(&dir);
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse(warehouse.clone()), None).await?;
    let storage = Storage::new(&warehouse, storage_options(std::env::vars_os()))?;
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
    for at in 0..4 {
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(at * 25..at * 25 + 25)).await?;
        main.transaction().append(writer.finish().await?).commit().await?;
    }

    // The cluster: its processes, configured by the environment alone.
    let scheduler = free_port();
    let mut processes = Processes(Vec::new());
    processes.0.push(
        Command::new(env!("CARGO_BIN_EXE_metalake-scheduler"))
            .env("METALAKE_POSTGRES", url())
            .env("METALAKE_SCHEMA", &schema)
            .env("METALAKE_SCHEDULER_BIND", format!("127.0.0.1:{scheduler}"))
            .env("METALAKE_SCHEDULER_HOST", "127.0.0.1")
            .env("RUST_LOG", "warn")
            .spawn()?,
    );
    listening(scheduler, "the scheduler");
    let mut flights = Vec::new();
    for _ in 0..2 {
        let (flight, grpc) = (free_port(), free_port());
        processes.0.push(
            Command::new(env!("CARGO_BIN_EXE_metalake-executor"))
                .env("METALAKE_SCHEDULER", format!("df://127.0.0.1:{scheduler}"))
                .env("METALAKE_WAREHOUSE", &warehouse)
                .env("METALAKE_EXECUTOR_BIND", "127.0.0.1")
                .env("METALAKE_EXECUTOR_HOST", "127.0.0.1")
                .env("METALAKE_EXECUTOR_PORT", flight.to_string())
                .env("METALAKE_EXECUTOR_GRPC_PORT", grpc.to_string())
                .env("METALAKE_EXECUTOR_SLOTS", "2")
                .env("METALAKE_EXECUTOR_WORK_DIR", dir.join(format!("work-{flight}")))
                .env("RUST_LOG", "warn")
                .spawn()?,
        );
        flights.push(flight);
    }
    for flight in flights {
        listening(flight, "an executor");
    }

    // The client: the lake, and the scheduler's address.
    let client = MetaLake::new(catalog, storage).await?;
    let local = client.context()?;
    let cluster = metalake_ballista::session(&client, &format!("df://127.0.0.1:{scheduler}")).await?;
    cluster.sql("set metalake.engine = 'cluster'").await?.collect().await?;
    for sql in [
        "select k, s from main.db.t where k % 10 = 3 order by k",
        "select k % 4 as g, count(*) as c, sum(k) as s from main.db.t group by 1 order by 1",
    ] {
        let (here, there) = (rows(&local, sql).await?, rows(&cluster, sql).await?);
        assert_eq!(here, there, "{sql}");
    }
    drop(processes);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
