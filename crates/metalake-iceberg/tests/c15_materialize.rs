//! Materializing on commit (REST-6, REST-9), end to end: the server, as a process, over a catalog on local disk, and what
//! it writes with no client loading anything. A table marked `on-commit` on a ref has its lists and a metadata file after
//! each of its commits there; one not marked, or a commit on a ref where it is not, has nothing; one process of a catalog
//! does it, another takes over, and one that starts again catches up.

mod common;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{Failure, append, create, fresh_schema, local_catalog, server, server_with, url};
use metalake::{Catalog, CatalogConfig, Ref, Storage};
use serde_json::json;

const KEY: &str = "metalake.iceberg.materialize";
/// How long a commit takes to be materialized at most, here: the feed delivers it a second after it (CHG-1).
const SOON: Duration = Duration::from_secs(10);
/// How long a test waits to say that nothing was materialized.
const QUIET: Duration = Duration::from_secs(4);
/// How long a process standing by takes to lead, at most, once the leader is gone.
const TAKEOVER: Duration = Duration::from_secs(40);

/// The directory of table `name`.
fn table_dir(directory: &Path, name: &str) -> PathBuf {
    let found = std::fs::read_dir(directory.join("db"))
        .into_iter()
        .flatten()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with(&format!("{name}-")));
    found.expect("the table's directory").path()
}

/// Whether the snapshot of commit `seq` of table `name` is materialized: its list, and a metadata file that serves it.
fn materialized(directory: &Path, name: &str, seq: i64) -> bool {
    let table = table_dir(directory, name);
    let listed = table
        .join("metalake-manifests")
        .join(format!("snap-{seq}.avro"))
        .exists();
    let served = std::fs::read_dir(table.join("metalake-metadata"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_prefix('v')?.split('-').next()?.parse::<i64>().ok()
        })
        .any(|served| served >= seq);
    listed && served
}

/// The metadata files of table `name`: a load that changed anything writes one more.
fn metadata_files(directory: &Path, name: &str) -> Vec<String> {
    let table = table_dir(directory, name);
    let mut found: Vec<String> = std::fs::read_dir(table.join("metalake-metadata"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    found
}

/// Whether anything was materialized for table `name`.
fn anything(directory: &Path, name: &str) -> bool {
    table_dir(directory, name).join("metalake-manifests").exists()
}

/// Waits `within` for `done`, and answers whether it came.
async fn eventually(within: Duration, done: impl Fn() -> bool) -> bool {
    let until = Instant::now() + within;
    while Instant::now() < until {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    done()
}

async fn mark(on: &Ref, name: &str, value: &str) -> Result<(), Failure> {
    let t = on.table(&["db"], name).await?;
    let marked = json!({"properties": {KEY: value}});
    on.transaction().define(t.id(), marked).commit().await?;
    Ok(())
}

/// Appends a row to table `name` on `on`, and answers its commit's sequence, the snapshot's id.
async fn commit(catalog: &Catalog, on: &Ref, storage: &Storage, name: &str, k: i64) -> Result<i64, Failure> {
    append(catalog, on, storage, name, k..k + 1).await?;
    Ok(on.table(&["db"], name).await?.snapshot_id().expect("a snapshot"))
}

/// A catalog with tables `names` in `db`, on local disk.
async fn lake(names: &[&str]) -> Result<(String, Catalog, Ref, Storage, PathBuf), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    for name in names {
        create(&main, name, "0", vec![]).await?;
    }
    Ok((schema, catalog, main, storage, directory))
}

/// An `updateTable` that sets properties, as an Iceberg client sends it.
fn set_properties(uri: &str, name: &str, properties: serde_json::Value) -> Result<(), Failure> {
    let address = uri.trim_start_matches("http://");
    let body =
        json!({"requirements": [], "updates": [{"action": "set-properties", "updates": properties}]}).to_string();
    let request = format!(
        "POST /v1/namespaces/db/tables/{name} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = std::net::TcpStream::connect(address)?;
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

/// Whether a process of the catalog of `schema` leads it.
async fn led(schema: &str) -> Result<bool, Failure> {
    let catalog = Catalog::connect(CatalogConfig::new(url(), schema)).await?;
    let free = catalog.lead("materialize").await?;
    let led = free.is_none();
    if let Some(lead) = free {
        lead.release().await?;
    }
    catalog.close();
    Ok(led)
}

#[tokio::test]
async fn rest6_marked_table_materialized_without_load() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["marked", "plain"]).await?;
    mark(&main, "marked", "on-commit").await?;
    let (_server, _) = server(&schema)?;

    let seq = commit(&catalog, &main, &storage, "marked", 1).await?;
    assert!(
        eventually(SOON, || materialized(&directory, "marked", seq)).await,
        "materialized with no load"
    );
    commit(&catalog, &main, &storage, "plain", 1).await?;
    tokio::time::sleep(QUIET).await;
    assert!(!anything(&directory, "plain"), "a table not marked waits for a load");
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_marking_through_service_wakes_worker() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["t"]).await?;
    commit(&catalog, &main, &storage, "t", 0).await?;
    let (_server, uri) = server(&schema)?;
    // nothing marked: the worker looks for a table marked once a minute
    tokio::time::sleep(Duration::from_secs(2)).await;
    // the answer to the commit is a load, which materializes what there is; the commit after it is the worker's
    set_properties(&uri, "t", json!({KEY: "on-commit"}))?;
    let next = commit(&catalog, &main, &storage, "t", 1).await?;
    assert!(
        eventually(SOON, || materialized(&directory, "t", next)).await,
        "within seconds of the commit that marked it, not a minute"
    );
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_commit_on_unmarked_ref_skips() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["t"]).await?;
    mark(&main, "t", "on-commit").await?;
    let (_server, _) = server(&schema)?;
    let on_main = commit(&catalog, &main, &storage, "t", 1).await?;
    assert!(eventually(SOON, || materialized(&directory, "t", on_main)).await);
    // what main's commits make due is done; a load generates the whole table, every ref's snapshots with it (REST-6)
    tokio::time::sleep(QUIET).await;
    let settled = metadata_files(&directory, "t");

    let dev = main.create_branch("dev").await?;
    mark(&dev, "t", "on-load").await?;
    commit(&catalog, &dev, &storage, "t", 2).await?;
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        metadata_files(&directory, "t"),
        settled,
        "dev keeps it on load: its commits load nothing"
    );
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_standby_process_takes_over_lead() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["t"]).await?;
    mark(&main, "t", "on-commit").await?;
    let first = server(&schema)?;
    assert!(eventually(SOON, || futures_led(&schema)).await, "the first leads");
    let (_second, _) = server(&schema)?;
    let before = commit(&catalog, &main, &storage, "t", 1).await?;
    assert!(eventually(SOON, || materialized(&directory, "t", before)).await);

    drop(first);
    let after = commit(&catalog, &main, &storage, "t", 2).await?;
    assert!(
        eventually(TAKEOVER, || materialized(&directory, "t", after)).await,
        "the second took over"
    );
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_restart_materializes_missed_commits() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["t"]).await?;
    mark(&main, "t", "on-commit").await?;
    let first = server(&schema)?;
    let before = commit(&catalog, &main, &storage, "t", 1).await?;
    assert!(eventually(SOON, || materialized(&directory, "t", before)).await);
    drop(first);

    let missed = commit(&catalog, &main, &storage, "t", 2).await?;
    let (_again, _) = server(&schema)?;
    assert!(
        eventually(SOON, || materialized(&directory, "t", missed)).await,
        "what it missed"
    );
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_materialize_off_skips() -> Result<(), Failure> {
    let (schema, catalog, main, storage, directory) = lake(&["t"]).await?;
    mark(&main, "t", "on-commit").await?;
    let (_server, _) = server_with(&schema, &["--materialize", "off"])?;
    commit(&catalog, &main, &storage, "t", 1).await?;
    tokio::time::sleep(QUIET).await;
    assert!(!anything(&directory, "t"));
    assert!(!led(&schema).await?, "and it does not lead");
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

/// [`led`], for [`eventually`]: blocks on it.
fn futures_led(schema: &str) -> bool {
    let schema = schema.to_string();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(led(&schema)).unwrap_or(false)
    })
    .join()
    .unwrap_or(false)
}
