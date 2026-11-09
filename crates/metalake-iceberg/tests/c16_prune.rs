//! Pruning what is generated (REST-6, RET-7b), end to end: the server, as a process, over a catalog on local disk. What a
//! snapshot kept reaches stays, its delete files with it; what expired goes once past the catalog's grace, and a client
//! still reads the table.

mod common;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use common::{Failure, append, create, fresh_schema, local_catalog, server};
use futures_util::TryStreamExt;
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Catalog, Ref, Scan, Storage};
use serde_json::{Value, json};

/// How long the pruning a load starts takes, at most, here.
const SOON: Duration = Duration::from_secs(10);

fn table_dir(directory: &Path) -> PathBuf {
    let found = std::fs::read_dir(directory.join("db"))
        .into_iter()
        .flatten()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("plain-"));
    found.expect("the table's directory").path()
}

/// The names of the objects in `dir` of the table's projection.
fn names(directory: &Path, dir: &str) -> Vec<String> {
    let mut found: Vec<String> = std::fs::read_dir(table_dir(directory).join(dir))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    found.sort();
    found
}

/// A `loadTable`, as any client asks it.
fn load_table(uri: &str) -> Result<(), Failure> {
    let address = uri.trim_start_matches("http://");
    let mut stream = std::net::TcpStream::connect(address)?;
    let request =
        format!("GET /v1/namespaces/db/tables/plain HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
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

/// The keys a real client reads of `db.plain`.
fn rows(uri: &str) -> Result<Value, Failure> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON: a Python with pyiceberg");
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/client.py");
    let run = Command::new(python).args([script, "rows", uri]).output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    let said: Value = serde_json::from_slice(&run.stdout)?;
    Ok(said["rows"].clone())
}

async fn delete_key(catalog: &Catalog, on: &Ref, storage: &Storage, k: i64) -> Result<(), Failure> {
    let t = on.table(&["db"], "plain").await?;
    let scan = Scan::new(catalog, &t).with_positions();
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, storage).try_collect().await?;
    let mut deletes = Deletes::new();
    for b in &batches {
        let column = |name: &str| b.column_by_name(name).unwrap().as_primitive::<Int64Type>().clone();
        let (keys, files, positions) = (column("k"), column(FILE_ID), column(POSITION));
        for i in (0..b.num_rows()).filter(|i| keys.value(*i) == k) {
            deletes.insert(files.value(i), positions.value(i) as u64);
        }
    }
    on.transaction().delete(&t, storage, deletes).commit().await?;
    Ok(())
}

/// Keeps the last `keep` snapshots of the table, whatever their age: a commit of the definition, which is a snapshot.
async fn keep(main: &Ref, keep: usize) -> Result<(), Failure> {
    let t = main.table(&["db"], "plain").await?;
    let properties = json!({"properties": {
        "history.expire.min-snapshots-to-keep": keep.to_string(),
        "history.expire.max-snapshot-age-ms": "1",
    }});
    main.transaction().define(t.id(), properties).commit().await?;
    Ok(())
}

/// Waits for `done`, and answers whether it came.
async fn eventually(done: impl Fn() -> bool) -> bool {
    let until = Instant::now() + SOON;
    while Instant::now() < until {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    done()
}

/// A table of one file of three rows, two of them deleted one at a time: the second deletion vector takes the place of
/// the first, so the first's delete file is reached only by the snapshot that set it. Loaded after each commit.
async fn history(grace: Option<Duration>) -> Result<(String, Catalog, Ref, PathBuf), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    if let Some(grace) = grace {
        catalog.set_collect_grace(grace).await?;
    }
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "plain", "0", vec![]).await?;
    let (_server, uri) = server(&schema)?;
    append(&catalog, &main, &storage, "plain", 10..13).await?;
    load_table(&uri)?;
    for k in [10, 11] {
        delete_key(&catalog, &main, &storage, k).await?;
        load_table(&uri)?;
    }
    assert_eq!(
        names(&directory, "metalake-deletes").len(),
        2,
        "a delete file for each vector"
    );
    Ok((schema, catalog, main, directory))
}

#[tokio::test]
async fn rest6_prune_expired_snapshots() -> Result<(), Failure> {
    let (schema, _catalog, main, directory) = history(Some(Duration::ZERO)).await?;
    let lists_before = names(&directory, "metalake-manifests")
        .into_iter()
        .filter(|n| n.starts_with("snap-"))
        .count();

    // Three kept: the first vector's snapshot among them, so its delete file stays, as the list that names it does.
    keep(&main, 3).await?;
    {
        let (_server, uri) = server(&schema)?;
        load_table(&uri)?;
        let lists = || {
            names(&directory, "metalake-manifests")
                .into_iter()
                .filter(|n| n.starts_with("snap-"))
                .count()
        };
        assert!(eventually(|| lists() == 3).await, "{} lists of {lists_before}", lists());
        assert_eq!(names(&directory, "metalake-deletes").len(), 2, "both reached");
    }

    // One kept: the first vector's delete file goes; the second, which the last snapshot reaches, stays.
    keep(&main, 1).await?;
    let (_server, uri) = server(&schema)?;
    load_table(&uri)?;
    assert!(
        eventually(|| names(&directory, "metalake-deletes").len() == 1).await,
        "{:?}",
        names(&directory, "metalake-deletes")
    );
    let lists: Vec<String> = names(&directory, "metalake-manifests")
        .into_iter()
        .filter(|n| n.starts_with("snap-"))
        .collect();
    assert_eq!(lists.len(), 1, "{lists:?}");
    assert_eq!(rows(&uri)?, json!([12]), "a client still reads it");
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn rest6_prune_grace_preserves_recent() -> Result<(), Failure> {
    let (schema, _catalog, main, directory) = history(None).await?;
    let before = names(&directory, "metalake-manifests");
    keep(&main, 1).await?;
    let (_server, uri) = server(&schema)?;
    load_table(&uri)?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = names(&directory, "metalake-manifests");
    assert!(before.iter().all(|n| after.contains(n)), "a day's grace: nothing goes");
    assert_eq!(names(&directory, "metalake-deletes").len(), 2);
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}
