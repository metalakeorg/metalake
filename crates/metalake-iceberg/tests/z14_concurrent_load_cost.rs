//! Z14: what first loads that come together cost. One table with a commit per snapshot, its projection removed, then
//! loaded by one client and by several at once: each answer, and how long the last of them took. A measurement: run it
//! with `--ignored`, and `--release` to believe it.

mod common;

use std::io::{Read, Write};
use std::time::Instant;

use common::{Failure, batch, create, fresh_schema, local_catalog, server};
use metalake::{TableWriter, WriteOptions};
use serde_json::json;

const SNAPSHOTS: i64 = 1_000;
const CLIENTS: [usize; 3] = [1, 4, 8];

/// One `loadTable` of `db.long`, and the metadata location it answered.
fn load(uri: &str) -> Result<String, Failure> {
    let address = uri.trim_start_matches("http://");
    let request = format!("GET /v1/namespaces/db/tables/long HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(request.as_bytes())?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    assert!(
        answer.starts_with("HTTP/1.1 200"),
        "{}",
        &answer[..answer.len().min(300)]
    );
    let (_, rest) = answer
        .split_once("\"metadata-location\":\"")
        .expect("a metadata location");
    Ok(rest.split('"').next().unwrap_or_default().to_string())
}

#[tokio::test]
#[ignore = "a measurement"]
async fn rest6_concurrent_first_load_cost() -> Result<(), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, directory) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "long", "0", vec![]).await?;
    let t = main.table(&["db"], "long").await?;
    let window = json!({"properties": {"history.expire.min-snapshots-to-keep": SNAPSHOTS.to_string()}});
    main.transaction().define(t.id(), window).commit().await?;
    for at in 0..SNAPSHOTS {
        let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
        writer.write(&batch(at..at + 1)).await?;
        main.transaction().append(writer.finish().await?).commit().await?;
    }
    eprintln!("{SNAPSHOTS} commits written");

    let location = t.location().trim_start_matches("file:///").to_string();
    let (_server, uri) = server(&schema)?;
    println!("clients | slowest first load ms");
    for clients in CLIENTS {
        for generated in ["metalake-manifests", "metalake-metadata"] {
            let _ = std::fs::remove_dir_all(format!("{location}/{generated}"));
        }
        let start = Instant::now();
        let answers: Vec<String> = std::thread::scope(|s| {
            let loads: Vec<_> = (0..clients).map(|_| s.spawn(|| load(&uri).unwrap())).collect();
            loads.into_iter().map(|l| l.join().unwrap()).collect()
        });
        let slowest = start.elapsed().as_secs_f64() * 1000.0;
        assert!(
            answers.iter().all(|a| a == &answers[0]),
            "every client is answered the same metadata"
        );
        println!("{clients} | {slowest:.0}");
    }
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}
