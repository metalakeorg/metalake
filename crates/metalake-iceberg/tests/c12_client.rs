//! The server as a process, and a real client: PyIceberg connects and lists what Postgres holds, and stopping leaves no
//! connection behind.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use metalake::{Catalog, CatalogConfig};
use metalake_iceberg::{State, serve};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-iceberg bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "iceberg_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// A catalog with namespaces in it.
async fn catalog(schema: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = CatalogConfig::new(url(), schema).warehouse("memory:///wh");
    let catalog = Catalog::init(config, None).await?;
    let main = catalog.branch("main").await?;
    let tx = main
        .transaction()
        .create_namespace(&["db"])
        .create_namespace(&["db", "sub"]);
    tx.create_namespace(&["other"]).commit().await?;
    catalog.close();
    Ok(())
}

/// Ends the process however the test does.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn pyiceberg_connects_and_lists_what_postgres_holds() -> Result<(), Box<dyn std::error::Error>> {
    let python = std::env::var("METALAKE_TEST_PYTHON").expect("METALAKE_TEST_PYTHON: a Python with pyiceberg");
    let schema = fresh_schema();
    catalog(&schema).await?;
    let mut server = Running(
        Command::new(env!("CARGO_BIN_EXE_metalake-iceberg"))
            .args(["--schema", &schema, "--listen", "127.0.0.1:0"])
            .env("METALAKE_POSTGRES", url())
            .stdout(Stdio::piped())
            .spawn()?,
    );
    let mut line = String::new();
    BufReader::new(server.0.stdout.take().expect("piped")).read_line(&mut line)?;
    let address = line
        .trim()
        .strip_prefix("listening on ")
        .expect("the first line says where");

    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/client.py");
    let run = Command::new(python)
        .args([script, &format!("http://{address}")])
        .output()?;
    assert!(run.status.success(), "{}", String::from_utf8_lossy(&run.stderr));
    let said: Value = serde_json::from_slice(&run.stdout)?;
    assert_eq!(said["top"][0], json!(["db"]));
    assert!(said["top"].as_array().unwrap().contains(&json!(["other"])));
    assert_eq!(said["under_db"], json!([["db", "sub"]]));
    assert!(
        said["nowhere"].as_str().unwrap().contains("no namespace nowhere"),
        "{said}"
    );
    Ok(())
}

#[tokio::test]
async fn a_catalog_it_cannot_reach_ends_the_process_with_a_reason() -> Result<(), Box<dyn std::error::Error>> {
    let run = Command::new(env!("CARGO_BIN_EXE_metalake-iceberg"))
        .args(["--schema", &fresh_schema(), "--listen", "127.0.0.1:0"])
        .env("METALAKE_POSTGRES", url())
        .output()?;
    assert!(!run.status.success());
    assert!(String::from_utf8_lossy(&run.stderr).starts_with("metalake-iceberg: "));
    Ok(())
}

#[tokio::test]
async fn stopping_closes_the_connections_even_of_a_state_still_held() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    catalog(&schema).await?;
    let named = format!("{} application_name={schema}", url());
    let state = State::connect(&named, &schema).await?;
    let held = state.clone();
    let (stop, stopped) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let running = tokio::spawn(serve(listener, state, async {
        let _ = stopped.await;
    }));

    let looks = metalake_pg::Pool::new(&url(), 1)?;
    let client = looks.get().await?;
    let query = "select count(*) from pg_stat_activity where application_name = $1";
    let open: i64 = client.query_one(query, &[&schema]).await?.get(0);
    assert!(open > 0, "the state holds a connection");

    stop.send(()).expect("it runs");
    tokio::time::timeout(Duration::from_secs(10), running).await???;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let open: i64 = client.query_one(query, &[&schema]).await?.get(0);
    assert_eq!(open, 0);
    drop(held);
    Ok(())
}
