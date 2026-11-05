//! C10e1 (ADR 0003 §7): a lake's own Postgres: real binaries, a real cluster, and the catalog on it. The binaries are
//! fetched the first time, so this needs the network once. Run by `cargo test -p metalake-local`.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use metalake::{Catalog, CatalogConfig};
use metalake_local::{Binaries, Cluster, Error};

/// A path for a new lake, which does not exist yet.
fn path() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    std::env::temp_dir().join(format!(
        "local_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn a_catalog_installs_on_a_lakes_own_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let began = Instant::now();
    let binaries = Binaries::find()?;
    println!("binaries {:?} in {:?}", binaries.directory(), began.elapsed());

    let lake = path();
    let began = Instant::now();
    let cluster = Cluster::init(&lake, binaries.clone())?;
    println!("initdb {:?}", began.elapsed());
    assert!(!cluster.is_running()?);

    let began = Instant::now();
    let connection = cluster.ensure()?;
    println!("start {:?}", began.elapsed());
    assert!(cluster.is_running()?);

    // `init` fails without `btree_gist`, which the format's tables need, and `track_commit_timestamp`.
    let began = Instant::now();
    let config = CatalogConfig::new(connection, "metalake").warehouse("memory:///lake");
    let catalog = Catalog::init(config, None).await?;
    println!("catalog {:?}", began.elapsed());
    assert_eq!(catalog.versions().format_version, "0");
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["default"]).commit().await?;
    assert!(main.as_of(std::time::SystemTime::now()).tables().await?.is_empty());
    drop(catalog);

    let began = Instant::now();
    cluster.stop()?;
    println!("stop {:?}", began.elapsed());
    assert!(!cluster.is_running()?);

    // It starts again, as it was.
    let again = Cluster::open(&lake, binaries.clone())?;
    let mut starts = Vec::new();
    for _ in 0..5 {
        let began = Instant::now();
        again.ensure()?;
        starts.push(began.elapsed());
        again.stop()?;
    }
    println!("starts {starts:?}");
    let connection = again.ensure()?;
    let began = Instant::now();
    assert_eq!(again.ensure()?, connection, "a server that is up stays as it is");
    println!("already up {:?}", began.elapsed());
    let catalog = Catalog::connect(CatalogConfig::new(connection, "metalake")).await?;
    assert_eq!(catalog.warehouse(), Some("memory:///lake"));
    drop(catalog);
    again.stop()?;

    fs::remove_dir_all(&lake)?;
    Ok(())
}

#[test]
fn a_cluster_of_another_version_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let lake = path();
    let binaries = Binaries::find()?;
    Cluster::init(&lake, binaries.clone())?;
    fs::write(lake.join("postgres/PG_VERSION"), "16\n")?;
    match Cluster::open(&lake, binaries) {
        Err(Error::Version { found }) => assert_eq!(found, "16"),
        other => panic!("{other:?}"),
    }
    fs::remove_dir_all(&lake)?;
    Ok(())
}

#[test]
fn binaries_of_another_version_are_refused() {
    let none = Binaries::at(path());
    assert!(matches!(none, Err(Error::Binaries(_))), "{none:?}");
}

/// What the tests below run as a process of its own: it brings up the server of the lake in `LAKE`, says how to reach it
/// and leaves.
#[test]
#[ignore = "run by the tests below"]
fn a_process_that_uses_a_lake() {
    let Ok(lake) = std::env::var("LAKE") else {
        return;
    };
    let cluster = Cluster::open(lake.as_ref(), Binaries::find().unwrap()).unwrap();
    println!("connection: {}", cluster.ensure().unwrap());
}

/// A command for that process.
fn process(lake: &std::path::Path) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "a_process_that_uses_a_lake", "--ignored", "--nocapture"])
        .env("LAKE", lake);
    command
}

/// A lake whose server is stopped when the test ends, however it ends.
struct Lake {
    path: PathBuf,
    cluster: Cluster,
}

impl Lake {
    fn new() -> Self {
        let path = path();
        let cluster = Cluster::init(&path, Binaries::find().unwrap()).unwrap();
        Self { path, cluster }
    }
}

impl Drop for Lake {
    fn drop(&mut self) {
        let _ = self.cluster.stop();
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn many_processes_bring_up_one_server_that_outlives_them() {
    let lake = Lake::new();
    let children: Vec<_> = (0..10)
        .map(|_| process(&lake.path).stdout(Stdio::piped()).spawn().unwrap())
        .collect();
    let mut connections = BTreeSet::new();
    for child in children {
        let output = child.wait_with_output().unwrap();
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        assert!(output.status.success(), "{text}");
        let connection = text.lines().find_map(|l| l.split_once("connection: "));
        connections.insert(connection.unwrap().1.to_string());
    }
    assert_eq!(connections.len(), 1, "{connections:?}");
    assert!(
        lake.cluster.is_running().unwrap(),
        "every process is gone, and the server is not"
    );
}

#[cfg(windows)]
#[test]
fn a_server_outlives_the_console_that_started_it() {
    use std::os::windows::process::CommandExt;
    const CREATE_NEW_CONSOLE: u32 = 0x10;

    let lake = Lake::new();
    let status = process(&lake.path).creation_flags(CREATE_NEW_CONSOLE).status().unwrap();
    assert!(status.success());
    std::thread::sleep(Duration::from_secs(2));
    assert!(lake.cluster.is_running().unwrap());
}

#[tokio::test]
async fn a_server_that_died_comes_back_with_what_was_committed() -> Result<(), Box<dyn std::error::Error>> {
    let lake = Lake::new();
    let connection = lake.cluster.ensure()?;
    let config = CatalogConfig::new(connection, "metalake").warehouse("memory:///lake");
    let catalog = Catalog::init(config, None).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["default"]).commit().await?;
    let committed = main.history(10).await?;
    assert_eq!(committed.len(), 1);
    drop(catalog);

    // As a crash would, with no shutdown.
    let pid = fs::read_to_string(lake.path.join("postgres/postmaster.pid"))?;
    let pid = pid.lines().next().unwrap().trim().to_string();
    let killed = match cfg!(windows) {
        true => Command::new("taskkill").args(["/F", "/T", "/PID", &pid]).output()?,
        false => Command::new("kill").args(["-9", &pid]).output()?,
    };
    assert!(killed.status.success(), "{}", String::from_utf8_lossy(&killed.stderr));
    for _ in 0..100 {
        if !lake.cluster.is_running()? {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(!lake.cluster.is_running()?);

    let connection = lake.cluster.ensure()?;
    let catalog = Catalog::connect(CatalogConfig::new(connection, "metalake")).await?;
    assert_eq!(catalog.branch("main").await?.history(10).await?.len(), committed.len());
    drop(catalog);
    Ok(())
}

#[test]
fn stopping_is_safe_to_repeat() {
    let lake = Lake::new();
    lake.cluster.stop().unwrap();
    lake.cluster.ensure().unwrap();
    lake.cluster.stop().unwrap();
    lake.cluster.stop().unwrap();
    assert!(!lake.cluster.is_running().unwrap());
}
