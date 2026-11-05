//! C10e1 (ADR 0003 §7): a lake's own Postgres: real binaries, a real cluster, and the catalog on it. The binaries are
//! fetched the first time, so this needs the network once. Run by `cargo test -p metalake-local`.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

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
    cluster.start()?;
    println!("start {:?}", began.elapsed());
    assert!(cluster.is_running()?);

    // `init` fails without `btree_gist`, which the format's tables need, and `track_commit_timestamp`.
    let began = Instant::now();
    let config = CatalogConfig::new(cluster.connection()?, "metalake").warehouse("memory:///lake");
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
        again.start()?;
        starts.push(began.elapsed());
        again.stop()?;
    }
    println!("starts {starts:?}");
    again.start()?;
    let catalog = Catalog::connect(CatalogConfig::new(again.connection()?, "metalake")).await?;
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
