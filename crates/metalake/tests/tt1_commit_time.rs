//! TT-1 on a Postgres that stops keeping commit timestamps, as `sql/commit-time.sh` runs it on one server in two
//! phases: a catalog made while it keeps them (`METALAKE_TT1=kept`); then, the server restarted without them
//! (`METALAKE_TT1=lost`), a new catalog refused with nothing of it made, and the first one read and never written.
//! Run any other way, it checks nothing.

mod common;

use common::url;
use metalake::{Catalog, CatalogConfig};

/// The catalog made while the server keeps commit timestamps.
const KEPT: &str = "tt1_kept";
/// The catalog asked for once it keeps none.
const REFUSED: &str = "tt1_refused";

#[tokio::test]
async fn tt1_commit_time_kept_then_lost() {
    match std::env::var("METALAKE_TT1").as_deref() {
        Ok("kept") => kept().await,
        Ok("lost") => lost().await,
        _ => {}
    }
}

async fn kept() {
    let config = CatalogConfig::new(url(), KEPT).warehouse("memory:///tt1");
    let catalog = Catalog::init(config, None).await.unwrap();
    assert!(catalog.keeps_commit_time());
    let main = catalog.branch("main").await.unwrap();
    main.transaction().create_namespace(&["shop"]).commit().await.unwrap();
}

async fn lost() {
    // A new catalog: refused before anything of it is made.
    let config = CatalogConfig::new(url(), REFUSED).warehouse("memory:///tt1");
    let refused = Catalog::init(config.clone(), None).await.unwrap_err().to_string();
    assert!(
        refused.starts_with("Unsupported (TT-1): this Postgres keeps no commit timestamps"),
        "{refused}"
    );
    let none = Catalog::connect(config).await.unwrap_err().to_string();
    assert!(none.contains("does not exist"), "{none}");

    // The catalog made before: read, and refused a commit, which would have no time.
    let catalog = Catalog::connect(CatalogConfig::new(url(), KEPT)).await.unwrap();
    assert!(!catalog.keeps_commit_time());
    let main = catalog.branch("main").await.unwrap();
    let names =
        |rows: Vec<metalake_pg::records::NamespaceRow>| rows.into_iter().map(|r| r.namespace).collect::<Vec<_>>();
    let before = names(main.namespaces().await.unwrap());
    assert!(before.contains(&vec!["shop".to_string()]), "{before:?}");
    let write = main
        .transaction()
        .create_namespace(&["more"])
        .commit()
        .await
        .unwrap_err()
        .to_string();
    assert!(write.starts_with("Unsupported (TT-1)"), "{write}");
    assert_eq!(names(main.namespaces().await.unwrap()), before);
}
