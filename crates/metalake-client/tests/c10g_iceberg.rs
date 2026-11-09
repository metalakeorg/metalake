//! C10g: when each table's Iceberg metadata is written, as every face sets it (REST-9): in one commit for the tables
//! named, on one ref, all of them or none, and only where it changes; and where a table set to `on-commit` stands.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_schema::{DataType, Field, Schema};
use metalake::{ICEBERG_METADATA, NewTable, Ref};
use metalake_client::{Kind, Lake};

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake-client bash sql/test.sh")
}

/// A lake on the tests' Postgres, its file and warehouse in a directory of its own, with tables `names` in `db`.
async fn lake(names: &[&str]) -> (Lake, PathBuf) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let schema = format!("client_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
    let dir = std::env::temp_dir().join(&schema);
    let warehouse = dir.join("data");
    let lake = Lake::create(
        &url(),
        Some(&warehouse.to_string_lossy()),
        Some(&schema),
        Some(&dir),
        Some(2),
    )
    .await
    .expect("a lake");
    let main = lake.catalog.branch("main").await.unwrap();
    main.transaction().create_namespace(&["db"]).commit().await.unwrap();
    let shape = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    for name in names {
        let (tx, _) = main
            .transaction()
            .create_table(NewTable {
                namespace: &["db"],
                name,
                schema: &shape,
                location: None,
                cluster_key: vec![],
                partitioning: vec![],
                properties: vec![],
            })
            .unwrap();
        tx.commit().await.unwrap();
    }
    (lake, dir)
}

/// The value of the property of `name` on `on`.
async fn value(on: &Ref, name: &str) -> Option<String> {
    let t = on.table(&["db"], name).await.unwrap();
    let definition = on.definition(&t, None).await.unwrap();
    definition["properties"][ICEBERG_METADATA].as_str().map(str::to_string)
}

#[tokio::test]
async fn rest9_on_commit_one_commit_for_all() {
    let (lake, dir) = lake(&["a", "b", "c"]).await;
    let main = lake.catalog.branch("main").await.unwrap();
    let before = main.history(1).await.unwrap()[0].commit_seq;

    let seq = lake.iceberg().on_commit("main", &["db.a", "db.b"]).await.unwrap();
    let history = main.history(5).await.unwrap();
    assert_eq!(seq, Some(history[0].commit_seq));
    assert_eq!(history[1].commit_seq, before, "one commit for both");
    assert_eq!(value(&main, "a").await.as_deref(), Some("on-commit"));
    assert_eq!(value(&main, "b").await.as_deref(), Some("on-commit"));
    assert_eq!(value(&main, "c").await, None);

    // again, nothing changes; with one more, only that one does
    assert_eq!(lake.iceberg().on_commit("main", &["db.a", "db.b"]).await.unwrap(), None);
    assert!(
        lake.iceberg()
            .on_commit("main", &["db.a", "db.c"])
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(value(&main, "c").await.as_deref(), Some("on-commit"));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn rest9_on_commit_missing_table_refuses_all() {
    let (lake, dir) = lake(&["a"]).await;
    let main = lake.catalog.branch("main").await.unwrap();
    let before = main.history(1).await.unwrap()[0].commit_seq;
    let refused = lake
        .iceberg()
        .on_commit("main", &["db.a", "db.nowhere"])
        .await
        .unwrap_err();
    assert_eq!(refused.kind, Kind::Failed);
    assert!(refused.to_string().contains("db.nowhere"), "{refused}");
    assert_eq!(
        main.history(1).await.unwrap()[0].commit_seq,
        before,
        "nothing committed"
    );
    assert_eq!(value(&main, "a").await, None);
    let malformed = lake.iceberg().on_commit("main", &["a"]).await.unwrap_err();
    assert_eq!(malformed.kind, Kind::Usage);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn rest9_on_load_branch_isolates() {
    let (lake, dir) = lake(&["a"]).await;
    lake.iceberg().on_commit("main", &["db.a"]).await.unwrap();
    let main = lake.catalog.branch("main").await.unwrap();
    let dev = main.create_branch("dev").await.unwrap();
    assert_eq!(value(&dev, "a").await.as_deref(), Some("on-commit"), "inherited");

    assert!(lake.iceberg().on_load("dev", &["db.a"]).await.unwrap().is_some());
    assert_eq!(value(&dev, "a").await, None, "gone on dev");
    assert_eq!(value(&main, "a").await.as_deref(), Some("on-commit"), "main keeps it");
    assert_eq!(lake.iceberg().on_load("dev", &["db.a"]).await.unwrap(), None, "already");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn rest6_status_reports_unwritten_commits() {
    let (lake, dir) = lake(&["a", "b"]).await;
    let seq = lake.iceberg().on_commit("main", &["db.a"]).await.unwrap().unwrap();
    let status = lake.iceberg().status("main").await.unwrap();
    assert_eq!(status.len(), 1, "only the table set: {status:?}");
    let a = &status[0];
    assert_eq!(a.table, "db.a");
    assert_eq!(
        (a.head, a.written, a.metadata.as_deref()),
        (Some(seq), None, None),
        "no service ran"
    );
    assert_eq!(a.behind, 2, "its creation and the commit that set it");
    assert!(a.since.is_some());

    // a metadata file that serves the head, as the service writes it: nothing behind
    let main = lake.catalog.branch("main").await.unwrap();
    let location = main.table(&["db"], "a").await.unwrap().location().to_string();
    let local = location.trim_start_matches("file://").trim_start_matches('/');
    let local = if cfg!(windows) {
        local.to_string()
    } else {
        format!("/{local}")
    };
    let metadata = PathBuf::from(local).join("metalake-metadata");
    std::fs::create_dir_all(&metadata).unwrap();
    std::fs::write(metadata.join(format!("v{}-0f.metadata.json", seq - 1)), "{}").unwrap();
    assert_eq!(
        lake.iceberg().status("main").await.unwrap()[0].behind,
        1,
        "one commit after it"
    );
    std::fs::write(metadata.join(format!("v{seq}-1f.metadata.json")), "{}").unwrap();
    let a = &lake.iceberg().status("main").await.unwrap()[0];
    assert_eq!(
        (a.written, a.behind, a.since.as_deref()),
        (Some(seq), 0, None),
        "up to date"
    );
    assert!(
        a.metadata
            .as_deref()
            .is_some_and(|m| m.ends_with(&format!("v{seq}-1f.metadata.json")))
    );
    let _ = std::fs::remove_dir_all(dir);
}
