//! C1b, C1c (ADR 0003 §7): the SDK's calls against a real catalog. A scene goes through every call of the interface
//! once and checks what each answers; the calls are held to the functions in `pg_proc`; refusals arrive with their
//! kind and rule; and only the SDK depends on the Postgres driver.

use std::time::{Duration, SystemTime};

use futures_util::TryStreamExt;
use metalake_pg::records::{FileRef, TableExpect};
use metalake_pg::{Commit, CreateRef, ErrorKind, INTERFACE, Merge, Protocol, Prune, Retention};
use metalake_protocol_tests::records::{encode_long, stored_file};
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::json;
use uuid::Uuid;

fn create(t: Uuid) -> Commit {
    Commit {
        ref_id: 1,
        operation: "append".into(),
        files: vec![stored_file(t, "gs://lake/a.parquet".into(), 10, 1, 0, 9)],
        changes: Some(json!({ t.to_string(): {
            "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
            "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
            "cluster_key": [1],
        }})),
        app_id: Some("c1".into()),
        app_version: Some(1),
        ..Default::default()
    }
}

fn append(ref_id: i32, t: Uuid, path: &str, lo: i64) -> Commit {
    Commit {
        ref_id,
        operation: "append".into(),
        files: vec![stored_file(t, path.into(), 10, 1, lo, lo + 9)],
        ..Default::default()
    }
}

#[tokio::test]
async fn files_at_paths_by_ref() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    p.commit(&on, &create(t)).await?;
    let branch = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    p.commit(&on, &append(branch, t, "gs://lake/dev.parquet", 100)).await?;
    p.commit(&on, &append(1, t, "gs://lake/main.parquet", 200)).await?;

    let names = [
        "gs://lake/a.parquet",
        "gs://lake/dev.parquet",
        "gs://lake/main.parquet",
        "gs://lake/none.parquet",
    ];
    let names = names.map(String::from);
    let paths = |found: Vec<(Option<String>, i64)>| {
        let mut paths: Vec<String> = found.into_iter().filter_map(|(path, _)| path).collect();
        paths.sort();
        paths
    };
    let on_main = p.files_at_paths(&on, 1, t, &names, &[]).await?;
    let id = on_main
        .iter()
        .find(|(path, _)| path.as_ref() == Some(&names[0]))
        .unwrap()
        .1;
    assert_eq!(paths(on_main), [names[0].clone(), names[2].clone()]);
    let on_dev = p.files_at_paths(&on, branch, t, &names, &[]).await?;
    assert_eq!(paths(on_dev), [names[0].clone(), names[1].clone()]);
    assert_eq!(p.files_at_paths(&on, 1, t, &[], &[id]).await?.len(), 1);
    assert_eq!(p.files_at_paths(&on, 1, t, &[], &[]).await?.len(), 0);

    let removal = Commit {
        ref_id: 1,
        operation: "delete".into(),
        removed: vec![FileRef {
            table_id: t,
            file_id: id,
        }],
        ..Default::default()
    };
    p.commit(&on, &removal).await?;
    assert_eq!(p.files_at_paths(&on, 1, t, &names[..1], &[]).await?.len(), 0);
    assert_eq!(
        p.files_at_paths(&on, branch, t, &names[..1], &[]).await?.len(),
        1,
        "dev forked before it"
    );
    Ok(())
}

#[tokio::test]
async fn commit_of_key_by_ref() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    assert_eq!(p.commit_of_key(&on, 1, "c1").await?, None);
    assert_eq!(p.key_version(&on, 1, "c1").await?, None);
    let applied = p.commit(&on, &create(t)).await?;
    assert_eq!(p.commit_of_key(&on, 1, "c1").await?, Some(applied.commit_seq));
    assert_eq!(p.key_version(&on, 1, "c1").await?, Some(1));
    let branch = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    assert_eq!(p.commit_of_key(&on, branch, "c1").await?, None);

    let mut stale = append(1, t, "gs://lake/b.parquet", 0);
    stale.expected = vec![TableExpect {
        table_id: t,
        snapshot_id: None,
    }];
    stale.app_id = Some("refused".into());
    stale.app_version = Some(1);
    assert!(p.commit(&on, &stale).await.is_err());
    assert_eq!(
        p.commit_of_key(&on, 1, "refused").await?,
        None,
        "a refusal leaves no key"
    );
    Ok(())
}

#[tokio::test]
async fn ftr1_add_footers_by_path() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    p.commit(&on, &create(t)).await?;
    let mut unknown = append(1, t, "gs://lake/b.parquet", 0);
    unknown.files[0].footer_size = 0;
    p.commit(&on, &unknown).await?;

    // b's footer is not known, so any footer is stored; a's is 10,000 bytes, and another length is not; c is no file
    let paths = ["gs://lake/b.parquet", "gs://lake/a.parquet", "gs://lake/c.parquet"].map(String::from);
    let footers = vec![vec![7u8; 77], vec![1u8; 5], vec![2u8; 5]];
    assert_eq!(p.add_footers(&on, t, &paths, &footers).await?, 1);
    assert_eq!(
        p.add_footers(&on, t, &paths, &footers).await?,
        0,
        "a repeat changes nothing"
    );
    let other = vec![vec![9u8; 3], vec![1u8; 5], vec![2u8; 5]];
    assert_eq!(
        p.add_footers(&on, t, &paths, &other).await?,
        0,
        "a footer once there stays"
    );
    let stored: Vec<i32> = on
        .query(
            &format!("select length(footer) from {}.file_footer", quote_ident(&schema)),
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(stored, [77]);

    // an inline file always knows its footer_size (STA-1)
    let mut inline = append(1, t, "gs://lake/d.parquet", 0);
    inline.files[0].path = None;
    inline.files[0].content = Some(vec![0; 100]);
    inline.files[0].file_size = 100;
    inline.files[0].footer_size = 0;
    let refused = p.commit(&on, &inline).await;
    assert!(
        refused.as_ref().is_err_and(|e| e.rule() == Some("STA-1")),
        "{refused:?}"
    );
    Ok(())
}

#[tokio::test]
async fn rest6_added_files_of_matches_added_files() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    let first = p.commit(&on, &create(t)).await?.commit_seq;
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    let on_dev = p
        .commit(&on, &append(dev, t, "gs://lake/dev.parquet", 100))
        .await?
        .commit_seq;
    let second = p
        .commit(&on, &append(1, t, "gs://lake/b.parquet", 200))
        .await?
        .commit_seq;
    let mut two = append(1, t, "gs://lake/c.parquet", 300);
    two.files
        .push(stored_file(t, "gs://lake/d.parquet".into(), 10, 1, 400, 409));
    let third = p.commit(&on, &two).await?.commit_seq;

    let shown = |files: Vec<metalake_pg::records::ListedFile>| -> Vec<(i64, i64, Option<String>)> {
        files.into_iter().map(|f| (f.begin_seq, f.file_id, f.path)).collect()
    };
    let one_by_one = async |r: i32, ats: &[i64]| -> Result<Vec<(i64, i64, Option<String>)>, Error> {
        let mut all = Vec::new();
        for at in ats {
            all.extend(shown(p.added_files(&on, r, t, *at).await?.try_collect().await?));
        }
        Ok(all)
    };
    let at_once = async |r: i32, ats: &[i64]| -> Result<Vec<(i64, i64, Option<String>)>, Error> {
        Ok(shown(p.added_files_of(&on, r, t, ats).await?.try_collect().await?))
    };

    // the same as a call a commit, in the order of commits and then of files, whatever order they are asked in
    let main = [third, first, second];
    let mut ordered = main;
    ordered.sort_unstable();
    let found = at_once(1, &main).await?;
    assert_eq!(found, one_by_one(1, &ordered).await?);
    let paths: Vec<_> = found.iter().filter_map(|(.., path)| path.as_deref()).collect();
    assert_eq!(
        paths,
        [
            "gs://lake/a.parquet",
            "gs://lake/b.parquet",
            "gs://lake/c.parquet",
            "gs://lake/d.parquet"
        ]
    );
    // a commit of another ref adds nothing here, and nothing asked finds nothing
    assert!(at_once(1, &[on_dev]).await?.is_empty());
    assert_eq!(at_once(dev, &[on_dev]).await?, one_by_one(dev, &[on_dev]).await?);
    assert!(at_once(1, &[]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn chg1_commit_tables_many_commits() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let (t, u) = (Uuid::from_u128(0xc1), Uuid::from_u128(0xc2));
    let first = p.commit(&on, &create(t)).await?.commit_seq;
    let mut other = create(u);
    other.app_id = None;
    other.files = vec![stored_file(u, "gs://lake/u.parquet".into(), 10, 1, 0, 9)];
    other.changes = Some(json!({ u.to_string(): {
        "create": {"namespace": ["db"], "name": "u", "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
    }}));
    p.commit(&on, &other).await?;
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    let mut both = append(dev, t, "gs://lake/t2.parquet", 100);
    both.files
        .push(stored_file(u, "gs://lake/u2.parquet".into(), 10, 1, 100, 109));
    let on_dev = p.commit(&on, &both).await?.commit_seq;
    // a property set alone makes a snapshot too, so that a follower sees a table marked by its commit
    let property = Commit {
        ref_id: 1,
        operation: "update".into(),
        changes: Some(json!({ t.to_string(): {"properties": {"a": "1"}} })),
        ..Default::default()
    };
    let defined = p.commit(&on, &property).await?.commit_seq;

    let found = p.commit_tables(&on, &[on_dev, defined, first, 999_999]).await?;
    assert_eq!(
        found,
        [(first, 1, t), (on_dev, dev, t), (on_dev, dev, u), (defined, 1, t)]
    );
    assert!(p.commit_tables(&on, &[]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn tbl2_tables_with_property_by_ref() -> Result<(), Error> {
    const KEY: &str = "metalake.iceberg.metadata";
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let (t, u) = (Uuid::from_u128(0xc1), Uuid::from_u128(0xc2));
    p.commit(&on, &create(t)).await?;
    let mut other = create(u);
    other.app_id = None;
    other.files = vec![];
    other.changes = Some(json!({ u.to_string(): {
        "create": {"namespace": ["db"], "name": "u", "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
    }}));
    p.commit(&on, &other).await?;
    let set = |r: i32, table: Uuid, value: serde_json::Value| Commit {
        ref_id: r,
        operation: "update".into(),
        changes: Some(json!({ table.to_string(): {"properties": {KEY: value}} })),
        ..Default::default()
    };
    let marked =
        async || -> Result<Vec<(i32, Uuid)>, Error> { Ok(p.tables_with_property(&on, KEY, "on-commit").await?) };
    assert!(marked().await?.is_empty(), "nothing marked");

    // main marks t; a branch made after sees it marked, and one made before
    let before = p.create_ref(&on, &CreateRef::branch("before", 1)).await?;
    p.commit(&on, &set(1, t, json!("on-commit"))).await?;
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    assert_eq!(
        marked().await?,
        [(1, t), (dev, t)],
        "a branch inherits, one forked before does not"
    );
    let _ = before;

    // the branch unmarks it for itself; main still has it; u is marked on the branch alone
    p.commit(&on, &set(dev, t, json!("on-load"))).await?;
    p.commit(&on, &set(dev, u, json!("on-commit"))).await?;
    assert_eq!(marked().await?, [(1, t), (dev, u)]);

    // removed on main, it is no longer marked there
    p.commit(&on, &set(1, t, json!(null))).await?;
    assert_eq!(marked().await?, [(dev, u)]);

    // a table dropped, and a ref deleted, are marked nowhere
    p.commit(&on, &set(1, t, json!("on-commit"))).await?;
    let drop = Commit {
        ref_id: 1,
        operation: "update".into(),
        changes: Some(json!({ t.to_string(): {"drop": true} })),
        ..Default::default()
    };
    p.commit(&on, &drop).await?;
    p.delete_ref(&on, dev).await?;
    assert!(marked().await?.is_empty(), "dropped on main, and dev deleted");
    Ok(())
}

#[tokio::test]
async fn tbl1_table_named_by_ref() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    p.commit(&on, &create(t)).await?;
    let db = ["db".to_string()];
    let named = |r: i32, name: &'static str| {
        let (p, on, db) = (&p, &on, &db);
        async move { p.table_named(on, r, db, name, None).await.map(|f| f.map(|(id, _)| id)) }
    };
    let definition = |change: serde_json::Value| Commit {
        ref_id: 1,
        operation: "update".into(),
        changes: Some(json!({ t.to_string(): change })),
        ..Default::default()
    };

    // a tag sees what main saw; a branch renames the table, which main still has by its old name
    let tag = CreateRef {
        kind: "tag".into(),
        ..CreateRef::branch("q3", 1)
    };
    let tag = p.create_ref(&on, &tag).await?;
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    let mut renamed = definition(json!({"rename": {"namespace": ["db"], "name": "u"}}));
    renamed.ref_id = dev;
    p.commit(&on, &renamed).await?;
    assert_eq!(named(dev, "t").await?, None, "renamed on the branch");
    assert_eq!(named(dev, "u").await?, Some(t));
    assert_eq!(named(1, "t").await?, Some(t), "main keeps its name");
    assert_eq!(named(1, "u").await?, None);

    // main drops it, and a new table takes the name; the tag still sees the first
    p.commit(&on, &definition(json!({"drop": true}))).await?;
    assert_eq!(named(1, "t").await?, None, "dropped");
    let other = Uuid::from_u128(0xc2);
    let mut again = create(other);
    again.app_id = None;
    again.changes = Some(json!({ other.to_string(): {
        "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
        "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
    }}));
    again.files = vec![];
    p.commit(&on, &again).await?;
    assert_eq!(named(1, "t").await?, Some(other), "the name's new table");
    assert_eq!(named(tag, "t").await?, Some(t), "the tag's table");

    // at a sequence, the sequence given; at the latest, the read point tables_latest gives
    let latest = p.tables_latest(&on, &[1]).await?;
    let seq = latest.iter().find(|l| l.name == "t").unwrap().seq;
    assert_eq!(p.table_named(&on, 1, &db, "t", None).await?, Some((other, seq)));
    assert_eq!(
        p.table_named(&on, tag, &db, "t", Some(seq)).await?.map(|(_, s)| s),
        Some(seq)
    );
    Ok(())
}

#[tokio::test]
async fn calls_match_functions() -> Result<(), Error> {
    let mut on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);

    // Writing: a commit, and the same one again under its idempotency key.
    let first = p.commit(&on, &create(t)).await?;
    assert!(first.applied);
    let again = p.commit(&on, &create(t)).await?;
    assert_eq!((again.commit_seq, again.applied), (first.commit_seq, false));
    assert!(p.next_id(&on, t, "field").await? > 1);
    let drawn = p.next_file_id(&on).await?;
    assert!(p.next_file_id(&on).await? > drawn, "each file id once");

    // Reading: the plan streams the file, and prunes it on its field.
    let files: Vec<_> = p.plan(&on, 1, t, None, None).await?.try_collect().await?;
    assert_eq!(
        files.iter().map(|f| f.path.as_deref()).collect::<Vec<_>>(),
        [Some("gs://lake/a.parquet")]
    );
    assert!(files[0].vector.is_none());
    let miss = Prune {
        field_id: 1,
        lo: Some(encode_long(20)),
        hi: Some(encode_long(30)),
    };
    assert_eq!(
        p.plan(&on, 1, t, None, Some(&miss))
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        0
    );
    assert_eq!(p.snapshot_at(&on, 1, t, None).await?, Some(first.commit_seq));
    let point = p.stable_point(&on).await?;
    assert!(point.seq >= first.commit_seq);
    p.is_stable(&on, point.horizon).await?;

    // Branches: a branch, a commit on it, the report of its merge, and the merge.
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    p.commit(&on, &append(dev, t, "gs://lake/b.parquet", 10)).await?;
    let report = p.merge_report(&on, &Merge::of(dev)).await?;
    assert!(report.is_object(), "{report}");
    let merged = p.merge(&on, &Merge::of(dev)).await?.expect("a commit to merge");
    assert!(merged.applied);
    assert_eq!(p.merge(&on, &Merge::of(dev)).await?, None, "nothing left to merge");
    let changed = p.table_changes(&on, 1, t, first.commit_seq, merged.commit_seq).await?;
    assert_eq!(
        changed
            .iter()
            .map(|c| (c.change.as_str(), c.path.as_deref()))
            .collect::<Vec<_>>(),
        [("added", Some("gs://lake/b.parquet"))]
    );
    let of_commit = p.commit_changes(&on, 1, t, merged.commit_seq).await?;
    assert_eq!(
        of_commit
            .iter()
            .map(|c| (c.change.as_str(), c.path.as_deref(), c.record_count))
            .collect::<Vec<_>>(),
        [("added", Some("gs://lake/b.parquet"), 10)]
    );

    // Time: the feed and a read as of a time deliver only what committed more than a second ago.
    std::thread::sleep(Duration::from_millis(1200));
    assert!(p.fill_committed_at(&on, None).await? >= 1);
    let feed = p.changes(&on, None, None, None).await?;
    assert_eq!(feed.first().map(|c| c.commit_seq), Some(first.commit_seq));
    assert_eq!(p.changes(&on, None, Some(1), Some(t)).await?.len(), 1);
    let at = p.commit_time(&on, merged.commit_seq).await?;
    assert_eq!(feed.last().map(|c| c.committed_at), Some(at));
    let now = SystemTime::now() - Duration::from_millis(1100);
    assert_eq!(p.as_of_time(&on, 1, t, now).await?, Some(merged.commit_seq));

    // The tables now: a table's sequence is its latest snapshot, and a branch reads what it inherited from its base.
    let child = p.create_ref(&on, &CreateRef::branch("child", 1)).await?;
    let latest = p.tables_latest(&on, &[1, child]).await?;
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].seq, merged.commit_seq);
    assert!(latest[1].seq > merged.commit_seq, "the start of the base, not below it");
    let planned = |r: i32, seq: i64| {
        let (p, on) = (&p, &on);
        async move { p.plan(on, r, t, Some(seq), None).await?.try_collect::<Vec<_>>().await }
    };
    assert_eq!(planned(child, latest[1].seq).await?.len(), 2, "the files it inherited");

    // A commit in flight is not in it, though it has drawn a higher sequence.
    let other = connect().await?;
    other.batch_execute("begin").await?;
    let flying = p.commit(&other, &append(1, t, "gs://lake/c.parquet", 10)).await?;
    assert!(flying.commit_seq > latest[0].seq);
    assert_eq!(p.tables_latest(&on, &[1]).await?[0].seq, latest[0].seq);
    other.batch_execute("commit").await?;
    assert_eq!(p.tables_latest(&on, &[1]).await?[0].seq, flying.commit_seq);

    // Intents: an intent that expires at once is an orphan, and is forgotten.
    let path = vec!["gs://lake/orphan.parquet".to_string()];
    p.register_intents(&on, 1, t, &path, Duration::ZERO).await?;
    assert_eq!(p.orphans(&on, None).await?, path);
    assert_eq!(p.forget_intents(&on, &path).await?, 1);

    // Retention: main keeps its latest snapshot; expiring collects; nothing is due before the grace.
    p.delete_ref(&on, dev).await?;
    p.set_retention(
        &on,
        1,
        &Retention {
            min_snapshots: Some(1),
            ..Default::default()
        },
    )
    .await?;
    while p.expire(&on, Some(10)).await? > 0 {}
    let tx = on.transaction().await?;
    assert_eq!(p.sweep_due(&tx, None).await?, Vec::<String>::new());
    assert_eq!(p.sweep_done(&tx, &[]).await?, 0);
    tx.commit().await?;

    on.batch_execute(&format!("drop schema {} cascade", quote_ident(&schema)))
        .await?;
    Ok(())
}

#[tokio::test]
async fn refusal_error_kind_and_rule() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc2);
    p.commit(&on, &create(t)).await?;

    // A commit that expects the table to have no snapshot conflicts with the one that made it (CMT-7).
    let mut stale = append(1, t, "gs://lake/c.parquet", 0);
    stale.expected = vec![TableExpect {
        table_id: t,
        snapshot_id: None,
    }];
    let conflict = p.commit(&on, &stale).await.unwrap_err();
    assert_eq!(
        (conflict.kind(), conflict.code()),
        (ErrorKind::Conflict, Some("40001")),
        "{conflict}"
    );
    assert!(conflict.rule().is_some_and(|r| r.starts_with("CMT-")), "{conflict}");
    assert!(!conflict.is_retryable());

    // Statistics that name a field twice are invalid (STA-2).
    let mut twice = append(1, t, "gs://lake/d.parquet", 0);
    let f = &mut twice.files[0];
    f.stat_field_ids = vec![1, 1];
    f.lower = vec![Some(encode_long(0)); 2];
    f.upper = vec![Some(encode_long(9)); 2];
    f.null_count = vec![Some(0); 2];
    f.nan_count = vec![None; 2];
    let missing = twice;
    let invalid = p.commit(&on, &missing).await.unwrap_err();
    assert_eq!(
        (invalid.kind(), invalid.code()),
        (ErrorKind::Invalid, Some("22023")),
        "{invalid}"
    );

    on.batch_execute(&format!("drop schema {} cascade", quote_ident(&schema)))
        .await?;
    Ok(())
}

/// A read of a long history pays for what it returns: from each head, through at most `depth` snapshots, once each.
#[tokio::test]
async fn chg3_tombstones_and_new_base() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc5);
    let first = p.commit(&on, &create(t)).await?.commit_seq;
    let file_id = p.commit_changes(&on, 1, t, first).await?[0].file_id;
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    p.commit(&on, &append(1, t, "gs://lake/main.parquet", 10)).await?;
    p.commit(&on, &append(dev, t, "gs://lake/dev.parquet", 20)).await?;
    let removal = Commit {
        ref_id: dev,
        operation: "delete".into(),
        removed: vec![FileRef { table_id: t, file_id }],
        ..Default::default()
    };
    let removed = p.commit(&on, &removal).await?.commit_seq;
    let of = |r: i32, s: i64| {
        let (p, on) = (&p, &on);
        async move {
            let mut changes: Vec<(String, Option<String>)> = p
                .commit_changes(on, r, t, s)
                .await?
                .into_iter()
                .map(|c| (c.change, c.path))
                .collect();
            changes.sort();
            Ok::<_, Error>(changes)
        }
    };
    let file = |change: &str, name: &str| (change.to_string(), Some(format!("gs://lake/{name}.parquet")));
    assert_eq!(of(dev, removed).await?, [file("removed", "a")], "by a tombstone");
    assert_eq!(of(1, removed).await?, [], "its parent keeps the file");
    let merged = p
        .merge(&on, &Merge::of(dev))
        .await?
        .expect("a commit to merge")
        .commit_seq;
    assert_eq!(of(1, merged).await?, [file("added", "dev"), file("removed", "a")]);
    assert_eq!(
        of(dev, merged).await?,
        [file("added", "main")],
        "what its new base brings"
    );
    Ok(())
}

#[tokio::test]
async fn ancestors_bounded_per_head() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc4);
    let mut seqs = vec![p.commit(&on, &create(t)).await?.commit_seq];
    for i in 0..4 {
        let commit = append(1, t, &format!("gs://lake/{i}.parquet"), i * 10);
        seqs.push(p.commit(&on, &commit).await?.commit_seq);
    }
    let found = |heads: Vec<i64>, depth: i32| {
        let (p, on) = (&p, &on);
        async move {
            let snapshots = p.ancestors(on, t, &heads, depth, None).await?;
            Ok::<_, Error>(snapshots.iter().map(|s| s.snapshot_id).collect::<Vec<_>>())
        }
    };
    assert_eq!(found(vec![seqs[4]], 3).await?, seqs[2..].to_vec());
    assert_eq!(found(vec![seqs[4]], 1).await?, [seqs[4]]);
    assert_eq!(found(vec![seqs[4]], 0).await?, Vec::<i64>::new());
    assert_eq!(found(vec![seqs[4]], 100).await?, seqs, "as far as they remain");
    assert_eq!(
        found(vec![seqs[4], seqs[1]], 2).await?,
        [seqs[0], seqs[1], seqs[3], seqs[4]],
        "each head its own"
    );
    assert_eq!(found(vec![seqs[4], seqs[3]], 2).await?, seqs[2..].to_vec(), "once each");
    let all = p.table_snapshots(&on, t).await?;
    assert_eq!(p.ancestors(&on, t, &[seqs[4]], 100, None).await?, all, "the same rows");
    let recent = p.ancestors(&on, t, &[seqs[4]], 1, Some(3_600_000)).await?;
    assert_eq!(recent, all, "those within an age, past `keep`");
    let aged = p.ancestors(&on, t, &[seqs[4]], 2, Some(0)).await?;
    assert_eq!(aged.len(), 2, "the last ones, past an age");
    on.batch_execute(&format!("drop schema {} cascade", quote_ident(&schema)))
        .await?;
    Ok(())
}

/// CMT-12 against the protocol's own function: a commit adds keys to a summary, those of the snapshot prevail, and keys
/// alone touch the table.
#[tokio::test]
async fn cmt12_summary_keeps_keys() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let q = quote_ident(&schema);
    let t = Uuid::from_u128(0xc3);
    p.commit(&on, &create(t)).await?;

    let keys = json!({t.to_string(): {"engine": "spark", "epoch": "7", "operation": "delete"}});
    let call = format!("select commit_seq from {q}.commit(ref_id => 1, operation => 'append', summaries => $1)");
    let seq: i64 = on.query_one(&call, &[&keys]).await?.get(0);
    let read = format!("select summary, parent_snapshot_id is not null from {q}.snapshot where snapshot_id = $1");
    let row = on.query_one(&read, &[&seq]).await?;
    let (summary, chained): (serde_json::Value, bool) = (row.get(0), row.get(1));
    assert_eq!(
        (summary["engine"].as_str(), summary["epoch"].as_str()),
        (Some("spark"), Some("7"))
    );
    assert_eq!(summary["operation"], "append");
    assert!(chained);

    for bad in [
        json!([1]),
        json!({t.to_string(): [1]}),
        json!({t.to_string(): {"epoch": 7}}),
        json!({t.to_string(): {"a": "x".repeat(65536)}}),
    ] {
        let refused = on.query_one(&call, &[&bad]).await.unwrap_err();
        assert_eq!(refused.code().map(|c| c.code()), Some("22023"), "{bad}");
    }
    on.batch_execute(&format!("drop schema {q} cascade")).await?;
    Ok(())
}

/// Every call has its function, with the same parameters in the same order, and the calls marked as writing are
/// those `writing_functions()` grants to writers alone.
#[tokio::test]
async fn calls_cover_catalog_functions() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    for (name, parameters, _) in INTERFACE {
        let found: Vec<Vec<String>> = on
            .query(
                "select case when p.proargmodes is null then coalesce(p.proargnames, '{}')
                        else array(select n from unnest(p.proargnames, p.proargmodes) u(n, m) where m in ('i', 'b')) end
                   from pg_proc p where p.pronamespace = $1::text::regnamespace and p.proname = $2",
                &[&p, name],
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(
            found,
            [parameters.iter().map(|s| s.to_string()).collect::<Vec<_>>()],
            "{name}"
        );
    }
    let mut writing: Vec<String> = on
        .query(
            &format!("select (f::regproc)::text from {p}.writing_functions() f"),
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get::<_, String>(0).rsplit('.').next().unwrap().to_string())
        .collect();
    writing.sort();
    let mut marked: Vec<String> = INTERFACE
        .iter()
        .filter(|(_, _, w)| *w)
        .map(|(n, _, _)| n.to_string())
        .collect();
    marked.sort();
    assert_eq!(writing, marked);
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    Ok(())
}

/// No SQL outside the SDK (ADR 0003 §3.5): no crate but the SDK and this harness depends on the Postgres driver.
#[test]
fn driver_dependency_limited_to_sdk() {
    let output = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata");
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).expect("its JSON");
    let mut users: Vec<&str> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| {
            p["dependencies"].as_array().unwrap().iter().any(|d| {
                ["tokio-postgres", "postgres", "sqlx", "deadpool-postgres"].contains(&d["name"].as_str().unwrap())
            })
        })
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    users.sort();
    assert_eq!(users, ["metalake-pg", "metalake-protocol-tests"]);
}

#[tokio::test]
async fn cmt5_expected_key_version() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);
    p.commit(&on, &create(t)).await?;
    let keyed = |path: &str, version: i64, seen: Option<i64>| Commit {
        app_id: Some("writer".into()),
        app_version: Some(version),
        expected_app_version: seen,
        ..append(1, t, path, 0)
    };

    // From where it left the key, a writer commits; another one moves the key past it.
    let first = p.commit(&on, &keyed("gs://lake/1.parquet", 1, Some(0))).await?;
    assert!(first.applied);
    let other = p.commit(&on, &keyed("gs://lake/5.parquet", 5, None)).await?;

    // The first writer, still at 1, is refused, and leaves the key where the other one put it.
    let refused = p
        .commit(&on, &keyed("gs://lake/6.parquet", 6, Some(1)))
        .await
        .unwrap_err();
    assert_eq!(
        (refused.kind(), refused.code()),
        (ErrorKind::Conflict, Some("40001")),
        "{refused}"
    );
    assert_eq!(refused.rule(), Some("CMT-5"), "{refused}");
    assert_eq!(p.key_version(&on, 1, "writer").await?, Some(5));

    // A version the key passed answers the commit stored, whatever its writer saw.
    let again = p.commit(&on, &keyed("gs://lake/1.parquet", 1, Some(0))).await?;
    assert_eq!((again.commit_seq, again.applied), (other.commit_seq, false));
    assert!(p.commit(&on, &keyed("gs://lake/6.parquet", 6, Some(5))).await?.applied);

    // Without a key, or below 0, an expected version means nothing.
    for wrong in [
        Commit {
            expected_app_version: Some(0),
            ..append(1, t, "gs://lake/x.parquet", 0)
        },
        keyed("gs://lake/x.parquet", 7, Some(-1)),
    ] {
        let invalid = p.commit(&on, &wrong).await.unwrap_err();
        assert_eq!(
            (invalid.kind(), invalid.code()),
            (ErrorKind::Invalid, Some("22023")),
            "{invalid}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cmt5_commit_on_catalog_before_expected_key() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c1");
    install_protocol(&on, &schema).await?;
    // A catalog whose protocol predates expected_app_version: its commit takes the first fifteen arguments only.
    let s = quote_ident(&schema);
    let old = format!(
        "alter function {s}.commit(int, text, {s}.file_add[], {s}.file_ref[], {s}.vector_set[], {s}.table_expect[], \
           jsonb, text, text, text, bigint, text, text, jsonb, jsonb, bigint) rename to commit_since;
         create function {s}.commit(ref_id int, operation text, files {s}.file_add[] default '{{}}',
           removed {s}.file_ref[] default '{{}}', vectors {s}.vector_set[] default '{{}}',
           expected {s}.table_expect[] default '{{}}', changes jsonb default null, author text default null,
           actor text default null, app_id text default null, app_version bigint default null,
           message text default null, definition text default null, namespaces jsonb default null,
           summaries jsonb default null)
         returns {s}.commit_result language sql as
           $$ select {s}.commit_since($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) $$;"
    );
    on.batch_execute(&old).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc1);

    // Every commit that does not say where it saw its key goes as before.
    assert!(p.commit(&on, &create(t)).await?.applied);
    // One that does is refused as unsupported, until the catalog is migrated.
    let keyed = Commit {
        app_id: Some("writer".into()),
        app_version: Some(2),
        expected_app_version: Some(1),
        ..append(1, t, "gs://lake/b.parquet", 0)
    };
    let refused = p.commit(&on, &keyed).await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Unsupported, "{refused}");
    Ok(())
}
