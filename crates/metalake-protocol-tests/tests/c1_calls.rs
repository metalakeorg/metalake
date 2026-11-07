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
    let applied = p.commit(&on, &create(t)).await?;
    assert_eq!(p.commit_of_key(&on, 1, "c1").await?, Some(applied.commit_seq));
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
