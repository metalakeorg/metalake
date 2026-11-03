//! R7: intents (RET-7). An expired intent no file refers to is an orphan; forget_intents forgets only orphans.

use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};

#[tokio::test]
async fn only_orphans_are_forgotten() -> Result<(), Error> {
    let schema = fresh_schema("p");
    let on = connect().await?;
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    on.batch_execute(&format!(
        "insert into {p}.intent values
           ('gs://lake/old.parquet', gen_random_uuid(), 1, now() - interval '1 hour'),
           ('gs://lake/new.parquet', gen_random_uuid(), 1, now() + interval '1 hour')"
    ))
    .await?;
    let orphans: Vec<String> = on
        .query_one(&format!("select array(select {p}.orphans())"), &[])
        .await?
        .get(0);
    let forgot: i64 = on
        .query_one(
            &format!("select {p}.forget_intents(array['gs://lake/old.parquet', 'gs://lake/new.parquet'])"),
            &[],
        )
        .await?
        .get(0);
    let left: Vec<String> = on
        .query_one(&format!("select array(select path from {p}.intent)"), &[])
        .await?
        .get(0);
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    if orphans != ["gs://lake/old.parquet"] || forgot != 1 || left != ["gs://lake/new.parquet"] {
        return Err(format!("orphans {orphans:?}, forgot {forgot}, left {left:?}").into());
    }
    Ok(())
}

/// RET-5, RET-8: a commit waits for an expiring that holds a path it adds, and a path in the deletion queue is refused.
#[tokio::test]
async fn queued_paths_are_refused_and_held_paths_wait() -> Result<(), Error> {
    use metalake_protocol_tests::differential::{Commit, Outcome, TableChange, commit_protocol};
    use metalake_protocol_tests::records::stored_file;
    use serde_json::json;
    use uuid::Uuid;

    let schema = fresh_schema("p");
    let (on, other) = (connect().await?, connect().await?);
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    let t = Uuid::from_u128(0xa7);
    let add = |path: &str, create: bool| {
        Commit {
        ref_id: 1,
        operation: "append".into(),
        tables: vec![TableChange {
            table_id: t,
            adds: vec![stored_file(t, path.into(), 10, 1, 0, 9)],
            definition: create.then(|| {
                json!({
                    "create": {"namespace": ["db"], "name": "t", "location": "gs://lake"},
                    "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
                    "cluster_key": [1],
                })
            }),
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    }
    };
    let outcome = |o: Outcome| match o {
        Outcome::Committed(_) => "ok".to_string(),
        Outcome::Refused(s) => s,
    };
    let mut seen = vec![outcome(
        commit_protocol(&on, &schema, &add("gs://lake/a.parquet", true))
            .await?
            .outcome,
    )];
    // An expiring holds gs://lake/b.parquet: a commit of it waits.
    other
        .batch_execute(&format!("begin; select {p}.path_lock('gs://lake/b.parquet', true)"))
        .await?;
    on.batch_execute("set lock_timeout = '300ms'").await?;
    seen.push(outcome(
        commit_protocol(&on, &schema, &add("gs://lake/b.parquet", false))
            .await?
            .outcome,
    ));
    on.batch_execute("reset lock_timeout").await?;
    other.batch_execute("rollback").await?;
    // A queued path is refused; another is not.
    on.batch_execute(&format!(
        "insert into {p}.deletion_queue values ('gs://lake/c.parquet', 'data', now())"
    ))
    .await?;
    seen.push(outcome(
        commit_protocol(&on, &schema, &add("gs://lake/c.parquet", false))
            .await?
            .outcome,
    ));
    seen.push(outcome(
        commit_protocol(&on, &schema, &add("gs://lake/d.parquet", false))
            .await?
            .outcome,
    ));
    on.batch_execute(&format!("drop schema {p} cascade")).await?;
    if seen != ["ok", "55P03", "22023", "ok"] {
        return Err(format!("{seen:?}").into());
    }
    Ok(())
}
