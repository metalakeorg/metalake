//! R8: the protocol's interface (ADR 0003 §2.1, §2.7, §2.8): versions and `migrate`.

use metalake_protocol_tests::differential::{clone_catalog, commit_protocol, Commit, Outcome, TableChange};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{compare, connect, fresh_schema, install_protocol, migrate_protocol, quote_ident, Error};
use serde_json::json;
use tokio_postgres::Client;
use uuid::Uuid;

async fn create_table(client: &Client, schema: &str, r: i32, t: u128) -> Result<(), Error> {
    let table_id = Uuid::from_u128(t);
    let c = Commit {
        ref_id: r,
        operation: "create".into(),
        tables: vec![TableChange {
            table_id,
            adds: vec![stored_file(table_id, format!("gs://lake/{t}.parquet"), 10, 1, 0, 9)],
            definition: Some(json!({
                "create": {"namespace": ["db"], "name": format!("t{t}"), "location": "gs://lake"},
                "fields": [{"field_id": 1, "new": true, "position": 1, "name": "k", "type": "long", "required": true}],
                "cluster_key": [1],
            })),
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    };
    match commit_protocol(client, schema, &c).await?.outcome {
        Outcome::Committed(_) => Ok(()),
        refused => Err(format!("creating t{t}: {refused:?}").into()),
    }
}

/// The definition of every function of the catalog, by signature.
async fn functions(client: &Client, schema: &str) -> Result<Vec<(String, String)>, Error> {
    Ok(client
        .query(
            "select p.oid::regprocedure::text, pg_get_functiondef(p.oid) from pg_proc p
              where p.pronamespace = $1::text::regnamespace order by 1",
            &[&quote_ident(schema)],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// Migrating twice changes no row and no function, and a catalog it does not know is refused without a change.
#[tokio::test]
async fn migrating_changes_nothing_and_refuses_what_it_does_not_know() -> Result<(), Error> {
    let (schema, twin) = (fresh_schema("p"), fresh_schema("p"));
    let on = connect().await?;
    install_protocol(&on, &schema).await?;
    install_protocol(&on, &twin).await?;
    let p = quote_ident(&schema);
    create_table(&on, &schema, 1, 0xd001).await?;
    on.query_one(&format!("select {p}.create_ref(name => 'b', parent => 1)"), &[])
        .await?;
    create_table(&on, &schema, 2, 0xd002).await?;
    clone_catalog(&on, &schema, &twin).await?;
    let before = functions(&on, &schema).await?;

    for i in 0..2 {
        let migrated = migrate_protocol(&on, &schema).await?;
        let rows = compare(&on, &schema, &twin).await?;
        let after = functions(&on, &schema).await?;
        if migrated.is_some() || !rows.is_empty() || after != before {
            return Err(format!(
                "migration {i}: {migrated:?}, rows {rows:?}, functions changed: {}",
                after != before
            )
            .into());
        }
    }
    let versions: (String, String, String) = on
        .query_one(&format!("select * from {p}.versions()"), &[])
        .await
        .map(|r| (r.get(0), r.get(1), r.get(2)))?;
    if versions != ("0".into(), "1".into(), "0".into()) {
        return Err(format!("versions {versions:?}").into());
    }

    for (key, value) in [("format_version", "99"), ("protocol_version", "99")] {
        on.batch_execute(&format!("update {p}.meta set value = '{value}' where key = '{key}'"))
            .await?;
        let refused = migrate_protocol(&on, &schema).await;
        on.batch_execute(&format!("update {p}.meta set value = '0' where key = '{key}'"))
            .await?;
        if !refused.as_ref().is_err_and(|e| e.to_string().contains("0A000")) {
            return Err(format!("{key} {value}: {refused:?}").into());
        }
        if !compare(&on, &schema, &twin).await?.is_empty() || functions(&on, &schema).await? != before {
            return Err(format!("a refused migration with {key} {value} changed the catalog").into());
        }
    }

    // A catalog of writer_version 0: migrating it gives its tables their namespaces (§18), and commits go on.
    on.batch_execute(&format!(
        "update {p}.meta set value = '0' where key = 'writer_version'; delete from {p}.namespace"
    ))
    .await?;
    let migrated = migrate_protocol(&on, &schema).await?;
    let spaces: i64 = on
        .query_one(
            &format!("select count(*) from {p}.namespace where namespace = '{{db}}'"),
            &[],
        )
        .await?
        .get(0);
    if migrated.is_none() || spaces != 2 {
        return Err(format!("the migration of writer_version 0 gave {migrated:?} and {spaces} rows of db").into());
    }
    create_table(&on, &schema, 1, 0xd003).await?;
    on.batch_execute(&format!(
        "drop schema {p} cascade; drop schema {} cascade",
        quote_ident(&twin)
    ))
    .await?;
    Ok(())
}

/// §2.8: on two refs, after a rename into another namespace, a drop, a new field and a namespace's property, each
/// view answers what the visibility functions do, for a client whose search_path is elsewhere.
#[tokio::test]
async fn the_views_answer_what_visibility_does() -> Result<(), Error> {
    let schema = fresh_schema("p");
    let on = connect().await?;
    install_protocol(&on, &schema).await?;
    let p = quote_ident(&schema);
    create_table(&on, &schema, 1, 0xe001).await?;
    create_table(&on, &schema, 1, 0xe002).await?;
    on.query_one(&format!("select {p}.create_ref(name => 'b', parent => 1)"), &[])
        .await?;
    let t1 = Uuid::from_u128(0xe001);
    // ID-3: a new field's id is drawn first.
    let field: i32 = on
        .query_one(&format!("select {p}.next_id($1, 'field')"), &[&t1])
        .await?
        .get(0);
    let rename = Commit {
        ref_id: 2,
        operation: "update".into(),
        tables: vec![TableChange {
            table_id: t1,
            definition: Some(json!({
                "rename": {"namespace": ["a"], "name": "x"},
                "fields": [{"field_id": field, "new": true, "position": 2, "name": "v", "type": "int"}],
            })),
            ..TableChange::default()
        }],
        app: None,
        namespaces: Some(json!([{"namespace": ["a"], "create": true, "properties": {"comment": "ours"}}])),
    };
    let drop = Commit {
        ref_id: 1,
        operation: "update".into(),
        tables: vec![TableChange {
            table_id: Uuid::from_u128(0xe002),
            definition: Some(json!({"drop": true})),
            ..TableChange::default()
        }],
        app: None,
        namespaces: None,
    };
    for c in [&rename, &drop] {
        if !matches!(commit_protocol(&on, &schema, c).await?.outcome, Outcome::Committed(_)) {
            return Err(format!("{:?}", commit_protocol(&on, &schema, c).await?).into());
        }
    }
    on.batch_execute("set search_path = public").await?;
    // Each pair: the view, and the same rows from the visibility functions, as text in one order.
    let latest = 9223372036854775806i64;
    let pairs = [
        (
            format!("select ref, namespace::text, name, table_id::text from {p}.tables order by 1, 2, 3"),
            format!(
                "select r.name, t.namespace::text, t.name, t.table_id::text from {p}.ref r,
                        {p}.latest_tables() t where t.ref_id = r.ref_id order by 1, 2, 3"
            ),
        ),
        (
            format!("select ref, table_id::text, field_id::text, name, type from {p}.columns order by 1, 2, 3"),
            format!(
                "select r.name, f.table_id::text, f.field_id::text, f.name, f.type from {p}.ref r, {p}.latest_columns() f
                  where f.ref_id = r.ref_id order by 1, 2, 3"
            ),
        ),
        (
            format!("select ref, namespace::text, properties::text from {p}.namespaces order by 1, 2"),
            "select * from (values ('b', '{a}', '{\"comment\": \"ours\"}'), ('b', '{db}', '{}'),
                                   ('main', '{db}', '{}')) v order by 1, 2"
                .to_string(),
        ),
        (
            format!("select name, coalesce(parent, '') from {p}.refs order by 1"),
            "select * from (values ('b', 'main'), ('main', '')) v order by 1".to_string(),
        ),
        (
            format!("select count(*)::text from {p}.history"),
            format!("select count(*)::text from {p}.commit"),
        ),
        (
            format!("select count(*)::text from {p}.files('b', '{t1}')"),
            format!("select count(*)::text from {p}.plan(2, '{t1}', {latest})"),
        ),
    ];
    for (view, want) in pairs {
        let read = |sql: String| {
            let on = &on;
            async move {
                let rows = on.query(&sql, &[]).await?;
                let text: Vec<Vec<String>> = rows
                    .iter()
                    .map(|r| (0..r.len()).map(|i| r.get::<_, String>(i)).collect())
                    .collect();
                Ok::<_, Error>(text)
            }
        };
        let (got, expected) = (read(view.clone()).await?, read(want).await?);
        if got != expected || got.is_empty() {
            return Err(format!("{view}: {got:?}, expected {expected:?}").into());
        }
    }
    // The renamed table is a.x on b with its new field, and db.t on main; the dropped one only on b.
    let tables: Vec<String> = on
        .query(
            &format!("select ref || ':' || array_to_string(namespace, '.') || '.' || name from {p}.tables order by 1"),
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    on.batch_execute(&format!("reset search_path; drop schema {p} cascade"))
        .await?;
    if tables != ["b:a.x", "b:db.t57346", "main:db.t57345"] {
        return Err(format!("tables {tables:?}").into());
    }
    Ok(())
}
