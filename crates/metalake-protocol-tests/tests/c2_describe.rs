//! C2a (ADR 0003 §7): a table as a read sees it. `describe` answers a table's definition at a ref and a sequence,
//! and `tables_at` the tables there: a branch sees its own changes and its parent does not, a read in the past sees
//! the table as it was, and a renamed or dropped table is found, or not, where it should be.

use metalake_pg::{Commit, CreateRef, Protocol};
use metalake_protocol_tests::records::stored_file;
use metalake_protocol_tests::{Error, connect, fresh_schema, install_protocol, quote_ident};
use serde_json::{Value, json};
use uuid::Uuid;

fn change(ref_id: i32, t: Uuid, definition: Value) -> Commit {
    Commit {
        ref_id,
        operation: "append".into(),
        changes: Some(json!({ t.to_string(): definition })),
        ..Default::default()
    }
}

fn field(id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, required: bool) -> Value {
    json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name, "type": ty,
           "required": required})
}

/// The definition's fields, as (field id, parent, name, type), in the order `describe` gives them.
fn fields(d: &Value) -> Vec<(i64, Option<i64>, String, String)> {
    d["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["field_id"].as_i64().unwrap(),
                f["parent_field_id"].as_i64(),
                f["name"].as_str().unwrap().to_string(),
                f["type"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn describe_by_ref() -> Result<(), Error> {
    let on = connect().await?;
    let schema = fresh_schema("c2");
    install_protocol(&on, &schema).await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc2a);

    let mut create = change(
        1,
        t,
        json!({
            "create": {"namespace": ["db"], "name": "t", "location": "gs://lake/t"},
            "fields": [
                field(1, None, 1, "k", "long", true),
                field(2, None, 2, "s", "struct", false),
                field(3, Some(2), 1, "x", "int", false),
                field(4, None, 3, "tags", "list", false),
                field(5, Some(4), 1, "element", "string", false),
                field(6, None, 4, "m", "map", false),
                field(7, Some(6), 1, "key", "string", true),
                field(8, Some(6), 2, "value", "double", false),
            ],
            "cluster_key": [1],
            "properties": {"owner": "a"},
        }),
    );
    create.files = vec![stored_file(t, "gs://lake/t/a.parquet".into(), 10, 1, 0, 9)];
    // Another table first, so that a definition is always the one asked for.
    let w = Uuid::from_u128(0xc2b);
    p.commit(
        &on,
        &change(
            1,
            w,
            json!({"create": {"namespace": ["db"], "name": "w", "location": "gs://lake/w"},
                   "fields": [field(1, None, 1, "k", "long", true)]}),
        ),
    )
    .await?;
    let created = p.commit(&on, &create).await?.commit_seq;

    let main = p.describe(&on, 1, t, None).await?.expect("main sees the table");
    assert_eq!(
        (main["namespace"].clone(), main["name"].clone()),
        (json!(["db"]), json!("t"))
    );
    assert_eq!(
        (main["location"].clone(), main["snapshot_id"].clone()),
        (json!("gs://lake/t"), json!(created))
    );
    assert_eq!(
        fields(&main)
            .iter()
            .map(|(id, parent, _, _)| (*id, *parent))
            .collect::<Vec<_>>(),
        [
            (1, None),
            (2, None),
            (4, None),
            (6, None),
            (3, Some(2)),
            (5, Some(4)),
            (7, Some(6)),
            (8, Some(6))
        ]
    );
    assert_eq!(
        (main["cluster_key"].clone(), main["properties"].clone()),
        (json!([1]), json!({"owner": "a"}))
    );
    let spec = main["spec_id"].as_i64().expect("a default spec");
    assert!(main["specs"].get(spec.to_string()).is_some(), "{main}");
    assert_eq!(
        p.tables_at(&on, 1, None)
            .await?
            .iter()
            .map(|n| n.name.as_str())
            .collect::<Vec<_>>(),
        ["t", "w"]
    );

    // A branch adds a field, changes a property and renames the table: its parent sees none of it.
    let dev = p.create_ref(&on, &CreateRef::branch("dev", 1)).await?;
    let y = p.next_id(&on, t, "field").await?;
    let renamed = p
        .commit(
            &on,
            &change(
                dev,
                t,
                json!({"fields": [field(y, None, 5, "y", "string", false)], "properties": {"owner": "b"},
                       "rename": {"namespace": ["db"], "name": "u"}}),
            ),
        )
        .await?
        .commit_seq;
    let on_dev = p.describe(&on, dev, t, None).await?.expect("dev sees the table");
    assert_eq!(on_dev["snapshot_id"], json!(renamed));
    assert_eq!(fields(&on_dev).len(), 9);
    assert_eq!(
        (on_dev["name"].clone(), on_dev["properties"].clone()),
        (json!("u"), json!({"owner": "b"}))
    );
    // The counters are the table's, not a ref's: what `dev` drew, `main` is told too, and nothing else of it changes.
    let mut again = p.describe(&on, 1, t, None).await?.expect("main sees the table");
    assert_eq!(again["last_field_id"], json!(y));
    again["last_field_id"] = main["last_field_id"].clone();
    assert_eq!(again, main);
    assert_eq!(
        p.tables_at(&on, dev, None)
            .await?
            .iter()
            .map(|n| n.name.as_str())
            .collect::<Vec<_>>(),
        ["u", "w"]
    );
    let before = p
        .describe(&on, dev, t, Some(renamed - 1))
        .await?
        .expect("dev before its commit");
    assert_eq!((before["name"].clone(), fields(&before).len()), (json!("t"), 8));
    assert_eq!(before["properties"], json!({"owner": "a"}));

    // Dropped on main, the table is gone there, and still there in the past and on the branch.
    let dropped = p.commit(&on, &change(1, t, json!({"drop": true}))).await?.commit_seq;
    assert_eq!(p.describe(&on, 1, t, None).await?, None);
    let left: Vec<String> = p.tables_at(&on, 1, None).await?.into_iter().map(|n| n.name).collect();
    assert_eq!(left, ["w"]);
    let past = p
        .describe(&on, 1, t, Some(dropped - 1))
        .await?
        .expect("main before the drop");
    assert_eq!(fields(&past), fields(&main));
    let then: Vec<String> = p
        .tables_at(&on, 1, Some(dropped - 1))
        .await?
        .into_iter()
        .map(|n| n.name)
        .collect();
    assert_eq!(then, ["t", "w"]);
    assert!(p.describe(&on, dev, t, None).await?.is_some());

    let refs = p.refs(&on).await?;
    assert_eq!(
        refs.iter()
            .map(|r| (r.name.as_str(), r.kind.as_str(), r.parent.as_deref()))
            .collect::<Vec<_>>(),
        [("main", "branch", None), ("dev", "branch", Some("main"))]
    );

    on.batch_execute(&format!("drop schema {} cascade", quote_ident(&schema)))
        .await?;
    Ok(())
}
