//! C2b (ADR 0003 §7): the core's skeleton against a real catalog, run by `CRATE=metalake bash sql/test.sh`. A
//! catalog created by `init`; a table with a struct, a list and a map, written through the SDK, read by the core as
//! an Arrow schema with its field ids, on main, on a branch that changed it, and in the past; and storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use metalake::schema::FIELD_ID;
use metalake::{Catalog, CatalogConfig, Error, Storage};
use metalake_pg::{Commit, CreateRef, Pool, Protocol};
use serde_json::{Value, json};
use uuid::Uuid;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: run CRATE=metalake bash sql/test.sh")
}

fn fresh_schema() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!("core_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed))
}

fn spec_field(id: i32, parent: Option<i32>, position: i32, name: &str, ty: &str, required: bool) -> Value {
    json!({"field_id": id, "new": true, "parent_field_id": parent, "position": position, "name": name, "type": ty,
           "required": required})
}

fn arrow(name: &str, ty: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, ty, nullable).with_metadata(HashMap::from([(FIELD_ID.to_string(), id.to_string())]))
}

/// The table's schema as the core must give it.
fn expected(with_y: bool) -> Schema {
    let entries = Fields::from(vec![
        arrow("key", DataType::Utf8, false, 7),
        arrow("value", DataType::Float64, true, 8),
    ]);
    let mut fields = vec![
        arrow("k", DataType::Int64, false, 1),
        arrow(
            "s",
            DataType::Struct(Fields::from(vec![arrow("x", DataType::Int32, true, 3)])),
            true,
            2,
        ),
        arrow(
            "tags",
            DataType::List(Arc::new(arrow("element", DataType::Utf8, true, 5))),
            true,
            4,
        ),
        arrow(
            "m",
            DataType::Map(
                Arc::new(Field::new("key_value", DataType::Struct(entries), false)),
                false,
            ),
            true,
            6,
        ),
    ];
    if with_y {
        fields.push(arrow("y", DataType::Utf8, true, 9));
    }
    Schema::new(fields)
}

#[tokio::test]
async fn the_core_reads_a_table_as_arrow_where_a_read_sees_it() -> Result<(), Box<dyn std::error::Error>> {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema), None).await?;
    assert!(catalog.versions().writable());

    // Written through the SDK, as a writer of C4 will.
    let pool = Pool::new(&url(), 1)?;
    let s = pool.get().await?;
    let p = Protocol::new(&schema);
    let t = Uuid::from_u128(0xc2b);
    let other = Uuid::from_u128(0xc2c);
    let create = Commit {
        ref_id: 1,
        operation: "append".into(),
        namespaces: Some(json!([{"namespace": ["db"], "create": true}, {"namespace": ["ops"], "create": true}])),
        changes: Some(json!({ t.to_string(): {
            "create": {"namespace": ["db"], "name": "t", "location": "memory:///lake/t"},
            "fields": [
                spec_field(1, None, 1, "k", "long", true),
                spec_field(2, None, 2, "s", "struct", false),
                spec_field(3, Some(2), 1, "x", "int", false),
                spec_field(4, None, 3, "tags", "list", false),
                spec_field(5, Some(4), 1, "element", "string", false),
                spec_field(6, None, 4, "m", "map", false),
                spec_field(7, Some(6), 1, "key", "string", true),
                spec_field(8, Some(6), 2, "value", "double", false),
            ],
            "cluster_key": [1],
            "properties": {"owner": "a"},
        },
        other.to_string(): {
            "create": {"namespace": ["ops"], "name": "t", "location": "memory:///lake/ops"},
            "fields": [spec_field(1, None, 1, "k", "long", true)],
        }})),
        ..Default::default()
    };
    let created = p.commit(&s, &create).await?.commit_seq;
    let dev = p.create_ref(&s, &CreateRef::branch("dev", 1)).await?;
    let y = p.next_id(&s, t, "field").await?;
    assert_eq!(y, 9);
    let mut add = Commit {
        ref_id: dev,
        operation: "append".into(),
        ..Default::default()
    };
    add.changes = Some(json!({ t.to_string(): {"fields": [spec_field(y, None, 5, "y", "string", false)]} }));
    let added = p.commit(&s, &add).await?.commit_seq;

    let main = catalog.branch("main").await?;
    let on_main = main.table(&["db"], "t").await?;
    assert_eq!(on_main.arrow_schema(), expected(false));
    assert_eq!((on_main.id(), on_main.ref_id(), on_main.name()), (t, 1, "t"));
    assert_eq!(
        (on_main.location(), on_main.cluster_key()),
        ("memory:///lake/t", &[1][..])
    );
    assert_eq!(on_main.properties().get("owner").map(String::as_str), Some("a"));
    assert_eq!(
        on_main.seq(),
        created,
        "its latest snapshot on main; the commit on dev is not one"
    );

    let branch = catalog.branch("dev").await?;
    assert_eq!(branch.table(&["db"], "t").await?.arrow_schema(), expected(true));
    assert_eq!(
        branch.at(added - 1).table(&["db"], "t").await?.arrow_schema(),
        expected(false)
    );
    assert_eq!(main.at(created).table(&["db"], "t").await?.seq(), created);
    let names: Vec<String> = branch.tables().await?.into_iter().map(|n| n.name).collect();
    assert_eq!(names, ["t", "t"]);
    assert_eq!(
        catalog.refs().await?.iter().map(|r| r.name()).collect::<Vec<_>>(),
        ["main", "dev"]
    );

    assert_eq!(main.table(&["ops"], "t").await?.id(), other);
    assert!(matches!(main.table(&["db"], "nope").await, Err(Error::NotFound(_))));
    assert!(matches!(catalog.branch("nope").await, Err(Error::NotFound(_))));
    assert!(matches!(catalog.tag("dev").await, Err(Error::NotFound(_))));
    Ok(())
}

#[tokio::test]
async fn storage_holds_what_is_put_under_its_location() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(fresh_schema());
    std::fs::create_dir_all(&dir)?;
    let location = url::Url::from_directory_path(&dir).unwrap().to_string();
    let stores = [
        (
            Storage::new(&location, Vec::<(String, String)>::new())?,
            location.clone(),
        ),
        (Storage::in_memory(), "memory:///lake/".to_string()),
    ];
    for (storage, base) in stores {
        let file = format!("{base}t/a.parquet");
        storage.put(&file, Bytes::from_static(b"PAR1")).await?;
        assert_eq!(storage.get(&file).await?, Bytes::from_static(b"PAR1"));
        storage.delete(&file).await?;
        storage.delete(&file).await?;
        assert!(matches!(storage.get(&file).await, Err(Error::Storage(_))));
        assert!(matches!(storage.path("s3://elsewhere/a"), Err(Error::Location(_))));
    }
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
