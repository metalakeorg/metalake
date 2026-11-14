//! C5c: a table's definition changed as `alter table` asks, on a branch: a column added, renamed, widened, made
//! optional and dropped, and the table renamed, each under the ids the format keeps (TYP-2, TYP-3, TBL-1). The rows
//! written before read under the new definition. What the format does not allow is refused, and changes nothing.

mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use futures_util::TryStreamExt;
use metalake::{Catalog, CatalogConfig, Error, NewTable, Scan, Storage, TableChange, TableWriter, WriteOptions};
use serde_json::json;

#[tokio::test]
async fn alter_a_table_with_rows() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::init(
        CatalogConfig::new(url(), fresh_schema()).warehouse("memory:///wh"),
        None,
    )
    .await?;
    let storage = Storage::in_memory();
    let main = catalog.branch("main").await?;
    let shape = Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("s", DataType::Utf8, true),
        Field::new("gone", DataType::Utf8, true),
    ]);
    let (tx, _) = main.transaction().create_namespace(&["db"]).create_table(NewTable {
        namespace: &["db"],
        name: "t",
        schema: &shape,
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    let t = main.table(&["db"], "t").await?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec![Some("a"), None])),
        Arc::new(StringArray::from(vec!["x", "y"])),
    ];
    let mut writer = TableWriter::new(&catalog, &t, storage.clone(), WriteOptions::default())?;
    writer
        .write(&RecordBatch::try_new(t.schema().clone(), columns)?)
        .await?;
    main.transaction().append(writer.finish().await?).commit().await?;

    // On a branch: main keeps its table as it was.
    let dev = main.create_branch("dev").await?;
    let t = dev.table(&["db"], "t").await?;
    let changes = [
        TableChange::Widen {
            column: "k".into(),
            ty: DataType::Int64,
        },
        TableChange::RenameColumn {
            from: "s".into(),
            to: "label".into(),
        },
        TableChange::DropNotNull("k".into()),
        TableChange::DropColumn("gone".into()),
        TableChange::AddColumn {
            name: "n".into(),
            ty: DataType::Int64,
            required: true,
            default: Some(json!(7)),
            doc: Some("added".into()),
        },
        TableChange::Rename {
            namespace: vec!["db".into()],
            name: "u".into(),
        },
    ];
    dev.transaction().alter(&t, &changes).await?.commit().await?;

    let u = dev.table(&["db"], "u").await?;
    let fields: Vec<(&str, &str, bool)> = u
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), f.ty.as_str(), f.required))
        .collect();
    assert_eq!(
        fields,
        [("k", "long", false), ("label", "string", false), ("n", "long", true)]
    );
    assert_eq!(u.fields()[0].field_id, t.fields()[0].field_id, "a column keeps its id");
    assert!(matches!(dev.table(&["db"], "t").await, Err(Error::NotFound(_))));
    let main_t = main.table(&["db"], "t").await?;
    assert_eq!(main_t.fields().len(), 3, "main is as it was");

    // The rows written before, under the new definition: widened, renamed, the dropped column gone, the added one its
    // default.
    let scan = Scan::new(&catalog, &u);
    let batches: Vec<RecordBatch> = scan.read(scan.plan().await?, &storage).try_collect().await?;
    let batch = &batches[0];
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["k", "label", "n"]
    );
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().values().to_vec(), [1, 2]);
    assert_eq!(batch.column(2).as_primitive::<Int64Type>().values().to_vec(), [7, 7]);

    // Refused, and nothing changes.
    let refusals = [
        (
            TableChange::Widen {
                column: "k".into(),
                ty: DataType::Int32,
            },
            "TYP-3",
        ),
        (
            TableChange::AddColumn {
                name: "r".into(),
                ty: DataType::Utf8,
                required: true,
                default: None,
                doc: None,
            },
            "TYP-5",
        ),
        (
            TableChange::RenameColumn {
                from: "k".into(),
                to: "label".into(),
            },
            "exists",
        ),
        (TableChange::DropColumn("nope".into()), "column nope"),
    ];
    for (change, says) in refusals {
        let refused = dev
            .transaction()
            .alter(&u, std::slice::from_ref(&change))
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            refused.as_deref().is_some_and(|r| r.contains(says)),
            "{change:?}: {refused:?}"
        );
    }
    assert_eq!(dev.table(&["db"], "u").await?.fields(), u.fields());
    Ok(())
}
