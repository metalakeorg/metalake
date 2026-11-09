//! Following the catalog (CHG-1): its feed of every commit from now, the lead among the processes that follow it, and a
//! table opened by the id the feed knows it by.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use metalake::{Catalog, CatalogConfig, Error, NewTable, Ref};
use serde_json::json;
use uuid::Uuid;

fn batch() -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, false)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

async fn lake() -> Result<(Catalog, Ref), Error> {
    lake_in(&fresh_schema()).await
}

async fn lake_in(schema: &str) -> Result<(Catalog, Ref), Error> {
    let config = CatalogConfig::new(url(), schema).warehouse("memory:///wh");
    let catalog = Catalog::init(config, None).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    Ok((catalog, main))
}

async fn create(main: &Ref, name: &str) -> Result<Uuid, Error> {
    let (tx, id) = main.transaction().create_table(NewTable {
        namespace: &["db"],
        name,
        schema: &batch().schema(),
        location: None,
        cluster_key: vec![],
        partitioning: vec![],
        properties: vec![],
    })?;
    tx.commit().await?;
    Ok(id)
}

#[tokio::test]
async fn chg1_feed_from_now_in_order_once() -> Result<(), Error> {
    let (catalog, main) = lake().await?;
    create(&main, "before").await?;
    let start = catalog.feed_start().await?;
    create(&main, "a").await?;
    let dev = main.create_branch("dev").await?;
    create(&dev, "b").await?;
    create(&main, "c").await?;
    // a commit is delivered a second after it (CHG-1)
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let all = catalog.feed(&start, 100).await?;
    assert_eq!(
        all.len(),
        4,
        "the four commits after the start, a branch made among them, and not the one before: {all:?}"
    );
    assert!(all.windows(2).all(|w| w[0].commit_seq < w[1].commit_seq), "in order");
    assert_eq!(
        all.iter().map(|c| (c.ref_id, c.operation.as_str())).collect::<Vec<_>>(),
        [
            (main.id(), "update"),
            (dev.id(), "branch"),
            (dev.id(), "update"),
            (main.id(), "update")
        ]
    );

    // a cursor past each commit takes the rest, one at a time, and then nothing
    let mut cursor = start;
    for expected in &all {
        let next = catalog.feed(&cursor, 1).await?;
        assert_eq!(next.as_slice(), std::slice::from_ref(expected));
        cursor = cursor.after(&next[0]);
    }
    assert!(catalog.feed(&cursor, 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn one_process_leads() -> Result<(), Error> {
    let schema = fresh_schema();
    let (catalog, _) = lake_in(&schema).await?;
    let other = Catalog::connect(CatalogConfig::new(url(), &schema)).await?;
    let lead = catalog.lead("materialize").await?.expect("the first takes it");
    assert!(lead.held().await);
    assert!(other.lead("materialize").await?.is_none(), "held by the first");
    assert!(
        other.lead("another").await?.is_some(),
        "a lead of another name is another"
    );

    lead.release().await?;
    let lead = other.lead("materialize").await?.expect("released, the other takes it");

    // dropped, as when its process goes, it is free again
    drop(lead);
    let mut taken = None;
    for _ in 0..50 {
        taken = catalog.lead("materialize").await?;
        if taken.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(taken.is_some(), "free once its connection closed");
    Ok(())
}

#[tokio::test]
async fn a_lead_is_a_catalogs() -> Result<(), Error> {
    let (one, _) = lake().await?;
    let (two, _) = lake().await?;
    let _held = one.lead("materialize").await?.expect("taken");
    assert!(
        two.lead("materialize").await?.is_some(),
        "another catalog in the same database has its own"
    );
    Ok(())
}

#[tokio::test]
async fn table_with_id_as_a_read_by_name_opens_it() -> Result<(), Error> {
    let (_catalog, main) = lake().await?;
    let id = create(&main, "t").await?;
    let opened = main.table_with_id(id).await?;
    assert_eq!((opened.id(), opened.name()), (id, "t"));

    // renamed, it is found by its new name; on a branch that dropped it, it is not there
    let rename = json!({"rename": {"namespace": ["db"], "name": "u"}});
    main.transaction().define(id, rename).commit().await?;
    assert_eq!(main.table_with_id(id).await?.name(), "u");
    let dev = main.create_branch("dev").await?;
    dev.transaction().define(id, json!({"drop": true})).commit().await?;
    assert!(matches!(dev.table_with_id(id).await, Err(Error::NotFound(_))));
    assert!(main.table_with_id(id).await.is_ok(), "main still has it");
    assert!(matches!(
        main.table_with_id(Uuid::from_u128(7)).await,
        Err(Error::NotFound(_))
    ));
    Ok(())
}
