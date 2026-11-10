//! A pipeline's tables found by name in a real catalog: each becomes the table a `CREATE TABLE … WITH (connector =
//! 'metalake', …)` would declare, with its columns and its role, and with only the names of the environment's variables,
//! never their values. It needs the Postgres at `DATABASE_URL`, as the source's tests do.

mod common;

use arrow_schema::{DataType, Field, Schema};
use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::ConnectionType;
use common::{Lake, url};
use metalake::NewTable;
use metalake_arroyo::resolve::{Environment, resolve};

const QUERY: &str = "INSERT INTO shop.totals SELECT k, count(*) FROM shop.orders GROUP BY k";

async fn with_totals(lake: &Lake) {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("orders", DataType::Int64, true),
    ]);
    let (tx, _) = lake
        .main
        .transaction()
        .create_table(NewTable {
            namespace: &["shop"],
            name: "totals",
            schema: &schema,
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![],
        })
        .unwrap();
    tx.commit().await.unwrap();
}

fn environment(lake: &Lake, storage: &[&str]) -> Environment {
    Environment {
        postgres: url(),
        schema: lake.schema.clone(),
        storage: storage.iter().map(|s| s.to_string()).collect(),
    }
}

fn named<'a>(tables: &'a [Connection], name: &str) -> &'a Connection {
    tables
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("{name} among {:?}", tables.iter().map(|c| &c.name).collect::<Vec<_>>()))
}

#[tokio::test]
async fn resolve_sources_and_sink() {
    let lake = Lake::new().await;
    with_totals(&lake).await;
    let env = environment(&lake, &["AWS_SECRET_ACCESS_KEY"]);
    let tables = resolve(QUERY, |_| false, &env).await.unwrap();
    assert_eq!(tables.len(), 2);

    let orders = named(&tables, "shop.orders");
    assert!(matches!(orders.connection_type, ConnectionType::Source));
    let columns: Vec<&str> = orders.schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert!(columns.contains(&"k"), "{columns:?}");
    let totals = named(&tables, "shop.totals");
    assert!(matches!(totals.connection_type, ConnectionType::Sink));

    // What the pipeline keeps is the variables' names: the values are each worker's to read.
    let config = &totals.config;
    assert!(config.contains("{{ METALAKE_POSTGRES }}"), "{config}");
    assert!(
        config.contains("\"aws_secret_access_key\":\"{{ AWS_SECRET_ACCESS_KEY }}\""),
        "{config}"
    );
    assert!(
        !config.contains(url().as_str()),
        "the connection string stays out of the pipeline: {config}"
    );
    assert!(config.contains(lake.schema.as_str()), "{config}");
}

#[tokio::test]
async fn resolve_leaves_unknown_and_known() {
    let lake = Lake::new().await;
    let env = environment(&lake, &[]);

    // A table no ref has, and a ref the catalog does not have, are the planner's to report.
    let query = "INSERT INTO shop.missing SELECT * FROM shop.orders UNION ALL SELECT * FROM nobranch.shop.orders";
    let tables = resolve(query, |_| false, &env).await.unwrap();
    assert_eq!(
        tables.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["shop.orders"]
    );

    // A name the planner knows already, as a table the query declares, is its own.
    let tables = resolve(QUERY, |name| name == "shop.orders", &env).await.unwrap();
    assert!(tables.iter().all(|c| c.name != "shop.orders"));
}

#[tokio::test]
async fn resolve_nothing_reaches_nothing() {
    let unreachable = Environment {
        postgres: "host=unreachable.invalid port=1 connect_timeout=1".into(),
        schema: "nowhere".into(),
        storage: vec![],
    };
    let tables = resolve(QUERY, |_| true, &unreachable).await.unwrap();
    assert!(tables.is_empty(), "with every name known, the catalog is not reached");
}

#[tokio::test]
async fn resolve_read_and_written_refused() {
    let lake = Lake::new().await;
    let refused = resolve(
        "INSERT INTO shop.orders SELECT * FROM shop.orders",
        |_| false,
        &environment(&lake, &[]),
    )
    .await
    .unwrap_err();
    assert!(refused.to_string().contains("both read and written"), "{refused}");
}
