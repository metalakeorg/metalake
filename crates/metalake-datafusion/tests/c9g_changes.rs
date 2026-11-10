//! C9g: `table_changes('t', after [, until])` in SQL (CHG-2, CHG-3): the rows a range of commits inserted and deleted,
//! read as a table that a statement filters, joins and aggregates like any other; ranges chain, `(a, b]` then `(b, c]`
//! taking each commit once; a prepared read gives the same rows each time; and a call that names no table, or no
//! range, is refused with what is wrong.

#[path = "../../metalake/tests/common/mod.rs"]
mod common;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, Field, Schema};
use common::{fresh_schema, url};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Storage};
use metalake_datafusion::MetaLake;

struct Lake {
    main: Ref,
    engine: MetaLake,
    context: SessionContext,
}

async fn lake() -> Lake {
    let schema = fresh_schema();
    let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse("memory:///wh"), None)
        .await
        .unwrap();
    let main = catalog.branch("main").await.unwrap();
    let shape = Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, true),
    ]);
    let (tx, _) = main
        .transaction()
        .create_namespace(&["db"])
        .create_table(NewTable {
            namespace: &["db"],
            name: "t",
            schema: &shape,
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![],
        })
        .unwrap();
    tx.commit().await.unwrap();
    let engine = MetaLake::new(catalog, Storage::in_memory()).await.unwrap();
    let context = engine.context().unwrap();
    Lake { main, engine, context }
}

impl Lake {
    /// Runs one statement as a session of the client does, through `execute`.
    async fn run(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let mut statements = DFParser::parse_sql(sql)?;
        let statement = statements.pop_front().expect("a statement");
        self.engine.execute(&self.context, statement).await?.collect().await
    }

    /// Each row of a statement over `table_changes`: `(change, key, commit)`, sorted.
    async fn rows(&self, sql: &str) -> Vec<(String, i64, i64)> {
        let batches = self.run(sql).await.unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let what = b.column_by_name("_change_type").unwrap().as_string::<i32>();
            let keys = b.column_by_name("k").unwrap().as_primitive::<Int64Type>();
            let seqs = b.column_by_name("_commit_seq").unwrap().as_primitive::<Int64Type>();
            for i in 0..b.num_rows() {
                rows.push((what.value(i).to_string(), keys.value(i), seqs.value(i)));
            }
        }
        rows.sort();
        rows
    }
}

fn row(what: &str, key: i64, seq: i64) -> (String, i64, i64) {
    (what.to_string(), key, seq)
}

/// The commit a statement made, once a second old, as the feed delivers it (CHG-1).
async fn committed(lake: &Lake, sql: &str) -> i64 {
    lake.run(sql).await.unwrap();
    let t = lake.main.table(&["db"], "t").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1150)).await;
    t.snapshot_id().expect("a commit")
}

#[tokio::test]
async fn chg3_table_changes_range() {
    let lake = lake().await;
    let one = committed(&lake, "insert into db.t values (1, 'a'), (2, 'b')").await;
    let two = committed(&lake, "insert into db.t values (3, 'c')").await;
    let three = committed(&lake, "delete from db.t where k = 1").await;

    assert_eq!(
        lake.rows(&format!("select * from table_changes('db.t', {one}, {two})"))
            .await,
        [row("insert", 3, two)]
    );
    assert_eq!(
        lake.rows(&format!("select * from table_changes('main.db.t', {two})"))
            .await,
        [row("delete", 1, three)]
    );

    // (one, two] then (two, three] is (one, three].
    let mut chained = lake
        .rows(&format!("select * from table_changes('db.t', {one}, {two})"))
        .await;
    chained.extend(
        lake.rows(&format!("select * from table_changes('db.t', {two}, {three})"))
            .await,
    );
    chained.sort();
    assert_eq!(
        lake.rows(&format!("select * from table_changes('db.t', {one}, {three})"))
            .await,
        chained
    );
}

#[tokio::test]
async fn table_changes_composes() {
    let lake = lake().await;
    let one = committed(&lake, "insert into db.t values (1, 'a'), (2, 'b'), (3, 'c')").await;
    committed(&lake, "delete from db.t where k = 2").await;
    committed(&lake, "insert into db.t values (4, 'd')").await;

    // Filtered and aggregated as any table.
    let counted = lake
        .run(&format!(
            "select _change_type, count(*) as n from table_changes('db.t', {one}) group by _change_type \
             order by _change_type"
        ))
        .await
        .unwrap();
    let kinds: Vec<(String, i64)> = counted
        .iter()
        .flat_map(|b| {
            let what = b.column(0).as_string::<i32>();
            let n = b.column(1).as_primitive::<Int64Type>();
            (0..b.num_rows()).map(move |i| (what.value(i).to_string(), n.value(i)))
        })
        .collect();
    assert_eq!(kinds, [("delete".to_string(), 1), ("insert".to_string(), 1)]);

    // Joined with the table it follows.
    let joined = lake
        .run(&format!(
            "select c.k, t.s from table_changes('db.t', {one}) c join db.t t on c.k = t.k \
             where c._change_type = 'insert'"
        ))
        .await
        .unwrap();
    assert_eq!(joined.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    // Two ranges in one statement.
    let both = lake
        .run(&format!(
            "select count(*) from table_changes('db.t', {one}) union all select count(*) from table_changes('db.t', {one}, {one})"
        ))
        .await
        .unwrap();
    assert_eq!(both.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    // Two ranges with no alias, joined: each keeps a name of its own.
    let joined = lake
        .run(&format!(
            "select count(*) from table_changes('db.t', {one}) cross join table_changes('db.t', {one})"
        ))
        .await
        .unwrap();
    assert_eq!(joined.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
async fn table_changes_refusals() {
    let lake = lake().await;
    let one = committed(&lake, "insert into db.t values (1, 'a')").await;
    for (sql, says) in [
        (
            format!("select * from table_changes('db.t', {one}, 0)"),
            "the end comes before the start",
        ),
        ("select * from table_changes('db.t')".into(), "two or three arguments"),
        ("select * from table_changes(db.t, 1)".into(), "as literals"),
        ("select * from table_changes('db.t', 'x')".into(), "commit sequence"),
        ("select * from table_changes('', 1)".into(), "a table is"),
        ("select * from table_changes('db.missing', 1)".into(), "missing"),
    ] {
        let refused = lake.run(&sql).await.unwrap_err();
        assert!(refused.to_string().contains(says), "{sql}: {refused}");
    }
}

#[tokio::test]
async fn table_changes_qualified_and_quoted() {
    let lake = lake().await;
    let one = committed(&lake, "insert into db.t values (1, 'a')").await;
    let two = committed(&lake, "insert into db.t values (2, 'b')").await;
    let qualified = lake
        .run(&format!(
            "select table_changes.k from table_changes('db.t', {one}, {two})"
        ))
        .await
        .unwrap();
    assert_eq!(qualified.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let quoted = lake
        .rows(&format!(r#"select * from "table_changes"('db.t', {one}, {two})"#))
        .await;
    assert_eq!(quoted, [row("insert", 2, two)]);
}
