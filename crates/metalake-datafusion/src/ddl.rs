//! `create schema` and `create table`, as commits. DataFusion registers what a statement creates through synchronous
//! calls that hand it a table in memory, which a protocol in Postgres cannot answer, so these statements are taken from
//! the plan before it runs: what a statement asks and this cannot do is refused, never dropped in silence.

use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{CreateMemoryTable, LogicalPlan};
use metalake::{Error, NewTable, Ref};

use crate::catalog::external;

/// Creates a namespace on `on`.
pub(crate) async fn create_schema(on: &Ref, namespace: &[&str], if_not_exists: bool) -> DataFusionResult<()> {
    if if_not_exists {
        return Err(refused("create schema if not exists"));
    }
    on.writable().map_err(external)?;
    let commit = on.transaction().create_namespace(namespace).commit().await;
    commit.map(|_| ()).map_err(external)
}

/// Creates the table a statement declares, in `namespace` of `on`.
pub(crate) async fn create_table(
    on: &Ref,
    namespace: &[&str],
    name: &str,
    create: &CreateMemoryTable,
) -> DataFusionResult<()> {
    let refusal = match create {
        c if !matches!(c.input.as_ref(), LogicalPlan::EmptyRelation(e) if !e.produce_one_row) => {
            Some("create table as: create it, then insert")
        }
        c if c.or_replace => Some("create or replace table"),
        c if c.temporary => Some("create temporary table"),
        c if !c.constraints.is_empty() => Some("a constraint: a table has no key"),
        c if !c.column_defaults.is_empty() => Some("a column default"),
        _ => None,
    };
    if let Some(what) = refusal {
        return Err(refused(what));
    }
    on.writable().map_err(external)?;
    if create.if_not_exists {
        match on.table(namespace, name).await {
            Ok(_) => return Ok(()),
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(external(e)),
        }
    }
    let schema = create.input.schema().as_arrow();
    let (tx, _) = on
        .transaction()
        .create_table(NewTable {
            namespace,
            name,
            schema,
            location: None,
            cluster_key: vec![],
            partitioning: vec![],
            properties: vec![],
        })
        .map_err(external)?;
    tx.commit().await.map(|_| ()).map_err(external)
}

fn refused(what: &str) -> datafusion::error::DataFusionError {
    external(Error::Unsupported(what.to_string()))
}
