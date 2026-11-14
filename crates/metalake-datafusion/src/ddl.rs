//! `create schema` and `create table`, as commits. DataFusion registers what a statement creates through synchronous
//! calls that hand it a table in memory, which a protocol in Postgres cannot answer, so these statements are taken from
//! the plan before it runs: what a statement asks and this cannot do is refused, never dropped in silence.

use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{CreateMemoryTable, LogicalPlan};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use futures_util::TryStreamExt;
use metalake::{Catalog, Error, NewTable, Ref, Storage, TableWriter, Transaction, WriteOptions};
use uuid::Uuid;

use crate::catalog::external;
use crate::details;

/// What a statement that writes rows writes them with: the session that runs its query, and the lake's catalog and
/// storage.
pub(crate) struct Writing<'a> {
    pub context: &'a SessionContext,
    pub catalog: &'a Catalog,
    pub storage: &'a Storage,
}

/// Creates a namespace on `on`.
pub(crate) async fn create_schema(on: &Ref, namespace: &[&str], if_not_exists: bool) -> DataFusionResult<()> {
    if if_not_exists {
        return Err(refused("create schema if not exists"));
    }
    on.writable().map_err(external)?;
    let commit = on.transaction().create_namespace(namespace).commit().await;
    commit.map(|_| ()).map_err(external)
}

/// Creates the table a statement declares, in `namespace` of `on`: by its columns, or by a query's, with its rows. One
/// commit makes the table and what the query gave, or neither.
pub(crate) async fn create_table(
    on: &Ref,
    namespace: &[&str],
    name: &str,
    create: &CreateMemoryTable,
    writing: Writing<'_>,
) -> DataFusionResult<()> {
    let refusal = match create {
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
    let (tx, id) = on
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
    let selects = !matches!(create.input.as_ref(), LogicalPlan::EmptyRelation(e) if !e.produce_one_row);
    let tx = match selects {
        true => filled(tx, id, create, &writing).await?,
        false => tx,
    };
    details::commit(tx, &writing.context.task_ctx()).await.map_err(external)
}

/// The rows of a `create table … as select`, written as the first files of the table the transaction creates.
async fn filled(
    tx: Transaction,
    id: Uuid,
    create: &CreateMemoryTable,
    writing: &Writing<'_>,
) -> DataFusionResult<Transaction> {
    let table = tx.created(id).map_err(external)?;
    let plan = writing.context.state().create_physical_plan(&create.input).await?;
    let mut rows = execute_stream(plan, writing.context.task_ctx())?;
    let mut writer = TableWriter::new(
        writing.catalog,
        &table,
        writing.storage.clone(),
        WriteOptions::default(),
    )
    .map_err(external)?;
    while let Some(batch) = rows.try_next().await? {
        writer.write(&batch).await.map_err(external)?;
    }
    Ok(tx.append(writer.finish().await.map_err(external)?))
}

fn refused(what: &str) -> datafusion::error::DataFusionError {
    external(Error::Unsupported(what.to_string()))
}
