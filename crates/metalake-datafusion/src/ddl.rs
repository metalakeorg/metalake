//! `create schema`, `create table` and `alter table`, as commits. DataFusion registers what a statement creates through synchronous
//! calls that hand it a table in memory, which a protocol in Postgres cannot answer, so these statements are taken from
//! the plan before it runs: what a statement asks and this cannot do is refused, never dropped in silence.

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::common::TableReference;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{CreateMemoryTable, LogicalPlan};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, ColumnOption, DataType, Expr, Ident, RenameTableNameKind,
};
use futures_util::TryStreamExt;
use metalake::{
    Catalog, Error, NewTable, Ref, Storage, TableChange, TablePath, TableWriter, Transaction, WriteOptions,
};
use serde_json::{Value, json};
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

/// Changes the table an `alter table` names, at `path`, in one commit: columns added (last), dropped, renamed, widened or
/// made optional, and the table renamed on its ref. What the format does not allow, or this does not do, is refused, and
/// nothing changes.
pub(crate) async fn alter_table(
    on: &Ref,
    path: &TablePath,
    alter: &AlterTable,
    context: &SessionContext,
) -> DataFusionResult<()> {
    if alter.only || alter.location.is_some() || alter.on_cluster.is_some() || alter.table_type.is_some() {
        return Err(refused("alter table: only a table's columns and its name change"));
    }
    let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
    let table = match on.table(&namespace, &path.name).await {
        Ok(table) => table,
        Err(Error::NotFound(_)) if alter.if_exists => return Ok(()),
        Err(e) => return Err(external(e)),
    };
    let normalize = context.state().config().options().sql_parser.enable_ident_normalization;
    let name = |ident: &Ident| match (normalize, ident.quote_style) {
        (true, None) => ident.value.to_ascii_lowercase(),
        _ => ident.value.clone(),
    };
    let has = |column: &str| {
        table
            .fields()
            .iter()
            .any(|f| f.parent_field_id.is_none() && f.name == column)
    };
    let mut changes = Vec::new();
    for operation in &alter.operations {
        match operation {
            AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } => {
                if column_position.is_some() {
                    return Err(refused("a column at a position: a new column goes last"));
                }
                let column = name(&column_def.name);
                if *if_not_exists && has(&column) {
                    continue;
                }
                let ty = arrow_type(context, &column_def.data_type).await?;
                let (mut required, mut default) = (false, None);
                for option in &column_def.options {
                    match &option.option {
                        ColumnOption::NotNull => required = true,
                        ColumnOption::Null => required = false,
                        ColumnOption::Default(expr) => {
                            default = Some(default_of(context, expr, &column_def.data_type).await?)
                        }
                        other => return Err(refused(&format!("a column's {other}"))),
                    }
                }
                changes.push(TableChange::AddColumn {
                    name: column,
                    ty,
                    required,
                    default,
                    doc: None,
                });
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                for column in column_names.iter().map(name) {
                    if *if_exists && !has(&column) {
                        continue;
                    }
                    changes.push(TableChange::DropColumn(column));
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => changes.push(TableChange::RenameColumn {
                from: name(old_column_name),
                to: name(new_column_name),
            }),
            AlterTableOperation::AlterColumn { column_name, op } => {
                let column = name(column_name);
                match op {
                    AlterColumnOperation::DropNotNull => changes.push(TableChange::DropNotNull(column)),
                    AlterColumnOperation::SetDataType {
                        data_type, using: None, ..
                    } => changes.push(TableChange::Widen {
                        column,
                        ty: arrow_type(context, data_type).await?,
                    }),
                    AlterColumnOperation::SetNotNull => {
                        return Err(refused("set not null: rows written before may hold nulls"));
                    }
                    other => return Err(refused(&format!("alter column {other}"))),
                }
            }
            AlterTableOperation::RenameTable { table_name } => {
                let (RenameTableNameKind::To(to) | RenameTableNameKind::As(to)) = table_name;
                let renamed = match object_name_to_table_reference(to.clone(), normalize)? {
                    TableReference::Bare { table } => (path.namespace.clone(), table.to_string()),
                    TableReference::Partial { schema, table } => {
                        let to = TablePath::from_parts(None, Some(&schema), &table);
                        (to.namespace, to.name)
                    }
                    TableReference::Full { catalog, schema, table } if *catalog == *path.on => {
                        let to = TablePath::from_parts(Some(&catalog), Some(&schema), &table);
                        (to.namespace, to.name)
                    }
                    TableReference::Full { .. } => {
                        return Err(refused("rename to another ref: a table is renamed on its own"));
                    }
                };
                changes.push(TableChange::Rename {
                    namespace: renamed.0,
                    name: renamed.1,
                });
            }
            other => return Err(refused(&format!("alter table … {other}"))),
        }
    }
    if changes.is_empty() {
        return Ok(());
    }
    on.writable().map_err(external)?;
    let tx = on.transaction().alter(&table, &changes).await.map_err(external)?;
    details::commit(tx, &context.task_ctx()).await.map_err(external)
}

/// The Arrow type a SQL type is, as this session's `create table` takes it.
async fn arrow_type(context: &SessionContext, ty: &DataType) -> DataFusionResult<ArrowType> {
    let plan = context
        .state()
        .create_logical_plan(&format!("select cast(null as {ty}) as v"))
        .await?;
    Ok(plan.schema().field(0).data_type().clone())
}

/// A column's default, a constant of its type, in Iceberg's single-value JSON (TYP-5).
async fn default_of(context: &SessionContext, expr: &Expr, ty: &DataType) -> DataFusionResult<Value> {
    let rows = context
        .sql(&format!("select cast({expr} as {ty}) as v"))
        .await?
        .collect()
        .await?;
    let value = rows
        .first()
        .map(|b| b.column(0).clone())
        .filter(|c| c.len() == 1 && c.is_valid(0));
    let Some(value) = value else {
        return Err(refused("a default that is not one value: a default is a constant"));
    };
    Ok(match value.data_type() {
        ArrowType::Boolean => json!(value.as_boolean().value(0)),
        ArrowType::Int8 => json!(value.as_primitive::<Int8Type>().value(0)),
        ArrowType::Int16 => json!(value.as_primitive::<Int16Type>().value(0)),
        ArrowType::Int32 => json!(value.as_primitive::<Int32Type>().value(0)),
        ArrowType::Int64 => json!(value.as_primitive::<Int64Type>().value(0)),
        ArrowType::Float32 => json!(value.as_primitive::<Float32Type>().value(0)),
        ArrowType::Float64 => json!(value.as_primitive::<Float64Type>().value(0)),
        // Dates, times and decimals are strings in Iceberg's JSON, as Arrow writes them.
        ArrowType::Utf8 | ArrowType::Date32 | ArrowType::Decimal128(..) => {
            json!(cast(&value, &ArrowType::Utf8)?.as_string::<i32>().value(0))
        }
        other => return Err(refused(&format!("a default of type {other}"))),
    })
}

fn refused(what: &str) -> datafusion::error::DataFusionError {
    external(Error::Unsupported(what.to_string()))
}
