//! `merge into`: a join of the table and a source, whose rows each take the first clause they meet, in one commit. The
//! join is DataFusion's, planned by the session, so it picks the join it would for any query; the table comes in through
//! the core's scan, which names each row by file and position, and what a clause does is a `delete`, an `update` or an
//! `insert`, as those statements do it.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, new_null_array};
use datafusion::arrow::compute::kernels::boolean::{and, not};
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::compute::{filter_record_batch, is_not_null, prep_null_mask_filter};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::catalog::default_table_source::provider_as_source;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{Column, DFSchema, DFSchemaRef, JoinType, TableReference, not_impl_err};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, col, lit};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures_util::{TryStreamExt, stream};
use metalake::vectors::{Deletes, FILE_ID};
use metalake::{Scan, Storage, TableWriter, WriteOptions};

use crate::catalog::external;
use crate::sink::mark;
use crate::table::{MetaTable, no_input};

/// The column that tells a row that came from the source.
const FROM_SOURCE: &str = "_metalake_from_source";

/// The rows of a table with their positions, as DataFusion reads a table.
#[derive(Debug)]
struct Rows {
    scan: Scan,
    storage: Storage,
}

impl PartitionStream for Rows {
    fn schema(&self) -> &SchemaRef {
        self.scan.schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let (scan, storage) = (self.scan.clone(), self.storage.clone());
        let schema = scan.schema().clone();
        let batches = stream::once(async move {
            let tasks = scan.plan().await?;
            Ok::<_, metalake::Error>(scan.read(tasks, &storage))
        })
        .try_flatten()
        .map_err(external);
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

/// The plan of a statement's source, as a table of a new plan.
#[derive(Debug)]
struct Source {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl PartitionStream for Source {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let plan = self.plan.clone();
        let batches = stream::once(async move { execute_stream(plan, context) }).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
    }
}

/// What a clause does.
#[derive(Debug)]
enum Action {
    /// The row anew, with these of the table's columns, by place, assigned.
    Update(Vec<(usize, Arc<dyn PhysicalExpr>)>),
    Delete,
    /// A new row, with these of the table's columns, by place; the rest null.
    Insert(Vec<(usize, Arc<dyn PhysicalExpr>)>),
}

/// A `when` of the statement, in physical terms, over the statement's combined columns.
#[derive(Debug)]
struct Clause {
    matched: bool,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    action: Action,
}

/// The plan of a `merge into` of `table`.
pub(crate) async fn plan(
    table: &MetaTable,
    state: &dyn Session,
    source: Arc<dyn ExecutionPlan>,
    combined: DFSchemaRef,
    on: Expr,
    clauses: Vec<MergeIntoClause>,
) -> Result<Arc<dyn ExecutionPlan>> {
    table.on.writable().map_err(external)?;
    let schema = table.table.schema().clone();
    let clauses = clauses
        .into_iter()
        .map(|clause| physical(clause, &schema, state, &combined))
        .collect::<Result<Vec<_>>>()?;

    // The table on the left, with its positions, and on the right the source, marked: its columns are the combined
    // schema's after the table's.
    let columns = schema.fields().len();
    let rows = Scan::new(&table.catalog, &table.table).with_positions();
    let rows = Rows {
        scan: rows,
        storage: table.storage.clone(),
    };
    let left = StreamingTable::try_new(rows.schema().clone(), vec![Arc::new(rows)])?;
    let left = LogicalPlanBuilder::scan(qualifier(&combined, 0), provider_as_source(Arc::new(left)), None)?;
    let source_schema = source.schema();
    let right = Source {
        plan: source,
        schema: source_schema.clone(),
    };
    let right = StreamingTable::try_new(source_schema.clone(), vec![Arc::new(right)])?;
    let right_name = qualifier(&combined, columns);
    let marked = source_schema
        .fields()
        .iter()
        .map(|f| col(Column::new(Some(right_name.clone()), f.name())))
        .chain([lit(true).alias(FROM_SOURCE)]);
    let right = LogicalPlanBuilder::scan(right_name.clone(), provider_as_source(Arc::new(right)), None)?
        .project(marked)?
        .build()?;
    let joined = left.join_on(right, JoinType::Right, [on])?.build()?;
    let joined = state.create_physical_plan(&joined).await?;

    // The rows clauses read, in the combined schema's order, where either side may be missing.
    let fields = combined.fields().iter().map(|f| f.as_ref().clone().with_nullable(true));
    let view = Arc::new(Schema::new(fields.collect::<Vec<Field>>()));
    let sink = Merge {
        table: table.clone(),
        joined,
        view,
        columns,
        clauses,
    };
    Ok(no_input(schema, sink))
}

/// The qualifier of the combined schema's column `at`.
fn qualifier(schema: &DFSchema, at: usize) -> TableReference {
    let (qualifier, _) = schema.qualified_field(at);
    qualifier.cloned().unwrap_or_else(|| TableReference::bare("source"))
}

fn physical(clause: MergeIntoClause, schema: &SchemaRef, state: &dyn Session, combined: &DFSchema) -> Result<Clause> {
    let matched = match clause.kind.canonical() {
        MergeIntoClauseKind::Matched => true,
        MergeIntoClauseKind::NotMatchedBySource => {
            return not_impl_err!("when not matched by source is not supported yet");
        }
        _ => false,
    };
    let expression = |e: Expr| state.create_physical_expr(e, combined);
    let predicate = clause.predicate.map(expression).transpose()?;
    let assigned = |(column, value): (String, Expr)| Ok((schema.index_of(&column)?, expression(value)?));
    let action = match clause.action {
        MergeIntoAction::Delete => Action::Delete,
        MergeIntoAction::Update(assignments) => {
            let assignments = assignments.into_iter().map(assigned);
            Action::Update(assignments.collect::<Result<_>>()?)
        }
        MergeIntoAction::Insert { columns, values } => {
            let columns = match columns.is_empty() {
                true => schema.fields().iter().map(|f| f.name().clone()).collect(),
                false => columns,
            };
            let values = columns.into_iter().zip(values).map(assigned);
            Action::Insert(values.collect::<Result<_>>()?)
        }
    };
    Ok(Clause {
        matched,
        predicate,
        action,
    })
}

/// A `merge into`: it runs the join, and commits what its clauses did to its rows, once.
#[derive(Debug)]
struct Merge {
    table: MetaTable,
    joined: Arc<dyn ExecutionPlan>,
    view: SchemaRef,
    /// How many of the join's first columns are the table's.
    columns: usize,
    clauses: Vec<Clause>,
}

impl Merge {
    /// A join's rows as clauses read them: the table's columns, then the source's, without the positions or the mark.
    fn view(&self, joined: &RecordBatch) -> Result<RecordBatch> {
        // The join's columns: the table's, its two of position, the source's, and the mark.
        let table = joined.columns()[..self.columns].iter();
        let source = joined.columns()[self.columns + 2..joined.num_columns() - 1].iter();
        Ok(RecordBatch::try_new(
            self.view.clone(),
            table.chain(source).cloned().collect(),
        )?)
    }

    /// A batch of the table's rows: each column from `base`, if any, else null, with `assigned` over it.
    fn row(&self, view: &RecordBatch, base: bool, assigned: &[(usize, Arc<dyn PhysicalExpr>)]) -> Result<RecordBatch> {
        let schema = self.table.table.schema();
        let rows = view.num_rows();
        let mut columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(at, field)| match base {
                true => view.column(at).clone(),
                false => new_null_array(field.data_type(), rows),
            })
            .collect();
        for (at, value) in assigned {
            let value = value.evaluate(view)?.into_array(rows)?;
            columns[*at] = cast(&value, schema.field(*at).data_type())?;
        }
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

/// Where `array` is true and known.
fn holds(array: &ArrayRef) -> BooleanArray {
    let array = array.as_boolean();
    match array.null_count() {
        0 => array.clone(),
        _ => prep_null_mask_filter(array),
    }
}

impl DisplayAs for Merge {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaLakeMerge: {}", self.table.table.name())
    }
}

#[async_trait]
impl DataSink for Merge {
    fn schema(&self) -> &SchemaRef {
        self.table.table.schema()
    }

    async fn write_all(&self, _: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
        let MetaTable {
            on,
            catalog,
            storage,
            table,
            ..
        } = &self.table;
        let mut joined = execute_stream(self.joined.clone(), context.clone())?;
        let mut writer =
            TableWriter::new(catalog, table, storage.clone(), WriteOptions::default()).map_err(external)?;
        let mut deletes = Deletes::new();
        let mut affected = 0;
        while let Some(batch) = joined.try_next().await? {
            let view = self.view(&batch)?;
            let in_table = is_not_null(batch.column_by_name(FILE_ID).expect("asked of the scan"))?;
            let mut pending = BooleanArray::from(vec![true; batch.num_rows()]);
            for clause in &self.clauses {
                let kind = if clause.matched {
                    in_table.clone()
                } else {
                    not(&in_table)?
                };
                let mut fires = and(&pending, &kind)?;
                if let Some(predicate) = &clause.predicate {
                    let holding = predicate.evaluate(&view)?.into_array(view.num_rows())?;
                    fires = and(&fires, &holds(&holding))?;
                }
                pending = and(&pending, &not(&fires)?)?;
                let rows = filter_record_batch(&view, &fires)?;
                if rows.num_rows() == 0 {
                    continue;
                }
                affected += rows.num_rows() as u64;
                match &clause.action {
                    Action::Delete => mark(&mut deletes, &filter_record_batch(&batch, &fires)?),
                    Action::Update(assigned) => {
                        mark(&mut deletes, &filter_record_batch(&batch, &fires)?);
                        writer
                            .write(&self.row(&rows, true, assigned)?)
                            .await
                            .map_err(external)?;
                    }
                    Action::Insert(assigned) => {
                        writer
                            .write(&self.row(&rows, false, assigned)?)
                            .await
                            .map_err(external)?;
                    }
                }
            }
        }
        let files = writer.finish().await.map_err(external)?;
        if !deletes.is_empty() || !files.is_empty() {
            let commit = on.transaction().append(files).delete(table, storage, deletes).commit();
            commit.await.map_err(external)?;
        }
        Ok(affected)
    }
}
