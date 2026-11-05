//! `merge into`: a join of the table and a source, whose rows each take the first clause they meet, in one commit. The
//! join is DataFusion's, planned by the session, so it picks the join it would for any query; the table comes in through
//! DataFusion's reader, with each row's file and position, and what a clause does is a `delete`, an `update` or an
//! `insert`, as those statements do it.
//!
//! An update or a delete conflicts by file, as those statements do. An insert decides by a row's absence, which touches
//! no file, so a merge that inserts is refused if the table changed at all: two merges adding one key would both go in.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, new_null_array};
use datafusion::arrow::compute::kernels::boolean::{and, not};
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::compute::{filter_record_batch, is_not_null, prep_null_mask_filter};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::default_table_source::provider_as_source;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, DFSchemaRef, JoinType, TableReference, exec_err};
use datafusion::datasource::TableType;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, col, lit};
use datafusion::parquet::arrow::RowNumber;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures_util::{TryStreamExt, stream};
use metalake::vectors::{Deletes, FILE_ID, POSITION};
use metalake::{Scan, TableWriter, WriteOptions};

use crate::catalog::external;
use crate::details;
use crate::sink::mark;
use crate::table::{MetaTable, no_input};

/// The column that tells a row that came from the source.
const FROM_SOURCE: &str = "_metalake_from_source";

/// The rows of a table with their positions, as DataFusion reads a table, so that a join's filters reach them.
#[derive(Debug)]
struct Positioned {
    table: MetaTable,
    schema: SchemaRef,
}

impl Positioned {
    fn new(table: &MetaTable) -> Self {
        let columns = table.table.schema().fields().iter().cloned();
        let file = Arc::new(Field::new(FILE_ID, DataType::Int64, false));
        let position = Field::new(POSITION, DataType::Int64, false).with_extension_type(RowNumber);
        let fields: Vec<FieldRef> = columns.chain([file, Arc::new(position)]).collect();
        Self {
            table: table.clone(),
            schema: Arc::new(Schema::new(fields)),
        }
    }
}

#[async_trait]
impl TableProvider for Positioned {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (table, read) = (&self.table, &self.table.table);
        let scan = Scan::new(&table.catalog, read);
        table
            .read(state, read, scan, projection, limit, Some(&self.schema))
            .await
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

/// Which joined rows a clause is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// In both.
    Matched,
    /// In the source only.
    NotMatched,
    /// In the table only.
    NotMatchedBySource,
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
    kind: Kind,
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

    // The source on the left, marked, so that it builds the join and its keys filter the table on the right, read with
    // its positions. Rows of the table that the source lacks are read only for a clause about them.
    let columns = schema.fields().len();
    let source_schema = source.schema();
    let right = Source {
        plan: source,
        schema: source_schema.clone(),
    };
    let right = StreamingTable::try_new(source_schema.clone(), vec![Arc::new(right)])?;
    let source_name = qualifier(&combined, columns);
    let marked = source_schema
        .fields()
        .iter()
        .map(|f| col(Column::new(Some(source_name.clone()), f.name())))
        .chain([lit(true).alias(FROM_SOURCE)]);
    let left =
        LogicalPlanBuilder::scan(source_name.clone(), provider_as_source(Arc::new(right)), None)?.project(marked)?;
    let rows = provider_as_source(Arc::new(Positioned::new(table)));
    let rows = LogicalPlanBuilder::scan(qualifier(&combined, 0), rows, None)?.build()?;
    let join = match clauses.iter().any(|c| c.kind == Kind::NotMatchedBySource) {
        true => JoinType::Full,
        false => JoinType::Left,
    };
    let joined = left.join_on(rows, join, [on])?.build()?;
    // A join across partitions starts reading the table before the source has been read, and its filter comes too late:
    // one that collects the source first reads the table after it. So the whole source goes into memory.
    let mut planner = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| DataFusionError::Internal("a session without its state".into()))?
        .clone();
    planner.config_mut().options_mut().optimizer.repartition_joins = false;
    let joined = planner.create_physical_plan(&joined).await?;

    let inserts = clauses.iter().any(|c| matches!(c.action, Action::Insert(_)));

    // The rows clauses read, in the combined schema's order, where either side may be missing.
    let fields = combined.fields().iter().map(|f| f.as_ref().clone().with_nullable(true));
    let view = Arc::new(Schema::new(fields.collect::<Vec<Field>>()));
    let sink = Merge {
        table: table.clone(),
        joined,
        view,
        columns,
        clauses,
        inserts,
    };
    Ok(no_input(schema, sink))
}

/// The qualifier of the combined schema's column `at`.
fn qualifier(schema: &DFSchema, at: usize) -> TableReference {
    let (qualifier, _) = schema.qualified_field(at);
    qualifier.cloned().unwrap_or_else(|| TableReference::bare("source"))
}

fn physical(clause: MergeIntoClause, schema: &SchemaRef, state: &dyn Session, combined: &DFSchema) -> Result<Clause> {
    let kind = match clause.kind.canonical() {
        MergeIntoClauseKind::Matched => Kind::Matched,
        MergeIntoClauseKind::NotMatchedBySource => Kind::NotMatchedBySource,
        _ => Kind::NotMatched,
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
        kind,
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
    /// How many of the view's first columns are the table's.
    columns: usize,
    clauses: Vec<Clause>,
    /// Whether a clause inserts: it decides by a row's absence, which no vector records.
    inserts: bool,
}

impl Merge {
    /// A join's rows as clauses read them: the table's columns, then the source's, without the positions or the mark.
    fn view(&self, joined: &RecordBatch) -> Result<RecordBatch> {
        // The join's columns: the source's, its mark, the table's, and its two of position.
        let sources = self.view.fields().len() - self.columns;
        let source = joined.columns()[..sources].iter();
        let table = joined.columns()[sources + 1..sources + 1 + self.columns].iter();
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
        let (mut affected, mut marked) = (0, 0);
        while let Some(batch) = joined.try_next().await? {
            let view = self.view(&batch)?;
            let column = |name: &str| batch.column_by_name(name).expect("asked of the join");
            let in_table = is_not_null(column(FILE_ID))?;
            let in_source = is_not_null(column(FROM_SOURCE))?;
            let mut pending = BooleanArray::from(vec![true; batch.num_rows()]);
            for clause in &self.clauses {
                let kind = match clause.kind {
                    Kind::Matched => and(&in_table, &in_source)?,
                    Kind::NotMatched => not(&in_table)?,
                    Kind::NotMatchedBySource => not(&in_source)?,
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
                if !matches!(clause.action, Action::Insert(_)) {
                    marked += rows.num_rows() as u64;
                    mark(&mut deletes, &filter_record_batch(&batch, &fires)?);
                }
                match &clause.action {
                    Action::Delete => {}
                    Action::Update(assigned) => {
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
        // A row taken twice would depend on the order of the source: the standard refuses it.
        let distinct: u64 = deletes.files().values().map(|v| v.cardinality()).sum();
        if distinct != marked {
            return exec_err!("merge into: a row of {} matches more than one source row", table.name());
        }
        let files = writer.finish().await.map_err(external)?;
        if !deletes.is_empty() || !files.is_empty() {
            let mut transaction = on.transaction().append(files).delete(table, storage, deletes);
            if self.inserts {
                transaction = transaction.expect_unchanged(table);
            }
            details::commit(transaction, context).await.map_err(external)?;
            details::merged(context, affected - marked, marked);
        }
        Ok(affected)
    }
}
