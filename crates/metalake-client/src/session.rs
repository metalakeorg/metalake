//! Statements on a lake. A read stops at its plan, which already names its files, so that it costs nothing until it is
//! read and sees the lake as it was when prepared. Anything else runs at once, so that a write happens whether or not its
//! answer is looked at.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{SchemaRef, UInt64Type};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, WriteOp};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use futures_util::{StreamExt, TryStreamExt};
use metalake_datafusion::{CommitDetails, MetaLake, Source};

use crate::write::{Details, TablePath, Written};
use crate::{Error, Lake, Result};

/// The engine over a lake, and a session of it.
pub struct Session {
    engine: MetaLake,
    context: SessionContext,
}

/// Whether a plan only reads, and so can wait to be read. What is not known to be a read runs at once: running one early
/// costs little, and a write that never ran is lost.
fn is_read(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::DescribeTable(_)
    )
}

/// The rows a statement says it wrote, in the one column of its answer.
fn rows_of(answer: &[RecordBatch]) -> u64 {
    let count = answer.first().filter(|b| b.num_rows() > 0 && b.num_columns() > 0);
    count
        .and_then(|b| b.column(0).as_primitive_opt::<UInt64Type>())
        .map_or(0, |column| column.value(0))
}

/// A statement ready to read: a read at the point it was prepared, or what a statement that already ran answered.
pub struct Prepared {
    plan: Arc<dyn ExecutionPlan>,
    task: Arc<TaskContext>,
}

impl Prepared {
    /// Rows read once, as they come, without holding them: a step a follower gave. A second read is refused.
    pub fn from_stream(schema: SchemaRef, rows: crate::Changes) -> Result<Self> {
        let once = Arc::new(Once {
            schema: schema.clone(),
            rows: std::sync::Mutex::new(Some(rows)),
        });
        let plan = StreamingTableExec::try_new(schema, vec![once], None, Vec::<LexOrdering>::new(), false, None)?;
        Ok(Self {
            plan: Arc::new(plan),
            task: Arc::new(TaskContext::default()),
        })
    }

    /// Rows already read, as what a statement that already ran answers: a step a follower took, read as a result is.
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let plan = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
        Ok(Self {
            plan,
            task: Arc::new(TaskContext::default()),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    /// Its rows, from the same files each time. A plan keeps the state of a run, so each read has a clean one. It must
    /// be called on a runtime.
    pub fn read(&self) -> Result<SendableRecordBatchStream> {
        let plan = reset_plan_states(self.plan.clone())?;
        Ok(execute_stream(plan, self.task.clone())?)
    }
}

impl Session {
    pub async fn new(lake: &Lake) -> Result<Self> {
        let engine = MetaLake::new(lake.catalog.clone(), lake.storage()?).await?;
        let context = engine.context()?;
        Ok(Self { engine, context })
    }

    /// The statements of a text, in order. A text with none is a wrong use.
    pub fn parse(text: &str) -> Result<Vec<Statement>> {
        let statements = DFParser::parse_sql(text)?;
        if statements.is_empty() {
            return Err(Error::usage("no statement to run"));
        }
        Ok(statements.into())
    }

    /// Prepares one statement, and runs it if it does more than read.
    pub async fn prepare(&self, statement: Statement) -> Result<Prepared> {
        let frame = self.engine.execute(&self.context, statement).await?;
        let task = Arc::new(frame.task_ctx());
        let read = is_read(frame.logical_plan());
        let plan = frame.create_physical_plan().await?;
        if read {
            return Ok(Prepared { plan, task });
        }
        let schema = plan.schema();
        let answer = collect(plan, task.clone()).await?;
        let plan = MemorySourceConfig::try_new_exec(&[answer], schema, None)?;
        Ok(Prepared { plan, task })
    }

    /// Deletes the rows of a table that a filter matches, in one commit. The filter is SQL, so what it makes is looked at
    /// and not its text: one `delete`, of this table, and nothing else.
    pub async fn delete(&self, path: &TablePath, filter: &str, details: Details) -> Result<Written> {
        let text = format!("delete from {} where {filter}", path.quoted());
        let statements = Self::parse(&text).map_err(|e| Error::usage(format!("{filter}: {e}")))?;
        let [statement] = <[Statement; 1]>::try_from(statements)
            .map_err(|_| Error::usage(format!("{filter}: a filter is one expression")))?;
        let frame = self.engine.execute(&self.context, statement).await?;
        let LogicalPlan::Dml(delete) = frame.logical_plan() else {
            return Err(Error::usage(format!("{filter}: a filter is one expression")));
        };
        if delete.op != WriteOp::Delete || delete.table_name.table() != path.name {
            return Err(Error::usage(format!("{filter}: a filter is one expression")));
        }
        let details = Arc::new(details.into_commit());
        let rows = self.run_with(frame, details.clone()).await?;
        Ok(Written::of_statement(details.outcome(), rows))
    }

    /// Runs what writes, with what its commit should say.
    async fn run_with(&self, frame: DataFrame, details: Arc<CommitDetails>) -> Result<u64> {
        let task = frame.task_ctx();
        let config = task.session_config().clone().with_extension(details);
        let task = Arc::new(task.with_session_config(config));
        let plan = frame.create_physical_plan().await?;
        Ok(rows_of(&collect(plan, task).await?))
    }

    /// Rows in memory that statements can read as long as what this answers lives.
    pub(crate) fn source(&self, schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Source> {
        Ok(self.engine.source(Arc::new(MemTable::try_new(schema, vec![batches])?)))
    }

    /// Whether a query has a row.
    pub(crate) async fn has_rows(&self, text: &str) -> Result<bool> {
        let frame = self.context.sql(text).await?;
        Ok(frame.limit(0, Some(1))?.count().await? > 0)
    }

    /// Runs one `merge into`, and says how many rows it inserted and updated.
    pub(crate) async fn merge(&self, text: &str, details: Details) -> Result<Written> {
        let [statement] =
            <[Statement; 1]>::try_from(Self::parse(text)?).map_err(|_| Error::failed("a merge is one statement"))?;
        let frame = self.engine.execute(&self.context, statement).await?;
        let details = Arc::new(details.into_commit());
        let rows = self.run_with(frame, details.clone()).await?;
        let (inserted, updated) = details.merged();
        Ok(Written::of_statement(details.outcome(), rows).merged(inserted, updated))
    }

    /// Prepares the statements of a text, each committed before the next, and answers the last.
    pub async fn run(&self, text: &str) -> Result<Prepared> {
        let mut last = None;
        for statement in Self::parse(text)? {
            last = Some(self.prepare(statement).await?);
        }
        Ok(last.expect("parse refuses a text with none"))
    }

    /// Takes a new picture of what `information_schema` shows, for what others changed. A table or a ref named in a
    /// statement is found without it.
    pub async fn refresh(&self) -> Result<()> {
        Ok(self.engine.refresh().await?)
    }
}

/// Rows that can be read once, as they come: a step a follower gave, which counts once it is read to its end.
struct Once {
    schema: SchemaRef,
    rows: std::sync::Mutex<Option<crate::Changes>>,
}

impl std::fmt::Debug for Once {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Once").field("schema", &self.schema).finish()
    }
}

impl PartitionStream for Once {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let rows = self.rows.lock().expect("rows are only taken").take();
        let rows: futures_util::stream::BoxStream<'static, datafusion::error::Result<RecordBatch>> = match rows {
            Some(rows) => rows.map_err(|e| DataFusionError::External(Box::new(e))).boxed(),
            None => futures_util::stream::once(async {
                Err(DataFusionError::Execution("the rows of a step are read once".into()))
            })
            .boxed(),
        };
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), rows))
    }
}
