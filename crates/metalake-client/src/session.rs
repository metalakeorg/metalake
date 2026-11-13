//! Statements on a lake. A read stops at its plan, which already names its files, so that it costs nothing until it is
//! read and sees the lake as it was when prepared. Anything else runs at once, so that a write happens whether or not its
//! answer is looked at.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
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
use metalake_datafusion::{MetaLake, MetaLakeOptions, Source, WrittenRow};

use crate::write::{Details, TablePath, Written};
use crate::{Error, Lake, Result};

/// One statement of a text: the engine's, or a pipeline's verb, which is the client's.
#[derive(Debug)]
pub enum Step {
    Statement(Statement),
    Pipeline(crate::pipeline::Verb),
}

/// The engine over a lake, and a session of it.
pub struct Session {
    engine: MetaLake,
    context: SessionContext,
    /// For what is the client's own and not the engine's: a pipeline's verbs.
    lake: Lake,
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

/// A session of `engine` that reaches the scheduler at `url`, its router over this process and the cluster.
#[cfg(feature = "distributed")]
async fn distributed(engine: &MetaLake, url: &str) -> Result<SessionContext> {
    Ok(metalake_ballista::session(engine, url).await?)
}

#[cfg(not(feature = "distributed"))]
async fn distributed(_: &MetaLake, url: &str) -> Result<SessionContext> {
    Err(Error::usage(format!(
        "{url}: this build runs statements in its own process only, without the `distributed` feature"
    )))
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
        // With a scheduler, the session's router chooses where each statement runs; without, all run here.
        let context = match lake.scheduler() {
            Some(url) => distributed(&engine, &url).await?,
            None => engine.context()?,
        };
        Ok(Self {
            engine,
            context,
            lake: lake.clone(),
        })
    }

    /// Names that do not say their ref read and write on `name`: `default.t` is `name.default.t`, as it is
    /// `main.default.t` until this is called. A name that says its ref is not changed.
    pub fn on_ref(&self, name: &str) {
        let state = self.context.state_ref();
        let mut state = state.write();
        state.config_mut().options_mut().catalog.default_catalog = name.to_string();
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
        let row = self.run_with(frame, details).await?;
        Ok(Written::of_statement(row.outcome, row.count))
    }

    /// Runs what writes, with what its commit should say, and answers its row.
    async fn run_with(&self, frame: DataFrame, details: Details) -> Result<WrittenRow> {
        let task = frame.task_ctx();
        // What it asks travels as options, to wherever it runs; what came of it comes back as the write's row.
        let (author, message, idempotent) = details.asked();
        let config = MetaLakeOptions::commit(task.session_config().clone(), author, message, idempotent);
        let task = Arc::new(task.with_session_config(config));
        let plan = frame.create_physical_plan().await?;
        let batches = collect(plan, task).await?;
        let batch = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| Error::failed("a write answered no row"))?;
        Ok(WrittenRow::of(batch)?)
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
        let row = self.run_with(frame, details).await?;
        Ok(Written::of_statement(row.outcome, row.count).merged(row.inserted, row.updated))
    }

    /// The statements of a text, in order: the engine's, and a pipeline's verbs (`CREATE PIPELINE …`, `SHOW PIPELINES`,
    /// …), which are the client's. A text with no verb goes to the engine whole, as written.
    pub fn steps(text: &str) -> Result<Vec<Step>> {
        let pieces = crate::pipeline::sql::pieces(text);
        let mut verbs = Vec::with_capacity(pieces.len());
        for piece in &pieces {
            verbs.push(crate::pipeline::sql::verb_of(piece)?);
        }
        if verbs.iter().all(Option::is_none) {
            return Ok(Self::parse(text)?.into_iter().map(Step::Statement).collect());
        }
        let mut steps = Vec::new();
        for (piece, verb) in pieces.iter().zip(verbs) {
            match verb {
                Some(verb) => steps.push(Step::Pipeline(verb)),
                None => steps.extend(Self::parse(piece)?.into_iter().map(Step::Statement)),
            }
        }
        Ok(steps)
    }

    /// Prepares one step, and runs it if it does more than read.
    pub async fn step(&self, step: Step) -> Result<Prepared> {
        match step {
            Step::Statement(statement) => self.prepare(statement).await,
            Step::Pipeline(verb) => crate::pipeline::sql::run(&self.lake, verb).await,
        }
    }

    /// Prepares the statements of a text, each committed before the next, and answers the last.
    pub async fn run(&self, text: &str) -> Result<Prepared> {
        let mut last = None;
        for step in Self::steps(text)? {
            last = Some(self.step(step).await?);
        }
        last.ok_or_else(|| Error::usage("no statement to run"))
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
