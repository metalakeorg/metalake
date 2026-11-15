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

/// One statement of a text: the engine's, a pipeline's verb, which is the client's, or one for another engine.
#[derive(Debug)]
pub enum Step {
    Statement(Statement),
    Pipeline(crate::pipeline::Verb),
    /// `USING <engine> <statement>`: the statement is the engine's, sent to it as written.
    Using(crate::Using),
    /// `USING <engine>` alone: runs nothing, and chooses the engine for the statements after it. A step of its own, so
    /// that where the engine changes is seen with what follows.
    Choose(String),
}

impl Step {
    /// The engine that runs it, when not the lake's own: what a face says beside its answer.
    pub fn engine(&self) -> Option<&str> {
        match self {
            Step::Statement(_) | Step::Pipeline(_) => None,
            Step::Using(using) => Some(&using.engine),
            Step::Choose(engine) => (engine != crate::OWN).then_some(engine.as_str()),
        }
    }
}

/// A step and the text it was written as, which is what a face shows for it: the piece of the text it came from, or,
/// where the pieces and the statements do not match one to one, the statement as the parser writes it.
#[derive(Debug)]
pub struct SourcedStep {
    pub text: String,
    pub step: Step,
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
    /// A read, whose rows are the answer, or a statement that already ran, whose rows say what it did.
    read: bool,
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
            read: true,
        })
    }

    /// Rows already read, as what a statement that already ran answers: a step a follower took, read as a result is.
    pub fn from_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let plan = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
        Ok(Self {
            plan,
            task: Arc::new(TaskContext::default()),
            read: true,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    /// Whether its rows are a read's answer. Otherwise the statement already ran, and they say what it did: a write's
    /// row, with its commit, or none.
    pub fn is_read(&self) -> bool {
        self.read
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

    /// The ref names that say none read and write on: `main`, unless [`on_ref`](Self::on_ref) said another.
    pub fn current_ref(&self) -> String {
        let state = self.context.state_ref();
        let state = state.read();
        state.config().options().catalog.default_catalog.clone()
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
            return Ok(Prepared { plan, task, read });
        }
        let schema = plan.schema();
        let answer = collect(plan, task.clone()).await?;
        let plan = MemorySourceConfig::try_new_exec(&[answer], schema, None)?;
        Ok(Prepared { plan, task, read })
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

    /// The statements of a text, in order: the engine's; a pipeline's verbs (`CREATE PIPELINE …`, `SHOW PIPELINES`, …),
    /// which are the client's; and `USING <engine> …`, another engine's. A text with none of the client's goes to the
    /// engine whole, as written. A statement for another engine is not parsed: it is that engine's SQL.
    pub fn steps(text: &str) -> Result<Vec<Step>> {
        Ok(Self::sourced_steps(text)?.into_iter().map(|s| s.step).collect())
    }

    /// The steps of a text, each with the text it was written as: its piece of the text, or, where a piece holds more
    /// than one statement or the text is the lake's alone and does not split one to one, the statement as the parser
    /// writes it. Each piece is placed where the text chose before it, in one pass.
    pub fn sourced_steps(text: &str) -> Result<Vec<SourcedStep>> {
        use crate::engine::Placed;
        let pieces = crate::pipeline::sql::pieces(text);
        let mut chosen = None;
        let mut placed = Vec::with_capacity(pieces.len());
        for piece in &pieces {
            let place = crate::engine::place(piece, &mut chosen)?;
            // On the lake's engine, a pipeline's verb is the client's; anything else, DataFusion's.
            let verb = match &place {
                Placed::Own(sql) => crate::pipeline::sql::verb_of(sql)?,
                _ => None,
            };
            placed.push((piece.trim().to_string(), place, verb));
        }
        let written = |statement: &Statement| statement.to_string();
        // A text that is the lake's alone, with none of the client's, goes to DataFusion whole, as written.
        let lakes_alone = placed.iter().all(|(piece, place, verb)| {
            matches!(place, Placed::Own(sql) if sql.trim() == piece.as_str()) && verb.is_none()
        });
        if lakes_alone {
            let statements = Self::parse(text)?;
            let one_to_one = statements.len() == placed.len();
            return Ok(statements
                .into_iter()
                .enumerate()
                .map(|(at, statement)| SourcedStep {
                    text: if one_to_one {
                        placed[at].0.clone()
                    } else {
                        written(&statement)
                    },
                    step: Step::Statement(statement),
                })
                .collect());
        }
        let mut steps = Vec::new();
        for (piece, place, verb) in placed {
            match (place, verb) {
                (Placed::Chosen(engine), _) => steps.push(SourcedStep {
                    text: piece,
                    step: Step::Choose(engine),
                }),
                (Placed::Other(using), _) => steps.push(SourcedStep {
                    text: piece,
                    step: Step::Using(using),
                }),
                (Placed::Own(_), Some(verb)) => steps.push(SourcedStep {
                    text: piece,
                    step: Step::Pipeline(verb),
                }),
                (Placed::Own(sql), None) => {
                    let statements = Self::parse(&sql)?;
                    let one = statements.len() == 1;
                    steps.extend(statements.into_iter().map(|statement| SourcedStep {
                        text: if one { piece.clone() } else { written(&statement) },
                        step: Step::Statement(statement),
                    }));
                }
            }
        }
        Ok(steps)
    }

    /// Prepares one step, and runs it if it does more than read.
    pub async fn step(&self, step: Step) -> Result<Prepared> {
        match step {
            Step::Statement(statement) => self.prepare(statement).await,
            Step::Pipeline(verb) => crate::pipeline::sql::run(&self.lake, verb).await,
            Step::Using(using) => crate::engine::run_using(&self.lake, &self.current_ref(), &using).await,
            // It ran nothing: an answer with no column, as a statement that leaves nothing to show.
            Step::Choose(_) => {
                Prepared::from_batches(Arc::new(datafusion::arrow::datatypes::Schema::empty()), Vec::new())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(text: &str) -> Vec<String> {
        Session::sourced_steps(text)
            .unwrap()
            .into_iter()
            .map(|s| s.text)
            .collect()
    }

    #[test]
    fn each_step_keeps_the_text_it_was_written_as() {
        assert_eq!(
            texts("create table demo.t (a int);\n  insert into demo.t values (1) ;\nselect a from demo.t"),
            [
                "create table demo.t (a int)",
                "insert into demo.t values (1)",
                "select a from demo.t"
            ]
        );
    }

    #[test]
    fn a_pipeline_verb_is_a_step_with_its_text() {
        let steps = Session::sourced_steps("show pipelines; select 1 as one").unwrap();
        assert!(matches!(steps[0].step, Step::Pipeline(_)));
        assert_eq!(steps[0].text, "show pipelines");
        assert!(matches!(steps[1].step, Step::Statement(_)));
        assert_eq!(steps[1].text, "select 1 as one");
    }

    #[test]
    fn a_semicolon_in_a_string_or_a_comment_ends_nothing() {
        assert_eq!(
            texts("select 'a;b' as s -- a ; here\n; select 2"),
            ["select 'a;b' as s -- a ; here", "select 2"]
        );
    }

    #[test]
    fn a_text_with_no_statement_is_refused() {
        assert!(
            Session::sourced_steps("  -- nothing\n")
                .map(|s| s.is_empty())
                .unwrap_or(true)
        );
    }
}
