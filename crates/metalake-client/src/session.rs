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

/// One statement of a text: the engine's, a pipeline's or a ref's verb, which are the client's, or one for another
/// engine.
#[derive(Debug)]
pub enum Step {
    Statement(Statement),
    Pipeline(crate::pipeline::Verb),
    /// A ref's verb (I5): `CREATE BRANCH`, `MERGE BRANCH`, `USE BRANCH`…
    Ref(crate::refs_sql::RefVerb),
    /// A catalog's verb (I7): `CREATE CATALOG`, `DROP CATALOG`, `SHOW CATALOGS`.
    Catalog(crate::catalogs_sql::CatalogVerb),
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
            Step::Statement(_) | Step::Pipeline(_) | Step::Ref(_) | Step::Catalog(_) => None,
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

/// The client's own step a text says, if it says one: a pipeline's verb or a ref's, which DataFusion never sees.
pub(crate) fn client_verb(text: &str) -> Result<Option<Step>> {
    if let Some(verb) = crate::pipeline::sql::verb_of(text)? {
        return Ok(Some(Step::Pipeline(verb)));
    }
    if let Some(verb) = crate::catalogs_sql::catalog_verb_of(text)? {
        return Ok(Some(Step::Catalog(verb)));
    }
    Ok(crate::refs_sql::ref_verb_of(text)?.map(Step::Ref))
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

    /// Every name reads and writes on `name` (or `name@point`), from the next statement: no name says its ref (I7). The
    /// session is on `main` until this is called.
    pub fn on_ref(&self, name: &str) {
        MetaLake::on_ref(&self.context, name);
    }

    /// The ref every name reads and writes on: `main`, unless [`on_ref`](Self::on_ref) said another.
    pub fn current_ref(&self) -> String {
        MetaLake::current_ref(&self.context)
    }

    /// Refuses `what` in a session read at a point (`USE BRANCH … AS OF`): it would act on the branch as it is now.
    fn live(&self, what: &str) -> Result<()> {
        if self.current_ref() == self.current_ref_name() {
            return Ok(());
        }
        Err(Error::usage(format!(
            "{what} is on a branch as it is now, and this session reads {} at a point: USE BRANCH {} first",
            self.current_ref_name(),
            self.current_ref_name()
        )))
    }

    /// The name of the ref the session is on, without the point `USE … AS OF` reads it at.
    fn current_ref_name(&self) -> String {
        let on = self.current_ref();
        on.split_once('@').map_or(on.clone(), |(name, _)| name.to_string())
    }

    /// The statements of a text, in order. A text with none is a wrong use.
    pub fn parse(text: &str) -> Result<Vec<Statement>> {
        let statements = DFParser::parse_sql(text)?;
        if statements.is_empty() {
            return Err(Error::usage("no statement to run"));
        }
        Ok(statements.into())
    }

    /// A context of this session's engine on the ref `on`: this session's own when it is on it, else one apart, so that
    /// a statement for a table opened on `on` reads and writes there whatever ref the session moved to (I7).
    fn context_on(&self, on: &str) -> Result<SessionContext> {
        if self.current_ref() == on {
            return Ok(self.context.clone());
        }
        let context = self.engine.context()?;
        MetaLake::on_ref(&context, on);
        Ok(context)
    }

    /// Prepares one statement, and runs it if it does more than read.
    pub async fn prepare(&self, statement: Statement) -> Result<Prepared> {
        self.prepare_in(&self.context.clone(), statement).await
    }

    /// Prepares one statement of text on the ref `on`, whatever ref the session is on: a table's own reads.
    pub async fn prepare_on(&self, text: &str, on: &str) -> Result<Prepared> {
        let [statement] = <[Statement; 1]>::try_from(Self::parse(text)?)
            .map_err(|_| Error::failed("one statement is prepared on a ref"))?;
        self.prepare_in(&self.context_on(on)?, statement).await
    }

    async fn prepare_in(&self, context: &SessionContext, statement: Statement) -> Result<Prepared> {
        let frame = self.engine.execute(context, statement).await?;
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

    /// Deletes the rows of a table that a filter matches, in one commit, on the table's ref. The filter is SQL, so what it
    /// makes is looked at and not its text: one `delete`, of this table, and nothing else.
    pub async fn delete(&self, path: &TablePath, filter: &str, details: Details) -> Result<Written> {
        let text = format!("delete from {} where {filter}", path.quoted());
        let statements = Self::parse(&text).map_err(|e| Error::usage(format!("{filter}: {e}")))?;
        let [statement] = <[Statement; 1]>::try_from(statements)
            .map_err(|_| Error::usage(format!("{filter}: a filter is one expression")))?;
        let frame = self.engine.execute(&self.context_on(&path.on)?, statement).await?;
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

    /// Runs one `merge into` on the ref `on`, and says how many rows it inserted and updated.
    pub(crate) async fn merge(&self, text: &str, on: &str, details: Details) -> Result<Written> {
        let [statement] =
            <[Statement; 1]>::try_from(Self::parse(text)?).map_err(|_| Error::failed("a merge is one statement"))?;
        let frame = self.engine.execute(&self.context_on(on)?, statement).await?;
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
            // On the lake's engine, a pipeline's verb or a ref's is the client's; anything else, DataFusion's.
            let verb = match &place {
                Placed::Own(sql) => client_verb(sql)?,
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
                (Placed::Own(_), Some(step)) => steps.push(SourcedStep { text: piece, step }),
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
            Step::Pipeline(verb) => {
                if matches!(verb, crate::pipeline::Verb::Create { .. }) {
                    self.live("CREATE PIPELINE")?;
                }
                crate::pipeline::sql::run(&self.lake, verb, &self.current_ref_name()).await
            }
            Step::Ref(verb) => self.run_ref(verb).await,
            Step::Catalog(verb) => {
                if !matches!(verb, crate::catalogs_sql::CatalogVerb::Show) {
                    self.live("a catalog's change")?;
                }
                let on = self.lake.find_ref(&self.current_ref()).await?;
                let answer = crate::catalogs_sql::run(&on, verb).await;
                // A picture of the listings that cannot be taken now does not fail what committed.
                let _ = self.engine.refresh().await;
                answer
            }
            Step::Using(using) => crate::engine::run_using(&self.lake, &self.current_ref(), &using).await,
            // It ran nothing: an answer with no column, as a statement that leaves nothing to show.
            Step::Choose(_) => {
                Prepared::from_batches(Arc::new(datafusion::arrow::datatypes::Schema::empty()), Vec::new())
            }
        }
    }

    /// Runs a ref's verb with what the command line and Python run, and answers its rows (I5). Whatever changes the refs
    /// takes a new picture of `information_schema`, which then shows them; `USE` moves the session, as `lake.on` does.
    async fn run_ref(&self, verb: crate::refs_sql::RefVerb) -> Result<Prepared> {
        use crate::refs_sql::{RefVerb, dropped_rows, log_rows, merge_rows, ref_rows, retain_rows};
        let lake = &self.lake;
        let named = |refs: Vec<crate::refs::RefInfo>, name: &str| -> Vec<crate::refs::RefInfo> {
            refs.into_iter().filter(|r| r.name == name).collect()
        };
        match verb {
            RefVerb::Create {
                kind,
                name,
                from,
                if_not_exists,
            } => {
                if if_not_exists && lake.has_ref(&name).await? {
                    return ref_rows(&named(lake.refs(Some(kind)).await?, &name));
                }
                let created = lake.create_ref(kind, &name, from.as_deref()).await?;
                self.refresh().await?;
                ref_rows(&[created])
            }
            RefVerb::Drop { kind, name, if_exists } => {
                if name == self.current_ref_name() {
                    return Err(Error::usage(format!(
                        "{name} is the ref this session is on: USE another before dropping it"
                    )));
                }
                if if_exists && named(lake.refs(Some(kind)).await?, &name).is_empty() {
                    return dropped_rows(None);
                }
                lake.delete_ref(kind, &name).await?;
                self.refresh().await?;
                dropped_rows(Some(&name))
            }
            RefVerb::Merge(request) => {
                let outcome = lake.merge(&request).await?;
                if !request.check {
                    self.refresh().await?;
                }
                merge_rows(&outcome)
            }
            RefVerb::Show(kind) => ref_rows(&lake.refs(Some(kind)).await?),
            RefVerb::Log { on, limit } => {
                let on = on.unwrap_or_else(|| self.current_ref_name());
                log_rows(&lake.commits(&on, limit).await?)
            }
            RefVerb::Use { kind, name, at } => {
                let found = named(lake.refs(Some(kind)).await?, &name);
                if found.is_empty() {
                    let kind = match kind {
                        crate::refs::RefKind::Branch => "branch",
                        crate::refs::RefKind::Tag => "tag",
                    };
                    return Err(Error::usage(format!("no {kind} {name}")));
                }
                // At a point, the ref is read as it was then: `name@point`, as the engine finds it.
                match at {
                    Some(at) => self.on_ref(&format!("{name}@{at}")),
                    None => self.on_ref(&name),
                }
                ref_rows(&found)
            }
            RefVerb::Retain { name, retention, .. } => {
                lake.retain(&name, &retention).await?;
                retain_rows(Some(&name), &retention, None)
            }
            RefVerb::Grace(grace) => {
                lake.set_grace(grace).await?;
                retain_rows(None, &metalake::Retention::default(), Some(grace))
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
