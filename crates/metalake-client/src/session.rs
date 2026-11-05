//! Statements on a lake. A read stops at its plan, which already names its files, so that it costs nothing until it is
//! read and sees the lake as it was when prepared. Anything else runs at once, so that a write happens whether or not its
//! answer is looked at.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use metalake_datafusion::MetaLake;

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

/// A statement ready to read: a read at the point it was prepared, or what a statement that already ran answered.
pub struct Prepared {
    plan: Arc<dyn ExecutionPlan>,
    task: Arc<TaskContext>,
}

impl Prepared {
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
