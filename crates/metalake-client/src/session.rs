//! Statements on a lake, each committed before the next is planned.

use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use futures_util::TryStreamExt;
use metalake_datafusion::MetaLake;

use crate::{Error, Lake, Result};

/// The engine over a lake, and a session of it.
pub struct Session {
    engine: MetaLake,
    context: SessionContext,
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

    /// Runs the statements of a text, each to its end before the next, and answers the rows of the last.
    pub async fn run(&self, text: &str) -> Result<SendableRecordBatchStream> {
        let mut statements = Self::parse(text)?;
        let last = statements.pop().expect("parse refuses a text with none");
        for statement in statements {
            let mut rows = self.execute(statement).await?;
            while rows.try_next().await?.is_some() {}
        }
        self.execute(last).await
    }

    /// Runs one statement, what creates a namespace or a table included, and answers its rows.
    pub async fn execute(&self, statement: Statement) -> Result<SendableRecordBatchStream> {
        let frame = self.engine.execute(&self.context, statement).await?;
        Ok(frame.execute_stream().await?)
    }
}
