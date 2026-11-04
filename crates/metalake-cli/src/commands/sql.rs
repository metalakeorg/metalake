//! `metalake sql`: statements, in order, each committed before the next is planned, up to the first that fails.

use std::io::{self, IsTerminal, Read};

use datafusion::sql::parser::DFParser;
use metalake_datafusion::MetaLake;

use crate::args::Sql;
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

pub async fn run(sql: &Sql, lake: &Lake, out: &Out) -> Result<(), Failure> {
    let statements = DFParser::parse_sql(&text(sql)?)?;
    if statements.is_empty() {
        return Err(Failure::usage("no statement to run"));
    }
    let engine = MetaLake::new(lake.catalog.clone(), lake.storage()?).await?;
    let context = engine.context()?;
    for statement in statements {
        let frame = engine.execute(&context, statement).await?;
        out.rows(frame.execute_stream().await?).await?;
    }
    Ok(())
}

/// The statements, from the argument, the file or stdin.
fn text(sql: &Sql) -> Result<String, Failure> {
    if let Some(text) = &sql.sql {
        return Ok(text.clone());
    }
    if let Some(file) = &sql.file {
        return std::fs::read_to_string(file).map_err(|e| Failure::failed(format!("{}: {e}", file.display())));
    }
    let mut stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(Failure::usage("give a statement, or --file, or pipe them in"));
    }
    let mut text = String::new();
    stdin.read_to_string(&mut text)?;
    Ok(text)
}
