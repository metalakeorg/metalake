//! `metalake sql`: statements, in order, each committed before the next is planned, up to the first that fails.

use std::io::{self, IsTerminal, Read};

use metalake_client::{Error, Lake, Session};

use crate::args::Sql;
use crate::out::Out;

pub async fn run(sql: &Sql, lake: &Lake, out: &Out) -> Result<(), Error> {
    let statements = Session::parse(&text(sql)?)?;
    let session = Session::new(lake).await?;
    for statement in statements {
        out.rows(session.prepare(statement).await?.read()?).await?;
    }
    Ok(())
}

/// The statements, from the argument, the file or stdin.
fn text(sql: &Sql) -> Result<String, Error> {
    if let Some(text) = &sql.sql {
        return Ok(text.clone());
    }
    if let Some(file) = &sql.file {
        return std::fs::read_to_string(file).map_err(|e| Error::failed(format!("{}: {e}", file.display())));
    }
    let mut stdin = io::stdin();
    if stdin.is_terminal() {
        return Err(Error::usage("give a statement, or --file, or pipe them in"));
    }
    let mut text = String::new();
    stdin.read_to_string(&mut text)?;
    Ok(text)
}
