//! MetaLake's command line (ADR 0003 §4): a client like any adapter. It turns arguments into calls and results into
//! text, and decides nothing. The Python package runs the same [`main`].
//!
//! stdout holds what was asked for, and stderr everything else. The process ends with 0, or with the code of an
//! [`Exit`]: 1 a failure, 2 a wrong use, 3 a conflict that left nothing changed, 4 a write whose fate is unknown.

mod args;
mod commands;
mod failure;
mod lake;
mod out;

use std::ffi::OsString;

use clap::Parser;

pub use failure::Exit;

use args::{Cli, Verb};
use commands::refs::Kind;
use failure::Failure;
use lake::Lake;
use out::Out;

/// Runs a command line, its first argument being the program's name, and answers the code to exit with.
pub fn main<I, T>(args: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            return e.exit_code();
        }
    };
    let format = cli.format;
    let done = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Failure::from)
        .and_then(|runtime| runtime.block_on(run(cli)));
    match done {
        Ok(()) => 0,
        Err(failure) => {
            failure.report(format);
            failure.exit as i32
        }
    }
}

async fn run(cli: Cli) -> Result<(), Failure> {
    let out = Out::new(cli.format);
    let dir = cli.lake.as_deref();
    match &cli.verb {
        Verb::Init(init) => commands::init::run(init, dir, &out).await,
        Verb::Status => commands::status::run(&Lake::open(dir).await?, &out).await,
        Verb::Sql(sql) => commands::sql::run(sql, &Lake::open(dir).await?, &out).await,
        Verb::Refs => commands::refs::list(&Lake::open(dir).await?, &out, None).await,
        Verb::Branch { verb } => commands::refs::run(verb, Kind::Branch, &Lake::open(dir).await?, &out).await,
        Verb::Tag { verb } => commands::refs::run(verb, Kind::Tag, &Lake::open(dir).await?, &out).await,
        Verb::Merge(merge) => commands::merge::run(merge, &Lake::open(dir).await?, &out).await,
    }
}
