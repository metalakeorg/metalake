//! MetaLake's command line (ADR 0003 §4): a client like any adapter. It turns arguments into calls and results into
//! text, and decides nothing. The Python package runs the same [`main`].
//!
//! stdout holds what was asked for, and stderr everything else. The process ends with 0, or with the code of an
//! [`Exit`]: 1 a failure, 2 a wrong use, 3 a conflict that left nothing changed, 4 a write whose fate is unknown.

mod args;
mod commands;
mod failure;
mod out;

use std::ffi::OsString;
use std::sync::Arc;

use clap::Parser;
use metalake_client::{Error, Lake, RefKind};

pub use failure::Exit;

use args::{Cli, IcebergVerb, Verb};
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
        .map_err(Error::from)
        .and_then(|runtime| runtime.block_on(run(cli)));
    match done {
        Ok(()) => 0,
        Err(failure) => {
            failure::report(&failure, format);
            Exit::from(failure.kind) as i32
        }
    }
}

async fn run(cli: Cli) -> Result<(), Error> {
    let out = Out::new(cli.format);
    let dir = cli.lake.as_deref();
    match &cli.verb {
        Verb::Init(init) => commands::init::run(init, dir, &out).await,
        Verb::Status => commands::status::run(&Lake::open(dir).await?, &out).await,
        Verb::Sql(sql) => commands::sql::run(sql, &Lake::open(dir).await?, &out).await,
        Verb::Refs => commands::refs::list(&Lake::open(dir).await?, &out, None).await,
        Verb::Branch { verb } => commands::refs::run(verb, RefKind::Branch, &Lake::open(dir).await?, &out).await,
        Verb::Tag { verb } => commands::refs::run(verb, RefKind::Tag, &Lake::open(dir).await?, &out).await,
        Verb::Merge(merge) => commands::merge::run(merge, &Lake::open(dir).await?, &out).await,
        Verb::Migrate => commands::migrate::run(dir, &out).await,
        Verb::Retain(retain) => commands::retain::run(retain, &Lake::open(dir).await?, &out).await,
        Verb::Maintain(maintain) => commands::maintain::run(maintain, &Lake::open(dir).await?, &out).await,
        Verb::Stop => commands::stop::run(dir, &out).await,
        Verb::Ui(ui) => commands::ui::run(ui, Lake::open(dir).await?).await,
        Verb::Log(log) => commands::log::run(log, &Lake::open(dir).await?, &out).await,
        // Stopping a server reaches no catalog, as stopping the lake's Postgres does not.
        Verb::Iceberg {
            verb: IcebergVerb::Stop,
        } => commands::iceberg::stop(dir, &out).await,
        Verb::Iceberg { verb } => commands::iceberg::run(verb, &Lake::open(dir).await?, &out).await,
        Verb::Changes(changes) => commands::changes::run(changes, &Lake::open(dir).await?, &out).await,
        Verb::Follow(follow) => commands::follow::run(follow, &Lake::open(dir).await?, cli.format).await,
        Verb::Ingest(ingest) => commands::ingest::run(ingest, Arc::new(Lake::open(dir).await?)).await,
        Verb::Pipeline { verb } => commands::pipeline::run(verb, &Lake::open(dir).await?, &out).await,
    }
}
