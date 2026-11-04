//! The grammar, and nothing else: no verb here reads a file or opens a connection.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::out::Format;

/// MetaLake, a lakehouse with its catalog in Postgres.
#[derive(Debug, Parser)]
#[command(name = "metalake", version)]
pub struct Cli {
    /// The directory of the lake's `metalake.toml`; by default this one or the nearest above it.
    #[arg(long, global = true, value_name = "DIR")]
    pub lake: Option<PathBuf>,

    /// How to print: a table at a terminal, comma-separated in a pipe; `json` is one object per line.
    #[arg(long, global = true, value_enum)]
    pub format: Option<Format>,

    #[command(subcommand)]
    pub verb: Verb,
}

#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Creates a catalog and writes `metalake.toml`.
    Init(Init),
    /// Says what the lake is: its catalog, versions, warehouse and refs.
    Status,
}

#[derive(Debug, Args)]
pub struct Init {
    /// The Postgres of the catalog, with no password: `postgresql://user@host/db`. The password is `PGPASSWORD`.
    pub postgres: String,

    /// Where tables put their files: a URL, such as `s3://bucket/prefix`, or a local directory.
    pub warehouse: String,

    /// The Postgres schema of the catalog, for more than one lake in a database.
    #[arg(long)]
    pub schema: Option<String>,
}
