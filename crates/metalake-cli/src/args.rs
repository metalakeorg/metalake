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
    /// Runs SQL, in DataFusion's dialect: tables are `ref.namespace.table`, by default on `main.default`.
    Sql(Sql),
    /// Lists every branch and tag.
    Refs,
    /// A working copy of the whole lake, which writes alone until it is merged.
    Branch {
        #[command(subcommand)]
        verb: RefVerb,
    },
    /// A name for a state of the whole lake, which no commit changes and which keeps that state's data.
    Tag {
        #[command(subcommand)]
        verb: RefVerb,
    },
    /// Merges a branch into its parent, in one transaction.
    Merge(Merge),
}

#[derive(Debug, Args)]
pub struct Merge {
    pub branch: String,

    /// Says what the merge would do, or why it would be refused, and does nothing.
    #[arg(long)]
    pub check: bool,

    /// Merges only this table, `namespace.table`; the branch's others are rebased onto the merge. Repeatable.
    #[arg(long = "table", value_name = "TABLE")]
    pub tables: Vec<String>,

    /// Allows a selection that one of the branch's commits crossed, by splitting that commit.
    #[arg(long)]
    pub split: bool,

    /// The commit's message.
    #[arg(short, long)]
    pub message: Option<String>,

    /// The commit's author; by default `METALAKE_AUTHOR`, or the user of the machine.
    #[arg(long)]
    pub author: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum RefVerb {
    /// Lists them.
    List,
    /// Creates one, from `main` unless `--from` says otherwise.
    Create {
        name: String,
        /// The ref to start from, at a sequence if given: `dev` or `main@1234`.
        #[arg(long, value_name = "REF")]
        from: Option<String>,
    },
    /// Deletes one. Its name is never used again.
    Delete {
        name: String,
        /// Confirms it.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Args)]
pub struct Sql {
    /// The statements, separated by `;`. With neither this nor `--file`, they are read from stdin.
    pub sql: Option<String>,

    /// A file of statements.
    #[arg(short, long, value_name = "FILE", conflicts_with = "sql")]
    pub file: Option<PathBuf>,
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
