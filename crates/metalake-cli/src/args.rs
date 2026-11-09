//! The grammar, and nothing else: no verb here reads a file or opens a connection.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

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
    /// Upgrades the catalog to this binary's protocol, once per upgrade of `metalake`, holding every ref meanwhile.
    Migrate,
    /// Says what the lake keeps: until a ref is told, it keeps everything, and `maintain` takes nothing away.
    Retain(Retain),
    /// Runs the upkeep once: all of it, in order, or one task. For whatever schedules it.
    Maintain(Maintain),
    /// Stops a lake's own Postgres, which stays up otherwise: before moving or deleting the lake. The next command starts it.
    Stop,
    /// Lists a ref's own commits, the latest first; their sequences read the past as `"main@1234"`.
    Log(Log),
    /// When Iceberg clients find each table's metadata written: when one loads it, or after each commit.
    Iceberg {
        #[command(subcommand)]
        verb: IcebergVerb,
    },
}

#[derive(Debug, Subcommand)]
pub enum IcebergVerb {
    /// Has the Iceberg service write these tables' metadata after each of their commits, in one commit.
    OnCommit(IcebergTables),
    /// Has it written when a client loads them, as every table has unless set.
    OnLoad(IcebergTables),
    /// Lists the tables set on-commit on a ref, and how far their metadata is behind.
    Status {
        /// The ref.
        #[arg(long = "ref", value_name = "REF", default_value = "main")]
        on: String,
    },
}

#[derive(Debug, Args)]
pub struct IcebergTables {
    /// The tables, each `namespace.table`.
    #[arg(required = true, value_name = "TABLE")]
    pub tables: Vec<String>,
    /// The ref.
    #[arg(long = "ref", value_name = "REF", default_value = "main")]
    pub on: String,
}

#[derive(Debug, Args)]
pub struct Retain {
    /// The ref the criteria are for.
    #[arg(value_name = "REF")]
    pub on: Option<String>,

    /// Keeps what was committed within this long: `30d`, `12h`, `15m`, `45s`.
    #[arg(long, value_name = "AGE", value_parser = duration)]
    pub max_age: Option<Duration>,

    /// Keeps at least this many of the latest snapshots.
    #[arg(long, value_name = "N")]
    pub min_snapshots: Option<i32>,

    /// Keeps everything from this sequence on.
    #[arg(long, value_name = "SEQ")]
    pub min_seq: Option<i64>,

    /// For the whole lake, how long an expired object waits before it is deleted, which reads should end within.
    #[arg(long, value_name = "AGE", value_parser = duration)]
    pub grace: Option<Duration>,
}

fn duration(text: &str) -> Result<Duration, String> {
    metalake_client::duration(text).map_err(|e| e.to_string())
}

#[derive(Debug, Args)]
pub struct Maintain {
    /// One task; by default all of them, in this order.
    #[arg(value_enum)]
    pub task: Option<Task>,

    /// Only this branch, for `flush` and `compact`.
    #[arg(long = "ref", value_name = "BRANCH")]
    pub on: Option<String>,

    /// Only this table, `namespace.table`, for `flush` and `compact`.
    #[arg(long, value_name = "TABLE")]
    pub table: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Task {
    /// Copies commit times before Postgres discards them.
    Times,
    /// Moves inline rows that are due to storage.
    Flush,
    /// Rewrites small files and those with deleted rows.
    Compact,
    /// Keeps in the catalog the footers of files other engines wrote, for reads to plan with.
    Footers,
    /// Takes away the history no ref keeps, and queues its objects.
    Expire,
    /// Deletes queued objects past the grace.
    Sweep,
    /// Deletes the objects of writes that never committed.
    Orphans,
}

#[derive(Debug, Args)]
pub struct Log {
    /// The ref.
    #[arg(default_value = "main", value_name = "REF")]
    pub on: String,

    /// How many.
    #[arg(long, default_value_t = 20)]
    pub limit: u32,

    /// Lists the snapshots of this table, `namespace.table`, instead.
    #[arg(long, value_name = "TABLE")]
    pub table: Option<String>,
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
    /// A directory, for a lake with a Postgres of its own, or a Postgres that exists, with no password:
    /// `postgresql://user@host/db`, whose password is `PGPASSWORD`.
    pub catalog: String,

    /// Where tables put their files: a URL, such as `s3://bucket/prefix`, or a local directory. In a lake with a Postgres
    /// of its own, its `data` directory by default.
    pub warehouse: Option<String>,

    /// The Postgres schema of the catalog, for more than one lake in a database.
    #[arg(long)]
    pub schema: Option<String>,
}
