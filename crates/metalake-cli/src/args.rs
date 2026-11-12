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
    /// Prints the rows the commits of a table in a range inserted and deleted, with `_change_type`, `_commit_seq` and
    /// `_commit_timestamp`. Ranges chain: after `a` up to `b`, then after `b`, take each commit once.
    Changes(Changes),
    /// Prints a table's changes as they come, commit after commit, until stopped: Ctrl-C, which says the position to go
    /// on after. With a position file, it goes on where it was.
    Follow(Follow),
    /// Writes rows from stdin into a table as they come, committing on a cadence. With a key, a run given the same input
    /// again goes on where the last one left it, and no row is written twice.
    Ingest(Ingest),
    /// A continuous pipeline: a query, `INSERT INTO t SELECT … FROM s`, that Arroyo keeps running over each commit of
    /// what it reads. As in SQL: `CREATE PIPELINE`, `SHOW PIPELINES`, `ALTER PIPELINE … SUSPEND`.
    Pipeline {
        #[command(subcommand)]
        verb: PipelineVerb,
    },
}

#[derive(Debug, Subcommand)]
pub enum PipelineVerb {
    /// Starts one: its tables are checked, its name taken, and it runs until suspended or dropped.
    Create(PipelineCreate),
    /// Lists every pipeline, and where each is.
    List,
    /// One pipeline: its query, where it is, and, if it failed, its log's last lines.
    Describe(PipelineName),
    /// Stops it with a checkpoint, to resume from it.
    Suspend(PipelineName),
    /// Runs it again from its last checkpoint: what its sources committed meanwhile comes then.
    Resume(PipelineName),
    /// Stops it and forgets it. What it wrote stays in its tables.
    Drop(PipelineName),
}

#[derive(Debug, Args)]
pub struct PipelineName {
    #[arg(value_name = "NAME")]
    pub name: String,
}

#[derive(Debug, Args)]
pub struct PipelineCreate {
    #[arg(value_name = "NAME")]
    pub name: String,
    /// Its query: `INSERT INTO t SELECT … FROM s`.
    #[arg(long, value_name = "SQL", conflicts_with = "file", required_unless_present = "file")]
    pub query: Option<String>,
    /// Its query, from a file.
    #[arg(long, value_name = "FILE")]
    pub file: Option<PathBuf>,
    /// The subtasks it runs in.
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub parallelism: u32,
    /// Empties the tables it writes first, when they have rows: they then hold what the query gives.
    #[arg(long)]
    pub empty_target: bool,
}

#[derive(Debug, Args)]
pub struct Changes {
    /// The table: `t`, `namespace.t` or `ref.namespace.t`.
    #[arg(value_name = "TABLE")]
    pub table: String,

    /// The commit the range starts after: a `_commit_seq`, or a position `follow` printed.
    #[arg(long, value_name = "SEQ")]
    pub after: i64,

    /// The last commit of the range; by default the last one a second old.
    #[arg(long, value_name = "SEQ")]
    pub until: Option<i64>,
}

#[derive(Debug, Args)]
pub struct Follow {
    /// The table: `t`, `namespace.t` or `ref.namespace.t`.
    #[arg(value_name = "TABLE")]
    pub table: String,

    /// Starts after this commit: a position it printed. By default, with what the table holds, then what comes.
    #[arg(long, value_name = "SEQ", conflicts_with_all = ["latest", "since"])]
    pub after: Option<i64>,

    /// Starts with what comes from now.
    #[arg(long, conflicts_with = "since")]
    pub latest: bool,

    /// Starts with what comes after this time, in RFC 3339.
    #[arg(long, value_name = "TIME")]
    pub since: Option<String>,

    /// Where its position is kept: read at the start when the file is there, and written after each step is printed.
    #[arg(long, value_name = "FILE")]
    pub position_file: Option<PathBuf>,

    /// How often it asks for new commits.
    #[arg(long, value_name = "DURATION", value_parser = interval, default_value = "1s")]
    pub interval: Duration,
}

#[derive(Debug, Args)]
pub struct Ingest {
    /// The table: `t`, `namespace.t` or `ref.namespace.t`.
    #[arg(value_name = "TABLE")]
    pub table: String,

    /// How the rows come: one JSON object per line, or CSV with a header. Columns go by name.
    #[arg(long, value_enum, default_value = "json")]
    pub input: Input,

    /// The producer's idempotency key: each commit applies once, and the key says where a run goes on from.
    #[arg(long, value_name = "ID")]
    pub app_id: Option<String>,

    /// The column whose value is a row's version, growing from row to row; by default its line's number.
    #[arg(long, value_name = "COLUMN", requires = "app_id")]
    pub version_column: Option<String>,

    /// Replaces the rows with these columns' values, and adds the others.
    #[arg(long, value_name = "COLUMNS", value_delimiter = ',')]
    pub upsert_on: Vec<String>,

    /// The longest rows wait for their commit.
    #[arg(long, value_name = "DURATION", value_parser = interval, default_value = "1s")]
    pub commit_every: Duration,

    /// What it holds before it commits, in bytes.
    #[arg(long, value_name = "BYTES", default_value_t = 64 << 20)]
    pub max_bytes: usize,

    /// How many lines make one batch.
    #[arg(long, value_name = "LINES", default_value_t = 1024)]
    pub batch_lines: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Input {
    Json,
    Csv,
}

/// A duration of whole units (`1s`, `5m`), or of milliseconds (`500ms`).
fn interval(text: &str) -> Result<Duration, String> {
    match text.strip_suffix("ms") {
        Some(millis) => millis
            .parse()
            .map(Duration::from_millis)
            .map_err(|_| format!("{text}: a number of milliseconds, as 500ms")),
        None => duration(text),
    }
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
    /// Runs the lake's Iceberg REST server in the foreground until Ctrl-C, for engines elsewhere: Spark, Trino, Flink,
    /// DuckDB, PyIceberg. A client starts one of its own when it connects an engine.
    Serve(IcebergServe),
    /// Stops the lake's server on this machine, the one a client started; `metalake stop` stops it too.
    Stop,
    /// Prints what an engine needs to connect to the lake, in the engine's own form: PyIceberg's YAML, DuckDB's and
    /// Flink's statements, Spark's defaults, Trino's catalog file. It starts the lake's server if it does not run, on
    /// the lake's own port, so that what is printed still reaches it after a restart.
    Connect(IcebergConnect),
}

#[derive(Debug, Args)]
pub struct IcebergConnect {
    /// The engine.
    #[arg(value_enum)]
    pub engine: EngineName,
    /// The name the lake's catalog takes in the engine, as `lake.db.t`.
    #[arg(long, default_value = "lake")]
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EngineName {
    Pyiceberg,
    Duckdb,
    Spark,
    Flink,
    Trino,
}

impl From<EngineName> for metalake_client::Engine {
    fn from(name: EngineName) -> Self {
        match name {
            EngineName::Pyiceberg => Self::PyIceberg,
            EngineName::Duckdb => Self::DuckDb,
            EngineName::Spark => Self::Spark,
            EngineName::Flink => Self::Flink,
            EngineName::Trino => Self::Trino,
        }
    }
}

#[derive(Debug, Args)]
pub struct IcebergServe {
    /// Where to listen; port 0 takes a free one, which the first line says.
    #[arg(long, value_name = "ADDRESS", default_value = "127.0.0.1:8181")]
    pub listen: String,
    /// Whether this process writes the metadata of the tables set on-commit after each of their commits, when it leads
    /// the lake's servers; one leads, whatever their number.
    #[arg(long, value_enum, default_value_t = Switch::On)]
    pub materialize: Switch,
    /// The file it writes once it listens and removes when it stops, which is how a client finds it. It takes its
    /// shutdown token from `METALAKE_ICEBERG_TOKEN`.
    #[arg(long, value_name = "FILE", hide = true)]
    pub runtime_file: Option<PathBuf>,
    /// If the port is taken, listens on a free one: how a client starts a lake's server on its preferred port.
    #[arg(long, hide = true)]
    pub or_any_port: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Switch {
    On,
    Off,
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

    /// How a client signs in to an existing Postgres: `aws-iam`, `gcp-iam` or `azure-entra`, a token of the client's own
    /// cloud identity, which the lake's file keeps; `PGPASSWORD` when not given. A token goes only to a server whose
    /// certificate is checked: `sslmode=verify-full` unless the connection string names `verify-ca`.
    #[arg(long, value_name = "AUTH", value_parser = ["password", "aws-iam", "gcp-iam", "azure-entra"])]
    pub auth: Option<String>,
}
