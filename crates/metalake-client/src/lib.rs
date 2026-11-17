//! What MetaLake's faces share (ADR 0003 §4). A face turns what it is given into calls and their answers into its own
//! form, and decides nothing: which lake is meant and what each verb does live here, once.

mod catalogs_sql;
mod details;
mod engine;
mod error;
mod follow;
mod hash;
mod history;
mod iceberg;
mod ingest;
mod lake;
mod maintain;
mod merge;
mod pipeline;
mod refs;
mod refs_sql;
pub mod server;
mod session;
mod status;
mod words;
mod write;

/// The ref a lake starts on, and a session reads and writes unless told another.
pub const MAIN: &str = "main";

pub use details::{Column, RefDetails, SchemaDetails, SchemaSummary, Size, TableDetails, TableSize, TableSummary};
pub use engine::{
    CATALOG, Checked, EngineConfig, EngineKind, Engines, LOCAL, LocalFailure, LocalRunner, OWN, Remote, Spark, Trino, Using,
    UsingOf, check_engine_name, engine_rows, how_it_ended, set_local_runner, using_of,
};
pub use error::{Error, Kind, Result};
pub use follow::{Changes, Follower, StartAt};
pub use history::duration;
pub use iceberg::{
    Artifact, Engine, FLINK, HADOOP, ICEBERG_JAVA, Iceberg, IcebergTable, Profile, SCALA, SPARK, Store, Target, profile,
};
pub use ingest::{Mode, Writer, WriterOptions, Wrote};
pub use lake::{Lake, LakeFile};
pub use maintain::{Done, MaintainRequest, MaintainRun, Task};
pub use merge::{MergeOutcome, MergeRequest};
pub use metalake::{Auth, CommitEntry, Mark, Retention, SnapshotEntry, TableName, Watch};
pub use pipeline::{
    ARROYO, ARROYO_IMAGE, Names, Pipeline, PipelineOptions, PipelineState, Verb, names_of, rows as pipeline_rows,
    state_name, verb_of,
};
pub use refs::{RefInfo, RefKind, table};
pub use server::{Runtime, SHUTDOWN_TOKEN, VERSION};
pub use session::{Prepared, Session, SourcedStep, Step};
pub use status::{Named, Status};
pub use write::{Details, TablePath, Written};
