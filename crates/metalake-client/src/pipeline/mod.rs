//! Continuous pipelines: a query of standard SQL over the lake's names, `INSERT INTO t SELECT … FROM s`, kept running by
//! Arroyo, which reads each commit of what it reads and commits what it writes. A client gives the name and the query;
//! where the catalog is, the tables' connections and the pipeline's state are the lake's, found as every other face finds
//! them.
//!
//! A pipeline is kept in the lake (its record and its checkpoints), and runs on the machine that started it. Suspended,
//! it stops with a checkpoint; resumed, it goes on from it. A new pipeline starts from what its sources hold, so a table
//! it writes must have no rows yet, or be emptied first with `empty_target`: what it holds is then what the query gives.

mod record;
mod runner;
pub(crate) mod sql;
mod statement;

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use metalake::environment;
use url::Url;

pub use runner::{ARROYO, ARROYO_IMAGE};
pub use sql::{Verb, rows, state_name, verb_of};
pub use statement::{Names, names_of};

use crate::pipeline::record::{Desired, Handle, Record, Records, check_name};
use crate::pipeline::runner::{Launch, Runner};
use crate::{Details, Error, Lake, Result, Session, TablePath};

/// How long a pipeline that just started has to show it runs.
const SETTLING: Duration = Duration::from_secs(6);

/// How a new pipeline runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineOptions {
    pub parallelism: u32,
    /// Empties the tables it writes first, when they have rows: what they hold is then what the query gives.
    pub empty_target: bool,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            parallelism: 1,
            empty_target: false,
        }
    }
}

/// Where a pipeline is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineState {
    Running,
    /// Stopped with a checkpoint, to resume from it.
    Suspended,
    /// Asked to run, and not running: why is in its log's last lines. Resuming starts it again from its checkpoint.
    Failed {
        log: String,
    },
    /// Running on another machine, which alone can stop it.
    Elsewhere {
        host: String,
    },
}

/// A pipeline, as it is now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    pub name: String,
    pub query: String,
    pub parallelism: u32,
    pub state: PipelineState,
    pub reads: Vec<TablePath>,
    pub writes: Vec<TablePath>,
    /// Each table it writes, with the last commit it holds.
    pub reached: Vec<(TablePath, Option<i64>)>,
    pub created_at: SystemTime,
}

impl Lake {
    fn records(&self) -> Result<Records> {
        Ok(Records::new(self.storage()?))
    }

    /// Starts a pipeline: its tables are checked, its name taken, and it runs until suspended or dropped.
    pub async fn create_pipeline(&self, name: &str, query: &str, options: PipelineOptions) -> Result<Pipeline> {
        check_name(name)?;
        if options.parallelism == 0 {
            return Err(Error::usage("a pipeline runs with a parallelism of 1 or more"));
        }
        let names = names_of(query)?;
        for path in names.reads.iter().chain(&names.writes) {
            self.table(path).await?;
        }
        if !options.empty_target {
            // By its rows: creating a table is already a commit.
            let session = Session::new(self).await?;
            for path in &names.writes {
                if session.has_rows(&format!("select 1 from {}", path.quoted())).await? {
                    return Err(Error::usage(format!(
                        "{path} has rows: a new pipeline starts from what its sources hold, so what it writes into \
                         starts empty; `empty_target` empties it first"
                    )));
                }
            }
        }
        let records = self.records()?;
        let strings = |paths: &[TablePath]| paths.iter().map(ToString::to_string).collect();
        let mut record = Record::new(
            name,
            query,
            options.parallelism,
            strings(&names.reads),
            strings(&names.writes),
        );
        records.create(&record).await?;
        let started = async {
            if options.empty_target {
                let session = Session::new(self).await?;
                for path in &names.writes {
                    let details = Details {
                        message: Some(format!("emptied for the pipeline {name}")),
                        ..Details::default()
                    };
                    session.delete(path, "true", details).await?;
                }
            }
            record.runner = Some(self.launch(&records, &record)?);
            records.put(&record).await
        };
        if let Err(e) = started.await {
            // A pipeline that never ran leaves its name free.
            let _ = records.delete(name).await;
            return Err(e);
        }
        self.settle(&records, record).await
    }

    /// A pipeline, as it is now.
    pub async fn pipeline(&self, name: &str) -> Result<Pipeline> {
        let records = self.records()?;
        let record = records.get(name).await?;
        self.described(&records, record).await
    }

    /// Every pipeline of the lake, by name.
    pub async fn pipelines(&self) -> Result<Vec<Pipeline>> {
        let records = self.records()?;
        let mut pipelines = Vec::new();
        for record in records.list().await? {
            pipelines.push(self.described(&records, record).await?);
        }
        Ok(pipelines)
    }

    /// Stops it with a checkpoint, to resume from it.
    pub async fn suspend_pipeline(&self, name: &str) -> Result<Pipeline> {
        let records = self.records()?;
        let mut record = records.get(name).await?;
        if let Some(handle) = &record.runner {
            here(handle, name)?;
            stopped(handle.clone()).await?;
        }
        record.desired = Desired::Suspended;
        records.put(&record).await?;
        self.described(&records, record).await
    }

    /// Runs it again, from its last checkpoint: what its sources committed meanwhile comes then.
    pub async fn resume_pipeline(&self, name: &str) -> Result<Pipeline> {
        let records = self.records()?;
        let mut record = records.get(name).await?;
        if let Some(handle) = &record.runner {
            here(handle, name)?;
            if handle.alive() {
                record.desired = Desired::Running;
                records.put(&record).await?;
                return self.described(&records, record).await;
            }
            handle.remove();
        }
        record.runner = Some(self.launch(&records, &record)?);
        record.desired = Desired::Running;
        records.put(&record).await?;
        self.settle(&records, record).await
    }

    /// Stops it and forgets it: its record and its state. What it wrote stays in its tables.
    pub async fn drop_pipeline(&self, name: &str) -> Result<()> {
        let records = self.records()?;
        let record = records.get(name).await?;
        if let Some(handle) = &record.runner {
            here(handle, name)?;
            stopped(handle.clone()).await?;
            handle.remove();
        }
        records.delete(name).await?;
        if let Ok(home) = runner::home(&records.state(name)) {
            let _ = std::fs::remove_dir_all(home);
        }
        Ok(())
    }

    /// Starts the pipeline's runner, with what it needs from this client.
    fn launch(&self, records: &Records, record: &Record) -> Result<Handle> {
        let state = records.state(&record.name);
        let home = runner::home(&state)?;
        let warehouse = self.storage()?.base().to_string();
        let launch = Launch {
            name: &record.name,
            query: &record.query,
            parallelism: record.parallelism,
            state: &state,
            env: self.pipeline_environment()?,
            home: &home,
            warehouse: Url::parse(&warehouse)
                .ok()
                .filter(|u| u.scheme() == "file")
                .and_then(|u| u.to_file_path().ok()),
        };
        Runner::find()?.start(&launch)
    }

    /// What a pipeline's process needs to find the lake as this client does: the catalog, its schema, the storage's
    /// credentials.
    fn pipeline_environment(&self) -> Result<Vec<(String, String)>> {
        let mut variables = vec![
            (environment::POSTGRES.to_string(), self.catalog_postgres()?),
            (environment::SCHEMA.to_string(), self.schema().to_string()),
        ];
        variables.extend(env::vars().filter(|(name, _)| environment::is_storage_variable(name)));
        Ok(variables)
    }

    /// A pipeline that just started, once it showed it runs or that it does not.
    async fn settle(&self, records: &Records, record: Record) -> Result<Pipeline> {
        if let Some(handle) = &record.runner {
            let since = Instant::now();
            while since.elapsed() < SETTLING && handle.alive() {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        self.described(records, record).await
    }

    async fn described(&self, records: &Records, record: Record) -> Result<Pipeline> {
        let state = match (&record.desired, &record.runner) {
            (Desired::Suspended, _) => PipelineState::Suspended,
            (Desired::Running, Some(handle)) if !handle.here() => PipelineState::Elsewhere {
                host: handle.host.clone(),
            },
            (Desired::Running, Some(handle)) if handle.alive() => PipelineState::Running,
            (Desired::Running, Some(handle)) => PipelineState::Failed {
                log: handle.log(&runner::home(&records.state(&record.name))?),
            },
            (Desired::Running, None) => PipelineState::Failed {
                log: "it never started".to_string(),
            },
        };
        let parse = |names: &[String]| {
            names
                .iter()
                .map(|n| TablePath::parse(n))
                .collect::<std::result::Result<Vec<_>, _>>()
        };
        let reads = parse(&record.reads)?;
        let writes = parse(&record.writes)?;
        let mut reached = Vec::with_capacity(writes.len());
        for path in &writes {
            let last = match self.table(path).await {
                Ok((_, table)) => table.snapshot_id(),
                Err(_) => None,
            };
            reached.push((path.clone(), last));
        }
        Ok(Pipeline {
            name: record.name,
            query: record.query,
            parallelism: record.parallelism,
            state,
            reads,
            writes,
            reached,
            created_at: UNIX_EPOCH + Duration::from_secs(record.created_at),
        })
    }
}

/// A pipeline is stopped and started only on the machine it runs on.
fn here(handle: &Handle, name: &str) -> Result<()> {
    if handle.here() {
        return Ok(());
    }
    Err(Error::usage(format!(
        "the pipeline {name} runs on {}: it is stopped and started there",
        handle.host
    )))
}

/// Stops it, off the runtime's threads: a final checkpoint takes its time.
async fn stopped(handle: Handle) -> Result<()> {
    if !handle.alive() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || handle.stop())
        .await
        .map_err(|e| Error::failed(e.to_string()))?
}
