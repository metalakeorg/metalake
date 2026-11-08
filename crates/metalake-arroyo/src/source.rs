//! The source: one per subtask, in Arroyo's worker. It follows the table with the core's stream, takes the tasks its
//! subtask reads, emits them through the envelope, and keeps its progress in Arroyo's state at each checkpoint. It
//! decides nothing itself: [`SourceProgress`] says what comes next.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::{DataflowError, DataflowResult, ErrorDomain, RetryHint};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, connector_err};
use arroyo_types::{SignalMessage, Watermark};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use metalake::{Catalog, CatalogConfig, Start, Storage};
use metalake_pg::ErrorKind;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::config::{MetaLakeProfile, MetaLakeSource, MetaLakeTable, MetaLakeTableType, StartMode};
use crate::envelope::{EnvelopeError, declared, envelope};
use crate::progress::{SourceProgress, unseen};
use crate::schema::check_columns;

/// The state table: each subtask's progress, by its index.
const STATE: &str = "m";
/// Asks for new commits that find none, in a row, before a subtask says it's idle.
const IDLE_POLLS: u32 = 3;

pub struct MetaLakeSourceFunc {
    profile: MetaLakeProfile,
    table: MetaLakeTable,
    source: MetaLakeSource,
}

impl MetaLakeSourceFunc {
    pub fn new(profile: MetaLakeProfile, table: MetaLakeTable) -> anyhow::Result<Self> {
        let MetaLakeTableType::Source(source) = table.table_type.clone() else {
            anyhow::bail!("{} is not a source", table.qualified_name());
        };
        Ok(Self { profile, table, source })
    }

    /// What every subtask opens: the catalog, the table on its branch, and the store of its files.
    async fn open(&self, out: &ArroyoSchema) -> DataflowResult<(metalake::Ref, metalake::Table, Storage)> {
        let name = self.table.qualified_name();
        let postgres = self.profile.postgres.sub_env_vars().map_err(|e| user(&name, e))?;
        let mut config = CatalogConfig::new(postgres, &self.profile.schema);
        config.connections = 2;
        let catalog = Catalog::connect(config).await.map_err(|e| core(&name, e))?;
        let branch = catalog.branch(&self.table.branch).await.map_err(|e| core(&name, e))?;
        let namespace: Vec<&str> = self.table.namespace.iter().map(String::as_str).collect();
        let table = branch
            .table(&namespace, &self.table.table)
            .await
            .map_err(|e| core(&name, e))?;

        let declared = declared(out).map_err(|e| envelope_error(&name, e))?;
        check_columns(&Schema::new(declared), table.schema()).map_err(|e| user(&name, e))?;

        let options = self
            .profile
            .storage
            .iter()
            .map(|(k, v)| Ok((k.clone(), v.sub_env_vars()?)))
            .collect::<anyhow::Result<Vec<(String, String)>>>()
            .map_err(|e| user(&name, e))?;
        let storage = Storage::new(table.location(), options).map_err(|e| core(&name, e))?;
        Ok((branch, table, storage))
    }

    /// This subtask's progress as the last checkpoint left it, or a new one.
    async fn restore(&self, ctx: &mut SourceContext) -> DataflowResult<SourceProgress> {
        let name = self.table.qualified_name();
        let (index, parallelism) = (ctx.task_info.task_index, ctx.task_info.parallelism as usize);
        let stored = ctx.table_manager.get_global_keyed_state::<u32, Vec<u8>>(STATE).await?;
        let all = stored.get_all();
        if !all.is_empty() && all.len() != parallelism {
            return Err(connector_err!(
                User,
                NoRetry,
                "{name}: restoring a MetaLake source at {parallelism} subtasks from {} isn't supported yet",
                all.len()
            ));
        }
        match all.get(&index) {
            Some(bytes) => SourceProgress::from_bytes(bytes)
                .map_err(|e| connector_err!(Internal, NoRetry, "{name}: the progress of subtask {index} is {e}")),
            None => Ok(SourceProgress::default()),
        }
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        let name = self.table.qualified_name();
        let (index, parallelism) = (ctx.task_info.task_index as usize, ctx.task_info.parallelism as usize);
        let (branch, table, storage) = self.open(&ctx.out_schema.clone()).await?;
        let mut progress = self.restore(ctx).await?;

        // from its cursor if it took a step; otherwise from the source's time, the same for every subtask (CHG-5)
        let mut stream = match progress.cursor() {
            Some(cursor) => branch.stream(&table, Start::Cursor(cursor.clone())).await,
            None => {
                let from = self.source.from_time().map_err(|e| user(&name, e))?;
                let from =
                    from.ok_or_else(|| connector_err!(Internal, NoRetry, "{name}: planned with no start time"))?;
                let start = match self.source.start {
                    StartMode::Snapshot => Start::Snapshot,
                    StartMode::Latest => Start::Latest,
                };
                branch.as_of(SystemTime::from(from)).stream(&table, start).await
            }
        }
        .map_err(|e| core(&name, e))?;
        info!(
            table = name,
            subtask = index,
            from = self.source.from,
            cursor = ?progress.cursor(),
            pending = progress.current().is_some(),
            "following a MetaLake table"
        );

        let poll_interval = Duration::from_millis(self.source.poll_interval_ms.unwrap_or(1_000));
        let mut poll = tokio::time::interval(poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let (mut batches, mut read) = (None, 0u64);
        let (mut empty_polls, mut idle) = (0u32, false);

        loop {
            if batches.is_none()
                && let Some((task, _)) = progress.current()
            {
                batches = Some(stream.open(task.clone(), &storage));
                read = 0;
            }

            select! {
                biased;
                control = ctx.control_rx.recv() => match control {
                    Some(ControlMessage::Checkpoint(c)) => {
                        let stored = ctx.table_manager.get_global_keyed_state::<u32, Vec<u8>>(STATE).await?;
                        stored.insert(index as u32, progress.to_bytes()).await;
                        if self.start_checkpoint(c, ctx, collector).await {
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                    Some(ControlMessage::Stop { mode }) => {
                        info!(table = name, subtask = index, ?mode, "stopping");
                        return Ok(match mode {
                            StopMode::Graceful => SourceFinishType::Graceful,
                            StopMode::Immediate => SourceFinishType::Immediate,
                        });
                    }
                    Some(ControlMessage::Commit { .. }) => unreachable!("sources take no commits"),
                    Some(ControlMessage::LoadCompacted { compacted }) => ctx.load_compacted(compacted).await,
                    Some(ControlMessage::NoOp) | None => {}
                },
                batch = next_batch(&mut batches) => match batch {
                    Some(batch) => {
                        let batch = batch.map_err(|e| core(&name, e))?;
                        let emitted = progress.current().map_or(0, |(_, e)| e);
                        let rows = unseen(read, batch.num_rows(), emitted);
                        read += batch.num_rows() as u64;
                        if !rows.is_empty() {
                            let part = batch.slice(rows.start, rows.len());
                            let out = envelope(&part, &ctx.out_schema).map_err(|e| envelope_error(&name, e))?;
                            collector.collect(out).await?;
                            progress.emitted(rows.len() as u64);
                            idle = false;
                        }
                    }
                    None => {
                        progress.finished();
                        batches = None;
                    }
                },
                _ = poll.tick(), if batches.is_none() => {
                    if let Some(step) = stream.next().await.map_err(|e| core(&name, e))? {
                        debug!(table = name, subtask = index, tasks = step.tasks.len(), "took a step");
                        progress.take(step, index, parallelism);
                    }
                    empty_polls = if progress.current().is_some() { 0 } else { empty_polls + 1 };
                    if empty_polls >= IDLE_POLLS && !idle {
                        warn!(table = name, subtask = index, "idle: no files of the table's commits are this subtask's");
                        collector.broadcast(SignalMessage::Watermark(Watermark::Idle)).await;
                        idle = true;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SourceOperator for MetaLakeSourceFunc {
    fn name(&self) -> String {
        format!("metalake-{}", self.table.qualified_name())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config(STATE, "metalake progress")
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        self.run_int(ctx, collector).await
    }
}

/// The next batch of the task being read, or never when none is: a `select!` evaluates a disabled branch's future too.
async fn next_batch(
    batches: &mut Option<BoxStream<'static, metalake::Result<RecordBatch>>>,
) -> Option<metalake::Result<RecordBatch>> {
    match batches {
        Some(batches) => batches.next().await,
        None => std::future::pending().await,
    }
}

/// A core error by what to do about it: try again for what may pass, stop for what the user has to change.
fn core(table: &str, e: metalake::Error) -> DataflowError {
    let (domain, retry) = classify(&e);
    DataflowError::ConnectorError {
        domain,
        retry,
        error: format!("{table}: {e}"),
        source: Some(e.into()),
    }
}

fn classify(e: &metalake::Error) -> (ErrorDomain, RetryHint) {
    use metalake::Error;
    match e {
        Error::Protocol(p) => match p.kind() {
            ErrorKind::Conflict | ErrorKind::Serialization | ErrorKind::Unknown | ErrorKind::Postgres => {
                (ErrorDomain::External, RetryHint::WithBackoff)
            }
            ErrorKind::Invalid | ErrorKind::Unsupported | ErrorKind::Permission => {
                (ErrorDomain::User, RetryHint::NoRetry)
            }
            ErrorKind::Corrupt => (ErrorDomain::Internal, RetryHint::NoRetry),
            _ => (ErrorDomain::Internal, RetryHint::NoRetry),
        },
        Error::Storage(_) => (ErrorDomain::External, RetryHint::WithBackoff),
        Error::NotFound(_) | Error::Invalid(_) | Error::Unsupported(_) | Error::Location(_) => {
            (ErrorDomain::User, RetryHint::NoRetry)
        }
        Error::Schema(_) | Error::Corrupt(_) => (ErrorDomain::Internal, RetryHint::NoRetry),
        _ => (ErrorDomain::Internal, RetryHint::NoRetry),
    }
}

fn envelope_error(table: &str, e: EnvelopeError) -> DataflowError {
    if e.is_data() {
        connector_err!(User, NoRetry, "{table}: {e}")
    } else {
        connector_err!(Internal, NoRetry, "{table}: {e}")
    }
}

fn user(table: &str, e: impl std::fmt::Display) -> DataflowError {
    connector_err!(User, NoRetry, "{table}: {e}")
}
