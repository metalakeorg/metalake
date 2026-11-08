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
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, connector_err};
use arroyo_types::{SignalMessage, Watermark};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use metalake::Start;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::config::{MetaLakeProfile, MetaLakeSource, MetaLakeTable, MetaLakeTableType, StartMode};
use crate::envelope::{EnvelopeError, declared, envelope};
use crate::lake::{Lake, core, internal, user};
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

    /// The table on its branch, once its declared columns are checked against the table's.
    async fn open(&self, out: &ArroyoSchema) -> DataflowResult<Lake> {
        let name = self.table.qualified_name();
        let lake = Lake::open(&self.profile, &self.table).await?;
        let declared = declared(out).map_err(|e| envelope_error(&name, e))?;
        check_columns(&Schema::new(declared), lake.table.schema()).map_err(|e| user(&name, e))?;
        Ok(lake)
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
        let Lake {
            branch, table, storage, ..
        } = self.open(&ctx.out_schema.clone()).await?;
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

fn envelope_error(table: &str, e: EnvelopeError) -> DataflowError {
    if e.is_data() {
        user(table, e)
    } else {
        internal(table, e)
    }
}
