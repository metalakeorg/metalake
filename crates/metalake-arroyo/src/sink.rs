//! The sink: one per subtask, in Arroyo's worker. Each writes its rows into files through the core's writer, which
//! registers each file's intent before its upload (CMT-10). At a checkpoint each keeps the files it prepared; once the
//! checkpoint is complete, subtask 0 commits every subtask's files in one transaction, keyed by the job and the epoch,
//! so that a commit replayed after a failure applies nothing twice (CMT-8). Arroyo gives it Debezium's envelope, as to
//! any updating sink; it inserts the rows inserted, and refuses deletes until the core can apply them by value.

use std::collections::HashMap;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum, TaskCheckpointEventType};
use arroyo_rpc::{CheckpointEvent, ControlResp};
use arroyo_types::{CheckpointBarrier, TaskInfo};
use async_trait::async_trait;
use metalake::{FileAdd, TableWriter, WriteOptions};
use prost::Message;
use tracing::{debug, info};

use crate::changelog::{ChangelogError, changes, declared};
use crate::config::{MetaLakeProfile, MetaLakeSink, MetaLakeTable, MetaLakeTableType};
use crate::conform::{ConformError, conform};
use crate::epoch::SinkEpoch;
use crate::lake::{Lake, core, internal, user};
use crate::schema::check_sink_columns;

/// The pre-commit table: each subtask's files of an epoch.
const PRE_COMMIT: &str = "p";

pub struct MetaLakeSinkFunc {
    profile: MetaLakeProfile,
    table: MetaLakeTable,
    sink: MetaLakeSink,
    lake: Option<Lake>,
    /// This epoch's files, from its first batch to its checkpoint.
    writer: Option<TableWriter>,
    /// Whether this run's first commit, which may replay the last checkpoint's, has restored its files.
    restored: bool,
}

impl MetaLakeSinkFunc {
    pub fn new(profile: MetaLakeProfile, table: MetaLakeTable) -> anyhow::Result<Self> {
        let MetaLakeTableType::Sink(sink) = table.table_type.clone() else {
            anyhow::bail!("{} is not a sink", table.qualified_name());
        };
        sink.validate()?;
        Ok(Self {
            profile,
            table,
            sink,
            lake: None,
            writer: None,
            restored: false,
        })
    }

    /// This subtask's files of the epoch a checkpoint ends: its writer's, closed, which the next batch replaces. Arroyo
    /// keeps them as pre-commit data, and hands every subtask's to the commit.
    pub async fn prepare(&mut self) -> DataflowResult<SinkEpoch> {
        let files = match self.writer.take() {
            Some(writer) => writer
                .finish()
                .await
                .map_err(|e| core(&self.table.qualified_name(), e))?,
            None => Vec::new(),
        };
        Ok(SinkEpoch::new(files))
    }

    /// Commits every subtask's files of `epoch`, once. The first commit of a run may replay the last checkpoint's, after
    /// a failure, long after its files were written: their intents are renewed first, and if that version committed
    /// already, nothing is (CMT-10).
    async fn commit(
        &mut self,
        epoch: u32,
        prepared: Option<&HashMap<u32, Vec<u8>>>,
        task: &TaskInfo,
    ) -> DataflowResult<()> {
        let name = format!("{} epoch {epoch}", self.table.qualified_name());
        let mut by_subtask: Vec<_> = prepared.into_iter().flatten().collect();
        by_subtask.sort_by_key(|(subtask, _)| **subtask);
        let mut files: Vec<FileAdd> = Vec::new();
        for (subtask, bytes) in by_subtask {
            let epoch = SinkEpoch::from_bytes(bytes)
                .map_err(|e| internal(&name, format!("the files of subtask {subtask} are {e}")))?;
            files.extend(epoch.files);
        }

        let lake = self.lake.as_ref().expect("opened on start");
        let app_id = format!("arroyo:{}:{}", task.job_id, task.operator_id);
        let version = i64::from(epoch);
        if !self.restored && !files.is_empty() {
            lake.branch
                .restore_prepared(&app_id, version, &files, &lake.storage, self.sink.intent_ttl())
                .await
                .map_err(|e| core(&name, e))?;
        }
        self.restored = true;
        if files.is_empty() {
            debug!(table = name, "nothing to commit");
            return Ok(());
        }

        let count = files.len();
        let outcome = lake
            .branch
            .transaction()
            .append(files)
            .idempotent(&app_id, version)
            .author("arroyo")
            .message(format!("job {}, epoch {epoch}", task.job_id))
            .commit()
            .await
            .map_err(|e| core(&name, e))?;
        info!(table = name, files = count, ?outcome, "committed");
        Ok(())
    }
}

#[async_trait]
impl ArrowOperator for MetaLakeSinkFunc {
    fn name(&self) -> String {
        format!("metalake-sink-{}", self.table.qualified_name())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let config = GlobalKeyedTableConfig {
            table_name: PRE_COMMIT.to_string(),
            description: "files to commit".to_string(),
            uses_two_phase_commit: true,
        };
        HashMap::from([(
            PRE_COMMIT.to_string(),
            TableConfig {
                table_type: TableEnum::GlobalKeyValue.into(),
                config: config.encode_to_vec(),
                state_version: 1,
            },
        )])
    }

    fn is_committing(&self) -> bool {
        true
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let name = self.table.qualified_name();
        let lake = Lake::open(&self.profile, &self.table).await?;
        let input = &ctx.in_schemas.first().expect("a sink has an input").schema;
        let input = Schema::new(declared(input).map_err(|e| internal(&name, e))?);
        check_sink_columns(&input, lake.table.schema()).map_err(|e| user(&name, e))?;
        info!(
            table = name,
            subtask = ctx.task_info.task_index,
            "writing a MetaLake table"
        );
        self.lake = Some(lake);
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let name = self.table.qualified_name();
        let lake = self.lake.as_ref().expect("opened on start");
        let changes = changes(&batch).map_err(|e| changelog_error(&name, e))?;
        if changes.deletes.num_rows() > 0 {
            return Err(user(
                &name,
                format!(
                    "{} rows deleted or updated, and a MetaLake sink takes only inserts for now",
                    changes.deletes.num_rows()
                ),
            ));
        }
        if changes.inserts.num_rows() == 0 {
            return Ok(());
        }
        let rows = conform(&changes.inserts, lake.table.schema()).map_err(|e| conform_error(&name, e))?;
        if self.writer.is_none() {
            let options = WriteOptions {
                intent_ttl: self.sink.intent_ttl(),
                ..Default::default()
            };
            let writer = TableWriter::new(&lake.catalog, &lake.table, lake.storage.clone(), options);
            self.writer = Some(writer.map_err(|e| core(&name, e))?);
        }
        let writer = self.writer.as_mut().expect("opened above");
        writer.write(&rows).await.map_err(|e| core(&name, e))
    }

    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let prepared = self.prepare().await?;
        debug!(
            table = self.table.qualified_name(),
            epoch = barrier.epoch,
            files = prepared.files.len(),
            "prepared"
        );
        ctx.table_manager
            .insert_committing_data(PRE_COMMIT, prepared.to_bytes())
            .await;
        Ok(())
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        if ctx.task_info.task_index == 0 {
            let task = ctx.task_info.clone();
            self.commit(epoch, commit_data.get(PRE_COMMIT), &task).await?;
        }
        // the controller waits for every subtask's, not only the one that commits
        let event = CheckpointEvent {
            checkpoint_epoch: epoch as u64,
            operator_idx: ctx.task_info.operator_idx,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_idx: ctx.task_info.task_index,
            time: SystemTime::now(),
            event_type: TaskCheckpointEventType::FinishedCommit,
        };
        ctx.control_tx
            .send(ControlResp::CheckpointEvent(event))
            .await
            .map_err(|_| internal(&self.table.qualified_name(), "the controller is gone"))
    }
}

fn changelog_error(table: &str, e: ChangelogError) -> DataflowError {
    match e {
        ChangelogError::UnknownChange(_) => user(table, e),
        ChangelogError::Internal(_) => internal(table, e),
    }
}

fn conform_error(table: &str, e: ConformError) -> DataflowError {
    if e.is_data() {
        user(table, e)
    } else {
        internal(table, e)
    }
}
