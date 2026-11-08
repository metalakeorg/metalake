//! The sink: one per subtask, in Arroyo's worker. Each writes its rows into files through the core's writer, which
//! registers each file's intent before its upload (CMT-10). At a checkpoint each keeps the files it prepared; once the
//! checkpoint is complete, subtask 0 commits every subtask's files in one transaction, keyed by the job and the epoch,
//! so that a commit replayed after a failure applies nothing twice (CMT-8). Arroyo gives it Debezium's envelope, as to
//! any updating sink. A delete of a row the subtask inserted in the same epoch deletes it where the writer put it, in
//! the commit that adds its file (DV-4); a delete of any other row goes with the epoch, and the commit finds it in the
//! table, as it reads it, and deletes it there.

use std::collections::HashMap;
use std::time::SystemTime;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::Schema;
use arrow_select::take::take_record_batch;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum, TaskCheckpointEventType};
use arroyo_rpc::{CheckpointEvent, ControlResp};
use arroyo_types::{CheckpointBarrier, TaskInfo};
use async_trait::async_trait;
use metalake::vectors::Deletes;
use metalake::{FileAdd, RowLocation, TableWriter, WriteOptions};
use prost::Message;
use tracing::{debug, info, warn};

use crate::changelog::{ChangelogError, Run, declared, runs};
use crate::config::{MetaLakeProfile, MetaLakeSink, MetaLakeTable, MetaLakeTableType};
use crate::conform::{ConformError, conform};
use crate::epoch::SinkEpoch;
use crate::inserted::InsertedRows;
use crate::lake::{Lake, core, internal, user};
use crate::locate::{LocateError, locate};
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
    /// The rows this epoch inserted, and where: built on start, emptied at each checkpoint.
    inserted: Option<InsertedRows>,
    /// The rows of this epoch's files that it deleted.
    deleted: Vec<RowLocation>,
    /// The rows it was asked to delete that it did not write, for the commit to find.
    to_find: Vec<RecordBatch>,
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
            inserted: None,
            deleted: Vec::new(),
            to_find: Vec::new(),
            restored: false,
        })
    }

    /// Writes rows inserted, and keeps where each went.
    async fn insert(&mut self, rows: &RecordBatch) -> DataflowResult<()> {
        let name = self.table.qualified_name();
        let lake = self.lake.as_ref().expect("opened on start");
        let conformed = conform(rows, lake.table.schema()).map_err(|e| conform_error(&name, e))?;
        if self.writer.is_none() {
            let options = WriteOptions {
                intent_ttl: self.sink.intent_ttl(),
                ..Default::default()
            };
            let writer = TableWriter::new(&lake.catalog, &lake.table, lake.storage.clone(), options);
            self.writer = Some(writer.map_err(|e| core(&name, e))?);
        }
        let writer = self.writer.as_mut().expect("opened above");
        let located = writer.write_located(&conformed).await.map_err(|e| core(&name, e))?;
        let inserted = self.inserted.as_mut().expect("built on start");
        inserted.insert(rows, &located).map_err(|e| internal(&name, e))
    }

    /// Deletes rows this epoch inserted, where they went; keeps the others for the commit to find.
    fn delete(&mut self, rows: &RecordBatch) -> DataflowResult<()> {
        let name = self.table.qualified_name();
        let inserted = self.inserted.as_mut().expect("built on start");
        let taken = inserted.take(rows).map_err(|e| internal(&name, e))?;
        self.deleted.extend(taken.located);
        if !taken.unmatched.is_empty() {
            let unmatched = UInt32Array::from(taken.unmatched);
            let others = take_record_batch(rows, &unmatched).map_err(|e| internal(&name, e))?;
            self.to_find.push(others);
        }
        Ok(())
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
        if let Some(inserted) = self.inserted.as_mut() {
            inserted.clear();
        }
        SinkEpoch::new(files, &std::mem::take(&mut self.deleted))
            .with_rows_to_find(&std::mem::take(&mut self.to_find))
            .map_err(|e| internal(&self.table.qualified_name(), e))
    }

    /// Commits every subtask's files of `epoch`, and its deletes, once. The first commit of a run may replay the last
    /// checkpoint's, after a failure, long after its files were written: their intents are renewed first, and if that
    /// version committed already, nothing is (CMT-10). Rows to delete that the epoch did not write are found in the
    /// table as the commit reads it; if another commit changes a vector first (DV-4), they are found again.
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
        let mut own = Deletes::new();
        let mut to_find: Vec<RecordBatch> = Vec::new();
        for (subtask, bytes) in by_subtask {
            let epoch = SinkEpoch::from_bytes(bytes)
                .map_err(|e| internal(&name, format!("the files of subtask {subtask} are {e}")))?;
            for at in epoch.deleted() {
                own.insert(at.file_id, at.position);
            }
            let rows = epoch
                .rows_to_find()
                .map_err(|e| internal(&name, format!("subtask {subtask}: {e}")))?;
            to_find.extend(rows);
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

        let mut attempt = 0;
        loop {
            attempt += 1;
            let table = lake.latest_table().await.map_err(|e| core(&name, e))?;
            let found = locate(&lake.catalog, &table, &lake.storage, &to_find, &self.sink.key)
                .await
                .map_err(|e| locate_error(&name, e))?;
            if found.unmatched > 0 {
                warn!(table = name, rows = found.unmatched, "rows to delete not in the table");
            }
            let mut deletes = own.clone();
            for (file_id, vector) in found.deletes.files() {
                for position in vector.positions() {
                    deletes.insert(*file_id, position);
                }
            }
            if files.is_empty() && deletes.is_empty() {
                debug!(table = name, "nothing to commit");
                return Ok(());
            }
            let (count, deleted) = (files.len(), cardinality(&deletes));
            let outcome = lake
                .branch
                .transaction()
                .append(files.clone())
                .delete(&table, &lake.storage, deletes)
                .idempotent(&app_id, version)
                .author("arroyo")
                .message(format!("job {}, epoch {epoch}", task.job_id))
                .commit()
                .await;
            match outcome {
                Ok(outcome) => {
                    info!(table = name, files = count, deleted, ?outcome, "committed");
                    return Ok(());
                }
                Err(e) if attempt < ATTEMPTS && vector_changed(&e) => {
                    debug!(
                        table = name,
                        attempt, "a vector changed under the commit: finding the rows again"
                    );
                }
                Err(e) => return Err(core(&name, e)),
            }
        }
    }
}

/// Commits a sink tries, finding its rows again each time another commit changed a vector under it.
const ATTEMPTS: u32 = 3;

fn cardinality(deletes: &Deletes) -> u64 {
    deletes.files().values().map(|v| v.cardinality()).sum()
}

/// Whether a commit was refused because a vector it replaces changed (DV-4): the rows are found again.
fn vector_changed(e: &metalake::Error) -> bool {
    use metalake_pg::ErrorKind;
    matches!(e, metalake::Error::Protocol(p) if p.kind() == ErrorKind::Conflict && p.rule() == Some("DV-4"))
}

fn locate_error(table: &str, e: LocateError) -> DataflowError {
    match e {
        LocateError::Core(e) => core(table, e),
        LocateError::Arrow(e) => internal(table, e),
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
        let fields = declared(input).map_err(|e| internal(&name, e))?;
        check_sink_columns(&Schema::new(fields.clone()), lake.table.schema()).map_err(|e| user(&name, e))?;
        self.inserted = Some(InsertedRows::new(&fields, &self.sink.key).map_err(|e| user(&name, e))?);
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
        for run in runs(&batch).map_err(|e| changelog_error(&name, e))? {
            match run {
                Run::Insert(rows) => self.insert(&rows).await?,
                Run::Delete(rows) => self.delete(&rows)?,
            }
        }
        Ok(())
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
