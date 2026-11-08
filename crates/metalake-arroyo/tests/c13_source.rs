//! The source against a real catalog: a table written through the core, read by the source as Arroyo runs it, with
//! its checkpoints and restores. It needs a Postgres with `track_commit_timestamp = on` at `DATABASE_URL`, as the
//! core's tests do (`sql/test.sh`).

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{
    ArrowCollector, BatchReceiver, OperatorContext, SourceCollector, SourceContext, batch_bounded,
};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata, StopMode};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{CheckpointCompleted, ControlMessage, ControlResp, MetadataOrManifest, TIMESTAMP_FIELD};
use arroyo_state::tables::ErasedTable;
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
use arroyo_types::{ArrowMessage, ChainInfo, CheckpointBarrier, TaskInfo, to_micros};
use chrono::{DateTime, SecondsFormat, Utc};
use futures::TryStreamExt;
use metalake::vectors::Deletes;
use metalake::{Catalog, CatalogConfig, NewTable, Ref, Scan, Storage, TableWriter, WriteOptions};
use metalake_arroyo::config::{MetaLakeProfile, MetaLakeSource, MetaLakeTable, MetaLakeTableType, StartMode};
use metalake_arroyo::source::MetaLakeSourceFunc;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinHandle;

fn url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL: a Postgres with track_commit_timestamp = on")
}

fn unique() -> String {
    format!("t{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())
}

/// A catalog in a schema of its own, with `shop.orders (k bigint)`, its files on local disk.
struct Lake {
    catalog: Catalog,
    main: Ref,
    storage: Storage,
    schema: String,
}

impl Lake {
    async fn new() -> Self {
        let schema = unique();
        let dir = std::env::temp_dir().join("metalake-arroyo").join(&schema);
        std::fs::create_dir_all(&dir).unwrap();
        let warehouse = format!("file://{}", dir.display());
        let catalog = Catalog::init(CatalogConfig::new(url(), &schema).warehouse(&warehouse), None)
            .await
            .unwrap();
        let main = catalog.branch("main").await.unwrap();
        let (tx, _) = main
            .transaction()
            .create_namespace(&["shop"])
            .create_table(NewTable {
                namespace: &["shop"],
                name: "orders",
                schema: &keys(0..1).schema(),
                location: None,
                cluster_key: vec![],
                partitioning: vec![],
                properties: vec![],
            })
            .unwrap();
        tx.commit().await.unwrap();
        let storage = Storage::new(&warehouse, Vec::<(String, String)>::new()).unwrap();
        Lake {
            catalog,
            main,
            storage,
            schema,
        }
    }

    /// Commits the rows `keys`, in a file of their own.
    async fn append(&self, keys: Range<i64>) -> i64 {
        let t = self.main.table(&["shop"], "orders").await.unwrap();
        let options = WriteOptions {
            stored: true,
            ..Default::default()
        };
        let mut writer = TableWriter::new(&self.catalog, &t, self.storage.clone(), options).unwrap();
        writer.write(&self::keys(keys)).await.unwrap();
        let files = writer.finish().await.unwrap();
        self.main.transaction().append(files).commit().await.unwrap().seq()
    }

    /// Deletes the rows whose key `chosen` keeps, as an engine does.
    async fn delete(&self, chosen: impl Fn(i64) -> bool) -> i64 {
        let t = self.main.table(&["shop"], "orders").await.unwrap();
        let scan = Scan::new(&self.catalog, &t).with_positions();
        let batches: Vec<RecordBatch> = scan
            .read(scan.plan().await.unwrap(), &self.storage)
            .try_collect()
            .await
            .unwrap();
        let mut deletes = Deletes::new();
        for b in &batches {
            let keys = b.column(0).as_primitive::<Int64Type>();
            let keep = BooleanArray::from_iter(keys.iter().map(|k| Some(chosen(k.unwrap()))));
            deletes
                .add(&arrow_select::filter::filter_record_batch(b, &keep).unwrap())
                .unwrap();
        }
        self.main
            .transaction()
            .delete(&t, &self.storage, deletes)
            .commit()
            .await
            .unwrap()
            .seq()
    }

    fn source(&self, from: SystemTime) -> MetaLakeSourceFunc {
        let profile = MetaLakeProfile {
            postgres: VarStr::new(url()),
            schema: self.schema.clone(),
            storage: BTreeMap::new(),
        };
        let from: DateTime<Utc> = from.into();
        let table = MetaLakeTable {
            branch: "main".to_string(),
            namespace: vec!["shop".to_string()],
            table: "orders".to_string(),
            table_type: MetaLakeTableType::Source(MetaLakeSource {
                start: StartMode::Snapshot,
                from: Some(from.to_rfc3339_opts(SecondsFormat::Micros, true)),
                poll_interval_ms: Some(100),
            }),
        };
        MetaLakeSourceFunc::new(profile, table).unwrap()
    }
}

fn keys(keys: Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("k", DataType::Int64, true)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(keys))];
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

/// What the planner gives a source of `shop.orders (k bigint)`.
fn out_schema() -> Arc<ArroyoSchema> {
    let row = DataType::Struct(vec![Field::new("k", DataType::Int64, true)].into());
    let schema = Schema::new(vec![
        Field::new("before", row.clone(), true),
        Field::new("after", row, true),
        Field::new("op", DataType::Utf8, false),
        Field::new(TIMESTAMP_FIELD, DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]);
    Arc::new(ArroyoSchema::new_unkeyed(Arc::new(schema), 3))
}

/// A source running as Arroyo runs one: its control channel, its responses, and what it emits.
struct Running {
    control: Sender<ControlMessage>,
    responses: Receiver<ControlResp>,
    data: BatchReceiver,
    handle: JoinHandle<DataflowResult<SourceFinishType>>,
}

async fn run(mut source: MetaLakeSourceFunc, task_info: TaskInfo, restore: Option<u32>) -> Running {
    let (control, control_rx) = channel(128);
    let (command_tx, responses) = channel(128);
    let (data_tx, data) = batch_bounded(128);
    let restore = restore.map(|epoch| {
        MetadataOrManifest::Metadata(CheckpointMetadata {
            job_id: task_info.job_id.clone(),
            epoch,
            min_epoch: 1,
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            operator_ids: vec![task_info.operator_id.clone()],
        })
    });
    let task_info = Arc::new(task_info);
    let ctx = OperatorContext::new(
        task_info.clone(),
        restore.as_ref(),
        command_tx.clone(),
        1,
        vec![],
        Some(out_schema()),
        source.tables(),
    )
    .await;
    let chain = Arc::new(ChainInfo {
        job_id: ctx.task_info.job_id.clone(),
        task_id: ctx.task_info.operator_idx,
        description: "metalake source".to_string(),
        task_index: ctx.task_info.task_index,
    });
    let mut ctx = SourceContext::from_operator(ctx, chain.clone(), control_rx);
    let arrow = ArrowCollector::new(chain, Some(ctx.out_schema.clone()), vec![vec![data_tx]]);
    let mut collector = SourceCollector::new(ctx.out_schema.clone(), arrow, command_tx, &task_info);
    let handle = tokio::spawn(async move { source.run(&mut ctx, &mut collector).await });
    Running {
        control,
        responses,
        data,
        handle,
    }
}

impl Running {
    /// The rows of the next batch it emits, as `(op, key)`.
    async fn batch_rows(&mut self) -> Vec<(String, i64)> {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), self.data.recv())
                .await
                .expect("waited for a batch")
                .expect("the source is running");
            if let ArrowMessage::Data(batch) = message {
                return enveloped_rows(&batch);
            }
        }
    }

    /// The next `n` rows it emits, as `(op, key)`.
    async fn rows(&mut self, n: usize) -> Vec<(String, i64)> {
        let mut rows = Vec::new();
        while rows.len() < n {
            let message = tokio::time::timeout(Duration::from_secs(30), self.data.recv())
                .await
                .unwrap_or_else(|_| panic!("waited for {n} rows, and had {rows:?}"))
                .expect("the source is running");
            if let ArrowMessage::Data(batch) = message {
                rows.extend(enveloped_rows(&batch));
            }
        }
        assert_eq!(rows.len(), n, "{rows:?}");
        rows
    }

    /// That it emits no row for a while.
    async fn quiet(&mut self) {
        let wait = tokio::time::sleep(Duration::from_millis(1500));
        tokio::pin!(wait);
        loop {
            tokio::select! {
                _ = &mut wait => return,
                message = self.data.recv() => {
                    if let Some(ArrowMessage::Data(batch)) = message {
                        panic!("emitted {:?}", enveloped_rows(&batch));
                    }
                }
            }
        }
    }

    async fn checkpoint(&mut self, epoch: u32) -> CheckpointCompleted {
        let barrier = CheckpointBarrier {
            epoch,
            min_epoch: 0,
            timestamp: SystemTime::now(),
            then_stop: false,
        };
        self.control.send(ControlMessage::Checkpoint(barrier)).await.unwrap();
        loop {
            if let ControlResp::CheckpointCompleted(c) = self.responses.recv().await.unwrap() {
                assert_eq!(c.checkpoint_epoch, epoch as u64);
                return c;
            }
        }
    }

    async fn stop(self) -> DataflowResult<SourceFinishType> {
        let _ = self
            .control
            .send(ControlMessage::Stop {
                mode: StopMode::Immediate,
            })
            .await;
        self.handle.await.unwrap()
    }
}

/// Each row of an enveloped batch as `(op, key)`: the key in `after` for a `c`, in `before` for a `d`.
fn enveloped_rows(batch: &RecordBatch) -> Vec<(String, i64)> {
    let (before, after) = (batch.column(0).as_struct(), batch.column(1).as_struct());
    let ops = batch.column(2).as_string::<i32>();
    (0..batch.num_rows())
        .map(|i| {
            let op = ops.value(i).to_string();
            let row = if op == "c" { after } else { before };
            assert!(row.is_valid(i), "{op} with its row in the other struct");
            (op, row.column(0).as_primitive::<Int64Type>().value(i))
        })
        .collect()
}

fn sorted(mut rows: Vec<(String, i64)>) -> Vec<(String, i64)> {
    rows.sort();
    rows
}

fn rows(op: &str, keys: Range<i64>) -> Vec<(String, i64)> {
    keys.map(|k| (op.to_string(), k)).collect()
}

fn task_info(job: &str, index: u32, parallelism: u32) -> TaskInfo {
    let mut info = arroyo_types::get_test_task_info();
    info.job_id = job.to_string();
    info.task_index = index;
    info.parallelism = parallelism;
    info
}

/// Writes what a completed checkpoint of one subtask leaves, so that a source can be restored from it.
async fn keep_checkpoint(info: &TaskInfo, epoch: u32, completed: CheckpointCompleted) {
    let subtask = completed.subtask_metadata;
    let table = GlobalKeyedTable::merge_checkpoint_metadata(
        subtask.table_configs.get("m").unwrap().clone(),
        HashMap::from([(info.task_index, subtask.table_metadata.get("m").unwrap().clone())]),
    )
    .unwrap()
    .unwrap();
    StateBackend::write_operator_checkpoint_metadata(
        &StorageProviderFor::Worker,
        OperatorCheckpointMetadata {
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: HashMap::from([("m".to_string(), table)]),
            table_configs: subtask.table_configs,
            operator_metadata: Some(OperatorMetadata {
                job_id: info.job_id.clone(),
                operator_id: info.operator_id.clone(),
                epoch,
                min_watermark: Some(0),
                max_watermark: Some(0),
                parallelism: info.parallelism as u64,
            }),
        },
    )
    .await
    .unwrap();
    StateBackend::write_checkpoint_metadata(
        &StorageProviderFor::Worker,
        CheckpointMetadata {
            job_id: info.job_id.clone(),
            epoch,
            min_epoch: 1,
            start_time: 0,
            finish_time: 0,
            operator_ids: vec![info.operator_id.clone()],
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn chg5_follow_snapshot_and_changes() {
    let lake = Lake::new().await;
    lake.append(0..3).await;
    let from = SystemTime::now();
    let mut source = run(lake.source(from), task_info(&unique(), 0, 1), None).await;

    // the table at its start time, then what each commit changed
    assert_eq!(sorted(source.rows(3).await), rows("c", 0..3));
    lake.delete(|k| k == 1).await;
    assert_eq!(source.rows(1).await, rows("d", 1..2));
    lake.append(3..5).await;
    assert_eq!(sorted(source.rows(2).await), rows("c", 3..5));
    source.quiet().await;
    source.stop().await.unwrap();
}

#[tokio::test]
async fn follow_files_split_across_subtasks() {
    let lake = Lake::new().await;
    for k in 0..8 {
        lake.append(2 * k..2 * k + 2).await;
    }
    let (job, from) = (unique(), SystemTime::now());
    let mut first = run(lake.source(from), task_info(&job, 0, 2), None).await;
    let mut second = run(lake.source(from), task_info(&job, 1, 2), None).await;

    // each row once, from one of them; neither waits for the other
    let mut by_subtask = [Vec::new(), Vec::new()];
    let mut seen = Vec::new();
    while seen.len() < 16 {
        let (i, rows) = tokio::select! {
            rows = first.batch_rows() => (0, rows),
            rows = second.batch_rows() => (1, rows),
        };
        by_subtask[i].extend(rows.iter().map(|r| r.1));
        seen.extend(rows);
    }
    assert_eq!(sorted(seen), rows("c", 0..16));
    assert!(
        by_subtask.iter().all(|keys| !keys.is_empty()),
        "both read some: {by_subtask:?}"
    );

    // a row is deleted by the subtask that emitted it, which reads its file
    lake.delete(|k| k % 2 == 0).await;
    let mut deleted: [Vec<i64>; 2] = [Vec::new(), Vec::new()];
    while deleted.iter().map(Vec::len).sum::<usize>() < 8 {
        let (i, rows) = tokio::select! {
            rows = first.batch_rows() => (0, rows),
            rows = second.batch_rows() => (1, rows),
        };
        assert!(rows.iter().all(|r| r.0 == "d"), "{rows:?}");
        deleted[i].extend(rows.iter().map(|r| r.1));
    }
    for i in 0..2 {
        let even: Vec<i64> = by_subtask[i].iter().copied().filter(|k| k % 2 == 0).collect();
        assert_eq!(sorted_keys(deleted[i].clone()), sorted_keys(even), "subtask {i}");
    }
    first.stop().await.unwrap();
    second.stop().await.unwrap();
}

fn sorted_keys(mut keys: Vec<i64>) -> Vec<i64> {
    keys.sort_unstable();
    keys
}

#[tokio::test]
async fn follow_recovers_from_checkpoint() {
    let lake = Lake::new().await;
    lake.append(0..3).await;
    let (info, from) = (task_info(&unique(), 0, 1), SystemTime::now());
    let mut source = run(lake.source(from), info.clone(), None).await;
    assert_eq!(sorted(source.rows(3).await), rows("c", 0..3));

    let completed = source.checkpoint(1).await;
    keep_checkpoint(&info, 1, completed).await;
    lake.append(3..5).await;
    assert_eq!(sorted(source.rows(2).await), rows("c", 3..5));
    source.stop().await.unwrap();

    // what came after the checkpoint again, and nothing from before it
    let mut restored = run(lake.source(from), info.clone(), Some(1)).await;
    assert_eq!(sorted(restored.rows(2).await), rows("c", 3..5));
    lake.append(5..6).await;
    assert_eq!(restored.rows(1).await, rows("c", 5..6));
    restored.quiet().await;
    restored.stop().await.unwrap();
}

/// Files go to subtasks by their number, and each subtask's cursor is its own: another parallelism can't restore.
#[tokio::test]
async fn restore_other_parallelism_refused() {
    let lake = Lake::new().await;
    lake.append(0..3).await;
    let (info, from) = (task_info(&unique(), 0, 1), SystemTime::now());
    let mut source = run(lake.source(from), info.clone(), None).await;
    assert_eq!(sorted(source.rows(3).await), rows("c", 0..3));
    let completed = source.checkpoint(1).await;
    keep_checkpoint(&info, 1, completed).await;
    source.stop().await.unwrap();

    let wider = run(lake.source(from), task_info(&info.job_id, 0, 2), Some(1)).await;
    let Err(error) = wider.handle.await.unwrap() else {
        panic!("a restore at another parallelism ran")
    };
    assert!(error.to_string().contains("at 2 subtasks from 1"), "{error}");
}
