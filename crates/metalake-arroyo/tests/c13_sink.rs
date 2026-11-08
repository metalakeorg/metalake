//! The sink against a real catalog: rows written as Arroyo runs a sink, each epoch's files prepared at its checkpoint
//! and committed once it is complete, then read back through the core. It needs a Postgres at `DATABASE_URL`, as
//! `c13_source.rs` does.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::TaskCheckpointEventType;
use arroyo_rpc::{ControlResp, TIMESTAMP_FIELD};
use arroyo_types::{TaskInfo, Watermark};
use async_trait::async_trait;
use metalake_arroyo::config::{MetaLakeSink, MetaLakeTableType};
use metalake_arroyo::sink::MetaLakeSinkFunc;
use tokio::sync::mpsc::{Receiver, channel};

mod common;

use common::{Lake, task_info, unique};

/// A sink's input: the declared `k`, and Arroyo's event time.
fn arroyo_rows(keys: Range<i64>) -> RecordBatch {
    let n = keys.clone().count();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("k", Arc::new(Int64Array::from_iter_values(keys))),
        (TIMESTAMP_FIELD, Arc::new(TimestampNanosecondArray::from(vec![0; n]))),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn input_schema() -> Arc<ArroyoSchema> {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new(TIMESTAMP_FIELD, DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]);
    Arc::new(ArroyoSchema::new_unkeyed(Arc::new(schema), 1))
}

/// A sink emits nothing downstream.
struct Nowhere;

#[async_trait]
impl Collector for Nowhere {
    async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
        unreachable!("a sink emits nothing")
    }

    async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
        Ok(())
    }
}

/// One subtask of a sink, driven as Arroyo drives one: batches, a checkpoint, then the commit.
struct Subtask {
    sink: MetaLakeSinkFunc,
    ctx: OperatorContext,
    responses: Receiver<ControlResp>,
}

impl Subtask {
    async fn start(lake: &Lake, info: TaskInfo) -> Self {
        let table = lake.table(MetaLakeTableType::Sink(MetaLakeSink::default()));
        let mut sink = MetaLakeSinkFunc::new(lake.profile(), table).unwrap();
        let (control_tx, responses) = channel(128);
        let tables = sink.tables();
        let mut ctx =
            OperatorContext::new(Arc::new(info), None, control_tx, 1, vec![input_schema()], None, tables).await;
        sink.on_start(&mut ctx).await.unwrap();
        Subtask { sink, ctx, responses }
    }

    async fn write(&mut self, keys: Range<i64>) {
        self.sink
            .process_batch(arroyo_rows(keys), &mut self.ctx, &mut Nowhere)
            .await
            .unwrap();
    }

    /// What its checkpoint keeps as pre-commit data.
    async fn prepare(&mut self) -> Vec<u8> {
        self.sink.prepare().await.unwrap().to_bytes()
    }

    /// The commit of `epoch`, with every subtask's pre-commit data, as the controller sends it; and its ack.
    async fn commit(&mut self, epoch: u32, prepared: &HashMap<u32, Vec<u8>>) {
        let data = HashMap::from([("p".to_string(), prepared.clone())]);
        self.sink.handle_commit(epoch, &data, &mut self.ctx).await.unwrap();
        loop {
            let response = tokio::time::timeout(Duration::from_secs(10), self.responses.recv())
                .await
                .expect("an ack")
                .expect("the channel is open");
            if let ControlResp::CheckpointEvent(e) = response
                && e.event_type == TaskCheckpointEventType::FinishedCommit
            {
                assert_eq!(e.checkpoint_epoch, epoch as u64);
                return;
            }
        }
    }
}

/// The commits whose message names `epoch` of `job`.
async fn commits_of(lake: &Lake, job: &str, epoch: u32) -> usize {
    let history = lake.main.history(100).await.unwrap();
    let message = format!("job {job}, epoch {epoch}");
    history
        .iter()
        .filter(|c| c.message.as_deref() == Some(message.as_str()))
        .count()
}

#[tokio::test]
async fn cmt10_sink_commits_prepared_epochs() {
    let lake = Lake::new().await;
    let job = unique();
    let mut sink = Subtask::start(&lake, task_info(&job, 0, 1)).await;

    sink.write(0..2).await;
    sink.write(2..3).await;
    let prepared = HashMap::from([(0, sink.prepare().await)]);
    assert_eq!(
        lake.keys().await,
        Vec::<i64>::new(),
        "nothing is visible before the commit"
    );
    sink.commit(1, &prepared).await;
    assert_eq!(lake.keys().await, [0, 1, 2]);

    sink.write(3..5).await;
    let prepared = HashMap::from([(0, sink.prepare().await)]);
    sink.commit(2, &prepared).await;
    assert_eq!(lake.keys().await, [0, 1, 2, 3, 4]);
    assert_eq!(
        (commits_of(&lake, &job, 1).await, commits_of(&lake, &job, 2).await),
        (1, 1)
    );
}

#[tokio::test]
async fn sink_subtasks_commit_atomically() {
    let lake = Lake::new().await;
    let job = unique();
    let mut first = Subtask::start(&lake, task_info(&job, 0, 2)).await;
    let mut second = Subtask::start(&lake, task_info(&job, 1, 2)).await;
    first.write(0..3).await;
    second.write(3..6).await;
    let prepared = HashMap::from([(0, first.prepare().await), (1, second.prepare().await)]);

    // only subtask 0 commits, both subtasks' files in one transaction; each acknowledges
    second.commit(1, &prepared).await;
    assert_eq!(lake.keys().await, Vec::<i64>::new(), "subtask 1 commits nothing");
    first.commit(1, &prepared).await;
    assert_eq!(lake.keys().await, [0, 1, 2, 3, 4, 5]);
    assert_eq!(commits_of(&lake, &job, 1).await, 1);
}

#[tokio::test]
async fn cmt8_sink_replayed_commit_is_idempotent() {
    let lake = Lake::new().await;
    let job = unique();
    let mut sink = Subtask::start(&lake, task_info(&job, 0, 1)).await;
    sink.write(0..3).await;
    let prepared = HashMap::from([(0, sink.prepare().await)]);
    sink.commit(1, &prepared).await;

    // the controller replays the last commit after a restart: a new run, the same epoch and files
    let mut restarted = Subtask::start(&lake, task_info(&job, 0, 1)).await;
    restarted.commit(1, &prepared).await;
    assert_eq!(lake.keys().await, [0, 1, 2], "no row twice");
    assert_eq!(commits_of(&lake, &job, 1).await, 1);
}

#[tokio::test]
async fn cmt10_sink_recovers_prepared_epoch() {
    let lake = Lake::new().await;
    let job = unique();

    // a run prepares epoch 1, and fails before its commit
    let mut failed = Subtask::start(&lake, task_info(&job, 0, 1)).await;
    failed.write(0..4).await;
    let prepared = HashMap::from([(0, failed.prepare().await)]);
    drop(failed);

    // the next run restores those files and commits them
    let mut restored = Subtask::start(&lake, task_info(&job, 0, 1)).await;
    restored.commit(1, &prepared).await;
    assert_eq!(lake.keys().await, [0, 1, 2, 3]);
}

#[tokio::test]
async fn sink_empty_epoch_skips() {
    let lake = Lake::new().await;
    let job = unique();
    let mut sink = Subtask::start(&lake, task_info(&job, 0, 1)).await;
    let prepared = HashMap::from([(0, sink.prepare().await)]);
    sink.commit(1, &prepared).await;
    assert_eq!(commits_of(&lake, &job, 1).await, 0, "no commit, and still the ack");
}
