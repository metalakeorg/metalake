//! `metalake maintain`: runs the core's tasks once, for whatever schedules it.

use metalake_client::{Error, Lake, MaintainRequest};

use crate::args::{Maintain, Task};
use crate::out::Out;

const COLUMNS: [&str; 4] = ["task", "ref", "table", "result"];

impl From<Task> for metalake_client::Task {
    fn from(task: Task) -> Self {
        match task {
            Task::Times => Self::Times,
            Task::Flush => Self::Flush,
            Task::Compact => Self::Compact,
            Task::Footers => Self::Footers,
            Task::Expire => Self::Expire,
            Task::Sweep => Self::Sweep,
            Task::Orphans => Self::Orphans,
        }
    }
}

/// Prints what was done even when a task failed.
pub async fn run(args: &Maintain, lake: &Lake, out: &Out) -> Result<(), Error> {
    let run = lake
        .maintain(&MaintainRequest {
            task: args.task.map(Into::into),
            on: args.on.clone(),
            table: args.table.clone(),
        })
        .await?;
    out.records(&COLUMNS, &run.done).await?;
    run.failed.map_or(Ok(()), Err)
}
