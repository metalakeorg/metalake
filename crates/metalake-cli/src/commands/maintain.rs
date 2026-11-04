//! `metalake maintain`: runs the core's tasks once, for whatever schedules it. Every task is safe to repeat, and to run
//! beside commits. In order, what rewrites comes before what takes away: a compaction's old files are collectable
//! only once expiring queues them, and only then can a sweep delete them.

use metalake::{Ref, Storage, Table};
use serde::Serialize;

use crate::args::{Maintain, Task};
use crate::failure::Failure;
use crate::lake::Lake;
use crate::out::Out;

const ORDER: [Task; 6] = [
    Task::Times,
    Task::Flush,
    Task::Compact,
    Task::Expire,
    Task::Sweep,
    Task::Orphans,
];
const COLUMNS: [&str; 4] = ["task", "ref", "table", "result"];

#[derive(Debug, Serialize)]
struct Done {
    task: &'static str,
    #[serde(rename = "ref")]
    on: Option<String>,
    table: Option<String>,
    /// How many rows or objects went, or the commit of a rewrite, if it wrote one.
    result: Option<u64>,
}

impl Task {
    fn name(self) -> &'static str {
        match self {
            Task::Times => "times",
            Task::Flush => "flush",
            Task::Compact => "compact",
            Task::Expire => "expire",
            Task::Sweep => "sweep",
            Task::Orphans => "orphans",
        }
    }

    fn rewrites(self) -> bool {
        matches!(self, Task::Flush | Task::Compact)
    }
}

/// Runs the tasks, and prints what was done even when one fails: the rest of the run is safe to repeat.
pub async fn run(args: &Maintain, lake: &Lake, out: &Out) -> Result<(), Failure> {
    let scoped = args.on.is_some() || args.table.is_some();
    if scoped && !args.task.is_some_and(Task::rewrites) {
        return Err(Failure::usage("--ref and --table are for flush and compact"));
    }
    let tasks = args.task.as_ref().map_or(&ORDER[..], std::slice::from_ref);

    let mut done = Vec::new();
    let mut failed = None;
    for task in tasks {
        if let Err(failure) = perform(*task, args, lake, &mut done).await {
            failed = Some(failure);
            break;
        }
    }
    out.records(&COLUMNS, &done).await?;
    failed.map_or(Ok(()), Err)
}

async fn perform(task: Task, args: &Maintain, lake: &Lake, done: &mut Vec<Done>) -> Result<(), Failure> {
    let maintenance = lake.catalog.maintenance();
    let whole = |result| Done {
        task: task.name(),
        on: None,
        table: None,
        result: Some(result),
    };
    match task {
        Task::Times => done.push(whole(maintenance.fill_commit_times().await?)),
        Task::Expire => done.push(whole(maintenance.expire().await?)),
        Task::Sweep => done.push(whole(maintenance.sweep(&lake.storage()?).await?)),
        Task::Orphans => done.push(whole(maintenance.orphans(&lake.storage()?).await?)),
        Task::Flush | Task::Compact => {
            let storage = lake.storage()?;
            for on in branches(args, lake).await? {
                for table in tables(args, &on).await? {
                    let commit = rewrite(task, lake, &table, &storage).await?;
                    done.push(Done {
                        task: task.name(),
                        on: Some(on.name().to_string()),
                        table: Some(format!("{}.{}", table.namespace().join("."), table.name())),
                        result: commit.map(|seq| seq as u64),
                    });
                }
            }
        }
    }
    Ok(())
}

async fn rewrite(task: Task, lake: &Lake, table: &Table, storage: &Storage) -> Result<Option<i64>, Failure> {
    let maintenance = lake.catalog.maintenance();
    Ok(match task {
        Task::Flush => maintenance.flush(table, storage).await?,
        _ => maintenance.compact(table, storage).await?,
    })
}

/// The branches a rewrite goes over: a tag takes no commits (BR-2).
async fn branches(args: &Maintain, lake: &Lake) -> Result<Vec<Ref>, Failure> {
    let mut refs = lake.catalog.refs().await?;
    refs.retain(|r| r.kind() == "branch");
    if let Some(name) = &args.on {
        refs.retain(|r| r.name() == name);
        if refs.is_empty() {
            return Err(Failure::failed(format!("no branch {name}")));
        }
    }
    Ok(refs)
}

/// The tables of a branch to rewrite, read now, since a rewrite is refused if what it read has changed.
async fn tables(args: &Maintain, on: &Ref) -> Result<Vec<Table>, Failure> {
    if let Some(name) = &args.table {
        return Ok(vec![super::table(on, name).await?]);
    }
    let mut tables = Vec::new();
    for table in on.tables().await? {
        let namespace: Vec<&str> = table.namespace.iter().map(String::as_str).collect();
        tables.push(on.table(&namespace, &table.name).await?);
    }
    Ok(tables)
}
