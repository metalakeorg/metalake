//! Runs the core's tasks once, for whatever schedules it. Every task is safe to repeat, and to run beside commits. In
//! order, what rewrites comes before what takes away: a compaction's old files are collectable only once expiring queues
//! them, and only then can a sweep delete them. Footers are filled after the rewrites, which replace small files with
//! files that have theirs.

use std::time::Duration;

use metalake::{Ref, Storage, Table, TableName};
use serde::Serialize;

use crate::{Error, Lake, Result, table};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Task {
    Times,
    Flush,
    Compact,
    /// The footers of files that came without one, kept in the catalog (FTR-1).
    Footers,
    Expire,
    Sweep,
    Orphans,
    /// What a client left in a table's location: asked for by table, as it alone lists storage (RET-7b).
    Strays,
}

/// How old an object must be for `strays` to take it: more than any write in progress.
const STRAY_AGE: Duration = Duration::from_secs(3 * 24 * 3600);

const ORDER: [Task; 7] = [
    Task::Times,
    Task::Flush,
    Task::Compact,
    Task::Footers,
    Task::Expire,
    Task::Sweep,
    Task::Orphans,
];

impl Task {
    /// A task by the name it goes by.
    pub fn parse(name: &str) -> Result<Self> {
        let tasks = ORDER.into_iter().chain([Task::Strays]);
        tasks.clone().find(|task| task.name() == name).ok_or_else(|| {
            let names: Vec<&str> = tasks.map(Task::name).collect();
            Error::usage(format!("{name} is not a task: {}", names.join(", ")))
        })
    }

    fn name(self) -> &'static str {
        match self {
            Task::Times => "times",
            Task::Flush => "flush",
            Task::Compact => "compact",
            Task::Footers => "footers",
            Task::Expire => "expire",
            Task::Sweep => "sweep",
            Task::Orphans => "orphans",
            Task::Strays => "strays",
        }
    }

    /// Whether it runs table by table, on one ref or one table if asked.
    fn by_table(self) -> bool {
        matches!(self, Task::Flush | Task::Compact | Task::Footers)
    }
}

/// What to run: every task, or one, and a rewrite on one ref or one table.
#[derive(Debug, Default)]
pub struct MaintainRequest {
    pub task: Option<Task>,
    pub on: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Done {
    pub task: &'static str,
    #[serde(rename = "ref")]
    pub on: Option<String>,
    pub table: Option<String>,
    /// How many rows or objects went, or the commit of a rewrite, if it wrote one.
    pub result: Option<u64>,
}

/// What was done, even when a task failed: the rest of the run is safe to repeat.
#[derive(Debug)]
pub struct MaintainRun {
    pub done: Vec<Done>,
    pub failed: Option<Error>,
}

impl Lake {
    /// Runs the tasks in order, up to the first that fails.
    pub async fn maintain(&self, request: &MaintainRequest) -> Result<MaintainRun> {
        let scoped = request.on.is_some() || request.table.is_some();
        let strays = matches!(request.task, Some(Task::Strays));
        if scoped && !strays && !request.task.is_some_and(Task::by_table) {
            return Err(Error::usage(
                "a ref or a table is for flush, compact, footers and strays",
            ));
        }
        if strays && request.table.is_none() {
            return Err(Error::usage("strays lists a table's location: name the table"));
        }
        let tasks = request.task.as_ref().map_or(&ORDER[..], std::slice::from_ref);

        let mut done = Vec::new();
        let mut failed = None;
        for task in tasks {
            if let Err(error) = self.perform(*task, request, &mut done).await {
                failed = Some(error);
                break;
            }
        }
        Ok(MaintainRun { done, failed })
    }

    async fn perform(&self, task: Task, request: &MaintainRequest, done: &mut Vec<Done>) -> Result<()> {
        let maintenance = self.catalog.maintenance();
        let whole = |result| Done {
            task: task.name(),
            on: None,
            table: None,
            result: Some(result),
        };
        match task {
            Task::Times => done.push(whole(maintenance.fill_commit_times().await?)),
            Task::Expire => done.push(whole(maintenance.expire().await?)),
            Task::Sweep => done.push(whole(maintenance.sweep(&self.storage()?).await?)),
            Task::Orphans => done.push(whole(maintenance.orphans(&self.storage()?).await?)),
            Task::Strays => {
                let storage = self.storage()?;
                for on in self.branches(request).await? {
                    if let Some(table) = tables(request, &on).await?.first() {
                        let adopted = maintenance.strays(table, &storage, STRAY_AGE).await?;
                        done.push(Done {
                            task: task.name(),
                            on: Some(on.name().to_string()),
                            table: Some(format!("{}.{}", table.namespace().join("."), table.name())),
                            result: Some(adopted),
                        });
                        return Ok(());
                    }
                }
                let name = request.table.as_deref().unwrap_or_default();
                return Err(Error::failed(format!("no branch has the table {name}")));
            }
            Task::Flush | Task::Compact | Task::Footers => {
                let storage = self.storage()?;
                let mut found = false;
                for on in self.branches(request).await? {
                    for table in tables(request, &on).await? {
                        found = true;
                        let result = self.by_table(task, &table, &storage).await?;
                        done.push(Done {
                            task: task.name(),
                            on: Some(on.name().to_string()),
                            table: Some(format!("{}.{}", table.namespace().join("."), table.name())),
                            result,
                        });
                    }
                }
                if let (false, Some(name)) = (found, &request.table) {
                    return Err(Error::failed(format!("no branch has the table {name}")));
                }
            }
        }
        Ok(())
    }

    /// A task of one table: a rewrite's commit, if it wrote one, or how many footers were kept.
    async fn by_table(&self, task: Task, table: &Table, storage: &Storage) -> Result<Option<u64>> {
        let maintenance = self.catalog.maintenance();
        let commit = match task {
            Task::Footers => return Ok(Some(maintenance.footers(table, storage).await?)),
            Task::Flush => maintenance.flush(table, storage).await?,
            _ => maintenance.compact(table, storage).await?,
        };
        Ok(commit.map(|seq| seq as u64))
    }

    /// The branches a rewrite goes over: a tag takes no commits (BR-2).
    async fn branches(&self, request: &MaintainRequest) -> Result<Vec<Ref>> {
        let mut refs = self.catalog.refs().await?;
        refs.retain(|r| r.kind() == "branch");
        if let Some(name) = &request.on {
            refs.retain(|r| r.name() == name);
            if refs.is_empty() {
                return Err(Error::failed(format!("no branch {name}")));
            }
        }
        Ok(refs)
    }
}

/// The tables of a branch to rewrite, read now, since a rewrite is refused if what it read has changed.
async fn tables(request: &MaintainRequest, on: &Ref) -> Result<Vec<Table>> {
    if let Some(name) = &request.table {
        // Over every branch, a table need not be on each. On a branch that was named, it must.
        let here = |t: &TableName| format!("{}.{}", t.namespace.join("."), t.name) == *name;
        if request.on.is_none() && !on.tables().await?.iter().any(here) {
            return Ok(vec![]);
        }
        return Ok(vec![table(on, name).await?]);
    }
    let mut tables = Vec::new();
    for table in on.tables().await? {
        let namespace: Vec<&str> = table.namespace.iter().map(String::as_str).collect();
        tables.push(on.table(&namespace, &table.name).await?);
    }
    Ok(tables)
}
