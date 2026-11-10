//! A table's changes (CHG-2 to CHG-5): the rows each commit inserted and deleted, with `_change_type`, `_commit_seq`
//! and `_commit_timestamp`. A reader is somewhere between two commits, and where is the `_commit_seq` of the last rows
//! it dealt with: a number it keeps with what it did with them, and gives back to go on from there. Commits come once
//! they are a second old (CHG-1), and nothing here waits for one: a caller asks again when it wants to.

use std::time::SystemTime;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use metalake::{Start, Storage, TableStream};

use crate::write::TablePath;
use crate::{Error, Lake, Result};

/// Rows of changes, read as they are asked for.
pub type Changes = BoxStream<'static, Result<RecordBatch>>;

/// Where a follower starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAt {
    /// What the table holds now, as rows inserted, then what comes.
    Snapshot,
    /// Only what comes from now.
    Latest,
    /// What comes after the commit with this `_commit_seq`.
    After(i64),
    /// What comes after this time (CHG-5).
    Time(SystemTime),
}

/// A reader of one table's changes, a step at a time.
pub struct Follower {
    stream: TableStream,
    storage: Storage,
}

impl Follower {
    /// The table's columns, then `_change_type`, `_commit_seq` and `_commit_timestamp`.
    pub fn schema(&self) -> SchemaRef {
        self.stream.schema().clone()
    }

    /// The commit a reader that dealt with every step so far goes on after, with [`StartAt::After`]. None before the
    /// first step.
    pub fn position(&self) -> Option<i64> {
        self.stream.position()
    }

    /// The rows of the commits that came since the last step, which may be none, or nothing when no commit came. Its
    /// [`position`](Self::position) counts once all of them are dealt with.
    pub async fn next(&mut self) -> Result<Option<Changes>> {
        let Some(step) = self.stream.next().await? else {
            return Ok(None);
        };
        let opened: Vec<_> = step
            .tasks
            .into_iter()
            .map(|task| self.stream.open(task, &self.storage))
            .collect();
        Ok(Some(stream::iter(opened).flatten().map_err(Error::from).boxed()))
    }
}

impl Lake {
    /// A reader of a table's changes on its ref (CHG-4, CHG-5).
    pub async fn follow(&self, path: &TablePath, start: StartAt) -> Result<Follower> {
        let (on, table) = self.table(path).await?;
        let storage = self.storage()?;
        let stream = match start {
            StartAt::Snapshot => on.stream(&table, Start::Snapshot).await?,
            StartAt::Latest => on.stream(&table, Start::Latest).await?,
            StartAt::After(seq) => on.stream(&table, Start::After(seq)).await?,
            StartAt::Time(time) => on.as_of(time).stream(&table, Start::Latest).await?,
        };
        Ok(Follower { stream, storage })
    }

    /// The rows of the commits of a table after `after` and up to `until`, both `_commit_seq`, or up to the last commit
    /// a second old: chained, `(a, b]` then `(b, c]` take each commit once (CHG-3).
    pub async fn changes(&self, path: &TablePath, after: i64, until: Option<i64>) -> Result<(SchemaRef, Changes)> {
        if until.is_some_and(|until| until < after) {
            return Err(Error::usage(format!(
                "changes of {path} after {after} up to {}: the end comes before the start",
                until.unwrap_or_default()
            )));
        }
        let (on, table) = self.table(path).await?;
        let storage = self.storage()?;
        let mut stream = on.stream(&table, Start::After(after)).await?;
        let mut opened = Vec::new();
        'steps: while let Some(step) = stream.next().await? {
            for task in step.tasks {
                if until.is_some_and(|until| task.seq > until) {
                    break 'steps;
                }
                opened.push(stream.open(task, &storage));
            }
            if until.is_some_and(|until| stream.position() >= Some(until)) {
                break;
            }
        }
        let schema = stream.schema().clone();
        Ok((schema, stream::iter(opened).flatten().map_err(Error::from).boxed()))
    }
}
