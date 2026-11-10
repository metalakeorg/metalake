//! A table's changes (CHG-2 to CHG-5): the rows each commit inserted and deleted, with `_change_type`, `_commit_seq`
//! and `_commit_timestamp`. A reader is somewhere between two commits, and where is the `_commit_seq` of the last rows
//! it dealt with: a number it keeps with what it did with them, and gives back to go on from there. Commits come once
//! they are a second old (CHG-1): [`Follower::next`] says what came, and [`Follower::wait`] asks until something does.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use metalake::{Field, Ref, Start, Storage, Table, TableStream};
use metalake_pg::records::LATEST;

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

/// A reader of one table's changes, a step at a time. When a commit changes the table's definition only by adding
/// columns that may be null, it goes on under the new one: [`schema`](Follower::schema) then has them, and rows of earlier
/// files have them null. Any other change of the columns stops it, with the commit to follow again after.
///
/// A step counts once its rows are read to the end: one whose rows failed, or were dropped before their end, is given
/// again by the next call, and [`position`](Follower::position) does not count it until then.
pub struct Follower {
    stream: TableStream,
    storage: Storage,
    /// The ref as it is now, not as of a time, to open the stream again on.
    on: Ref,
    /// Where it started, to start again when its first step was not read.
    start: StartAt,
    /// The position before the last step.
    before: Option<i64>,
    /// Whether the rows of the last step were read to the end.
    read: Arc<AtomicBool>,
}

impl Follower {
    /// The table's columns, then `_change_type`, `_commit_seq` and `_commit_timestamp`.
    pub fn schema(&self) -> SchemaRef {
        self.stream.schema().clone()
    }

    /// The commit a reader that read every step so far to its end goes on after, with [`StartAt::After`]. None before
    /// the first step.
    pub fn position(&self) -> Option<i64> {
        match self.read.load(Ordering::SeqCst) {
            true => self.stream.position(),
            false => self.before,
        }
    }

    /// The rows of the commits that came since the last step, which may be none, or nothing when no commit came; or
    /// the last step again, when its rows were not read to the end.
    pub async fn next(&mut self) -> Result<Option<Changes>> {
        if !self.read.load(Ordering::SeqCst) {
            self.stream = self.again().await?;
            self.read.store(true, Ordering::SeqCst);
        }
        let before = self.stream.position();
        let step = match self.stream.next().await {
            Err(metalake::Error::Unsupported(why)) => {
                self.stream = self.reopen(why).await?;
                self.stream.next().await?
            }
            taken => taken?,
        };
        let Some(step) = step else {
            return Ok(None);
        };
        self.before = before;
        // A step with no rows is read once it is given.
        let read = Arc::new(AtomicBool::new(step.tasks.is_empty()));
        self.read = read.clone();
        let opened: Vec<_> = step
            .tasks
            .into_iter()
            .map(|task| self.stream.open(task, &self.storage))
            .collect();
        let failed = Arc::new(AtomicBool::new(false));
        let rows = stream::iter(opened).flatten().map_err(Error::from).inspect_err({
            let failed = failed.clone();
            move |_| failed.store(true, Ordering::SeqCst)
        });
        let end = stream::once(async move {
            if !failed.load(Ordering::SeqCst) {
                read.store(true, Ordering::SeqCst);
            }
        })
        .filter_map(|()| async { None });
        Ok(Some(rows.chain(end).boxed()))
    }

    /// A stream that gives the last step again: after the position before it, or from the start when it was the first.
    async fn again(&self) -> Result<TableStream> {
        let table = self.stream.table();
        Ok(match self.before {
            Some(position) => self.on.stream(table, Start::After(position)).await?,
            None => open(&self.on, table, self.start).await?,
        })
    }

    /// The next step, asking again every `interval` until a commit comes. Its rows may be none: a commit that changed
    /// no row, as a compaction, still moves the position. Dropping it while it waits leaves the follower where it was,
    /// so a caller cancels it by dropping it.
    pub async fn wait(&mut self, interval: Duration) -> Result<Changes> {
        loop {
            if let Some(step) = self.next().await? {
                return Ok(step);
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// The stream after its position, under the table's definition now, if that only added columns that may be null to
    /// the one it read under, or left them as they were (a property or a partition spec changed). Without a position
    /// there is nothing to go on after, and what the stream said is the answer.
    async fn reopen(&self, why: String) -> Result<TableStream> {
        let read = self.stream.table();
        let now = self.on.table_at(read, LATEST).await?;
        let Some(position) = self.stream.position() else {
            return Err(metalake::Error::Unsupported(why).into());
        };
        let refused = not_additive(read.fields(), now.fields());
        if !refused.is_empty() {
            return Err(Error::failed(format!(
                "{} changed its columns after commit {position}: {}; follow it again after {position}",
                read.name(),
                refused.join(", ")
            )));
        }
        Ok(self.on.stream(&now, Start::After(position)).await?)
    }
}

/// A stream of `table` on `on` from `start`.
async fn open(on: &Ref, table: &Table, start: StartAt) -> metalake::Result<TableStream> {
    match start {
        StartAt::Snapshot => on.stream(table, Start::Snapshot).await,
        StartAt::Latest => on.stream(table, Start::Latest).await,
        StartAt::After(seq) => on.stream(table, Start::After(seq)).await,
        StartAt::Time(time) => on.as_of(time).stream(table, Start::Latest).await,
    }
}

/// How `now` changes the columns of `before` other than by adding ones that may be null: each field is known by its id,
/// and the ones it keeps keep their order.
fn not_additive(before: &[Field], now: &[Field]) -> Vec<String> {
    let mut refused = Vec::new();
    for old in before {
        match now.iter().find(|f| f.field_id == old.field_id) {
            None => refused.push(format!("{} dropped", old.name)),
            Some(new) => {
                if new.name != old.name {
                    refused.push(format!("{} renamed to {}", old.name, new.name));
                }
                if new.ty != old.ty {
                    refused.push(format!("{} became {}", old.name, new.ty));
                }
                if new.parent_field_id != old.parent_field_id {
                    refused.push(format!("{} moved", old.name));
                }
                if new.required && !old.required {
                    refused.push(format!("{} became required", old.name));
                }
            }
        }
    }
    let order = |fields: &[Field]| {
        let mut kept: Vec<&Field> = fields
            .iter()
            .filter(|f| {
                before.iter().any(|old| old.field_id == f.field_id) && now.iter().any(|new| new.field_id == f.field_id)
            })
            .collect();
        kept.sort_by_key(|f| (f.parent_field_id, f.position));
        kept.iter().map(|f| f.field_id).collect::<Vec<_>>()
    };
    if order(before) != order(now) {
        refused.push("columns reordered".to_string());
    }
    let added = now
        .iter()
        .filter(|f| before.iter().all(|old| old.field_id != f.field_id));
    refused.extend(
        added
            .filter(|f| f.required)
            .map(|f| format!("{} added as required", f.name)),
    );
    refused
}

impl Lake {
    /// A reader of a table's changes on its ref (CHG-4, CHG-5).
    pub async fn follow(&self, path: &TablePath, start: StartAt) -> Result<Follower> {
        let (on, table) = self.table(path).await?;
        let storage = self.storage()?;
        let stream = open(&on, &table, start).await?;
        Ok(Follower {
            stream,
            storage,
            on,
            start,
            before: None,
            read: Arc::new(AtomicBool::new(true)),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn field(field_id: i32, name: &str, ty: &str, required: bool) -> Field {
        Field {
            field_id,
            parent_field_id: None,
            position: field_id,
            name: name.into(),
            ty: ty.into(),
            required,
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    #[test]
    fn additive_change_accepted() {
        let before = [field(1, "k", "long", true)];
        assert!(not_additive(&before, &before).is_empty(), "nothing changed");
        let wider = [field(1, "k", "long", true), field(2, "v", "string", false)];
        assert!(not_additive(&before, &wider).is_empty());
        let mut relaxed = wider.clone();
        relaxed[0].required = false;
        assert!(
            not_additive(&before, &relaxed).is_empty(),
            "a column may become nullable"
        );
    }

    #[test]
    fn other_changes_refused() {
        let before = [field(1, "k", "long", false), field(2, "v", "string", false)];
        let mut now = [field(1, "key", "int", true), field(3, "w", "string", true)];
        assert_eq!(
            not_additive(&before, &now),
            [
                "k renamed to key",
                "k became int",
                "k became required",
                "v dropped",
                "w added as required"
            ]
        );
        now[0] = field(1, "k", "long", false);
        now[0].parent_field_id = Some(9);
        assert!(not_additive(&before, &now).contains(&"k moved".to_string()));

        let mut swapped = before.clone();
        (swapped[0].position, swapped[1].position) = (2, 1);
        assert_eq!(not_additive(&before, &swapped), ["columns reordered"]);
        let mut wider = before.to_vec();
        wider.insert(1, field(3, "w", "string", false));
        (wider[1].position, wider[2].position) = (2, 3);
        assert!(
            not_additive(&before, &wider).is_empty(),
            "a column added between keeps the order"
        );
    }
}
