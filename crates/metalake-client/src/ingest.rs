//! Writing a stream into a table from one producer, as rows come. A writer gathers them and commits on its own cadence:
//! a commit each `commit_every`, or sooner when what it holds reaches `max_bytes`. With a key, each commit is idempotent
//! (CMT-5) with the highest version the producer gave its rows, so that a producer that fell over asks the writer where
//! it was and gives again what came after; what it gives again of before is skipped. A version is a unit of the
//! producer's, as a Kafka offset or a step a follower took: several writes may give one, and a commit never splits it.
//!
//! A writer has no thread of its own: it looks at its cadence when rows are given, and rows that wait while a producer
//! is quiet go with the next ones, a flush, or its close. A key is one producer's: two writers with one key would each
//! skip the other's versions, and the second to commit is refused.

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use futures_util::stream;

use crate::write::{Details, TablePath, Written};
use crate::{Error, Kind, Lake, Result, Session};

/// How rows go into the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    Append,
    /// By these columns: a row replaces the one with its key, and of rows with one key in a commit the last one given
    /// stays.
    Upsert(Vec<String>),
}

/// How a writer commits.
#[derive(Debug, Clone)]
pub struct WriterOptions {
    /// The producer's idempotency key. Without one, a commit that is sent again applies again.
    pub app_id: Option<String>,
    pub mode: Mode,
    /// The longest rows wait for their commit, once a write looks: the horizon a follower sees commits at (CHG-1).
    pub commit_every: Duration,
    /// What it holds before it commits, measured as Arrow holds it in memory.
    pub max_bytes: usize,
    pub author: Option<String>,
    pub message: Option<String>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            app_id: None,
            mode: Mode::Append,
            commit_every: Duration::from_secs(1),
            max_bytes: 64 << 20,
            author: None,
            message: None,
        }
    }
}

/// What a write did.
#[derive(Debug)]
pub enum Wrote {
    /// It holds the rows for its next commit.
    Buffered,
    /// It committed them, with what it held.
    Committed(Written),
    /// Its version is at or below the key's: the table already has them.
    Skipped,
}

/// A producer's writer into one table.
pub struct Writer {
    lake: Arc<Lake>,
    session: Option<Session>,
    path: TablePath,
    options: WriterOptions,
    held: Vec<RecordBatch>,
    bytes: usize,
    since: Option<Instant>,
    /// The version the key reached in the table.
    version: Option<i64>,
    /// The highest version it holds rows of.
    pending: Option<i64>,
}

impl Lake {
    /// A writer into a table, which knows where its key is.
    pub async fn writer(self: &Arc<Self>, path: &TablePath, options: WriterOptions) -> Result<Writer> {
        let (on, table) = self.table(path).await?;
        on.writable()?;
        let session = match &options.mode {
            Mode::Append => None,
            Mode::Upsert(keys) => {
                if keys.is_empty() {
                    return Err(Error::usage("an upsert needs the columns of its key"));
                }
                if let Some(unknown) = keys.iter().find(|k| table.schema().column_with_name(k).is_none()) {
                    return Err(Error::usage(format!("{unknown} is not a column of {path}")));
                }
                Some(Session::new(self).await?)
            }
        };
        let version = match &options.app_id {
            Some(app_id) => on.key_version(app_id).await?,
            None => None,
        };
        Ok(Writer {
            lake: self.clone(),
            session,
            path: path.clone(),
            options,
            held: Vec::new(),
            bytes: 0,
            since: None,
            version,
            pending: None,
        })
    }
}

impl Writer {
    /// The version the key reached in the table: a producer goes on with what comes after it. None without a key, or
    /// before its first commit.
    pub fn version(&self) -> Option<i64> {
        self.version
    }

    /// Takes rows, with the producer's version of them when it has a key. A version is a unit of the producer's: several
    /// writes may give one, versions never go back, and a commit never splits one. Without a key it commits once its
    /// cadence says, with these rows; with one, once its cadence says and another version comes, before its rows, so
    /// that what it commits is whole.
    pub async fn write(&mut self, batch: RecordBatch, version: Option<i64>) -> Result<Wrote> {
        match (&self.options.app_id, version) {
            (Some(_), None) => return Err(Error::usage("a writer with a key needs the version of each write")),
            (None, Some(_)) => return Err(Error::usage("a version means nothing to a writer without a key")),
            (Some(_), Some(v)) if self.version.is_some_and(|stored| v <= stored) => return Ok(Wrote::Skipped),
            (Some(_), Some(v)) if self.pending.is_some_and(|held| v < held) => {
                return Err(Error::usage(format!(
                    "version {v} after {}: the versions of a writer do not go back",
                    self.pending.unwrap_or_default()
                )));
            }
            _ => {}
        }
        if let Some(first) = self.held.first()
            && first.schema() != batch.schema()
        {
            return Err(Error::usage("the rows of one writer have one schema"));
        }
        // No rows give no version: an empty write must not lend its version to rows it holds of an earlier one.
        if batch.num_rows() == 0 {
            return Ok(Wrote::Buffered);
        }
        let mut committed = None;
        if version.is_some() && version != self.pending && self.due() {
            committed = self.flush().await?;
        }
        if version.is_some() {
            self.pending = version;
        }
        self.bytes += batch.get_array_memory_size();
        self.held.push(batch);
        self.since.get_or_insert_with(Instant::now);
        if version.is_none() && self.due() {
            committed = self.flush().await?;
        }
        Ok(match committed {
            Some(written) => Wrote::Committed(written),
            None => Wrote::Buffered,
        })
    }

    /// Whether its cadence says to commit what it holds.
    fn due(&self) -> bool {
        self.bytes >= self.options.max_bytes
            || self
                .since
                .is_some_and(|since| since.elapsed() >= self.options.commit_every)
    }

    /// Commits what it holds, if anything: with a key, a caller flushes between versions. A commit that fails keeps the
    /// rows, and is sent again by the next one: with a key, a commit that had applied after all, its answer lost, is
    /// found by the key standing at its version, and is not sent again.
    ///
    /// With a key, the key is read first: if it stands neither where this writer left it nor at the version it holds,
    /// another writer has it, and nothing is committed. A writer that takes the key between that read and the commit is
    /// not seen; one key per producer is what makes that impossible, as the protocol does not compare a key's version
    /// with an expected one.
    pub async fn flush(&mut self) -> Result<Option<Written>> {
        if self.held.is_empty() {
            return Ok(None);
        }
        if let Some(app_id) = &self.options.app_id {
            let stored = self.lake.key_version(&self.path.on, app_id).await?;
            if stored == self.pending && self.pending > self.version {
                return Ok(Some(self.applied_before().await?));
            }
            if stored != self.version {
                return Err(self.taken());
            }
        }
        let details = Details {
            author: self.options.author.clone(),
            message: self.options.message.clone(),
            idempotent: self.options.app_id.clone().zip(self.pending),
        };
        let given = self.held[0].schema();
        let written = match (&self.options.mode, &self.session) {
            (Mode::Upsert(keys), Some(session)) => {
                let rows = last_of_each_key(&given, &self.held, keys)?;
                let rows = stream::iter([Ok(rows)]);
                self.lake
                    .upsert(session, &self.path, &given, rows, keys, details)
                    .await?
            }
            _ => {
                let rows = stream::iter(self.held.clone().into_iter().map(Ok));
                self.lake.append(&self.path, &given, rows, details).await?
            }
        };
        // Rows that changed nothing made no commit, and say nothing of the key.
        if let Some(app_id) = &self.options.app_id
            && !written.applied
            && written.commit.is_some()
        {
            // The key was at this version or past it already: an earlier send of these rows, or another writer.
            if self.lake.key_version(&self.path.on, app_id).await? != self.pending {
                return Err(self.taken());
            }
            return Ok(Some(self.applied_before().await?));
        }
        if written.applied {
            self.version = self.pending.or(self.version);
        }
        self.clear();
        Ok(Some(written))
    }

    /// What an earlier send of the rows it holds committed, its answer lost: the key stands at their version.
    async fn applied_before(&mut self) -> Result<Written> {
        let app_id = self.options.app_id.as_deref().unwrap_or_default();
        let on = self.lake.find_ref(&self.path.on).await?;
        let commit = on.commit_of_key(app_id).await?;
        let rows = self.held.iter().map(|b| b.num_rows() as u64).sum();
        self.version = self.pending;
        self.clear();
        Ok(Written {
            commit,
            rows,
            applied: false,
            inserted: 0,
            updated: 0,
        })
    }

    /// Another writer has the key: what it holds is not this writer's to commit.
    fn taken(&mut self) -> Error {
        self.clear();
        Error::new(
            Kind::Conflict,
            format!(
                "the key {} on {} is not where this writer left it: another writer has it",
                self.options.app_id.as_deref().unwrap_or_default(),
                self.path.on
            ),
        )
    }

    /// Commits what it holds, and ends.
    pub async fn close(mut self) -> Result<Option<Written>> {
        self.flush().await
    }

    fn clear(&mut self) {
        self.held.clear();
        self.bytes = 0;
        self.since = None;
    }
}

/// The rows held, with one row per key: the last one given of each.
fn last_of_each_key(schema: &SchemaRef, held: &[RecordBatch], keys: &[String]) -> Result<RecordBatch> {
    let all = concat_batches(schema, held)?;
    let columns = keys
        .iter()
        .map(|k| {
            all.column_by_name(k)
                .cloned()
                .ok_or_else(|| Error::usage(format!("{k} is not a column of the rows")))
        })
        .collect::<Result<Vec<_>>>()?;
    let fields = columns.iter().map(|c| SortField::new(c.data_type().clone())).collect();
    let rows = RowConverter::new(fields)?.convert_columns(&columns)?;
    let mut seen = std::collections::HashSet::new();
    let mut kept: Vec<u32> = (0..all.num_rows() as u32)
        .rev()
        .filter(|i| seen.insert(rows.row(*i as usize)))
        .collect();
    kept.reverse();
    Ok(take_record_batch(&all, &UInt32Array::from(kept))?)
}
