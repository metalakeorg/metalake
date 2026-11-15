//! The API over a lake: metalake-client's `Lake` is the [`Backend`], so that the UI calls what the command line and
//! Python call, and only translates its answers (ADR 0007 §3).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef, UInt64Type};
use datafusion::arrow::ipc::writer::StreamWriter;
use futures_util::{StreamExt, TryStreamExt};
use metalake_client::{Lake, Prepared, Session, SourcedStep, Step};

use crate::api::{
    Answer, Backend, Column, Commit, Fault, IcebergState, Kind, Mark, Marks, Named, Ref, RefDetails, Rows,
    SchemaDetails, SchemaSummary, Size, Snapshot, Statement, Status, StepAnswer, StepAnswers, StepOutcome, Table,
    TableDetails, TableSummary,
};

/// A run's state between its statements: the session they share, those left, and whether one failed.
struct Run {
    session: Session,
    steps: VecDeque<SourcedStep>,
    index: usize,
    failed: bool,
    limit: usize,
}

/// One statement of a run, run and answered: a read's first rows; what ran, by what it answered.
async fn answer(session: &Session, step: Step, limit: usize) -> Result<StepOutcome, Fault> {
    let prepared = session.step(step).await?;
    if prepared.is_read() {
        return Ok(StepOutcome::Rows(first_rows(&prepared, limit).await?));
    }
    let batches: Vec<RecordBatch> = prepared
        .read()?
        .try_collect()
        .await
        .map_err(metalake_client::Error::from)?;
    let schema = prepared.schema();
    // A write answers one row: its commit, none when it changed nothing, and its count.
    if let (Ok(commit), Ok(count)) = (schema.index_of("commit"), schema.index_of("count"))
        && let Some(row) = batches.iter().find(|b| b.num_rows() > 0)
    {
        let commit = row.column(commit).as_primitive::<Int64Type>();
        return Ok(StepOutcome::Written {
            commit: (!commit.is_null(0)).then(|| commit.value(0)),
            rows: row.column(count).as_primitive::<UInt64Type>().value(0),
        });
    }
    if batches.iter().any(|b| b.num_rows() > 0) {
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        return Ok(StepOutcome::Rows(Rows {
            arrow: arrow_of(&schema, &batches)?,
            rows,
            truncated: false,
        }));
    }
    Ok(StepOutcome::Done)
}

/// A statement's first rows, at most `limit`: read until the limit, then one batch more, to tell whether there was
/// more. What is not read is not run.
async fn first_rows(prepared: &Prepared, limit: usize) -> Result<Rows, Fault> {
    let schema = prepared.schema();
    let mut stream = prepared.read()?;
    let (mut batches, mut rows, mut truncated) = (Vec::<RecordBatch>::new(), 0, false);
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(metalake_client::Error::from)?;
        if batch.num_rows() == 0 {
            continue;
        }
        if rows == limit {
            truncated = true;
            break;
        }
        let taken = batch.num_rows().min(limit - rows);
        truncated = taken < batch.num_rows();
        batches.push(batch.slice(0, taken));
        rows += taken;
        if truncated {
            break;
        }
    }
    drop(stream);
    Ok(Rows {
        arrow: arrow_of(&schema, &batches)?,
        rows,
        truncated,
    })
}

/// Rows as an Arrow IPC stream: their schema, then their batches.
fn arrow_of(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, Fault> {
    let mut arrow = Vec::new();
    let written = (|| {
        let mut writer = StreamWriter::try_new(&mut arrow, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()
    })();
    written.map_err(|e| Fault::new(Kind::Failed, format!("the rows could not be written as Arrow: {e}")))?;
    Ok(arrow)
}

/// How often the lake's mark is read again besides its notifications: what tells a deleted ref, which notifies
/// nothing, and a change while the connection that listens was lost.
const EVERY: Duration = Duration::from_secs(5);

impl Backend for Lake {
    fn status(&self) -> Answer<'_, Status> {
        Box::pin(async move {
            let status = Lake::status(self).await?;
            Ok(Status {
                postgres: status.postgres,
                local: status.local,
                schema: status.schema,
                warehouse: status.warehouse,
                format_version: status.format_version,
                writer_version: status.writer_version,
                protocol_version: status.protocol_version,
                refs: status
                    .refs
                    .into_iter()
                    .map(|r| Named {
                        name: r.name,
                        kind: r.kind,
                    })
                    .collect(),
            })
        })
    }

    fn refs(&self) -> Answer<'_, Vec<Ref>> {
        Box::pin(async move {
            let refs = Lake::refs(self, None).await?;
            Ok(refs
                .into_iter()
                .map(|r| Ref {
                    name: r.name,
                    kind: r.kind,
                    parent: r.parent,
                    fork_seq: r.fork_seq,
                    created_seq: r.created_seq,
                    protected: r.protected,
                })
                .collect())
        })
    }

    fn tables<'a>(&'a self, on: &'a str) -> Answer<'a, Vec<Table>> {
        Box::pin(async move {
            let tables = Lake::tables(self, on).await?;
            Ok(tables
                .into_iter()
                .map(|t| Table {
                    namespace: t.namespace,
                    name: t.name,
                })
                .collect())
        })
    }

    fn watch(&self) -> Answer<'_, Marks> {
        Box::pin(async move {
            let watch = Lake::watch(self, EVERY).await?;
            // Each mark as it comes; the first that fails is the last.
            let marks = futures_util::stream::unfold(Some(watch), |watch| async move {
                let mut watch = watch?;
                match watch.next().await {
                    Ok(mark) => Some((
                        Ok(Mark {
                            seq: mark.seq,
                            recent: mark.recent,
                            deleted: mark.deleted,
                        }),
                        Some(watch),
                    )),
                    Err(e) => Some((Err(Fault::from(metalake_client::Error::from(e))), None)),
                }
            });
            Ok(Box::pin(marks) as Marks)
        })
    }

    fn sql(&self, statement: Statement) -> Answer<'_, Rows> {
        Box::pin(async move {
            let limit = statement.limit();
            // A session of its own, as each `metalake sql` has: it sees the catalog as it is now.
            let session = Session::new(self).await?;
            if let Some(branch) = &statement.branch {
                session.on_ref(branch);
            }
            let prepared = session.run(&statement.sql).await?;
            first_rows(&prepared, limit).await
        })
    }

    fn run(&self, statement: Statement) -> Answer<'_, StepAnswers> {
        Box::pin(async move {
            let limit = statement.limit();
            // A text that cannot be split fails as a whole, before any statement runs.
            let steps: VecDeque<SourcedStep> = Session::sourced_steps(&statement.sql)?.into();
            if steps.is_empty() {
                return Err(Fault::new(Kind::Usage, "no statement to run"));
            }
            // One session for the run, as a SQL editor's: what a statement sets, the next sees.
            let session = Session::new(self).await?;
            if let Some(branch) = &statement.branch {
                session.on_ref(branch);
            }
            let run = Run {
                session,
                steps,
                index: 0,
                failed: false,
                limit,
            };
            let answers = futures_util::stream::unfold(run, |mut run| async move {
                let sourced = run.steps.pop_front()?;
                let index = run.index;
                run.index += 1;
                let sql = sourced.text;
                if run.failed {
                    let outcome = StepOutcome::Skipped;
                    return Some((
                        StepAnswer {
                            index,
                            sql,
                            ms: None,
                            outcome,
                        },
                        run,
                    ));
                }
                let started = Instant::now();
                let outcome = match answer(&run.session, sourced.step, run.limit).await {
                    Ok(outcome) => outcome,
                    Err(fault) => {
                        run.failed = true;
                        StepOutcome::Failed(fault)
                    }
                };
                let ms = Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
                Some((
                    StepAnswer {
                        index,
                        sql,
                        ms,
                        outcome,
                    },
                    run,
                ))
            });
            Ok(Box::pin(answers) as StepAnswers)
        })
    }

    fn ref_details<'a>(&'a self, on: &'a str) -> Answer<'a, RefDetails> {
        Box::pin(async move {
            let d = Lake::ref_details(self, on).await?;
            Ok(RefDetails {
                r#ref: Ref {
                    name: d.info.name,
                    kind: d.info.kind,
                    parent: d.info.parent,
                    fork_seq: d.info.fork_seq,
                    created_seq: d.info.created_seq,
                    protected: d.info.protected,
                },
                warehouse: d.warehouse,
                size: size(d.size),
                schemas: d
                    .schemas
                    .into_iter()
                    .map(|s| SchemaSummary {
                        namespace: s.namespace,
                        size: size(s.size),
                    })
                    .collect(),
            })
        })
    }

    fn commits<'a>(&'a self, on: &'a str, limit: i64) -> Answer<'a, Vec<Commit>> {
        Box::pin(async move {
            let commits = Lake::commits(self, on, limit).await?;
            Ok(commits
                .into_iter()
                .map(|c| Commit {
                    seq: c.commit_seq,
                    committed_at: c.committed_at,
                    author: c.author,
                    operation: c.operation,
                    message: c.message,
                })
                .collect())
        })
    }

    fn schema_details<'a>(&'a self, on: &'a str, schema: &'a [String]) -> Answer<'a, SchemaDetails> {
        Box::pin(async move {
            let d = Lake::schema_details(self, on, schema).await?;
            Ok(SchemaDetails {
                namespace: d.namespace,
                properties: d.properties,
                size: size(d.size),
                tables: d
                    .tables
                    .into_iter()
                    .map(|t| TableSummary {
                        name: t.name,
                        rows: t.rows,
                        bytes: t.bytes,
                        changed_at: t.changed_at,
                    })
                    .collect(),
            })
        })
    }

    fn table_details<'a>(&'a self, on: &'a str, schema: &'a [String], table: &'a str) -> Answer<'a, TableDetails> {
        Box::pin(async move {
            let d = Lake::table_details(self, on, schema, table).await?;
            Ok(TableDetails {
                namespace: d.namespace,
                name: d.name,
                id: d.id,
                location: d.location,
                columns: d
                    .columns
                    .into_iter()
                    .map(|c| Column {
                        id: c.id,
                        parent: c.parent,
                        name: c.name,
                        r#type: c.r#type,
                        required: c.required,
                        doc: c.doc,
                        default: c.default,
                    })
                    .collect(),
                rows: d.size.rows,
                bytes: d.size.bytes,
                files: d.size.files,
                inline_files: d.size.inline_files,
                deleted_rows: d.size.deleted_rows,
                partition: d.partition,
                sort_order: d.sort_order,
                cluster_key: d.cluster_key,
                properties: d.properties,
                iceberg: d.iceberg.map(|i| IcebergState {
                    head: i.head,
                    written: i.written,
                    behind: i.behind,
                    since: i.since,
                    metadata: i.metadata,
                }),
            })
        })
    }

    fn snapshots<'a>(
        &'a self,
        on: &'a str,
        schema: &'a [String],
        table: &'a str,
        limit: i64,
    ) -> Answer<'a, Vec<Snapshot>> {
        Box::pin(async move {
            let snapshots = Lake::table_snapshots(self, on, schema, table, limit).await?;
            Ok(snapshots
                .into_iter()
                .map(|s| Snapshot {
                    id: s.snapshot_id,
                    parent: s.parent_snapshot_id,
                    operation: s.operation,
                    committed_at: s.committed_at,
                })
                .collect())
        })
    }
}

/// What the client says something holds, as the API says it.
fn size(s: metalake_client::Size) -> Size {
    Size {
        tables: s.tables,
        rows: s.rows,
        bytes: s.bytes,
    }
}

impl From<metalake_client::Error> for Fault {
    fn from(e: metalake_client::Error) -> Self {
        Fault {
            kind: match e.kind {
                metalake_client::Kind::Usage => Kind::Usage,
                metalake_client::Kind::Failed => Kind::Failed,
                metalake_client::Kind::Conflict => Kind::Conflict,
                metalake_client::Kind::Unknown => Kind::Unknown,
            },
            message: e.message,
            rule: e.rule,
            code: e.code,
            retryable: e.retryable,
        }
    }
}
