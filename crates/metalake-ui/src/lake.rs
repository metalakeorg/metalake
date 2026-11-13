//! The API over a lake: metalake-client's `Lake` is the [`Backend`], so that the UI calls what the command line and
//! Python call, and only translates its answers (ADR 0007 §3).

use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use futures_util::StreamExt;
use metalake_client::{Lake, Session};

use crate::api::{Answer, Backend, Fault, Kind, Mark, Marks, Named, Ref, Rows, Statement, Status, Table};

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
            let schema = prepared.schema();
            let mut stream = prepared.read()?;
            let (mut batches, mut rows, mut truncated) = (Vec::<RecordBatch>::new(), 0, false);
            // Read until the limit; then one batch more, to tell whether there was more. What is not read is not run.
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
            let mut arrow = Vec::new();
            let written = (|| {
                let mut writer = StreamWriter::try_new(&mut arrow, &schema)?;
                for batch in &batches {
                    writer.write(batch)?;
                }
                writer.finish()
            })();
            written.map_err(|e| Fault::new(Kind::Failed, format!("the rows could not be written as Arrow: {e}")))?;
            Ok(Rows { arrow, rows, truncated })
        })
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
