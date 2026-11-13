//! The API over a lake: metalake-client's `Lake` is the [`Backend`], so that the UI calls what the command line and
//! Python call, and only translates its answers (ADR 0007 §3).

use std::time::Duration;

use metalake_client::Lake;

use crate::api::{Answer, Backend, Fault, Kind, Mark, Marks, Named, Ref, Status, Table};

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
