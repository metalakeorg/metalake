//! The API over a lake: metalake-client's `Lake` is the [`Backend`], so that the UI calls what the command line and
//! Python call, and only translates its answers (ADR 0007 §3).

use metalake_client::Lake;

use crate::api::{Answer, Backend, Fault, Kind, Named, Status};

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
