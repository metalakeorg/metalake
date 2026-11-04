//! What a catalog says of itself in `meta` (LAY-2): its versions, whether this SDK may read or write it (LAY-3,
//! LAY-5), and its warehouse.

use tokio_postgres::GenericClient;

use crate::{Error, ErrorKind, Result, Session, quote_ident};

/// The format this SDK reads and writes.
pub const FORMAT_VERSION: &str = "0";
/// The newest writer version this SDK writes (revision 0.2). A catalog with a newer one is read, never written.
pub const WRITER_VERSION: u32 = 2;
/// The protocol these sources install.
pub const PROTOCOL_VERSION: &str = "0";

/// The three versions of a catalog, as `versions()` answers them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versions {
    pub format_version: String,
    pub writer_version: String,
    pub protocol_version: Option<String>,
}

impl Versions {
    /// Whether this SDK may write the catalog: a writer version it knows (LAY-5).
    pub fn writable(&self) -> bool {
        self.writer_version.parse::<u32>().is_ok_and(|v| v <= WRITER_VERSION)
    }
}

/// The versions of the catalog in `schema`.
pub async fn versions(session: &impl Session, schema: &str) -> Result<Versions> {
    let row = session
        .client()
        .query_one(&format!("select * from {}.versions()", quote_ident(schema)), &[])
        .await?;
    Ok(Versions {
        format_version: row.get("format_version"),
        writer_version: row.get("writer_version"),
        protocol_version: row.get("protocol_version"),
    })
}

/// Whether a catalog's protocol is ahead of these sources. One that is not a number is.
pub(crate) fn newer(protocol: Option<&str>) -> bool {
    let known: u64 = PROTOCOL_VERSION.parse().expect("the protocol's version is a number");
    protocol.is_some_and(|v| v.parse::<u64>().map_or(true, |v| v > known))
}

/// Where the catalog in `schema` puts the files of a table that names no location (LAY-2), if it says.
pub async fn warehouse(session: &impl Session, schema: &str) -> Result<Option<String>> {
    let select = format!("select value from {}.meta where key = 'warehouse'", quote_ident(schema));
    let row = session.client().query_opt(&select, &[]).await?;
    Ok(row.map(|r| r.get(0)))
}

/// The versions of the catalog in `schema`, refused unless this SDK speaks them (LAY-3): the format it knows, and
/// the protocol it installs. A catalog installed by an older SDK is refused until it is migrated.
pub async fn check_versions(session: &impl Session, schema: &str) -> Result<Versions> {
    let found = versions(session, schema).await?;
    if found.format_version != FORMAT_VERSION {
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!("format_version {} is not {FORMAT_VERSION}", found.format_version),
        ));
    }
    if found.protocol_version.as_deref() != Some(PROTOCOL_VERSION) {
        let action = match newer(found.protocol_version.as_deref()) {
            true => "the catalog is ahead of this client: upgrade it",
            false => "run `metalake migrate`",
        };
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "protocol_version {:?} is not {PROTOCOL_VERSION}: {action}",
                found.protocol_version
            ),
        ));
    }
    Ok(found)
}
