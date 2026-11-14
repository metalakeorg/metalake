//! What a catalog requires of the Postgres it is in: commit timestamps, which a commit's time is (TT-1). A commit made
//! where they are off has no time, and never will: a time is not invented. Postgres changes the setting only when it
//! restarts, which ends every connection, so what a connection sees when it opens holds for as long as it lives.

use tokio_postgres::GenericClient;

use crate::{Error, ErrorKind, Result};

/// Refuses a Postgres that keeps no commit timestamps (TT-1), saying how to turn them on.
pub async fn check_commit_time(client: &impl GenericClient) -> Result<()> {
    match commit_time_kept(client).await? {
        true => Ok(()),
        false => Err(no_commit_time()),
    }
}

/// Whether this Postgres keeps commit timestamps, which a commit's time is (TT-1).
pub async fn commit_time_kept(client: &impl GenericClient) -> Result<bool> {
    let row = client
        .query_one("select current_setting('track_commit_timestamp')", &[])
        .await?;
    Ok(row.get::<_, String>(0) == "on")
}

/// The refusal of a Postgres that keeps no commit timestamps, which says how to turn them on (TT-1).
pub fn no_commit_time() -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "this Postgres keeps no commit timestamps (track_commit_timestamp is off), and a commit's time is its \
         transaction's: turn it on and restart the server, then run this again. Cloud SQL: `gcloud sql instances patch \
         <instance> --database-flags track_commit_timestamp=on`, naming the instance's other flags too, as it replaces \
         them all; RDS and Aurora: in the instance's parameter group; Azure Database for PostgreSQL: in the server's \
         parameters; any other: in postgresql.conf",
    )
    .with_rule("TT-1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_commit_time_says_how_to_turn_it_on() {
        let refused = no_commit_time();
        assert_eq!(refused.kind(), ErrorKind::Unsupported);
        let said = refused.to_string();
        assert!(
            said.starts_with("Unsupported (TT-1): this Postgres keeps no commit timestamps"),
            "{said}"
        );
        for remedy in [
            "track_commit_timestamp=on",
            "parameter group",
            "server's parameters",
            "postgresql.conf",
        ] {
            assert!(said.contains(remedy), "{remedy}: {said}");
        }
    }
}
