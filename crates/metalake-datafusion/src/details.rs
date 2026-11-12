//! A statement's commit: what the caller asks of it, which comes in the options of the session, and what came of it,
//! which goes in a slot of the write's execution, where it runs. A sink is made by DataFusion from the SQL and knows
//! nothing of its caller, but it runs with the context of its execution, for that one execution: two threads of one
//! session do not meet.

use std::sync::Mutex;

use datafusion::execution::TaskContext;
use metalake::{Outcome, Result, Transaction};

use crate::options::MetaLakeOptions;

/// What came of a write's commit, where it runs: the slot `MetaWriteExec` hands its sink, read into the write's row.
#[derive(Debug, Default)]
pub(crate) struct CommitDetails {
    outcome: Mutex<Option<Outcome>>,
    merged: Mutex<(u64, u64)>,
}

impl CommitDetails {
    /// How many rows a merge inserted and how many it updated.
    pub(crate) fn merged(&self) -> (u64, u64) {
        *self.merged.lock().expect("a count is only set")
    }

    /// What the statement's commit did, if it had one: a statement that changed no row has none.
    pub(crate) fn outcome(&self) -> Option<Outcome> {
        *self.outcome.lock().expect("an outcome is only set")
    }
}

/// Says how many rows a merge inserted and how many it updated.
pub(crate) fn merged(context: &TaskContext, inserted: u64, updated: u64) {
    if let Some(details) = context.session_config().get_extension::<CommitDetails>() {
        *details.merged.lock().expect("a count is only set") = (inserted, updated);
    }
}

/// Commits `transaction` with what the execution asks for, if it asks.
pub(crate) async fn commit(mut transaction: Transaction, context: &TaskContext) -> Result<()> {
    // What is asked comes in the options, which reach any process; what came of it goes in the slot of the write
    // running here, which answers it as its row.
    if let Some(options) = context.session_config().options().extensions.get::<MetaLakeOptions>() {
        if let Some(author) = &options.author {
            transaction = transaction.author(author);
        }
        if let Some(message) = &options.message {
            transaction = transaction.message(message);
        }
        if let (Some(app), Some(version)) = (&options.app, options.app_version) {
            let version = i64::try_from(version)
                .map_err(|_| metalake::Error::Invalid(format!("{app}: version {version} is past a bigint")))?;
            transaction = transaction.idempotent(app, version);
        }
    }
    let details = context.session_config().get_extension::<CommitDetails>();
    let outcome = transaction.commit().await?;
    if let Some(details) = &details {
        *details.outcome.lock().expect("an outcome is only set") = Some(outcome);
    }
    Ok(())
}
