//! What a caller wants said in the commit of a statement, and what came of it. A sink is made by DataFusion from the SQL
//! and knows nothing of its caller, but it runs with the context of its execution, so this travels there, for that one
//! execution: two threads of one session do not meet.

use std::sync::Mutex;

use datafusion::execution::TaskContext;
use metalake::{Outcome, Result, Transaction};

use crate::options::MetaLakeOptions;

#[derive(Debug, Default)]
pub struct CommitDetails {
    pub author: Option<String>,
    pub message: Option<String>,
    pub idempotent: Option<(String, i64)>,
    outcome: Mutex<Option<Outcome>>,
    merged: Mutex<(u64, u64)>,
}

impl CommitDetails {
    pub fn new(author: Option<String>, message: Option<String>, idempotent: Option<(String, i64)>) -> Self {
        Self {
            author,
            message,
            idempotent,
            outcome: Mutex::default(),
            merged: Mutex::default(),
        }
    }

    /// How many rows a merge inserted and how many it updated.
    pub fn merged(&self) -> (u64, u64) {
        *self.merged.lock().expect("a count is only set")
    }

    /// What the statement's commit did, if it had one: a statement that changed no row has none.
    pub fn outcome(&self) -> Option<Outcome> {
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
    // What is asked comes in the options, which reach any process; what came of it goes back in memory, to the
    // process that asked, when it is this one.
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
