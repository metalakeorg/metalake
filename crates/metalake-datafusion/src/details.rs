//! What a caller wants said in the commit of a statement, and what came of it. A sink is made by DataFusion from the SQL
//! and knows nothing of its caller, but it runs with the context of its execution, so this travels there, for that one
//! execution: two threads of one session do not meet.

use std::sync::Mutex;

use datafusion::execution::TaskContext;
use metalake::{Outcome, Result, Transaction};

#[derive(Debug, Default)]
pub struct CommitDetails {
    pub author: Option<String>,
    pub message: Option<String>,
    pub idempotent: Option<(String, i64)>,
    outcome: Mutex<Option<Outcome>>,
}

impl CommitDetails {
    pub fn new(author: Option<String>, message: Option<String>, idempotent: Option<(String, i64)>) -> Self {
        Self {
            author,
            message,
            idempotent,
            outcome: Mutex::default(),
        }
    }

    /// What the statement's commit did, if it had one: a statement that changed no row has none.
    pub fn outcome(&self) -> Option<Outcome> {
        *self.outcome.lock().expect("an outcome is only set")
    }
}

/// Commits `transaction` with what the execution asks for, if it asks.
pub(crate) async fn commit(mut transaction: Transaction, context: &TaskContext) -> Result<()> {
    let details = context.session_config().get_extension::<CommitDetails>();
    if let Some(details) = &details {
        if let Some(author) = &details.author {
            transaction = transaction.author(author);
        }
        if let Some(message) = &details.message {
            transaction = transaction.message(message);
        }
        if let Some((app, version)) = &details.idempotent {
            transaction = transaction.idempotent(app, *version);
        }
    }
    let outcome = transaction.commit().await?;
    if let Some(details) = &details {
        *details.outcome.lock().expect("an outcome is only set") = Some(outcome);
    }
    Ok(())
}
