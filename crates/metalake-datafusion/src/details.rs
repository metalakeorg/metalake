//! What a caller wants said in the commit of a statement, and what came of it. A sink is made by DataFusion from the SQL
//! and knows nothing of its caller, but it runs with the context of its execution, so this travels there, for that one
//! execution: two threads of one session do not meet.

use std::sync::Mutex;

use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use metalake::{Outcome, Result, Transaction};

extensions_options! {
    /// What a caller wants said in a statement's commit, as options of the session's configuration: text that
    /// travels wherever the configuration does, to a Ballista scheduler and its executors, where `CommitDetails`, an
    /// object in memory, does not. SQL sets them too: `set metalake.author = 'ana'`.
    pub struct CommitOptions {
        /// Who commits.
        pub author: Option<String>, default = None
        /// What the commit says.
        pub message: Option<String>, default = None
        /// A writer's id: with `app_version`, a commit that applies once (CMT-8).
        pub app: Option<String>, default = None
        /// A version `app` never repeats for different data: positive (CMT-8).
        pub app_version: Option<u64>, default = None
    }
}

impl ConfigExtension for CommitOptions {
    const PREFIX: &'static str = "metalake";
}

impl CommitOptions {
    /// `config`, able to hold MetaLake's commit options: their keys known, so that a value set by name, or sent by
    /// another process, is taken and not dropped.
    pub fn register(mut config: SessionConfig) -> SessionConfig {
        if config.options().extensions.get::<CommitOptions>().is_none() {
            config.options_mut().extensions.insert(CommitOptions::default());
        }
        config
    }

    /// `config` with the options `details` asks for.
    pub fn of(details: &CommitDetails, mut config: SessionConfig) -> SessionConfig {
        let options = CommitOptions {
            author: details.author.clone(),
            message: details.message.clone(),
            app: details.idempotent.as_ref().map(|(app, _)| app.clone()),
            // A version is positive (CMT-8): one that is not is sent as 0, for the protocol to refuse, not dropped.
            app_version: details
                .idempotent
                .as_ref()
                .map(|(_, version)| u64::try_from(*version).unwrap_or(0)),
        };
        config.options_mut().extensions.insert(options);
        config
    }
}

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
    if let Some(options) = context.session_config().options().extensions.get::<CommitOptions>() {
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
