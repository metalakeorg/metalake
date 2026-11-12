//! What a caller wants said in the commit of a statement, and what came of it. A sink is made by DataFusion from the SQL
//! and knows nothing of its caller, but it runs with the context of its execution, so this travels there, for that one
//! execution: two threads of one session do not meet.

use std::any::Any;
use std::sync::Mutex;

use datafusion::common::DataFusionError;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use metalake::{Outcome, Result, Transaction};

/// What a caller wants said in a statement's commit, as options of the session's configuration: text that travels
/// wherever the configuration does, to a Ballista scheduler and its executors, where `CommitDetails`, an object in
/// memory, does not. SQL sets them too: `set metalake.author = 'ana'`.
///
/// Not DataFusion's `extensions_options!`: the entries it lists are named without their prefix (`author`), and another
/// process that sets them back by those names finds no such option and drops them. These are named in full.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitOptions {
    /// Who commits.
    pub author: Option<String>,
    /// What the commit says.
    pub message: Option<String>,
    /// A writer's id: with `app_version`, a commit that applies once (CMT-8).
    pub app: Option<String>,
    /// A version `app` never repeats for different data: positive (CMT-8).
    pub app_version: Option<u64>,
}

impl ConfigExtension for CommitOptions {
    const PREFIX: &'static str = "metalake";
}

impl ExtensionOptions for CommitOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// `key` without the prefix, as DataFusion hands it on.
    fn set(&mut self, key: &str, value: &str) -> datafusion::common::Result<()> {
        let text = || Some(value.to_string());
        match key {
            "author" => self.author = text(),
            "message" => self.message = text(),
            "app" => self.app = text(),
            "app_version" => {
                let version = value
                    .parse()
                    .map_err(|e| DataFusionError::Configuration(format!("metalake.app_version {value}: {e}")))?;
                self.app_version = Some(version);
            }
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "metalake.{key}: no such option of a commit"
                )));
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        let entry = |name: &str, value: Option<String>, description| ConfigEntry {
            key: format!("{}.{name}", Self::PREFIX),
            value,
            description,
        };
        vec![
            entry("author", self.author.clone(), "Who commits."),
            entry("message", self.message.clone(), "What the commit says."),
            entry(
                "app",
                self.app.clone(),
                "A writer's id, for a commit that applies once.",
            ),
            entry(
                "app_version",
                self.app_version.map(|v| v.to_string()),
                "A version the writer never repeats for different data.",
            ),
        ]
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// What one process lists, another sets back by name, as Ballista carries a job's configuration.
    #[test]
    fn options_entries_round_trip() {
        let sent = CommitOptions {
            author: Some("ana".into()),
            message: Some("two rows".into()),
            app: Some("loader".into()),
            app_version: Some(7),
        };
        let mut here = CommitOptions::register(SessionConfig::new());
        here.options_mut().extensions.insert(sent.clone());
        let mut there = CommitOptions::register(SessionConfig::new());
        for entry in here.options().entries() {
            if let (true, Some(value)) = (entry.key.starts_with("metalake."), entry.value) {
                there.options_mut().set(&entry.key, &value).unwrap();
            }
        }
        assert_eq!(there.options().extensions.get::<CommitOptions>(), Some(&sent));
    }
}
