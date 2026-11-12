//! MetaLake's options of a session, as DataFusion's configuration holds them: what a statement's commit says, and where
//! a session's statements run. Text, so that they travel wherever the configuration does, to a Ballista scheduler and
//! its executors. SQL sets them too: `set metalake.author = 'ana'`, `set metalake.engine = 'local'`.
//!
//! One type for all: DataFusion keeps one extension per prefix, and these share `metalake`.

use std::any::Any;
use std::fmt;
use std::str::FromStr;

use datafusion::common::DataFusionError;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::prelude::SessionConfig;

/// Where a session's statements run, in a session that can reach a cluster. One that cannot runs them where it is,
/// whatever this says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// In the session's own process.
    Local,
    /// On the cluster the session reaches.
    Cluster,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Engine::Local => "local",
            Engine::Cluster => "cluster",
        })
    }
}

impl FromStr for Engine {
    type Err = DataFusionError;

    fn from_str(value: &str) -> datafusion::common::Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "local" => Ok(Engine::Local),
            "cluster" => Ok(Engine::Cluster),
            _ => Err(DataFusionError::Configuration(format!(
                "metalake.engine {value}: one of local, cluster"
            ))),
        }
    }
}

/// MetaLake's options of a session.
///
/// Not DataFusion's `extensions_options!`: the entries it lists are named without their prefix (`author`), and another
/// process that sets them back by those names finds no such option and drops them. These are named in full.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetaLakeOptions {
    /// Who commits.
    pub author: Option<String>,
    /// What the commit says.
    pub message: Option<String>,
    /// A writer's id: with `app_version`, a commit that applies once (CMT-8).
    pub app: Option<String>,
    /// A version `app` never repeats for different data: positive (CMT-8).
    pub app_version: Option<u64>,
    /// Where statements run; none, where the session runs them by default.
    pub engine: Option<Engine>,
}

impl ConfigExtension for MetaLakeOptions {
    const PREFIX: &'static str = "metalake";
}

impl ExtensionOptions for MetaLakeOptions {
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
            "engine" => self.engine = Some(value.parse()?),
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "metalake.{key}: no such option"
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
            entry(
                "engine",
                self.engine.map(|e| e.to_string()),
                "Where statements run: local or cluster.",
            ),
        ]
    }
}

impl MetaLakeOptions {
    /// `config`, able to hold MetaLake's options: their keys known, so that a value set by name, or sent by another
    /// process, is taken and not dropped.
    pub fn register(mut config: SessionConfig) -> SessionConfig {
        if config.options().extensions.get::<MetaLakeOptions>().is_none() {
            config.options_mut().extensions.insert(MetaLakeOptions::default());
        }
        config
    }

    /// `config` with the commit asked for: who, what it says, and a writer's key; its other options as they were.
    pub fn commit(
        mut config: SessionConfig,
        author: Option<String>,
        message: Option<String>,
        idempotent: Option<(String, i64)>,
    ) -> SessionConfig {
        let mut options = config.options().extensions.get::<MetaLakeOptions>().cloned().unwrap_or_default();
        options.author = author;
        options.message = message;
        // A version is positive (CMT-8): one that is not is sent as 0, for the protocol to refuse, not dropped.
        options.app_version = idempotent.as_ref().map(|(_, version)| u64::try_from(*version).unwrap_or(0));
        options.app = idempotent.map(|(app, _)| app);
        config.options_mut().extensions.insert(options);
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What one process lists, another sets back by name, as Ballista carries a job's configuration.
    #[test]
    fn options_entries_round_trip() {
        let sent = MetaLakeOptions {
            author: Some("ana".into()),
            message: Some("two rows".into()),
            app: Some("loader".into()),
            app_version: Some(7),
            engine: Some(Engine::Local),
        };
        let mut here = MetaLakeOptions::register(SessionConfig::new());
        here.options_mut().extensions.insert(sent.clone());
        let mut there = MetaLakeOptions::register(SessionConfig::new());
        for entry in here.options().entries() {
            if let (true, Some(value)) = (entry.key.starts_with("metalake."), entry.value) {
                there.options_mut().set(&entry.key, &value).unwrap();
            }
        }
        assert_eq!(there.options().extensions.get::<MetaLakeOptions>(), Some(&sent));
    }

    /// The commit asked for leaves the engine as the session had it.
    #[test]
    fn commit_keeps_engine() {
        let mut config = MetaLakeOptions::register(SessionConfig::new());
        config.options_mut().set("metalake.engine", "cluster").unwrap();
        let config = MetaLakeOptions::commit(config, Some("ana".into()), None, None);
        let options = config.options().extensions.get::<MetaLakeOptions>().unwrap();
        assert_eq!((options.engine, options.author.as_deref()), (Some(Engine::Cluster), Some("ana")));
    }
}
