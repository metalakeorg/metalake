//! Engines a lake's faces send statements to: a cluster elsewhere, such as Trino, that reaches the lake through its
//! Iceberg REST server. Each is named once in the lake's `metalake.toml`, `[engines.<name>]`, with where it answers and
//! who asks; a secret is never written there, only the variable of the environment that holds it.
//!
//! Naming an engine reaches nothing: it is written whether or not the engine runs, or reaches the lake. What it takes
//! to send it a statement is checked when one is sent, and by `check`.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, Result};

mod local;
mod trino;
mod using;

use local::run_local;
pub use local::{LOCAL, LocalFailure, LocalRunner, how_it_ended, set_local_runner};
pub use trino::Trino;
pub use using::{OWN, Using, UsingOf, using_of};
pub(crate) use using::{Placed, place};

/// An engine the lake names, ready to be sent statements.
#[derive(Debug, Clone)]
pub enum Remote {
    Trino(Trino),
}

impl Remote {
    /// The engine `config` names: its secret is read from the environment now, and nothing is sent yet.
    pub fn new(name: &str, config: &EngineConfig) -> Result<Self> {
        match config.kind {
            EngineKind::Trino => Ok(Self::Trino(Trino::new(name, config)?)),
        }
    }

    /// Runs one statement on it, and answers its rows.
    pub async fn sql(&self, text: &str) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        match self {
            Self::Trino(trino) => trino.sql(text).await,
        }
    }

    /// The lake's catalog in it.
    pub fn catalog(&self) -> &str {
        match self {
            Self::Trino(trino) => trino.catalog(),
        }
    }

    /// Whether each of the two links a statement takes holds, apart: this side reaches the engine and is let in, and
    /// the engine reaches the lake, through the lake's Iceberg server, as its catalog. The second is not asked when
    /// the first fails.
    pub async fn check(&self) -> Checked {
        let engine = self
            .sql("select 1")
            .await
            .map(|_| "answers, and lets this side in".to_string());
        let lake = match &engine {
            Err(_) => Err("not asked: the engine is not reached".to_string()),
            Ok(_) => match self.sql(&format!("show schemas from {}", self.catalog())).await {
                Ok((_, batches)) => {
                    let schemas: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    Ok(format!("sees the lake as `{}`: {schemas} schemas", self.catalog()))
                }
                Err(e) => Err(e.to_string()),
            },
        };
        Checked {
            engine: engine.map_err(|e| e.to_string()),
            lake,
        }
    }
}

/// What `check` found of each link: what holds, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    /// This side → the engine.
    pub engine: std::result::Result<String, String>,
    /// The engine → the lake, through its Iceberg server.
    pub lake: std::result::Result<String, String>,
}

impl Checked {
    pub fn holds(&self) -> bool {
        self.engine.is_ok() && self.lake.is_ok()
    }

    /// As rows: a link each, whether it holds, and what was found.
    pub fn rows(&self, name: &str) -> Result<(SchemaRef, RecordBatch)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("link", DataType::Utf8, false),
            Field::new("holds", DataType::Boolean, false),
            Field::new("said", DataType::Utf8, false),
        ]));
        let links = [
            (format!("metalake -> {name}"), &self.engine),
            (format!("{name} -> lake"), &self.lake),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(links.iter().map(|(l, _)| Some(l.clone())).collect::<StringArray>()),
            Arc::new(links.iter().map(|(_, r)| Some(r.is_ok())).collect::<BooleanArray>()),
            Arc::new(
                links
                    .iter()
                    .map(|(_, r)| {
                        Some(match r {
                            Ok(s) | Err(s) => s.clone(),
                        })
                    })
                    .collect::<StringArray>(),
            ),
        ];
        Ok((schema.clone(), RecordBatch::try_new(schema, columns)?))
    }
}

/// The catalog the lake is in an engine, unless its entry names another: what `metalake iceberg connect` names it.
pub const CATALOG: &str = "lake";

/// The kinds of engine a lake can send statements to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind {
    Trino,
}

impl EngineKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Trino => "trino",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "trino" => Ok(Self::Trino),
            other => Err(Error::usage(format!(
                "an engine of type `{other}`: the types are trino"
            ))),
        }
    }
}

/// An engine as `[engines.<name>]` holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    #[serde(rename = "type")]
    pub kind: EngineKind,
    /// Where it answers: Trino's coordinator, `https://trino.example:8443`.
    pub url: String,
    /// Who asks, as the engine knows them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The variable of the environment that holds a bearer token for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    /// The variable of the environment that holds the user's password for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
    /// The lake's catalog in the engine; [`CATALOG`] when not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
}

impl EngineConfig {
    /// Refuses what could never be asked: a URL that is not http or https, a password with no user, two secrets.
    pub fn validate(&self) -> Result<()> {
        let url = Url::parse(&self.url).map_err(|e| Error::usage(format!("an engine's url {}: {e}", self.url)))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::usage(format!(
                "an engine's url is http or https, not {}",
                self.url
            )));
        }
        if self.token_env.is_some() && self.password_env.is_some() {
            return Err(Error::usage("an engine signs in by a token or by a password, not both"));
        }
        if self.password_env.is_some() && self.user.is_none() {
            return Err(Error::usage("a password is a user's: name the user too"));
        }
        if self.kind == EngineKind::Trino && self.user.is_none() {
            return Err(Error::usage("Trino asks who runs a statement: name the user"));
        }
        for variable in [&self.token_env, &self.password_env].into_iter().flatten() {
            if variable.is_empty() || variable.contains('=') {
                return Err(Error::usage(format!("`{variable}` is not the name of a variable")));
            }
        }
        Ok(())
    }

    /// The lake's catalog in the engine.
    pub fn catalog(&self) -> &str {
        self.catalog.as_deref().unwrap_or(CATALOG)
    }

    /// How it signs in, as a person reads it: never the secret.
    pub fn auth(&self) -> String {
        match (&self.token_env, &self.password_env) {
            (Some(token), _) => format!("token from ${token}"),
            (None, Some(password)) => format!("password from ${password}"),
            (None, None) => "none".into(),
        }
    }
}

/// The engines a lake names, by name.
pub type Engines = BTreeMap<String, EngineConfig>;

/// One statement `USING` an engine, on `lake`, the session reading `on`: a local engine, run in a process of its own, or
/// one the lake's file names, sent the statement. A local engine's name is never another's.
pub(crate) async fn run_using(lake: &crate::Lake, on: &str, using: &Using) -> Result<crate::Prepared> {
    // An engine sees the lake through its Iceberg server, whose catalog is `main`'s: on another ref, it would answer
    // for `main` without a word.
    if on != "main" {
        return Err(Error::usage(format!(
            "USING {} reads `main`, and this session is on `{on}`",
            using.engine
        )));
    }
    if LOCAL.contains(&using.engine.as_str()) {
        return run_local(lake, using).await;
    }
    let engines = lake.engines()?;
    let Some(config) = engines.get(&using.engine) else {
        let named: Vec<&str> = engines.keys().map(String::as_str).collect();
        return Err(Error::usage(format!(
            "USING {}: no such engine; {} run here, and {}",
            using.engine,
            LOCAL.join(", "),
            match named.as_slice() {
                [] => "metalake.toml names none: `metalake engine add` names one".to_string(),
                named => format!("metalake.toml names {}", named.join(", ")),
            }
        )));
    };
    let (schema, batches) = Remote::new(&using.engine, config)?.sql(&using.sql).await?;
    crate::Prepared::from_batches(schema, batches)
}

/// Refuses a name `USING` could not take bare: lowercase letters, digits and `_`, not first a digit.
pub fn check_engine_name(name: &str) -> Result<()> {
    if LOCAL.contains(&name) {
        return Err(Error::usage(format!(
            "`{name}` is an engine that runs here, as `USING {name}` names it: an engine takes another name"
        )));
    }
    if name == using::OWN {
        return Err(Error::usage(format!(
            "`{name}` is the lake's own engine, as `USING {name}` names it: an engine takes another name"
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    if first && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        Ok(())
    } else {
        Err(Error::usage(format!(
            "an engine's name is lowercase letters, digits and `_`, not first a digit: `{name}`"
        )))
    }
}

/// Engines as rows: what `metalake engine list` answers.
pub fn engine_rows(engines: &Engines) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("user", DataType::Utf8, true),
        Field::new("auth", DataType::Utf8, false),
        Field::new("catalog", DataType::Utf8, false),
    ]));
    let column = |f: &dyn Fn(&str, &EngineConfig) -> Option<String>| -> ArrayRef {
        Arc::new(engines.iter().map(|(name, e)| f(name, e)).collect::<StringArray>())
    };
    let columns = vec![
        column(&|name, _| Some(name.to_string())),
        column(&|_, e| Some(e.kind.name().to_string())),
        column(&|_, e| Some(e.url.clone())),
        column(&|_, e| e.user.clone()),
        column(&|_, e| Some(e.auth())),
        column(&|_, e| Some(e.catalog().to_string())),
    ];
    Ok((schema.clone(), RecordBatch::try_new(schema, columns)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trino() -> EngineConfig {
        EngineConfig {
            kind: EngineKind::Trino,
            url: "https://trino.example:8443".into(),
            user: Some("ana".into()),
            token_env: Some("TRINO_TOKEN".into()),
            password_env: None,
            catalog: None,
        }
    }

    #[test]
    fn an_engine_reads_as_written() {
        let text =
            "type = \"trino\"\nurl = \"https://trino.example:8443\"\nuser = \"ana\"\ntoken_env = \"TRINO_TOKEN\"\n";
        let read: EngineConfig = toml::from_str(text).unwrap();
        assert_eq!(read, trino());
        assert_eq!(toml::to_string(&read).unwrap(), text);
        assert_eq!(read.catalog(), "lake");
        assert_eq!(read.auth(), "token from $TRINO_TOKEN");
    }

    #[test]
    fn an_engine_of_an_unknown_type_or_field_is_refused() {
        assert!(toml::from_str::<EngineConfig>("type = \"oracle\"\nurl = \"https://x\"\n").is_err());
        assert!(toml::from_str::<EngineConfig>("type = \"trino\"\nurl = \"https://x\"\ntoken = \"s\"\n").is_err());
    }

    #[test]
    fn what_could_never_be_asked_is_refused() {
        let with = |f: &dyn Fn(&mut EngineConfig)| {
            let mut e = trino();
            f(&mut e);
            e.validate()
        };
        assert!(with(&|_| {}).is_ok());
        assert!(with(&|e| e.url = "ftp://trino".into()).is_err());
        assert!(with(&|e| e.url = "not a url".into()).is_err());
        assert!(with(&|e| e.password_env = Some("P".into())).is_err(), "two secrets");
        assert!(with(&|e| e.user = None).is_err(), "Trino asks who runs");
        assert!(with(&|e| e.token_env = Some("A=B".into())).is_err());
    }

    #[test]
    fn a_name_using_takes_bare() {
        for good in ["trino_prod", "_t", "t2"] {
            assert!(check_engine_name(good).is_ok(), "{good}");
        }
        for bad in ["", "2t", "Trino", "trino-prod", "a.b", "metalake", "duckdb"] {
            assert!(check_engine_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn rows_never_show_a_secret() {
        let engines = Engines::from([("trino_prod".to_string(), trino())]);
        let (_, batch) = engine_rows(&engines).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let auth = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(auth.value(0), "token from $TRINO_TOKEN");
    }
}
