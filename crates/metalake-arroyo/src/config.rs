//! What a MetaLake table takes: its catalog (the profile) and the table on a branch, as a source or a sink. Each comes
//! from the API's JSON, through serde, or from SQL options, through [`FromOpts`].

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, FromOpts};
use chrono::{DateTime, SecondsFormat, Utc};
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The catalog: the protocol in a Postgres schema, and the object store of the tables' files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "MetaLake catalog")]
pub struct MetaLakeProfile {
    /// The catalog's Postgres, as a libpq connection string or URL; `{{ VAR }}` reads a part from the environment
    pub postgres: VarStr,
    /// The Postgres schema that holds the catalog
    pub schema: String,
    /// Options of the object store that holds the tables' files, such as credentials
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub storage: BTreeMap<String, VarStr>,
}

impl FromOpts for MetaLakeProfile {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let keys: Vec<String> = opts.keys_with_prefix("storage.").cloned().collect();
        let mut storage = BTreeMap::new();
        for key in keys {
            let value = opts.pull_str(&key)?;
            storage.insert(key["storage.".len()..].to_string(), VarStr::new(value));
        }
        Ok(Self {
            postgres: VarStr::new(opts.pull_str("catalog.postgres")?),
            schema: opts.pull_str("catalog.schema")?,
            storage,
        })
    }
}

/// A table on a branch, read as a source or written as a sink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "MetaLake table")]
pub struct MetaLakeTable {
    /// The branch the table is read on or written to
    #[serde(default = "main_branch")]
    pub branch: String,
    /// The table's namespace, level by level
    pub namespace: Vec<String>,
    /// The table's name in its namespace
    pub table: String,
    pub table_type: MetaLakeTableType,
}

fn main_branch() -> String {
    "main".to_string()
}

impl MetaLakeTable {
    /// `branch:namespace.table`, with each level quoted as SQL would need it.
    pub fn qualified_name(&self) -> String {
        format!("{}:{}.{}", self.branch, write_namespace(&self.namespace), self.table)
    }
}

impl FromOpts for MetaLakeTable {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let namespace = opts.pull_str("namespace")?;
        Ok(Self {
            branch: opts.pull_opt_str("branch")?.unwrap_or_else(main_branch),
            namespace: parse_namespace(&namespace).map_err(|e| plan_datafusion_err!("invalid 'namespace': {e}"))?,
            table: opts.pull_str("table")?,
            table_type: match opts.pull_str("type")?.as_str() {
                "source" => MetaLakeTableType::Source(opts.pull_struct()?),
                "sink" => MetaLakeTableType::Sink(opts.pull_struct()?),
                _ => return plan_err!("type must be one of 'source' or 'sink'"),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[schemars(title = "Table type")]
pub enum MetaLakeTableType {
    Source(MetaLakeSource),
    Sink(MetaLakeSink),
}

/// A table followed as rows inserted and deleted (CHG-4, CHG-5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Source")]
pub struct MetaLakeSource {
    /// What the source reads first: the table as it is at its start, then its changes, or only its changes
    #[serde(default)]
    pub start: StartMode,
    /// The time every subtask starts from, in RFC 3339; the time the pipeline is planned if not given
    pub from: Option<String>,
    /// How long the source waits before it asks for new commits again, when there were none, in milliseconds
    pub poll_interval_ms: Option<u64>,
}

impl MetaLakeSource {
    /// The time it starts from, once it has one.
    pub fn from_time(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.from.as_deref().map(parse_time).transpose()
    }

    /// Gives it the time `now` to start from, unless it has one.
    pub fn stamp(&mut self, now: DateTime<Utc>) {
        self.from
            .get_or_insert_with(|| now.to_rfc3339_opts(SecondsFormat::Micros, true));
    }
}

impl FromOpts for MetaLakeSource {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        let poll_interval = opts.pull_opt_duration("source.poll_interval")?;
        Ok(Self {
            start: opts.pull_opt_parsed("source.start")?.unwrap_or_default(),
            from: opts.pull_opt_str("source.from")?,
            poll_interval_ms: poll_interval.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
        })
    }
}

fn parse_time(text: &str) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("'{text}' is not an RFC 3339 time: {e}"))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    /// The table as it is at the start, then its changes
    #[default]
    Snapshot,
    /// Only the changes after the start
    Latest,
}

impl FromStr for StartMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "snapshot" => Ok(Self::Snapshot),
            "latest" => Ok(Self::Latest),
            _ => Err(format!("'{s}' is not one of 'snapshot' or 'latest'")),
        }
    }
}

/// A table written in two phases: its files at each checkpoint, committed once the checkpoint is complete.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Sink")]
pub struct MetaLakeSink {
    /// How long a file written and not yet committed may wait for its commit, in seconds; 24 hours if not given. A
    /// commit past it is refused (CMT-9), so it must cover the longest a pipeline may be down before it's restored
    pub intent_ttl_seconds: Option<u64>,
}

/// The intent lifetime when none is given: a night's outage, restored in the morning.
const INTENT_TTL: Duration = Duration::from_secs(24 * 3600);
/// Below this, an ordinary restart may outlive a file's intent.
const MIN_INTENT_TTL: Duration = Duration::from_secs(10 * 60);

impl MetaLakeSink {
    pub fn intent_ttl(&self) -> Duration {
        self.intent_ttl_seconds.map_or(INTENT_TTL, Duration::from_secs)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.intent_ttl() < MIN_INTENT_TTL {
            anyhow::bail!(
                "'sink.intent_ttl' is {:?}, and must be at least {:?}: a file waits that long for its commit, which a                  restart may delay",
                self.intent_ttl(),
                MIN_INTENT_TTL
            );
        }
        Ok(())
    }
}

impl FromOpts for MetaLakeSink {
    fn from_opts(opts: &mut ConnectorOptions) -> Result<Self, DataFusionError> {
        Ok(Self {
            intent_ttl_seconds: opts.pull_opt_duration("sink.intent_ttl")?.map(|d| d.as_secs()),
        })
    }
}

/// A namespace as SQL writes it: levels separated by dots, a level in double quotes when it holds a dot or a quote, and
/// a quote in it doubled. A level is any non-empty string (NS-1), so the quotes are what tells `"a.b"` from `a.b`.
pub fn parse_namespace(text: &str) -> Result<Vec<String>, String> {
    let mut levels = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        let mut level = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') if chars.peek() == Some(&'"') => {
                        chars.next();
                        level.push('"');
                    }
                    Some('"') => break,
                    Some(c) => level.push(c),
                    None => return Err(format!("{text}: a quote is not closed")),
                }
            }
            if !matches!(chars.peek(), None | Some('.')) {
                return Err(format!("{text}: a quoted level is followed by more than a dot"));
            }
        } else {
            while let Some(&c) = chars.peek() {
                match c {
                    '.' => break,
                    '"' => return Err(format!("{text}: a quote inside a level that isn't quoted")),
                    _ => level.push(c),
                }
                chars.next();
            }
        }
        if level.is_empty() {
            return Err(format!("{text}: a level is empty"));
        }
        levels.push(level);
        if chars.next().is_none() {
            return Ok(levels);
        }
    }
}

/// The namespace as [`parse_namespace`] reads it, quoting only the levels that need it.
pub fn write_namespace(levels: &[String]) -> String {
    let level = |l: &String| {
        if l.contains(['.', '"']) {
            format!("\"{}\"", l.replace('"', "\"\""))
        } else {
            l.clone()
        }
    };
    levels.iter().map(level).collect::<Vec<_>>().join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> ConnectorOptions {
        let mut opts = ConnectorOptions::new(&[], &None).unwrap();
        for (k, v) in pairs {
            opts.insert_str(*k, *v).unwrap();
        }
        opts
    }

    #[test]
    fn ns1_namespace_quoting_round_trips() {
        let levels = |l: &[&str]| Ok(l.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(parse_namespace("shop"), levels(&["shop"]));
        assert_eq!(parse_namespace("shop.eu"), levels(&["shop", "eu"]));
        assert_eq!(parse_namespace(r#""a.b".c"#), levels(&["a.b", "c"]));
        assert_eq!(parse_namespace(r#"x."say ""hi""""#), levels(&["x", r#"say "hi""#]));
        for bad in ["", "a.", ".a", "a..b", r#""a"#, r#""a"b"#, r#"a"b"#] {
            assert!(parse_namespace(bad).is_err(), "{bad}");
        }
        for levels in [vec!["shop", "eu"], vec!["a.b", "c"], vec![r#"say "hi""#]] {
            let levels: Vec<String> = levels.into_iter().map(String::from).collect();
            assert_eq!(parse_namespace(&write_namespace(&levels)), Ok(levels));
        }
    }

    #[test]
    fn source_options() {
        let mut opts = options(&[
            ("namespace", "shop.eu"),
            ("table", "orders"),
            ("type", "source"),
            ("source.start", "latest"),
            ("source.from", "2026-11-08T09:00:00Z"),
            ("source.poll_interval", "250 milliseconds"),
        ]);
        let table: MetaLakeTable = opts.pull_struct().unwrap();
        assert!(opts.is_empty(), "every option is taken");
        assert_eq!(table.branch, "main");
        assert_eq!(table.qualified_name(), "main:shop.eu.orders");
        let MetaLakeTableType::Source(source) = table.table_type else {
            panic!("a source")
        };
        assert_eq!(source.start, StartMode::Latest);
        assert_eq!(source.poll_interval_ms, Some(250));
        assert_eq!(
            source.from_time().unwrap().unwrap().to_rfc3339(),
            "2026-11-08T09:00:00+00:00"
        );
    }

    #[test]
    fn sink_and_profile_options() {
        let mut opts = options(&[
            ("catalog.postgres", "host=pg user={{ PG_USER }}"),
            ("catalog.schema", "lake"),
            ("storage.aws_region", "eu-west-1"),
            ("branch", "dev"),
            ("namespace", "shop"),
            ("table", "orders"),
            ("type", "sink"),
        ]);
        let profile: MetaLakeProfile = opts.pull_struct().unwrap();
        let table: MetaLakeTable = opts.pull_struct().unwrap();
        assert!(opts.is_empty());
        assert_eq!(profile.schema, "lake");
        assert_eq!(profile.storage.keys().collect::<Vec<_>>(), ["aws_region"]);
        assert_eq!(table.table_type, MetaLakeTableType::Sink(MetaLakeSink::default()));
        assert_eq!(table.qualified_name(), "dev:shop.orders");
    }

    #[test]
    fn invalid_table_options_refused() {
        let table = |pairs: &[(&str, &str)]| options(pairs).pull_struct::<MetaLakeTable>();
        let base = [("namespace", "shop"), ("table", "orders")];
        assert!(
            table(&[base[0], base[1], ("type", "lookup")]).is_err(),
            "a type it doesn't have"
        );
        assert!(table(&[base[0], base[1], ("type", "source"), ("source.start", "now")]).is_err());
        assert!(table(&[("namespace", "a..b"), base[1], ("type", "sink")]).is_err());
        assert!(table(&[base[1], ("type", "sink")]).is_err(), "no namespace");
    }

    #[test]
    fn chg5_start_time_stamp_is_idempotent() {
        let mut source = MetaLakeSource {
            start: StartMode::Snapshot,
            from: None,
            poll_interval_ms: None,
        };
        let now = Utc::now();
        source.stamp(now);
        let stamped = source.from_time().unwrap().unwrap();
        assert_eq!(stamped.timestamp_micros(), now.timestamp_micros());
        source.stamp(now + chrono::Duration::hours(1));
        assert_eq!(source.from_time().unwrap(), Some(stamped), "a time it has stays");
        source.from = Some("yesterday".into());
        assert!(source.from_time().is_err());
    }

    #[test]
    fn sink_intent_ttl() {
        assert_eq!(MetaLakeSink::default().intent_ttl(), Duration::from_secs(24 * 3600));
        let mut opts = options(&[("sink.intent_ttl", "2 hours")]);
        let sink: MetaLakeSink = opts.pull_struct().unwrap();
        assert_eq!(sink.intent_ttl(), Duration::from_secs(7200));
        assert!(sink.validate().is_ok());
        let short = MetaLakeSink {
            intent_ttl_seconds: Some(60),
        };
        assert!(short.validate().unwrap_err().to_string().contains("at least"));
    }

    #[test]
    fn table_config_json_round_trips() {
        let json = serde_json::json!({
            "namespace": ["a.b", "c"],
            "table": "t",
            "table_type": {"type": "source", "start": "latest"},
        });
        let table: MetaLakeTable = serde_json::from_value(json).unwrap();
        assert_eq!(table.branch, "main");
        assert_eq!(table.qualified_name(), r#"main:"a.b".c.t"#);
        assert_eq!(
            serde_json::from_value::<MetaLakeTable>(serde_json::to_value(&table).unwrap()).unwrap(),
            table
        );
        let unknown = serde_json::json!({"namespace": ["a"], "table": "t", "table_type": {"type": "sink"}, "extra": 1});
        assert!(serde_json::from_value::<MetaLakeTable>(unknown).is_err());
    }
}
