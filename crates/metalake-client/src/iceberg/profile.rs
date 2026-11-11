//! An engine's profile for a lake: what the engine needs to connect to the lake's Iceberg REST server, derived each time
//! from the lake (its server's URI, its warehouse's store) and the engine, and nothing kept. Each face hands it on in
//! its own way: the command line prints it in the engine's own form, Python connects the engine with it.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use url::Url;

use super::{Iceberg, duckdb, pyiceberg, spark};
use crate::{Error, Result};

/// The version of Iceberg's Java libraries a profile names: the one CI and the REST Compatibility Kit hold the service
/// to, so that a profile names what was tested.
pub const ICEBERG_JAVA: &str = "1.11.0";
/// The Spark, and its Scala, whose Iceberg runtime a Spark profile names.
pub const SPARK: &str = "3.5";
pub const SCALA: &str = "2.12";
/// The Flink whose Iceberg runtime a Flink profile names.
pub const FLINK: &str = "1.20";

/// The engines with a profile, named as text by `FromStr` and `Display`. Polars and Daft take PyIceberg's catalog as
/// it is, and need none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Engine {
    PyIceberg,
    DuckDb,
    Spark,
    Flink,
    Trino,
}

impl Engine {
    pub const ALL: [Engine; 5] = [
        Engine::PyIceberg,
        Engine::DuckDb,
        Engine::Spark,
        Engine::Flink,
        Engine::Trino,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Engine::PyIceberg => "pyiceberg",
            Engine::DuckDb => "duckdb",
            Engine::Spark => "spark",
            Engine::Flink => "flink",
            Engine::Trino => "trino",
        }
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Engine {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self> {
        Engine::ALL
            .into_iter()
            .find(|engine| engine.as_str() == name)
            .ok_or_else(|| {
                let known: Vec<_> = Engine::ALL.iter().map(|e| e.as_str()).collect();
                Error::usage(format!("no engine {name}: one of {}", known.join(", ")))
            })
    }
}

/// Where a lake keeps its files, by its warehouse's scheme: what decides what an engine adds to read them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    Local,
    S3,
    Gcs,
    Azure,
}

impl Store {
    /// The store of a warehouse's URL.
    pub fn of(warehouse: &str) -> Result<Store> {
        let url = Url::parse(warehouse).map_err(|e| Error::failed(format!("the warehouse {warehouse}: {e}")))?;
        match url.scheme() {
            "file" => Ok(Store::Local),
            "s3" | "s3a" => Ok(Store::S3),
            "gs" => Ok(Store::Gcs),
            "abfs" | "abfss" => Ok(Store::Azure),
            other => Err(Error::failed(format!(
                "no engine profile reads a warehouse on {other}: {warehouse}"
            ))),
        }
    }
}

/// What a lake gives a profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The lake's Iceberg REST server.
    pub uri: String,
    /// Where its files are.
    pub store: Store,
    /// The warehouse's URL.
    pub warehouse: String,
}

/// A library of Iceberg's an engine loads, by its Maven coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub group: &'static str,
    pub name: String,
    pub version: &'static str,
}

impl Artifact {
    /// One of Iceberg's, at [`ICEBERG_JAVA`].
    pub fn iceberg(name: impl Into<String>) -> Artifact {
        Artifact {
            group: "org.apache.iceberg",
            name: name.into(),
            version: ICEBERG_JAVA,
        }
    }

    /// `group:name:version`, as Spark resolves it.
    pub fn coordinate(&self) -> String {
        format!("{}:{}:{}", self.group, self.name, self.version)
    }

    /// Its jar on Maven Central.
    pub fn url(&self) -> String {
        let (group, name, version) = (self.group.replace('.', "/"), &self.name, self.version);
        format!("https://repo1.maven.org/maven2/{group}/{name}/{version}/{name}-{version}.jar")
    }
}

/// The header by which a Java engine asks the server for credentials to the lake's files, as Iceberg's REST catalog
/// sends every catalog property under `header.`.
pub(crate) const DELEGATION: (&str, &str) = ("header.X-Iceberg-Access-Delegation", "vended-credentials");

impl Store {
    /// The library of Iceberg's by which a Java engine reads this store with the credentials the server vends; none for
    /// a local warehouse. Without it, Iceberg falls back to Hadoop's file system, which ignores them.
    pub(crate) fn bundle(self) -> Option<Artifact> {
        match self {
            Store::Local => None,
            Store::S3 => Some(Artifact::iceberg("iceberg-aws-bundle")),
            Store::Gcs => Some(Artifact::iceberg("iceberg-gcp-bundle")),
            Store::Azure => Some(Artifact::iceberg("iceberg-azure-bundle")),
        }
    }
}

/// What an engine needs to connect to a lake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub engine: Engine,
    /// The name the lake's catalog takes in the engine: `lake` reads `lake.db.t`.
    pub name: String,
    /// What can be set on an engine already running: the catalog's own.
    pub properties: BTreeMap<String, String>,
    /// What is fixed only as an engine starts: its packages, extensions, jars.
    pub startup: BTreeMap<String, String>,
    /// The libraries it loads.
    pub artifacts: Vec<Artifact>,
}

impl Profile {
    /// An engine's, with nothing in it yet.
    pub(super) fn new(engine: Engine, name: &str) -> Profile {
        Profile {
            engine,
            name: name.to_string(),
            properties: BTreeMap::new(),
            startup: BTreeMap::new(),
            artifacts: Vec::new(),
        }
    }

    /// The profile in the engine's own form: what the command line prints.
    pub fn render(&self) -> String {
        match self.engine {
            Engine::PyIceberg => pyiceberg::render(self),
            Engine::DuckDb => duckdb::render(self),
            Engine::Spark => spark::render(self),
            engine => unreachable!("no profile of {engine} is built"),
        }
    }

    /// For an engine connected by statements, DuckDB and Flink: those statements, in order.
    pub fn statements(&self) -> Vec<String> {
        match self.engine {
            Engine::PyIceberg => Vec::new(),
            Engine::DuckDb => duckdb::statements(self),
            Engine::Spark => Vec::new(),
            engine => unreachable!("no profile of {engine} is built"),
        }
    }
}

/// The profile of `engine` for a lake at `target`, its catalog named `name`.
pub fn profile(engine: Engine, name: &str, target: &Target) -> Result<Profile> {
    let name = catalog_name(name)?;
    match engine {
        Engine::PyIceberg => Ok(pyiceberg::profile(name, target)),
        Engine::DuckDb => Ok(duckdb::profile(name, target)),
        Engine::Spark => Ok(spark::profile(name, target)),
        engine => Err(Error::usage(format!("the profile of {engine} is not built yet"))),
    }
}

/// A catalog's name as an engine takes it: an identifier, since it goes into statements and file names.
fn catalog_name(name: &str) -> Result<&str> {
    let mut chars = name.chars();
    let first = chars.next().is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    if first && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        Ok(name)
    } else {
        Err(Error::usage(format!(
            "a catalog's name is lowercase letters, digits and _, not starting with a digit: {name}"
        )))
    }
}

impl Iceberg<'_> {
    /// What this lake gives a profile: its server's URI, the server started if it is not, and its warehouse's store.
    pub async fn target(&self) -> Result<Target> {
        let warehouse = self.lake.warehouse()?;
        let store = Store::of(&warehouse)?;
        Ok(Target {
            uri: self.uri().await?,
            store,
            warehouse,
        })
    }

    /// The profile of `engine` for this lake, its catalog named `name`.
    pub async fn profile(&self, engine: Engine, name: &str) -> Result<Profile> {
        profile(engine, name, &self.target().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_by_scheme() {
        let cases = [
            ("file:///tmp/lake/", Store::Local),
            ("file:///C:/lake/", Store::Local),
            ("s3://bucket/lake", Store::S3),
            ("s3a://bucket/lake", Store::S3),
            ("gs://bucket/lake", Store::Gcs),
            ("abfss://container@account.dfs.core.windows.net/lake", Store::Azure),
        ];
        for (warehouse, store) in cases {
            assert_eq!(Store::of(warehouse).unwrap(), store, "{warehouse}");
        }
        assert!(Store::of("hdfs://namenode/lake").is_err());
    }

    #[test]
    fn engine_names() {
        for engine in Engine::ALL {
            assert_eq!(engine.as_str().parse::<Engine>().unwrap(), engine);
        }
        let error = "hive".parse::<Engine>().unwrap_err().to_string();
        assert!(error.contains("pyiceberg, duckdb, spark, flink, trino"), "{error}");
    }

    #[test]
    fn catalog_name_rules() {
        for name in ["lake", "_lake", "lake_2"] {
            assert!(catalog_name(name).is_ok(), "{name}");
        }
        for name in ["", "2lake", "Lake", "my-lake", "lake.db", "lake'"] {
            assert!(catalog_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn artifact_coordinates() {
        let runtime = Artifact::iceberg(format!("iceberg-spark-runtime-{SPARK}_{SCALA}"));
        assert_eq!(
            runtime.coordinate(),
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
        );
        assert_eq!(
            runtime.url(),
            "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/\
             iceberg-spark-runtime-3.5_2.12-1.11.0.jar"
        );
    }
}
