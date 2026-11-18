//! An engine's profile for a lake: what the engine needs to connect to the lake's Iceberg REST server, derived each time
//! from the lake (its server's URI, its warehouse's store) and the engine, and nothing kept. Each face hands it on in
//! its own way: the command line prints it in the engine's own form, Python connects the engine with it.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::Serialize;
use url::Url;

use super::{Iceberg, duckdb, flink, jars, pyiceberg, spark, trino};
use crate::{Error, Result};

/// The version of Iceberg's Java libraries a profile names: the one CI and the REST Compatibility Kit hold the service
/// to, so that a profile names what was tested.
pub const ICEBERG_JAVA: &str = "1.11.0";
/// The Spark, and its Scala, whose Iceberg runtime a Spark profile names.
pub const SPARK: &str = "3.5";
pub const SCALA: &str = "2.12";
/// The Hadoop whose client jars a Flink profile names: Iceberg's Flink runtime needs Hadoop's classes, which Flink
/// does not carry.
pub const HADOOP: &str = "3.4.1";
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
    /// HDFS or ViewFS, which an engine reads through Hadoop's file system, with the cluster's configuration and its own
    /// Kerberos identity: no credentials are vended for it.
    Hdfs,
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
            "hdfs" | "viewfs" => Ok(Store::Hdfs),
            other => Err(Error::failed(format!(
                "no engine profile reads a warehouse on {other}: {warehouse}"
            ))),
        }
    }
}

/// For a JVM that writes a lake's files on GCS with the credentials the lake's server vends: no file to read a quota
/// project from. Google's Java clients read `quota_project_id` from the file `GOOGLE_APPLICATION_CREDENTIALS` names,
/// the JVM's property first and then the environment's, whatever credentials they are given, and send it as
/// `x-goog-user-project`; a vended token is Storage's alone and cannot be billed to a project, so Google refuses the
/// write (`serviceusage.services.use`). An empty name reads no file. Nothing else in the JVM asks for it: the token is
/// the vended one.
pub(super) const NO_QUOTA_PROJECT: &str = "-DGOOGLE_APPLICATION_CREDENTIALS=";

/// What a lake gives a profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The lake's Iceberg REST server.
    pub uri: String,
    /// Where its files are.
    pub store: Store,
    /// The warehouse's URL.
    pub warehouse: String,
    /// What the engine bears to be a writer to the lake's server (REST-11): the lake's token, for its own server.
    pub token: Option<String>,
    /// On S3, the region and, for a store of its own, the endpoint the lake's store reaches it by, as its environment
    /// says: what an engine that reads the store itself, as Trino, is told besides its credentials.
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// The ref the engine is on (I6): a ref's name, which it stays on; `session:<id>`, a session, whose ref it follows
    /// request by request; none for `main`.
    pub on: Option<String>,
    /// The catalog of the lake the engine sees (I7), whose schemas are its namespaces: none for `default`.
    pub catalog: Option<String>,
}

/// What a warehouse starts with that asks the server for a session rather than a ref, as the server reads it.
pub const SESSION: &str = "session:";

impl Target {
    /// This target, on the ref `on`: `main` asks for no warehouse, so that its profile is what it always was.
    pub fn on(mut self, on: &str) -> Target {
        self.on = (on != crate::MAIN).then(|| on.to_string());
        self
    }

    /// This target, following session `id`: on whatever ref the session says it is on, at each request.
    pub fn in_session(mut self, id: &str) -> Target {
        self.on = Some(format!("{SESSION}{id}"));
        self
    }

    /// This target, seeing the lake's catalog `catalog`: `default` asks for no warehouse of its own (I7).
    pub fn in_catalog(mut self, catalog: &str) -> Target {
        self.catalog = (catalog != metalake::path::DEFAULT_CATALOG).then(|| catalog.to_string());
        self
    }

    /// The `warehouse` the engine asks the server for: `catalog@ref`, or `catalog@session:<id>` (I6, I7); none for the
    /// catalog `default` on `main`, which a client that asks for no warehouse sees.
    pub fn asks(&self) -> Option<String> {
        let catalog = self.catalog.as_deref().unwrap_or(metalake::path::DEFAULT_CATALOG);
        match (&self.catalog, &self.on) {
            (None, None) => None,
            (Some(_), None) => Some(catalog.to_string()),
            (_, Some(on)) => Some(format!("{catalog}@{on}")),
        }
    }

    /// Whether the engine follows a session, whose ref may move under a table the engine has loaded: one that keeps
    /// tables it loaded (Spark's and Flink's catalogs, by default) is told to keep none.
    pub fn follows_a_session(&self) -> bool {
        self.on.as_deref().is_some_and(|on| on.starts_with(SESSION))
    }
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

    /// One of Hadoop's, at [`HADOOP`].
    pub fn hadoop(name: &str) -> Artifact {
        Artifact {
            group: "org.apache.hadoop",
            name: name.into(),
            version: HADOOP,
        }
    }

    /// `group:name:version`, as Spark resolves it.
    pub fn coordinate(&self) -> String {
        format!("{}:{}:{}", self.group, self.name, self.version)
    }

    /// Its jar's file name, `name-version.jar`.
    pub fn file_name(&self) -> String {
        format!("{}-{}.jar", self.name, self.version)
    }

    /// Its jar's path in a Maven repository.
    pub fn path(&self) -> String {
        let group = self.group.replace('.', "/");
        format!("{group}/{}/{}/{}", self.name, self.version, self.file_name())
    }

    /// Its jar on Maven Central.
    pub fn url(&self) -> String {
        format!("https://repo1.maven.org/maven2/{}", self.path())
    }
}

/// The header by which a Java engine asks the server for credentials to the lake's files, as Iceberg's REST catalog
/// sends every catalog property under `header.`.
pub(crate) const DELEGATION: (&str, &str) = ("header.X-Iceberg-Access-Delegation", "vended-credentials");

impl Store {
    /// The library of Iceberg's by which a Java engine reads this store with the credentials the server vends; none for
    /// a local warehouse or HDFS, which Hadoop's file system reads with no credentials vended. Without it, Iceberg falls
    /// back to Hadoop's file system, which ignores them.
    pub(crate) fn bundle(self) -> Option<Artifact> {
        match self {
            Store::Local | Store::Hdfs => None,
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
    /// What the lake gave it.
    pub target: Target,
}

impl Profile {
    /// An engine's, with nothing in it yet.
    pub(super) fn new(engine: Engine, name: &str, target: &Target) -> Profile {
        Profile {
            engine,
            name: name.to_string(),
            properties: BTreeMap::new(),
            startup: BTreeMap::new(),
            artifacts: Vec::new(),
            target: target.clone(),
        }
    }

    /// The profile in the engine's own form: what the command line prints.
    pub fn render(&self) -> String {
        match self.engine {
            Engine::PyIceberg => pyiceberg::render(self),
            Engine::DuckDb => duckdb::render(self),
            Engine::Spark => spark::render(self),
            Engine::Trino => trino::render(self),
            Engine::Flink => flink::render(self),
        }
    }

    /// For an engine connected by statements, DuckDB and Flink: those statements, in order.
    pub fn statements(&self) -> Vec<String> {
        match self.engine {
            Engine::DuckDb => duckdb::statements(self),
            Engine::PyIceberg | Engine::Spark | Engine::Trino => Vec::new(),
            Engine::Flink => flink::statements(self),
        }
    }
}

/// In the engine's own form, as [`Profile::render`] gives it.
impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.render().trim_end_matches('\n'))
    }
}

/// What a program reads of a profile: the engine, the catalog's name, the server's URI, both sets of properties, and
/// the libraries by their coordinates.
impl Serialize for Profile {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Shown<'a> {
            engine: &'static str,
            name: &'a str,
            uri: &'a str,
            properties: &'a BTreeMap<String, String>,
            startup: &'a BTreeMap<String, String>,
            artifacts: Vec<String>,
        }
        Shown {
            engine: self.engine.as_str(),
            name: &self.name,
            uri: &self.target.uri,
            properties: &self.properties,
            startup: &self.startup,
            artifacts: self.artifacts.iter().map(Artifact::coordinate).collect(),
        }
        .serialize(serializer)
    }
}

/// The profile of `engine` for a lake at `target`, its catalog named `name`.
pub fn profile(engine: Engine, name: &str, target: &Target) -> Result<Profile> {
    let name = catalog_name(name)?;
    match engine {
        Engine::PyIceberg => Ok(pyiceberg::profile(name, target)),
        Engine::DuckDb => Ok(duckdb::profile(name, target)),
        Engine::Spark => Ok(spark::profile(name, target)),
        Engine::Trino => Ok(trino::profile(name, target)),
        Engine::Flink => flink::profile(name, target),
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
        let server = self.ensure().await?;
        self.target_of(server.url, Some(server.token))
    }

    /// What this lake gives a profile for its server at `uri`, one that runs elsewhere, as `metalake iceberg serve`
    /// there: nothing is started here. The token an engine bears to write is the one that server was started with, in
    /// `METALAKE_ICEBERG_TOKEN`; without it, the engine reads.
    pub fn target_at(&self, uri: &str) -> Result<Target> {
        let parsed = url::Url::parse(uri).map_err(|e| Error::usage(format!("the server's uri {uri}: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(Error::usage(format!("the server's uri is http or https, not {uri}")));
        }
        let token = std::env::var(crate::SHUTDOWN_TOKEN).ok().filter(|t| !t.is_empty());
        self.target_of(uri.trim_end_matches('/').to_string(), token)
    }

    fn target_of(&self, uri: String, token: Option<String>) -> Result<Target> {
        let warehouse = self.lake.warehouse()?;
        let store = Store::of(&warehouse)?;
        let var = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        };
        let s3 = store == Store::S3;
        Ok(Target {
            uri,
            store,
            warehouse,
            token,
            region: s3.then(|| var(&["AWS_REGION", "AWS_DEFAULT_REGION"]).unwrap_or_else(|| "us-east-1".into())),
            endpoint: s3.then(|| var(&["AWS_ENDPOINT", "AWS_ENDPOINT_URL"])).flatten(),
            on: None,
            catalog: None,
        })
    }

    /// The profile of `engine` for this lake on `main`, its catalog named `name`, with the jars it is given as files
    /// fetched.
    pub async fn profile(&self, engine: Engine, name: &str) -> Result<Profile> {
        self.profile_on(engine, name, crate::MAIN, metalake::path::DEFAULT_CATALOG)
            .await
    }

    /// The same, on the ref `on`, seeing the lake's catalog `catalog`: the engine reads and writes that catalog's
    /// schemas and tables on that ref (I6, I7).
    pub async fn profile_on(&self, engine: Engine, name: &str, on: &str, catalog: &str) -> Result<Profile> {
        // A name refused before a server is started for it.
        catalog_name(name)?;
        let target = self.target().await?.on(on).in_catalog(catalog);
        self.profile_for(engine, name, &target).await
    }

    /// The profile of `engine` for this lake's server at `uri`, which runs elsewhere, on the ref `on`: nothing is started
    /// here.
    pub async fn profile_at(&self, engine: Engine, name: &str, uri: &str, on: &str, catalog: &str) -> Result<Profile> {
        catalog_name(name)?;
        let target = self.target_at(uri)?.on(on).in_catalog(catalog);
        self.profile_for(engine, name, &target).await
    }

    /// The profile of `engine` for session `id`, which is on `on` now (I6): the engine follows the session, reading and
    /// writing whatever ref it says it is on, with no new connection. The server is told `on` first.
    pub async fn profile_in_session(
        &self,
        engine: Engine,
        name: &str,
        id: &str,
        on: &str,
        catalog: &str,
    ) -> Result<Profile> {
        catalog_name(name)?;
        self.follow(id, on).await?;
        let target = self.target().await?.in_session(id).in_catalog(catalog);
        self.profile_for(engine, name, &target).await
    }

    /// Tells the lake's server on this machine that session `id` is on `on`, from its next request: what `lake.on` and
    /// a notebook's selector do. The server is started if it is not; the request bears the lake's token.
    pub async fn follow(&self, id: &str, on: &str) -> Result<()> {
        let server = self.ensure().await?;
        let mut url = url::Url::parse(&server.url).map_err(|e| Error::failed(format!("the server's url: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| Error::failed("the server's url takes no path"))?
            .extend(["metalake", "sessions", id]);
        let answer = reqwest::Client::new()
            .put(url)
            .bearer_auth(&server.token)
            .json(&serde_json::json!({"ref": on}))
            .send()
            .await
            .map_err(|e| Error::failed(format!("the lake's server, telling it session {id} is on {on}: {e}")))?;
        if answer.status().is_success() {
            return Ok(());
        }
        let status = answer.status();
        let said: serde_json::Value = answer.json().await.unwrap_or_default();
        let message = said["error"]["message"]
            .as_str()
            .unwrap_or("no reason given")
            .to_string();
        Err(match status.as_u16() {
            404 => Error::usage(message),
            _ => Error::failed(format!(
                "the lake's server refused session {id} on {on} ({status}): {message}"
            )),
        })
    }

    async fn profile_for(&self, engine: Engine, name: &str, target: &Target) -> Result<Profile> {
        let profile = profile(engine, name, target)?;
        if engine == Engine::Flink {
            jars::fetch(&profile.artifacts).await?;
        }
        Ok(profile)
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
            ("hdfs://namenode:8020/lake", Store::Hdfs),
            ("hdfs://nameservice1/lake", Store::Hdfs),
            ("viewfs://cluster/lake", Store::Hdfs),
        ];
        for (warehouse, store) in cases {
            assert_eq!(Store::of(warehouse).unwrap(), store, "{warehouse}");
            assert_eq!(store.bundle().is_none(), matches!(store, Store::Local | Store::Hdfs));
        }
        assert!(Store::of("ftp://host/lake").is_err());
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

    /// On a ref other than `main`, each engine asks the server for it as its warehouse, in its own words (I6); on
    /// `main`, its profile is what it always was.
    #[test]
    fn i6_each_engine_asks_for_the_ref_as_its_warehouse() {
        let target = Target {
            uri: "http://127.0.0.1:8181".into(),
            store: Store::Gcs,
            warehouse: "gs://bucket/lake".into(),
            token: None,
            region: None,
            endpoint: None,
            on: None,
            catalog: None,
        };
        let asked = [
            (Engine::PyIceberg, "warehouse: \"default@ventas_ana\""),
            (Engine::DuckDb, "ATTACH 'default@ventas_ana' AS lake ("),
            (Engine::Spark, "spark.sql.catalog.lake.warehouse default@ventas_ana"),
            (Engine::Flink, "'warehouse' = 'default@ventas_ana'"),
            (Engine::Trino, "iceberg.rest-catalog.warehouse=default@ventas_ana"),
        ];
        for (engine, said) in asked {
            let on = profile(engine, "lake", &target.clone().on("ventas_ana"))
                .unwrap()
                .render();
            assert!(on.contains(said), "{engine:?}: {on}");
            let main = profile(engine, "lake", &target.clone().on("main")).unwrap().render();
            assert_eq!(main, profile(engine, "lake", &target).unwrap().render(), "{engine:?}");
            assert!(
                !main.contains("ventas_ana") && !main.contains("warehouse"),
                "{engine:?}: {main}"
            );
        }
        let quoted = profile(Engine::DuckDb, "lake", &target.on("ana's")).unwrap().render();
        assert!(quoted.contains("ATTACH 'default@ana''s' AS lake"), "{quoted}");
    }

    /// A catalog other than `default` is asked for with the ref, or alone on `main` (I7).
    #[test]
    fn i7_each_engine_asks_for_its_catalog() {
        let target = Target {
            uri: "http://127.0.0.1:8181".into(),
            store: Store::Local,
            warehouse: "file:///tmp/lake/".into(),
            token: None,
            region: None,
            endpoint: None,
            on: None,
            catalog: None,
        };
        assert_eq!(target.clone().in_catalog("ventas").asks().as_deref(), Some("ventas"));
        assert_eq!(
            target.clone().on("dev").in_catalog("ventas").asks().as_deref(),
            Some("ventas@dev")
        );
        assert_eq!(
            target.clone().in_session("k1").in_catalog("ventas").asks().as_deref(),
            Some("ventas@session:k1")
        );
        assert_eq!(target.clone().in_catalog("default").asks(), None);
        let duckdb = profile(Engine::DuckDb, "lake", &target.in_catalog("ventas"))
            .unwrap()
            .render();
        assert!(duckdb.contains("ATTACH 'ventas' AS lake ("), "{duckdb}");
    }

    /// Following a session, each engine asks for `session:<id>` as its warehouse, and those whose catalog keeps the
    /// tables it loaded, Spark and Flink, are told to keep none (I6).
    #[test]
    fn i6_each_engine_follows_a_session_and_caches_no_table() {
        let target = Target {
            uri: "http://127.0.0.1:8181".into(),
            store: Store::Gcs,
            warehouse: "gs://bucket/lake".into(),
            token: None,
            region: None,
            endpoint: None,
            on: None,
            catalog: None,
        }
        .in_session("k1");
        assert!(target.follows_a_session());
        let asked = [
            (Engine::PyIceberg, "warehouse: \"default@session:k1\""),
            (Engine::DuckDb, "ATTACH 'default@session:k1' AS lake ("),
            (Engine::Spark, "spark.sql.catalog.lake.warehouse default@session:k1"),
            (Engine::Flink, "'warehouse' = 'default@session:k1'"),
            (Engine::Trino, "iceberg.rest-catalog.warehouse=default@session:k1"),
        ];
        for (engine, said) in asked {
            let rendered = profile(engine, "lake", &target).unwrap().render();
            assert!(rendered.contains(said), "{engine:?}: {rendered}");
        }
        let spark = profile(Engine::Spark, "lake", &target).unwrap().render();
        assert!(spark.contains("spark.sql.catalog.lake.cache-enabled false"), "{spark}");
        let flink = profile(Engine::Flink, "lake", &target).unwrap().render();
        assert!(flink.contains("'cache-enabled' = 'false'"), "{flink}");
        let pinned = profile(Engine::Spark, "lake", &target.clone().on("dev"))
            .unwrap()
            .render();
        assert!(
            !pinned.contains("cache-enabled"),
            "a ref, which does not move, keeps Iceberg's default: {pinned}"
        );
    }
}
