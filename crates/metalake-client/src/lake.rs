//! Which lake a command means, and the one place that knows how to reach it. What a lake is lives in its catalog; a
//! client holds only where the catalog is, and secrets come from the environment, never from a file.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use metalake::environment::{self, has_password, storage_options, without_password};
use metalake::path::{DEFAULT_CATALOG, DEFAULT_NAMESPACE};
use metalake::{Auth, Catalog, CatalogConfig, Storage};
use metalake_local::{Binaries, Cluster};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::engine::{EngineConfig, Engines, check_engine_name};
use crate::{Error, Result};

/// The file `init` writes, which a command finds in its directory or the nearest above it.
const FILE: &str = "metalake.toml";
/// The catalog's schema, unless one Postgres database holds more than one lake.
const SCHEMA: &str = environment::DEFAULT_SCHEMA;
/// The cluster's directory in a lake's own directory, which is how one is known without its file.
const CLUSTER: &str = "postgres";

/// Where a catalog is: all that a client has to know. A lake's own Postgres is a directory, relative to the file, which
/// holds the cluster, and a lake reaching one is its own concern: nothing past this file can tell it from any other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    postgres: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    /// A Ballista scheduler of the lake's (`df://host:port`), where the router may send a statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scheduler: Option<String>,
    /// How a client signs in to `postgres`: a cloud's token for its identity, by name, or its password when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    /// Engines its faces send statements to, by name (`[engines.<name>]`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    engines: Engines,
    /// The directory of the file that named the lake, if a file did: where the lake's own things beside it go.
    #[serde(skip)]
    directory: Option<PathBuf>,
}

impl Location {
    fn schema(&self) -> &str {
        self.schema.as_deref().unwrap_or(SCHEMA)
    }

    /// The scheduler the file names, or a program set; else the environment's.
    fn scheduler(&self) -> Option<String> {
        let named = || env::var(environment::SCHEDULER).ok().filter(|s| !s.is_empty());
        self.scheduler.clone().or_else(named)
    }

    fn auth(&self) -> Result<Auth> {
        Ok(self.auth.as_deref().map(Auth::parse).transpose()?.unwrap_or_default())
    }

    /// The auth the environment names, for a catalog it or a caller names by its connection string.
    fn auth_of_env() -> Option<String> {
        env::var(environment::POSTGRES_AUTH).ok().filter(|a| !a.is_empty())
    }

    /// What names the lake whatever reaching it takes: its own directory, or its catalog without a password. A lake's
    /// own Postgres may listen elsewhere each time it starts, and a password may change.
    fn identity(&self) -> String {
        match (&self.local, &self.postgres) {
            (Some(local), _) => format!("local:{}", local.display()),
            (None, Some(postgres)) => format!("catalog:{}", without_password(postgres)),
            (None, None) => String::new(),
        }
    }

    /// Stops the lake's Iceberg server on this machine, if one runs, and says whether one did.
    fn stop_server(&self) -> Result<bool> {
        crate::server::stop(&self.identity(), self.schema())
    }

    /// How to connect: a lake's own Postgres is brought up if it is not.
    fn config(&self, connections: Option<usize>) -> Result<CatalogConfig> {
        let auth = self.auth()?;
        let connection = match (&self.postgres, &self.local, auth) {
            (Some(postgres), None, Auth::Password) => with_password(postgres),
            (Some(_), None, auth) if env::var_os(environment::PASSWORD).is_some() => {
                return Err(Error::usage(format!(
                    "auth `{}` signs in with a token: unset PGPASSWORD",
                    auth.name()
                )));
            }
            (Some(postgres), None, _) => postgres.clone(),
            (None, Some(_), Auth::Password) => self.cluster()?.ensure()?,
            (None, Some(_), auth) => {
                return Err(Error::usage(format!(
                    "a lake's own Postgres signs in by its own password, not auth `{}`",
                    auth.name()
                )));
            }
            _ => return Err(Error::failed(format!("{FILE} must say either postgres or local"))),
        };
        let mut config = CatalogConfig::new(connection, self.schema()).auth(auth);
        if let Some(connections) = connections {
            config.connections = connections;
        }
        Ok(config)
    }

    /// Creates the catalog here, and the lake's own Postgres first if it has one.
    async fn install(&self, warehouse: String, connections: Option<usize>) -> Result<Catalog> {
        check_warehouse(&warehouse).await?;
        if let Some(local) = &self.local {
            Cluster::init(local, Binaries::find()?)?;
        }
        Ok(Catalog::init(self.config(connections)?.warehouse(warehouse), None).await?)
    }

    fn cluster(&self) -> Result<Cluster> {
        let local = self
            .local
            .as_ref()
            .ok_or_else(|| Error::failed("this lake has no Postgres of its own"))?;
        Ok(Cluster::open(local, Binaries::find()?)?)
    }

    /// The lake a command means: the directory it names, the environment, or the nearest file.
    fn find(dir: Option<&Path>) -> Result<Self> {
        if let Some(dir) = dir {
            // Absolute, as `named` makes it: the lake's identity, and so its server, is the same however it is named.
            return Self::read(&std::path::absolute(dir)?.join(FILE));
        }
        if let Ok(postgres) = env::var(environment::POSTGRES) {
            let schema = env::var(environment::SCHEMA).ok();
            return Ok(Self {
                postgres: Some(postgres),
                local: None,
                schema,
                scheduler: None,
                auth: Self::auth_of_env(),
                engines: Engines::new(),
                directory: None,
            });
        }
        let here = env::current_dir()?;
        match here.ancestors().map(|d| d.join(FILE)).find(|f| f.is_file()) {
            Some(file) => Self::read(&file),
            None => Err(Error::failed(format!(
                "no {FILE} here or above: run `metalake init`, or set METALAKE_POSTGRES"
            ))),
        }
    }

    /// The lake a caller names: a connection string, a directory with the file, a lake's own directory, or, naming
    /// none, what a command would find. A process may change directory, so what it names is made absolute.
    fn named(target: Option<&str>, schema: Option<&str>) -> Result<Self> {
        let mut location = match target {
            None => Self::find(None)?,
            Some(postgres) if is_postgres(postgres) => Self {
                postgres: Some(postgres.to_string()),
                local: None,
                schema: None,
                scheduler: None,
                auth: Self::auth_of_env(),
                engines: Engines::new(),
                directory: None,
            },
            Some(directory) => {
                let directory = std::path::absolute(directory)?;
                match (directory.join(FILE), directory.join(CLUSTER)) {
                    (file, _) if file.is_file() => Self::read(&file)?,
                    (_, cluster) if cluster.is_dir() => Self {
                        postgres: None,
                        local: Some(directory),
                        schema: None,
                        scheduler: None,
                        auth: None,
                        engines: Engines::new(),
                        directory: None,
                    },
                    _ => {
                        return Err(Error::failed(format!(
                            "{}: neither a {FILE} nor a lake is there",
                            directory.display()
                        )));
                    }
                }
            }
        };
        location.schema = schema.map(str::to_string).or(location.schema);
        Ok(location)
    }

    fn read(file: &Path) -> Result<Self> {
        let text = fs::read_to_string(file).map_err(|e| Error::failed(format!("{}: {e}", file.display())))?;
        let mut location: Self =
            toml::from_str(&text).map_err(|e| Error::failed(format!("{}: {e}", file.display())))?;
        let directory = file.parent().unwrap_or(Path::new("."));
        location.local = location.local.map(|local| within(directory, &local));
        location.directory = Some(std::path::absolute(directory)?);
        Ok(location)
    }
}

/// The engines a lake's file names, edited in place: what else the file holds, comments too, is kept. Written whole to a
/// file beside it, then renamed over it, so that a reader never sees half of it.
fn edit_engines<T>(file: &Path, edit: impl FnOnce(&mut toml_edit::Table) -> Result<T>) -> Result<T> {
    let text = fs::read_to_string(file).map_err(|e| Error::failed(format!("{}: {e}", file.display())))?;
    let mut document: toml_edit::DocumentMut = text
        .parse()
        .map_err(|e| Error::failed(format!("{}: {e}", file.display())))?;
    let engines = document
        .entry("engines")
        .or_insert_with(|| {
            let mut table = toml_edit::Table::new();
            table.set_implicit(true);
            toml_edit::Item::Table(table)
        })
        .as_table_mut()
        .ok_or_else(|| Error::failed(format!("{}: `engines` is not a table", file.display())))?;
    let answer = edit(engines)?;
    let written = file.with_extension("toml.writing");
    fs::write(&written, document.to_string()).map_err(|e| Error::failed(format!("{}: {e}", written.display())))?;
    fs::rename(&written, file).map_err(|e| Error::failed(format!("{}: {e}", file.display())))?;
    Ok(answer)
}

/// An engine as a table of the file.
fn engine_table(config: &EngineConfig) -> toml_edit::Table {
    let mut table = toml_edit::Table::new();
    table["type"] = toml_edit::value(config.kind.name());
    table["url"] = toml_edit::value(&config.url);
    for (key, value) in [
        ("user", &config.user),
        ("token_env", &config.token_env),
        ("password_env", &config.password_env),
        ("catalog", &config.catalog),
        ("lake_url", &config.lake_url),
    ] {
        if let Some(value) = value {
            table[key] = toml_edit::value(value);
        }
    }
    table
}

/// A lake's file, found as a command finds it, and nothing reached: what is named in it alone, its engines, is read
/// and written without the catalog.
#[derive(Debug, Clone)]
pub struct LakeFile {
    path: PathBuf,
}

impl LakeFile {
    /// The file a command in `dir` means: there, or the nearest above it.
    pub fn find(dir: Option<&Path>) -> Result<Self> {
        let location = Location::find(dir)?;
        let directory = location.directory.ok_or_else(|| {
            Error::usage(format!(
                "the lake is named by the environment, with no {FILE}: an engine is named in one"
            ))
        })?;
        Ok(Self {
            path: directory.join(FILE),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The engines it names now.
    pub fn engines(&self) -> Result<Engines> {
        Ok(Location::read(&self.path)?.engines)
    }

    /// Names an engine. It reaches nothing: the engine need not run, nor reach the lake, until a statement is sent to
    /// it. A name taken is refused, unless `replace`.
    pub fn add_engine(&self, name: &str, config: &EngineConfig, replace: bool) -> Result<()> {
        add_engine_in(&self.path, name, config, replace)
    }

    /// Forgets an engine. The engine itself is not touched.
    pub fn remove_engine(&self, name: &str) -> Result<()> {
        remove_engine_in(&self.path, name)
    }

    /// One engine it names, ready to be sent statements: its secret is read from the environment now.
    pub fn engine(&self, name: &str) -> Result<crate::Remote> {
        crate::Remote::new(name, &named_engine(self.engines()?, name)?)
    }
}

/// One engine of `engines`, or why there is none.
pub(crate) fn named_engine(mut engines: Engines, name: &str) -> Result<EngineConfig> {
    engines.remove(name).ok_or_else(|| {
        let named: Vec<&str> = engines.keys().map(String::as_str).collect();
        Error::usage(match named.as_slice() {
            [] => format!("no engine `{name}`: none is named; `metalake engine add` names one"),
            named => format!("no engine `{name}`: the engines are {}", named.join(", ")),
        })
    })
}

fn add_engine_in(file: &Path, name: &str, config: &EngineConfig, replace: bool) -> Result<()> {
    check_engine_name(name)?;
    config.validate()?;
    edit_engines(file, |engines| {
        if engines.contains_key(name) && !replace {
            return Err(Error::usage(format!(
                "an engine `{name}` is named already: --replace replaces it"
            )));
        }
        engines.insert(name, toml_edit::Item::Table(engine_table(config)));
        Ok(())
    })
}

fn remove_engine_in(file: &Path, name: &str) -> Result<()> {
    edit_engines(file, |engines| match engines.remove(name) {
        Some(_) => Ok(()),
        None => Err(Error::usage(format!("no engine `{name}`"))),
    })
}

/// A lake, reached.
#[derive(Debug, Clone)]
pub struct Lake {
    pub catalog: Catalog,
    location: Location,
}

impl Lake {
    /// Enough for one statement: a command runs one at a time, and the core's default is for a long-lived engine.
    pub const ONE_COMMAND: Option<usize> = Some(4);

    /// The Ballista scheduler this lake's sessions reach (`df://host:port`), if any: what `with_scheduler` set, the
    /// file's `scheduler`, or `METALAKE_SCHEDULER`. With one, a session's statements go where its router says; without,
    /// they run in this process.
    pub fn scheduler(&self) -> Option<String> {
        self.location.scheduler()
    }

    /// This lake, its sessions reaching the scheduler at `url`.
    pub fn with_scheduler(mut self, url: impl Into<String>) -> Self {
        self.location.scheduler = Some(url.into());
        self
    }

    /// The file that names this lake: where its engines are named.
    fn file(&self) -> Result<PathBuf> {
        let directory = self.location.directory.as_ref().ok_or_else(|| {
            Error::usage(format!(
                "this lake was named by its catalog, with no {FILE}: an engine is named in one"
            ))
        })?;
        Ok(directory.join(FILE))
    }

    /// The engines the lake's file names now, read again each time: another process may have named one since. A lake
    /// named by its catalog alone has none.
    pub fn engines(&self) -> Result<Engines> {
        match self.location.directory {
            Some(_) => Ok(Location::read(&self.file()?)?.engines),
            None => Ok(Engines::new()),
        }
    }

    /// One engine the lake's file names.
    pub fn engine_config(&self, name: &str) -> Result<EngineConfig> {
        named_engine(self.engines()?, name)
    }

    /// An engine the lake's file names, ready to be sent statements: its secret is read from the environment now.
    pub fn engine(&self, name: &str) -> Result<crate::Remote> {
        crate::Remote::new(name, &self.engine_config(name)?)
    }

    /// Names an engine in the lake's file. It reaches nothing: the engine need not run, nor reach the lake, until a
    /// statement is sent to it. A name taken is refused, unless `replace`.
    pub fn add_engine(&self, name: &str, config: &EngineConfig, replace: bool) -> Result<()> {
        add_engine_in(&self.file()?, name, config, replace)
    }

    /// Forgets an engine the lake's file names. The engine itself is not touched.
    pub fn remove_engine(&self, name: &str) -> Result<()> {
        remove_engine_in(&self.file()?, name)
    }

    /// The lake a command means.
    pub async fn open(dir: Option<&Path>) -> Result<Self> {
        let location = Location::find(dir)?;
        let catalog = Catalog::connect(location.config(Self::ONE_COMMAND)?).await?;
        Ok(Self { catalog, location })
    }

    /// The lake a long-lived program names, with the core's default of connections. A password may come in the
    /// connection string, since nothing is written; it is never shown.
    pub async fn connect(target: Option<&str>, schema: Option<&str>) -> Result<Self> {
        let location = Location::named(target, schema)?;
        let catalog = Catalog::connect(location.config(None)?).await?;
        Ok(Self { catalog, location })
    }

    /// Stops a lake's own Postgres, and answers where it is. The next command brings it up again.
    pub fn stop(dir: Option<&Path>) -> Result<PathBuf> {
        Self::stop_at(Location::find(dir)?)
    }

    /// Stops the Iceberg server of the lake a command means, if one runs on this machine, with no connection to its
    /// catalog, and says whether one did.
    pub fn stop_server(dir: Option<&Path>) -> Result<bool> {
        Location::find(dir)?.stop_server()
    }

    /// Stops the Postgres of the lake a program names, as `connect` names one.
    pub fn stop_named(target: Option<&str>) -> Result<PathBuf> {
        Self::stop_at(Location::named(target, None)?)
    }

    /// The lake's Iceberg server first, which reaches its Postgres, then the Postgres: one command leaves the lake with
    /// nothing running.
    fn stop_at(location: Location) -> Result<PathBuf> {
        // A lake with no Postgres of its own is refused before anything stops: `metalake iceberg stop` stops its server.
        let cluster = location.cluster()?;
        location.stop_server()?;
        cluster.stop()?;
        Ok(location.local.expect("a cluster has a directory"))
    }

    /// Upgrades the catalog of the lake a command means, which `open` refuses, and reaches it.
    pub async fn migrate(dir: Option<&Path>) -> Result<(Self, Option<i64>)> {
        Self::migrate_at(Location::find(dir)?, Self::ONE_COMMAND).await
    }

    /// Upgrades the catalog of the lake a program names, as `connect` names one, and reaches it.
    pub async fn migrate_named(target: Option<&str>, schema: Option<&str>) -> Result<(Self, Option<i64>)> {
        Self::migrate_at(Location::named(target, schema)?, None).await
    }

    async fn migrate_at(location: Location, connections: Option<usize>) -> Result<(Self, Option<i64>)> {
        let config = location.config(connections)?;
        let format_migration = Catalog::migrate(&config).await?;
        let catalog = Catalog::connect(config).await?;
        Ok((Self { catalog, location }, format_migration))
    }

    /// A lake a program makes: its file beside it wherever the program runs, with the core's default of connections.
    pub async fn init(catalog: &str, warehouse: Option<&str>, schema: Option<&str>) -> Result<Self> {
        Self::init_with(catalog, warehouse, schema, Auth::Password).await
    }

    /// The same, signing in to an existing Postgres as `auth` says.
    pub async fn init_with(catalog: &str, warehouse: Option<&str>, schema: Option<&str>, auth: Auth) -> Result<Self> {
        if is_postgres(catalog) {
            return Self::create_with(catalog, warehouse, schema, None, None, auth).await;
        }
        if auth != Auth::Password {
            return Err(Error::usage(format!(
                "a lake's own Postgres signs in by its own password, not auth `{}`",
                auth.name()
            )));
        }
        let path = std::path::absolute(catalog)?;
        let (Some(parent), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
            return Err(Error::usage(format!("{catalog}: a lake is a directory inside another")));
        };
        Self::create(name, warehouse, schema, Some(parent), None).await
    }

    /// Creates a catalog and the file that finds it, on a Postgres that exists or on one of the lake's own. The file
    /// comes first, so that a lake already here is refused before a catalog exists, and it goes if the catalog fails.
    /// A lake's sessions read unqualified tables from `main.default`, so it is born with that namespace.
    pub async fn create(
        catalog: &str,
        warehouse: Option<&str>,
        schema: Option<&str>,
        dir: Option<&Path>,
        connections: Option<usize>,
    ) -> Result<Self> {
        Self::create_with(catalog, warehouse, schema, dir, connections, Auth::Password).await
    }

    /// The same, signing in to an existing Postgres as `auth` says, which the file keeps.
    pub async fn create_with(
        catalog: &str,
        warehouse: Option<&str>,
        schema: Option<&str>,
        dir: Option<&Path>,
        connections: Option<usize>,
        auth: Auth,
    ) -> Result<Self> {
        if has_password(catalog) {
            return Err(Error::usage(
                "a password does not go in the command line or in a file: set PGPASSWORD",
            ));
        }
        let dir = dir.unwrap_or(Path::new("."));
        let own = !is_postgres(catalog);
        let warehouse = match warehouse {
            Some(warehouse) => warehouse_url(warehouse)?,
            None if own => {
                let data = dir.join(catalog).join("data");
                warehouse_url(&data.to_string_lossy())?
            }
            None => return Err(Error::usage("a lake on an existing Postgres needs a warehouse")),
        };
        let mut location = Location {
            postgres: (!own).then(|| catalog.to_string()),
            local: own.then(|| PathBuf::from(catalog)),
            schema: schema.filter(|s| *s != SCHEMA).map(str::to_string),
            scheduler: None,
            auth: (auth != Auth::Password).then(|| auth.name().to_string()),
            engines: Engines::new(),
            directory: None,
        };
        // A lake's own Postgres is refused a cloud's auth before anything is written.
        location.auth()?;
        if own && auth != Auth::Password {
            return Err(Error::usage(format!(
                "a lake's own Postgres signs in by its own password, not auth `{}`",
                auth.name()
            )));
        }
        fs::create_dir_all(dir)?;
        let file = dir.join(FILE);
        let text = toml::to_string(&location).map_err(|e| Error::failed(e.to_string()))?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)
            .and_then(|mut f| f.write_all(text.as_bytes()))
            .map_err(|e| match e.kind() {
                ErrorKind::AlreadyExists => Error::failed(format!("{} already exists", file.display())),
                _ => Error::failed(format!("{}: {e}", file.display())),
            })?;
        location.local = location.local.map(|local| within(dir, &local));
        // The lake just made is named by the file just written, as the next command finds it.
        location.directory = Some(std::path::absolute(dir)?);
        let catalog = match location.install(warehouse, connections).await {
            Ok(catalog) => catalog,
            Err(e) => {
                fs::remove_file(&file)?;
                return Err(e);
            }
        };
        let main = catalog.branch("main").await?;
        // The catalog and the schema a name without them is in (I7): `t` is `default.default.t`.
        main.transaction()
            .create_namespace(&[DEFAULT_CATALOG])
            .create_namespace(&[DEFAULT_CATALOG, DEFAULT_NAMESPACE])
            .commit()
            .await?;
        Ok(Self { catalog, location })
    }

    /// Closes the connections of this lake. Its server stays up: it is the lake's, and others may be on it.
    pub fn close(&self) {
        self.catalog.close();
    }

    /// The store of the catalog's warehouse, with the credentials of the environment. Only a verb that moves bytes
    /// asks for it.
    pub fn storage(&self) -> Result<Storage> {
        let warehouse = self
            .catalog
            .warehouse()
            .ok_or_else(|| Error::failed("the catalog has no warehouse"))?;
        Ok(Storage::new(warehouse, storage_options(env::vars_os()))?)
    }

    pub fn schema(&self) -> &str {
        self.location.schema()
    }

    /// The directory of the `metalake.toml` that named the lake; none for a lake its environment names alone.
    pub fn directory(&self) -> Option<&Path> {
        self.location.directory.as_deref()
    }

    /// The port the lake's UI tries first: the same each run, so that a browser keeps what it stored for the lake, by
    /// origin, and a bookmark keeps working (ADR 0007 §6.2).
    pub fn ui_port(&self) -> u16 {
        crate::server::port_in(crate::server::UI_PORTS, &self.identity(), self.schema())
    }

    /// What names the lake to its Iceberg server's files, whatever reaching it takes.
    pub(crate) fn identity(&self) -> String {
        self.location.identity()
    }

    /// The warehouse's URL, where the lake's files are.
    pub(crate) fn warehouse(&self) -> Result<String> {
        self.catalog
            .warehouse()
            .map(str::to_string)
            .ok_or_else(|| Error::failed("the catalog has no warehouse"))
    }

    /// How to reach the catalog's Postgres, with its password: what a pipeline's process is given, never printed.
    pub fn catalog_postgres(&self) -> Result<String> {
        Ok(self.location.config(None)?.postgres)
    }

    /// How a client signs in to the catalog.
    pub fn catalog_auth(&self) -> Result<Auth> {
        self.location.auth()
    }

    /// The Postgres of the catalog, fit to print, unless it is the lake's own.
    pub fn postgres(&self) -> Option<String> {
        self.location.postgres.as_deref().map(without_password)
    }

    /// The directory of the lake's own Postgres, if it has one.
    pub fn local(&self) -> Option<&Path> {
        self.location.local.as_deref()
    }
}

/// Writes and deletes an object in a new warehouse, so that a store that cannot be reached or credentials that do not
/// serve are said now, and not at the first write large enough to leave the catalog.
async fn check_warehouse(warehouse: &str) -> Result<()> {
    let failed = |e: metalake::Error| Error::failed(format!("the warehouse {warehouse} cannot be written: {e}"));
    let storage = Storage::new(warehouse, storage_options(env::vars_os())).map_err(failed)?;
    let probe = format!(
        "{}/.metalake-{}",
        warehouse.trim_end_matches('/'),
        uuid::Uuid::new_v4().simple()
    );
    storage.put(&probe, Vec::new().into()).await.map_err(failed)?;
    storage.delete(&probe).await.map_err(failed)
}

/// `local`, relative to `directory`, with no `.` in it, which is how a path gets printed.
fn within(directory: &Path, local: &Path) -> PathBuf {
    directory.join(local).components().collect()
}

/// A URL of a warehouse: as given if it is one, and a local directory, made if need be, as a `file:` URL.
fn warehouse_url(warehouse: &str) -> Result<String> {
    // A scheme of one letter is a Windows drive.
    if let Ok(url) = Url::parse(warehouse)
        && url.scheme().len() > 1
    {
        return Ok(warehouse.to_string());
    }
    fs::create_dir_all(warehouse)?;
    let path: PathBuf = std::path::absolute(warehouse)?;
    let url = Url::from_directory_path(&path)
        .map_err(|()| Error::failed(format!("{} is not a directory", path.display())))?;
    Ok(url.to_string())
}

/// Whether what `init` was given is a connection string, as opposed to a directory for a lake's own Postgres.
fn is_postgres(catalog: &str) -> bool {
    environment::is_url(catalog) || catalog.contains('=')
}

/// The connection string with `PGPASSWORD`, which the driver does not read, unless it already has one.
fn with_password(postgres: &str) -> String {
    environment::with_password(postgres, env::var(environment::PASSWORD).ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_relative_and_absolute() {
        // Relative to where tests run, the crate's directory.
        let name = format!("lake-identity-{}", uuid::Uuid::new_v4().simple());
        fs::create_dir_all(&name).unwrap();
        fs::write(Path::new(&name).join(FILE), "local = \"lake\"\n").unwrap();
        let relative = Location::find(Some(Path::new(&name))).unwrap();
        let absolute = Location::find(Some(&std::path::absolute(&name).unwrap())).unwrap();
        let dotted = Location::find(Some(&Path::new(&name).join("."))).unwrap();
        fs::remove_dir_all(&name).unwrap();
        assert!(
            Path::new(&relative.identity()["local:".len()..]).is_absolute(),
            "{}",
            relative.identity()
        );
        assert_eq!(relative.identity(), absolute.identity());
        assert_eq!(dotted.identity(), absolute.identity());
    }

    #[test]
    fn config_file_omits_defaults() {
        let plain = Location {
            postgres: Some("postgresql://host/db".into()),
            local: None,
            schema: None,
            scheduler: None,
            auth: None,
            engines: Engines::new(),
            directory: None,
        };
        assert_eq!(
            toml::to_string(&plain).unwrap(),
            "postgres = \"postgresql://host/db\"\n"
        );
        let named = Location {
            postgres: Some("host=h".into()),
            local: None,
            schema: Some("other".into()),
            scheduler: None,
            auth: Some("aws-iam".into()),
            engines: Engines::new(),
            directory: None,
        };
        let own = Location {
            postgres: None,
            local: Some("./lake".into()),
            schema: None,
            scheduler: None,
            auth: None,
            engines: Engines::new(),
            directory: None,
        };
        assert!(toml::to_string(&named).unwrap().contains("auth = \"aws-iam\"\n"));
        assert_eq!(named.auth().unwrap(), Auth::AwsIam);
        let unknown = Location {
            auth: Some("kerberos".into()),
            ..named.clone()
        };
        assert!(unknown.config(None).is_err(), "an auth no client knows is refused");
        let own_in_cloud = Location {
            auth: Some("gcp-iam".into()),
            ..own.clone()
        };
        let refused = own_in_cloud.config(None).unwrap_err().to_string();
        assert!(
            refused.contains("a lake's own Postgres signs in by its own password"),
            "{refused}"
        );
        assert_eq!(toml::to_string(&own).unwrap(), "local = \"./lake\"\n");
        assert!(is_postgres("postgresql://h/db") && is_postgres("host=h user=u"));
        assert!(!is_postgres("./lake") && !is_postgres(r"C:\lake"));
        assert_eq!(
            toml::from_str::<Location>(&toml::to_string(&named).unwrap()).unwrap(),
            named
        );
        assert_eq!(plain.schema(), SCHEMA);
        assert!(toml::from_str::<Location>("postgres = 'h'\nwarehouse = 's3://b'\n").is_err());
    }

    #[test]
    fn location_forms() {
        let postgres = Location::named(Some("postgresql://u@h/db"), Some("other")).unwrap();
        assert_eq!(postgres.postgres.as_deref(), Some("postgresql://u@h/db"));
        assert_eq!(postgres.schema(), "other");

        let dir = env::temp_dir().join(format!("metalake-named-{}", std::process::id()));
        let own = dir.join("lake");
        fs::create_dir_all(own.join(CLUSTER)).unwrap();
        let found = Location::named(own.to_str(), None).unwrap();
        assert_eq!(found.local.as_deref(), Some(own.as_path()));
        assert_eq!(found.schema(), SCHEMA);

        let empty = dir.join("empty");
        fs::create_dir_all(&empty).unwrap();
        let refused = Location::named(empty.to_str(), None).unwrap_err();
        assert!(refused.message.contains("neither"), "{refused}");

        fs::write(dir.join(FILE), "local = \"./lake\"\nschema = \"named\"\n").unwrap();
        let from_file = Location::named(dir.to_str(), None).unwrap();
        assert_eq!(from_file.local.as_deref(), Some(own.as_path()));
        assert_eq!(from_file.schema(), "named");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn warehouse_url_forms() {
        assert_eq!(warehouse_url("s3://bucket/prefix").unwrap(), "s3://bucket/prefix");
        let dir = env::temp_dir().join(format!("metalake-warehouse-{}", std::process::id()));
        let url = warehouse_url(dir.to_str().unwrap()).unwrap();
        assert!(url.starts_with("file:///") && url.ends_with('/'), "{url}");
        assert!(dir.is_dir());
        fs::remove_dir(dir).unwrap();
    }

    fn trino(url: &str) -> EngineConfig {
        EngineConfig {
            kind: crate::EngineKind::Trino,
            url: url.into(),
            user: Some("ana".into()),
            token_env: Some("TRINO_TOKEN".into()),
            password_env: None,
            catalog: None,
            lake_url: None,
        }
    }

    /// An engine is named in the file whether or not anything runs, read back as named, and forgotten; what else the
    /// file holds, its comments too, is kept.
    #[test]
    fn engines_are_named_in_the_file_and_the_rest_is_kept() {
        let dir = std::env::temp_dir().join(format!("engines_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join(FILE);
        let written = "# the lake of the team
postgres = \"postgresql://a@h/db\"
warehouse = \"gs://b/lake\"
";
        fs::write(
            &file,
            written.replace(
                "warehouse = \"gs://b/lake\"
",
                "",
            ),
        )
        .unwrap();

        add_engine_in(&file, "trino_prod", &trino("https://trino.example:8443"), false).unwrap();
        let read = Location::read(&file).unwrap();
        assert_eq!(
            read.engines.get("trino_prod"),
            Some(&trino("https://trino.example:8443"))
        );
        let text = fs::read_to_string(&file).unwrap();
        assert!(
            text.starts_with(
                "# the lake of the team
postgres"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "[engines.trino_prod]
"
            ),
            "{text}"
        );
        assert!(
            !text.contains(
                "[engines]
"
            ),
            "no empty header: {text}"
        );

        let taken = add_engine_in(&file, "trino_prod", &trino("https://other:8443"), false).unwrap_err();
        assert!(taken.to_string().contains("--replace"), "{taken}");
        add_engine_in(&file, "trino_prod", &trino("https://other:8443"), true).unwrap();
        assert_eq!(
            Location::read(&file).unwrap().engines["trino_prod"].url,
            "https://other:8443"
        );

        assert!(add_engine_in(&file, "Bad-Name", &trino("https://x"), false).is_err());
        assert!(add_engine_in(&file, "t", &trino("ftp://x"), false).is_err());

        remove_engine_in(&file, "trino_prod").unwrap();
        assert!(Location::read(&file).unwrap().engines.is_empty());
        assert!(remove_engine_in(&file, "trino_prod").is_err());
        assert!(!dir.join("metalake.toml.writing").exists());
        let _ = fs::remove_dir_all(dir);
    }
}
