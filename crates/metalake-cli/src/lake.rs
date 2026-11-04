//! Which lake a command means, and the one place that knows how to reach it. What a lake is lives in its catalog; a
//! client holds only where the catalog is, and secrets come from the environment, never from a file.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use metalake::{Catalog, CatalogConfig, Storage};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::args::Init;
use crate::failure::Failure;

/// The file `init` writes, which a command finds in its directory or the nearest above it.
const FILE: &str = "metalake.toml";
/// The catalog's schema, unless one Postgres database holds more than one lake.
const SCHEMA: &str = "metalake";
/// Enough for one statement: a command runs one at a time, and the core's default is for a long-lived engine.
const CONNECTIONS: usize = 4;
/// The services whose variables `object_store` reads, by the same names in lower case.
const STORAGE_ENV: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];

/// Where a catalog is: all that a client has to know.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    postgres: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
}

impl Location {
    fn schema(&self) -> &str {
        self.schema.as_deref().unwrap_or(SCHEMA)
    }

    fn config(&self) -> CatalogConfig {
        let mut config = CatalogConfig::new(with_password(&self.postgres), self.schema());
        config.connections = CONNECTIONS;
        config
    }

    /// The lake a command means: the directory it names, the environment, or the nearest file.
    fn find(dir: Option<&Path>) -> Result<Self, Failure> {
        if let Some(dir) = dir {
            return Self::read(&dir.join(FILE));
        }
        if let Ok(postgres) = env::var("METALAKE_POSTGRES") {
            let schema = env::var("METALAKE_SCHEMA").ok();
            return Ok(Self { postgres, schema });
        }
        let here = env::current_dir()?;
        match here.ancestors().map(|d| d.join(FILE)).find(|f| f.is_file()) {
            Some(file) => Self::read(&file),
            None => Err(Failure::failed(format!(
                "no {FILE} here or above: run `metalake init`, or set METALAKE_POSTGRES"
            ))),
        }
    }

    fn read(file: &Path) -> Result<Self, Failure> {
        let text = fs::read_to_string(file).map_err(|e| Failure::failed(format!("{}: {e}", file.display())))?;
        toml::from_str(&text).map_err(|e| Failure::failed(format!("{}: {e}", file.display())))
    }
}

/// A lake, reached.
#[derive(Debug)]
pub struct Lake {
    pub catalog: Catalog,
    location: Location,
}

impl Lake {
    /// The lake a command means.
    pub async fn open(dir: Option<&Path>) -> Result<Self, Failure> {
        let location = Location::find(dir)?;
        let catalog = Catalog::connect(location.config()).await?;
        Ok(Self { catalog, location })
    }

    /// Upgrades the catalog of the lake a command means, which `open` refuses, and reaches it.
    pub async fn migrate(dir: Option<&Path>) -> Result<(Self, Option<i64>), Failure> {
        let location = Location::find(dir)?;
        let format_migration = Catalog::migrate(&location.config()).await?;
        let catalog = Catalog::connect(location.config()).await?;
        Ok((Self { catalog, location }, format_migration))
    }

    /// Creates a catalog and the file that finds it. The file comes first, so that a lake already here is refused
    /// before a catalog exists, and it goes if the catalog fails.
    pub async fn create(init: &Init, dir: Option<&Path>) -> Result<Self, Failure> {
        if has_password(&init.postgres) {
            return Err(Failure::usage(
                "a password does not go in the command line or in a file: set PGPASSWORD",
            ));
        }
        let warehouse = warehouse_url(&init.warehouse)?;
        let location = Location {
            postgres: init.postgres.clone(),
            schema: init.schema.clone().filter(|s| s != SCHEMA),
        };
        let dir = dir.unwrap_or(Path::new("."));
        fs::create_dir_all(dir)?;
        let file = dir.join(FILE);
        let text = toml::to_string(&location).map_err(|e| Failure::failed(e.to_string()))?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)
            .and_then(|mut f| f.write_all(text.as_bytes()))
            .map_err(|e| match e.kind() {
                ErrorKind::AlreadyExists => Failure::failed(format!("{} already exists", file.display())),
                _ => Failure::failed(format!("{}: {e}", file.display())),
            })?;
        match Catalog::init(location.config().warehouse(warehouse), None).await {
            Ok(catalog) => Ok(Self { catalog, location }),
            Err(e) => {
                fs::remove_file(&file)?;
                Err(e.into())
            }
        }
    }

    /// The store of the catalog's warehouse, with the credentials of the environment. Only a verb that moves bytes
    /// asks for it.
    pub fn storage(&self) -> Result<Storage, Failure> {
        let warehouse = self
            .catalog
            .warehouse()
            .ok_or_else(|| Failure::failed("the catalog has no warehouse"))?;
        Ok(Storage::new(warehouse, storage_options(env::vars_os()))?)
    }

    pub fn schema(&self) -> &str {
        self.location.schema()
    }

    /// Where the catalog is, fit to print.
    pub fn postgres(&self) -> String {
        without_password(&self.location.postgres)
    }
}

/// The options of `object_store` among these variables, which it names in lower case.
fn storage_options(vars: impl Iterator<Item = (std::ffi::OsString, std::ffi::OsString)>) -> Vec<(String, String)> {
    vars.filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, _)| STORAGE_ENV.iter().any(|prefix| key.starts_with(prefix)))
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect()
}

/// A URL of a warehouse: as given if it is one, and a local directory, made if need be, as a `file:` URL.
fn warehouse_url(warehouse: &str) -> Result<String, Failure> {
    // A scheme of one letter is a Windows drive.
    if let Ok(url) = Url::parse(warehouse)
        && url.scheme().len() > 1
    {
        return Ok(warehouse.to_string());
    }
    fs::create_dir_all(warehouse)?;
    let path: PathBuf = std::path::absolute(warehouse)?;
    let url = Url::from_directory_path(&path)
        .map_err(|()| Failure::failed(format!("{} is not a directory", path.display())))?;
    Ok(url.to_string())
}

/// Whether a connection string is a URL, as opposed to libpq's `key=value` pairs.
fn as_url(postgres: &str) -> Option<Url> {
    Url::parse(postgres)
        .ok()
        .filter(|u| matches!(u.scheme(), "postgres" | "postgresql"))
}

fn has_password(postgres: &str) -> bool {
    match as_url(postgres) {
        Some(url) => url.password().is_some() || url.query_pairs().any(|(k, _)| k == "password"),
        None => postgres.split_whitespace().any(|pair| pair.starts_with("password=")),
    }
}

fn without_password(postgres: &str) -> String {
    match as_url(postgres) {
        Some(mut url) => {
            let kept: Vec<(String, String)> = url
                .query_pairs()
                .into_owned()
                .filter(|(k, _)| k != "password")
                .collect();
            let _ = url.set_password(None);
            url.set_query(None);
            if !kept.is_empty() {
                url.query_pairs_mut().extend_pairs(kept);
            }
            url.to_string()
        }
        None => {
            let kept = postgres
                .split_whitespace()
                .filter(|pair| !pair.starts_with("password="));
            kept.collect::<Vec<_>>().join(" ")
        }
    }
}

/// The connection string with `PGPASSWORD`, which the driver does not read, unless it already has one.
fn with_password(postgres: &str) -> String {
    match env::var("PGPASSWORD") {
        Ok(password) if !has_password(postgres) => joined(postgres, &password),
        _ => postgres.to_string(),
    }
}

fn joined(postgres: &str, password: &str) -> String {
    match as_url(postgres) {
        Some(mut url) => {
            let _ = url.set_password(Some(password));
            url.to_string()
        }
        None => {
            let quoted = password.replace('\\', "\\\\").replace('\'', "\\'");
            format!("{postgres} password='{quoted}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_says_only_what_differs() {
        let plain = Location {
            postgres: "postgresql://host/db".into(),
            schema: None,
        };
        assert_eq!(
            toml::to_string(&plain).unwrap(),
            "postgres = \"postgresql://host/db\"\n"
        );
        let named = Location {
            postgres: "host=h".into(),
            schema: Some("other".into()),
        };
        assert_eq!(
            toml::from_str::<Location>(&toml::to_string(&named).unwrap()).unwrap(),
            named
        );
        assert_eq!(plain.schema(), SCHEMA);
        assert!(toml::from_str::<Location>("postgres = 'h'\nwarehouse = 's3://b'\n").is_err());
    }

    #[test]
    fn a_password_is_seen_in_each_form() {
        for with in [
            "postgresql://u:secret@h/db",
            "postgres://u@h/db?password=secret",
            "host=h password=secret user=u",
        ] {
            assert!(has_password(with), "{with}");
            assert!(!without_password(with).contains("secret"), "{with}");
        }
        for without in [
            "postgresql://u@h/db",
            "host=h user=u",
            "host=h application_name=password",
        ] {
            assert!(!has_password(without), "{without}");
        }
        assert_eq!(without_password("postgres://u:s@h/db?a=1"), "postgres://u@h/db?a=1");
    }

    #[test]
    fn a_password_joins_each_form() {
        assert_eq!(joined("postgresql://u@h/db", "a b/c"), "postgresql://u:a%20b%2Fc@h/db");
        assert_eq!(joined("host=h", r"it's\"), r"host=h password='it\'s\\'");
    }

    #[test]
    fn storage_takes_its_services_variables_in_lower_case() {
        let vars = [
            ("AWS_REGION", "eu"),
            ("GOOGLE_SERVICE_ACCOUNT", "k"),
            ("AZURE_X", "y"),
            ("PATH", "p"),
        ];
        let options = storage_options(vars.into_iter().map(|(k, v)| (k.into(), v.into())));
        assert_eq!(
            options,
            [("aws_region", "eu"), ("google_service_account", "k"), ("azure_x", "y")]
                .map(|(k, v)| (k.to_string(), v.to_string()))
        );
    }

    #[test]
    fn a_warehouse_is_a_url() {
        assert_eq!(warehouse_url("s3://bucket/prefix").unwrap(), "s3://bucket/prefix");
        let dir = env::temp_dir().join(format!("metalake-warehouse-{}", std::process::id()));
        let url = warehouse_url(dir.to_str().unwrap()).unwrap();
        assert!(url.starts_with("file:///") && url.ends_with('/'), "{url}");
        assert!(dir.is_dir());
        fs::remove_dir(dir).unwrap();
    }
}
