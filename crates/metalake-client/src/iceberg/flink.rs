//! Flink's profile. Hadoop's client, whose classes Iceberg's runtime needs, goes on the classpath Flink starts with
//! (`HADOOP_CLASSPATH`): Flink loads `org.apache.hadoop` from there alone, whatever jars it is given later. Iceberg's
//! runtime, and on an object store the cloud's bundle, are added with `ADD JAR`, which puts them where Flink resolves
//! a catalog's classes, in an environment that runs already; `pipeline.jars` names them for an environment's
//! configuration as it starts. Then the lake as a REST catalog, with the header that asks the server for credentials
//! on an object store. The jars are where [`super::jars`] fetches them: [`super::Iceberg::profile`] fetches them, the
//! pure profile only names them.

use std::path::PathBuf;

use url::Url;

use super::jars;
use super::profile::{Artifact, DELEGATION, Engine, FLINK, Profile, Target};
use crate::{Error, Result};

/// The property Flink's `CREATE CATALOG` names first, the kind of catalog.
const TYPE: &str = "type";
/// What Flink starts with on its classpath, read as its JVM starts: PyFlink's gateway, the SQL client, a cluster.
pub const HADOOP_CLASSPATH: &str = "HADOOP_CLASSPATH";

pub(super) fn profile(name: &str, target: &Target) -> Result<Profile> {
    let mut profile = Profile::new(Engine::Flink, name, target);
    let hadoop = [
        Artifact::hadoop("hadoop-client-api"),
        Artifact::hadoop("hadoop-client-runtime"),
    ];
    let added: Vec<Artifact> = std::iter::once(Artifact::iceberg(format!("iceberg-flink-runtime-{FLINK}")))
        .chain(target.store.bundle())
        .collect();

    let directory = jars::directory()?;
    let paths = |artifacts: &[Artifact]| -> Vec<PathBuf> {
        artifacts
            .iter()
            .map(|artifact| jars::path(&directory, artifact))
            .collect()
    };
    let classpath = std::env::join_paths(paths(&hadoop))
        .map_err(|e| Error::failed(format!("Hadoop's jars as a classpath: {e}")))?;
    profile
        .startup
        .insert(HADOOP_CLASSPATH.into(), classpath.to_string_lossy().into_owned());
    let files = paths(&added)
        .iter()
        .map(|path| {
            Url::from_file_path(path)
                .map(String::from)
                .map_err(|()| Error::failed(format!("{} is not a file's path", path.display())))
        })
        .collect::<Result<Vec<_>>>()?;
    profile.startup.insert("pipeline.jars".into(), files.join(";"));
    profile.artifacts = added.into_iter().chain(hadoop).collect();

    profile.properties.insert(TYPE.into(), "iceberg".into());
    profile.properties.insert("catalog-type".into(), "rest".into());
    profile.properties.insert("uri".into(), target.uri.clone());
    if target.store.bundle().is_some() {
        profile.properties.insert(DELEGATION.0.into(), DELEGATION.1.into());
    }
    Ok(profile)
}

/// Each of Iceberg's jars added, then the catalog made under the profile's name, `type` first as Flink's documents
/// write it.
pub(super) fn statements(profile: &Profile) -> Vec<String> {
    let jars = profile.startup.get("pipeline.jars").map(String::as_str).unwrap_or("");
    let mut statements: Vec<String> = jars
        .split(';')
        .filter(|jar| !jar.is_empty())
        .map(|jar| format!("ADD JAR {}", literal(jar)))
        .collect();
    let properties = profile
        .properties
        .get_key_value(TYPE)
        .into_iter()
        .chain(profile.properties.iter().filter(|(key, _)| *key != TYPE))
        .map(|(key, value)| format!("{} = {}", literal(key), literal(value)))
        .collect::<Vec<_>>()
        .join(", ");
    statements.push(format!("CREATE CATALOG {} WITH ({properties})", profile.name));
    statements
}

/// As Flink's SQL client takes it: what it must start with, said first, then each statement ended.
pub(super) fn render(profile: &Profile) -> String {
    let mut sql = String::new();
    if let Some(classpath) = profile.startup.get(HADOOP_CLASSPATH) {
        sql.push_str(&format!(
            "-- Flink starts with Hadoop's client: {HADOOP_CLASSPATH}={classpath}\n"
        ));
    }
    for statement in statements(profile) {
        sql.push_str(&format!("{statement};\n"));
    }
    sql
}

/// A SQL string literal.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::super::profile::{Store, profile};
    use super::*;

    fn target(store: Store, warehouse: &str) -> Target {
        Target {
            uri: "http://127.0.0.1:8181".into(),
            store,
            warehouse: warehouse.into(),
        }
    }

    #[test]
    fn flink_profile_local() {
        let profile = profile(Engine::Flink, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        let jars = &profile.startup["pipeline.jars"];
        assert!(
            jars.starts_with("file://") && !jars.contains(';'),
            "Iceberg's runtime alone: {jars}"
        );
        assert!(
            jars.ends_with("/.metalake/jars/iceberg-flink-runtime-1.20-1.11.0.jar"),
            "{jars}"
        );

        let classpath: Vec<_> = std::env::split_paths(&profile.startup[HADOOP_CLASSPATH]).collect();
        assert_eq!(classpath.len(), 2, "{classpath:?}");
        assert!(classpath[0].ends_with("hadoop-client-api-3.4.1.jar"), "{classpath:?}");
        assert!(
            classpath[1].ends_with("hadoop-client-runtime-3.4.1.jar"),
            "{classpath:?}"
        );
        assert_eq!(profile.artifacts.len(), 3, "the runtime and Hadoop's two, to fetch");

        assert_eq!(
            profile.statements(),
            [
                format!("ADD JAR '{jars}'"),
                "CREATE CATALOG lake WITH ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://127.0.0.1:8181')"
                    .to_string(),
            ]
        );
        let rendered = profile.render();
        assert!(
            rendered.starts_with("-- Flink starts with Hadoop's client: HADOOP_CLASSPATH="),
            "{rendered}"
        );
        assert!(rendered.contains(&format!("\nADD JAR '{jars}';\n")), "{rendered}");
        assert!(rendered.ends_with("'uri' = 'http://127.0.0.1:8181');\n"), "{rendered}");
    }

    #[test]
    fn flink_profile_object_store() {
        let profile = profile(Engine::Flink, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        let jars: Vec<_> = profile.startup["pipeline.jars"].split(';').collect();
        assert_eq!(jars.len(), 2);
        assert!(jars[1].ends_with("/iceberg-aws-bundle-1.11.0.jar"), "{jars:?}");
        let statements = profile.statements();
        assert!(
            statements[2].contains("'header.X-Iceberg-Access-Delegation' = 'vended-credentials'"),
            "{statements:?}"
        );
    }

    #[test]
    fn flink_profile_name() {
        let profile = profile(Engine::Flink, "prod", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(profile.statements()[1].starts_with("CREATE CATALOG prod WITH"));
    }
}
