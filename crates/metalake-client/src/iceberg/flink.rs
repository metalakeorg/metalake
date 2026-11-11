//! Flink's profile. Every jar the lake's catalog needs goes on the classpath Flink starts with: Iceberg's Flink runtime,
//! on an object store the cloud's bundle, and Hadoop's client, whose classes the runtime needs. Flink loads
//! `org.apache.hadoop` from there alone, and Iceberg makes its REST catalog by name with the JVM's own class loader, so
//! no jar given to an environment that runs already, by `pipeline.jars` or `ADD JAR`, serves. PyFlink's gateway and
//! Flink's scripts add `HADOOP_CLASSPATH` to the classpath they start with, which is how the jars get there. Then the
//! lake as a REST catalog, with the header that asks the server for credentials on an object store. The jars are where
//! [`super::jars`] fetches them: [`super::Iceberg::profile`] fetches them, the pure profile only names them.

use super::jars;
use super::profile::{Artifact, DELEGATION, Engine, FLINK, Profile, Target};
use crate::{Error, Result};

/// The property Flink's `CREATE CATALOG` names first, the kind of catalog.
const TYPE: &str = "type";
/// What Flink adds to the classpath it starts with, read as its JVM starts: PyFlink's gateway, the SQL client, a
/// cluster's scripts.
pub const HADOOP_CLASSPATH: &str = "HADOOP_CLASSPATH";

pub(super) fn profile(name: &str, target: &Target) -> Result<Profile> {
    let mut profile = Profile::new(Engine::Flink, name, target);
    profile.artifacts = std::iter::once(Artifact::iceberg(format!("iceberg-flink-runtime-{FLINK}")))
        .chain(target.store.bundle())
        .chain([
            Artifact::hadoop("hadoop-client-api"),
            Artifact::hadoop("hadoop-client-runtime"),
        ])
        .collect();

    let directory = jars::directory()?;
    let paths = profile
        .artifacts
        .iter()
        .map(|artifact| jars::path(&directory, artifact));
    let classpath = std::env::join_paths(paths).map_err(|e| Error::failed(format!("the jars as a classpath: {e}")))?;
    profile
        .startup
        .insert(HADOOP_CLASSPATH.into(), classpath.to_string_lossy().into_owned());

    profile.properties.insert(TYPE.into(), "iceberg".into());
    profile.properties.insert("catalog-type".into(), "rest".into());
    profile.properties.insert("uri".into(), target.uri.clone());
    if let Some(token) = &target.token {
        profile.properties.insert("token".into(), token.clone());
    }
    if target.store.bundle().is_some() {
        profile.properties.insert(DELEGATION.0.into(), DELEGATION.1.into());
    }
    Ok(profile)
}

/// The catalog made under the profile's name, `type` first as Flink's documents write it.
pub(super) fn statements(profile: &Profile) -> Vec<String> {
    let properties = profile
        .properties
        .get_key_value(TYPE)
        .into_iter()
        .chain(profile.properties.iter().filter(|(key, _)| *key != TYPE))
        .map(|(key, value)| format!("{} = {}", literal(key), literal(value)))
        .collect::<Vec<_>>()
        .join(", ");
    vec![format!("CREATE CATALOG {} WITH ({properties})", profile.name)]
}

/// As Flink's SQL client takes it: what it must start with, said first, then the statement ended.
pub(super) fn render(profile: &Profile) -> String {
    let mut sql = String::new();
    if let Some(classpath) = profile.startup.get(HADOOP_CLASSPATH) {
        sql.push_str(&format!(
            "-- Flink starts with the lake's jars: {HADOOP_CLASSPATH}={classpath}\n"
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
            token: None,
            region: None,
            endpoint: None,
        }
    }

    fn classpath(profile: &Profile) -> Vec<std::path::PathBuf> {
        std::env::split_paths(&profile.startup[HADOOP_CLASSPATH]).collect()
    }

    #[test]
    fn flink_profile_local() {
        let profile = profile(Engine::Flink, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        let classpath = classpath(&profile);
        assert_eq!(classpath.len(), 3, "{classpath:?}");
        assert!(
            classpath[0].ends_with(".metalake/jars/iceberg-flink-runtime-1.20-1.11.0.jar"),
            "{classpath:?}"
        );
        assert!(classpath[1].ends_with("hadoop-client-api-3.4.1.jar"), "{classpath:?}");
        assert!(
            classpath[2].ends_with("hadoop-client-runtime-3.4.1.jar"),
            "{classpath:?}"
        );
        assert_eq!(profile.startup.len(), 1, "the classpath alone: {:?}", profile.startup);
        assert_eq!(profile.artifacts.len(), 3, "each fetched");

        assert_eq!(
            profile.statements(),
            ["CREATE CATALOG lake WITH ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://127.0.0.1:8181')"]
        );
        let rendered = profile.render();
        assert!(
            rendered.starts_with("-- Flink starts with the lake's jars: HADOOP_CLASSPATH="),
            "{rendered}"
        );
        assert!(rendered.ends_with("'uri' = 'http://127.0.0.1:8181');\n"), "{rendered}");
    }

    #[test]
    fn flink_profile_object_store() {
        let profile = profile(Engine::Flink, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        let classpath = classpath(&profile);
        assert_eq!(classpath.len(), 4);
        assert!(classpath[1].ends_with("iceberg-aws-bundle-1.11.0.jar"), "{classpath:?}");
        let statements = profile.statements();
        assert!(
            statements[0].contains("'header.X-Iceberg-Access-Delegation' = 'vended-credentials'"),
            "{statements:?}"
        );
    }

    #[test]
    fn flink_profile_token() {
        let mut target = target(Store::Local, "file:///tmp/lake/");
        target.token = Some("the-token".into());
        let profile = profile(Engine::Flink, "lake", &target).unwrap();
        assert!(
            profile.statements()[0].contains("'token' = 'the-token'"),
            "{:?}",
            profile.statements()
        );
    }

    #[test]
    fn flink_profile_name() {
        let profile = profile(Engine::Flink, "prod", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(profile.statements()[0].starts_with("CREATE CATALOG prod WITH"));
    }
}
