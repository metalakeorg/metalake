//! Flink's profile: Iceberg's Flink runtime and Hadoop's client, whose classes it needs, as files on the job's classpath
//! (`pipeline.jars`), fixed as an environment starts, and the lake as a REST catalog made by a statement. On an object store, the cloud's bundle and the header
//! that asks the server for credentials. The jars are where [`super::jars`] fetches them: [`super::Iceberg::profile`]
//! fetches them, the pure profile only names them.

use url::Url;

use super::jars;
use super::profile::{Artifact, DELEGATION, Engine, FLINK, Profile, Target};
use crate::{Error, Result};

/// The property Flink's `CREATE CATALOG` names first, the kind of catalog.
const TYPE: &str = "type";

pub(super) fn profile(name: &str, target: &Target) -> Result<Profile> {
    let mut profile = Profile::new(Engine::Flink, name, target);
    profile
        .artifacts
        .push(Artifact::iceberg(format!("iceberg-flink-runtime-{FLINK}")));
    profile.artifacts.push(Artifact::hadoop("hadoop-client-api"));
    profile.artifacts.push(Artifact::hadoop("hadoop-client-runtime"));
    profile.artifacts.extend(target.store.bundle());

    let directory = jars::directory()?;
    let files = profile
        .artifacts
        .iter()
        .map(|artifact| {
            let path = jars::path(&directory, artifact);
            Url::from_file_path(&path)
                .map(String::from)
                .map_err(|()| Error::failed(format!("{} is not a file's path", path.display())))
        })
        .collect::<Result<Vec<_>>>()?;
    profile.startup.insert("pipeline.jars".into(), files.join(";"));

    profile.properties.insert(TYPE.into(), "iceberg".into());
    profile.properties.insert("catalog-type".into(), "rest".into());
    profile.properties.insert("uri".into(), target.uri.clone());
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

/// As Flink's SQL client takes it: the jars set, then the catalog made.
pub(super) fn render(profile: &Profile) -> String {
    let mut sql = String::new();
    for (key, value) in &profile.startup {
        sql.push_str(&format!("SET {} = {};\n", literal(key), literal(value)));
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
        assert_eq!(
            profile.statements(),
            ["CREATE CATALOG lake WITH ('type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = 'http://127.0.0.1:8181')"]
        );
        let jars = &profile.startup["pipeline.jars"];
        let files: Vec<_> = jars.split(';').collect();
        assert_eq!(
            files.len(),
            3,
            "the runtime and Hadoop's client on a local warehouse: {files:?}"
        );
        assert!(files.iter().all(|file| file.starts_with("file://")), "{files:?}");
        assert!(
            files[0].ends_with("/.metalake/jars/iceberg-flink-runtime-1.20-1.11.0.jar"),
            "{files:?}"
        );
        assert!(files[1].ends_with("/hadoop-client-api-3.4.1.jar"), "{files:?}");
        assert!(files[2].ends_with("/hadoop-client-runtime-3.4.1.jar"), "{files:?}");
        let rendered = profile.render();
        assert!(
            rendered.starts_with(&format!("SET 'pipeline.jars' = '{jars}';\n")),
            "{rendered}"
        );
        assert!(rendered.ends_with("'uri' = 'http://127.0.0.1:8181');\n"), "{rendered}");
    }

    #[test]
    fn flink_profile_object_store() {
        let profile = profile(Engine::Flink, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        let jars: Vec<_> = profile.startup["pipeline.jars"].split(';').collect();
        assert_eq!(jars.len(), 4);
        assert!(jars[3].ends_with("/iceberg-aws-bundle-1.11.0.jar"), "{jars:?}");
        assert!(
            profile.statements()[0].contains("'header.X-Iceberg-Access-Delegation' = 'vended-credentials'"),
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
