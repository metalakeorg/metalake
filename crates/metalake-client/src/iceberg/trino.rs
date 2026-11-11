//! Trino's profile: a catalog file, since Trino is a server of its own that a deployment starts, and its catalog's name
//! is the file's. The lake as an Iceberg REST catalog; on a local warehouse, Hadoop's file system, which reads it at
//! its own path; on an object store, the credentials the server vends, with Trino's file system for that store.
//! Vending is off on a local warehouse, where Trino refuses a `file` location with it on.

use super::profile::{Engine, Profile, Store, Target};

pub(super) fn profile(name: &str, target: &Target) -> Profile {
    let mut profile = Profile::new(Engine::Trino, name, target);
    let mut set = |key: &str, value: &str| {
        profile.properties.insert(key.into(), value.into());
    };
    set("connector.name", "iceberg");
    set("iceberg.catalog.type", "rest");
    set("iceberg.rest-catalog.uri", &target.uri);
    if let Some(token) = &target.token {
        set("iceberg.rest-catalog.security", "OAUTH2");
        set("iceberg.rest-catalog.oauth2.token", token);
    }
    match target.store {
        Store::Local => set("fs.hadoop.enabled", "true"),
        store => {
            set("iceberg.rest-catalog.vended-credentials-enabled", "true");
            set(native(store), "true");
        }
    }
    profile
}

/// Trino's own file system for an object store.
fn native(store: Store) -> &'static str {
    match store {
        Store::S3 => "fs.native-s3.enabled",
        Store::Gcs => "fs.native-gcs.enabled",
        Store::Azure => "fs.native-azure.enabled",
        Store::Local => unreachable!("a local warehouse is read through Hadoop's"),
    }
}

/// As `etc/catalog/<name>.properties` has it. On a local warehouse, a comment says where Trino must see it.
pub(super) fn render(profile: &Profile) -> String {
    let target = &profile.target;
    let mut file = format!("# etc/catalog/{}.properties\n", profile.name);
    if target.store == Store::Local {
        file.push_str(&format!(
            "# Trino reads the lake's files at their own path: it must see {} there.\n",
            target.warehouse
        ));
    }
    for (key, value) in &profile.properties {
        file.push_str(&format!("{key}={value}\n"));
    }
    file
}

#[cfg(test)]
mod tests {
    use super::super::profile::profile;
    use super::*;

    fn target(store: Store, warehouse: &str) -> Target {
        Target {
            uri: "http://127.0.0.1:8181".into(),
            store,
            warehouse: warehouse.into(),
            token: None,
        }
    }

    #[test]
    fn trino_profile_local() {
        let target = target(Store::Local, "file:///tmp/lake/");
        let profile = profile(Engine::Trino, "lake", &target).unwrap();
        assert!(profile.startup.is_empty() && profile.artifacts.is_empty());
        assert!(profile.statements().is_empty());
        assert_eq!(
            profile.render(),
            "# etc/catalog/lake.properties\n\
             # Trino reads the lake's files at their own path: it must see file:///tmp/lake/ there.\n\
             connector.name=iceberg\n\
             fs.hadoop.enabled=true\n\
             iceberg.catalog.type=rest\n\
             iceberg.rest-catalog.uri=http://127.0.0.1:8181\n"
        );
    }

    #[test]
    fn trino_profile_object_store() {
        let target = target(Store::S3, "s3://bucket/lake");
        let profile = profile(Engine::Trino, "lake", &target).unwrap();
        assert_eq!(
            profile.properties["iceberg.rest-catalog.vended-credentials-enabled"],
            "true"
        );
        assert_eq!(profile.properties["fs.native-s3.enabled"], "true");
        assert!(!profile.properties.contains_key("fs.hadoop.enabled"));
        assert!(!profile.render().contains("their own path"));
    }

    #[test]
    fn trino_file_system_by_store() {
        for (store, warehouse, key) in [
            (Store::Gcs, "gs://b/lake", "fs.native-gcs.enabled"),
            (
                Store::Azure,
                "abfss://c@a.dfs.core.windows.net/lake",
                "fs.native-azure.enabled",
            ),
        ] {
            let profile = profile(Engine::Trino, "lake", &target(store, warehouse)).unwrap();
            assert_eq!(profile.properties[key], "true", "{store:?}");
        }
    }

    #[test]
    fn trino_profile_token() {
        let mut target = target(Store::Local, "file:///tmp/lake/");
        target.token = Some("the-token".into());
        let profile = profile(Engine::Trino, "lake", &target).unwrap();
        assert_eq!(profile.properties["iceberg.rest-catalog.security"], "OAUTH2");
        assert_eq!(profile.properties["iceberg.rest-catalog.oauth2.token"], "the-token");
    }

    #[test]
    fn trino_profile_name() {
        let target = target(Store::Local, "file:///tmp/lake/");
        let profile = profile(Engine::Trino, "prod", &target).unwrap();
        assert!(profile.render().starts_with("# etc/catalog/prod.properties\n"));
    }
}
