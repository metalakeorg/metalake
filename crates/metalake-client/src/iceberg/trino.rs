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
    if let Some(on) = &target.asks() {
        set("iceberg.rest-catalog.warehouse", on);
    }
    if let Some(token) = &target.token {
        set("iceberg.rest-catalog.security", "OAUTH2");
        set("iceberg.rest-catalog.oauth2.token", token);
    }
    match target.store {
        Store::Local => set("fs.hadoop.enabled", "true"),
        // Hadoop's file system with the cluster's configuration, which resolves a nameservice and its NameNodes, and
        // Trino's own Kerberos identity: nothing vended.
        Store::Hdfs => {
            set("fs.hadoop.enabled", "true");
            if let Some(conf) = &target.hadoop_conf {
                let conf = std::path::Path::new(conf);
                let files: Vec<String> = ["core-site.xml", "hdfs-site.xml"]
                    .iter()
                    .map(|file| conf.join(file).display().to_string())
                    .collect();
                set("hive.config.resources", &files.join(","));
            }
        }
        store => {
            set("iceberg.rest-catalog.vended-credentials-enabled", "true");
            set(native(store), "true");
            // Trino's own S3 file system is told where the store is; the credentials it is vended say who reads it.
            if store == Store::S3 {
                if let Some(region) = &target.region {
                    set("s3.region", region);
                }
                if let Some(endpoint) = &target.endpoint {
                    set("s3.endpoint", endpoint);
                    set("s3.path-style-access", "true");
                }
            }
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
        Store::Local | Store::Hdfs => unreachable!("a local warehouse and HDFS are read through Hadoop's"),
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
    if target.store == Store::Hdfs {
        file.push_str(match &target.hadoop_conf {
            Some(_) => {
                "# Trino reads the cluster's configuration at the paths below: it must see them there.
"
            }
            None => {
                "# Set hive.config.resources to the cluster's core-site.xml,hdfs-site.xml as Trino sees them.
"
            }
        });
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
            region: None,
            endpoint: None,
            hadoop_conf: None,
            on: None,
            catalog: None,
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
    fn trino_profile_s3_endpoint() {
        let mut target = target(Store::S3, "s3://bucket/lake");
        target.region = Some("eu-west-1".into());
        target.endpoint = Some("http://127.0.0.1:9000".into());
        let profile = profile(Engine::Trino, "lake", &target).unwrap();
        assert_eq!(profile.properties["s3.region"], "eu-west-1");
        assert_eq!(profile.properties["s3.endpoint"], "http://127.0.0.1:9000");
        assert_eq!(profile.properties["s3.path-style-access"], "true");
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
    fn trino_profile_hdfs() {
        let mut target = target(Store::Hdfs, "hdfs://nameservice1/lake");
        let bare = profile(Engine::Trino, "lake", &target).unwrap();
        assert_eq!(bare.properties["fs.hadoop.enabled"], "true");
        assert!(
            !bare
                .properties
                .contains_key("iceberg.rest-catalog.vended-credentials-enabled")
        );
        assert!(!bare.properties.contains_key("hive.config.resources"));
        assert!(bare.render().contains("# Set hive.config.resources"));

        target.hadoop_conf = Some("/etc/hadoop/conf".into());
        let profile = profile(Engine::Trino, "lake", &target).unwrap();
        let resources = &profile.properties["hive.config.resources"];
        let files: Vec<&str> = resources.split(',').collect();
        assert_eq!(files.len(), 2, "{resources}");
        assert!(
            files[0].ends_with("core-site.xml") && files[1].ends_with("hdfs-site.xml"),
            "{resources}"
        );
        assert!(files.iter().all(|f| f.starts_with("/etc/hadoop/conf")), "{resources}");
        assert!(profile.render().contains("it must see them there"));
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
