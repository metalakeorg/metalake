//! Spark's profile: Iceberg's Spark runtime and SQL extensions, fixed as a session starts, and the lake as a REST
//! catalog, which Spark loads the first time the catalog is named and so can be set on a session already running. On
//! an object store, the cloud's bundle and the header that asks the server for credentials.

use super::profile::{Artifact, DELEGATION, Engine, Profile, SCALA, SPARK, Target};

/// Iceberg's SQL extensions: `MERGE INTO`, its procedures, `ALTER TABLE … WRITE ORDERED BY`.
const EXTENSIONS: &str = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
const CATALOG: &str = "org.apache.iceberg.spark.SparkCatalog";

pub(super) fn profile(name: &str, target: &Target) -> Profile {
    let mut profile = Profile::new(Engine::Spark, name, target);
    profile
        .artifacts
        .push(Artifact::iceberg(format!("iceberg-spark-runtime-{SPARK}_{SCALA}")));
    profile.artifacts.extend(target.store.bundle());

    let packages: Vec<String> = profile.artifacts.iter().map(Artifact::coordinate).collect();
    profile.startup.insert("spark.jars.packages".into(), packages.join(","));
    profile.startup.insert("spark.sql.extensions".into(), EXTENSIONS.into());

    let catalog = format!("spark.sql.catalog.{name}");
    let mut set = |key: &str, value: &str| {
        let key = if key.is_empty() {
            catalog.clone()
        } else {
            format!("{catalog}.{key}")
        };
        profile.properties.insert(key, value.into());
    };
    set("", CATALOG);
    set("type", "rest");
    set("uri", &target.uri);
    if let Some(token) = &target.token {
        set("token", token);
    }
    if target.store.bundle().is_some() {
        set(DELEGATION.0, DELEGATION.1);
    }
    profile
}

/// As `spark-defaults.conf` has it, a key and its value on each line: what starts a session first.
pub(super) fn render(profile: &Profile) -> String {
    profile
        .startup
        .iter()
        .chain(&profile.properties)
        .map(|(key, value)| format!("{key} {value}\n"))
        .collect()
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
        }
    }

    #[test]
    fn spark_profile_local() {
        let profile = profile(Engine::Spark, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(profile.statements().is_empty());
        assert_eq!(
            profile.render(),
            "spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0\n\
             spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\n\
             spark.sql.catalog.lake org.apache.iceberg.spark.SparkCatalog\n\
             spark.sql.catalog.lake.type rest\n\
             spark.sql.catalog.lake.uri http://127.0.0.1:8181\n"
        );
    }

    #[test]
    fn spark_profile_object_store() {
        let profile = profile(Engine::Spark, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        assert_eq!(
            profile.startup["spark.jars.packages"],
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
        );
        assert_eq!(
            profile.properties["spark.sql.catalog.lake.header.X-Iceberg-Access-Delegation"],
            "vended-credentials"
        );
    }

    #[test]
    fn spark_bundle_by_store() {
        let bundle = |store, warehouse| {
            let profile = profile(Engine::Spark, "lake", &target(store, warehouse)).unwrap();
            profile.artifacts.get(1).map(|a| a.name.clone())
        };
        assert_eq!(bundle(Store::Local, "file:///tmp/lake/"), None);
        assert_eq!(bundle(Store::Gcs, "gs://b/lake").as_deref(), Some("iceberg-gcp-bundle"));
        assert_eq!(
            bundle(Store::Azure, "abfss://c@a.dfs.core.windows.net/lake").as_deref(),
            Some("iceberg-azure-bundle")
        );
    }

    #[test]
    fn spark_profile_token() {
        let mut target = target(Store::Local, "file:///tmp/lake/");
        target.token = Some("the-token".into());
        let profile = profile(Engine::Spark, "lake", &target).unwrap();
        assert_eq!(profile.properties["spark.sql.catalog.lake.token"], "the-token");
    }

    #[test]
    fn spark_profile_name() {
        let profile = profile(Engine::Spark, "prod", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(
            profile
                .properties
                .keys()
                .all(|key| key.starts_with("spark.sql.catalog.prod"))
        );
    }
}
