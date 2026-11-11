//! PyIceberg's profile: a REST catalog by its URI, nothing else on any store, since PyIceberg asks the server for
//! credentials by default and applies what it vends. Polars and Daft take the catalog it gives as it is.

use super::profile::{Engine, Profile, Target};

pub(super) fn profile(name: &str, target: &Target) -> Profile {
    let mut profile = Profile::new(Engine::PyIceberg, name, target);
    profile.properties.insert("type".into(), "rest".into());
    profile.properties.insert("uri".into(), target.uri.clone());
    profile
}

/// As `~/.pyiceberg.yaml` has it, where every tool that loads catalogs through PyIceberg finds it by its name.
pub(super) fn render(profile: &Profile) -> String {
    let mut yaml = format!("catalog:\n  {}:\n", profile.name);
    for (key, value) in &profile.properties {
        // A JSON string is a YAML one: quoted, so that no value is read as anything else.
        yaml.push_str(&format!("    {key}: {}\n", serde_json::Value::from(value.as_str())));
    }
    yaml
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
    fn pyiceberg_profile_local() {
        let profile = profile(Engine::PyIceberg, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        let properties: Vec<_> = profile
            .properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(properties, [("type", "rest"), ("uri", "http://127.0.0.1:8181")]);
        assert!(profile.startup.is_empty() && profile.artifacts.is_empty());
        assert!(profile.statements().is_empty());
        assert_eq!(
            profile.render(),
            "catalog:\n  lake:\n    type: \"rest\"\n    uri: \"http://127.0.0.1:8181\"\n"
        );
    }

    #[test]
    fn pyiceberg_profile_object_store() {
        let local = profile(Engine::PyIceberg, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        let s3 = profile(Engine::PyIceberg, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        assert_eq!(
            (s3.properties, s3.startup, s3.artifacts),
            (local.properties, local.startup, local.artifacts),
            "it asks for credentials by default"
        );
    }

    #[test]
    fn pyiceberg_profile_name() {
        let profile = profile(Engine::PyIceberg, "prod", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(profile.render().starts_with("catalog:\n  prod:\n"));
    }
}
