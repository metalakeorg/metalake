//! PyIceberg's profile: a REST catalog by its URI, nothing else on any store, since PyIceberg asks the server for
//! credentials by default and applies what it vends. Polars and Daft take the catalog it gives as it is.

use super::profile::{Engine, Profile, Store, Target};

pub(super) fn profile(name: &str, target: &Target) -> Profile {
    let mut profile = Profile::new(Engine::PyIceberg, name, target);
    profile.properties.insert("type".into(), "rest".into());
    profile.properties.insert("uri".into(), target.uri.clone());
    if let Some(token) = &target.token {
        profile.properties.insert("token".into(), token.clone());
    }
    // PyArrow's file IO, PyIceberg's default, reads `file:///C:/…` as the path `/C:/…`, which Windows refuses; fsspec's
    // reads a drive's path as it is.
    if target.store == Store::Local && on_a_drive(&target.warehouse) {
        profile
            .properties
            .insert("py-io-impl".into(), "pyiceberg.io.fsspec.FsspecFileIO".into());
    }
    profile
}

/// Whether a `file:` URL is of a path on a Windows drive, `file:///C:/…`.
fn on_a_drive(warehouse: &str) -> bool {
    let path = warehouse.strip_prefix("file:///").unwrap_or("").as_bytes();
    path.len() >= 2 && path[0].is_ascii_alphabetic() && path[1] == b':'
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
            token: None,
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
    fn pyiceberg_profile_windows_drive() {
        let profile = profile(
            Engine::PyIceberg,
            "lake",
            &target(Store::Local, "file:///C:/Users/a/lake/"),
        )
        .unwrap();
        assert_eq!(profile.properties["py-io-impl"], "pyiceberg.io.fsspec.FsspecFileIO");
        assert!(on_a_drive("file:///d:/lake/"));
        assert!(!on_a_drive("file:///tmp/lake/") && !on_a_drive("s3://c:/lake"));
    }

    #[test]
    fn pyiceberg_profile_token() {
        let mut target = target(Store::Local, "file:///tmp/lake/");
        target.token = Some("the-token".into());
        let profile = profile(Engine::PyIceberg, "lake", &target).unwrap();
        assert_eq!(profile.properties["token"], "the-token");
        assert!(
            profile.render().contains("    token: \"the-token\"\n"),
            "{}",
            profile.render()
        );
    }

    #[test]
    fn pyiceberg_profile_name() {
        let profile = profile(Engine::PyIceberg, "prod", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert!(profile.render().starts_with("catalog:\n  prod:\n"));
    }
}
