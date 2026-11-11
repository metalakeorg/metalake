//! DuckDB's profile: its `iceberg` extension, and the lake attached as an Iceberg REST catalog by its endpoint, with no
//! authorization while the server has none. Nothing more on any store: DuckDB asks the server for credentials by
//! default, and renews an expiring one through `loadCredentials`.

use super::profile::{Engine, Profile, Target};

/// The option DuckDB's `ATTACH` names first, the kind of catalog.
const TYPE: &str = "TYPE";

pub(super) fn profile(name: &str, target: &Target) -> Profile {
    let mut profile = Profile::new(Engine::DuckDb, name);
    profile.properties.insert(TYPE.into(), "iceberg".into());
    profile.properties.insert("ENDPOINT".into(), target.uri.clone());
    profile.properties.insert("AUTHORIZATION_TYPE".into(), "none".into());
    profile
}

/// The extension, then the catalog attached under the profile's name: `TYPE` first and bare, as DuckDB's documents
/// write it, then each option a string.
pub(super) fn statements(profile: &Profile) -> Vec<String> {
    let kind = profile.properties.get(TYPE).map(|kind| format!("{TYPE} {kind}"));
    let options = kind
        .into_iter()
        .chain(
            profile
                .properties
                .iter()
                .filter(|(key, _)| *key != TYPE)
                .map(|(key, value)| format!("{key} {}", literal(value))),
        )
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        "INSTALL iceberg".into(),
        "LOAD iceberg".into(),
        format!("ATTACH '' AS {} ({options})", profile.name),
    ]
}

/// As the DuckDB shell takes it: each statement ended.
pub(super) fn render(profile: &Profile) -> String {
    statements(profile)
        .iter()
        .map(|statement| format!("{statement};\n"))
        .collect()
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
    fn duckdb_profile_local() {
        let profile = profile(Engine::DuckDb, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        assert_eq!(profile.properties.len(), 3);
        assert!(profile.startup.is_empty() && profile.artifacts.is_empty());
        assert_eq!(
            profile.statements(),
            [
                "INSTALL iceberg",
                "LOAD iceberg",
                "ATTACH '' AS lake (TYPE iceberg, AUTHORIZATION_TYPE 'none', ENDPOINT 'http://127.0.0.1:8181')",
            ]
        );
        assert_eq!(
            profile.render(),
            "INSTALL iceberg;\nLOAD iceberg;\n\
             ATTACH '' AS lake (TYPE iceberg, AUTHORIZATION_TYPE 'none', ENDPOINT 'http://127.0.0.1:8181');\n"
        );
    }

    #[test]
    fn duckdb_profile_object_store() {
        let local = profile(Engine::DuckDb, "lake", &target(Store::Local, "file:///tmp/lake/")).unwrap();
        let s3 = profile(Engine::DuckDb, "lake", &target(Store::S3, "s3://bucket/lake")).unwrap();
        assert_eq!(s3, local, "it asks for credentials by default");
    }

    #[test]
    fn duckdb_literal_quoting() {
        assert_eq!(literal("it's"), "'it''s'");
    }
}
