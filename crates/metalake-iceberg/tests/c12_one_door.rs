//! This crate translates, and only the SDK speaks to Postgres: it reaches no engine, no face and no driver.

const OUT_OF_BOUNDS: [&str; 5] = [
    "datafusion",
    "metalake-client",
    "metalake-cli",
    "tokio-postgres.workspace",
    "deadpool-postgres",
];

#[test]
fn the_server_depends_on_the_core_and_the_sdk_and_on_no_engine() {
    let manifest = include_str!("../Cargo.toml");
    let (dependencies, for_tests) = manifest.split_once("[dev-dependencies]").expect("dev-dependencies");
    for name in OUT_OF_BOUNDS {
        assert!(!dependencies.contains(name), "{name} is out of bounds");
    }
    assert!(
        for_tests.contains("tokio-postgres"),
        "a test looks at Postgres, the server does not"
    );
}
