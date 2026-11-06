//! This crate translates, and only the SDK speaks to Postgres: it reaches no engine, no face and no driver.

const OUT_OF_BOUNDS: [&str; 5] = [
    "datafusion",
    "metalake-client",
    "metalake-cli",
    "tokio-postgres",
    "deadpool-postgres",
];

#[test]
fn server_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    for name in OUT_OF_BOUNDS {
        assert!(!manifest.contains(name), "{name} is out of bounds");
    }
}
