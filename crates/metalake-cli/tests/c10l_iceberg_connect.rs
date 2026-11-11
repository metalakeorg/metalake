//! C10l: `metalake iceberg connect`, an engine's profile for the lake, in the engine's own form: printed alone on stdout
//! so that a redirection writes it, naming the lake's server, which it starts on the lake's own port and reuses; as a
//! record in `json`; under the catalog's name asked for. Flink's, which fetches its jars, is connected in CI's engines.

mod common;

use common::{lake, metalake, text};
use metalake_client::server::PORTS;

/// What `connect` prints, and what it says apart.
fn connect(dir: &std::path::Path, args: &[&str]) -> (String, String) {
    let output = metalake(dir).args(["iceberg", "connect"]).args(args).output().unwrap();
    assert!(output.status.success(), "{args:?}: {}", text(&output.stderr));
    (text(&output.stdout), text(&output.stderr))
}

/// The server's URI, as `connect` says it apart.
fn uri(said: &str) -> String {
    let start = said.find("http://").expect(said);
    said[start..].split(':').take(3).collect::<Vec<_>>().join(":")
}

#[test]
fn connect_profile_per_engine() {
    let (dir, _) = lake();
    let (_, said) = connect(&dir, &["pyiceberg"]);
    let uri = uri(&said);
    let port: u16 = uri.rsplit(':').next().unwrap().parse().unwrap();
    assert!(PORTS.contains(&port), "the lake's own port: {uri}");

    let cases = [
        ("pyiceberg", format!("catalog:\n  lake:\n    type: \"rest\"\n    uri: \"{uri}\"\n")),
        ("duckdb", format!("ENDPOINT '{uri}'")),
        ("spark", format!("spark.sql.catalog.lake.uri {uri}\n")),
        ("trino", format!("iceberg.rest-catalog.uri={uri}\n")),
    ];
    for (engine, expected) in cases {
        let (printed, said) = connect(&dir, &[engine]);
        assert!(printed.contains(&expected), "{engine}: {printed}");
        assert_eq!(self::uri(&said), uri, "{engine}: the same server");
    }
    common::ok(&dir, &["iceberg", "stop"]);
}

#[test]
fn connect_profile_json() {
    let (dir, _) = lake();
    let (printed, _) = connect(&dir, &["spark", "--name", "prod", "--format", "json"]);
    let profile: serde_json::Value = serde_json::from_str(printed.trim()).unwrap();
    assert_eq!(profile["engine"], "spark");
    assert_eq!(profile["name"], "prod");
    let uri = profile["uri"].as_str().unwrap();
    assert_eq!(profile["properties"]["spark.sql.catalog.prod.uri"], uri);
    assert_eq!(
        profile["artifacts"],
        serde_json::json!(["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"])
    );
    common::ok(&dir, &["iceberg", "stop"]);
}

#[test]
fn connect_refuses_bad_names() {
    let (dir, _) = lake();
    let unknown = metalake(&dir).args(["iceberg", "connect", "hive"]).output().unwrap();
    assert!(!unknown.status.success());
    assert!(text(&unknown.stderr).contains("trino"), "{}", text(&unknown.stderr));

    let bad = metalake(&dir)
        .args(["iceberg", "connect", "duckdb", "--name", "my-lake"])
        .output()
        .unwrap();
    assert!(!bad.status.success());
    assert!(text(&bad.stderr).contains("catalog's name"), "{}", text(&bad.stderr));
}
