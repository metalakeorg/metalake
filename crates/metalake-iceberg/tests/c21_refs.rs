//! A lake's refs through the Iceberg service (I6), end to end: the server, as a process. `GET /v1/config` hands a
//! client that asks for a warehouse the prefix of the ref of that name, whatever its characters; a request under a
//! prefix is on that ref, and one under a prefix no ref has is refused before anything reads.

mod common;

use std::io::{Read, Write};

use common::{Failure, fresh_schema, local_catalog, server};
use serde_json::{Value, json};

/// A `GET` to the server at `uri`, and its status and body.
fn get(uri: &str, path: &str) -> Result<(u16, Value), Failure> {
    let address = uri.trim_start_matches("http://");
    let sent = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(sent.as_bytes())?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    let status = answer[9..12].parse()?;
    let (_, payload) = answer.split_once("\r\n\r\n").unwrap_or((&answer, ""));
    Ok((status, serde_json::from_str(payload).unwrap_or(Value::Null)))
}

/// A catalog with namespace `db` on `main`, branches `dev` and `feature/x` and tag `v1` from it, and the server on it.
async fn served() -> Result<(common::Running, String), Failure> {
    let schema = fresh_schema();
    let (catalog, _, _) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    let main = catalog.branch("main").await?;
    main.create_branch("dev").await?;
    main.create_branch("feature/x").await?;
    main.create_tag("v1").await?;
    server(&schema)
}

#[tokio::test]
async fn i6_config_hands_the_prefix_of_the_ref_a_warehouse_names() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    for (warehouse, overrides) in [
        ("", json!({})),
        ("?warehouse=", json!({})),
        ("?warehouse=dev", json!({"prefix": "refs/dev"})),
        ("?warehouse=main", json!({"prefix": "refs/main"})),
        ("?warehouse=v1", json!({"prefix": "refs/v1"})),
        ("?warehouse=feature%2Fx", json!({"prefix": "refs/feature%2Fx"})),
    ] {
        let (status, body) = get(&uri, &format!("/v1/config{warehouse}"))?;
        assert_eq!(status, 200, "{warehouse}: {body}");
        assert_eq!(body["overrides"], overrides, "{warehouse}");
        assert_eq!(body["defaults"], json!({}), "{warehouse}");
    }
    let (status, body) = get(&uri, "/v1/config?warehouse=nope")?;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("no ref nope"),
        "{body}"
    );
    Ok(())
}

#[tokio::test]
async fn i6_a_request_under_a_prefix_is_routed_and_one_under_no_ref_refused() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    let bare = get(&uri, "/v1/namespaces")?;
    assert_eq!(bare.0, 200, "{}", bare.1);
    for prefix in ["refs/main", "refs/dev", "refs/v1", "refs/feature%2Fx"] {
        let (status, body) = get(&uri, &format!("/v1/{prefix}/namespaces"))?;
        assert_eq!(status, 200, "{prefix}: {body}");
        assert_eq!(body["namespaces"], json!([["db"]]), "{prefix}");
    }
    let (status, body) = get(&uri, "/v1/refs/nope/namespaces")?;
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"]["type"], "NotFoundException", "{body}");
    let (status, _) = get(&uri, "/v1/refs/dev")?;
    assert_eq!(status, 404, "a prefix alone is no route");
    Ok(())
}
