//! Credentials for a table's files (REST-11), what every store shares, end to end: the server in this process, holding
//! the lake's token, with a minter for local files that mints what it is asked, as no store's would. A load that asks is
//! given one credential for the table's directory, saying where to renew it; one that does not ask is given none; one
//! that bears no token is refused, and one that bears another; `loadCredentials` answers where the credential says.

mod common;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Arc;

use common::{Failure, create, fresh_schema, local_catalog};
use futures_util::future::BoxFuture;
use metalake_iceberg::State;
use metalake_iceberg::vend::{Access, Minted, Minter};
use serde_json::Value;

const TOKEN: &str = "the-lake-token";

/// Mints what it is asked, for an hour.
struct Fake;

impl Minter for Fake {
    fn mint<'a>(&'a self, prefix: &'a str, access: Access) -> BoxFuture<'a, Result<Minted, String>> {
        Box::pin(async move {
            let config = BTreeMap::from([
                ("fake.prefix".to_string(), prefix.to_string()),
                ("fake.access".to_string(), format!("{access:?}")),
            ]);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap();
            Ok(Minted {
                config,
                expires_at_ms: now.as_millis() as i64 + 3_600_000,
            })
        })
    }
}

/// A catalog with table `db.t`, served in this process with the lake's token and the fake minter.
async fn served() -> Result<String, Failure> {
    let schema = fresh_schema();
    let (catalog, _, _) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "t", "0", vec![]).await?;
    let state = State::connect(&common::url(), &schema)
        .await?
        .vending("file", Arc::new(Fake));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(metalake_iceberg::serve_with(
        listener,
        state,
        std::future::pending(),
        Some(TOKEN),
    ));
    Ok(address.to_string())
}

/// A GET of `path` with `headers`, and its status and body.
fn get(address: &str, path: &str, headers: &[(&str, &str)]) -> Result<(u16, Value), Failure> {
    let extra: String = headers.iter().map(|(k, v)| format!("{k}: {v}\r\n")).collect();
    let sent = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n{extra}Connection: close\r\n\r\n");
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(sent.as_bytes())?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    let status = answer[9..12].parse()?;
    let (_, payload) = answer.split_once("\r\n\r\n").unwrap_or((&answer, ""));
    Ok((status, serde_json::from_str(payload).unwrap_or(Value::Null)))
}

const TABLE: &str = "/v1/namespaces/db/tables/t";
const ASK: (&str, &str) = ("X-Iceberg-Access-Delegation", "vended-credentials");

fn bearer(token: &str) -> (&'static str, String) {
    ("Authorization", format!("Bearer {token}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn rest11_load_vends_to_writer() -> Result<(), Failure> {
    let address = served().await?;
    let token = bearer(TOKEN);
    let (status, loaded) = get(&address, TABLE, &[ASK, (token.0, &token.1)])?;
    assert_eq!(status, 200, "{loaded}");
    let vended = loaded["storage-credentials"].as_array().expect("credentials");
    assert_eq!(vended.len(), 1, "{loaded}");
    let location = loaded["metadata"]["location"].as_str().unwrap();
    assert_eq!(vended[0]["prefix"], format!("{}/", location.trim_end_matches('/')));
    let config = &vended[0]["config"];
    assert_eq!(config["fake.access"], "ReadWrite", "a writer reads and writes");
    let refresh = config["client.refresh-credentials-endpoint"].as_str().unwrap();
    assert_eq!(refresh, "v1/namespaces/db/tables/t/credentials");

    // Where the credential says to renew it, relative to the catalog's URI, a writer renews it.
    let (status, renewed) = get(&address, &format!("/{refresh}"), &[(token.0, &token.1)])?;
    assert_eq!(status, 200, "{renewed}");
    assert_eq!(renewed["storage-credentials"][0]["prefix"], vended[0]["prefix"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rest11_vends_only_when_asked_with_token() -> Result<(), Failure> {
    let address = served().await?;
    let token = bearer(TOKEN);

    let (status, loaded) = get(&address, TABLE, &[(token.0, &token.1)])?;
    assert_eq!(status, 200);
    assert!(
        loaded.get("storage-credentials").is_none(),
        "not asked, none given: {loaded}"
    );

    let (status, refused) = get(&address, TABLE, &[ASK])?;
    assert_eq!(status, 403, "{refused}");
    assert_eq!(refused["error"]["type"], "ForbiddenException");
    let (status, _) = get(&address, &format!("{TABLE}/credentials"), &[])?;
    assert_eq!(status, 403, "loadCredentials with no token");

    let other = bearer("another");
    let (status, refused) = get(&address, TABLE, &[ASK, (other.0, &other.1)])?;
    assert_eq!(status, 401, "{refused}");

    let (status, config) = get(&address, "/v1/config", &[])?;
    assert_eq!(status, 200);
    let endpoints = config["endpoints"].as_array().unwrap();
    assert!(
        endpoints
            .iter()
            .any(|e| e == "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials")
    );
    Ok(())
}
