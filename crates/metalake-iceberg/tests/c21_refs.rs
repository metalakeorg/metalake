//! A lake's refs through the Iceberg service (I6), end to end: the server, as a process. `GET /v1/config` hands a
//! client that asks for a warehouse the prefix of the ref of that name, whatever its characters; a request under a
//! prefix is on that ref, and one under a prefix no ref has is refused before anything reads. Under a prefix, every
//! name is the ref's: its namespaces, its tables, its current snapshot, which is Iceberg's `main`; what is created or
//! dropped there is the ref's alone; and a tag takes no commit.

mod common;

use std::io::{Read, Write};

use common::{Failure, append, create, fresh_schema, local_catalog, server};
use serde_json::{Value, json};

/// A `GET` to the server at `uri`, and its status and body.
fn get(uri: &str, path: &str) -> Result<(u16, Value), Failure> {
    request(uri, "GET", path, None)
}

/// A request to the server at `uri`, and its status and body.
fn request(uri: &str, method: &str, path: &str, body: Option<Value>) -> Result<(u16, Value), Failure> {
    let address = uri.trim_start_matches("http://");
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let sent = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
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
        // A location, as Iceberg's Java catalogs set `warehouse`: `main`, as before refs were served.
        ("?warehouse=%2Ftmp%2Fwarehouse", json!({})),
        ("?warehouse=s3%3A%2F%2Fbucket%2Fwh", json!({})),
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

/// `db.t` on `main` with one append; `dev` from it, where `t` has one more and `db.only_dev` is created; tag `v1` of
/// `main`. The server on it.
async fn written() -> Result<(common::Running, String), Failure> {
    let schema = fresh_schema();
    let (catalog, storage, _) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&catalog.branch("main").await?, "t", "0", vec![]).await?;
    append(&catalog, &catalog.branch("main").await?, &storage, "t", 0..2).await?;
    let main = catalog.branch("main").await?;
    main.create_branch("dev").await?;
    main.create_tag("v1").await?;
    append(&catalog, &catalog.branch("dev").await?, &storage, "t", 2..3).await?;
    create(&catalog.branch("dev").await?, "only_dev", "0", vec![]).await?;
    server(&schema)
}

fn names(body: &Value) -> Vec<String> {
    let mut names: Vec<String> = body["identifiers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|i| i["name"].as_str().map(String::from))
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn i6_reads_under_a_prefix_are_the_refs() -> Result<(), Failure> {
    let (_server, uri) = written().await?;
    let (_, bare) = get(&uri, "/v1/namespaces/db/tables")?;
    let (_, dev) = get(&uri, "/v1/refs/dev/namespaces/db/tables")?;
    assert_eq!(names(&bare), ["t"]);
    assert_eq!(names(&dev), ["only_dev", "t"]);
    assert_eq!(get(&uri, "/v1/namespaces/db/tables/only_dev")?.0, 404);
    assert_eq!(get(&uri, "/v1/refs/dev/namespaces/db/tables/only_dev")?.0, 200);

    let (status, main_t) = get(&uri, "/v1/namespaces/db/tables/t")?;
    assert_eq!(status, 200, "{main_t}");
    let (status, dev_t) = get(&uri, "/v1/refs/dev/namespaces/db/tables/t")?;
    assert_eq!(status, 200, "{dev_t}");
    let current = |loaded: &Value| loaded["metadata"]["current-snapshot-id"].as_i64();
    let refs = |loaded: &Value| loaded["metadata"]["refs"].clone();
    assert_ne!(current(&main_t), current(&dev_t), "each its ref's current snapshot");
    assert_eq!(
        refs(&dev_t)["main"]["snapshot-id"].as_i64(),
        current(&dev_t),
        "Iceberg's main is dev"
    );
    assert!(refs(&dev_t).get("dev").is_none(), "dev is not served twice");
    assert_eq!(
        refs(&main_t)["dev"]["snapshot-id"].as_i64(),
        current(&dev_t),
        "on main, dev is a ref of the table"
    );
    assert_eq!(refs(&main_t)["main"]["snapshot-id"].as_i64(), current(&main_t));

    // Loads of either ref, one after another, each finds its metadata file: what one prunes, the other does not need.
    for path in ["/v1/namespaces/db/tables/t", "/v1/refs/dev/namespaces/db/tables/t"].repeat(3) {
        let (status, loaded) = get(&uri, path)?;
        assert_eq!(status, 200, "{path}");
        let location = loaded["metadata-location"].as_str().unwrap_or_default();
        let file = url::Url::parse(location)?.to_file_path().map_err(|_| "not a file")?;
        assert!(file.exists(), "{path}: {location}");
    }
    Ok(())
}

#[tokio::test]
async fn i6_writes_under_a_prefix_are_the_refs_alone_and_a_tag_takes_none() -> Result<(), Failure> {
    let (_server, uri) = written().await?;
    let schema = json!({"type": "struct", "fields": [{"id": 1, "name": "k", "type": "long", "required": false}]});
    let created = request(
        &uri,
        "POST",
        "/v1/refs/dev/namespaces/db/tables",
        Some(json!({"name": "made_on_dev", "schema": schema})),
    )?;
    assert_eq!(created.0, 200, "{}", created.1);
    assert_eq!(
        names(&get(&uri, "/v1/refs/dev/namespaces/db/tables")?.1),
        ["made_on_dev", "only_dev", "t"]
    );
    assert_eq!(
        names(&get(&uri, "/v1/namespaces/db/tables")?.1),
        ["t"],
        "main is untouched"
    );

    let dropped = request(&uri, "DELETE", "/v1/refs/dev/namespaces/db/tables/t", None)?;
    assert_eq!(dropped.0, 204, "{}", dropped.1);
    assert_eq!(get(&uri, "/v1/refs/dev/namespaces/db/tables/t")?.0, 404);
    assert_eq!(get(&uri, "/v1/namespaces/db/tables/t")?.0, 200, "main keeps t");

    let namespace = request(
        &uri,
        "POST",
        "/v1/refs/dev/namespaces",
        Some(json!({"namespace": ["dev_only"]})),
    )?;
    assert_eq!(namespace.0, 200, "{}", namespace.1);
    assert_eq!(get(&uri, "/v1/refs/dev/namespaces/dev_only")?.0, 200);
    assert_eq!(get(&uri, "/v1/namespaces/dev_only")?.0, 404);

    let on_tag = request(
        &uri,
        "POST",
        "/v1/refs/v1/namespaces/db/tables",
        Some(json!({"name": "no", "schema": schema})),
    )?;
    assert_eq!(on_tag.0, 403, "{}", on_tag.1);
    assert_eq!(on_tag.1["error"]["type"], "ForbiddenException");
    assert_eq!(get(&uri, "/v1/refs/v1/namespaces/db/tables/t")?.0, 200, "a tag is read");
    Ok(())
}

const TOKEN: &str = "the-lakes-token";

/// `written`'s catalog, served in this process with the lake's token, which a session's ref is set with.
async fn with_token() -> Result<String, Failure> {
    let schema = fresh_schema();
    let (catalog, storage, _) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&catalog.branch("main").await?, "t", "0", vec![]).await?;
    append(&catalog, &catalog.branch("main").await?, &storage, "t", 0..2).await?;
    catalog.branch("main").await?.create_branch("dev").await?;
    append(&catalog, &catalog.branch("dev").await?, &storage, "t", 2..3).await?;
    create(&catalog.branch("dev").await?, "only_dev", "0", vec![]).await?;
    let state = metalake_iceberg::State::connect(&common::url(), &schema).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(metalake_iceberg::serve_with(
        listener,
        state,
        std::future::pending(),
        Some(TOKEN),
    ));
    Ok(format!("http://{address}"))
}

/// Session `id` says it is on `on`, bearing `token`.
fn say(uri: &str, id: &str, on: &str, token: Option<&str>) -> Result<(u16, Value), Failure> {
    let address = uri.trim_start_matches("http://");
    let body = json!({"ref": on}).to_string();
    let bearer = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let sent = format!(
        "PUT /metalake/sessions/{id} HTTP/1.1\r\nHost: {address}\r\n{bearer}Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(sent.as_bytes())?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    let status = answer[9..12].parse()?;
    let (_, payload) = answer.split_once("\r\n\r\n").unwrap_or((&answer, ""));
    Ok((status, serde_json::from_str(payload).unwrap_or(Value::Null)))
}

/// A client connected to a session, rather than to a ref, follows the ref the session says it is on, request by
/// request, by the same prefix: as `HEAD` follows the branch checked out.
// Multi-threaded: the server runs in this process, and the test's requests block a thread of their own.
#[tokio::test(flavor = "multi_thread")]
async fn i6_a_session_prefix_follows_the_ref_the_session_is_on() -> Result<(), Failure> {
    let uri = with_token().await?;
    assert_eq!(
        say(&uri, "k1", "main", None)?.0,
        403,
        "only the lake's token sets a session's ref"
    );
    assert_eq!(say(&uri, "k1", "nope", Some(TOKEN))?.0, 404, "a ref the lake has not");
    assert_eq!(
        get(&uri, "/v1/config?warehouse=session:k1")?.0,
        400,
        "a session that has said no ref"
    );
    assert_eq!(say(&uri, "k1", "main", Some(TOKEN))?.0, 200);

    let (status, config) = get(&uri, "/v1/config?warehouse=session:k1")?;
    assert_eq!(status, 200, "{config}");
    assert_eq!(config["overrides"]["prefix"], "sessions/k1");

    let tables = "/v1/sessions/k1/namespaces/db/tables";
    let table = "/v1/sessions/k1/namespaces/db/tables/t";
    let current = |loaded: &Value| loaded["metadata"]["current-snapshot-id"].as_i64();
    assert_eq!(names(&get(&uri, tables)?.1), ["t"]);
    let on_main = get(&uri, table)?.1;

    assert_eq!(say(&uri, "k1", "dev", Some(TOKEN))?.0, 200);
    assert_eq!(
        names(&get(&uri, tables)?.1),
        ["only_dev", "t"],
        "the same prefix, now dev"
    );
    let on_dev = get(&uri, table)?.1;
    assert_ne!(current(&on_main), current(&on_dev));
    assert_eq!(
        current(&on_dev),
        current(&get(&uri, "/v1/refs/dev/namespaces/db/tables/t")?.1)
    );

    assert_eq!(say(&uri, "k1", "main", Some(TOKEN))?.0, 200);
    assert_eq!(names(&get(&uri, tables)?.1), ["t"], "and back");
    assert_eq!(
        get(&uri, "/v1/sessions/other/namespaces")?.0,
        404,
        "a session that has said no ref"
    );
    Ok(())
}
