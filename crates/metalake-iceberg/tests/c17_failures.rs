//! Failures as an Iceberg client reads them (REST-2), end to end: the server, as a process. A name that is missing or
//! taken is reported as Iceberg's catalogs word it; a table that is not the one a commit asserts conflicts before
//! anything the commit asks is refused (REST-3); a commit's requirements on the definition are held to the table, as a
//! load serves it (REST-12); and a creation whose name another took first reports Iceberg's failed `assert-create`
//! (REST-7).

mod common;

use std::io::{Read, Write};

use common::{Failure, create, fresh_schema, local_catalog, server};
use serde_json::{Value, json};

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

fn message(body: &Value) -> &str {
    body["error"]["message"].as_str().unwrap_or_default()
}

/// A catalog with namespace `db` and table `db.t`, and the server on it.
async fn served() -> Result<(common::Running, String), Failure> {
    let schema = fresh_schema();
    let (catalog, _, _) = local_catalog(&schema).await?;
    let main = catalog.branch("main").await?;
    main.transaction().create_namespace(&["db"]).commit().await?;
    create(&main, "t", "0", vec![]).await?;
    server(&schema)
}

#[tokio::test]
async fn rest2_missing_or_taken_name_reports_iceberg_wording() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    let schema = json!({"type": "struct", "fields": [{"id": 1, "name": "k", "type": "long", "required": false}]});
    for (method, path, body, code, said) in [
        (
            "GET",
            "/v1/namespaces/db/tables/nowhere",
            None,
            404,
            "Table does not exist: db.nowhere",
        ),
        (
            "GET",
            "/v1/namespaces/nowhere",
            None,
            404,
            "Namespace does not exist: nowhere",
        ),
        (
            "POST",
            "/v1/namespaces",
            Some(json!({"namespace": ["db"]})),
            409,
            "Namespace already exists: db",
        ),
        (
            "POST",
            "/v1/namespaces/db/tables",
            Some(json!({"name": "t", "schema": schema})),
            409,
            "Table already exists: db.t",
        ),
        (
            "POST",
            "/v1/tables/rename",
            Some(json!({"source": {"namespace": ["db"], "name": "nowhere"},
                        "destination": {"namespace": ["db"], "name": "u"}})),
            404,
            "Table does not exist: db.nowhere",
        ),
    ] {
        let (status, answer) = request(&uri, method, path, body)?;
        assert_eq!((status, message(&answer)), (code, said), "{method} {path}: {answer}");
    }
    Ok(())
}

#[tokio::test]
async fn rest3_other_table_uuid_before_refusal_conflicts() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    let body = json!({"requirements": [
            {"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000001"},
            {"type": "assert-last-assigned-field-id", "last-assigned-field-id": 1}],
        "updates": []});
    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/t", Some(body))?;
    assert_eq!(status, 409, "{answer}");
    assert_eq!(answer["error"]["type"], "CommitFailedException");
    assert!(
        message(&answer).starts_with("Requirement failed: UUID does not match"),
        "{answer}"
    );

    Ok(())
}

#[tokio::test]
async fn rest12_definition_requirements_checked() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    let (status, loaded) = request(&uri, "GET", "/v1/namespaces/db/tables/t", None)?;
    assert_eq!(status, 200, "{loaded}");
    let served = &loaded["metadata"];
    let asserted = |schema: &Value| {
        json!([
            {"type": "assert-current-schema-id", "current-schema-id": schema},
            {"type": "assert-last-assigned-field-id", "last-assigned-field-id": served["last-column-id"]},
            {"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": served["last-partition-id"]},
            {"type": "assert-default-spec-id", "default-spec-id": served["default-spec-id"]},
            {"type": "assert-default-sort-order-id", "default-sort-order-id": served["default-sort-order-id"]}])
    };
    let set = |team: &str| json!([{"action": "set-properties", "updates": {"team": team}}]);

    // As the load served it: the commit applies.
    let body = json!({"requirements": asserted(&served["current-schema-id"]), "updates": set("data")});
    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/t", Some(body))?;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["metadata"]["properties"]["team"], "data");

    // Another schema than the table's: a conflict, and nothing applied.
    let other = json!(served["current-schema-id"].as_i64().unwrap_or(0) + 1000);
    let body = json!({"requirements": asserted(&other), "updates": set("other")});
    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/t", Some(body))?;
    assert_eq!(status, 409, "{answer}");
    assert_eq!(answer["error"]["type"], "CommitFailedException");
    assert!(
        message(&answer).starts_with("Requirement failed: current schema id changed"),
        "{answer}"
    );
    let (_, loaded) = request(&uri, "GET", "/v1/namespaces/db/tables/t", None)?;
    assert_eq!(loaded["metadata"]["properties"]["team"], "data", "nothing applied");

    // The updates they guard stay refused.
    let body = json!({"requirements": asserted(&served["current-schema-id"]),
        "updates": [{"action": "set-current-schema", "schema-id": 1}]});
    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/t", Some(body))?;
    assert_eq!(status, 400, "{answer}");
    Ok(())
}

/// The commit of a stage-create, from the metadata the server staged.
fn creation(staged: &Value) -> Value {
    let metadata = &staged["metadata"];
    json!({"requirements": [{"type": "assert-create"}], "updates": [
        {"action": "assign-uuid", "uuid": metadata["table-uuid"]},
        {"action": "upgrade-format-version", "format-version": 2},
        {"action": "add-schema", "schema": metadata["schemas"][0]},
        {"action": "set-current-schema", "schema-id": -1},
        {"action": "add-spec", "spec": metadata["partition-specs"][0]},
        {"action": "set-default-spec", "spec-id": -1},
        {"action": "add-sort-order", "sort-order": metadata["sort-orders"][0]},
        {"action": "set-default-sort-order", "sort-order-id": -1},
        {"action": "set-location", "location": metadata["location"]}]})
}

#[tokio::test]
async fn rest7_create_taken_name_reports_requirement_failed() -> Result<(), Failure> {
    let (_server, uri) = served().await?;
    let schema = json!({"type": "struct", "fields": [{"id": 1, "name": "k", "type": "long", "required": false}]});
    let stage = json!({"name": "n", "schema": schema, "stage-create": true});
    let (_, first) = request(&uri, "POST", "/v1/namespaces/db/tables", Some(stage.clone()))?;
    let (_, second) = request(&uri, "POST", "/v1/namespaces/db/tables", Some(stage))?;

    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/n", Some(creation(&first)))?;
    assert_eq!(status, 200, "{answer}");
    let (status, answer) = request(&uri, "POST", "/v1/namespaces/db/tables/n", Some(creation(&second)))?;
    assert_eq!(status, 409, "{answer}");
    assert_eq!(message(&answer), "Requirement failed: table already exists: db.n");
    Ok(())
}
