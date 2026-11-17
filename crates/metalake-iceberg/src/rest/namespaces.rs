//! Namespaces (ICE-7), on the ref a client reads.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use metalake::{NamespaceRow, Ref};
use serde::Deserialize;
use serde_json::{Value, json};

use super::errors::Failure;
use super::prefix::On;
use super::tables::{decoded, inside, levels, shown, strs};
use crate::apply::ddl;
use crate::wire::commit::{CreateNamespaceRequest, UpdateNamespaceRequest};

/// How the OpenAPI joins the levels of a namespace in a query.
pub(super) const SEPARATOR: char = '\x1f';

#[derive(Deserialize)]
pub(crate) struct Listing {
    parent: Option<String>,
}

/// The namespaces directly under `parent`, or none if `parent` is not one. The top level has no parent.
fn children<'a>(all: &'a [Vec<String>], parent: &[&str]) -> Option<Vec<&'a [String]>> {
    if !parent.is_empty() && !all.iter().any(|n| n.iter().eq(parent)) {
        return None;
    }
    let directly_under = |n: &&Vec<String>| n.len() == parent.len() + 1 && n.iter().zip(parent).all(|(a, b)| a == b);
    Some(all.iter().filter(directly_under).map(Vec::as_slice).collect())
}

/// `GET /v1/namespaces`.
pub(crate) async fn list(On(on, catalog): On, Query(listing): Query<Listing>) -> Result<Json<Value>, Failure> {
    // Under the catalog the client sees: its top level is that catalog's schemas (I7).
    let asked: Vec<&str> = listing.parent.iter().flat_map(|p| p.split(SEPARATOR)).collect();
    let parent = catalog.inside(&asked);
    let namespaces = on.namespaces().await?;
    let names: Vec<Vec<String>> = namespaces.into_iter().map(|n| n.namespace).collect();
    let found = children(&names, &strs(&parent)).ok_or_else(|| Failure::no_namespace(&asked.join(".")))?;
    let found: Vec<&[String]> = found.into_iter().filter_map(|n| catalog.outside(n)).collect();
    Ok(Json(json!({"namespaces": found})))
}

/// A namespace of the ref, or a 404.
async fn found(on: &Ref, wanted: &[&str]) -> Result<NamespaceRow, Failure> {
    let namespaces = on.namespaces().await?;
    let row = namespaces.into_iter().find(|n| n.namespace.iter().eq(wanted));
    row.ok_or_else(|| Failure::no_namespace(&shown(wanted).join(".")))
}

/// `POST /v1/namespaces`.
pub(crate) async fn create(On(on, catalog): On, body: Bytes) -> Result<Json<Value>, Failure> {
    let request: CreateNamespaceRequest = decoded(&body)?;
    ddl::create_namespace(&on, &catalog.inside(&request.namespace), &request.properties)
        .await
        .map_err(|e| match Failure::from(e) {
            failure if failure.is_conflict() => Failure::namespace_exists(&request.namespace.join(".")),
            failure => failure,
        })?;
    Ok(Json(
        json!({"namespace": request.namespace, "properties": request.properties}),
    ))
}

/// `GET /v1/namespaces/{namespace}`.
pub(crate) async fn load(On(on, catalog): On, Path(namespace): Path<String>) -> Result<Json<Value>, Failure> {
    let row = found(&on, &strs(&inside(&catalog, &namespace))).await?;
    Ok(Json(
        json!({"namespace": levels(&namespace), "properties": row.properties}),
    ))
}

/// `HEAD /v1/namespaces/{namespace}`.
pub(crate) async fn exists(On(on, catalog): On, Path(namespace): Path<String>) -> Result<StatusCode, Failure> {
    found(&on, &strs(&inside(&catalog, &namespace))).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /v1/namespaces/{namespace}/properties`.
pub(crate) async fn update(
    On(on, catalog): On,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, Failure> {
    let full = inside(&catalog, &namespace);
    let wanted = strs(&full);
    let request: UpdateNamespaceRequest = decoded(&body)?;
    let row = found(&on, &wanted).await?;
    let changes = ddl::property_changes(&request.updates, &request.removals)?;
    ddl::update_namespace(&on, &wanted, changes).await?;
    Ok(Json(ddl::update_summary(
        &row.properties,
        &request.updates,
        &request.removals,
    )))
}

/// `DELETE /v1/namespaces/{namespace}`. Refused while it holds a table or a namespace (NS-3).
pub(crate) async fn drop(On(on, catalog): On, Path(namespace): Path<String>) -> Result<StatusCode, Failure> {
    let full = inside(&catalog, &namespace);
    let wanted = strs(&full);
    found(&on, &wanted).await?;
    ddl::drop_namespace(&on, &wanted)
        .await
        .map_err(|e| match Failure::from(e) {
            failure if failure.is_conflict() => Failure::namespace_not_empty(&shown(&wanted).join(".")),
            failure => failure,
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(all: &[&[&str]]) -> Vec<Vec<String>> {
        all.iter().map(|n| n.iter().map(|s| s.to_string()).collect()).collect()
    }

    #[test]
    fn ice7_list_direct_children() {
        let all = names(&[&["a"], &["a", "b"], &["a", "b", "c"], &["ab"]]);
        let at = |parent: &[&str]| children(&all, parent).map(|c| c.iter().map(|n| n.join(".")).collect::<Vec<_>>());
        assert_eq!(at(&[]).unwrap(), ["a", "ab"]);
        assert_eq!(at(&["a"]).unwrap(), ["a.b"]);
        assert_eq!(at(&["a", "b"]).unwrap(), ["a.b.c"]);
        assert_eq!(at(&["ab"]).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn ice7_list_missing_parent() {
        assert!(children(&names(&[&["a"]]), &["b"]).is_none());
    }
}
