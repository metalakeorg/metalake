//! Namespaces (ICE-7), on the ref a client reads.

use axum::Json;
use axum::extract::{Query, State as Shared};
use serde::Deserialize;
use serde_json::{Value, json};

use super::errors::Failure;
use crate::state::State;

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
pub(crate) async fn list(Shared(state): Shared<State>, Query(listing): Query<Listing>) -> Result<Json<Value>, Failure> {
    let parent: Vec<&str> = listing.parent.iter().flat_map(|p| p.split(SEPARATOR)).collect();
    let namespaces = state.main().namespaces().await?;
    let names: Vec<Vec<String>> = namespaces.into_iter().map(|n| n.namespace).collect();
    let found = children(&names, &parent).ok_or_else(|| Failure::no_namespace(&parent.join(".")))?;
    Ok(Json(json!({"namespaces": found})))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(all: &[&[&str]]) -> Vec<Vec<String>> {
        all.iter().map(|n| n.iter().map(|s| s.to_string()).collect()).collect()
    }

    #[test]
    fn a_listing_is_one_level_under_its_parent() {
        let all = names(&[&["a"], &["a", "b"], &["a", "b", "c"], &["ab"]]);
        let at = |parent: &[&str]| children(&all, parent).map(|c| c.iter().map(|n| n.join(".")).collect::<Vec<_>>());
        assert_eq!(at(&[]).unwrap(), ["a", "ab"]);
        assert_eq!(at(&["a"]).unwrap(), ["a.b"]);
        assert_eq!(at(&["a", "b"]).unwrap(), ["a.b.c"]);
        assert_eq!(at(&["ab"]).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn a_parent_that_is_not_there_is_no_empty_listing() {
        assert!(children(&names(&[&["a"]]), &["b"]).is_none());
    }
}
