//! A lake's catalogs in SQL (I7): the first level of its namespaces, the level above a schema, on the ref the session is
//! on. Read by the client before DataFusion, as a ref's verbs are ([`crate::refs_sql`]).
//!
//! ```sql
//! CREATE CATALOG [IF NOT EXISTS] ventas;
//! DROP CATALOG [IF EXISTS] ventas;     -- an empty one: its schemas are dropped first
//! SHOW CATALOGS;
//! ```
//!
//! A catalog is also made with its first schema, `CREATE SCHEMA ventas.eu`; these make one empty, drop one and list
//! them. Each answers rows, as any statement does.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use metalake::Ref;
use serde_json::json;

use crate::words::Words;
use crate::{Error, Prepared, Result};

/// A catalog's verb, as a statement says it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogVerb {
    Create { name: String, if_not_exists: bool },
    Drop { name: String, if_exists: bool },
    Show,
}

/// The catalog verb a text says, if it says one. `CREATE CATALOG`, `DROP CATALOG` and `SHOW CATALOGS` start no other
/// statement, so a text that starts so is this or is refused.
pub fn catalog_verb_of(text: &str) -> Result<Option<CatalogVerb>> {
    let Some(mut words) = Words::of(text) else {
        return Ok(None);
    };
    let (first, second) = (words.keyword(), words.keyword());
    let verb = match (first.as_deref(), second.as_deref()) {
        (Some("CREATE"), Some("CATALOG")) => {
            let if_not_exists = words.take("IF");
            if if_not_exists && !(words.take("NOT") && words.take("EXISTS")) {
                return Err(Error::usage("CREATE CATALOG [IF NOT EXISTS] name"));
            }
            CatalogVerb::Create {
                name: words.name("a catalog")?,
                if_not_exists,
            }
        }
        (Some("DROP"), Some("CATALOG")) => {
            let if_exists = words.take("IF");
            if if_exists && !words.take("EXISTS") {
                return Err(Error::usage("DROP CATALOG [IF EXISTS] name"));
            }
            CatalogVerb::Drop {
                name: words.name("a catalog")?,
                if_exists,
            }
        }
        (Some("SHOW"), Some("CATALOGS")) => CatalogVerb::Show,
        _ => return Ok(None),
    };
    words.end()?;
    Ok(Some(verb))
}

/// The catalogs of `on`: the first levels of its namespaces, in order.
async fn catalogs(on: &Ref) -> Result<Vec<String>> {
    let mut names: Vec<String> = on
        .namespaces()
        .await?
        .into_iter()
        .filter(|row| row.namespace.len() == 1)
        .map(|row| row.namespace[0].clone())
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

/// Runs a verb on `on`, the session's ref, and answers its rows.
pub async fn run(on: &Ref, verb: CatalogVerb) -> Result<Prepared> {
    match verb {
        CatalogVerb::Show => rows("name", catalogs(on).await?),
        CatalogVerb::Create { name, if_not_exists } => {
            check_name(&name)?;
            if catalogs(on).await?.contains(&name) {
                if if_not_exists {
                    return rows("name", vec![name]);
                }
                return Err(Error::usage(format!("a catalog {name} exists")));
            }
            on.writable()?;
            on.transaction().create_namespace(&[&name]).commit().await?;
            rows("name", vec![name])
        }
        CatalogVerb::Drop { name, if_exists } => {
            if !catalogs(on).await?.contains(&name) {
                if if_exists {
                    return rows("deleted", Vec::new());
                }
                return Err(Error::usage(format!("no catalog {name}")));
            }
            let inside = on
                .namespaces()
                .await?
                .into_iter()
                .any(|row| row.namespace.len() > 1 && row.namespace[0] == name);
            if inside {
                return Err(Error::usage(format!(
                    "{name} holds schemas: drop them first, DROP CATALOG drops an empty one"
                )));
            }
            on.writable()?;
            let change = json!({"namespace": [name], "drop": true});
            on.transaction().change_namespace(change).commit().await?;
            rows("deleted", vec![name])
        }
    }
}

/// A catalog's name holds no `@`, which an Iceberg warehouse puts between it and its ref (`ventas@dev`).
pub(crate) fn check_name(name: &str) -> Result<()> {
    if name.contains('@') {
        return Err(Error::usage(format!(
            "{name}: a catalog's name holds no @, which an Iceberg warehouse puts between a catalog and its ref"
        )));
    }
    Ok(())
}

/// One column of names, as rows.
fn rows(column: &str, names: Vec<String>) -> Result<Prepared> {
    let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Utf8, false)]));
    let values: ArrayRef = Arc::new(names.into_iter().map(Some).collect::<StringArray>());
    let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
    Prepared::from_batches(schema, vec![batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verb(text: &str) -> CatalogVerb {
        catalog_verb_of(text).unwrap().unwrap()
    }

    #[test]
    fn i7_catalog_verbs_read() {
        assert_eq!(
            verb("create catalog ventas"),
            CatalogVerb::Create {
                name: "ventas".into(),
                if_not_exists: false
            }
        );
        assert_eq!(
            verb("CREATE CATALOG IF NOT EXISTS \"Ventas\";"),
            CatalogVerb::Create {
                name: "Ventas".into(),
                if_not_exists: true
            }
        );
        assert_eq!(
            verb("drop catalog if exists ventas"),
            CatalogVerb::Drop {
                name: "ventas".into(),
                if_exists: true
            }
        );
        assert_eq!(verb("show catalogs"), CatalogVerb::Show);
    }

    #[test]
    fn i7_other_statements_are_not_catalog_verbs() {
        for text in ["create schema demo", "show tables", "drop table t", "select 1"] {
            assert!(catalog_verb_of(text).unwrap().is_none(), "{text}");
        }
        assert!(catalog_verb_of("show catalogs now").is_err());
    }
}
