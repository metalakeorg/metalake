//! Writing to a table by its name, as a statement would name it.

use metalake::{Ref, Table};

use crate::{Error, Lake, Result};

const MAIN: &str = "main";
const DEFAULT_NAMESPACE: &str = "default";

/// Where a table is, as SQL says it: `t` is `main.default.t`, `n.t` is `main.n.t`, and `r.n.t` is on the ref `r`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    pub on: String,
    pub namespace: Vec<String>,
    pub name: String,
}

impl TablePath {
    pub fn parse(text: &str) -> Result<Self> {
        let parts: Vec<&str> = text.split('.').collect();
        if parts.iter().any(|part| part.is_empty()) {
            return Err(Error::usage(format!("{text}: a table is [ref.][namespace.]table")));
        }
        let (on, namespace, name) = match parts.as_slice() {
            [name] => (MAIN, vec![DEFAULT_NAMESPACE], *name),
            [namespace, name] => (MAIN, vec![*namespace], *name),
            [on, namespace @ .., name] => (*on, namespace.to_vec(), *name),
            [] => unreachable!("a split has a part"),
        };
        Ok(Self {
            on: on.to_string(),
            namespace: namespace.into_iter().map(str::to_string).collect(),
            name: name.to_string(),
        })
    }
}

impl std::fmt::Display for TablePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.on, self.namespace.join("."), self.name)
    }
}

impl Lake {
    /// A table as it is now, on its ref, which Postgres says. A write takes it anew, so that it sees the table's schema
    /// of that moment and not of when a program first named it.
    pub async fn table(&self, path: &TablePath) -> Result<(Ref, Table)> {
        let refs = self.catalog.refs().await?;
        let on = refs
            .into_iter()
            .find(|r| r.name() == path.on)
            .ok_or_else(|| Error::failed(format!("no ref {}", path.on)))?;
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        let table = on.table(&namespace, &path.name).await?;
        Ok((on, table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(on: &str, namespace: &[&str], name: &str) -> TablePath {
        TablePath {
            on: on.into(),
            namespace: namespace.iter().map(|n| n.to_string()).collect(),
            name: name.into(),
        }
    }

    #[test]
    fn a_name_reads_as_in_a_statement() {
        assert_eq!(TablePath::parse("t").unwrap(), path("main", &["default"], "t"));
        assert_eq!(TablePath::parse("n.t").unwrap(), path("main", &["n"], "t"));
        assert_eq!(TablePath::parse("r.n.t").unwrap(), path("r", &["n"], "t"));
        assert_eq!(TablePath::parse("r.a.b.t").unwrap(), path("r", &["a", "b"], "t"));
        assert_eq!(TablePath::parse("r.n.t").unwrap().to_string(), "r.n.t");
        assert!(TablePath::parse("").is_err() && TablePath::parse("a..b").is_err());
    }
}
