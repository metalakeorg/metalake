//! Where a table is, by its name: on a ref, in a namespace, its own name. Every face that takes a table's name reads it
//! by this rule, so that a statement, a command, a library call and a pipeline mean the same table by the same words.
//! A name is turned into a table by the catalog (`Ref::table`, TBL-1), which answers its id; this only reads the words.

use std::fmt;

/// The ref a name without one is on.
pub const MAIN: &str = "main";
/// The namespace a name without one is in.
pub const DEFAULT_NAMESPACE: &str = "default";
/// What separates the levels of a nested namespace, in a name and in an engine's schema name.
pub const NAMESPACE_SEPARATOR: char = '.';

/// A table's name: `t` is `main.default.t`, `n.t` is `main.n.t`, and `r.n.t` is on the ref `r`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TablePath {
    pub on: String,
    pub namespace: Vec<String>,
    pub name: String,
}

/// A name that names no table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPath(String);

impl fmt::Display for InvalidPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: a table is [ref.][namespace.]table", self.0)
    }
}

impl std::error::Error for InvalidPath {}

impl TablePath {
    /// A name as text, its parts split at dots: past the ref and before the table's name, every part is a level of the
    /// namespace (`r.a.b.t`).
    pub fn parse(text: &str) -> Result<Self, InvalidPath> {
        let parts: Vec<&str> = text.split(NAMESPACE_SEPARATOR).collect();
        if parts.iter().any(|part| part.is_empty()) {
            return Err(InvalidPath(text.to_string()));
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

    /// A name as a SQL engine gives it, already split into catalog, schema and table: the catalog is the ref, and the
    /// schema the namespace, with its levels joined by dots.
    pub fn from_parts(catalog: Option<&str>, schema: Option<&str>, table: &str) -> Self {
        Self {
            on: catalog.unwrap_or(MAIN).to_string(),
            namespace: levels(schema.unwrap_or(DEFAULT_NAMESPACE)),
            name: table.to_string(),
        }
    }

    /// Its namespace as an engine's schema name.
    pub fn schema_name(&self) -> String {
        self.namespace.join(&NAMESPACE_SEPARATOR.to_string())
    }

    /// As a statement names it, whatever its names hold: a namespace of several levels is one identifier.
    pub fn quoted(&self) -> String {
        format!(
            "{}.{}.{}",
            quoted(&self.on),
            quoted(&self.schema_name()),
            quoted(&self.name)
        )
    }
}

/// The levels of a namespace named as an engine's schema.
pub fn levels(schema: &str) -> Vec<String> {
    schema.split(NAMESPACE_SEPARATOR).map(str::to_string).collect()
}

/// A name as a statement says it, whatever it holds.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.on, self.schema_name(), self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(on: &str, namespace: &[&str], table: &str) -> TablePath {
        TablePath {
            on: on.into(),
            namespace: namespace.iter().map(|n| n.to_string()).collect(),
            name: table.into(),
        }
    }

    #[test]
    fn parse_defaults_and_levels() {
        assert_eq!(TablePath::parse("t").unwrap(), name("main", &["default"], "t"));
        assert_eq!(TablePath::parse("n.t").unwrap(), name("main", &["n"], "t"));
        assert_eq!(TablePath::parse("r.n.t").unwrap(), name("r", &["n"], "t"));
        assert_eq!(TablePath::parse("r.a.b.t").unwrap(), name("r", &["a", "b"], "t"));
        assert_eq!(TablePath::parse("r.a.b.t").unwrap().to_string(), "r.a.b.t");
    }

    #[test]
    fn parse_empty_part_refused() {
        for text in ["", ".", "a..b", ".t", "n."] {
            let refused = TablePath::parse(text).unwrap_err();
            assert!(refused.to_string().starts_with(&format!("{text}:")), "{refused}");
        }
    }

    #[test]
    fn from_parts_as_parse() {
        assert_eq!(TablePath::from_parts(None, None, "t"), TablePath::parse("t").unwrap());
        assert_eq!(
            TablePath::from_parts(None, Some("n"), "t"),
            TablePath::parse("n.t").unwrap()
        );
        assert_eq!(
            TablePath::from_parts(Some("r"), Some("n"), "t"),
            TablePath::parse("r.n.t").unwrap()
        );
        assert_eq!(
            TablePath::from_parts(Some("r"), Some("a.b"), "t"),
            TablePath::parse("r.a.b.t").unwrap(),
            "a schema of several levels is one name with dots"
        );
        // An engine's catalog may carry a point after the ref (`main@1234`): it stays with the ref, for the engine.
        assert_eq!(TablePath::from_parts(Some("main@12"), None, "t").on, "main@12");
    }

    #[test]
    fn quoted_keeps_levels_together() {
        assert_eq!(TablePath::parse("r.a.b.t").unwrap().quoted(), r#""r"."a.b"."t""#);
        assert_eq!(
            TablePath::from_parts(None, None, r#"we"ird"#).quoted(),
            r#""main"."default"."we""ird""#
        );
    }
}
