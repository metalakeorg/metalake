//! Where a table is, by its name: in a catalog, a schema, its own name (I7). Every face that takes a table's name reads
//! it by this rule, so that a statement, a command, a library call and a pipeline mean the same table by the same words.
//! A name is turned into a table by the catalog (`Ref::table`, TBL-1), which answers its id; this only reads the words.
//!
//! A name says no ref: the ref is the session's, as `USE BRANCH`, a selector or `lake.on` set it, and a branch covers
//! every catalog of the lake. A catalog is the first level of a nested namespace, the schema the levels after it.

use std::fmt;

/// The ref a session is on until it says another.
pub const MAIN: &str = "main";
/// The catalog a name without one is in: the first level of its namespace.
pub const DEFAULT_CATALOG: &str = "default";
/// The schema a name without one is in, in its catalog.
pub const DEFAULT_NAMESPACE: &str = "default";
/// What separates the levels of a nested namespace, in a name and in an engine's schema name.
pub const NAMESPACE_SEPARATOR: char = '.';

/// A table's name, on a ref: `t` is `default.default.t`, `s.t` is `default.s.t`, `c.s.t` is in catalog `c`, and
/// `c.a.b.t` in the schema `a.b` of `c`. The ref is not in the name: whoever reads it says it.
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
        write!(f, "{}: a table is [[catalog.]schema.]table", self.0)
    }
}

impl std::error::Error for InvalidPath {}

impl TablePath {
    /// A name as text, on `main`: its parts split at dots, the first of three or more the catalog, the last the table,
    /// and every part between a level of the schema (`c.a.b.t`).
    pub fn parse(text: &str) -> Result<Self, InvalidPath> {
        Self::parse_on(text, MAIN)
    }

    /// A name as text, on the ref `on`: a session's, which reads and writes on another ref than `main`.
    pub fn parse_on(text: &str, on: &str) -> Result<Self, InvalidPath> {
        let parts: Vec<&str> = text.split(NAMESPACE_SEPARATOR).collect();
        if parts.iter().any(|part| part.is_empty()) {
            return Err(InvalidPath(text.to_string()));
        }
        let (namespace, name) = match parts.as_slice() {
            [name] => (vec![DEFAULT_CATALOG, DEFAULT_NAMESPACE], *name),
            [schema, name] => (vec![DEFAULT_CATALOG, *schema], *name),
            [namespace @ .., name] => (namespace.to_vec(), *name),
            [] => unreachable!("a split has a part"),
        };
        Ok(Self {
            on: on.to_string(),
            namespace: namespace.into_iter().map(str::to_string).collect(),
            name: name.to_string(),
        })
    }

    /// A name as a SQL engine gives it, already split into catalog, schema and table, on the ref `on`: the catalog is the
    /// namespace's first level, and the schema the levels after it, joined by dots.
    pub fn from_parts(on: &str, catalog: Option<&str>, schema: Option<&str>, table: &str) -> Self {
        let mut namespace = vec![catalog.unwrap_or(DEFAULT_CATALOG).to_string()];
        namespace.extend(levels(schema.unwrap_or(DEFAULT_NAMESPACE)));
        Self {
            on: on.to_string(),
            namespace,
            name: table.to_string(),
        }
    }

    /// Its catalog: the first level of its namespace.
    pub fn catalog_name(&self) -> &str {
        self.namespace.first().map_or(DEFAULT_CATALOG, String::as_str)
    }

    /// Its schema as an engine names it: the levels of its namespace past the catalog, joined by dots.
    pub fn schema_name(&self) -> String {
        schema_of(&self.namespace)
    }

    /// As a statement of a session on its ref names it, whatever its names hold: a schema of several levels is one
    /// identifier.
    pub fn quoted(&self) -> String {
        format!(
            "{}.{}.{}",
            quoted(self.catalog_name()),
            quoted(&self.schema_name()),
            quoted(&self.name)
        )
    }
}

/// The levels of a namespace named as an engine's schema.
pub fn levels(schema: &str) -> Vec<String> {
    schema.split(NAMESPACE_SEPARATOR).map(str::to_string).collect()
}

/// A namespace's schema as an engine names it: its levels past the catalog, joined by dots.
pub fn schema_of(namespace: &[String]) -> String {
    namespace
        .get(1..)
        .unwrap_or_default()
        .join(&NAMESPACE_SEPARATOR.to_string())
}

/// A name as a statement says it, whatever it holds.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}",
            self.namespace.join(&NAMESPACE_SEPARATOR.to_string()),
            self.name
        )
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
    fn i7_parse_defaults_and_levels() {
        assert_eq!(
            TablePath::parse("t").unwrap(),
            name("main", &["default", "default"], "t")
        );
        assert_eq!(TablePath::parse("s.t").unwrap(), name("main", &["default", "s"], "t"));
        assert_eq!(TablePath::parse("c.s.t").unwrap(), name("main", &["c", "s"], "t"));
        assert_eq!(
            TablePath::parse("c.a.b.t").unwrap(),
            name("main", &["c", "a", "b"], "t")
        );
        assert_eq!(TablePath::parse("c.a.b.t").unwrap().to_string(), "c.a.b.t");
        assert_eq!(TablePath::parse("t").unwrap().to_string(), "default.default.t");
    }

    /// A session on another ref: every name is on it, whatever it says.
    #[test]
    fn i7_parse_on_another_ref() {
        assert_eq!(
            TablePath::parse_on("t", "dev").unwrap(),
            name("dev", &["default", "default"], "t")
        );
        assert_eq!(
            TablePath::parse_on("s.t", "dev").unwrap(),
            name("dev", &["default", "s"], "t")
        );
        assert_eq!(
            TablePath::parse_on("c.s.t", "dev").unwrap(),
            name("dev", &["c", "s"], "t")
        );
    }

    #[test]
    fn parse_empty_part_refused() {
        for text in ["", ".", "a..b", ".t", "n."] {
            let refused = TablePath::parse(text).unwrap_err();
            assert!(refused.to_string().starts_with(&format!("{text}:")), "{refused}");
        }
    }

    #[test]
    fn i7_from_parts_as_parse() {
        assert_eq!(
            TablePath::from_parts("main", None, None, "t"),
            TablePath::parse("t").unwrap()
        );
        assert_eq!(
            TablePath::from_parts("main", None, Some("s"), "t"),
            TablePath::parse("s.t").unwrap()
        );
        assert_eq!(
            TablePath::from_parts("main", Some("c"), Some("s"), "t"),
            TablePath::parse("c.s.t").unwrap()
        );
        assert_eq!(
            TablePath::from_parts("dev", Some("c"), Some("a.b"), "t"),
            TablePath::parse_on("c.a.b.t", "dev").unwrap(),
            "a schema of several levels is one name with dots"
        );
    }

    #[test]
    fn i7_quoted_names_no_ref() {
        let path = TablePath::parse_on("c.a.b.t", "dev").unwrap();
        assert_eq!(path.quoted(), r#""c"."a.b"."t""#);
        assert_eq!((path.catalog_name(), path.schema_name().as_str()), ("c", "a.b"));
        assert_eq!(
            TablePath::from_parts("main", None, None, r#"we"ird"#).quoted(),
            r#""default"."default"."we""ird""#
        );
    }
}
