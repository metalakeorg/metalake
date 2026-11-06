//! The protocol's SQL, embedded by `build.rs` in the order it installs, and rendered for one catalog.

/// What a source holds, which says whether it is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// The tables of the specification (`spec/v0/schema/`), created under the catalog's `search_path`.
    Table,
    /// A file of the reference the protocol reuses as it is (`sql/v0/reference-reused.txt`).
    Reference,
    /// The protocol's own (`sql/v0/`), rendered: every object it names is qualified with the catalog's schema.
    Protocol,
}

include!(concat!(env!("OUT_DIR"), "/sources.rs"));

/// One source, ready to run in the catalog's schema.
pub(crate) struct Source {
    pub name: &'static str,
    pub sql: String,
}

/// The sources of `kinds`, in the order they install, rendered for the catalog in `schema`.
pub(crate) fn sources(schema: &str, kinds: &[Kind]) -> Vec<Source> {
    SOURCES
        .iter()
        .filter(|(_, kind, _)| kinds.contains(kind))
        .map(|&(name, kind, text)| Source {
            name,
            sql: if kind == Kind::Protocol {
                render(text, schema)
            } else {
                text.to_string()
            },
        })
        .collect()
}

/// An identifier, quoted for SQL.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A source of the protocol rendered for one catalog: `@schema@` becomes the catalog's schema, quoted, so that no
/// object depends on the caller's `search_path`.
pub fn render(source: &str, schema: &str) -> String {
    source.replace("@schema@", &quote_ident(schema))
}

/// The views, functions and types a source creates: (kind, name as written).
pub(crate) fn created(sql: &str) -> Vec<(&'static str, &str)> {
    sql.lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let rest = line
                .strip_prefix("create or replace ")
                .or_else(|| line.strip_prefix("create "))?;
            let (kind, rest) = [("function", "function "), ("type", "type "), ("view", "view ")]
                .into_iter()
                .find_map(|(kind, prefix)| rest.strip_prefix(prefix).map(|r| (kind, r)))?;
            let name = rest.split(|c: char| c == '(' || c.is_whitespace()).next()?;
            Some((kind, name))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_order() {
        let kinds: Vec<Kind> = SOURCES.iter().map(|(_, kind, _)| *kind).collect();
        let first_protocol = kinds.iter().position(|k| *k == Kind::Protocol).unwrap();
        assert!(kinds.iter().take_while(|k| **k == Kind::Table).count() > 0);
        assert!(kinds[..first_protocol].iter().all(|k| *k != Kind::Protocol));
        assert!(
            kinds
                .iter()
                .skip_while(|k| **k == Kind::Table)
                .all(|k| *k != Kind::Table)
        );
    }

    #[test]
    fn install_order_before_reused_file() {
        let names: Vec<&str> = SOURCES.iter().map(|(name, _, _)| *name).collect();
        let retention = names
            .iter()
            .position(|n| *n == "spec/v0/reference/13-retention.sql")
            .unwrap();
        assert_eq!(names[retention + 1], "sql/v0/50-retention.sql");
    }

    #[test]
    fn render_qualified_schema() {
        assert_eq!(
            render("create function @schema@.f()", "a\"b"),
            "create function \"a\"\"b\".f()"
        );
        let protocol = sources("cat", &[Kind::Protocol]);
        assert!(protocol.iter().all(|s| !s.sql.contains("@schema@")));
        assert!(
            sources("cat", &[Kind::Table])
                .iter()
                .all(|s| s.name.starts_with("spec/v0/schema/"))
        );
    }

    #[test]
    fn parse_created_objects() {
        let sql = "create function \"c\".plan(r int)\n  create or replace view \"c\".refs as\ncreate type \"c\".file_add as (";
        assert_eq!(
            created(sql),
            vec![
                ("function", "\"c\".plan"),
                ("view", "\"c\".refs"),
                ("type", "\"c\".file_add")
            ]
        );
    }
}
