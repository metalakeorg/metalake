//! Writing to a table by its name, as a statement would name it. Rows from outside come in the representation of whatever
//! made them, and the core takes only the table's own, so this is where one becomes the other.

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use futures_util::{Stream, TryStreamExt};
use metalake::{Outcome, Ref, Table, TableWriter, Transaction, WriteOptions};
use serde::Serialize;

use crate::merge::person;
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

/// What goes with a commit.
#[derive(Debug, Default)]
pub struct Details {
    pub author: Option<String>,
    pub message: Option<String>,
    /// A writer's id and a version it never repeats for different data: a commit with these applies once.
    pub idempotent: Option<(String, i64)>,
}

impl Details {
    fn on(self, mut transaction: Transaction) -> Transaction {
        if let Some(author) = person(self.author) {
            transaction = transaction.author(author);
        }
        if let Some(message) = self.message {
            transaction = transaction.message(message);
        }
        if let Some((app, version)) = self.idempotent {
            transaction = transaction.idempotent(app, version);
        }
        transaction
    }
}

/// What a write left. `commit` is the sequence of its commit, or of the one that had already applied the same
/// idempotency key, when `applied` is false. Rows with nothing to commit have none.
#[derive(Debug, Serialize)]
pub struct Written {
    pub commit: Option<i64>,
    pub rows: u64,
    pub applied: bool,
}

impl Written {
    fn of(outcome: Outcome, rows: u64) -> Self {
        match outcome {
            Outcome::Committed(seq) => Self {
                commit: Some(seq),
                rows,
                applied: true,
            },
            Outcome::AlreadyApplied(seq) => Self {
                commit: Some(seq),
                rows: 0,
                applied: false,
            },
        }
    }
}

/// Whether a value of `from` can become one of `to`, losing nothing or failing. What is not known to be safe is refused:
/// `double` to `bigint` would cut `1.5` to `1` with no error.
fn allowed(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    fn fineness(unit: &TimeUnit) -> u8 {
        match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        }
    }
    let int = |t: &DataType| t.is_integer();
    match (from, to) {
        (a, b) if a == b => true,
        // A column with no value at all has no type of its own.
        (Null, _) => true,
        (Utf8View | LargeUtf8, Utf8) | (BinaryView | LargeBinary, Binary) => true,
        (List(a) | LargeList(a), List(b)) => allowed(a.data_type(), b.data_type()),
        (Struct(a), Struct(b)) => {
            a.len() == b.len()
                && b.iter().all(|f| {
                    let found = a.iter().find(|g| g.name() == f.name());
                    found.is_some_and(|g| allowed(g.data_type(), f.data_type()))
                })
        }
        (a, b) if int(a) && int(b) => true,
        (Float32, Float64) => true,
        (Decimal128(_, a), Decimal128(_, b)) => a <= b,
        (Timestamp(a, x), Timestamp(b, y)) => x.is_some() == y.is_some() && fineness(a) <= fineness(b),
        _ => false,
    }
}

/// How the columns of what is given become the table's: the same names in any order, and types that `allowed` says so.
#[derive(Debug)]
pub(crate) struct Adapter {
    schema: SchemaRef,
    columns: Vec<usize>,
}

impl Adapter {
    pub(crate) fn new(given: &Schema, table: &SchemaRef) -> Result<Self> {
        let names = |s: &Schema| s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
        let (have, want) = (names(given), names(table));
        let missing: Vec<&String> = want.iter().filter(|n| !have.contains(n)).collect();
        let extra: Vec<&String> = have.iter().filter(|n| !want.contains(n)).collect();
        if !missing.is_empty() || !extra.is_empty() {
            return Err(Error::usage(format!(
                "the columns are not the table's: missing {missing:?}, extra {extra:?}"
            )));
        }
        let mut columns = Vec::with_capacity(want.len());
        for field in table.fields() {
            let (at, column) = given.column_with_name(field.name()).expect("no column is missing");
            if !allowed(column.data_type(), field.data_type()) {
                let hint = match column.data_type().is_floating() && field.data_type().is_integer() {
                    true => "; a pandas column of integers with nulls is double: use the Int64 dtype",
                    false => "",
                };
                return Err(Error::usage(format!(
                    "{} is {} and the table has {}{hint}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
            columns.push(at);
        }
        Ok(Self {
            schema: table.clone(),
            columns,
        })
    }

    /// A batch in the table's representation. A value that does not fit fails, and does not become a null.
    pub(crate) fn adapt(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let strict = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(&self.columns)
            .map(|(field, at)| {
                let column = batch.column(*at);
                cast_with_options(column, field.data_type(), &strict)
                    .map_err(|e| Error::usage(format!("{}: {e}", field.name())))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| Error::usage(e.to_string()))
    }
}

impl Lake {
    /// Adds rows to a table in one commit. Rows that do not suit the table are refused before anything is written.
    pub async fn append<S>(&self, path: &TablePath, given: &Schema, mut batches: S, details: Details) -> Result<Written>
    where
        S: Stream<Item = Result<RecordBatch>> + Unpin + Send,
    {
        let (on, table) = self.table(path).await?;
        on.writable()?;
        let adapter = Adapter::new(given, table.schema())?;
        let mut writer = TableWriter::new(&self.catalog, &table, self.storage()?, WriteOptions::default())?;
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            rows += batch.num_rows() as u64;
            writer.write(&adapter.adapt(&batch)?).await?;
        }
        let files = writer.finish().await?;
        if files.is_empty() {
            return Ok(Written {
                commit: None,
                rows: 0,
                applied: false,
            });
        }
        let outcome = details.on(on.transaction().append(files)).commit().await?;
        Ok(Written::of(outcome, rows))
    }

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
    fn only_what_loses_nothing_or_fails_is_allowed() {
        use DataType::*;
        assert!(allowed(&Utf8View, &Utf8) && allowed(&LargeUtf8, &Utf8) && allowed(&Null, &Int64));
        assert!(allowed(&Int32, &Int64) && allowed(&Int64, &Int32) && allowed(&Float32, &Float64));
        assert!(allowed(&Decimal128(38, 2), &Decimal128(10, 2)));
        assert!(allowed(
            &Timestamp(TimeUnit::Microsecond, None),
            &Timestamp(TimeUnit::Nanosecond, None)
        ));
        assert!(!allowed(&Float64, &Int64), "1.5 would become 1");
        assert!(!allowed(&Float64, &Float32) && !allowed(&Utf8, &Int64) && !allowed(&Int64, &Utf8));
        assert!(!allowed(&Decimal128(10, 3), &Decimal128(10, 2)), "it would round");
        assert!(!allowed(
            &Timestamp(TimeUnit::Nanosecond, None),
            &Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(!allowed(
            &Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &Timestamp(TimeUnit::Microsecond, None)
        ));
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
