//! Writing to a table by its name, as a statement would name it. Rows from outside come in the representation of whatever
//! made them, and the core takes only the table's own, so this is where one becomes the other.

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use futures_util::{Stream, TryStreamExt};
use metalake::{FileAdd, Outcome, Ref, Table, TableWriter, Transaction, WriteOptions};
use metalake_datafusion::CommitDetails;
use serde::Serialize;

use crate::merge::person;
use crate::{Error, Lake, Result, Session};

/// Where a table is, as SQL says it: the core's rule, which every face reads names by.
pub use metalake::TablePath;

/// A name as a statement says it, whatever it holds.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The `merge into` of an upsert. A table of nothing but its key has nothing to update.
fn merge_text(table: &str, source: &str, schema: &Schema, on: &[String]) -> String {
    let columns: Vec<String> = schema.fields().iter().map(|f| quoted(f.name())).collect();
    let matching: Vec<String> = on.iter().map(|k| format!("t.{0} = s.{0}", quoted(k))).collect();
    let assigned: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| !on.contains(f.name()))
        .map(|f| format!("{0} = s.{0}", quoted(f.name())))
        .collect();
    let values: Vec<String> = columns.iter().map(|c| format!("s.{c}")).collect();
    let update = match assigned.is_empty() {
        true => String::new(),
        false => format!(" when matched then update set {}", assigned.join(", ")),
    };
    format!(
        "merge into {table} t using {source} s on {}{update} when not matched then insert ({}) values ({})",
        matching.join(" and "),
        columns.join(", "),
        values.join(", ")
    )
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
    pub(crate) fn into_commit(self) -> CommitDetails {
        CommitDetails::new(person(self.author), self.message, self.idempotent)
    }

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
    /// Of `rows`, those new to the table and those that replaced one by its key.
    pub inserted: u64,
    pub updated: u64,
}

impl Written {
    /// What a statement left, which has no commit when it changed no row.
    pub(crate) fn of_statement(outcome: Option<Outcome>, rows: u64) -> Self {
        match outcome {
            Some(outcome) => Self::of(outcome, rows),
            None => Self::nothing(),
        }
    }

    fn nothing() -> Self {
        Self {
            commit: None,
            rows: 0,
            applied: false,
            inserted: 0,
            updated: 0,
        }
    }

    pub(crate) fn merged(self, inserted: u64, updated: u64) -> Self {
        Self {
            inserted,
            updated,
            ..self
        }
    }

    fn of(outcome: Outcome, rows: u64) -> Self {
        match outcome {
            Outcome::Committed(seq) => Self {
                commit: Some(seq),
                rows,
                applied: true,
                inserted: rows,
                updated: 0,
            },
            Outcome::AlreadyApplied(seq) => Self {
                commit: Some(seq),
                ..Self::nothing()
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

/// Rows in the store, not yet committed, and where they go.
struct Staged {
    on: Ref,
    table: Table,
    files: Vec<FileAdd>,
    rows: u64,
}

impl Lake {
    /// Writes rows to a table's store. Rows that do not suit the table are refused before anything is written.
    async fn stage<S>(&self, path: &TablePath, given: &Schema, mut batches: S) -> Result<Staged>
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
        Ok(Staged { on, table, files, rows })
    }

    /// Adds rows to a table in one commit. No rows commit nothing.
    pub async fn append<S>(&self, path: &TablePath, given: &Schema, batches: S, details: Details) -> Result<Written>
    where
        S: Stream<Item = Result<RecordBatch>> + Unpin + Send,
    {
        let staged = self.stage(path, given, batches).await?;
        if staged.files.is_empty() {
            return Ok(Written::nothing());
        }
        let transaction = staged.on.transaction().append(staged.files);
        Ok(Written::of(details.on(transaction).commit().await?, staged.rows))
    }

    /// Replaces what a table holds with these rows, in one commit, so that no rows leave it empty. What it held stays
    /// readable at its earlier points, for as long as a ref keeps them. It is refused if the table changes meanwhile.
    pub async fn overwrite<S>(&self, path: &TablePath, given: &Schema, batches: S, details: Details) -> Result<Written>
    where
        S: Stream<Item = Result<RecordBatch>> + Unpin + Send,
    {
        let staged = self.stage(path, given, batches).await?;
        let transaction = staged.on.transaction().append(staged.files);
        let transaction = transaction.replaces(&staged.table).await?;
        Ok(Written::of(details.on(transaction).commit().await?, staged.rows))
    }

    /// Replaces the rows of a table whose key is among these rows, and adds the others, in one commit. `on` is that key:
    /// a table has none of its own. Its values must be there and be unique here, since two rows for one key would make
    /// the outcome depend on their order, or, for a new key, leave it twice. All the rows are held in memory, for the join.
    /// A table that changes meanwhile refuses it, since a row's absence is what it decides on.
    pub async fn upsert<S>(
        &self,
        session: &Session,
        path: &TablePath,
        given: &Schema,
        mut batches: S,
        on: &[String],
        details: Details,
    ) -> Result<Written>
    where
        S: Stream<Item = Result<RecordBatch>> + Unpin + Send,
    {
        if on.is_empty() {
            return Err(Error::usage("an upsert needs the columns of its key"));
        }
        let (branch, table) = self.table(path).await?;
        branch.writable()?;
        let schema = table.schema().clone();
        if let Some(unknown) = on.iter().find(|k| schema.column_with_name(k).is_none()) {
            return Err(Error::usage(format!("{unknown} is not a column of the table")));
        }
        let adapter = Adapter::new(given, &schema)?;
        let mut rows = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            if batch.num_rows() > 0 {
                rows.push(adapter.adapt(&batch)?);
            }
        }
        if rows.is_empty() {
            return Ok(Written::nothing());
        }
        for key in on {
            if rows
                .iter()
                .any(|b| b.column_by_name(key).is_some_and(|c| c.null_count() > 0))
            {
                return Err(Error::usage(format!("{key} is part of the key and has nulls")));
            }
        }

        let source = session.source(schema.clone(), rows)?;
        let keys: Vec<String> = on.iter().map(|k| quoted(k)).collect();
        let repeated = format!(
            "select 1 from {} group by {} having count(*) > 1",
            source.name(),
            keys.join(", ")
        );
        if session.has_rows(&repeated).await? {
            return Err(Error::usage(format!("a key of {on:?} is there more than once")));
        }
        let text = merge_text(&path.quoted(), &source.name(), &schema, on);
        session.merge(&text, details).await
    }

    /// A table as it is now, on its ref, which Postgres says. A write takes it anew, so that it sees the table's schema
    /// of that moment and not of when a program first named it.
    pub async fn table(&self, path: &TablePath) -> Result<(Ref, Table)> {
        let on = self.find_ref(&path.on).await?;
        let namespace: Vec<&str> = path.namespace.iter().map(String::as_str).collect();
        let table = on.table(&namespace, &path.name).await?;
        Ok((on, table))
    }

    /// The version a writer's idempotency key reached on a ref, if any (CMT-5): where that writer goes on from.
    pub async fn key_version(&self, on: &str, app_id: &str) -> Result<Option<i64>> {
        Ok(self.find_ref(on).await?.key_version(app_id).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_casts() {
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
}
