//! Registering files in place: the Parquet files under a prefix of the lake's store become a table's, by commits of the
//! catalog's rows and no byte copied, as when a Hive table is moved under MetaLake. Each file keeps its path; the table
//! writes what is new under its own location, and owns the source only when it adopts it (OWN-1, OWN-2).
//!
//! A run plans first: it lists the source, reads each file's footer and directories, and settles the schema. A file
//! that does not fit is refused, and a run with a refusal commits nothing, so that a table never silently lacks part of
//! its data. Then it commits: the table's creation, with its name mapping (ENG-4), then the files in batches, their
//! statistics read through the mapping (ENG-3). A run again adds only what the table does not hold, so a run that
//! failed is resumed by running it again.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow_schema::{Field as ArrowField, Schema};
use futures_util::{StreamExt, TryStreamExt, stream};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::Serialize;
use serde_json::{Value, json};

use crate::catalog::Ref;
use crate::error::{Error, Result};
use crate::mapping::{self, NameMapping};
use crate::scan::Scan;
use crate::schema::{self, spec_fields, spec_type};
use crate::stats::{self, FileFacts};
use crate::storage::{Listed, Storage};
use crate::table::Table;
use crate::write::NewTable;

/// Footers read at once.
const CONCURRENCY: usize = 32;
/// Files of one commit.
pub const BATCH: usize = 10_000;
/// What a first request of a file's tail fetches: most footers, and their length.
const TAIL: u64 = 64 * 1024;
/// The partition types a directory's value is read as.
const PARTITION_TYPES: [&str; 5] = ["string", "int", "long", "date", "boolean"];
/// Hive's name of a null partition value.
const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// What to register.
#[derive(Debug, Clone)]
pub struct Register<'a> {
    /// The prefix whose files are registered, on the lake's store.
    pub source: &'a str,
    pub namespace: &'a [&'a str],
    pub name: &'a str,
    /// The directories' keys that are partition columns, in order, each with its type.
    pub partition: Vec<(&'a str, &'a str)>,
    /// The table owns the source from this run's first commit (OWN-2).
    pub adopt: bool,
    /// Where the table writes what is new; the warehouse's, by name, when not given.
    pub location: Option<String>,
    /// Plan and report, committing nothing.
    pub check: bool,
    pub batch: usize,
}

/// What a run did, or would do with `check`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Report {
    pub table: String,
    pub created: bool,
    pub check: bool,
    pub registered: usize,
    pub already: usize,
    pub skipped_hidden: usize,
    pub refused: Vec<Refusal>,
    pub rows: i64,
    pub bytes: u64,
    pub commits: Vec<i64>,
    pub adopted: Option<String>,
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let verb = match (self.check, self.refused.is_empty()) {
            (true, _) => "would register",
            (false, true) => "registered",
            (false, false) => "registered nothing:",
        };
        write!(
            f,
            "{} {verb} {} files, {} rows, {} bytes",
            self.table, self.registered, self.rows, self.bytes
        )?;
        write!(
            f,
            "; {} already held, {} hidden skipped",
            self.already, self.skipped_hidden
        )?;
        if self.created {
            write!(f, "; created")?;
        }
        if let Some(prefix) = &self.adopted {
            write!(f, "; owns {prefix}")?;
        }
        if !self.commits.is_empty() {
            let commits: Vec<String> = self.commits.iter().map(i64::to_string).collect();
            write!(f, "; commits {}", commits.join(", "))?;
        }
        for refusal in &self.refused {
            write!(f, "\nrefused {} ({}): {}", refusal.path, refusal.rule, refusal.why)?;
        }
        Ok(())
    }
}

/// A file a run refuses, the rule it breaks, and why.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Refusal {
    pub path: String,
    pub rule: &'static str,
    pub why: String,
}

/// A file that fits, as the plan found it.
#[derive(Debug, Clone)]
struct Candidate {
    url: String,
    size: u64,
    partition: Value,
    rows: i64,
}

/// Registers the files under `r.source` as table `r.namespace.r.name` on `on`, creating it on the first run.
pub async fn register(on: &Ref, storage: &Storage, r: &Register<'_>) -> Result<Report> {
    let source = r.source.trim_end_matches('/');
    if storage.path(source).is_err() {
        return Err(Error::Location(format!(
            "{source} is not on the lake's store, {}: the source is spelled as the warehouse is, scheme and authority",
            storage.base()
        )));
    }
    for (column, ty) in &r.partition {
        if !PARTITION_TYPES.contains(ty) {
            return Err(Error::Invalid(format!(
                "partition {column}: {ty} is none of {}",
                PARTITION_TYPES.join(", ")
            )));
        }
    }
    let existing = match on.table(r.namespace, r.name).await {
        Ok(table) => Some(table),
        Err(Error::NotFound(_)) => None,
        Err(e) => return Err(e),
    };
    let mut report = Report {
        table: format!("{}.{}", r.namespace.join("."), r.name),
        created: existing.is_none(),
        check: r.check,
        ..Report::default()
    };
    if let Some(table) = &existing {
        check_partitioning(table, &r.partition)?;
    }
    let mapping = match &existing {
        Some(table) => Some(NameMapping::of_table(table)?.ok_or_else(|| {
            Error::Invalid(format!(
                "{} has no name mapping ({}): it holds files MetaLake wrote, not registered ones",
                report.table,
                mapping::PROPERTY
            ))
        })?),
        None => None,
    };
    let held: HashSet<String> = match &existing {
        Some(table) => Scan::new(on.catalog(), table)
            .list()
            .await?
            .map_ok(|f| f.path)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect(),
        None => HashSet::new(),
    };

    // The plan: what fits, what is refused, and the schema the files give.
    let mut listed = Vec::new();
    for object in storage.listed(source).await? {
        let relative = object.url[source.len()..].trim_start_matches('/').to_string();
        if relative
            .split('/')
            .any(|part| part.starts_with('_') || part.starts_with('.'))
        {
            report.skipped_hidden += 1;
        } else if held.contains(&object.url) {
            report.already += 1;
        } else {
            listed.push((object, relative));
        }
    }
    listed.sort_by(|a, b| a.0.url.cmp(&b.0.url));
    let read = stream::iter(listed.into_iter().map(|(object, relative)| {
        let storage = storage.clone();
        async move {
            let footer = footer(&storage, &object).await?;
            Ok::<_, Error>((object, relative, footer))
        }
    }))
    .buffered(CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;

    let mut merged = Merged::default();
    let mut candidates = Vec::new();
    for (object, relative, footer) in read {
        let refuse = |rule, why| Refusal {
            path: object.url.clone(),
            rule,
            why,
        };
        let metadata = match footer {
            Ok((metadata, _)) => metadata,
            Err(why) => {
                report.refused.push(refuse("parquet", why));
                continue;
            }
        };
        let partition = match partition_of(&relative, &r.partition) {
            Ok(partition) => partition,
            Err(why) => {
                report.refused.push(refuse("partition", why));
                continue;
            }
        };
        let file = metadata.file_metadata();
        let schema = match parquet_to_arrow_schema(file.schema_descr(), file.key_value_metadata()) {
            Ok(schema) => schema,
            Err(e) => {
                report.refused.push(refuse("schema", e.to_string()));
                continue;
            }
        };
        let fits = match (&existing, &mapping) {
            (Some(table), Some(mapping)) => fits_table(table, mapping, &schema),
            _ => merged.add(&schema),
        };
        if let Err(why) = fits {
            report.refused.push(refuse("schema", why));
            continue;
        }
        candidates.push(Candidate {
            url: object.url,
            size: object.size,
            partition,
            rows: file.num_rows(),
        });
    }
    report.registered = candidates.len();
    report.rows = candidates.iter().map(|c| c.rows).sum();
    report.bytes = candidates.iter().map(|c| c.size).sum();
    if r.adopt {
        report.adopted = Some(source.to_string());
    }
    if r.check || !report.refused.is_empty() {
        report.commits.clear();
        return Ok(report);
    }

    // The commits: the table, then its files in batches.
    let table = match existing {
        Some(table) => table,
        None => {
            let schema = merged.schema(&r.partition)?;
            let fields = spec_fields(&schema)?;
            let mapping = NameMapping::of_fields(&fields).to_json();
            let partitioning: Vec<(&str, &str)> = r.partition.iter().map(|(c, _)| (*c, "identity")).collect();
            // The table's namespace and each above it that the ref lacks, in the same commit (NS-2).
            let known: HashSet<Vec<String>> = on.namespaces().await?.into_iter().map(|n| n.namespace).collect();
            let mut tx = on.transaction();
            for depth in 1..=r.namespace.len() {
                let level = &r.namespace[..depth];
                if !known.contains(&level.iter().map(|s| s.to_string()).collect::<Vec<_>>()) {
                    tx = tx.create_namespace(level);
                }
            }
            let (tx, id) = tx.create_table(NewTable {
                namespace: r.namespace,
                name: r.name,
                schema: &schema,
                location: r.location.clone(),
                cluster_key: vec![],
                partitioning,
                properties: vec![(mapping::PROPERTY, &mapping)],
            })?;
            let tx = if r.adopt { tx.own(id, &[source]) } else { tx };
            report.commits.push(tx.message("register").commit().await?.seq());
            on.table(r.namespace, r.name).await?
        }
    };
    let mut adopt = r.adopt && !report.created;
    for batch in candidates.chunks(r.batch.max(1)) {
        let adds = stream::iter(batch.iter().cloned().map(|c| {
            let (storage, table) = (storage.clone(), table.clone());
            async move { file_add(&storage, &table, &c).await }
        }))
        .buffered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
        let mut tx = on.transaction().append(adds).message("register");
        if adopt {
            (tx, adopt) = (tx.own(table.id(), &[source]), false);
        }
        report.commits.push(tx.commit().await?.seq());
    }
    if adopt {
        let tx = on.transaction().own(table.id(), &[source]).message("register");
        report.commits.push(tx.commit().await?.seq());
    }
    Ok(report)
}

/// A file's footer, its length, read with its size: the tail, then the rest of the footer if the tail was short. A file
/// that is not Parquet is a refusal, not an error.
async fn footer(storage: &Storage, object: &Listed) -> Result<std::result::Result<(ParquetMetaData, i32), String>> {
    if object.size < 12 {
        return Ok(Err(format!("{} bytes is too few for Parquet", object.size)));
    }
    let mut tail = storage.tail(&object.url, object.size, TAIL.min(object.size)).await?;
    let Some(length) = stats::footer_size(&tail).filter(|l| *l >= 0) else {
        return Ok(Err("no Parquet footer: the file does not end in PAR1".into()));
    };
    let needed = length as u64 + 8;
    if needed > object.size {
        return Ok(Err(format!("a footer of {length} bytes in a file of {}", object.size)));
    }
    if needed > tail.len() as u64 {
        tail = storage.tail(&object.url, object.size, needed).await?;
    }
    let end = tail.len() - 8;
    Ok(
        ParquetMetaDataReader::decode_metadata(&tail[end - length as usize..end])
            .map(|metadata| (metadata, length))
            .map_err(|e| format!("its footer does not read: {e}")),
    )
}

/// The file's row, ready to commit: its statistics from its footer, read again, through the table's mapping (ENG-3).
async fn file_add(storage: &Storage, table: &Table, c: &Candidate) -> Result<metalake_pg::records::FileAdd> {
    let listed = Listed {
        url: c.url.clone(),
        size: c.size,
        age: 0,
    };
    let (metadata, footer_size) = footer(storage, &listed)
        .await?
        .map_err(|why| Error::Corrupt(format!("{}: {why}", c.url)))?;
    let statistics = stats::of_footer(table, &metadata, &HashMap::new());
    Ok(statistics.into_file_add(
        table.id(),
        FileFacts {
            path: Some(c.url.clone()),
            content: None,
            file_size: c.size as i64,
            footer_size,
            footer: None,
            spec_id: table.spec_id().unwrap_or(0),
            partition: c.partition.clone(),
            sort_order_id: None,
        },
    ))
}

/// The partition of a file from the directories between the source and its name: `k=v` for each declared column, in
/// order, unescaped as Hive escapes them, each value in Iceberg's JSON single value (PAR-2).
fn partition_of(relative: &str, declared: &[(&str, &str)]) -> std::result::Result<Value, String> {
    let directories: Vec<&str> = relative.split('/').collect();
    let directories = &directories[..directories.len() - 1];
    if directories.len() != declared.len() {
        let keys: Vec<&str> = declared.iter().map(|(c, _)| *c).collect();
        return Err(format!(
            "it lies {} directories deep, and the partition is [{}]",
            directories.len(),
            keys.join(", ")
        ));
    }
    let mut values = Vec::with_capacity(declared.len());
    for (directory, (column, ty)) in directories.iter().zip(declared) {
        let Some((key, raw)) = directory.split_once('=') else {
            return Err(format!("{directory} is not {column}=value"));
        };
        if unescaped(key) != *column {
            return Err(format!("{directory} is not {column}=value"));
        }
        let value = unescaped(raw);
        values.push(if value == NULL_PARTITION {
            Value::Null
        } else {
            typed(&value, ty).ok_or_else(|| format!("{column}={value} is not a {ty}"))?
        });
    }
    Ok(Value::Array(values))
}

/// A directory's value as a partition value of `ty`, in Iceberg's JSON single value.
fn typed(value: &str, ty: &str) -> Option<Value> {
    Some(match ty {
        "string" => json!(value),
        "int" => json!(value.parse::<i32>().ok()?),
        "long" => json!(value.parse::<i64>().ok()?),
        "boolean" => json!(value.parse::<bool>().ok()?),
        "date" => {
            let mut parts = value.splitn(3, '-').map(|p| p.parse::<u32>().ok());
            let (y, m, d) = (parts.next()??, parts.next()??, parts.next()??);
            let days = [
                31,
                if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                    29
                } else {
                    28
                },
                31,
                30,
                31,
                30,
                31,
                31,
                30,
                31,
                30,
                31,
            ];
            if value.len() != 10 || !(1..=12).contains(&m) || d == 0 || d > days[m as usize - 1] {
                return None;
            }
            json!(value)
        }
        _ => return None,
    })
}

/// A directory's text as Hive escapes it, `%3A` for `:`.
fn unescaped(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match (
            bytes[i],
            bytes.get(i + 1).copied().and_then(hex),
            bytes.get(i + 2).copied().and_then(hex),
        ) {
            (b'%', Some(hi), Some(lo)) => {
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            (b, _, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The table's partitioning is the one a later run declares: identity fields over those columns, in order.
fn check_partitioning(table: &Table, declared: &[(&str, &str)]) -> Result<()> {
    let names: HashMap<i32, &str> = table
        .fields()
        .iter()
        .filter(|f| f.parent_field_id.is_none())
        .map(|f| (f.field_id, f.name.as_str()))
        .collect();
    let spec = table
        .spec_id()
        .and_then(|id| table.specs().get(&id))
        .cloned()
        .unwrap_or(json!([]));
    let has: Vec<(String, String)> = spec
        .as_array()
        .into_iter()
        .flatten()
        .map(|f| {
            let source = f["source_ids"][0]
                .as_i64()
                .and_then(|i| names.get(&(i as i32)).copied());
            (
                source.unwrap_or("?").to_string(),
                f["transform"].as_str().unwrap_or("?").to_string(),
            )
        })
        .collect();
    let wanted: Vec<(String, String)> = declared
        .iter()
        .map(|(c, _)| (c.to_string(), "identity".into()))
        .collect();
    if has != wanted {
        return Err(Error::Invalid(format!(
            "{} is partitioned by {has:?}, not by {wanted:?} as declared",
            table.name()
        )));
    }
    Ok(())
}

/// Whether a file's columns are the table's: each top-level column named by the mapping, of the field's type.
fn fits_table(table: &Table, mapping: &NameMapping, schema: &Schema) -> std::result::Result<(), String> {
    let mapped = mapping.apply(schema);
    let fields: HashMap<i32, &str> = table.fields().iter().map(|f| (f.field_id, f.ty.as_str())).collect();
    for column in mapped.fields() {
        let id = column
            .metadata()
            .get(schema::FIELD_ID)
            .and_then(|i| i.parse::<i32>().ok());
        let Some(ty) = id.and_then(|i| fields.get(&i)) else {
            return Err(format!("{} is a column the table lacks", column.name()));
        };
        if let Ok(file) = spec_type(column.data_type())
            && file != *ty
        {
            return Err(format!("{} is a {file}, and the table's is a {ty}", column.name()));
        }
    }
    Ok(())
}

/// The schema of a first run: the files' top-level columns, by name, in the order first seen; one missing from a file
/// is optional.
#[derive(Debug, Default)]
struct Merged {
    columns: BTreeMap<usize, ArrowField>,
    at: HashMap<String, usize>,
    files: usize,
    seen: HashMap<String, usize>,
}

impl Merged {
    fn add(&mut self, schema: &Schema) -> std::result::Result<(), String> {
        for column in schema.fields() {
            if let Some(at) = self.at.get(column.name()) {
                let known = &self.columns[at];
                if known.data_type() != column.data_type() {
                    return Err(format!(
                        "{} is a {}, and another file's is a {}",
                        column.name(),
                        column.data_type(),
                        known.data_type()
                    ));
                }
            }
        }
        for column in schema.fields() {
            if !self.at.contains_key(column.name()) {
                let at = self.columns.len();
                self.at.insert(column.name().clone(), at);
                // A column first seen after some files is missing from them.
                self.columns.insert(
                    at,
                    column
                        .as_ref()
                        .clone()
                        .with_nullable(column.is_nullable() || self.files > 0),
                );
            }
            *self.seen.entry(column.name().clone()).or_default() += 1;
        }
        self.files += 1;
        Ok(())
    }

    /// The table's schema: the files' columns, each optional unless every file has it as required, then the partition
    /// columns no file holds, of their declared types.
    fn schema(&self, partition: &[(&str, &str)]) -> Result<Schema> {
        let mut fields: Vec<ArrowField> = self
            .columns
            .values()
            .map(|c| {
                let everywhere = self.seen.get(c.name()) == Some(&self.files);
                c.clone().with_nullable(c.is_nullable() || !everywhere)
            })
            .collect();
        for (column, ty) in partition {
            if !self.at.contains_key(*column) {
                fields.push(ArrowField::new(*column, schema::arrow_type(ty)?, true));
            }
        }
        if fields.is_empty() {
            return Err(Error::Invalid("the source holds no Parquet file to register".into()));
        }
        Ok(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn partition_from_directories() {
        let declared = [("dt", "date"), ("country", "string")];
        assert_eq!(
            partition_of("dt=2024-01-02/country=ES/part-0.parquet", &declared),
            Ok(json!(["2024-01-02", "ES"]))
        );
        assert_eq!(
            partition_of("dt=2024-01-02/country=a%3Ab/x.parquet", &declared),
            Ok(json!(["2024-01-02", "a:b"])),
            "unescaped as Hive escapes"
        );
        assert_eq!(
            partition_of("dt=__HIVE_DEFAULT_PARTITION__/country=ES/x.parquet", &declared),
            Ok(json!([null, "ES"]))
        );
        for (relative, why) in [
            ("dt=2024-13-01/country=ES/x.parquet", "is not a date"),
            ("country=ES/dt=2024-01-02/x.parquet", "is not dt=value"),
            ("dt=2024-01-02/x.parquet", "1 directories deep"),
            ("x.parquet", "0 directories deep"),
        ] {
            let refused = partition_of(relative, &declared).unwrap_err();
            assert!(refused.contains(why), "{relative}: {refused}");
        }
        assert_eq!(partition_of("x.parquet", &[]), Ok(json!([])));
        assert_eq!(partition_of("n=7/x.parquet", &[("n", "int")]), Ok(json!([7])));
        assert!(partition_of("n=x/x.parquet", &[("n", "long")]).is_err());
        assert!(typed("2024-02-29", "date").is_some() && typed("2023-02-29", "date").is_none());
    }

    #[test]
    fn merged_schema_by_name() {
        let a = Schema::new(vec![
            ArrowField::new("k", DataType::Int64, false),
            ArrowField::new("v", DataType::Utf8, true),
        ]);
        let b = Schema::new(vec![
            ArrowField::new("k", DataType::Int64, false),
            ArrowField::new("w", DataType::Int32, false),
        ]);
        let mut merged = Merged::default();
        merged.add(&a).unwrap();
        merged.add(&b).unwrap();
        let schema = merged.schema(&[("dt", "date")]).unwrap();
        let shape: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            shape,
            [("k", false), ("v", true), ("w", true), ("dt", true)],
            "k in every file and required; w only in the second; dt from the directories"
        );
        let clash = Schema::new(vec![ArrowField::new("k", DataType::Utf8, true)]);
        assert!(merged.add(&clash).unwrap_err().contains("k is a Utf8"));
    }
}
