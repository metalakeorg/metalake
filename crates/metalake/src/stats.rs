//! A file's statistics, from its Parquet footer (STA-1 to STA-8), in the shape a commit registers them: what the
//! writer knows once it has closed the file, without reading it again. NaN counts are not in a footer: the writer
//! counts them as it writes and hands them in; without them they are unknown (STA-2).
//!
//! Bounds are aggregated over row groups on their encoding, which orders as the values do (STA-5), so the least
//! lower and the greatest upper are found by comparing bytes.

use std::collections::HashMap;

use metalake_pg::records::FileAdd;
use parquet::basic::{LogicalType, TimeUnit};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use serde_json::Value;
use uuid::Uuid;

use crate::bounds::{self, Truncation};
use crate::table::{Field, Table};

/// The statistics of one file: its records, and per field the parallel arrays of STA-2.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FileStatistics {
    pub record_count: i64,
    pub field_ids: Vec<i32>,
    pub lower: Vec<Option<Vec<u8>>>,
    pub upper: Vec<Option<Vec<u8>>>,
    pub null_count: Vec<Option<i64>>,
    pub nan_count: Vec<Option<i64>>,
    pub value_count: Vec<Option<i64>>,
}

/// Where a file is and what the writer knows of it beyond its footer.
#[derive(Debug, Clone)]
pub struct FileFacts {
    /// Its path, or none for an inline file, whose bytes travel in `content` (INL-1).
    pub path: Option<String>,
    pub content: Option<Vec<u8>>,
    pub file_size: i64,
    pub footer_size: i32,
    /// The footer itself, for the catalog to serve it to readers (FTR-1); none to read it from the file.
    pub footer: Option<Vec<u8>>,
    pub spec_id: i32,
    /// One element per field of the spec, in Iceberg's JSON single-value serialization (STA-4).
    pub partition: Value,
    pub sort_order_id: Option<i32>,
}

impl FileStatistics {
    /// The file a commit adds.
    pub fn into_file_add(self, table_id: Uuid, facts: FileFacts) -> FileAdd {
        FileAdd {
            table_id,
            file_id: None,
            path: facts.path,
            spec_id: facts.spec_id,
            partition: facts.partition,
            sort_order_id: facts.sort_order_id,
            record_count: self.record_count,
            file_size: facts.file_size,
            footer_size: facts.footer_size,
            first_row_id: None,
            stat_field_ids: self.field_ids,
            lower: self.lower,
            upper: self.upper,
            null_count: self.null_count,
            nan_count: self.nan_count,
            value_count: Some(self.value_count),
            content: facts.content,
            footer: facts.footer,
        }
    }
}

/// The length of a Parquet file's footer (`FileMetaData`), from the file's last 8 bytes: the footer's length, then
/// `PAR1`.
pub fn footer_size(tail: &[u8]) -> Option<i32> {
    let n = tail.len();
    if n < 8 || &tail[n - 4..] != b"PAR1" {
        return None;
    }
    i32::try_from(u32::from_le_bytes(tail[n - 8..n - 4].try_into().ok()?)).ok()
}

/// The statistics of a file of `table` from its footer. Every leaf column whose field id is a primitive field of the
/// table gets an entry, the fields of the cluster key first and in key order (§2.5), the rest in the file's order.
pub fn of_footer(table: &Table, metadata: &ParquetMetaData, nan_counts: &HashMap<i32, i64>) -> FileStatistics {
    let fields: HashMap<i32, &Field> = table.fields().iter().map(|f| (f.field_id, f)).collect();
    let schema = metadata.file_metadata().schema_descr();
    let mut columns: Vec<(usize, &Field)> = (0..schema.num_columns())
        .filter_map(|i| {
            let ty = schema.column(i).self_type_ptr();
            let info = ty.get_basic_info();
            let field = fields.get(&(info.has_id().then(|| info.id())?))?;
            Some((i, *field))
        })
        .collect();
    let key = |f: &Field| {
        table
            .cluster_key()
            .iter()
            .position(|k| *k == f.field_id)
            .unwrap_or(usize::MAX)
    };
    columns.sort_by_key(|(i, f)| (key(f), *i));

    let mut stats = FileStatistics {
        record_count: metadata.file_metadata().num_rows(),
        ..Default::default()
    };
    for (i, field) in columns {
        let column = schema.column(i);
        let truncation = Truncation::of(&field.ty);
        let (mut lower, mut upper): (Option<Vec<u8>>, Option<Vec<u8>>) = (None, None);
        let (mut known, mut nulls, mut values) = (true, Some(0i64), 0i64);
        for group in metadata.row_groups() {
            let chunk = group.column(i);
            values += chunk.num_values();
            let s = chunk.statistics();
            nulls = match (nulls, s.and_then(Statistics::null_count_opt)) {
                (Some(n), Some(m)) => Some(n + m as i64),
                _ => None,
            };
            let Some(s) = s else {
                known = false;
                continue;
            };
            let all_null = s.null_count_opt() == Some(chunk.num_values() as u64);
            match bounds_of(field, column.logical_type_ref(), column.type_scale(), s) {
                Some((lo, hi)) => {
                    lower = Some(lower.map_or(lo.clone(), |l| l.min(lo)));
                    upper = Some(upper.map_or(hi.clone(), |u| u.max(hi)));
                }
                None if all_null => {}
                None => known = false,
            }
        }
        let (lower, upper) = match (known, lower, upper) {
            (true, Some(lo), Some(hi)) => (
                Some(bounds::truncate_lower(truncation, &lo)),
                bounds::truncate_upper(truncation, &hi),
            ),
            _ => (None, None),
        };
        stats.field_ids.push(field.field_id);
        stats.lower.push(lower);
        stats.upper.push(upper);
        stats.null_count.push(nulls);
        stats.nan_count.push(
            matches!(field.ty.as_str(), "float" | "double")
                .then(|| nan_counts.get(&field.field_id).copied())
                .flatten(),
        );
        stats.value_count.push(Some(values));
    }
    stats
}

/// The bounds of row group `group` in column `column` of a file, encoded for `field`'s type, or none when the footer
/// has none that can be trusted.
pub(crate) fn row_group_bounds(
    field: &Field,
    metadata: &ParquetMetaData,
    group: usize,
    column: usize,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let descr = metadata.file_metadata().schema_descr().column(column);
    let s = metadata.row_group(group).column(column).statistics()?;
    bounds_of(field, descr.logical_type_ref(), descr.type_scale(), s)
}

/// A row group's bounds of one column, encoded for the field's type, or none when the footer has none that can be
/// trusted: absent, of a type the field cannot have, or of the old signed order for bytes.
fn bounds_of(field: &Field, logical: Option<&LogicalType>, scale: i32, s: &Statistics) -> Option<(Vec<u8>, Vec<u8>)> {
    let ty = field.ty.as_str();
    let pair = |lo: Option<Vec<u8>>, hi: Option<Vec<u8>>| Some((lo?, hi?));
    match s {
        Statistics::Boolean(v) if ty == "boolean" => pair(
            v.min_opt().map(|b| bounds::boolean(*b)),
            v.max_opt().map(|b| bounds::boolean(*b)),
        ),
        Statistics::Int32(v) => {
            let encode = |x: i32| -> Option<Vec<u8>> {
                match ty {
                    "int" | "long" => Some(bounds::long(i64::from(x))),
                    "date" => Some(bounds::date(x)),
                    // A date written before its field was promoted to a timestamp (STA-7).
                    "timestamp" if matches!(logical, Some(LogicalType::Date)) => {
                        Some(bounds::long(i64::from(x).checked_mul(86_400_000_000)?))
                    }
                    "timestamp_ns" if matches!(logical, Some(LogicalType::Date)) => {
                        Some(bounds::long(i64::from(x).checked_mul(86_400_000_000_000)?))
                    }
                    "time" => Some(bounds::long(i64::from(x) * 1000)),
                    t if t.starts_with("decimal(") && decimal_scale(t) == Some(scale) => {
                        Some(bounds::decimal(i128::from(x)))
                    }
                    _ => None,
                }
            };
            pair(
                v.min_opt().and_then(|x| encode(*x)),
                v.max_opt().and_then(|x| encode(*x)),
            )
        }
        Statistics::Int64(v) => {
            let unit = |u: &TimeUnit| match u {
                TimeUnit::MILLIS => 1_000_000,
                TimeUnit::MICROS => 1_000,
                TimeUnit::NANOS => 1,
            };
            let nanos_per = match logical {
                Some(LogicalType::Timestamp(t)) => Some(unit(&t.unit)),
                Some(LogicalType::Time(t)) => Some(unit(&t.unit)),
                _ => None,
            };
            let encode = |x: i64| -> Option<Vec<u8>> {
                match ty {
                    "int" | "long" => Some(bounds::long(x)),
                    "time" | "timestamp" | "timestamptz" => Some(bounds::long(rescale(x, nanos_per?, 1_000)?)),
                    "timestamp_ns" | "timestamptz_ns" => Some(bounds::long(rescale(x, nanos_per?, 1)?)),
                    t if t.starts_with("decimal(") && decimal_scale(t) == Some(scale) => {
                        Some(bounds::decimal(i128::from(x)))
                    }
                    _ => None,
                }
            };
            pair(
                v.min_opt().and_then(|x| encode(*x)),
                v.max_opt().and_then(|x| encode(*x)),
            )
        }
        Statistics::Float(v) if ty == "float" || ty == "double" => pair(
            v.min_opt().and_then(|x| bounds::double(f64::from(*x))),
            v.max_opt().and_then(|x| bounds::double(f64::from(*x))),
        ),
        Statistics::Double(v) if ty == "double" => pair(
            v.min_opt().and_then(|x| bounds::double(*x)),
            v.max_opt().and_then(|x| bounds::double(*x)),
        ),
        Statistics::ByteArray(_) | Statistics::FixedLenByteArray(_) => {
            if s.is_min_max_deprecated() && !s.is_min_max_backwards_compatible() {
                return None;
            }
            let (lo, hi) = (s.min_bytes_opt()?, s.max_bytes_opt()?);
            match ty {
                "string" if std::str::from_utf8(lo).is_ok() && std::str::from_utf8(hi).is_ok() => {
                    Some((lo.to_vec(), hi.to_vec()))
                }
                "binary" | "uuid" => Some((lo.to_vec(), hi.to_vec())),
                t if t.starts_with("fixed[") => Some((lo.to_vec(), hi.to_vec())),
                t if t.starts_with("decimal(") && decimal_scale(t) == Some(scale) => {
                    Some((bounds::decimal(unscaled(lo)?), bounds::decimal(unscaled(hi)?)))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// `x` in units of `from_nanos` nanoseconds, in units of `to_nanos`: exact, or none.
fn rescale(x: i64, from_nanos: i64, to_nanos: i64) -> Option<i64> {
    if from_nanos >= to_nanos {
        x.checked_mul(from_nanos / to_nanos)
    } else {
        let d = to_nanos / from_nanos;
        (x % d == 0).then_some(x / d)
    }
}

/// The scale of a type `decimal(P,S)`.
fn decimal_scale(ty: &str) -> Option<i32> {
    ty.strip_suffix(')')?.split_once(',')?.1.trim().parse().ok()
}

/// A big-endian two's complement integer of up to 16 bytes, as Parquet stores a decimal's unscaled value.
fn unscaled(b: &[u8]) -> Option<i128> {
    if b.is_empty() || b.len() > 16 {
        return None;
    }
    let fill = if b[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut bytes = [fill; 16];
    bytes[16 - b.len()..].copy_from_slice(b);
    Some(i128::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rescale_exact_only() {
        assert_eq!(rescale(5, 1_000_000, 1_000), Some(5_000));
        assert_eq!(rescale(5_000, 1, 1_000), Some(5));
        assert_eq!(rescale(5_001, 1, 1_000), None);
        assert_eq!(rescale(i64::MAX, 1_000_000, 1_000), None);
    }

    #[test]
    fn promoted_date_bounds() {
        let field = |ty: &str| Field {
            field_id: 1,
            parent_field_id: None,
            position: 1,
            name: "day".into(),
            ty: ty.into(),
            required: false,
            doc: None,
            initial_default: None,
            write_default: None,
        };
        let s = Statistics::int32(Some(1), Some(2), None, Some(0), false);
        let date = Some(&LogicalType::Date);
        let want = |per_day: i64| Some((bounds::long(per_day), bounds::long(2 * per_day)));
        assert_eq!(bounds_of(&field("timestamp"), date, 0, &s), want(86_400_000_000));
        assert_eq!(bounds_of(&field("timestamp_ns"), date, 0, &s), want(86_400_000_000_000));
        assert_eq!(bounds_of(&field("timestamp"), None, 0, &s), None);
    }

    #[test]
    fn unscaled_decimal_sign() {
        assert_eq!(unscaled(&[0xFF]), Some(-1));
        assert_eq!(unscaled(&[0x00, 0x80]), Some(128));
        assert_eq!(unscaled(&[0x80, 0x00]), Some(-32768));
        assert_eq!(unscaled(&[]), None);
        assert_eq!(decimal_scale("decimal(10,2)"), Some(2));
        assert_eq!(footer_size(&[1, 0, 0, 0, b'P', b'A', b'R', b'1']), Some(1));
        assert_eq!(footer_size(b"nope"), None);
    }
}
