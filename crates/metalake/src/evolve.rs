//! A file's columns under the table's fields as a read sees them (§6): a file keeps the schema it was written with,
//! and a column is found by its field id, whatever it was called. A field the file has not is its initial default,
//! or null (TYP-5); a field whose type was promoted (TYP-3) is widened from the file's; a struct's children are found
//! the same way, and a dropped field is never asked for.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal128Type, Float32Type, Int32Type, Int64Type};
use arrow_array::types::{TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, new_null_array,
};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, TimeUnit};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::schema::{self, FIELD_ID};
use crate::table::{Field, Table};

type Arrow<T> = std::result::Result<T, ArrowError>;

const MICROS_PER_DAY: i64 = 86_400_000_000;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// The table's fields by id, to read files under.
#[derive(Debug, Clone)]
pub struct Evolve {
    fields: HashMap<i32, Field>,
}

impl Evolve {
    pub fn new(table: &Table) -> Self {
        Self {
            fields: table.fields().iter().map(|f| (f.field_id, f.clone())).collect(),
        }
    }

    /// The column of `field`, from the file's column of its id, or of `rows` rows when the file has none.
    pub fn column(&self, field: &ArrowField, file: Option<&ArrayRef>, rows: usize) -> Result<ArrayRef> {
        self.of(field, file, rows)
            .map_err(|e| Error::Schema(format!("{}: {e}", field.name())))
    }

    fn of(&self, field: &ArrowField, file: Option<&ArrayRef>, rows: usize) -> Arrow<ArrayRef> {
        match file {
            Some(array) => self.present(field, array),
            None => self.missing(field, rows),
        }
    }

    fn present(&self, field: &ArrowField, array: &ArrayRef) -> Arrow<ArrayRef> {
        Ok(match (array.data_type(), field.data_type()) {
            (DataType::Struct(from), DataType::Struct(to)) => {
                let s = array.as_struct();
                let children = to
                    .iter()
                    .map(|child| {
                        let at = from.iter().position(|f| id(f).is_some() && id(f) == id(child));
                        self.of(child, at.map(|i| s.column(i)), s.len())
                    })
                    .collect::<Arrow<Vec<_>>>()?;
                Arc::new(StructArray::try_new(to.clone(), children, s.nulls().cloned())?)
            }
            (DataType::List(_), DataType::List(element)) => {
                let l = array.as_list::<i32>();
                let values = self.of(element, Some(l.values()), 0)?;
                Arc::new(ListArray::try_new(
                    element.clone(),
                    l.offsets().clone(),
                    values,
                    l.nulls().cloned(),
                )?)
            }
            (DataType::Map(_, _), DataType::Map(entries, sorted)) => {
                let m = array.as_map();
                let given: ArrayRef = Arc::new(m.entries().clone());
                let evolved = self.of(entries, Some(&given), 0)?;
                Arc::new(MapArray::try_new(
                    entries.clone(),
                    m.offsets().clone(),
                    evolved.as_struct().clone(),
                    m.nulls().cloned(),
                    *sorted,
                )?)
            }
            (from, to) => promote(array, from, to)?,
        })
    }

    fn missing(&self, field: &ArrowField, rows: usize) -> Arrow<ArrayRef> {
        let spec = id(field).and_then(|i| self.fields.get(&i));
        let default = spec.and_then(|f| f.initial_default.as_ref());
        match (field.data_type(), default, spec) {
            // A struct with a default is there in every row, its children their own defaults.
            (DataType::Struct(children), Some(_), _) => {
                let columns = children
                    .iter()
                    .map(|c| self.of(c, None, rows))
                    .collect::<Arrow<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new(children.clone(), columns, None)?))
            }
            (DataType::List(_) | DataType::Map(_, _), Some(_), _) => Err(ArrowError::NotYetImplemented(
                "the core does not read the initial default of a list or a map yet".into(),
            )),
            (ty, Some(v), Some(spec)) => constant(&spec.ty, ty, v, rows)
                .ok_or_else(|| ArrowError::SchemaError(format!("its initial default {v} is not a {}", spec.ty))),
            _ if !field.is_nullable() => Err(ArrowError::SchemaError(
                "required, in no column of the file and without an initial default".into(),
            )),
            (ty, _, _) => Ok(new_null_array(ty, rows)),
        }
    }
}

fn id(f: &ArrowField) -> Option<i32> {
    f.metadata().get(FIELD_ID).and_then(|v| v.parse().ok())
}

/// A column of a primitive type, under the type it was promoted to (TYP-3), or the same.
fn promote(array: &ArrayRef, from: &DataType, to: &DataType) -> Arrow<ArrayRef> {
    let days = |per_day: i64| {
        move |d: i32| {
            i64::from(d)
                .checked_mul(per_day)
                .ok_or_else(|| ArrowError::ComputeError(format!("the date {d} is past the timestamps")))
        }
    };
    Ok(match (from, to) {
        (DataType::Int32, DataType::Int64) => {
            Arc::new(array.as_primitive::<Int32Type>().unary::<_, Int64Type>(i64::from))
        }
        (DataType::Float32, DataType::Float64) => Arc::new(
            array
                .as_primitive::<Float32Type>()
                .unary::<_, arrow_array::types::Float64Type>(f64::from),
        ),
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, None)) => Arc::new(
            array
                .as_primitive::<Date32Type>()
                .try_unary::<_, TimestampMicrosecondType, _>(days(MICROS_PER_DAY))?,
        ),
        (DataType::Date32, DataType::Timestamp(TimeUnit::Nanosecond, None)) => Arc::new(
            array
                .as_primitive::<Date32Type>()
                .try_unary::<_, TimestampNanosecondType, _>(days(NANOS_PER_DAY))?,
        ),
        (DataType::Decimal128(_, s), DataType::Decimal128(p, t)) if s == t => Arc::new(
            array
                .as_primitive::<Decimal128Type>()
                .clone()
                .with_precision_and_scale(*p, *t)?,
        ),
        _ => schema::retype(array, to)?,
    })
}

/// `rows` copies of a value in Iceberg's JSON single-value serialization, of the specification's type `ty`.
fn constant(ty: &str, data_type: &DataType, v: &Value, rows: usize) -> Option<ArrayRef> {
    let base = ty.split(['(', '[']).next().unwrap_or(ty);
    let text = v.as_str();
    Some(match (base, data_type) {
        ("boolean", _) => Arc::new(BooleanArray::from(vec![v.as_bool()?; rows])),
        ("int", _) => Arc::new(Int32Array::from(vec![i32::try_from(v.as_i64()?).ok()?; rows])),
        ("long", _) => Arc::new(Int64Array::from(vec![v.as_i64()?; rows])),
        ("float", _) => Arc::new(Float32Array::from(vec![v.as_f64()? as f32; rows])),
        ("double", _) => Arc::new(Float64Array::from(vec![v.as_f64()?; rows])),
        ("decimal", DataType::Decimal128(p, s)) => Arc::new(
            Decimal128Array::from(vec![unscaled(text?, *s as u32)?; rows])
                .with_precision_and_scale(*p, *s)
                .ok()?,
        ),
        ("date", _) => Arc::new(Date32Array::from(vec![i32::try_from(days(text?)?).ok()?; rows])),
        ("time", _) => Arc::new(Time64MicrosecondArray::from(vec![time(text?, 6)?; rows])),
        ("timestamp" | "timestamptz", DataType::Timestamp(_, zone)) => Arc::new(
            TimestampMicrosecondArray::from(vec![timestamp(text?, 6, zone.is_some())?; rows])
                .with_timezone_opt(zone.clone()),
        ),
        ("timestamp_ns" | "timestamptz_ns", DataType::Timestamp(_, zone)) => Arc::new(
            TimestampNanosecondArray::from(vec![timestamp(text?, 9, zone.is_some())?; rows])
                .with_timezone_opt(zone.clone()),
        ),
        ("string", _) => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(text?, rows))),
        ("uuid", _) => {
            let u = uuid::Uuid::parse_str(text?).ok()?;
            Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(u.as_bytes(), rows)).ok()?)
        }
        ("fixed", DataType::FixedSizeBinary(n)) => {
            let b = unhex(text?)?;
            if b.len() != *n as usize {
                return None;
            }
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(std::iter::repeat_n(Some(b.as_slice()), rows), *n)
                    .ok()?,
            )
        }
        ("binary", _) => {
            let b = unhex(text?)?;
            Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(b.as_slice(), rows)))
        }
        _ => return None,
    })
}

/// The unscaled value of a decimal's text, at `scale`: `-12.3` at scale 2 is -1230.
fn unscaled(text: &str, scale: u32) -> Option<i128> {
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || fraction.len() > scale as usize
        || !(whole.chars().chain(fraction.chars())).all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let padded = format!("{whole}{fraction:0<width$}", width = scale as usize);
    padded.parse::<i128>().ok().map(|u| sign * u)
}

/// The days from 1970-01-01 of a date `YYYY-MM-DD`, in the proleptic calendar (Howard Hinnant's algorithm).
pub(crate) fn days(text: &str) -> Option<i64> {
    let mut parts = text.rsplitn(3, '-');
    let (d, m, y) = (parts.next()?, parts.next()?, parts.next()?);
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    Some(era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468)
}

/// A time of day `HH:MM:SS[.f]`, in units of 10^-digits seconds since midnight.
fn time(text: &str, digits: u32) -> Option<i64> {
    let (clock, fraction) = text.split_once('.').unwrap_or((text, ""));
    let mut parts = clock.split(':');
    let (h, m, s): (i64, i64, i64) = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    if parts.next().is_some() || h > 23 || m > 59 || s > 59 || fraction.len() > digits as usize {
        return None;
    }
    let fraction: i64 = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<width$}", width = digits as usize).parse().ok()?
    };
    Some(((h * 60 + m) * 60 + s) * 10i64.pow(digits) + fraction)
}

/// A timestamp `YYYY-MM-DDTHH:MM:SS[.f]`, with `+00:00` when it has a zone, in units of 10^-digits seconds.
fn timestamp(text: &str, digits: u32, zone: bool) -> Option<i64> {
    let text = if zone { text.strip_suffix("+00:00")? } else { text };
    let (date, clock) = text.split_once('T')?;
    days(date)?
        .checked_mul(86_400 * 10i64.pow(digits))?
        .checked_add(time(clock, digits)?)
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse_as_icebergs_single_values() {
        assert_eq!(days("1970-01-01"), Some(0));
        assert_eq!(days("2017-11-16"), Some(17_486));
        assert_eq!(days("0001-01-01"), Some(-719_162));
        assert_eq!(days("2000-03-01"), Some(11_017));
        assert_eq!(days("2017-13-01"), None);
        assert_eq!(time("22:31:08", 6), Some(81_068_000_000));
        assert_eq!(time("22:31:08.1", 6), Some(81_068_100_000));
        assert_eq!(time("24:00:00", 6), None);
        assert_eq!(
            timestamp("2017-11-16T22:31:08.000001+00:00", 6, true),
            Some(1_510_871_468_000_001)
        );
        assert_eq!(timestamp("2017-11-16T22:31:08.000001", 6, true), None);
        assert_eq!(timestamp("1969-12-31T23:59:59.999999999", 9, false), Some(-1));
        assert_eq!(unscaled("-12.3", 2), Some(-1230));
        assert_eq!(unscaled("14.20", 2), Some(1420));
        assert_eq!(unscaled("1.234", 2), None);
        assert_eq!(unscaled("", 2), None);
        assert_eq!(unhex("00ff"), Some(vec![0, 255]));
        assert_eq!(unhex("0"), None);
    }

    #[test]
    fn a_column_a_file_has_not_is_its_default_or_null() {
        let field = |id: i32, ty: &str, required: bool, default: Option<Value>| Field {
            field_id: id,
            parent_field_id: None,
            position: id,
            name: format!("c{id}"),
            ty: ty.into(),
            required,
            doc: None,
            initial_default: default,
            write_default: None,
        };
        let fields = [
            field(1, "long", false, None),
            field(2, "long", true, None),
            field(3, "date", false, Some(Value::from("2017-11-16"))),
            field(4, "date", false, Some(Value::from(3))),
            field(5, "list", false, Some(serde_json::json!([]))),
        ];
        let evolve = Evolve {
            fields: fields.iter().map(|f| (f.field_id, f.clone())).collect(),
        };
        let arrow = |f: &Field, ty: DataType| {
            ArrowField::new(&f.name, ty, !f.required)
                .with_metadata([(FIELD_ID.to_string(), f.field_id.to_string())].into())
        };
        let nulls = evolve.column(&arrow(&fields[0], DataType::Int64), None, 3).unwrap();
        assert_eq!(nulls.null_count(), 3);
        let required = evolve.column(&arrow(&fields[1], DataType::Int64), None, 3);
        assert!(matches!(required, Err(Error::Schema(m)) if m.contains("without an initial default")));
        let dates = evolve.column(&arrow(&fields[2], DataType::Date32), None, 2).unwrap();
        assert_eq!(dates.as_primitive::<Date32Type>().values(), &[17_486, 17_486]);
        let wrong = evolve.column(&arrow(&fields[3], DataType::Date32), None, 2);
        assert!(matches!(wrong, Err(Error::Schema(m)) if m.contains("is not a date")));
        let element = Arc::new(ArrowField::new("element", DataType::Int64, true));
        let list = evolve.column(&arrow(&fields[4], DataType::List(element)), None, 2);
        assert!(matches!(list, Err(Error::Schema(m)) if m.contains("list or a map")));
    }

    #[test]
    fn promotions_widen_and_keep_nulls() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(-1), None, Some(i32::MAX)]));
        let longs = promote(&ints, &DataType::Int32, &DataType::Int64).unwrap();
        assert_eq!(
            longs.as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(-1), None, Some(2_147_483_647)])
        );
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![1, -1]));
        let micros = promote(
            &dates,
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            micros.as_primitive::<TimestampMicrosecondType>().values(),
            &[MICROS_PER_DAY, -MICROS_PER_DAY]
        );
        let nanos = promote(
            &dates,
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            nanos.as_primitive::<TimestampNanosecondType>().values(),
            &[NANOS_PER_DAY, -NANOS_PER_DAY]
        );
        let far: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
        assert!(
            promote(
                &far,
                &DataType::Date32,
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            )
            .is_err()
        );
        assert!(promote(&ints, &DataType::Int32, &DataType::Utf8).is_err());
    }
}
