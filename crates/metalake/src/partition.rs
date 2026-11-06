//! Partition values (PAR-1, PAR-2): the transforms of Iceberg, over a value of the specification's types, and the
//! value in Iceberg's JSON single-value serialization, as `data_file.partition` holds it. The reference's
//! `transform_value` is the oracle these are tested against.

use serde_json::{Value, json};

use crate::error::{Error, Result};

/// A value of a field, as a transform takes it: integers in the unit of their type (days for a `date`,
/// microseconds for `time`, `timestamp` and `timestamptz`, nanoseconds for the `_ns` timestamps).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar<'a> {
    Null,
    Boolean(bool),
    Long(i64),
    Double(f64),
    Decimal(i128),
    String(&'a str),
    Bytes(&'a [u8]),
}

/// A partition transform (PAR-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

impl Transform {
    /// The transform a partition spec names: `identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day`,
    /// `hour` or `void`.
    pub fn parse(s: &str) -> Result<Self> {
        let width = |prefix: &str| {
            s.strip_prefix(prefix)?
                .strip_suffix(']')?
                .parse::<u32>()
                .ok()
                .filter(|w| *w > 0)
        };
        Ok(match s {
            "identity" => Self::Identity,
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "void" => Self::Void,
            _ => match (width("bucket["), width("truncate[")) {
                (Some(n), _) => Self::Bucket(n),
                (_, Some(w)) => Self::Truncate(w),
                _ => return Err(Error::Schema(format!("{s} is not a partition transform"))),
            },
        })
    }

    /// The name of a partition field over `column`, as Iceberg names them.
    pub fn field_name(&self, column: &str) -> String {
        match self {
            Self::Identity => column.to_string(),
            Self::Bucket(_) => format!("{column}_bucket"),
            Self::Truncate(_) => format!("{column}_trunc"),
            Self::Year => format!("{column}_year"),
            Self::Month => format!("{column}_month"),
            Self::Day => format!("{column}_day"),
            Self::Hour => format!("{column}_hour"),
            Self::Void => format!("{column}_null"),
        }
    }

    /// The partition value of `value`, of the specification's type `ty`, in Iceberg's JSON single-value
    /// serialization of the transform's result type: the source type for `identity` and `truncate`, `int` for the
    /// others. A null maps to null.
    pub fn apply(&self, ty: &str, value: Scalar<'_>) -> Result<Value> {
        let refuse = || Error::Schema(format!("{self:?} does not apply to a {ty} value {value:?}"));
        if matches!(value, Scalar::Null) || *self == Self::Void {
            return Ok(Value::Null);
        }
        Ok(match self {
            Self::Identity => single_value(ty, value).ok_or_else(refuse)?,
            Self::Bucket(n) => {
                let h = murmur3_32(&hash_bytes(ty, value).ok_or_else(refuse)?);
                json!((h & i32::MAX) % *n as i32)
            }
            Self::Truncate(w) => {
                let w = i128::from(*w);
                match (value, base(ty)) {
                    (Scalar::Long(v), "int" | "long") => {
                        let t = i128::from(v) - (((i128::from(v) % w) + w) % w);
                        json!(i64::try_from(t).map_err(|_| refuse())?)
                    }
                    (Scalar::Decimal(u), "decimal") => {
                        json!(decimal_text(u - (((u % w) + w) % w), scale(ty).ok_or_else(refuse)?))
                    }
                    (Scalar::String(s), "string") => json!(s.chars().take(w as usize).collect::<String>()),
                    (Scalar::Bytes(b), "binary") => json!(hex(&b[..b.len().min(w as usize)])),
                    _ => return Err(refuse()),
                }
            }
            Self::Year | Self::Month | Self::Day | Self::Hour => {
                let Scalar::Long(v) = value else { return Err(refuse()) };
                let micros = match base(ty) {
                    "date" => i128::from(v) * 86_400_000_000,
                    "timestamp" | "timestamptz" => i128::from(v),
                    "timestamp_ns" | "timestamptz_ns" => i128::from(v).div_euclid(1000),
                    _ => return Err(refuse()),
                };
                let days = micros.div_euclid(86_400_000_000) as i64;
                match self {
                    Self::Day => json!(days),
                    Self::Hour => json!(micros.div_euclid(3_600_000_000) as i64),
                    _ => {
                        let (y, m, _) = civil_from_days(days);
                        if *self == Self::Year {
                            json!(y - 1970)
                        } else {
                            json!((y - 1970) * 12 + m - 1)
                        }
                    }
                }
            }
            Self::Void => Value::Null,
        })
    }
}

/// A type without its parameters: `decimal`, `fixed`, or the type itself.
fn base(ty: &str) -> &str {
    ty.split(['(', '[']).next().unwrap_or(ty)
}

fn scale(ty: &str) -> Option<u32> {
    ty.strip_suffix(')')?.split_once(',')?.1.trim().parse().ok()
}

/// A value in Iceberg's JSON single-value serialization.
fn single_value(ty: &str, value: Scalar<'_>) -> Option<Value> {
    Some(match (base(ty), value) {
        ("boolean", Scalar::Boolean(b)) => json!(b),
        ("int" | "long", Scalar::Long(v)) => json!(v),
        ("float" | "double", Scalar::Double(v)) => json!(v),
        ("decimal", Scalar::Decimal(u)) => json!(decimal_text(u, scale(ty)?)),
        ("date", Scalar::Long(d)) => {
            let (y, m, d) = civil_from_days(d);
            json!(format!("{y:04}-{m:02}-{d:02}"))
        }
        ("time", Scalar::Long(us)) => json!(time_text(us.rem_euclid(86_400_000_000), 6)),
        ("timestamp", Scalar::Long(us)) => json!(timestamp_text(i128::from(us), 6, false)),
        ("timestamptz", Scalar::Long(us)) => json!(timestamp_text(i128::from(us), 6, true)),
        ("timestamp_ns", Scalar::Long(ns)) => json!(timestamp_text(i128::from(ns), 9, false)),
        ("timestamptz_ns", Scalar::Long(ns)) => json!(timestamp_text(i128::from(ns), 9, true)),
        ("string", Scalar::String(s)) => json!(s),
        ("uuid", Scalar::Bytes(b)) => json!(uuid::Uuid::from_slice(b).ok()?.to_string()),
        ("fixed" | "binary", Scalar::Bytes(b)) => json!(hex(b)),
        _ => return None,
    })
}

/// The days from 1970-01-01 of a date in Iceberg's JSON single-value serialization.
pub fn days(text: &str) -> Option<i64> {
    crate::evolve::days(text)
}

/// The hash serialization of a value (PAR-1).
fn hash_bytes(ty: &str, value: Scalar<'_>) -> Option<Vec<u8>> {
    Some(match (base(ty), value) {
        ("int" | "long" | "date" | "time" | "timestamp" | "timestamptz", Scalar::Long(v)) => v.to_le_bytes().to_vec(),
        ("timestamp_ns" | "timestamptz_ns", Scalar::Long(ns)) => ns.div_euclid(1000).to_le_bytes().to_vec(),
        ("decimal", Scalar::Decimal(u)) => min_be_bytes(u),
        ("string", Scalar::String(s)) => s.as_bytes().to_vec(),
        ("uuid" | "fixed" | "binary", Scalar::Bytes(b)) => b.to_vec(),
        _ => return None,
    })
}

/// A two's complement integer in the fewest big-endian bytes that hold it.
fn min_be_bytes(v: i128) -> Vec<u8> {
    let bytes = v.to_be_bytes();
    let mut start = 0;
    while start < 15 {
        let (b, next) = (bytes[start], bytes[start + 1]);
        if (b == 0x00 && next & 0x80 == 0) || (b == 0xFF && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

/// Murmur3, 32 bits, x86 variant, seed 0, as a signed integer.
pub fn murmur3_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h: u32 = 0;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k: u32 = 0;
        for (i, b) in tail.iter().enumerate() {
            k |= u32::from(*b) << (8 * i);
        }
        h ^= k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// A decimal's text, of scale `scale`, from its unscaled value.
fn decimal_text(unscaled: i128, scale: u32) -> String {
    let digits = format!("{:0>width$}", unscaled.unsigned_abs(), width = scale as usize + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
    let sign = if unscaled < 0 { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}

/// The civil date of a count of days from 1970-01-01 (Howard Hinnant's algorithm): (year, month, day).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// A time of day, from units of 10^-digits seconds since midnight.
fn time_text(units: i64, digits: u32) -> String {
    let per_second = 10i64.pow(digits);
    let (secs, frac) = (units / per_second, units % per_second);
    format!(
        "{:02}:{:02}:{:02}.{frac:0width$}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        width = digits as usize
    )
}

/// A timestamp, from units of 10^-digits seconds since the epoch, in Iceberg's form: `T` between date and time,
/// and `+00:00` for one with a zone.
fn timestamp_text(units: i128, digits: u32, zone: bool) -> String {
    let per_day = 86_400 * 10i128.pow(digits);
    let (days, rest) = (units.div_euclid(per_day) as i64, units.rem_euclid(per_day) as i64);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{}{}",
        time_text(rest, digits),
        if zone { "+00:00" } else { "" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur3_matches_iceberg() {
        // The values of Iceberg's specification, Appendix B.
        let hash = |ty: &str, v: Scalar<'_>| murmur3_32(&hash_bytes(ty, v).unwrap());
        assert_eq!(hash("int", Scalar::Long(34)), 2_017_239_379);
        assert_eq!(hash("long", Scalar::Long(34)), 2_017_239_379);
        assert_eq!(hash("decimal(4,2)", Scalar::Decimal(1420)), -500_754_589);
        assert_eq!(hash("date", Scalar::Long(17_486)), -653_330_422);
        assert_eq!(hash("time", Scalar::Long(81_068_000_000)), -662_762_989);
        assert_eq!(hash("timestamp", Scalar::Long(1_510_871_468_000_000)), -2_047_944_441);
        assert_eq!(hash("string", Scalar::String("iceberg")), 1_210_000_089);
        let uuid = uuid::Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        assert_eq!(hash("uuid", Scalar::Bytes(uuid.as_bytes())), 1_488_055_340);
        assert_eq!(hash("binary", Scalar::Bytes(&[0, 1, 2, 3])), -188_683_207);
    }

    #[test]
    fn transform_parse_and_field_name() {
        assert_eq!(Transform::parse("bucket[16]").unwrap(), Transform::Bucket(16));
        assert_eq!(Transform::parse("truncate[4]").unwrap(), Transform::Truncate(4));
        assert!(Transform::parse("bucket[0]").is_err() && Transform::parse("hours").is_err());
        assert_eq!(Transform::Day.field_name("at"), "at_day");
        assert_eq!(min_be_bytes(-1), [0xFF]);
        assert_eq!(min_be_bytes(128), [0x00, 0x80]);
        assert_eq!(
            timestamp_text(1_510_871_468_000_001, 6, true),
            "2017-11-16T22:31:08.000001+00:00"
        );
        assert_eq!(timestamp_text(-1, 6, false), "1969-12-31T23:59:59.999999");
    }
}
