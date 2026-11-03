//! Bounds in the order-preserving encoding of the specification (STA-5 to STA-8): for two values `a` and `b` of one
//! field, `encode(a) < encode(b)`, comparing bytes, exactly when `a < b`. Pruning is then plain SQL over the catalog.
//! The reference's `encode_bound`, `truncate_lower` and `truncate_upper` are the oracle these are tested against.

use crate::partition::Scalar;

/// The length strings and binaries are truncated to: 16 code points, 16 bytes (STA-8).
pub const TRUNCATE: usize = 16;

/// A `boolean`: one byte.
pub fn boolean(v: bool) -> Vec<u8> {
    vec![u8::from(v)]
}

/// An `int` or a `long`, and every type encoded as a `long` (`time`, the timestamps, in their unit): 64-bit
/// two's complement, big-endian, sign bit flipped. An `int` and a `long` share it, so promotion keeps bounds (STA-7).
pub fn long(v: i64) -> Vec<u8> {
    ((v as u64) ^ (1 << 63)).to_be_bytes().to_vec()
}

/// A `date`, days from 1970-01-01: 32-bit two's complement, big-endian, sign bit flipped.
pub fn date(days: i32) -> Vec<u8> {
    ((days as u32) ^ (1 << 31)).to_be_bytes().to_vec()
}

/// A `float` or a `double`, as the double it is or promotes to, in IEEE 754 `totalOrder`: `-0.0` precedes `+0.0`.
/// NaN is never a bound (STA-6): none.
pub fn double(v: f64) -> Option<Vec<u8>> {
    if v.is_nan() {
        return None;
    }
    let bits = v.to_bits();
    let ordered = if bits >> 63 == 0 { bits ^ (1 << 63) } else { !bits };
    Some(ordered.to_be_bytes().to_vec())
}

/// A `decimal`, by its unscaled value: 128-bit two's complement, big-endian, sign bit flipped.
pub fn decimal(unscaled: i128) -> Vec<u8> {
    ((unscaled as u128) ^ (1 << 127)).to_be_bytes().to_vec()
}

/// A `string`: its UTF-8 bytes, which order by code point.
pub fn string(v: &str) -> Vec<u8> {
    v.as_bytes().to_vec()
}

/// A `uuid` (its 16 bytes, big-endian), a `fixed[L]` or a `binary`: the bytes.
pub fn bytes(v: &[u8]) -> Vec<u8> {
    v.to_vec()
}

/// A value of a field of type `ty`, as `partition::Scalar` holds it (a `date` in days, a `time` and the timestamps
/// in their unit), encoded as a bound: none for a null, a NaN, or a value the type cannot have.
pub fn value(ty: &str, v: Scalar<'_>) -> Option<Vec<u8>> {
    let base = ty.split(['(', '[']).next().unwrap_or(ty);
    match (base, v) {
        ("boolean", Scalar::Boolean(b)) => Some(boolean(b)),
        (
            "int" | "long" | "time" | "timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns",
            Scalar::Long(x),
        ) => Some(long(x)),
        ("date", Scalar::Long(x)) => Some(date(i32::try_from(x).ok()?)),
        ("float" | "double", Scalar::Double(x)) => double(x),
        ("decimal", Scalar::Decimal(x)) => Some(decimal(x)),
        ("string", Scalar::String(x)) => Some(string(x)),
        ("binary" | "uuid" | "fixed", Scalar::Bytes(x)) => Some(bytes(x)),
        _ => None,
    }
}

/// What a type's bounds may be truncated as (STA-8): a `string` by code points, a `binary` by bytes, others never.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truncation {
    String,
    Binary,
    None,
}

impl Truncation {
    /// The truncation of a type of the specification.
    pub fn of(ty: &str) -> Self {
        match ty {
            "string" => Self::String,
            "binary" => Self::Binary,
            _ => Self::None,
        }
    }
}

/// A lower bound, truncated: its prefix.
pub fn truncate_lower(t: Truncation, b: &[u8]) -> Vec<u8> {
    match t {
        Truncation::String => match std::str::from_utf8(b) {
            Ok(s) => s.chars().take(TRUNCATE).collect::<String>().into_bytes(),
            Err(_) => b.to_vec(),
        },
        Truncation::Binary => b[..b.len().min(TRUNCATE)].to_vec(),
        Truncation::None => b.to_vec(),
    }
}

/// An upper bound, truncated: its prefix with the last code point (or byte) incremented, after dropping trailing
/// ones that cannot be (U+10FFFF, `0xFF`); the increment skips the surrogates. None if nothing can be incremented.
/// A bound short enough is kept as it is.
pub fn truncate_upper(t: Truncation, b: &[u8]) -> Option<Vec<u8>> {
    match t {
        Truncation::String => {
            let Ok(s) = std::str::from_utf8(b) else {
                return Some(b.to_vec());
            };
            if s.chars().count() <= TRUNCATE {
                return Some(b.to_vec());
            }
            let mut prefix: Vec<char> = s.chars().take(TRUNCATE).collect();
            while let Some(last) = prefix.pop() {
                if last != char::MAX {
                    let next = match last as u32 + 1 {
                        0xD800 => '\u{E000}',
                        n => char::from_u32(n).expect("below U+10FFFF and not a surrogate"),
                    };
                    prefix.push(next);
                    return Some(prefix.into_iter().collect::<String>().into_bytes());
                }
            }
            None
        }
        Truncation::Binary => {
            if b.len() <= TRUNCATE {
                return Some(b.to_vec());
            }
            let mut prefix = b[..TRUNCATE].to_vec();
            while let Some(last) = prefix.pop() {
                if last != u8::MAX {
                    prefix.push(last + 1);
                    return Some(prefix);
                }
            }
            None
        }
        Truncation::None => Some(b.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_edges_of_each_encoding_order_as_their_values() {
        assert!(long(i64::MIN) < long(-1) && long(-1) < long(0) && long(0) < long(i64::MAX));
        assert_eq!(long(i64::MIN), [0u8; 8]);
        assert!(date(i32::MIN) < date(-1) && date(-1) < date(0));
        let ds: Vec<Vec<u8>> = [
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            2.5,
            f64::INFINITY,
        ]
        .into_iter()
        .map(|v| double(v).unwrap())
        .collect();
        assert!(ds.windows(2).all(|w| w[0] < w[1]), "{ds:?}");
        assert_eq!(double(f64::NAN), None);
        assert!(decimal(i128::MIN) < decimal(-1) && decimal(-1) < decimal(0) && decimal(0) < decimal(i128::MAX));
        assert!(boolean(false) < boolean(true));
    }

    #[test]
    fn values_encode_as_their_type_does() {
        assert_eq!(value("int", Scalar::Long(-3)), Some(long(-3)));
        assert_eq!(value("timestamptz", Scalar::Long(7)), Some(long(7)));
        assert_eq!(value("date", Scalar::Long(-2)), Some(date(-2)));
        assert_eq!(value("date", Scalar::Long(i64::MAX)), None);
        assert_eq!(value("double", Scalar::Double(0.5)), double(0.5));
        assert_eq!(value("float", Scalar::Double(f64::NAN)), None);
        assert_eq!(value("decimal(10,2)", Scalar::Decimal(-5)), Some(decimal(-5)));
        assert_eq!(value("string", Scalar::String("ñ")), Some(string("ñ")));
        assert_eq!(value("fixed[2]", Scalar::Bytes(&[1, 2])), Some(vec![1, 2]));
        assert_eq!(value("boolean", Scalar::Boolean(true)), Some(boolean(true)));
        assert_eq!(value("long", Scalar::String("1")), None);
        assert_eq!(value("long", Scalar::Null), None);
    }

    #[test]
    fn upper_bounds_skip_surrogates_and_drop_what_cannot_grow() {
        let s = |v: &str| v.as_bytes().to_vec();
        let upper = |v: &str| truncate_upper(Truncation::String, v.as_bytes());
        assert_eq!(upper("short"), Some(s("short")));
        assert_eq!(upper("abcdefghijklmnopq"), Some(s("abcdefghijklmnoq")));
        let before_surrogates = format!("{}\u{D7FF}x", "a".repeat(15));
        assert_eq!(
            upper(&before_surrogates),
            Some(format!("{}\u{E000}", "a".repeat(15)).into_bytes())
        );
        let at_max = format!("a{}x", "\u{10FFFF}".repeat(15));
        assert_eq!(upper(&at_max), Some(s("b")));
        assert_eq!(upper(&"\u{10FFFF}".repeat(17)), None);
        assert_eq!(
            truncate_lower(Truncation::String, "abcdefghijklmnopq".as_bytes()),
            s("abcdefghijklmnop")
        );
        let mut bin = vec![7u8; 15];
        bin.extend([0xFF, 0x01]);
        assert_eq!(
            truncate_upper(Truncation::Binary, &bin),
            Some(vec![7u8; 14].into_iter().chain([8]).collect())
        );
        assert_eq!(truncate_upper(Truncation::Binary, &[0xFF; 17]), None);
        assert_eq!(truncate_upper(Truncation::None, &[0xFF; 40]), Some(vec![0xFF; 40]));
    }
}
