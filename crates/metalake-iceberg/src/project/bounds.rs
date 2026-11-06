//! Conversion between the catalog's order-preserving bounds (STA-5) and Iceberg's single-value serialization (ICE-3,
//! ICE-6). The reference's `iceberg_bound` and `iceberg_bound_in` are the oracles of both directions.

/// A bound of a field of type `ty`, as it is stored, in Iceberg's serialization; none if the type has no such thing.
pub fn to_iceberg(ty: &str, bound: &[u8]) -> Option<Vec<u8>> {
    let base = ty.split(['(', '[']).next().unwrap_or(ty);
    let widened = widened(base, bound);
    let bound = widened.as_deref().unwrap_or(bound);
    match base {
        "boolean" => Some(vec![u8::from(*bound.first()? != 0)]),
        "int" => Some(i32::try_from(long(bound)?).ok()?.to_le_bytes().to_vec()),
        "date" => Some(int32(bound)?.to_le_bytes().to_vec()),
        "long" | "time" | "timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns" => {
            Some(long(bound)?.to_le_bytes().to_vec())
        }
        "float" => Some((double(bound)? as f32).to_le_bytes().to_vec()),
        "double" => Some(double(bound)?.to_le_bytes().to_vec()),
        "decimal" => Some(shortest(decimal(bound)?)),
        "geometry" | "geography" => {
            let (points, _) = bound.as_chunks::<8>();
            let points = points.iter().map(|p| double(p)).collect::<Option<Vec<f64>>>()?;
            Some(points.iter().flat_map(|p| p.to_le_bytes()).collect())
        }
        "string" | "uuid" | "fixed" | "binary" => Some(bound.to_vec()),
        _ => None,
    }
}

/// Converts an Iceberg bound of a field of type `ty` to the catalog's encoding. `None` drops the bound: a NaN (STA-6), a
/// type without bounds (STA-9), or bytes of the wrong length. A missing bound only costs pruning.
pub fn from_iceberg(ty: &str, bound: &[u8]) -> Option<Vec<u8>> {
    use metalake::bounds as encode;
    let base = ty.split(['(', '[']).next().unwrap_or(ty);
    match base {
        "boolean" => Some(encode::boolean(*bound.first()? != 0)),
        "int" | "long" | "time" | "timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns" => {
            Some(encode::long(little_endian(bound)?))
        }
        "date" => Some(encode::date(i32::try_from(little_endian(bound)?).ok()?)),
        "float" => encode::double(f64::from(f32::from_le_bytes(bound.try_into().ok()?))),
        "double" => encode::double(f64::from_le_bytes(bound.try_into().ok()?)),
        "decimal" => Some(encode::decimal(two_complement(bound)?)),
        "string" | "uuid" | "fixed" | "binary" => Some(bound.to_vec()),
        _ => None,
    }
}

/// An integer of 4 or 8 bytes, little-endian.
fn little_endian(bound: &[u8]) -> Option<i64> {
    match bound.len() {
        4 => Some(i64::from(i32::from_le_bytes(bound.try_into().ok()?))),
        8 => Some(i64::from_le_bytes(bound.try_into().ok()?)),
        _ => None,
    }
}

/// Two's complement, big-endian, of up to 16 bytes.
fn two_complement(bound: &[u8]) -> Option<i128> {
    if bound.is_empty() || bound.len() > 16 {
        return None;
    }
    let fill = if bound[0] & 0x80 == 0 { 0 } else { 0xff };
    let mut wide = [fill; 16];
    wide[16 - bound.len()..].copy_from_slice(bound);
    Some(i128::from_be_bytes(wide))
}

/// A `date` promoted to a timestamp keeps its bounds of 4 bytes, in days (STA-7).
fn widened(base: &str, bound: &[u8]) -> Option<Vec<u8>> {
    let per_day: i64 = match base {
        "timestamp" | "timestamptz" => 86_400_000_000,
        "timestamp_ns" | "timestamptz_ns" => 86_400_000_000_000,
        _ => return None,
    };
    (bound.len() == 4).then(|| metalake::bounds::long(i64::from(int32(bound).unwrap_or(0)) * per_day))
}

fn long(bound: &[u8]) -> Option<i64> {
    Some((u64::from_be_bytes(bound.try_into().ok()?) ^ (1 << 63)) as i64)
}

fn int32(bound: &[u8]) -> Option<i32> {
    Some((u32::from_be_bytes(bound.try_into().ok()?) ^ (1 << 31)) as i32)
}

fn decimal(bound: &[u8]) -> Option<i128> {
    Some((u128::from_be_bytes(bound.try_into().ok()?) ^ (1 << 127)) as i128)
}

/// The inverse of `totalOrder`'s encoding: a positive has the sign flipped, and a negative every bit.
fn double(bound: &[u8]) -> Option<f64> {
    let ordered = u64::from_be_bytes(bound.try_into().ok()?);
    let bits = if ordered >> 63 == 1 {
        ordered ^ (1 << 63)
    } else {
        !ordered
    };
    Some(f64::from_bits(bits))
}

/// The fewest bytes of two's complement, big-endian.
fn shortest(unscaled: i128) -> Vec<u8> {
    let bytes = unscaled.to_be_bytes();
    let redundant = |pair: &[u8]| {
        let fill = if unscaled < 0 { 0xff } else { 0 };
        pair[0] == fill && (pair[1] & 0x80 == fill & 0x80)
    };
    let skip = bytes.windows(2).take_while(|pair| redundant(pair)).count();
    bytes[skip..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ice3_decimal_minimal_encoding() {
        assert_eq!(shortest(0), [0]);
        assert_eq!(shortest(127), [127]);
        assert_eq!(shortest(128), [0, 128]);
        assert_eq!(shortest(-1), [0xff]);
        assert_eq!(shortest(-128), [0x80]);
        assert_eq!(shortest(-129), [0xff, 0x7f]);
    }

    #[test]
    fn ice3_unserializable_bound() {
        assert_eq!(to_iceberg("variant", &[1, 2]), None);
        assert_eq!(to_iceberg("long", &[1, 2]), None);
        let too_wide = metalake::bounds::long(i64::from(i32::MAX) + 1);
        assert_eq!(to_iceberg("int", &too_wide), None);
    }

    #[test]
    fn ice6_bound_little_endian_widths() {
        use metalake::bounds::long;
        assert_eq!(from_iceberg("long", &(-5_i32).to_le_bytes()), Some(long(-5)));
        assert_eq!(from_iceberg("long", &(-5_i64).to_le_bytes()), Some(long(-5)));
        assert_eq!(from_iceberg("long", &[1, 2]), None);
        assert_eq!(from_iceberg("date", &i64::MAX.to_le_bytes()), None);
    }

    #[test]
    fn ice6_bound_decimal_sign_extension() {
        use metalake::bounds::decimal;
        assert_eq!(from_iceberg("decimal(10,2)", &[0xff]), Some(decimal(-1)));
        assert_eq!(from_iceberg("decimal(10,2)", &[0, 0x80]), Some(decimal(128)));
        assert_eq!(from_iceberg("decimal(10,2)", &[]), None);
        assert_eq!(from_iceberg("decimal(10,2)", &[0; 17]), None);
    }

    #[test]
    fn sta6_nan_bound_dropped() {
        assert_eq!(from_iceberg("double", &f64::NAN.to_le_bytes()), None);
        assert_eq!(from_iceberg("float", &f32::NAN.to_le_bytes()), None);
    }

    #[test]
    fn sta9_unbounded_types_dropped() {
        assert_eq!(from_iceberg("variant", &[1]), None);
        assert_eq!(from_iceberg("geometry", &[0; 16]), None);
    }
}
