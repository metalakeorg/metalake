//! A hash for names that must be the same on every machine and in every build: of a pipeline's container, of a lake's
//! server files. The standard library's own is not, by its documentation.

/// FNV-1a, 64 bits.
pub(crate) fn fnv(text: &str) -> u64 {
    text.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ byte as u64).wrapping_mul(0x100000001b3)
    })
}
