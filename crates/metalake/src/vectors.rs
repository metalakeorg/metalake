//! Deletion vectors (DV-1, DV-2): the positions of a file's deleted rows, as Iceberg v3 stores them. A vector is a
//! `deletion-vector-v1` blob, the same bytes inline in the catalog or at an offset of a Puffin file: its length
//! (big-endian), the magic `D1 D3 39 64`, a 64-bit Roaring bitmap in its portable serialization, and a CRC-32 of the
//! magic and the bitmap (big-endian). A reader skips the positions as a Parquet `RowSelection`.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use roaring::RoaringTreemap;

use crate::error::{Error, Result};

const MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// The column of a scan read `with_positions` that holds each row's file.
pub const FILE_ID: &str = "_file_id";
/// The column that holds each row's position in its file, from 0.
pub const POSITION: &str = "_pos";

/// The deleted positions of one data file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeletionVector {
    positions: RoaringTreemap,
}

impl DeletionVector {
    pub fn from_positions(positions: impl IntoIterator<Item = u64>) -> Self {
        Self {
            positions: positions.into_iter().collect(),
        }
    }

    /// The vector a `deletion-vector-v1` blob holds, after checking its length, magic and checksum.
    pub fn from_blob(blob: &[u8]) -> Result<Self> {
        let corrupt = |what: &str| Error::Corrupt(format!("a deletion vector {what}"));
        if blob.len() < 20 {
            return Err(corrupt("is too short"));
        }
        let length = u32::from_be_bytes(blob[..4].try_into().expect("four bytes")) as usize;
        if length != blob.len() - 8 {
            return Err(corrupt("has a length that does not match"));
        }
        let body = &blob[4..4 + length];
        if body[..4] != MAGIC {
            return Err(corrupt("lacks its magic"));
        }
        let checksum = u32::from_be_bytes(blob[4 + length..].try_into().expect("four bytes"));
        if crc32fast::hash(body) != checksum {
            return Err(corrupt("has a checksum that does not match"));
        }
        let mut bitmap = &body[4..];
        let positions = RoaringTreemap::deserialize_from(&mut bitmap)
            .map_err(|e| corrupt(&format!("holds no Roaring bitmap: {e}")))?;
        if !bitmap.is_empty() {
            return Err(corrupt("has bytes after its bitmap"));
        }
        if positions.max().is_some_and(|p| p >= 1 << 63) {
            return Err(corrupt("holds a position of 2^63 or more"));
        }
        Ok(Self { positions })
    }

    /// The vector as a `deletion-vector-v1` blob.
    pub fn to_blob(&self) -> Vec<u8> {
        let mut body = MAGIC.to_vec();
        self.positions.serialize_into(&mut body).expect("writing to memory");
        let mut blob = Vec::with_capacity(body.len() + 8);
        blob.extend_from_slice(&(body.len() as u32).to_be_bytes());
        blob.extend_from_slice(&body);
        blob.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        blob
    }

    pub fn insert(&mut self, position: u64) {
        self.positions.insert(position);
    }

    /// The positions either deletes: the vector of a file that two writers, or a branch and its parent, both
    /// deleted from (DV-4, MRG-5).
    pub fn union(&self, other: &Self) -> Self {
        Self {
            positions: &self.positions | &other.positions,
        }
    }

    /// The greatest position it deletes.
    pub fn max(&self) -> Option<u64> {
        self.positions.max()
    }

    /// How many rows it deletes.
    pub fn cardinality(&self) -> u64 {
        self.positions.len()
    }

    pub fn contains(&self, position: u64) -> bool {
        self.positions.contains(position)
    }

    /// The deleted positions, ascending.
    pub fn positions(&self) -> impl Iterator<Item = u64> + '_ {
        self.positions.iter()
    }

    /// The rows a reader keeps of the row groups it reads, each `(first row, rows)` in the file and in ascending
    /// order: a selection over those groups' rows, one after the other, as Parquet applies it.
    pub fn row_selection(&self, groups: &[(u64, u64)]) -> RowSelection {
        let mut selectors: Vec<RowSelector> = Vec::new();
        let mut push = |s: RowSelector| match selectors.last_mut() {
            Some(last) if last.skip == s.skip => last.row_count += s.row_count,
            _ => selectors.push(s),
        };
        let mut deleted = self.positions.iter().peekable();
        for &(start, rows) in groups {
            let end = start + rows;
            let mut at = start;
            while let Some(&p) = deleted.peek() {
                if p >= end {
                    break;
                }
                deleted.next();
                if p < start {
                    continue;
                }
                if p > at {
                    push(RowSelector::select((p - at) as usize));
                }
                push(RowSelector::skip(1));
                at = p + 1;
            }
            if end > at {
                push(RowSelector::select((end - at) as usize));
            }
        }
        selectors.into()
    }
}

/// Rows to delete, by file: what an engine gathers from the rows of a scan read `with_positions` that it chose.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Deletes {
    files: BTreeMap<i64, DeletionVector>,
}

impl Deletes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deletes the row at `position` of file `file_id`.
    pub fn insert(&mut self, file_id: i64, position: u64) {
        self.files.entry(file_id).or_default().insert(position);
    }

    /// Deletes every row of a batch read `with_positions`, by its `_file_id` and `_pos`.
    pub fn add(&mut self, batch: &RecordBatch) -> Result<()> {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_primitive_opt::<Int64Type>())
                .ok_or_else(|| Error::Schema(format!("the batch has no {name}: read it with_positions")))
        };
        let (files, positions) = (column(FILE_ID)?, column(POSITION)?);
        for (file, position) in files.values().iter().zip(positions.values()) {
            let position = u64::try_from(*position).map_err(|_| Error::Schema(format!("a position {position}")))?;
            self.insert(*file, position);
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The files and what to delete of each.
    pub fn files(&self) -> &BTreeMap<i64, DeletionVector> {
        &self.files
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blobs_come_back_the_same_and_damage_is_refused() {
        for positions in [vec![], vec![0], vec![5, 70_000, (1 << 32) + 3, 1 << 40]] {
            let v = DeletionVector::from_positions(positions.clone());
            let back = DeletionVector::from_blob(&v.to_blob()).unwrap();
            assert_eq!(back.positions().collect::<Vec<_>>(), positions);
        }
        let blob = DeletionVector::from_positions([1, 2, 3]).to_blob();
        let damaged = |at: usize| {
            let mut b = blob.clone();
            b[at] ^= 1;
            DeletionVector::from_blob(&b)
        };
        for at in [0, 5, blob.len() - 1, 16] {
            assert!(matches!(damaged(at), Err(Error::Corrupt(_))), "byte {at}");
        }
        assert!(matches!(DeletionVector::from_blob(&blob[..19]), Err(Error::Corrupt(_))));
        let mut longer = blob.clone();
        longer.insert(10, 0);
        assert!(matches!(DeletionVector::from_blob(&longer), Err(Error::Corrupt(_))));
        // Each check on its own, the checksum right: another magic, and bytes after the bitmap.
        let framed = |body: &[u8]| {
            let mut b = (body.len() as u32).to_be_bytes().to_vec();
            b.extend_from_slice(body);
            b.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
            b
        };
        let mut body = blob[4..blob.len() - 4].to_vec();
        body[0] = 0;
        assert!(matches!(DeletionVector::from_blob(&framed(&body)), Err(Error::Corrupt(m)) if m.contains("magic")));
        let mut body = blob[4..blob.len() - 4].to_vec();
        body.push(0);
        assert!(matches!(DeletionVector::from_blob(&framed(&body)), Err(Error::Corrupt(m)) if m.contains("after")));
        let far = DeletionVector::from_positions([1 << 63]).to_blob();
        assert!(matches!(DeletionVector::from_blob(&far), Err(Error::Corrupt(m)) if m.contains("2^63")));
    }

    #[test]
    fn a_selection_skips_the_deleted_rows_of_the_groups_read() {
        let v = DeletionVector::from_positions([0, 1, 4, 12, 13, 25, 29]);
        let runs = |groups: &[(u64, u64)]| -> Vec<(bool, usize)> {
            let s: Vec<RowSelector> = v.row_selection(groups).into();
            s.into_iter().map(|r| (r.skip, r.row_count)).collect()
        };
        // One group of 10 rows: 0 and 1 skipped, 2–3 kept, 4 skipped, 5–9 kept.
        assert_eq!(runs(&[(0, 10)]), [(true, 2), (false, 2), (true, 1), (false, 5)]);
        // Groups 1 and 2: 12 and 13 in the first, 25 and 29 in the second; 0, 1 and 4 are in group 0, not read.
        assert_eq!(
            runs(&[(10, 10), (20, 10)]),
            [(false, 2), (true, 2), (false, 11), (true, 1), (false, 3), (true, 1)]
        );
        assert_eq!(runs(&[(30, 5)]), [(false, 5)]);
        assert_eq!(DeletionVector::default().row_selection(&[(0, 3)]).row_count(), 3);
    }
}
