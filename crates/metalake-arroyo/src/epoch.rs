//! What one subtask of a sink wrote in an epoch: the files it prepared, which its checkpoint keeps as pre-commit data
//! and the commit adds once the checkpoint is complete (CMT-10), and the rows of them it deleted, which the same
//! commit deletes (DV-4).

use metalake::{FileAdd, RowLocation};
use serde::{Deserialize, Serialize};

/// The version of an epoch as it is stored. Version 1, files alone, is still read; a commit refuses any other.
const VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SinkEpoch {
    version: u32,
    pub files: Vec<FileAdd>,
    /// Rows of `files`, by file id and position, deleted before the commit.
    #[serde(default)]
    deleted: Vec<(i64, u64)>,
}

impl SinkEpoch {
    pub fn new(files: Vec<FileAdd>, deleted: &[RowLocation]) -> Self {
        Self {
            version: VERSION,
            files,
            deleted: deleted.iter().map(|at| (at.file_id, at.position)).collect(),
        }
    }

    pub fn deleted(&self) -> impl Iterator<Item = RowLocation> + '_ {
        self.deleted.iter().map(|(file_id, position)| RowLocation {
            file_id: *file_id,
            position: *position,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an epoch is plain data")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let epoch: Self = serde_json::from_slice(bytes).map_err(|e| format!("unreadable: {e}"))?;
        if !(1..=VERSION).contains(&epoch.version) {
            return Err(format!(
                "version {}, and this connector reads 1 to {VERSION}",
                epoch.version
            ));
        }
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn file(path: &str) -> FileAdd {
        FileAdd {
            table_id: "7b1f0c4e-1d2a-4e44-9a51-3c0f6c1b2d3e".parse().unwrap(),
            file_id: None,
            path: Some(path.to_string()),
            spec_id: 0,
            partition: json!([]),
            sort_order_id: None,
            record_count: 3,
            file_size: 900,
            footer_size: 120,
            first_row_id: None,
            stat_field_ids: vec![1],
            lower: vec![Some(vec![0, 0, 0, 0, 0, 0, 0, 1])],
            upper: vec![Some(vec![0, 0, 0, 0, 0, 0, 0, 3])],
            null_count: vec![Some(0)],
            nan_count: vec![None],
            value_count: Some(vec![Some(3)]),
            content: None,
            footer: Some(vec![1, 2, 3]),
        }
    }

    #[test]
    fn sink_epoch_round_trips() {
        let deleted = [RowLocation {
            file_id: 3,
            position: 9,
        }];
        for epoch in [
            SinkEpoch::new(vec![], &[]),
            SinkEpoch::new(vec![file("file:///t/a.parquet"), file("file:///t/b.parquet")], &deleted),
        ] {
            assert_eq!(SinkEpoch::from_bytes(&epoch.to_bytes()), Ok(epoch));
        }
    }

    #[test]
    fn sink_epoch_unknown_version_refused() {
        let mut stored: serde_json::Value = serde_json::from_slice(&SinkEpoch::new(vec![], &[]).to_bytes()).unwrap();
        stored["version"] = json!(3);
        let error = SinkEpoch::from_bytes(&serde_json::to_vec(&stored).unwrap()).unwrap_err();
        assert!(error.contains("version 3"), "{error}");
    }

    #[test]
    fn sink_epoch_version_1_maps() {
        let stored = json!({"version": 1, "files": [file("file:///t/a.parquet")]});
        let epoch = SinkEpoch::from_bytes(&serde_json::to_vec(&stored).unwrap()).unwrap();
        assert_eq!((epoch.files.len(), epoch.deleted().count()), (1, 0));
    }
}
