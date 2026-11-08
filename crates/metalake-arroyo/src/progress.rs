//! Where one subtask of a source is, apart from any I/O: the cursor past the last step it took, its tasks of that step
//! not yet emitted, and how many rows of the first one it emitted when a checkpoint came in its middle. The source
//! keeps it in Arroyo's state at each checkpoint and goes on from it after a restore.

use std::collections::VecDeque;
use std::ops::Range;

use metalake::{StreamCursor, StreamStep, StreamTask};
use serde::{Deserialize, Serialize};

use crate::split::reads;

/// The version of the progress as it is stored; a restore refuses any other.
const VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceProgress {
    version: u32,
    /// Past the last step taken; none until the first, whose start is the source's time (CHG-5).
    cursor: Option<StreamCursor>,
    /// This subtask's tasks of the steps taken, in commit order, not yet emitted whole.
    pending: VecDeque<StreamTask>,
    /// Rows of the first pending task already emitted.
    emitted: u64,
}

impl Default for SourceProgress {
    fn default() -> Self {
        Self {
            version: VERSION,
            cursor: None,
            pending: VecDeque::new(),
            emitted: 0,
        }
    }
}

impl SourceProgress {
    pub fn cursor(&self) -> Option<&StreamCursor> {
        self.cursor.as_ref()
    }

    /// Takes a step: this subtask's tasks of it, after those still pending, and its cursor.
    pub fn take(&mut self, step: StreamStep, index: usize, parallelism: usize) {
        let ours = step
            .tasks
            .into_iter()
            .filter(|t| reads(t.file.file_id, index, parallelism));
        self.pending.extend(ours);
        self.cursor = Some(step.cursor);
    }

    /// The task to read now, and how many of its rows were emitted already.
    pub fn current(&self) -> Option<(&StreamTask, u64)> {
        self.pending.front().map(|t| (t, self.emitted))
    }

    /// `rows` more of the current task were emitted.
    pub fn emitted(&mut self, rows: u64) {
        self.emitted += rows;
    }

    /// The current task was emitted whole.
    pub fn finished(&mut self) {
        self.pending.pop_front();
        self.emitted = 0;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("progress is plain data")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let progress: Self = serde_json::from_slice(bytes).map_err(|e| format!("unreadable: {e}"))?;
        if progress.version != VERSION {
            return Err(format!(
                "version {}, and this connector reads {VERSION}",
                progress.version
            ));
        }
        Ok(progress)
    }
}

/// The rows of a batch to emit: those past what was `emitted` of its task, when the batch begins `read` rows into it.
/// A task reopened after a restore reads its rows again, in the same order, since its file and vectors are immutable.
pub fn unseen(read: u64, rows: usize, emitted: u64) -> Range<usize> {
    let skip = emitted.saturating_sub(read).min(rows as u64) as usize;
    skip..rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use metalake::{RowChange, ScanTask, TaskRows};
    use serde_json::json;
    use std::time::{Duration, UNIX_EPOCH};

    fn cursor(seq: i64) -> StreamCursor {
        serde_json::from_value(json!({
            "at": {"secs_since_epoch": 1_700_000_000 + seq, "nanos_since_epoch": 0},
            "seq": seq,
            "floor": 1,
        }))
        .unwrap()
    }

    fn task(file_id: i64, seq: i64) -> StreamTask {
        StreamTask {
            seq,
            committed_at: UNIX_EPOCH + Duration::from_secs(1_700_000_000 + seq as u64),
            change: RowChange::Insert,
            rows: TaskRows::Live,
            file: ScanTask {
                file_id,
                path: Some(format!("memory:///t/{file_id}.parquet")),
                content: None,
                file_size: 1000,
                footer_size: 100,
                footer: None,
                record_count: 10,
                spec_id: 0,
                partition: json!([]),
                vector: None,
            },
        }
    }

    fn step(files: &[i64], seq: i64) -> StreamStep {
        StreamStep {
            tasks: files.iter().map(|f| task(*f, seq)).collect(),
            cursor: cursor(seq),
        }
    }

    fn files(progress: &SourceProgress) -> Vec<i64> {
        progress.pending.iter().map(|t| t.file.file_id).collect()
    }

    #[test]
    fn take_splits_step_across_subtasks() {
        let files_of = |index| {
            let mut progress = SourceProgress::default();
            progress.take(step(&(1..=20).collect::<Vec<_>>(), 5), index, 3);
            assert_eq!(progress.cursor(), Some(&cursor(5)));
            files(&progress)
        };
        let all: Vec<Vec<i64>> = (0..3).map(files_of).collect();
        let mut together: Vec<i64> = all.concat();
        together.sort_unstable();
        assert_eq!(together, (1..=20).collect::<Vec<_>>(), "each file once, among them all");
        assert!(all.iter().all(|f| f.is_sorted()), "each in the step's order");
    }

    #[test]
    fn pending_tasks_in_commit_order() {
        let mut progress = SourceProgress::default();
        progress.take(step(&[1, 2], 5), 0, 1);
        progress.take(step(&[3], 6), 0, 1);
        assert_eq!(files(&progress), [1, 2, 3], "a later step after what is pending");
        assert_eq!(progress.cursor(), Some(&cursor(6)));

        assert_eq!(progress.current().map(|(t, e)| (t.file.file_id, e)), Some((1, 0)));
        progress.emitted(4);
        progress.emitted(3);
        assert_eq!(progress.current().map(|(t, e)| (t.file.file_id, e)), Some((1, 7)));
        progress.finished();
        assert_eq!(progress.current().map(|(t, e)| (t.file.file_id, e)), Some((2, 0)));
        progress.finished();
        progress.finished();
        assert_eq!(progress.current(), None);
    }

    #[test]
    fn progress_round_trips() {
        let mut progress = SourceProgress::default();
        assert_eq!(
            SourceProgress::from_bytes(&progress.to_bytes()),
            Ok(progress.clone()),
            "before any step"
        );
        progress.take(step(&[1, 2, 3], 9), 0, 1);
        progress.emitted(6);
        let restored = SourceProgress::from_bytes(&progress.to_bytes()).unwrap();
        assert_eq!(restored, progress);
        assert_eq!(restored.current().map(|(t, e)| (t.file.file_id, e)), Some((1, 6)));
    }

    #[test]
    fn progress_unknown_version_refused() {
        let mut stored: serde_json::Value = serde_json::from_slice(&SourceProgress::default().to_bytes()).unwrap();
        stored["version"] = json!(2);
        let error = SourceProgress::from_bytes(&serde_json::to_vec(&stored).unwrap()).unwrap_err();
        assert!(error.contains("version 2"), "{error}");
        assert!(SourceProgress::from_bytes(b"not json").is_err());
    }

    #[test]
    fn reopened_task_skips_emitted_rows() {
        // a task of 25 rows in batches of 10, 7 of which were emitted before a restore
        let batches = [10, 10, 5];
        let mut read = 0;
        let mut emitted = Vec::new();
        for rows in batches {
            emitted.push(unseen(read, rows, 7));
            read += rows as u64;
        }
        assert_eq!(emitted, [7..10, 0..10, 0..5]);

        assert_eq!(unseen(0, 10, 0), 0..10, "nothing emitted yet");
        assert_eq!(unseen(0, 10, 10), 10..10, "a whole batch emitted");
        assert_eq!(unseen(10, 5, 10), 0..5, "the batch after it");
        assert_eq!(unseen(0, 10, 25), 10..10, "all of it emitted");
        assert_eq!(unseen(20, 5, 25), 5..5);
    }
}
