//! What a lake says of itself, as objects with attributes: short lists of metadata, not data to analyse.

use metalake_client::{CommitEntry, MergeOutcome, RefInfo, SnapshotEntry, Status};
use pyo3::prelude::*;

/// A branch or a tag.
#[pyclass(name = "Ref", module = "metalake", frozen, get_all)]
pub struct PyRef {
    name: String,
    kind: String,
    parent: Option<String>,
    fork_seq: Option<i64>,
    created_seq: i64,
    protected: bool,
}

impl From<RefInfo> for PyRef {
    fn from(on: RefInfo) -> Self {
        Self {
            name: on.name,
            kind: on.kind,
            parent: on.parent,
            fork_seq: on.fork_seq,
            created_seq: on.created_seq,
            protected: on.protected,
        }
    }
}

#[pymethods]
impl PyRef {
    fn __repr__(&self) -> String {
        format!("Ref({:?}, kind={:?}, parent={:?})", self.name, self.kind, self.parent)
    }
}

/// A commit of a ref. Its sequence is what `"main@1234"` reads and a ref can start from.
#[pyclass(name = "Commit", module = "metalake", frozen, get_all)]
pub struct PyCommit {
    seq: i64,
    committed_at: Option<String>,
    author: Option<String>,
    operation: String,
    message: Option<String>,
}

impl From<CommitEntry> for PyCommit {
    fn from(commit: CommitEntry) -> Self {
        Self {
            seq: commit.commit_seq,
            committed_at: commit.committed_at,
            author: commit.author,
            operation: commit.operation,
            message: commit.message,
        }
    }
}

#[pymethods]
impl PyCommit {
    fn __repr__(&self) -> String {
        format!("Commit({}, {:?}, message={:?})", self.seq, self.operation, self.message)
    }
}

/// A snapshot of a table.
#[pyclass(name = "Snapshot", module = "metalake", frozen, get_all)]
pub struct PySnapshot {
    id: i64,
    parent: Option<i64>,
    operation: Option<String>,
    committed_at: Option<String>,
}

impl From<SnapshotEntry> for PySnapshot {
    fn from(snapshot: SnapshotEntry) -> Self {
        Self {
            id: snapshot.snapshot_id,
            parent: snapshot.parent_snapshot_id,
            operation: snapshot.operation,
            committed_at: snapshot.committed_at,
        }
    }
}

#[pymethods]
impl PySnapshot {
    fn __repr__(&self) -> String {
        format!("Snapshot({}, {:?})", self.id, self.operation)
    }
}

/// What a merge did, or, checked, what it would do. `outcome` is `merged`, `nothing` or `would merge`, and `merged` and
/// `rebased` name tables as `namespace.table`.
#[pyclass(name = "Merged", module = "metalake", frozen, get_all)]
pub struct PyMerged {
    branch: String,
    outcome: String,
    commit: Option<i64>,
    merged: Vec<String>,
    rebased: Vec<String>,
}

impl From<MergeOutcome> for PyMerged {
    fn from(merge: MergeOutcome) -> Self {
        Self {
            branch: merge.branch,
            outcome: merge.outcome.to_string(),
            commit: merge.commit,
            merged: merge.merged,
            rebased: merge.rebased,
        }
    }
}

#[pymethods]
impl PyMerged {
    fn __repr__(&self) -> String {
        format!(
            "Merged({:?}, {:?}, commit={:?})",
            self.branch, self.outcome, self.commit
        )
    }
}

/// What the history of a ref or of a table is made of.
#[derive(IntoPyObject)]
pub enum History {
    Commits(Vec<PyCommit>),
    Snapshots(Vec<PySnapshot>),
}

/// What a lake is. Printed, it reads as `metalake status` does.
#[pyclass(name = "Status", module = "metalake", frozen)]
pub struct PyStatus {
    #[pyo3(get)]
    postgres: Option<String>,
    #[pyo3(get)]
    local: Option<String>,
    #[pyo3(get)]
    schema: String,
    #[pyo3(get)]
    warehouse: Option<String>,
    #[pyo3(get)]
    format_version: String,
    #[pyo3(get)]
    writer_version: String,
    #[pyo3(get)]
    protocol_version: Option<String>,
    #[pyo3(get)]
    refs: Vec<String>,
    text: String,
}

impl From<Status> for PyStatus {
    fn from(status: Status) -> Self {
        let text = status.to_string();
        Self {
            postgres: status.postgres,
            local: status.local,
            schema: status.schema,
            warehouse: status.warehouse,
            format_version: status.format_version,
            writer_version: status.writer_version,
            protocol_version: status.protocol_version,
            refs: status.refs.into_iter().map(|r| r.name).collect(),
            text,
        }
    }
}

#[pymethods]
impl PyStatus {
    fn __str__(&self) -> &str {
        &self.text
    }

    fn __repr__(&self) -> String {
        format!("Status(schema={:?}, refs={:?})", self.schema, self.refs)
    }
}
