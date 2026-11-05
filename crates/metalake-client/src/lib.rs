//! What MetaLake's faces share (ADR 0003 §4). A face turns what it is given into calls and their answers into its own
//! form, and decides nothing: which lake is meant and what each verb does live here, once.

mod error;
mod history;
mod lake;
mod maintain;
mod merge;
mod refs;
mod session;
mod status;
mod write;

pub use error::{Error, Kind, Result};
pub use history::duration;
pub use lake::Lake;
pub use maintain::{Done, MaintainRequest, MaintainRun, Task};
pub use merge::{MergeOutcome, MergeRequest};
pub use metalake::{CommitEntry, Retention, SnapshotEntry};
pub use refs::{RefInfo, RefKind, table};
pub use session::{Prepared, Session};
pub use status::{Named, Status};
pub use write::{Details, TablePath, Written};
