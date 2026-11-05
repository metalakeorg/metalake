//! What MetaLake's faces share (ADR 0003 §4). A face turns what it is given into calls and their answers into its own
//! form, and decides nothing: which lake is meant and what each verb does live here, once.

mod error;
mod lake;
mod maintain;
mod merge;
mod refs;
mod session;

pub use error::{Error, Kind, Result};
pub use lake::Lake;
pub use maintain::{Done, MaintainRequest, MaintainRun, Task};
pub use merge::{MergeOutcome, MergeRequest};
pub use refs::{RefInfo, RefKind, table};
pub use session::{Prepared, Session};
