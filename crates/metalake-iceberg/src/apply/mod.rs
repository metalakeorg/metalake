//! What a client asks to change, turned into what the protocol commits. Nothing here knows HTTP.

pub mod commit;
pub mod create;
pub mod ddl;
pub mod files;
pub mod update;

/// Why a request is not committed. Neither has changed anything (REST-2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The request asks what the service does not do: it would fail the same way again.
    Invalid(String),
    /// The table is not as the client saw it: it may read the table again and rebuild its commit.
    Conflict(String),
    /// A write on a tag, which no commit moves (I6).
    ReadOnly(String),
}

/// That `on` takes commits: a branch does, a tag does not.
pub fn writable(on: &metalake::Ref) -> Result<(), Refused> {
    match on.kind() {
        "tag" => Err(Refused::ReadOnly(format!(
            "{} is a tag, which no commit moves: connect to a branch to write",
            on.name()
        ))),
        _ => Ok(()),
    }
}

/// A failure of applying a request.
#[derive(Debug)]
pub enum ApplyError {
    Refused(Refused),
    /// The storage or the catalog failed. The commit was not attempted, or its outcome is the protocol's to say.
    Core(metalake::Error),
}

impl From<Refused> for ApplyError {
    fn from(refused: Refused) -> Self {
        Self::Refused(refused)
    }
}

impl From<metalake::Error> for ApplyError {
    fn from(error: metalake::Error) -> Self {
        Self::Core(error)
    }
}
