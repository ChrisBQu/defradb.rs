//! The embedded crate's error type: a node build or an operation failed, with
//! the reason as one plain sentence that already names the failing step and
//! its cause.

use std::fmt::Display;

/// Why an embedded node could not be built or an operation on it failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A failure described by its message.
    #[error("{0}")]
    Message(String),

    /// The node's store failed.
    #[error("{0}")]
    Storage(#[from] storage::corekv::Error),
}

/// The result of an embedded node operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// An error with `message` as its text.
    pub fn msg(message: impl Display) -> Self {
        Self::Message(message.to_string())
    }

    /// An error reading `"{step}: {cause}"`.
    pub fn because(step: impl Display, cause: impl Display) -> Self {
        Self::Message(format!("{step}: {cause}"))
    }
}

/// Names the failing step of a result: the error reads `"{step}: {cause}"`.
pub(crate) trait ResultExt<T> {
    fn context(self, step: &str) -> Result<T>;

    fn with_context<F>(self, step: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Display,
{
    fn context(self, step: &str) -> Result<T> {
        self.map_err(|error| Error::because(step, error))
    }

    fn with_context<F>(self, step: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|error| Error::because(step(), error))
    }
}
