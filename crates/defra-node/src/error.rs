//! The node crate's error type.
//!
//! One enum for the embedded node, its trait surfaces and the benchmark
//! helpers. Library errors convert with `?`; ad-hoc failures carry their
//! message through [`err!`](crate::err) and [`bail!`](crate::bail).

use std::fmt::Display;

/// Failure of an embedded node operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A failure described by a plain sentence.
    #[error("{0}")]
    Message(String),
    /// The database rejected the operation.
    #[error(transparent)]
    Db(#[from] db::Error),
    /// A DID or identity key was rejected.
    #[error(transparent)]
    Identity(#[from] identity::Error),
    /// An HTTP request failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// The P2P transport failed.
    #[cfg(feature = "p2p")]
    #[error(transparent)]
    P2p(#[from] p2p::Error),
    /// The HTTP server failed.
    #[cfg(feature = "http")]
    #[error(transparent)]
    HttpServer(#[from] defra_http::HttpError),
    /// A file or directory operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A JSON document could not be read or written.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Result alias for node operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// An error carrying `message`.
    pub fn msg(message: impl Display) -> Self {
        Self::Message(message.to_string())
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self::Message(message.to_owned())
    }
}

/// Builds an [`Error::Message`] from a format string.
#[doc(hidden)]
#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {
        $crate::Error::Message(::std::format!($($arg)*))
    };
}

/// Returns early with an [`Error::Message`] built from a format string.
#[doc(hidden)]
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return ::std::result::Result::Err($crate::err!($($arg)*))
    };
}

/// Prefixes a failure, or a missing value, with what the caller was doing.
pub trait Context<T> {
    /// Fails with `context` as the message when `self` is an error or `None`.
    fn context(self, context: impl Display) -> Result<T>;

    /// Like [`Context::context`], building the message only on failure.
    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display,
        F: FnOnce() -> C;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: Display,
{
    fn context(self, context: impl Display) -> Result<T> {
        self.map_err(|error| err!("{context}: {error}"))
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display,
        F: FnOnce() -> C,
    {
        self.map_err(|error| err!("{}: {error}", context()))
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, context: impl Display) -> Result<T> {
        self.ok_or_else(|| err!("{context}"))
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: Display,
        F: FnOnce() -> C,
    {
        self.ok_or_else(|| err!("{}", context()))
    }
}
