//! The engine-owned retry, as the key-value layer sees it: an attempt runs
//! in a transaction its store owns and commits, again after each conflict,
//! up to one policy's budget ([`policy`]). An attempt sees a restricted view
//! ([`RetryTxn`]), returns a value or a typed error ([`RetryAttemptError`]),
//! and can be wrapped to see every write it stages ([`intercept_retry_writes`]).

use std::any::Any;
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;

use super::callbacks::RetryCallbacks;
use super::errors::Result;
use super::traits::{private, MaybeSendSync, ReaderWriter};

mod intercept;
pub mod policy;

pub use intercept::{intercept_retry_writes, RetryWriteInterceptor};

/// A retry attempt's restricted transaction view. It deliberately has no
/// commit, discard, iterator, or callback methods: the store owns the attempt
/// lifetime and commit, and the view cannot escape through this API.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RetryTxn: MaybeSendSync + private::Sealed {
    /// Why the previous commit lost, without exposing user key bytes.
    fn previous_conflict(&self) -> Option<super::errors::TransactionConflict>;

    /// A plain validated read, as `Reader::get`: checked at commit when the
    /// attempt writes, so an identical rewrite of the key is not a conflict
    /// and an attempt that ends up writing nothing commits at its snapshot.
    /// Projected and unvalidated reads go through `begin_attempt_io`.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    /// Whether `key` holds a value, read as `get` reads it.
    async fn has(&self, key: &[u8]) -> Result<bool>;
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Open a sealed, attempt-scoped Reader/Writer capability for datastore
    /// facades. It has no transaction lifecycle methods and cannot commit.
    #[doc(hidden)]
    fn begin_attempt_io(&mut self) -> Result<Box<dyn RetryAttemptIO>> {
        Err(super::errors::Error::Other(
            "backend does not expose scoped retry IO".into(),
        ))
    }

    /// Return the attempt capability after all namespace views and iterators
    /// have been dropped. Backends reject retained references before commit.
    #[doc(hidden)]
    fn finish_attempt_io(
        &mut self,
        _io: Box<dyn RetryAttemptIO>,
        _callbacks: RetryCallbacks,
    ) -> Result<()> {
        Err(super::errors::Error::Other(
            "backend does not accept scoped retry IO".into(),
        ))
    }
}

/// Engine-owned Reader/Writer access scoped to one retry attempt. This
/// intentionally omits `Txn`, callbacks, commit, and discard.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RetryAttemptIO: ReaderWriter + private::Sealed {
    #[doc(hidden)]
    fn as_any(&self) -> &dyn Any;

    #[doc(hidden)]
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttemptValue = Box<dyn Any + Send>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttemptValue = Box<dyn Any>;

/// Result of an engine-owned retry operation. Storage failures remain typed;
/// domain aborts cross the engine boundary untouched and are never retried.
pub enum RetryAttemptError {
    Storage(super::errors::Error),
    Exhausted(super::errors::TransactionConflict),
    #[cfg(not(target_arch = "wasm32"))]
    Abort(Box<dyn Any + Send>),
    #[cfg(target_arch = "wasm32")]
    Abort(Box<dyn Any>),
}

impl std::fmt::Debug for RetryAttemptError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::Exhausted(conflict) => {
                formatter.debug_tuple("Exhausted").field(conflict).finish()
            }
            Self::Abort(_) => formatter.write_str("Abort(<typed domain error>)"),
        }
    }
}

impl From<super::errors::Error> for RetryAttemptError {
    fn from(error: super::errors::Error) -> Self {
        Self::Storage(error)
    }
}

impl RetryAttemptError {
    pub fn into_storage(self) -> super::errors::Error {
        match self {
            Self::Storage(error) => error,
            Self::Exhausted(conflict) => super::errors::Error::TxnRetryExhausted(conflict),
            Self::Abort(_) => super::errors::Error::Other(
                "transaction attempt aborted with a domain error".to_string(),
            ),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn downcast_abort<T: Any + Send>(self) -> std::result::Result<T, Self> {
        match self {
            Self::Abort(error) => match error.downcast::<T>() {
                Ok(value) => Ok(*value),
                Err(error) => Err(Self::Abort(error)),
            },
            storage => Err(storage),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn downcast_abort<T: Any>(self) -> std::result::Result<T, Self> {
        match self {
            Self::Abort(error) => match error.downcast::<T>() {
                Ok(value) => Ok(*value),
                Err(error) => Err(Self::Abort(error)),
            },
            storage => Err(storage),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttemptFuture<'a> = Pin<
    Box<dyn Future<Output = std::result::Result<RetryAttemptValue, RetryAttemptError>> + Send + 'a>,
>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttemptFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<RetryAttemptValue, RetryAttemptError>> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
pub trait RetryAttemptFn: Send {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>;
}
#[cfg(target_arch = "wasm32")]
pub trait RetryAttemptFn {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>;
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttempt = Box<dyn RetryAttemptFn>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttempt = Box<dyn RetryAttemptFn>;

#[cfg(not(target_arch = "wasm32"))]
impl<F> RetryAttemptFn for F
where
    F: Send + for<'a> FnMut(&'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>,
{
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        self(txn)
    }
}

#[cfg(target_arch = "wasm32")]
impl<F> RetryAttemptFn for F
where
    F: for<'a> FnMut(&'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>,
{
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        self(txn)
    }
}

/// Number of total transaction attempts, including the first run.
#[derive(Clone, Copy, Debug)]
pub struct RetryOptions {
    pub max_attempts: u32,
}

impl RetryOptions {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }
}
