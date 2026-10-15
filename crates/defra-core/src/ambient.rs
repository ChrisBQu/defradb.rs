//! The request state tests set the old way, through thread-locals and
//! task-locals, and the one place it turns back into a [`RequestContext`].
//!
//! Production code never reads or writes this state: a boundary builds a
//! [`RequestContext`] and hands it down. Tests written against the
//! thread-local API still set it, and [`RequestContext::ambient`] reads it
//! wherever a call arrives without a context of its own. It is compiled only
//! for tests: this crate's, and those of crates that enable the `ambient`
//! feature from their dev-dependencies.

use std::cell::RefCell;
use std::future::Future;

use crate::encryption::EncryptionConfig;
use crate::signing::SigningConfig;
use crate::RequestContext;

thread_local! {
    static DAC_BYPASS: RefCell<bool> = const { RefCell::new(false) };
    static ENCRYPTION: RefCell<Option<EncryptionConfig>> = const { RefCell::new(None) };
    static SIGNING: RefCell<Option<SigningConfig>> = const { RefCell::new(None) };
    static BATCH_SESSION: RefCell<Option<String>> = const { RefCell::new(None) };
    static CREATOR: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[cfg(not(target_arch = "wasm32"))]
tokio::task_local! {
    static TASK_CREATOR: Option<String>;
}

/// The context a test set up on this thread and task.
pub(crate) fn context() -> RequestContext {
    RequestContext::default()
        .with_identity(crate::current_identity::get_effective_identity())
        .with_dac_bypass(get_dac_bypass())
        .with_encryption(get_encryption_config())
        .with_signing(get_signing_config())
        .with_batch_session(get_batch_session_key())
        .with_creator(get_broadcast_creator_did())
}

/// Set the DAC bypass flag for the current thread.
pub fn set_dac_bypass(bypass: bool) {
    DAC_BYPASS.with(|c| *c.borrow_mut() = bypass);
}

/// Get the current DAC bypass flag for this thread.
pub fn get_dac_bypass() -> bool {
    DAC_BYPASS.with(|c| *c.borrow())
}

/// Set the encryption config for the current thread.
pub fn set_encryption_config(config: Option<EncryptionConfig>) {
    ENCRYPTION.with(|c| *c.borrow_mut() = config);
}

/// Get the current encryption config for this thread.
pub fn get_encryption_config() -> Option<EncryptionConfig> {
    ENCRYPTION.with(|c| c.borrow().clone())
}

/// Set the signing config for the current thread.
pub fn set_signing_config(config: Option<SigningConfig>) {
    SIGNING.with(|c| *c.borrow_mut() = config);
}

/// Get the current signing config for this thread.
pub fn get_signing_config() -> Option<SigningConfig> {
    SIGNING.with(|c| c.borrow().clone())
}

/// Set the batch session key for the current thread.
pub fn set_batch_session_key(key: Option<String>) {
    BATCH_SESSION.with(|k| *k.borrow_mut() = key);
}

/// Get the batch session key for the current thread.
pub fn get_batch_session_key() -> Option<String> {
    BATCH_SESSION.with(|k| k.borrow().clone())
}

/// Scope the broadcast creator DID to one async task.
#[cfg(not(target_arch = "wasm32"))]
pub async fn scope_broadcast_creator_did<F>(did: Option<String>, future: F) -> F::Output
where
    F: Future,
{
    TASK_CREATOR.scope(did, future).await
}

/// Run an async task without a task-scoped creator on WASM.
#[cfg(target_arch = "wasm32")]
pub async fn scope_broadcast_creator_did<F>(_did: Option<String>, future: F) -> F::Output
where
    F: Future,
{
    future.await
}

/// Set the broadcast creator DID for the current thread.
pub fn set_broadcast_creator_did(did: Option<String>) {
    CREATOR.with(|c| *c.borrow_mut() = did);
}

/// Get the broadcast creator DID: the task's scope when one is active, the
/// thread's otherwise.
#[cfg(not(target_arch = "wasm32"))]
pub fn get_broadcast_creator_did() -> Option<String> {
    TASK_CREATOR
        .try_with(Clone::clone)
        .unwrap_or_else(|_| CREATOR.with(|c| c.borrow().clone()))
}

/// Get the broadcast creator DID set for this thread.
#[cfg(target_arch = "wasm32")]
pub fn get_broadcast_creator_did() -> Option<String> {
    CREATOR.with(|c| c.borrow().clone())
}
