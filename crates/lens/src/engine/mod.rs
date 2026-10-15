//! The lens engine: the one WASM runtime a database runs its lens transforms
//! on, and the store a database gets when no runtime is built in.
//!
//! A database takes its store from [`default_store`]. With the
//! `wasmtime-runtime` feature that store runs on one `LensEngine`: a wasmtime
//! engine, the sandbox its calls run under, and the epoch clock that
//! interrupts a call past its deadline. Every store derived from it, a
//! transaction's registrations included, shares that engine and that clock: a
//! transaction never builds an engine or a clock thread of its own. The clock
//! stops with the last clone of the engine, and a clock thread that panicked is
//! reported then. Every call's wasmtime store is made here too
//! (`LensEngine::store`), so the limits a call runs under always match the
//! features the engine was built with.
//!
//! Without the `wasmtime-runtime` feature there is no engine: the default store
//! is [`UnsupportedTransformStore`], which refuses registration and execution
//! with an error that names the missing feature.

#[cfg(feature = "wasmtime-runtime")]
mod clock;
#[cfg(feature = "wasmtime-runtime")]
mod sandbox;
#[cfg(feature = "wasmtime-runtime")]
mod shared;
#[cfg(all(test, feature = "wasmtime-runtime"))]
mod tests;
mod unsupported;

use std::sync::Arc;

#[cfg(feature = "wasmtime-runtime")]
pub use sandbox::WasmSandboxConfig;
#[cfg(feature = "wasmtime-runtime")]
pub(crate) use shared::{LensEngine, Sandboxed};
pub use unsupported::UnsupportedTransformStore;

use crate::{Result, TransformStore};

/// The transform store a database runs its lens migrations on: WASM on one
/// shared engine under [`WasmSandboxConfig::restrictive`].
#[cfg(feature = "wasmtime-runtime")]
pub fn default_store() -> Result<Arc<dyn TransformStore>> {
    let store = crate::WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive()))?;
    Ok(Arc::new(store))
}

/// The transform store a database runs its lens migrations on. Built without
/// the `wasmtime-runtime` feature, it refuses every transform.
#[cfg(not(feature = "wasmtime-runtime"))]
pub fn default_store() -> Result<Arc<dyn TransformStore>> {
    Ok(Arc::new(UnsupportedTransformStore))
}
