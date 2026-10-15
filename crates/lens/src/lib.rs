//! Lens schema migration support for DefraDB.
//!
//! Lens enables non-destructive schema evolution via WASM transforms.
//! Documents are transformed at query time from old schema versions to new ones.

mod config;
mod doc;
mod engine;
mod error;
mod history;
mod pipeline;
mod store;
#[cfg(feature = "wasmtime-runtime")]
mod wasm;
#[cfg(feature = "wasmtime-runtime")]
mod wasm_runtime;

pub use config::{LensConfig, LensModule};
pub use doc::{LensDoc, DELETED_FIELD, DOC_ID_FIELD};
pub use engine::{default_store, UnsupportedTransformStore};
pub use error::{Error, Result};
pub use history::{build_targeted_history, CollectionHistoryLink, TargetedHistoryLink};
pub use pipeline::{transform_to_target, Lens, LensInput};
pub use store::{
    LensDocResultStream, LensDocStream, MemoryTransformStore, TransformId, TransformStore,
};
#[cfg(feature = "wasmtime-runtime")]
pub use engine::WasmSandboxConfig;
#[cfg(feature = "wasmtime-runtime")]
pub use wasm::WasmTransformStore;
