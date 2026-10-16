//! What differs between targets: how work is spawned, how a timer is made,
//! and who owns the event loop. Natively one tokio runtime on an owner
//! thread; on wasm32 the browser's event loop.

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::{wait_here, Owner, Rt};
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm::{Owner, Rt};
