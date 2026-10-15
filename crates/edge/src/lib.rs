//! The network edge: the one runtime that serves I/O and networking, held
//! in one place and reached only through a narrow handle.
//!
//! [`EdgeRuntime`] starts the runtime from an [`EdgeConfig`] and shuts it
//! down in one order: stop accepting, drain admitted work with a deadline,
//! stop the rest, drop the runtime outside any async context. Every network
//! component (HTTP, P2P, the KMS transport, the Orbis and Vera clients) is
//! given an [`Edge`] at construction and runs on it; nothing looks a
//! runtime up on its own. A [`Scope`] lets one component stop accepting and
//! drain its own work.
//!
//! Every completion the handle gives back, a [`Task`], carries a deadline,
//! and a panic in network work fails only that work's completion and is
//! counted ([`EdgeStats`]). Long-running work, such as an accept loop or a
//! subscription, runs as a [`Service`] that stops when its runtime or scope
//! stops accepting.
//!
//! No type of the runtime underneath appears in this crate's API, so
//! replacing it replaces this crate alone. Natively it is one tokio
//! runtime; on wasm32 it is the browser's event loop, behind the same
//! handle (without the blocking calls a browser cannot make). A build
//! that starts no networking starts no runtime.

mod config;
mod contain;
mod error;
mod gate;
mod handle;
mod platform;
mod pool;
mod runtime;
mod shared;
mod stats;
mod task;
#[cfg(feature = "testing")]
pub mod testing;
mod timer;

pub use config::{EdgeConfig, ThreadStart};
pub use error::EdgeError;
pub use handle::{Drained, Edge, Scope};
pub use runtime::EdgeRuntime;
pub use stats::EdgeStats;
#[cfg(target_arch = "wasm32")]
pub use pool::page_pool;
pub use task::{Service, Task};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
