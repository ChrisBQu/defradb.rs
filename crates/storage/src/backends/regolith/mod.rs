//! regolith-backed storage: the one backend, on every target.
//!
//! The store implements the regolith traits its use of the engine calls for,
//! each in its own module with its contract tests: the key classifier
//! (`classifier`) and the prefix extractor (`prefix`), both read from one
//! table of key families (`families`); the merge operator (`merge`); the
//! arrival journal's log layout (`log_layout`); the event listener
//! (`listener`); the transaction hooks (`hooks`); and the at-rest key provider
//! (`encryption`). Every open installs the same set (`config`).
//!
//! # No compaction filter
//!
//! A compaction filter decides on one entry at a time, from its key and value
//! alone, and nothing the store keeps can be dropped on that evidence:
//!
//! - a superseded head key, or a marker against one, is garbage only beside
//!   another key (the marker that names the head, the head the marker names,
//!   the head's publication guard). Writers reclaim them in their own
//!   transactions, and regolith's deletion-density compaction retires the
//!   tombstones (`db::block::heads`);
//! - nothing the store keeps expires by time alone. A pending DAG record
//!   carries its replay authorization's expiry, yet it leaves only when its
//!   root is merged or quarantined; downsample retention is cut from the
//!   collection's policy and its downsample target's window, both in other
//!   keys;
//! - a document part's tombstone carries the priority a late merge is ordered
//!   against, and an index entry, a block, an ACP tuple and a guard each
//!   decide something until another key says otherwise.
//!
//! A filter that dropped any of them would change what a reader or a later
//! merge sees, which compaction must never do, so the store installs none.

mod classifier;
mod config;
mod encryption;
mod event_order;
mod families;
mod handle;
mod hooks;
pub(crate) mod iterator;
mod listener;
mod log_layout;
pub mod merge;
mod post_commit;
mod prefix;
mod retry;
mod slots;
#[cfg(not(target_arch = "wasm32"))]
mod rewrite;
mod store;
mod transaction;
mod worker_io;

pub use config::RegolithStoreOptions;
pub use store::RegolithStore;
pub use transaction::RegolithTxn;
#[cfg(not(target_arch = "wasm32"))]
pub use worker_io::start_pool;
pub use worker_io::{with_worker_io, WorkerIo};
