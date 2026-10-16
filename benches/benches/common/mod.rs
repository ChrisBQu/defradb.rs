//! Helpers shared by more than one benchmark target.
//!
//! Only helpers that are byte-identical at every call site live here. A helper
//! that differs between targets stays with its target: for tokio runtimes in
//! particular, the builder configuration is part of what a benchmark measures,
//! and four of the six variants in this suite are deliberately unique to their
//! target.

#![allow(dead_code)]

pub mod sift;
pub mod vector;

use std::sync::atomic::{AtomicBool, Ordering};

use regolith::sync::OnceCell;
use tokio::runtime::Runtime;

/// Built once per process and reused. `Runtime::new()` is multi-threaded across
/// all available cores.
pub fn shared_runtime() -> &'static Runtime {
    static RUNTIME: OnceCell<Runtime> = OnceCell::new();
    static CLAIMED: AtomicBool = AtomicBool::new(false);
    // The runtime spawns threads, so exactly one caller builds it; the rest wait for it.
    if !CLAIMED.swap(true, Ordering::AcqRel) {
        let _ = RUNTIME.set(Runtime::new().unwrap());
    }
    loop {
        if let Some(runtime) = RUNTIME.get() {
            return runtime;
        }
        std::thread::yield_now();
    }
}

/// A fresh multi-threaded runtime per call, for targets that build one outside
/// the measured region and hand it to criterion.
pub fn owned_runtime() -> Runtime {
    Runtime::new().expect("a tokio runtime")
}
