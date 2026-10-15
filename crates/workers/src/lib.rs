//! A thread-per-core worker pool, and everything about threads: the machine's
//! topology, where workers run, their priority and memory policy, each
//! worker's local executor, routing work to a worker, admission, blocking
//! code kept off the workers, and shutdown.

mod clock;
mod config;
mod error;
mod hook;
mod os;
mod placement;
pub mod topology;

pub use clock::{Clock, EnvClock};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use clock::StdClock;
pub use config::{
    Config, MemoryLock, MemoryPolicy, Placement, Priorities, Priority, QosClass, RealTimePolicy, Smt,
    ThreadClass, Workers,
};
pub use error::Error;
pub use hook::{Hook, HookFactory, HookProgress};
pub use os::Outcome;

/// A worker's identity: its index in the pool and the NUMA node it runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WorkerId {
    /// Its index, from 0.
    pub index: usize,
    /// The NUMA node it is placed on (0 when unpinned or without NUMA).
    pub node: usize,
}
