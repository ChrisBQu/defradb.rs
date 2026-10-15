//! The I/O a pool worker does for the stores it reads and writes.
//!
//! Each worker of a [`workers::Pool`] built with [`with_worker_io`] holds one
//! regolith `IoQueue` per database it touches, opened on its own thread the
//! first time a transaction there reaches that database, and polls every one
//! of them between tasks ([`WorkerIo`], the pool's hook). A transaction that
//! starts on such a worker reads in `ReadMode::CacheOnly` on that queue, so a
//! block the cache does not hold suspends the task on its `IoWait` instead of
//! the thread on the device, and commits with `commit_nowait`, whose ticket
//! the same queue completes. Nothing is shared with another thread: the
//! queues live in the worker's thread-local registry ([`registry`]) and only
//! that worker polls them.
//!
//! A thread that is not such a worker (a test, a one-shot CLI tool, a plain
//! thread with no pool) has no registry: its transactions read in
//! `ReadMode::Blocking` and commit synchronously, as a synchronous caller
//! asks for.
//!
//! [`settle`] waits out what a non-blocking call hands back, and [`drive`]
//! runs work whose waits are on a queue of its own from the task that awaits
//! it, for a caller that is not a worker.

mod drive;
mod registry;
mod settle;

use std::sync::Arc;

use regolith::{OptimisticTransactionDb, QueueId, ReadMode};

pub(crate) use drive::drive;
pub(crate) use settle::{settle, wait_out};

/// The pool hook that polls this worker's regolith queues. Built on the
/// worker's own thread by the factory [`with_worker_io`] installs; while it
/// lives, transactions on the thread use the thread's queues.
pub struct WorkerIo {
    _bound: registry::Bound,
}

impl workers::Hook for WorkerIo {
    fn poll(&mut self, budget: usize) -> workers::HookProgress {
        registry::poll(budget)
    }

    fn idle(&mut self, waker: &std::task::Waker) {
        registry::idle(waker);
    }
}

/// `config` with the hook that gives each worker its regolith queues: what
/// every pool that runs the data path is started with.
///
/// ```no_run
/// let pool = workers::Pool::start(storage::with_worker_io(workers::Config::default()))?;
/// let options = storage::RegolithStoreOptions::default().placed_on(&pool);
/// let store = storage::RegolithStore::open_with_options("data", options)?;
/// # let _ = store;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn with_worker_io(config: workers::Config) -> workers::Config {
    let hook: workers::HookFactory = Arc::new(|_worker| {
        Box::new(WorkerIo {
            _bound: registry::Bound::here(),
        })
    });
    workers::Config {
        hook: Some(hook),
        ..config
    }
}

/// Starts the pool a database's data path runs on: `config` with the hook
/// that gives each worker its regolith queues ([`with_worker_io`]), and with
/// `monitor` (the network edge's timer) running its stuck-poll scan when one
/// is given. What every node builder starts its pool with.
#[cfg(not(target_arch = "wasm32"))]
pub fn start_pool(
    config: workers::Config,
    monitor: Option<workers::MonitorDriver>,
) -> Result<workers::Pool, workers::Error> {
    let config = workers::Config {
        monitor: monitor.or(config.monitor.clone()),
        ..config
    };
    workers::Pool::start(with_worker_io(config))
}

/// The calling worker's queue for `db`, opened on first use; `None` off a
/// worker that polls regolith queues.
pub(super) fn queue_for(db: &Arc<OptimisticTransactionDb>) -> Option<QueueId> {
    registry::queue_for(db)
}

/// How a handle opened on the calling thread for `db` reads.
pub(super) fn read_mode_for(db: &Arc<OptimisticTransactionDb>) -> ReadMode {
    queue_for(db).map_or(ReadMode::Blocking, ReadMode::CacheOnly)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
