//! What advances an engine's epoch. A call's tick budget and its yields both
//! count the ticks, so a call past its budget traps instead of running forever.
//!
//! The source is a [`TickSource`]: the engine hands it an [`EpochTick`] and the
//! sandbox's period, and holds the guard it returns while any clone of the
//! engine lives. Today a thread ticks ([`ThreadTicks`]); a worker pool's
//! deadline queue can take over through the same trait.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::error;
use wasmtime::Engine;

use crate::Result;

/// Stops a tick source when dropped.
pub type TickGuard = Box<dyn Send + Sync>;

/// Advances a lens engine's epoch every period until its guard is dropped.
pub trait TickSource: Send + Sync {
    /// Starts calling [`EpochTick::tick`] every `period`.
    fn start(&self, tick: EpochTick, period: Duration) -> Result<TickGuard>;
}

/// The handle a [`TickSource`] advances one engine's epoch through.
pub struct EpochTick(pub(super) Engine);

impl EpochTick {
    /// Advances the epoch by one tick: every call running on the engine then
    /// yields, and a call out of ticks traps.
    pub fn tick(&self) {
        self.0.increment_epoch();
    }
}

/// Ticks on a thread of its own, parked between ticks.
pub(crate) struct ThreadTicks;

impl TickSource for ThreadTicks {
    fn start(&self, tick: EpochTick, period: Duration) -> Result<TickGuard> {
        Ok(Box::new(TickThread::start(tick, period)?))
    }
}

struct TickThread {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl TickThread {
    fn start(tick: EpochTick, period: Duration) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::Builder::new()
            .name("defradb-lens-epoch".into())
            .spawn(move || loop {
                thread::park_timeout(period);
                if worker_stop.load(Ordering::Acquire) {
                    break;
                }
                tick.tick();
            })?;

        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }
}

impl Drop for TickThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            if worker.join().is_err() {
                error!("lens epoch clock thread panicked; lens time limits stopped at the panic");
            }
        }
    }
}
