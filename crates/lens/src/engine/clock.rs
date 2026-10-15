//! What advances an engine's epoch. A call's tick budget and its yields both
//! count the ticks, so a call past its budget traps instead of running forever.
//!
//! The source is a [`TickSource`]: the engine hands it an [`EpochTick`] and the
//! sandbox's period, and holds the guard it returns while any clone of the
//! engine lives. On a worker pool the ticks are a periodic job of its deadline
//! queue ([`PoolTicks`]); without one a thread of its own ticks
//! ([`ThreadTicks`]).

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

/// Ticks from a background job on a worker of the pool: one timer on that
/// worker's deadline queue per period, and no thread of its own.
pub struct PoolTicks(pub workers::Pool);

impl TickSource for PoolTicks {
    fn start(&self, tick: EpochTick, period: Duration) -> Result<TickGuard> {
        let job = workers::spawn_background(Some(&self.0), "lens::epoch", move || async move {
            let Err(error) = workers::every(period, || {
                tick.tick();
                async {}
            })
            .await;
            error!(%error, "lens epoch ticks stopped: they run only on a worker");
        })
        .map_err(|error| crate::Error::WasmLoad(format!("lens epoch ticks did not start: {error}")))?;
        Ok(Box::new(StopOnDrop(job)))
    }
}

/// Stops the tick job when the engine lets go of it.
struct StopOnDrop(workers::Background);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.abort();
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
