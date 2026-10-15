//! The epoch clock: a thread that advances one engine's epoch every period, so
//! a call past its deadline traps instead of running forever.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::error;
use wasmtime::Engine;

use crate::{Error, Result};

/// One epoch clock per shared engine. It runs on its own thread, never on an
/// async runtime: transform execution is synchronous, so every runtime worker
/// could be occupied by WASM while the clock needs to advance.
pub(crate) struct EpochClock {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl EpochClock {
    pub(crate) fn start(engine: Engine, period: Duration) -> Result<Self> {
        if period.is_zero() {
            return Err(Error::InvalidConfig(
                "WASM epoch tick period must be greater than zero".into(),
            ));
        }

        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::Builder::new()
            .name("defradb-lens-epoch".into())
            .spawn(move || loop {
                thread::park_timeout(period);
                if worker_stop.load(Ordering::Acquire) {
                    break;
                }
                engine.increment_epoch();
            })?;

        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }
}

impl Drop for EpochClock {
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
