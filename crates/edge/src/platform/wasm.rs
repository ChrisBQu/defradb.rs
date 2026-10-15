//! The runtime on wasm32: the browser's event loop, through
//! wasm-bindgen-futures and browser timers. Nothing is built and nothing is
//! dropped; shutdown stops accepting and drains, and the page's loop goes
//! on. A panic aborts the module, so containment cannot catch it here.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use n0_future::task::JoinHandle;
use n0_future::time::{Instant, Sleep};

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::gate::Gate;
use crate::handle::{drain_gate, Drained};
use crate::shared::Shared;

/// Where network work runs: the browser's event loop.
#[derive(Clone)]
pub(crate) struct Rt;

impl Rt {
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        n0_future::task::spawn(future)
    }

    /// One thread runs everything, so blocking work runs as a task.
    pub(crate) fn blocking<F, T>(&self, work: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + 'static,
        T: 'static,
    {
        n0_future::task::spawn(async move { work() })
    }

    pub(crate) fn sleep_until(&self, at: Instant) -> Sleep {
        n0_future::time::sleep_until(at)
    }
}

/// The event loop belongs to the page; the owner keeps only how long a
/// shutdown drains.
pub(crate) struct Owner {
    drain: Duration,
    gate: Arc<Gate>,
    shared: Arc<Shared>,
}

impl Owner {
    pub(crate) fn start(
        config: &EdgeConfig,
        gate: &Arc<Gate>,
    ) -> Result<(Arc<Shared>, Self), EdgeError> {
        let shared = Shared::new(Rt, config.deadline);
        Ok((
            Arc::clone(&shared),
            Self {
                drain: config.drain,
                gate: Arc::clone(gate),
                shared,
            },
        ))
    }

    pub(crate) fn request_stop(&self) {
        self.gate.close();
    }

    pub(crate) async fn finish(&mut self) -> Result<Drained, EdgeError> {
        self.request_stop();
        Ok(drain_gate(&self.gate, &self.shared, self.drain).await)
    }
}
