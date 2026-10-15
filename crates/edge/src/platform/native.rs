//! The runtime natively: one tokio runtime, built, held and dropped by an
//! owner thread of its own, so it is always dropped outside any async
//! context, whoever drops the `EdgeRuntime` and wherever.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread;

use n0_future::task::JoinHandle;
use n0_future::time::{Instant, Sleep};
use regolith::sync::{Event, OnceCell};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};

use crate::config::EdgeConfig;
use crate::error::EdgeError;
use crate::gate::Gate;
use crate::handle::{drain_gate, Drained};
use crate::shared::Shared;

/// Where network work runs: the runtime's handle.
#[derive(Clone)]
pub(crate) struct Rt {
    handle: Handle,
}

impl Rt {
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    pub(crate) fn blocking<F, T>(&self, work: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.handle.spawn_blocking(work)
    }

    /// Drives `future` on the calling thread, which must be outside any
    /// executor (a blocking thread of this runtime, or a plain thread).
    pub(crate) fn drive<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.handle.block_on(future)
    }

    /// A timer of this runtime's clock, pollable from any thread.
    pub(crate) fn sleep_until(&self, at: Instant) -> Sleep {
        let _context = self.handle.enter();
        tokio::time::sleep_until(at)
    }
}

/// Runs `wait` on the calling thread, which it blocks. On a thread of a
/// multi-threaded runtime the thread's executor work is handed to another
/// thread first; on a single-threaded executor nothing else could run its
/// tasks, so the wait is refused. A panic inside `wait` comes back as the
/// inner error.
pub(crate) fn wait_here<T>(wait: impl FnOnce() -> T) -> Result<thread::Result<T>, EdgeError> {
    // The edge is the one place that may ask which runtime the caller is on.
    #[allow(clippy::disallowed_methods)]
    let flavor = Handle::try_current()
        .ok()
        .map(|current| current.runtime_flavor());
    match flavor {
        None => Ok(catch_unwind(AssertUnwindSafe(wait))),
        Some(RuntimeFlavor::MultiThread) => Ok(catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(wait)
        }))),
        Some(_) => Err(EdgeError::WouldBlock),
    }
}

#[derive(Default)]
struct Control {
    stop: Event,
    finished: Event,
    drained: OnceCell<Drained>,
}

/// The owner thread: it builds the runtime, waits for the stop request,
/// then drains, stops the rest and drops the runtime, in that order.
pub(crate) struct Owner {
    control: Arc<Control>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Owner {
    pub(crate) fn start(
        config: &EdgeConfig,
        gate: &Arc<Gate>,
    ) -> Result<(Arc<Shared>, Self), EdgeError> {
        let control = Arc::new(Control::default());
        let (started, starting) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(format!("{}-owner", config.thread_name))
            .spawn({
                let config = config.clone();
                let gate = Arc::clone(gate);
                let control = Arc::clone(&control);
                move || own(config, gate, control, started)
            })
            .map_err(|error| EdgeError::Start(error.to_string()))?;
        match starting.recv() {
            Ok(Ok(shared)) => Ok((
                shared,
                Self {
                    control,
                    thread: Some(thread),
                },
            )),
            Ok(Err(error)) => {
                let _ = thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = thread.join();
                Err(EdgeError::Start(
                    "the runtime's owner thread exited while starting".to_string(),
                ))
            }
        }
    }

    pub(crate) fn request_stop(&self) {
        self.control.stop.set();
    }

    /// Waits on the calling thread until the owner dropped the runtime.
    pub(crate) fn wait(&mut self) -> Result<Drained, EdgeError> {
        let Some(thread) = self.thread.take() else {
            return self.drained();
        };
        match wait_here(|| thread.join()) {
            Ok(Ok(Ok(()))) => self.drained(),
            Ok(Ok(Err(_))) | Ok(Err(_)) => Err(EdgeError::Panicked(
                "the network runtime's owner thread panicked while stopping".to_string(),
            )),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<Drained, EdgeError> {
        self.request_stop();
        self.control.finished.wait().await;
        self.drained()
    }

    fn drained(&self) -> Result<Drained, EdgeError> {
        self.control
            .drained
            .get()
            .copied()
            .ok_or(EdgeError::Cancelled)
    }
}

type Started = std::sync::mpsc::SyncSender<Result<Arc<Shared>, EdgeError>>;

fn own(config: EdgeConfig, gate: Arc<Gate>, control: Arc<Control>, started: Started) {
    let runtime = match build(&config) {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = started.send(Err(EdgeError::Start(error.to_string())));
            return;
        }
    };
    let shared = Shared::new(
        Rt {
            handle: runtime.handle().clone(),
        },
        config.deadline,
    );
    if started.send(Ok(Arc::clone(&shared))).is_err() {
        return;
    }
    let _finished = Finished(&control);
    tracing::info!(
        threads = config.threads.map(|threads| threads.get()),
        name = %config.thread_name,
        "network runtime started"
    );

    runtime.block_on(control.stop.wait());
    gate.close();
    let drained = runtime.block_on(drain_gate(&gate, &shared, config.drain));
    shared.stop();
    runtime.shutdown_timeout(config.stop);

    if drained.is_complete() {
        tracing::info!("network runtime stopped");
    } else {
        tracing::warn!(
            remaining = drained.remaining,
            drain_secs = config.drain.as_secs_f64(),
            "network runtime stopped work that outlived the drain deadline"
        );
    }
    let _ = control.drained.set(drained);
}

/// Sets `finished` however `own` ends, so an awaited shutdown never hangs.
struct Finished<'a>(&'a Control);

impl Drop for Finished<'_> {
    fn drop(&mut self) {
        self.0.finished.set();
    }
}

fn build(config: &EdgeConfig) -> std::io::Result<Runtime> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all().thread_name(config.thread_name.clone());
    if let Some(threads) = config.threads {
        builder.worker_threads(threads.get());
    }
    if let Some(hook) = config.on_thread_start.clone() {
        builder.on_thread_start(move || hook());
    }
    builder.build()
}
