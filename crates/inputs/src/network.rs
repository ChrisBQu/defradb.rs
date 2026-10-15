//! The runtime a service runs its round trips on, and the clock its
//! deadlines and retries read.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use defra_core::thread_bounds::{MaybeBoxFuture, MaybeSendSync};
use workers::Clock;

/// Where round trips run: network work spawned apart from the tasks that
/// wait on it, and timers for deadlines and backoff.
///
/// The network edge implements it over its runtime; [`Ambient`] runs on the
/// async runtime the caller is on.
pub trait Network: MaybeSendSync + 'static {
    /// Runs `work` to its end, whoever waits on it.
    fn spawn(&self, work: MaybeBoxFuture<'static, ()>);

    /// Completes after `duration`.
    fn sleep(&self, duration: Duration) -> MaybeBoxFuture<'static, ()>;
}

/// The async runtime the caller runs on: tokio natively, the browser's event
/// loop on wasm.
#[derive(Clone, Copy, Debug, Default)]
pub struct Ambient;

impl Network for Ambient {
    fn spawn(&self, work: MaybeBoxFuture<'static, ()>) {
        // Detached: dropping the handle never cancels the round trip.
        drop(n0_future::task::spawn(work));
    }

    fn sleep(&self, duration: Duration) -> MaybeBoxFuture<'static, ()> {
        Box::pin(n0_future::time::sleep(duration))
    }
}

/// The network edge: round trips run as its network tasks, so they stop
/// with it, and its timers time deadlines and backoff.
impl Network for edge::Edge {
    fn spawn(&self, work: MaybeBoxFuture<'static, ()>) {
        // Dropping the completion leaves the work running. A round trip
        // refused at shutdown settles its keys as closed when dropped.
        drop(edge::Edge::spawn(self, work));
    }

    fn sleep(&self, duration: Duration) -> MaybeBoxFuture<'static, ()> {
        Box::pin(edge::Edge::sleep(self, duration))
    }
}

/// The runtime's monotonic clock: tokio's natively (so a test that pauses
/// tokio's time moves it too), the browser's on wasm.
#[derive(Debug)]
pub struct RuntimeClock {
    origin: n0_future::time::Instant,
}

impl RuntimeClock {
    /// A clock whose origin is now.
    pub fn new() -> Self {
        Self {
            origin: n0_future::time::Instant::now(),
        }
    }
}

impl Default for RuntimeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RuntimeClock {
    fn now_micros(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

/// Time since it was started, on the network runtime's clock: what a caller
/// measures its own wait with.
#[derive(Debug)]
pub struct Stopwatch(n0_future::time::Instant);

impl Stopwatch {
    /// A stopwatch running from now.
    pub fn start() -> Self {
        Self(n0_future::time::Instant::now())
    }

    /// Microseconds since it started.
    pub fn elapsed_micros(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

/// A service's network and clock, passed together.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) network: Arc<dyn Network>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Runtime {
    /// Round trips on `network`, time read from `clock` (a regolith `Env`'s,
    /// through [`workers::EnvClock`], where the node has one).
    pub fn new(network: Arc<dyn Network>, clock: Arc<dyn Clock>) -> Self {
        Self { network, clock }
    }

    /// The caller's async runtime and its clock.
    pub fn ambient() -> Self {
        Self::new(Arc::new(Ambient), Arc::new(RuntimeClock::new()))
    }

    /// The network edge `edge` and its runtime's clock.
    pub fn edge(edge: edge::Edge) -> Self {
        Self::new(Arc::new(edge), Arc::new(RuntimeClock::new()))
    }

    /// The same network with `clock`.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) fn now(&self) -> u64 {
        self.clock.now_micros()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::ambient()
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}
