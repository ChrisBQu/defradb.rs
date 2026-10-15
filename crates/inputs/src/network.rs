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
/// The network edge ([`edge::Edge`]) is the implementation a node passes at
/// construction. A service given none runs inline ([`Runtime::inline`]).
pub trait Network: MaybeSendSync + 'static {
    /// Runs `work` to its end, whoever waits on it. A network that cannot
    /// run it drops it, which settles its keys as closed.
    fn spawn(&self, work: MaybeBoxFuture<'static, ()>);

    /// Completes after `duration`.
    fn sleep(&self, duration: Duration) -> MaybeBoxFuture<'static, ()>;
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
///
/// With a network (the node's edge) round trips run as its tasks, shared by
/// every caller that asks for a key in flight, and timed by its timers. Inline
/// (no network given) a caller's round trips run in the caller's own task:
/// nothing is spawned and no timer is read, so it never reaches for a runtime
/// it was not given. Its keys are batched and cached as usual but shared with
/// no other caller, a round trip is bounded only by the source's own
/// timeouts, and a retry follows a failure at once.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) network: Option<Arc<dyn Network>>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Runtime {
    /// Round trips on `network`, time read from `clock` (a regolith `Env`'s,
    /// through [`workers::EnvClock`], where the node has one).
    pub fn new(network: Arc<dyn Network>, clock: Arc<dyn Clock>) -> Self {
        Self {
            network: Some(network),
            clock,
        }
    }

    /// The network edge `edge` and its runtime's clock.
    pub fn edge(edge: edge::Edge) -> Self {
        Self::new(Arc::new(edge), Arc::new(RuntimeClock::new()))
    }

    /// No network: each caller runs its own round trips in its own task.
    pub fn inline() -> Self {
        Self {
            network: None,
            clock: Arc::new(RuntimeClock::new()),
        }
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

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("inline", &self.network.is_none())
            .field("clock", &self.clock)
            .finish_non_exhaustive()
    }
}
