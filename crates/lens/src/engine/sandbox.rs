//! The limits a lens call runs under.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Opt-in resource limits for WASM execution.
///
/// All fields are optional. When `None`, the corresponding limit is not applied,
/// giving the WASM module full access to that resource.
///
/// Fuel and ticks are budgets per input document: each document a call takes
/// in through `lens::next` refills both. A stream of any length runs, while a
/// call that keeps running without taking a document (a loop that never
/// ends, output that never ends) traps.
#[derive(Debug, Clone)]
pub struct WasmSandboxConfig {
    /// Maximum memory a single WASM instance may allocate (bytes).
    pub max_memory_bytes: Option<usize>,

    /// Fuel per input document (roughly an instruction count).
    pub fuel_budget: Option<u64>,

    /// Epoch ticks a call may run per input document before it traps. A call
    /// yields to the executor polling it on every tick it runs through.
    pub epoch_deadline_ticks: Option<u64>,

    /// Period at which the shared engine advances its epoch clock. The 100 ms
    /// restrictive default is a measured provisional setting; tune it for the
    /// workload with the ignored probe described in
    /// `docs/benchmarks/lens-epoch-period/measurement.md`.
    pub epoch_tick_period: Option<Duration>,

    /// The most lens calls that run to completion for their caller (one
    /// document's migration, a JSON batch) at once; a call past it waits for
    /// one to end. Each call holds at most `max_memory_bytes` of instance
    /// memory and one fiber stack per module, so this bounds their memory.
    ///
    /// A streaming call (a lensed view reading its source) takes no permit: it
    /// waits on its source, which may itself run lens calls, so a permit held
    /// across that wait could leave every permit held by calls waiting for
    /// one. Its memory is bounded by the reads in flight instead.
    pub max_concurrent_calls: Option<usize>,
}

impl WasmSandboxConfig {
    /// Recommended defaults for high-security environments. Calls that run to
    /// completion are bounded at one per core: they only compute, so more
    /// would add memory without adding throughput.
    pub fn restrictive() -> Self {
        Self {
            max_memory_bytes: Some(64 * 1024 * 1024), // 64 MiB
            fuel_budget: Some(1_000_000),
            epoch_deadline_ticks: Some(2),
            epoch_tick_period: Some(Duration::from_millis(100)),
            max_concurrent_calls: Some(
                std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
            ),
        }
    }
}
