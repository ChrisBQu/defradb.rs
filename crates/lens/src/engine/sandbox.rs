//! The limits a lens call runs under.

use std::time::Duration;

/// Opt-in resource limits for WASM execution.
///
/// All fields are optional. When `None`, the corresponding limit is not applied,
/// giving the WASM module full access to that resource.
#[derive(Debug, Clone)]
pub struct WasmSandboxConfig {
    /// Maximum memory a single WASM instance may allocate (bytes).
    pub max_memory_bytes: Option<usize>,

    /// Fuel budget per transform execution (roughly maps to instruction count).
    pub fuel_budget: Option<u64>,

    /// Epoch deadline ticks before interruption.
    pub epoch_deadline_ticks: Option<u64>,

    /// Period at which the shared engine advances its epoch clock. The 100 ms
    /// restrictive default is a measured provisional setting; tune it for the
    /// workload with the ignored probe described in
    /// `docs/benchmarks/lens-epoch-period/measurement.md`.
    pub epoch_tick_period: Option<Duration>,
}

impl WasmSandboxConfig {
    /// Recommended defaults for high-security environments.
    pub fn restrictive() -> Self {
        Self {
            max_memory_bytes: Some(64 * 1024 * 1024), // 64 MiB
            fuel_budget: Some(1_000_000),
            epoch_deadline_ticks: Some(2),
            epoch_tick_period: Some(Duration::from_millis(100)),
        }
    }
}
