//! One wasmtime engine with its sandbox and epoch clock, shared by its clones.

use std::sync::Arc;

use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};

use super::clock::EpochClock;
use super::sandbox::WasmSandboxConfig;
use crate::{Error, Result};

/// A wasmtime engine, the sandbox its calls run under, and the clock that
/// advances its epoch. Clones share all three; the clock stops when the last
/// clone is dropped.
#[derive(Clone)]
pub(crate) struct LensEngine {
    engine: Engine,
    sandbox: Option<WasmSandboxConfig>,
    /// Held, never read: the clock runs while any clone holds it.
    _clock: Option<Arc<EpochClock>>,
}

/// What one call's wasmtime store holds: the host's state, and the memory
/// limit the sandbox puts on the call.
pub(crate) struct Sandboxed<T> {
    pub(crate) host: T,
    limits: StoreLimits,
}

impl LensEngine {
    /// Build an engine. Without a sandbox, calls run without limits and no
    /// clock runs.
    pub(crate) fn new(sandbox: Option<WasmSandboxConfig>) -> Result<Self> {
        let mut config = Config::new();
        #[cfg(target_os = "macos")]
        {
            // Fork-capable embedders can crash when Wasmtime's Mach-port
            // exception handler is initialized before spawning child processes.
            config.macos_use_mach_ports(false);
        }
        if let Some(ref sb) = sandbox {
            if sb.fuel_budget.is_some() {
                config.consume_fuel(true);
            }
            if sb.epoch_deadline_ticks.is_some() {
                config.epoch_interruption(true);
            }
        }
        let engine = Engine::new(&config)
            .map_err(|e| Error::WasmLoad(format!("failed to create WASM engine: {}", e)))?;
        let clock = match sandbox.as_ref() {
            Some(sb) => match (sb.epoch_deadline_ticks, sb.epoch_tick_period) {
                (Some(_), Some(period)) => {
                    Some(Arc::new(EpochClock::start(engine.clone(), period)?))
                }
                (Some(_), None) => {
                    return Err(Error::InvalidConfig(
                        "WASM epoch interruption requires an epoch tick period".into(),
                    ));
                }
                _ => None,
            },
            None => None,
        };

        Ok(Self {
            engine,
            sandbox,
            _clock: clock,
        })
    }

    /// The wasmtime engine, to compile modules and build linkers on.
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    /// A store for one call, under the sandbox: its memory limit, its fuel and
    /// its epoch deadline. The engine turns fuel and epoch interruption on
    /// exactly when the sandbox sets them, so the two always agree.
    pub(crate) fn store<T>(&self, host: T) -> Result<Store<Sandboxed<T>>>
    where
        T: 'static,
    {
        let limits = self
            .sandbox
            .as_ref()
            .and_then(|sb| sb.max_memory_bytes)
            .map_or_else(
                || StoreLimitsBuilder::new().build(),
                |max| StoreLimitsBuilder::new().memory_size(max).build(),
            );
        let mut store = Store::new(&self.engine, Sandboxed { host, limits });
        store.limiter(|state| &mut state.limits);
        if let Some(ref sb) = self.sandbox {
            if let Some(fuel) = sb.fuel_budget {
                store
                    .set_fuel(fuel)
                    .map_err(|e| Error::WasmExecution(format!("failed to set fuel: {}", e)))?;
            }
            if let Some(ticks) = sb.epoch_deadline_ticks {
                store.set_epoch_deadline(ticks);
            }
        }
        Ok(store)
    }

    /// A handle that shows whether the clock still runs, for tests of its
    /// lifetime.
    #[cfg(test)]
    pub(crate) fn clock_handle(&self) -> Option<std::sync::Weak<EpochClock>> {
        self._clock.as_ref().map(Arc::downgrade)
    }
}
