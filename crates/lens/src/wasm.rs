//! WASM transform execution using wasmtime.
//!
//! Provides the runtime for executing Lens WASM modules.
//! Resource limits (memory, CPU fuel, epoch interruption) are opt-in via
//! `WasmSandboxConfig` -- by default modules run without restrictions.

use async_trait::async_trait;
use futures::StreamExt;
use kovan_map::HopscotchMap;
use rapidhash::fast::RandomState;
use rapidhash::RapidHashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use wasmtime::{Config, Engine, Module, StoreLimits, StoreLimitsBuilder};

use tracing::{info, warn};

use crate::store::{LensDocResultStream, LensDocStream, TransformId, TransformStore};
use crate::wasm_runtime::execute_batch_transform;
use crate::{Error, LensConfig, LensDoc, LensModule, Result};

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

    /// Period at which the shared engine advances its epoch clock. The
    /// restrictive default is provisional until lens-call measurements tune it.
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

/// One independent epoch clock per shared Wasmtime engine. This must not run
/// on the Tokio runtime: transform execution is synchronous, so every runtime
/// worker could be occupied by WASM while the clock needs to advance.
struct EpochTicker {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl EpochTicker {
    fn start(engine: Engine, period: Duration) -> Result<Self> {
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

impl Drop for EpochTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            let _ = worker.join();
        }
    }
}

/// WASM-based transform store.
///
/// Manages WASM module instances and executes transforms.
pub struct WasmTransformStore {
    engine: Engine,
    modules: HopscotchMap<TransformId, Arc<Vec<CompiledModule>>, RandomState>,
    configs: HopscotchMap<TransformId, LensConfig, RandomState>,
    sandbox: Option<WasmSandboxConfig>,
    epoch_ticker: Option<Arc<EpochTicker>>,
}

#[derive(Clone)]
struct CompiledModule {
    module: Module,
    arguments: Option<serde_json::Value>,
    inverse: bool,
}

impl WasmTransformStore {
    /// Create a new WASM transform store.
    ///
    /// By default, no resource limits are applied. Pass a `WasmSandboxConfig`
    /// via `with_sandbox` to enable opt-in resource restrictions.
    pub fn new() -> Result<Self> {
        Self::with_sandbox(None)
    }

    /// Create a WASM transform store with optional sandboxing.
    pub fn with_sandbox(sandbox: Option<WasmSandboxConfig>) -> Result<Self> {
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
        let epoch_ticker = match sandbox.as_ref() {
            Some(sb) => match (sb.epoch_deadline_ticks, sb.epoch_tick_period) {
                (Some(_), Some(period)) => {
                    Some(Arc::new(EpochTicker::start(engine.clone(), period)?))
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
            modules: HopscotchMap::with_hasher(RandomState::default()),
            configs: HopscotchMap::with_hasher(RandomState::default()),
            sandbox,
            epoch_ticker,
        })
    }

    fn empty_with_shared_engine(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            modules: HopscotchMap::with_hasher(RandomState::default()),
            configs: HopscotchMap::with_hasher(RandomState::default()),
            sandbox: self.sandbox.clone(),
            epoch_ticker: self.epoch_ticker.clone(),
        }
    }

    /// Load a WASM module from the given lens configuration.
    ///
    /// When loading from a file path, validates the path to prevent traversal
    /// attacks. The path must be absolute, must not contain `..` segments,
    /// and must have a `.wasm` extension.
    fn load_module(&self, lens: &LensModule) -> Result<Module> {
        if let Some(ref path_str) = lens.path {
            let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
            Self::validate_wasm_path(clean_path)?;
            let path = Path::new(clean_path);
            Module::from_file(&self.engine, path).map_err(|e| {
                Error::WasmLoad(format!(
                    "failed to load WASM from {}: {}",
                    path.display(),
                    e
                ))
            })
        } else if let Some(ref bytes) = lens.module {
            Module::new(&self.engine, bytes)
                .map_err(|e| Error::WasmLoad(format!("failed to load WASM from bytes: {}", e)))
        } else {
            Err(Error::InvalidConfig(
                "lens module must have either path or module bytes".to_string(),
            ))
        }
    }

    fn compile_modules(&self, lenses: &[LensModule]) -> Result<Vec<CompiledModule>> {
        lenses
            .iter()
            .map(|lens| {
                Ok(CompiledModule {
                    module: self.load_module(lens)?,
                    arguments: lens.arguments.clone(),
                    inverse: lens.inverse,
                })
            })
            .collect()
    }

    /// Transform JSON values using a registered lens pipeline.
    ///
    /// This mirrors the standalone host contract where top-level inputs and
    /// outputs are JSON arrays, including `null` items.
    pub fn transform_json(
        &self,
        id: &TransformId,
        values: Vec<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>> {
        let compiled_modules = self
            .modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;

        execute_pipeline_values(
            &self.engine,
            (*compiled_modules).clone(),
            values,
            self.sandbox.clone(),
            false,
        )
    }

    /// Inverse transform JSON values using a registered lens pipeline.
    pub fn inverse_json(
        &self,
        id: &TransformId,
        values: Vec<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>> {
        let compiled_modules = self
            .modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;

        execute_pipeline_values(
            &self.engine,
            (*compiled_modules).clone(),
            values,
            self.sandbox.clone(),
            true,
        )
    }

    /// Validate a WASM module file path to prevent path traversal.
    fn validate_wasm_path(path_str: &str) -> Result<()> {
        let path = Path::new(path_str);

        if !path.is_absolute() {
            return Err(Error::PathNotAllowed(
                "WASM module path must be absolute".to_string(),
            ));
        }

        for component in path.components() {
            if let std::path::Component::ParentDir = component {
                return Err(Error::PathNotAllowed(
                    "WASM module path must not contain '..' segments".to_string(),
                ));
            }
        }

        match path.extension().and_then(|e| e.to_str()) {
            Some("wasm") => {}
            _ => {
                return Err(Error::PathNotAllowed(
                    "WASM module path must have .wasm extension".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransformStore for WasmTransformStore {
    fn new_isolated_store(&self) -> Option<Arc<dyn TransformStore>> {
        Some(Arc::new(self.empty_with_shared_engine()))
    }

    async fn add(&self, config: LensConfig) -> Result<TransformId> {
        use sha2::{Digest, Sha256};

        info!(
            source_version = %config.source_schema_version_id,
            dest_version = %config.destination_schema_version_id,
            lenses_count = config.lenses.len(),
            "Adding transform to WASM store"
        );

        let lenses_json = serde_json::to_vec(&config.lenses)
            .map_err(|e| Error::Pipeline(format!("failed to serialize lenses: {}", e)))?;
        let mut hasher = Sha256::new();
        hasher.update(&lenses_json);
        let hash = hasher.finalize();
        let id = TransformId::new(format!("baf{}", hex::encode(&hash[..16])));

        info!(transform_id = %id, "Computed transform ID");

        if self.modules.contains_key(&id) {
            info!(transform_id = %id, "Transform already exists, returning existing ID");
            return Ok(id);
        }

        info!("Loading WASM modules");
        let compiled_modules = self.compile_modules(&config.lenses)?;
        info!(transform_id = %id, "WASM module compiled successfully");

        self.modules
            .insert_if_absent(id.clone(), Arc::new(compiled_modules));
        self.configs.insert_if_absent(id.clone(), config);

        info!(transform_id = %id, "Transform added to store");

        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> Result<()> {
        info!(transform_id = %id, "Adding transform to WASM store with explicit ID");

        if self.modules.contains_key(&id) {
            info!(transform_id = %id, "Transform already exists");
            return Ok(());
        }

        let compiled_modules = self.compile_modules(&config.lenses)?;

        self.modules
            .insert_if_absent(id.clone(), Arc::new(compiled_modules));
        self.configs.insert_if_absent(id.clone(), config);

        info!(transform_id = %id, "Transform added with explicit ID");
        Ok(())
    }

    async fn list(&self) -> Result<RapidHashMap<String, crate::LensModule>> {
        let result = self
            .configs
            .iter()
            .filter_map(|(id, config)| config.lens().cloned().map(|l| (id.to_string(), l)))
            .collect();
        Ok(result)
    }

    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        info!(transform_id = %id, "WasmTransformStore::transform called");

        let compiled_modules = self.modules.get(id).ok_or_else(|| {
            warn!(
                transform_id = %id,
                stored_ids = ?self.modules.keys().map(|k| k.to_string()).collect::<Vec<_>>(),
                "Transform not found in WASM store"
            );
            Error::TransformNotFound(id.to_string())
        })?;

        info!(
            transform_id = %id,
            modules_count = compiled_modules.len(),
            "Transform found, creating execution stream"
        );

        info!(transform_id = %id, "Executing forward WASM transform (batch mode)");
        Ok(execute_pipeline_stream(
            self.engine.clone(),
            (*compiled_modules).clone(),
            docs,
            self.sandbox.clone(),
            false,
            self.epoch_ticker.clone(),
        ))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        let compiled_modules = self
            .modules
            .get(id)
            .ok_or_else(|| Error::TransformNotFound(id.to_string()))?;

        Ok(execute_pipeline_stream(
            self.engine.clone(),
            (*compiled_modules).clone(),
            docs,
            self.sandbox.clone(),
            true,
            self.epoch_ticker.clone(),
        ))
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.modules.contains_key(id)
    }

    async fn remove(&self, id: &TransformId) -> Result<()> {
        if self.modules.remove(id).is_none() {
            return Err(Error::TransformNotFound(id.to_string()));
        }
        self.configs.remove(id);
        Ok(())
    }
}

fn execute_pipeline_stream(
    engine: Engine,
    mut modules: Vec<CompiledModule>,
    docs: LensDocStream,
    sandbox: Option<WasmSandboxConfig>,
    inverse: bool,
    epoch_ticker: Option<Arc<EpochTicker>>,
) -> LensDocResultStream {
    enum Phase {
        Collecting {
            engine: Engine,
            modules: Vec<CompiledModule>,
            docs: LensDocStream,
            sandbox: Option<WasmSandboxConfig>,
            epoch_ticker: Option<Arc<EpochTicker>>,
        },
        Yielding {
            results: Vec<LensDoc>,
            index: usize,
        },
    }

    if inverse {
        modules.reverse();
        for module in &mut modules {
            module.inverse = !module.inverse;
        }
    }

    let initial = Phase::Collecting {
        engine,
        modules,
        docs,
        sandbox,
        epoch_ticker,
    };

    Box::pin(futures::stream::unfold(initial, |phase| async {
        match phase {
            Phase::Collecting {
                engine,
                modules,
                docs,
                sandbox,
                epoch_ticker,
            } => {
                let _ticker_keepalive = epoch_ticker;
                let current_docs: Vec<serde_json::Value> = docs
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .map(serde_json::Value::Object)
                    .collect();

                let current_docs =
                    match execute_pipeline_values(&engine, modules, current_docs, sandbox, false) {
                        Ok(outputs) => outputs,
                        Err(e) => {
                            return Some((
                                Err(e),
                                Phase::Yielding {
                                    results: Vec::new(),
                                    index: 0,
                                },
                            ));
                        }
                    };

                let mut results = Vec::with_capacity(current_docs.len());
                for value in current_docs {
                    match value {
                        serde_json::Value::Object(doc) => results.push(doc),
                        other => {
                            return Some((
                                Err(Error::WasmExecution(format!(
                                    "expected JSON object output, got {}",
                                    json_value_type(&other)
                                ))),
                                Phase::Yielding {
                                    results: Vec::new(),
                                    index: 0,
                                },
                            ));
                        }
                    }
                }

                if results.is_empty() {
                    None
                } else {
                    let doc = results[0].clone();
                    Some((Ok(doc), Phase::Yielding { results, index: 1 }))
                }
            }
            Phase::Yielding { results, index } => {
                if index < results.len() {
                    let doc = results[index].clone();
                    Some((
                        Ok(doc),
                        Phase::Yielding {
                            results,
                            index: index + 1,
                        },
                    ))
                } else {
                    None
                }
            }
        }
    }))
}

fn execute_pipeline_values(
    engine: &Engine,
    mut modules: Vec<CompiledModule>,
    mut current_docs: Vec<serde_json::Value>,
    sandbox: Option<WasmSandboxConfig>,
    inverse: bool,
) -> Result<Vec<serde_json::Value>> {
    if inverse {
        modules.reverse();
        for module in &mut modules {
            module.inverse = !module.inverse;
        }
    }

    for module in modules {
        current_docs = execute_batch_transform(
            engine,
            &module.module,
            current_docs,
            module.arguments,
            module.inverse,
            store_limits(&sandbox),
            &sandbox,
        )?;
    }

    Ok(current_docs)
}

fn store_limits(sandbox: &Option<WasmSandboxConfig>) -> Option<StoreLimits> {
    sandbox.as_ref().and_then(|sb| {
        sb.max_memory_bytes
            .map(|max| StoreLimitsBuilder::new().memory_size(max).build())
    })
}

fn json_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_store_creation() {
        let store = WasmTransformStore::new();
        assert!(store.is_ok());
    }

    #[test]
    fn test_wasm_store_with_sandbox() {
        let store = WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive()));
        assert!(store.is_ok());
    }

    #[test]
    fn shared_engine_epoch_ticker_interrupts_an_infinite_call() {
        // A module with one exported function whose body loops forever.
        const INFINITE_LOOP_WASM: &[u8] = &[
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
            0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type () -> ()
            0x03, 0x02, 0x01, 0x00, // one function of type 0
            0x07, 0x07, 0x01, 0x03, b'r', b'u', b'n', 0x00, 0x00, // export run
            0x0a, 0x09, 0x01, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b, // loop { br 0 }
        ];

        let mut sandbox = WasmSandboxConfig::restrictive();
        // Fuel is a final bound if epoch ticking regresses; the expected trap
        // is specifically the epoch interrupt.
        sandbox.fuel_budget = Some(50_000_000);
        sandbox.epoch_deadline_ticks = Some(1);
        sandbox.epoch_tick_period = Some(Duration::from_millis(2));
        let base = WasmTransformStore::with_sandbox(Some(sandbox)).unwrap();
        // Transaction-local stores share the base engine and its ticker.
        let local = base.empty_with_shared_engine();
        assert!(Engine::same(&base.engine, &local.engine));
        let module = Module::new(&local.engine, INFINITE_LOOP_WASM).unwrap();
        let ticker = Arc::downgrade(local.epoch_ticker.as_ref().unwrap());
        drop(base);

        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut store = wasmtime::Store::new(&local.engine, ());
            store.set_epoch_deadline(1);
            let result = wasmtime::Instance::new(&mut store, &module, &[]).and_then(|instance| {
                let func = instance
                    .get_func(&mut store, "run")
                    .expect("test module exports run");
                func.call(&mut store, &[], &mut [])
            });
            drop(store);
            drop(local);
            let _ = result_tx.send(result);
        });

        let result = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("epoch interruption must finish within the bounded test window");
        assert_eq!(
            result
                .unwrap_err()
                .downcast_ref::<wasmtime::Trap>()
                .copied(),
            Some(wasmtime::Trap::Interrupt)
        );
        assert!(
            ticker.upgrade().is_none(),
            "last local store stops the ticker"
        );
    }

    #[test]
    fn returned_stream_keeps_epoch_ticker_alive_until_dropped() {
        let store =
            WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive())).unwrap();
        let ticker = Arc::downgrade(store.epoch_ticker.as_ref().unwrap());
        let stream = execute_pipeline_stream(
            store.engine.clone(),
            Vec::new(),
            Box::pin(futures::stream::empty()),
            None,
            false,
            store.epoch_ticker.clone(),
        );

        drop(store);
        assert!(ticker.upgrade().is_some());
        drop(stream);
        assert!(ticker.upgrade().is_none());
    }

    #[test]
    fn test_invalid_lens_config() {
        let store = WasmTransformStore::new().unwrap();
        let config = LensConfig::new("v1", "v2", LensModule::default());

        let first_lens = config.lens().cloned().unwrap_or_default();
        let result = store.load_module(&first_lens);
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_relative() {
        let result = WasmTransformStore::validate_wasm_path("relative/path.wasm");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_traversal() {
        let result = WasmTransformStore::validate_wasm_path("/safe/../../etc/passwd.wasm");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_rejects_non_wasm_extension() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/module.so");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }

    #[test]
    fn test_validate_wasm_path_accepts_valid() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/transform.wasm");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_wasm_path_rejects_no_extension() {
        let result = WasmTransformStore::validate_wasm_path("/path/to/module");
        assert!(matches!(result, Err(Error::PathNotAllowed(_))));
    }
}
