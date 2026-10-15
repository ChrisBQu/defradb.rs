//! The engine's contract: one engine and one clock shared by every clone, a
//! clock that interrupts a call past its deadline, and a clock that stops with
//! the last clone.

use std::time::Duration;

use wasmtime::Module;

use super::{LensEngine, WasmSandboxConfig};
use crate::wasm_runtime::execute_batch_transform;
use crate::Error;

const IDENTITY_PASSTHROUGH_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
    0x01, 0x0a, 0x02, 0x60, 0x00, 0x01, 0x7f, 0x60, 0x01, 0x7f, 0x01, 0x7f, // types
    0x02, 0x0d, 0x01, 0x04, b'l', b'e', b'n', b's', 0x04, b'n', b'e', b'x', b't', 0x00,
    0x00, // import lens.next
    0x03, 0x03, 0x02, 0x01, 0x00, // alloc and transform functions
    0x05, 0x03, 0x01, 0x00, 0x01, // one-page memory
    0x07, 0x1e, 0x03, 0x06, b'm', b'e', b'm', b'o', b'r', b'y', 0x02, 0x00, 0x05, b'a', b'l',
    b'l', b'o', b'c', 0x00, 0x01, 0x09, b't', b'r', b'a', b'n', b's', b'f', b'o', b'r', b'm',
    0x00, 0x02, // exports
    0x0a, 0x0c, 0x02, 0x05, 0x00, 0x41, 0x80, 0x08, 0x0b, 0x04, 0x00, 0x10, 0x00,
    0x0b,
    // alloc returns the same in-bounds scratch offset; transform returns the next input
    // record. This exercises the real host JSON marshalling and result decoding path.
];

const INFINITE_LOOP_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
    0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type () -> ()
    0x03, 0x02, 0x01, 0x00, // one function of type 0
    0x07, 0x07, 0x01, 0x03, b'r', b'u', b'n', 0x00, 0x00, // export run
    0x0a, 0x09, 0x01, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b, // loop { br 0 }
];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

#[test]
fn a_shared_engine_interrupts_an_infinite_call_and_stops_its_clock_with_the_last_clone() {
    let mut sandbox = WasmSandboxConfig::restrictive();
    // Fuel is a final bound if epoch ticking regresses; the expected trap is
    // specifically the epoch interrupt.
    sandbox.fuel_budget = Some(50_000_000);
    sandbox.epoch_deadline_ticks = Some(1);
    sandbox.epoch_tick_period = Some(Duration::from_millis(2));
    let base = LensEngine::new(Some(sandbox)).unwrap();
    let shared = base.clone();
    assert!(wasmtime::Engine::same(base.engine(), shared.engine()));
    let module = Module::new(shared.engine(), INFINITE_LOOP_WASM).unwrap();
    let clock = shared.clock_handle().unwrap();
    drop(base);

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let result = shared.store(()).map(|mut store| {
            wasmtime::Instance::new(&mut store, &module, &[]).and_then(|instance| {
                let func = instance
                    .get_func(&mut store, "run")
                    .expect("test module exports run");
                func.call(&mut store, &[], &mut [])
            })
        });
        drop(shared);
        let _ = result_tx.send(result);
    });

    let result = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("epoch interruption must finish within the bounded test window")
        .expect("the sandboxed store is built");
    assert_eq!(
        result
            .unwrap_err()
            .downcast_ref::<wasmtime::Trap>()
            .copied(),
        Some(wasmtime::Trap::Interrupt)
    );
    assert!(clock.upgrade().is_none(), "the last clone stops the clock");
}

#[test]
fn an_engine_without_a_sandbox_runs_no_clock() {
    let engine = LensEngine::new(None).unwrap();
    assert!(engine.clock_handle().is_none());
    assert!(engine.store(()).is_ok());
}

#[test]
fn an_epoch_deadline_needs_a_positive_tick_period() {
    let mut sandbox = WasmSandboxConfig::restrictive();
    sandbox.epoch_tick_period = Some(Duration::ZERO);
    assert!(matches!(
        LensEngine::new(Some(sandbox.clone())),
        Err(Error::InvalidConfig(_))
    ));
    sandbox.epoch_tick_period = None;
    assert!(matches!(
        LensEngine::new(Some(sandbox)),
        Err(Error::InvalidConfig(_))
    ));
}

/// Manual performance probe for choosing the epoch ticker period. Run with:
/// `cargo test -p lens measure_epoch_ticker_periods -- --ignored --nocapture`.
/// `DEFRA_LENS_TICKER_SAMPLES` and `DEFRA_LENS_TICKER_LIGHT_MS` shorten or
/// lengthen the run. It reports wall latency only; portable Rust does not
/// expose reliable thread CPU-time or context-switch counters here.
#[test]
#[ignore = "manual epoch ticker timing probe; run on an otherwise idle host"]
fn measure_epoch_ticker_periods() {
    let samples = env_usize("DEFRA_LENS_TICKER_SAMPLES", 20);
    let light_sample_ms = env_usize("DEFRA_LENS_TICKER_LIGHT_MS", 2_000);
    let light_fuel_budget = env_u64("DEFRA_LENS_TICKER_FUEL", u64::MAX);
    let docs_per_batch = 16;
    let periods_ms = [10, 25, 50, 100, 250];
    let input_docs = vec![
        serde_json::json!({"id": "doc-1", "name": "Ada", "count": 7}),
        serde_json::json!({"id": "doc-2", "name": "Grace", "count": 11}),
    ];
    let module_bytes = IDENTITY_PASSTHROUGH_WASM;

    println!("epoch ticker probe: profile={}, samples={samples}, light_ms/sample={light_sample_ms}, docs/batch={docs_per_batch}, light_fuel={light_fuel_budget}, trap_fuel=u64::MAX, epoch_deadline_ticks=2", if cfg!(debug_assertions) { "debug" } else { "release" });
    println!("period_ms,light_median_ns_per_doc,light_p95_ns_per_doc,light_epoch_traps,trap_median_ms,trap_p95_ms,trap_min_ms,trap_max_ms");
    println!("CPU time/context switches: not measured by this portable harness; optionally wrap the test process in platform-specific perf tooling.");

    for period_ms in periods_ms {
        let period = Duration::from_millis(period_ms);
        let mut sandbox = WasmSandboxConfig::restrictive();
        // Use the actual two-tick deadline for ordinary work too. The
        // light probe runs long enough to span many timer periods and
        // reports any accidental interruption. Fuel is only a distant
        // safety bound so it does not mask epoch timing.
        sandbox.fuel_budget = Some(light_fuel_budget);
        sandbox.epoch_deadline_ticks = Some(2);
        sandbox.epoch_tick_period = Some(period);
        let engine = LensEngine::new(Some(sandbox)).unwrap();
        let module = Module::new(engine.engine(), module_bytes).unwrap();

        // Warm module instantiation, host marshalling, and the timer before sampling.
        for _ in 0..5 {
            let result =
                execute_batch_transform(&engine, &module, input_docs.clone(), None, false)
                    .unwrap();
            assert_eq!(result, input_docs);
        }

        let mut light_ns_per_doc = Vec::with_capacity(samples);
        let mut light_epoch_traps = 0usize;
        let mut light_traps_per_sample = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = std::time::Instant::now();
            let mut doc_count = 0usize;
            let mut sample_traps = 0usize;
            while started.elapsed() < Duration::from_millis(light_sample_ms as u64) {
                let docs = (0..docs_per_batch)
                    .map(|index| input_docs[index % input_docs.len()].clone())
                    .collect();
                match execute_batch_transform(&engine, &module, docs, None, false) {
                    Ok(result) => doc_count += result.len(),
                    Err(error) if error.to_string().to_lowercase().contains("interrupt") => {
                        light_epoch_traps += 1;
                        sample_traps += 1;
                    }
                    Err(error) => panic!("light transform failed unexpectedly: {error}"),
                }
            }
            light_ns_per_doc
                .push(started.elapsed().as_nanos() as f64 / doc_count.max(1) as f64);
            light_traps_per_sample.push(sample_traps);
        }
        let raw_light_ns_per_doc = light_ns_per_doc.clone();
        light_ns_per_doc.sort_by(f64::total_cmp);

        let module = Module::new(engine.engine(), INFINITE_LOOP_WASM).unwrap();
        let mut trap_ms = Vec::with_capacity(samples);
        for _ in 0..samples {
            let engine = engine.clone();
            let module = module.clone();
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            std::thread::spawn(move || {
                // The sandbox's own fuel (unbounded unless the probe's
                // variable sets it) and its two-tick deadline.
                let mut store = engine.store(()).unwrap();
                let instance = wasmtime::Instance::new(&mut store, &module, &[])
                    .expect("infinite loop module instantiates");
                let function = instance
                    .get_func(&mut store, "run")
                    .expect("infinite loop module exports run");
                let started = std::time::Instant::now();
                let result = function.call(&mut store, &[], &mut []);
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                let epoch_interrupt = result.err().is_some_and(|error| {
                    error
                        .downcast_ref::<wasmtime::Trap>()
                        .is_some_and(|trap| *trap == wasmtime::Trap::Interrupt)
                });
                let _ = sender.send((elapsed_ms, epoch_interrupt));
            });
            let completed = receiver
                .recv_timeout(Duration::from_secs(10))
                .expect("infinite transform must trap within the safety timeout");
            assert!(
                completed.1,
                "epoch deadline should interrupt before fuel fallback"
            );
            trap_ms.push(completed.0);
        }
        let raw_trap_ms = trap_ms.clone();
        trap_ms.sort_by(f64::total_cmp);

        println!(
            "{period_ms},{:.1},{:.1},{light_epoch_traps},{:.3},{:.3},{:.3},{:.3}",
            percentile(&light_ns_per_doc, 0.50),
            percentile(&light_ns_per_doc, 0.95),
            percentile(&trap_ms, 0.50),
            percentile(&trap_ms, 0.95),
            trap_ms[0],
            trap_ms[trap_ms.len() - 1],
        );
        for sample_index in 0..samples {
            println!(
                "raw_sample,{period_ms},{sample_index},{:.1},{},{:.3}",
                raw_light_ns_per_doc[sample_index],
                light_traps_per_sample[sample_index],
                raw_trap_ms[sample_index],
            );
        }
    }
}
