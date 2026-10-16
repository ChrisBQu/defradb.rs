# Lens epoch ticker period probe

This probe provides a local measurement basis for the restrictive sandbox's 100 ms epoch tick. It does not establish a universal lens latency SLO or show CPU savings: the portable harness measures wall time, not thread CPU time or context switches.

## Method

The ignored Rust test `wasm::tests::measure_epoch_ticker_periods` was run from the step-0 lens worktree at source revision `77f019fe8be3bc3b3f8e8d60c8ad5c0c3bcae016` with:

```sh
DEFRA_LENS_TICKER_SAMPLES=20 \
DEFRA_LENS_TICKER_LIGHT_MS=2000 \
CARGO_TARGET_DIR=/tmp/defra-kms-ci-target \
cargo test -p lens measure_epoch_ticker_periods -- --ignored --nocapture
```

The test binary SHA-256 was `c5028d821f42685e92fa221c1e1b60307aba8e66f45e6e7ecfdeb5ba41f9ba52`. It was built in the debug test profile. The host was an Apple M5 Max, arm64, macOS Darwin 25.5.0, with 18 reported cores. The pre-run load guard passed at one-minute load 5.31 against its 9.0 ceiling; its complete output is in `loadguard.json`.

For each period, the probe warmed up five calls, then collected 20 light samples of two seconds each. Each sample repeatedly called the real batch-transform host path with an identity WASM module, 16 small JSON documents per batch, a two-tick epoch deadline, and `u64::MAX` fuel. This exercises host JSON marshalling and result decoding, but not a representative complex lens. The test separately measured 20 infinite-loop trap latencies per period with two ticks and effectively unlimited fuel. The complete test output, including each raw light/trap sample, is in `raw-samples.log`.

## Results

| Tick period | Light median (ns/doc) | Light p95 (ns/doc) | Light epoch traps | Infinite-loop trap median | Trap p95 | Trap min–max |
|---:|---:|---:|---:|---:|---:|---:|
| 10 ms | 5,790.2 | 5,860.3 | 0 | 28.395 ms | 29.972 ms | 15.142–29.988 ms |
| 25 ms | 5,809.6 | 5,859.8 | 0 | 63.452 ms | 69.420 ms | 51.480–69.953 ms |
| 50 ms | 5,807.3 | 6,229.6 | 0 | 113.735 ms | 118.764 ms | 69.786–119.342 ms |
| 100 ms | 5,811.5 | 6,296.0 | 0 | 212.831 ms | 219.956 ms | 154.051–219.962 ms |
| 250 ms | 5,805.8 | 5,860.8 | 0 | 515.910 ms | 519.957 ms | 427.324–519.958 ms |

At 100 ms, the identity fixture's median was about 93 microseconds per 16-document batch, and no light call was interrupted over the 40 seconds sampled at that period. The two-tick infinite-loop trap was about 213 ms median and 220 ms p95. The light-call medians were similar across periods; the observed difference is within the limits of this debug-profile fixture and host.

These results support keeping 100 ms as a provisional default: it avoids the 100 wake-ups/second implied by 10 ms while the measured light call is far shorter than the roughly 200 ms two-tick interruption window. They do not prove the best setting, characterize larger or more complex transforms, quantify CPU/context-switch costs, or predict production builds. Fuel was set effectively unlimited to isolate epoch behavior; the probe does not compare the production 1,000,000-fuel limit. Run the ignored probe on the target workload and tune `WasmSandboxConfig::epoch_tick_period` when its latency and interrupt requirements differ.
