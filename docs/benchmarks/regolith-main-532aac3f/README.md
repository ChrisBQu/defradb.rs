# Main benchmark evidence pack

Frozen main-run evidence for commit `532aac3fd06a61c319c2d5fbde84da42aab67f0d` (label `main-532aac3f`). This directory is relocatable and contains raw outputs, not executables. `provenance/RUN.json` and `provenance/BUILD.txt` are exact source copies; `provenance/RUN.portable.json` rewrites artifact paths to pack-relative paths and records the omitted executable location convention. The executable hashes and build command remain in the provenance files.

## Benchmark protocol

- Host: Apple M5 Max, macOS, arm64, 18 cores, 128 GiB RAM; disk store; immediate durability.
- Toolchain/profile: rustc 1.99.0 (`b940084d7`, 2026-09-28), `release-fast`; build tree was clean.
- Write workload: 1/4/16/64 writers × 256 operations × 3 repetitions, for each of `creates`, `shared_updates`, and `counters_indexes` (36 raw samples).
- Allocation workload: five separate count-only samples. No throughput timings are included in the allocation result.
- Load guards passed for all eight run commands. Guard JSON files and SHA-256 values are retained.
- Criterion selection: document creates at 4 and 16, branchable create at 14, and embedded cold startup with 10 collections.

## Main baseline medians

Values below are medians over three repetitions. The storage-conflict count comes from the storage transaction stats delta and is present in this run; the zeros shown are observed values, not missing-metric defaults. HTTP conflict and exhausted-request metrics preserve the legacy benchmark metric names; they are not relabeled as engine conflicts. Latency is descriptive.

| Workload | Writers | Commits/s | Storage conflicts/commit | HTTP conflicts metric | Exhausted requests | p50 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| creates | 1 | 251.217 | 0 | 0 | 0 | 4.056 | 6.120 |
| creates | 4 | 253.827 | 0 | 0 | 0 | 15.946 | 20.273 |
| creates | 16 | 255.890 | 0 | 0 | 0 | 61.953 | 80.591 |
| creates | 64 | 245.874 | 0 | 0 | 0 | 254.971 | 515.300 |
| shared_updates | 1 | 247.321 | 0 | 0 | 0 | 3.989 | 8.634 |
| shared_updates | 4 | 252.532 | 0 | 0.0107422 | 0 | 15.945 | 30.911 |
| shared_updates | 16 | 234.951 | 0 | 0.505859 | 1 | 25.736 | 799.758 |
| shared_updates | 64 | 241.604 | 0 | 1.82375 | 600 | 30.036 | 2847.352 |
| counters_indexes | 1 | 233.376 | 0 | 0 | 0 | 4.051 | 6.997 |
| counters_indexes | 4 | 250.750 | 0 | 0.0253906 | 0 | 15.964 | 48.130 |
| counters_indexes | 16 | 237.480 | 0 | 0.555664 | 0 | 26.867 | 792.877 |
| counters_indexes | 64 | 232.350 | 0 | 1.89018 | 566 | 32.081 | 2868.311 |

Allocation samples: 2994.7, 2993.5, 2989.6, 2984.6, 2981.7 document-create allocations/op; median **2989.6**.

## Criterion median point estimates

Raw `estimates.json` files are under `criterion/`; values below are their median point estimates in nanoseconds. The run manifest retains each source SHA-256.

| Benchmark | Median point estimate |
|---|---:|
| `document_create/4` | 22,326.805 ns |
| `document_create/16` | 77,180.264 ns |
| `document_create_branchable/14` | 29,464.611 ns |
| `startup/cold_open/embedded/10_collections` | 8,972,135.375 ns |

## Limits

This is the main baseline only. It does **not** establish that any candidate meets non-regression gates; candidate runs are not part of this pack. The shared-update aggregate mixes conflicting and non-conflicting field patterns and cannot prove the disjoint-field correctness target. The binaries are intentionally omitted, so executable identity is recorded by SHA-256 but cannot be re-executed from this pack alone. The raw `RUN.json` retains original absolute paths as provenance; use `RUN.portable.json` for the relocatable artifact index.

## Contents

- `commands/`: eight command logs, eight Family JSONL outputs, and the corresponding load-guard attempt JSON.
- `allocations/`: five raw allocation samples, consolidated output, raw output, and the recorded checksum.
- `criterion/`: four exact Criterion `estimates.json` files used by the run manifest.
- `provenance/`: exact `RUN.json`, exact `BUILD.txt`, and path-normalized `RUN.portable.json`.
- `SHA256SUMS`: checksum manifest for every other file in the pack.
