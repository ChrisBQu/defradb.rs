# Complete Regolith adoption measurements

**All 22 checkpoints are measured; the performance gates have not passed.** [Results](results.md) and [machine-readable summary](results.json) report measured regressions separately from evidence validity. Every checkpoint has 36 workload samples and eight completed commands: 792 samples and 176 command runs total, with no missing comparisons. Rejected load-guard attempts are retained separately.

The current-main baseline is `8a38e96f213cd9edc55c89eed272ba8f83c76524`. The frozen final-stack executable was built at `051fd5fc3d794844d083feba219c8d5e2b0eb710`; its default benchmark compilation is equivalent to the published runtime at `f7813d07f2f16eefb6ebc994ff09fc08b63e0742`. Later schema-test/documentation changes do not enter these targets, and the native Vera lifecycle fix is outside their dependency graph. This is compilation-scope equivalence, not whole-tree identity. Exact build revisions, executable hashes, path differences, and dependency evidence are in the support archive.

## Protocol and limits

- Apple M5 Max, macOS arm64, 18 cores, 128 GiB RAM; on-disk store with Immediate durability; `release-fast`. Exact toolchain/build commands are in each BUILD manifest.
- Creates, shared updates, and counters/indexes: 1/4/16/64 writers, 256 operations per writer, three repetitions. Every successful request's persisted result is checked. Exhausted requests are counted as failures, never commits.
- Three create Criterion benchmarks and cold startup with ten collections; five allocation samples per checkpoint.
- Every measured command began with a passing one-minute load guard (ceiling 9 on this host). Repeated high-load refusals after contention workloads caused cooldown and resume, without rerunning completed samples. This does not establish that the host was idle throughout every sample. Post-run process snapshots cannot uniquely attribute load between benchmark residue and other host processes.
- The runner was paused while step1b's workload finished for native ACP diagnosis. Its command wall time includes the pause; internal workload samples do not. The pause and resumption are preserved in provenance.
- Comparisons use strict medians/point estimates with no tolerance or statistical-significance claim. Request latency includes successes and failures. Legacy HTTP and current engine counters retain their original meanings; absent layers are not fabricated as zero.
- Shared updates alternate left/right fields across eight documents, including same-field contention. Aggregate results do not prove the separate disjoint-field target; focused correctness regressions provide that evidence.

## Main findings

| Metric | Current main | Final published runtime |
|---|---:|---:|
| 64-writer creates, commits/sec | 247.019 | 7,366.801 |
| 64-writer shared updates, commits/sec | 242.428 | 493.410 |
| 64-writer counters/indexes, commits/sec | 232.505 | 299.086 |
| Shared-update exhausted requests / 16,384 | 577 (legacy HTTP) | 4,775 (engine) |
| Counter/index exhausted requests / 16,384 | 543 (legacy HTTP) | 8,434 (engine) |
| Allocations/create, five-run median | 2,991.135 | 3,133.550 |
| Create/4 Criterion median, microseconds | 22.588 | 37.770 |
| Create/16 Criterion median, microseconds | 77.405 | 100.359 |
| Branchable/14 Criterion median, microseconds | 29.817 | 45.965 |
| Cold startup/10 collections, milliseconds | 8.561 | 8.504 |

Creates have zero observed conflicts in the final runtime. Strict workload comparisons still have eight failing cells, and three create Criterion comparisons and allocations fail against main. Earlier checkpoints have their own failures, including ID-reservation conflicts before engine allocation. None are waived.

The optional `3a90482b2` create-validation optimization is **not adopted**. It reduces allocations to 3,111.540 and improves three create Criterion point estimates against the final runtime, but fails the strict throughput comparison and regresses cold startup. It does not address shared-update or counter contention.

The [retry-policy investigation](retry-policy-findings.md) identifies four immediate conflict-budgeted engine attempts versus up to six historical HTTP executions with jitter. That is a concrete policy difference and a follow-up experiment, not a demonstrated cause of the observed exhaustion increase. Preserve engine-owned retries and dependency correctness when investigating it.

## Evidence and reproduction

[index.json](index.json) records SHA-256 and size for every archive and member. The 22 per-label archives contain exact RUN/BUILD provenance, raw command logs, Family JSONL, Criterion samples/estimates, allocation samples, and guard attempts. `support-and-analysis.tar.gz` contains the complete final analysis, incremental reports, build/run/analyzer scripts, source-equivalence mappings, dependency graph, and pause provenance. The old `532aac3f` baseline is explicitly historical and is not counted among the 22.

Extract the archives into one empty directory. `PORTABLE-EVIDENCE-README.md` in the support archive explains checksum verification and raw-median inspection; each label has a `RUN.portable.json` artifact index. Original manifests are unchanged. Executables are omitted: their hashes record identity but cannot be re-executed from the measurement pack alone, and a rebuild at different paths need not have identical bytes.

`source-revisions.bundle` preserves the exact frozen and published commits; `source-revisions.json` lists refs, prerequisite, size, and checksum. From a clone containing prerequisite main `8a38e96f2`, verify and import it with:

```sh
git bundle verify /path/to/source-revisions.bundle
git fetch /path/to/source-revisions.bundle 'refs/regolith-evidence/20261015/*:refs/remotes/regolith-evidence/*'
```

Use a separate clean worktree at the desired recorded revision and the archived build command to reproduce a run. Re-run the load guard and preserve the new machine/toolchain provenance; new timings are separate evidence, not a replacement for these samples.
