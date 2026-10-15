# Regolith adoption handoff

[PR #1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) is Theo's permanent review entry point. **The actual top of the stack is [#1961](https://github.com/sourcenetwork/defradb.rs/pull/1961). All eight adoption steps are represented by 11 PRs, but implementation and validation gates remain open. No PR has been merged.**

Scope: §5.2 steps 1–8 plus preparatory step 0 from Theo's bundle. New installations only; Jack waived migrations and compatibility with existing encrypted/value layouts. Step 9's worker-pool redesign is excluded. Main base: `532aac3f`. Regolith pin: `d1e36aa747913f6b1ecc14c821f4e5e6314674cf`.

## Stack and merge order

Merge from the bottom upward, retargeting the next PR after each merge. The branch name on #1920 predates its replacement implementation; its current content is engine encryption, not the old marker proposal.

| Order | Plan | PR | Branch | Base |
|---|---|---|---|---|
| 1 | Prep / step 0 | [#1922](https://github.com/sourcenetwork/defradb.rs/pull/1922) | `fix/step0-kms-writes` | `main` |
| 2 | 1a API bump, still RepeatableRead | [#1949](https://github.com/sourcenetwork/defradb.rs/pull/1949) | `chore/regolith-defralevel-api` | `fix/step0-kms-writes` |
| 3 | 1b engine encryption | [#1920](https://github.com/sourcenetwork/defradb.rs/pull/1920) | `defralevel/s0-change-markers` | `chore/regolith-defralevel-api` |
| 4 | 1c classifier / DefraLevel | [#1951](https://github.com/sourcenetwork/defradb.rs/pull/1951) | `defralevel/01c-key-classifier` | `defralevel/s0-change-markers` |
| 5 | 2 transactional decisions | [#1952](https://github.com/sourcenetwork/defradb.rs/pull/1952) | `defralevel/02-transaction-decisions` | `defralevel/01c-key-classifier` |
| 6 | 3 mechanisms | [#1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) | `defralevel/03-engine-mechanisms` | `defralevel/02-transaction-decisions` |
| 7 | 4 ordered events | [#1955](https://github.com/sourcenetwork/defradb.rs/pull/1955) | `defralevel/04-ordered-events` | `defralevel/03-engine-mechanisms` |
| 8 | 5 engine-owned retries | [#1956](https://github.com/sourcenetwork/defradb.rs/pull/1956) | `defralevel/05-engine-retries` | `defralevel/04-ordered-events` |
| 9 | 6 lock removal | [#1959](https://github.com/sourcenetwork/defradb.rs/pull/1959) | `defralevel/06-lock-removal` | `defralevel/05-engine-retries` |
| 10 | 7 engine telemetry | [#1960](https://github.com/sourcenetwork/defradb.rs/pull/1960) | `defralevel/07-engine-telemetry` | `defralevel/06-lock-removal` |
| 11 | 8 proof alignment | [#1961](https://github.com/sourcenetwork/defradb.rs/pull/1961) | `defralevel/08-proof-alignment` | `defralevel/07-engine-telemetry` |

The stack has the intended 11 PRs. All eleven PRs remain drafts while adoption gates are completed. No auto-merge is enabled on #1922. Split into two stacks if further necessary splits exceed 11. Do not merge superseded generated implementations (#1897/#1899/#1911); their closure remains pending completion of this adoption.

## Current implementation and remaining work

| Step | Published implementation | Remaining work |
|---|---|---|
| 1 | Pinned API, engine encryption, classifier/DefraLevel in three PRs | Final stack validation and performance gates |
| 2 | Transactional decision reads, local ACP, relationship/NAC/downsample decisions | Lower-checkpoint interoperability passes; final stack performance gates remain |
| 3 | Arrival append, ID allocation, field/counter/vector/full-text operators, head markers, read-only lenses and known-field write projections | General query/output and conditional reads remain conservative; no blanket zero-conflict claim |
| 4 | Receipt-ordered events and fallible ordered ACP hooks | Final CI/performance gates; winner-only hooks and suppression on hook failure are covered |
| 5 | Engine retries for individual/many writes, merges, lenses, backfill and pending DAG writes; bounded prepared inputs | Generic HTTP/embedded business-conflict replay removed; eligible single mutations now rerun selection inside engine batches. Final CI and performance gates remain |
| 6 | Requested locks/cache/forced-read APIs removed; snapshot metadata, collection OCC, immutable ACP projections and peer generation fences replace them | Generation/incarnation admission, registration, failure, ACK and status fences validated; lock-free ACP projection, exact-wire schema reuse and key-buffer sizing validated. Performance gates remain |
| 7 | Native commit/policy tickers and bounded conflict reasons; caller instrumentation retained | Current-head remote CI and performance gates |
| 8 | Obsolete cache/counter/queue proofs retired; HeadSet retained; pinned arrival proof and new adapter boundary models | Revalidate after lower-stack restack; models do not verify Rust or remote authorization |

### Immediate blockers

The §2.4 fixes are now placed at their proper stack boundaries. The restacked runtime at `cb5abaf99` passes **2,371 core tests (33 ignored), 358 HTTP/embedded tests (5 ignored), five benchmark-accounting tests, native all-target Clippy, WASM Clippy, and fresh CLI basic 56, ACP 204 (16 ignored), encryption 25, counter 9 (8 ignored), and write-skew protection**. Logs are `/tmp/defra-final-surface-cb5abaf99-*.log`. A subsequent history-only rebase changes no source. The later test-only change at `7efaf2f2c` strengthens physical-blob rollback assertions; all three statement-savepoint tests pass. These local results do not substitute for new-head remote CI.

The shared Wasmtime engine owns an independent parked epoch-clock thread so synchronous WASM cannot starve its clock. Transaction-local transform maps initialize lazily. The provisional **100 ms** period now has a [bounded measurement report](benchmarks/lens-epoch-period/measurement.md): no light-workload traps across the measured periods; the 100 ms setting gives approximately 213 ms median / 220 ms p95 interruption with two ticks. This is not an optimality claim or a complex-lens latency guarantee.

Statement savepoints enter at step 2 after the API bump supplies releasable savepoints; ordered queues enter their snapshots at step 4, retry bundles at step 5, and ACP projection state at step 6. Failed, timed-out, and cancelled explicit GraphQL mutation statements roll back staged writes and effects while retaining prior successful statements. The regression also verifies failed-statement blobs are absent and earlier successful blobs remain. Direct registry DDL calls recover pending GraphQL cleanup but do not receive independent operation savepoints. Finalization may consume the registry entry when cleanup fails; it cannot successfully commit partial statement writes.

Schema decision scans use the same write transaction with validation enabled; schema-head caches publish after successful commit. Legacy index representations are compared after normalization, while other schema fields remain checked. The stale-scan regression stages a write and checks the typed transaction conflict.

1. Inspect new-head CI after publication. Fresh CLI validation of all eight lower PR checkpoints is running independently: prep `921824bfa` and API `75238ac25` pass; remaining checkpoints are still being checked. Older lower-checkpoint results below are historical.
2. Complete §5.3 timing and allocation gates on a quiet host. The main baseline is complete; refreshed candidate and removal-checkpoint binaries are being built. No performance gate is waived. The historical allocation pair was about 4.6% above its main baseline; it is not a measurement of this restack.

The browser identity regression is fixed at step 5: identity is scoped per future poll and restored before suspension. CI at `1fb3ebed0` passes 73 browser library tests, two browser integration tests and four relay tests; six native identity tests also pass.

Step 6 now supports projected GraphQL updates for unconditional explicit document IDs, scalar patches and `_docID`-only output. Full documents are materialized, with only relevant field dependencies validated. Filters, other returned fields, migrations, downsample and unsupported shapes retain conservative reads. Preparation uses the same dependency set, preserves found-target membership and invokes preparation hooks; custom mutators default to full validation. Eight real GraphQL regressions pass, including disjoint commits with required untouched fields, same-field/predicate/output/index conflicts and later full reads. A custom-mutator regression confirms hooks run and missing IDs stay excluded. Query 599, DB library 29, write 64 and index 61 tests pass. At `b4a2552f7`, the full core run passes 2,365 tests (33 ignored), HTTP/embedded passes 358 (5 ignored), and benchmark accounting passes five. After an unused-import cleanup, `888e2da4c` passes broad native all-target Clippy, WASM Clippy, formatting, and a fresh CLI run: ACP 204 (16 ignored), basic 56, encryption 25, counter 9 (8 ignored), and write-skew one. The cleanup changes no runtime behavior. Current-head remote CI remains pending.

## Validation evidence

| Revision / scope | Result | Limit |
|---|---|---|
| Combined `f0d3fd4d5` | 2,364 DB/query/authorization/data tests pass, 33 ignored; HTTP/embedded tests 358 pass, 5 ignored; benchmark accounting tests 5 pass | Fresh CLI: ACP 204 pass/16 ignored, basic 56 pass, encryption 25 pass, counter 9 pass/8 ignored and write-skew 1 pass |
| Combined `f0d3fd4d5` | Broad native all-target Clippy includes CLI and benchmarks; WASM Clippy, cargo-machete, formatting and diff checks pass | Fresh remote CI remains required |
| Key capacity `2bb8dd9f5` | All 8 key-layout tests and storage all-target Clippy pass | Only buffer allocation changes after the full combined validation |
| Engine telemetry on combined stack | All 3 integration tests pass | Local native validation |
| Step 5 alone `427da1b4e` | Batch retry tests 4/4, HTTP context tests 7/7, DB/query/HTTP all-target Clippy pass after workspace-cache cleanup | Does not include step-6 remote projection |
| Earlier combined `9afeb8447` | Storage 257, P2P 395 and adapter 94 pass; external ACP 204 pass/16 ignored, basic 56, encryption 25; counter 9 pass/8 ignored; real point-read write-skew passes | Predates final retry-surface cleanup; exact Go pin `53f0e76a32d1761e3396a88ce607ebc3e37baa5a` |
| Lower checkpoints `7fc488f2`, `f39db77d`, `5ed6e2fa`, `82047ca2` | At each: fresh CLI build; basic 56, ACP 204 (16 ignored), encryption 25 and full counter 9 (8 ignored) pass | Exact Go pin `53f0e76a32d1761e3396a88ce607ebc3e37baa5a`; see binary hashes below |
| Remote CI inspection | #1922 and #1953 have 28 passing checks; #1955 has passing live checks; lower four active checks pass with explicit scope skips | New upper-stack CI remains required; cancelled prior attempts are excluded |
| Step 8 proof suite (source unchanged by restack) | All 167 configured TLC cases have expected verdicts verified across full-suite and focused runs (including five new statement-atomicity cases); five library/registry and two Lean contract checks pass | Finite boundary models, not Rust verification |
| Earlier repaired step-6 removal history | All nine checkpoint benchmark builds passed; queue benchmark removal and dependency cleanup moved to their removal boundaries | New restacked checkpoint builds and timing pending |

Lower-checkpoint CLI SHA-256 values (the first checkpoint's integration run was serial; later runs used four test threads):

| Revision | Saved CLI SHA-256 |
|---|---|
| `7fc488f21192` | `57865f5a57d16c8c56b317875fc24d4e47a346aeb48bd5174329787fdffa8f1d` |
| `f39db77d84e9` | `e5ec041aa61268c255becf35a9722021ce74d375ca7350e0884c3c554abdf480` |
| `5ed6e2fa8034` | `040b6925089f10647a4adce5c200b8917dd5c225626e6c985efa809f5bd2e692` |
| `82047ca26fd7` | `cd7854bae0f7d0b6f91ad1dceb296cd1c84eb28ccdd6039e696dface5e077e22` |

The single-update regression verifies that a concurrent change causes the engine retry to reselect targets instead of overwriting the winner. Prepared node permissions use the same acting identity as execution; authoritative local NAC checks remain inside the transaction. The local ACP revocation regression retains READ access and revokes UPDATE during the first attempt, then verifies an explicit denial and unchanged stored data. Read-denied documents are intentionally invisible and may produce an empty mutation result.

P2P validation covers admission backoff, exact marker generations, registration ordering, forget/re-add incarnation fencing, stale failure refresh, atomic ACK cleanup and conditional status transitions. Already-admitted external I/O cannot be revoked; durable side effects are fenced. Obsolete SSI/SSI-range proofs referencing the removed ConflictTracker are retired, while the real write-skew behavior test remains.

Earlier mixed Go/Rust parity failures passed all three local controls and ten mixed repeats; fresh step-5 Go compatibility CI also passed before this final restack. Use current-head runs, not cancelled or superseded rollups.

## Performance gates: not passed

The [frozen clean-host main evidence](benchmarks/regolith-main-532aac3f/README.md) includes all 36 workload samples, eight guarded command runs, five allocation samples (median **2989.575 allocations/create**), four Criterion estimates, executable hashes and checksums. Structural/provenance validation passes. This baseline does not establish candidate non-regression; the lower-stack fixes require refreshed candidate binaries and measurements.

The historical head-cache comparison is retained in [the raw artifact](benchmarks/regolith-head-cache-2026-10-14.json), before `c29a923b` and after `14e7264a`. Each run has 36 samples: creates/shared updates/counters+indexes, 1/4/16/64 writers, 256 operations per writer, three repetitions, release-fast and on-disk Immediate durability. Ambient host load contaminated timing. Creates had zero conflicts but did not scale consistently; four-writer shared/mixed conflicts increased, and 16-writer mixed exhaustion rose from zero to four. Higher-contention improvements do not waive these failures.

Five-run allocation counts and binary hashes are in [the checkpoint artifact](benchmarks/allocation-adoption-checkpoints.md). True medians (allocations per operation):

| Checkpoint | Create | Get by ID |
|---|---:|---:|
| Main `532aac3f` | 2991.5 | 165 |
| Step 0 `199daa57` | 3002.3 | 165 |
| Step 1 `5ed6e2fa` | 3025.0 | 165 |
| Step 2 `82047ca2` | 3049.7 | 169 |
| Step 3 `230c36c1` | 3622.3 | 269 |
| Step 4 `3c8bed28` | 3624.3 | 269 |
| Step 6 measured candidate `3bd4fdcd` | 4234.4 | 198 |

The largest early increase is at step 3. Safe read-only fetching and one-pass decoding reduce read allocations while preserving writable field-validation dependencies. These counts do not establish throughput. No fresh per-step or per-removal throughput gate is claimed.

[Paired allocation results](benchmarks/allocation-sort-and-schema.md): CID-string sort caching reduces create allocations from 4237.8 to 3588.4 on its exact baseline pair. On a separate exact pair, immutable schema sharing reduces 3612.3 to 3346.4 and get-by-ID 198 to 160. Downsample filtering before cloning reduces creates to 3307.4; reserving computed-create buffers reduces them further to 3296.3. Block ordering tests (61) and collection/definition/index suites (232) pass. These counts do not establish final-tip or throughput gates.

Historical frozen step-3/4/5 binaries yielded create medians **2980.7 / 2982.8 / 3411.4**, respectively (get-by-ID 269 at all three). The first two are slightly below the measured main create median; the step-5 increase remains under investigation. Exact raw samples, revisions and executable hashes are in the paired allocation artifact. The earlier nine repaired lock-removal checkpoint binaries built successfully; the new restack requires refreshed binaries and timings.

[Sharing immutable create inputs](benchmarks/create-retry-sharing.md) reduces the next exact paired create median from 3296.4 to **3231.3**, with get-by-ID unchanged at **160**. This remains about 8% above main for creates. The [schema-byte reuse pair](benchmarks/schema-wire-reuse.md) reduces its own five-run median from 3231.4 to 3189.3, with reads unchanged. That pair uses the default release profile; final release-fast gate measurements remain pending.

[The release-fast key-buffer pair](benchmarks/doc-id-index-key-capacity.md) confirms a baseline create median of 3189.3 and reduces it to **3130.4**, with get-by-ID falling from **160 to 157**. The new create count is still about **4.6% above main**; the allocation gate remains open. Exact key bytes are unchanged, with all eight layout tests and storage all-target Clippy passing.

The [gate runner and analyzer](../tools/regolith-gates/README.md) preserve source/executable hashes, all raw samples, failed and passed guard attempts, exact Criterion estimates and five allocation runs. They distinguish invalid evidence from observed regressions; historical #218 comparisons are descriptive because hardware/profile equivalence is unknown.

Required timing evidence: before/after each step and lock-removal checkpoint, all writer counts/workloads/repetitions, commits/s, conflicts/commit, retries by layer, p50/p99, create benchmarks at 4/16 fields, branchable create at 14, and cold startup at 10 collections. Use `tools/perf-site/loadguard.py`, record its result and source revision, and do not treat a failed/missing guard as clean-host evidence.

## Review constraints and intentional limits

- Accepted D39/D56 supersede the earlier synchronous-trait wording: retain async data paths and `transact_async`, using the existing native blocking bridge until step 9. Scoped attempt I/O cannot commit/discard independently; retained views fail closed before the real engine transaction is returned.
- Every retry attempt gets fresh reads, writes, projections and hooks. Prepared inputs are request-scoped, exactly keyed, bounded to 64 entries / 8 MiB / eight misses, and debug-redacted. A typed input miss exits the attempt; external providers resolve it outside the callback. IDs, DEKs, ciphertext and counter nonces stay stable across replay.
- Interactive transactions return conflicts to their caller. Ordinary ordered hooks and retry-only once hooks remain distinct. Ordered ACP failure suppresses events; cancellation after durability can still lose nondurable delivery.
- Writable projections include changed fields, all components of touched indexes, immutable checks, downsample source dependencies and embedding sources. Unknown/conditional changes, structural replacements, lens migrations and general query reads retain conservative validation. Counter reads remain validated because document/index mirrors depend on them.
- Collection/index metadata is reloaded from the attempt snapshot. Only index-list drift is accepted; schema drift fails closed. Index backfill requires a global cutoff after publication, with restart-safe no-progress handling.
- The P2P `pending_metadata_writer` remains: it coordinates multi-operation durable-cap transitions with in-memory root/scope caches. Engine retries cover individual pending-store transactions, not that whole protocol. It is outside §5.2 step 6's listed lock removals and remains a network-runtime follow-up; this stack does not claim to remove every P2P mutex.
- Remote authorization, dynamic KMS scope and browser retry execution are not formally verified. The browser queue driver has compile/lint coverage only. Proof models are explicit finite boundary abstractions, not a proof of the Rust adapter.

Prior progress notes and superseded measurements remain available in this file's Git history. This document and PR #1953 should be updated together after the final restack, replacing pending items with exact results or explicit unresolved gaps.
