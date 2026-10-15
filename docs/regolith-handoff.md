# Regolith adoption handoff

[PR #1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) is Theo's permanent review entry point. **The actual top of the stack is [#1961](https://github.com/sourcenetwork/defradb.rs/pull/1961). All eight adoption steps are represented by 11 draft PRs, but implementation and validation gates remain open. No PR has been merged.**

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

The stack now has the intended 11 draft PRs. Split into two stacks if further necessary splits exceed 11. Do not merge superseded generated implementations (#1897/#1899/#1911); their closure remains pending completion of this adoption.

## Current implementation and remaining work

| Step | Published implementation | Remaining work |
|---|---|---|
| 1 | Pinned API, engine encryption, classifier/DefraLevel in three PRs | Final stack validation and performance gates |
| 2 | Transactional decision reads, local ACP, relationship/NAC/downsample decisions | Preserve these fences through batch integration |
| 3 | Arrival append, ID allocation, field/counter/vector/full-text operators, head markers, read-only lenses and known-field write projections | General query/output and conditional reads remain conservative; no blanket zero-conflict claim |
| 4 | Receipt-ordered events and fallible ordered ACP hooks | Preserve winner-only hooks and suppress events on hook failure in whole-batch retries |
| 5 | Engine retries for individual/many writes, merges, lenses, backfill and pending DAG writes; bounded prepared inputs | Whole-batch conversion, bearer propagation and target/prepared-key separation published at `56ea4db1a`; removal of generic HTTP/embedded conflict replay and single-mutation engine-batch routing in progress |
| 6 | Requested locks/cache/forced-read APIs removed; snapshot metadata, collection OCC, immutable ACP projections and peer generation fences replace them | Generation/incarnation admission, registration, failure, ACK and status fences validated; lock-free ACP projection validated. Exact-wire schema reuse optimization and performance gates pending |
| 7 | Native commit/policy tickers and bounded conflict reasons; caller instrumentation retained | Revalidate after lower-stack restack |
| 8 | Obsolete cache/counter/queue proofs retired; HeadSet retained; pinned arrival proof and new adapter boundary models | Revalidate after lower-stack restack; models do not verify Rust or remote authorization |

### Immediate blockers

1. Finish removing generic HTTP/embedded business-conflict replay above engine retries. Typed external-input resolution remains outside attempts. Route eligible single implicit mutations through the engine batch so conflict retries reselect their targets. This is active implementation work.
2. Evaluate exact persisted-definition-byte reuse without dropping snapshot reads, runtime root-ID checks or current index-action statuses. The measured create allocation count remains above main.
3. Revalidate and restack incoming changes, inspect current-head CI, and complete §5.3 performance gates. All nine repaired lock-removal checkpoints now build; their timing gates remain pending. No performance gate is waived.

## Validation evidence

| Revision / scope | Result | Limit |
|---|---|---|
| Combined checkpoint `9afeb8447` | 2,360 DB/query/authorization/data tests pass, 33 ignored; storage 257, P2P 395, adapter 94 pass | Predates lock-free projection and pending front-door cleanup |
| Fresh CLI at `9afeb8447` | External ACP 204 pass/16 ignored, basic 56, encryption 25; counter conformance 9 pass/8 ignored; real point-read write-skew regression passes | Exact Go pin `53f0e76a32d1761e3396a88ce607ebc3e37baa5a`; later code needs targeted reruns |
| Projection checkpoint `2077620ca` and style-only successor `d19f72a57` | Five focused projection tests, query library 598 and DB write 63 pass; WASM lint and broad native all-target Clippy pass | Pending retry-surface/schema optimizations not covered |
| Published step 5 `56ea4db1a` | Latest inspection has no failed CI checks; Go compatibility and browser checks pass | Native ACP/macOS jobs were running or queued; inspect live checks before merge |
| Step 8 current proof suite | All 162 currently configured TLC cases have expected verdicts verified across full-suite and focused runs; five library/registry and two Lean contract checks pass | Finite boundary models, not Rust verification |
| Repaired step-6 removal history | All nine checkpoint benchmark builds pass; final-tree-preserving history repair complete | Fresh timing measurements pending |

P2P validation includes admission backoff, exact marker generations, registration ordering, forget/re-add incarnation fencing, stale failure refresh, atomic ACK cleanup and conditional status transitions. Already-admitted external I/O cannot be revoked; durable side effects are fenced. Old SSI/SSI-range proofs referencing the removed ConflictTracker have been retired, while the real write-skew behavior test remains.

Earlier mixed Go/Rust parity failures passed all three local controls and ten mixed repeats; the fresh step-5 Go compatibility CI check also passes. Use current-head CI: cancelled or superseded rollups do not establish the final stack's status.

## Performance gates: not passed

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

Fresh frozen step-3/4/5 binaries yield create medians **2980.7 / 2982.8 / 3411.4**, respectively (get-by-ID 269 at all three). The first two are slightly below the measured main create median; the step-5 increase remains under investigation. Exact raw samples, revisions and executable hashes are in the paired allocation artifact. All nine repaired lock-removal checkpoint binaries have built successfully; timing awaits a quiet-host window.

[Sharing immutable create inputs](benchmarks/create-retry-sharing.md) reduces the next exact paired create median from 3296.4 to **3231.3**, with get-by-ID unchanged at **160**. This remains about 8% above main for creates. A snapshot-safe schema-byte reuse candidate is being evaluated.

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
