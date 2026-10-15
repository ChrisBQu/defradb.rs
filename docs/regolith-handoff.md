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
| 6 | 3 mechanisms, in progress | [#1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) | `defralevel/03-engine-mechanisms` | `defralevel/02-transaction-decisions` |
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
| 5 | Engine retries for individual/many writes, merges, lenses, backfill and pending DAG writes; bounded prepared inputs | Whole implicit GraphQL batch conversion, request bearer propagation and target/prepared-key separation published at `438cffd57`; final combined CI/performance pending |
| 6 | Requested locks/cache/forced-read APIs removed; snapshot metadata, collection OCC, immutable ACP projections and peer generation fences replace them | Final batch projection, global backfill fence and safe allocation changes integrated locally at `0dc247b6c`; final combined validation and publication pending |
| 7 | Native commit/policy tickers and bounded conflict reasons; caller instrumentation retained | Revalidate after lower-stack restack |
| 8 | Obsolete cache/counter/queue proofs retired; HeadSet retained; pinned arrival proof and new adapter boundary models | Revalidate after lower-stack restack; models do not verify Rust or remote authorization |

### Immediate blockers

1. Finish combined validation. Plain update/upsert target membership is now tracked independently from optional key artifacts (`438cffd57`), fixing the DateTime/browser-relay preparation failure. Indexed/nonindexed DateTime tests and the hidden-target Upsert regression pass. Dedicated mixed-target coverage is being added. CI's mixed Go/Rust counter parity failure passed all three local runtime controls and ten additional mixed repeats, but remains unresolved until fresh CI.
2. Publish the combined step-6 repair at `0dc247b6c`. This includes the namespaced global allocator backfill cutoff, snapshot metadata reload, safe read/decode allocation changes, batch ACP projection and captured bearer contexts. Native all-target and WASM lint pass; exact deferred-ACP-removal boundary `e6d44f8f1` compiles. Steps 7/8 have been restacked locally. Broader P2P library tests exposed stale boolean durability senders after the generation-token API change; a step-6 test repair is in progress.
3. Run final combined native/WASM, external interoperability, counter conformance and fresh CI after restacking. Checkpoint passes below are not final-tip passes.
4. Complete §5.3 performance and allocation gates. Existing measurements show regressions; none is waived. Prepare isolated benchmark binaries, then measure with a fresh load guard and no concurrent builds/tests.

## Validation evidence

| Revision / scope | Result | Limit |
|---|---|---|
| Published step 3 `230c36c1`, prep `199daa57` | All reported CI checks pass at latest inspection | Does not cover later PRs |
| Published step 4 `3c8bed28` | All reported CI checks pass at inspection | Check live CI |
| Step 5 before new whole-batch work | Native DB/storage/P2P and WASM Clippy pass; read/search/transaction/vector/view/write and merge checkpoints pass | Needs final combined rerun |
| Step 5 `d761f2fe` | ACP 204 pass/16 ignored, basic 56 pass, encryption 25 pass, counter 9 pass/8 ignored; mixed runtime parity controls 3 pass plus ten mixed repeats | Predates target-set fix |
| Step 5 `438cffd57` / combined tip `731a90a5c` | Native all-target step-5 lint and combined-tip WASM lint pass; DateTime and hidden-target Upsert regressions pass | Final native/external suites and fresh CI pending |
| Step 6 `0dc247b6c` | Native all-target and WASM Clippy pass; remote ACP winner-only conflict regression and three bearer-hook tests pass | Full combined tests running |
| Independent query provider-boundary candidate | 744 query tests pass | Predates final combined batch/projection changes |
| Step 6 before latest repairs | 679 DB tail tests pass (14 ignored); 1275 query/storage/authorization tests pass | Later race repairs and batch changes require rerun |
| Step 6 external CLI | 204 ACP pass (16 ignored), 55 basic pass, 25 encryption pass; version-JSON smoke passes with clean DEFRA_ROOTDIR | Uses exact Go pin `53f0e76a32d1761e3396a88ce607ebc3e37baa5a`; predates final batch changes |
| Step 6 counter conformance | 9 pass, 8 diagnostic/parity cases ignored | Predates final batch changes |
| Snapshot metadata repair `36ac4cc8` | 59 index tests, four schema/OCC tests, native/WASM lint pass | Subsequent deterministic fence tests found and repaired additional races |
| Global fence candidate `81e46176` | 61 index tests and schema-comparison unit test pass; fmt/diff checks pass | Local candidate, not published tip |
| Step 7 | Three telemetry regressions and native/WASM Clippy pass | Revalidate after restack |
| Step 8 | All 163 TLC verdicts match expectation; Lean and three registry checks pass | Boundary models, not Rust verification |

Step-5 CI exposed an unused telemetry dependency (removed locally at `0f219851`, cargo-machete passes) and concurrent HTTP retry-test counter interference (serialized in the batch candidate, focused tests pass). Lower middle PRs run current lint/feature checks while full suites may be skipped by stack-scope policy. Historical cancelled runs may remain in rollups; use current head checks.

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

Required timing evidence: before/after each step and lock-removal checkpoint, all writer counts/workloads/repetitions, commits/s, conflicts/commit, retries by layer, p50/p99, create benchmarks at 4/16 fields, branchable create at 14, and cold startup at 10 collections. Use `tools/perf-site/loadguard.py`, record its result and source revision, and do not treat a failed/missing guard as clean-host evidence.

## Review constraints and intentional limits

- Accepted D39/D56 supersede the earlier synchronous-trait wording: retain async data paths and `transact_async`, using the existing native blocking bridge until step 9. Scoped attempt I/O cannot commit/discard independently; retained views fail closed before the real engine transaction is returned.
- Every retry attempt gets fresh reads, writes, projections and hooks. Prepared inputs are request-scoped, exactly keyed, bounded to 64 entries / 8 MiB / eight misses, and debug-redacted. A typed input miss exits the attempt; external providers resolve it outside the callback. IDs, DEKs, ciphertext and counter nonces stay stable across replay.
- Interactive transactions return conflicts to their caller. Ordinary ordered hooks and retry-only once hooks remain distinct. Ordered ACP failure suppresses events; cancellation after durability can still lose nondurable delivery.
- Writable projections include changed fields, all components of touched indexes, immutable checks, downsample source dependencies and embedding sources. Unknown/conditional changes, structural replacements, lens migrations and general query reads retain conservative validation. Counter reads remain validated because document/index mirrors depend on them.
- Collection/index metadata is reloaded from the attempt snapshot. Only index-list drift is accepted; schema drift fails closed. Index backfill requires a global cutoff after publication, with restart-safe no-progress handling.
- Remote authorization, dynamic KMS scope and browser retry execution are not formally verified. The browser queue driver has compile/lint coverage only. Proof models are explicit finite boundary abstractions, not a proof of the Rust adapter.

Prior progress notes and superseded measurements remain available in this file's Git history. This document and PR #1953 should be updated together after the final restack, replacing pending items with exact results or explicit unresolved gaps.
