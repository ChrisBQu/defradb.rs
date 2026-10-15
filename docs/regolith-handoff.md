# Regolith adoption handoff

This is the review entry point for Theo (vertexclique). [PR #1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) is the permanent handoff link: its description will point to the latest stack tip even when later steps add PRs above it. It follows §5.2 of the supplied `regolith_defralevel_docs` bundle. **The stack is incomplete and remains draft.** This file is updated as implementation and verification progress; an implemented item is not a claim that its PR is ready to merge.

Target: new installations. Jack explicitly waived compatibility with existing encrypted/value layouts and migrations. Regolith is pinned to `d1e36aa747913f6b1ecc14c821f4e5e6314674cf`. Worker-pool/runtime redesign (§5.2 step 9) is outside these eight adoption steps. No PR has been merged.

Latest published tip: [#1961 — proof alignment](https://github.com/sourcenetwork/defradb.rs/pull/1961). **All eight steps are represented by 11 draft PRs; implementation and gates are not complete.** Blocking work: whole implicit GraphQL batch engine retries (step 5), final combined validation, and performance/allocation gates. The step-6 index-metadata race is fixed at `36ac4cc8`. No PR has been merged.

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

## Implementation status

| Step | Current state |
|---|---|
| 1 | Published as three drafts: pinned API, engine encryption, classifier/DefraLevel. New installations only. |
| 2 | Published: transactional decision reads, local ACP, relationships, embedded NAC and downsample uniqueness. Continue auditing copied write paths during retry conversion. |
| 3 | Published: arrival append, ID allocation, field/counter/vector/full-text operators, head markers, read-only lenses and known-field write projections. General query/output and conditional reads remain conservative. |
| 4 | Published #1955: receipt-ordered events and fallible ordered ACP hooks. Delivery remains nondurable if cancellation occurs after commit. |
| 5 | Published #1956, incomplete: engine retries cover individual/many document writes, remote merges, eager lenses, backfill/publication and pending DAG writes. Exact prepared inputs and typed exhaustion are implemented. Whole implicit GraphQL batches (including transactional ACP) still need their complete engine-owned callback path and provider-boundary validation. Async traits follow accepted D39/D56; runtime redesign is step 9. |
| 6 | Published #1959, incomplete: requested locks/cache/forced-read APIs removed; collection OCC, immutable ACP/cache snapshots and peer generation fences replace them. Peer conflict transitions use engine retries; legacy migration is removed. Snapshot-correct index metadata refresh is integrated at `36ac4cc8`; all 59 index tests and four schema-conflict tests pass, with native/WASM lint. Performance gates remain open. |
| 7 | Published #1960: native commit/policy tickers and bounded conflict-reason listener preserve caller instrumentation, with duplicate adapter increments removed. Three regressions and native/WASM Clippy pass. |
| 8 | Published #1961: obsolete cache/counter/queue models retired, HeadSet retained, pinned engine arrival proof bound. New boundary models cover prepared inputs, marker generations and collection-version OCC. All 163 TLC verdicts, Lean build and registry checks pass; lower-stack fixes and gates still block readiness. |

### Step 3 decision-read audit (2026-10-14)

The remaining document conflict is not fixed by narrowing only the old-document read in
`Collection::update_with_indexes_parts`. The local autocommit path first reloads the document through
`LensedDocFetcher::get_by_ids` under its write guard; that calls `get_by_doc_id` and validates all fields in
`read_document_blob_and_version`. Ordinary query reads use the same full-document path. Both can still reject a
transaction after a concurrent disjoint field operand changes the blob.

A sound write projection must include every field the mutation actually consumes: changed fields, changed fields
participating in indexes, changed immutable fields, the downsample `source_doc_id` when it changes, and any embedding
source fields used to regenerate a vector. Index maintenance must skip indexes untouched by the mutation; otherwise
index maintenance unnecessarily depends on unchanged fields. Projected reads return the complete value bytes; only
the validation dependency set is narrowed. Structural puts,
schema-version transitions, migrations, and callers without an explicit modified-field set retain whole-value reads.
Query output reads require the selected fields plus predicate, ordering, join, and computed-field dependencies, so a
field dependency set must flow from query planning into the fetcher before query reads can be narrowed safely.

Counter reads remain validated. `ValueReader::value` is used to materialize counter values, and the document mirror
repair path derives an operand from the stored mirror. An advisory counter read could therefore commit a stale
document mirror and corrupt index/materialized query results even though the authoritative counter accumulator
itself merges correctly.

The implementation now narrows the explicit-modified-field collection update path and the autocommit mutation reload:
the reload requests changed fields, all fields of touched composite indexes, and all source fields used by embedding
regeneration. The old-document index path uses the same dependency set, skips untouched indexes, and only performs
series-key release/claim when `source_doc_id` changes. Conditional (`expected`) updates, general query/output reads,
unknown-modification callers, and lens migrations stay conservative. Counter reads stay validated. Validation passed:
`cargo check -p db`; after review corrections, all 58 write and all 58 index tests pass on the integrated stack, including the explicit
disjoint and same-indexed-field transaction regressions. This does not claim generic query projection or zero
incidental conflicts for explicit transactions that already performed a full document read. A stale composite-index
regression was verified RED with the dependency repair disabled (missing expected index row), then GREEN after
restoring it: untouched index components come from the transaction snapshot, not the caller’s stale document.

### Step 5 execution choice

The plan has an unresolved mismatch: §5.2 step 5 says to make data-path traits synchronous so retry closures can use
`transact`, while later decision D56 says the data path stays async on a pinned local executor. The pinned Regolith
API provides both `transact` and `transact_async` (D39). This stack follows the later D56 decision: keep the data path
async and use `transact_async` for retry-owned transactions. The synchronous-trait conversion in §5.2 is superseded
for this adoption. Retry integration must preserve engine ownership of the transaction and must not adapt an engine
borrow into a fake owning `Txn` or extend its lifetime unsafely. The current backend may drive a per-call Regolith
`IoQueue` through the existing `blocking()` bridge until step 9 replaces that execution boundary; it does not add a
queue worker or hold a shared transaction lock across the attempt. The sealed attempt I/O capability has no independent commit/discard lifecycle. Namespace views and iterators must be dropped before the real engine transaction is returned; retained references fail closed. Winning callbacks transfer separately to the engine adapter, and an operation error is returned without storage retry. Thus callers can return
domain outcomes such as `NeedsInput` to their resolver outside the transaction attempt.

### Step 5 integration validation

At `097f1708`, the integrated merge suite passes 216 tests. The write suite passed 62 tests before the final local-key regression was added; all ten KMS/signing tests then passed, including encrypted counter updates and whole-document encryption without KMS. Eighteen migration tests plus a new forced concurrent-write materialization regression pass. The full storage suite passes; later focused checks cover cold SST point/scans, normal-versus-retry receipt ordering, and independence from Tokio's cooperative budget. Native DB/storage/lens all-targets Clippy and defra-wasm Clippy pass at their recorded checkpoints. Full DB validation is running; these are not final-stack results.

The engine queue uses the existing native blocking bridge. Browser attempts instead poll the queue from the event loop and register completion/timer wakes. Transactional lens transforms run on the caller task rather than spawning a task on the suspended Tokio executor. The browser driver has compile/lint coverage; browser runtime execution remains unverified locally.

Published tip #1955 at `3c8bed28` has passing native unit shards, feature checks, lint, WASM, conformance, signed-docs, Go compatibility, Iroh and P2P CI. Native ACP consumer and quick benchmark were still running at the latest check. Historical failures below are retained as repair provenance, not the current status of that tip.

## Latest implementation checkpoints

- `c8146173`: document value integration. Local schema field CIDs remain unchanged; separately allocated immutable part IDs drive projected validation. Stored document format is `DFV\x01`; block wire CBOR stays canonical. Remote tombstones preserve winning LWW priority/raw bytes. A standalone counter block followed by a local increment repairs a stale blob with a validated put. Field heads keep field priorities rather than inheriting composite priorities.
- `9e146e42`: document head markers. `/d/` and `/ds/`, and collection `/c/` and `/cs/`, are exact commutative families. Pruning removes parent+markers atomically in a separate transaction. Ordinary `/r/` publication guards survive pruning: they prevent replay from resurrecting reclaimed heads and are separate from counter application markers. Truncation removes publication guards too. Late-parent and old-snapshot cases have regression tests.

## Validation and review limits

Latest step-3 CI repairs: FFI and public embedded stores now forward allocation and transaction statistics; `Store::allocate` is mandatory so wrappers cannot silently omit it. All 170 native minimal-feature FFI tests and the embedded suite pass. The vector-link proof family is registered in the coverage matrix; all five conformance library tests pass. Formatting passes.

The stack has been rebased onto main `532aac3f`. Its newly added DiskANN disk-cost test used the private Regolith `Options.statistics` field; step 1a now uses the public builder. This addresses the current tip CI lint/optional-feature compile failures. Fresh CI and validation of the combined main/stack changes are pending; earlier local test results predate this rebase.


Local checks passed at the document/head checkpoint and subsequent HNSW integration:

- Database block, merge, write and read integration suites; the read suite has one pre-existing ignored test. Earlier collection/definition/query suites passed during document and readonly-lens integration.
- New document-head tests: overlapping writers both commit; prune preserves live heads and old snapshots; a marker received before its parent survives; replay before/after prune cannot resurrect a parent.
- Remote tombstone and independent field/composite priority tests. The stale counter mirror regression was also run with its repair disabled and failed with 13 instead of 18, then passed with the repair restored.
- Native workspace all-targets Clippy and `defra-wasm` Clippy, warnings denied.
- HeadSet TLC GREEN passed (12 distinct states); the eager-delete RED failed `INV_NoWriteConflict`, as expected. Lean was not rerun at this checkpoint.
- HNSW: 227 vector tests passed, 12 measurements remain ignored; concurrent inserts preserve both shared back-links and metadata counts, stale structural pruning conflicts; encrypted reopen and put terminators pass. VectorLinks TLC GREEN passes; stale-put and unvalidated-prune REDs fail as expected. Native/WASM Clippy pass.
- Earlier storage operator contract/property tests cover malformed operands, integer exact folding, ordered float/stat operands, document touches/folding, encrypted reopen, and concurrent full-text updates.

**CI is not all green.** At the 2026-10-14 checkpoint, lower PRs have failing surface-test jobs; #1949 also has a failing build job and #1952 has signed-docs/Go-compat failures. Investigation found missing searchable-encryption artifact propagation in the step-2 batch path; the fix passes both affected Rust external-node tests locally. The surface-test accounting failure was traced to internal short-ID reservation retries omitted from the measurement; `e91ed833` adds their counters and benchmark accounting to step 0 and is carried through the published stack. All four benchmark verification tests pass locally. Go interoperability and fresh CI still need verification. Many macOS jobs and latest-tip checks are queued/running. Consult each PR's current checks rather than treating this snapshot as current CI status.

Local external-node encryption regressions `rust_rust_se_cross_node` and `merged_and_self_written_docs_both_hit_on_same_name` now pass after restoring searchable-encryption artifact propagation in the batch path. The fix is carried in step 2 and above. Go interoperability remains unverified locally. The remote document operand checkpoint passes all 216 merge tests and 55 write tests, plus DB all-targets Clippy with warnings denied. The full pre-head-cache-removal baseline has run (1/4/16/64 writers, 256 operations per writer, three repetitions, Immediate on-disk durability); the matching after run is complete; see the performance checkpoint below. Creates have zero conflicts at all counts; shared/mixed workloads still exhaust requests at 64 writers. Ambient host load is recorded, so this is not an isolated-machine measurement. No step-6 performance gate is yet satisfied. HNSW clustered-corpus recall@10 (N=2000, ef=64) is 0.9967 at d=16 and 0.9767 at d=128; these are corpus-specific gates, not a real-embedding workload claim. No zero-incidental-conflict or throughput improvement claim is made. Do not remove guards merely because the functional suites pass.

## Review focus / next work

1. Finish step 3's remaining decision-read audit. HNSW and remote document operands are implemented; retain validated pruning and structural replacements.
2. Narrow reads only where decision/index dependencies are known. In particular, do not make counter reads advisory while the document/index mirror still depends on them.
3. Investigate lower-stack CI failures and apply fixes at the earliest affected PR, carrying them upward without dropping newer work.
4. Step 4 is published as #1955. Finish steps 5–8 in order, updating this handoff and the top PR link after each checkpoint.
5. Verify all stack bases, final CI, external integration, proof gates and required benchmarks before marking any incomplete PR ready.

## Head-cache candidate performance checkpoint (2026-10-14)

Both complete runs contain 36 samples: three workloads, 1/4/16/64 writers, 256 operations per writer, three repetitions, release-fast, on-disk Immediate durability. Before is `c29a923b`; after is `14e7264a`. This isolates cache/API removal before step 5 retry integration. Raw metrics, including latency and retries, are in [the benchmark artifact](benchmarks/regolith-head-cache-2026-10-14.json). Builds/tests were held idle, but unrelated host CPU load remained.

**Gate not passed.** Creates remain conflict-free, but throughput does not consistently rise with writers. At four writers, shared/mixed HTTP conflicts per committed request increase. Mixed workload exhaustion rises from zero to four at 16 writers. Higher-contention throughput improves substantially, but that does not waive these failures. Storage conflicts are reported as zero by this adapter checkpoint; these measurements do not prove an absence of engine conflicts.

| Workload | Writers | Commits/s before → after (median) | HTTP conflicts/commit before → after (weighted) | Exhausted requests before → after (sum) |
|---|---:|---:|---:|---:|
| creates | 1 | 254.4 → 255.9 | 0.0000 → 0.0000 | 0 → 0 |
| creates | 4 | 267.2 → 259.3 | 0.0000 → 0.0000 | 0 → 0 |
| creates | 16 | 266.5 → 263.5 | 0.0000 → 0.0000 | 0 → 0 |
| creates | 64 | 247.9 → 260.3 | 0.0000 → 0.0000 | 0 → 0 |
| shared_updates | 1 | 135.5 → 228.8 | 0.0000 → 0.0000 | 0 → 0 |
| shared_updates | 4 | 261.2 → 461.2 | 0.0221 → 0.0479 | 0 → 0 |
| shared_updates | 16 | 244.9 → 635.4 | 0.5888 → 0.3582 | 2 → 0 |
| shared_updates | 64 | 209.5 → 567.5 | 2.0169 → 1.4233 | 2342 → 321 |
| counters_indexes | 1 | 225.2 → 228.6 | 0.0000 → 0.0000 | 0 → 0 |
| counters_indexes | 4 | 234.5 → 470.4 | 0.0199 → 0.0645 | 0 → 0 |
| counters_indexes | 16 | 214.9 → 605.0 | 0.6013 → 0.3626 | 0 → 4 |
| counters_indexes | 64 | 200.4 → 468.3 | 2.3044 → 1.6741 | 1694 → 461 |

Latest CI on #1953 at `230c36c1`: signed-docs, Go compatibility, Iroh and P2P integration, Lean/conformance, native lint and WASM checks pass. Several native test/benchmark/ACP jobs remain in progress or queued; no all-green claim. #1955 has passing lint, feature checks and Lean/conformance, with remaining jobs still running. These statuses supersede the older failing-CI snapshot above for these two heads only.

## Step 5 published checkpoint (#1956)

The draft step-5 PR is above #1955; #1953 remains the permanent handoff. Backfill and final index publication pass all 58 index tests. The adapter retry-conflict regression verifies previous-conflict facts are available on replay and absent on the first attempt. All database read (65, one ignored), search (12), transaction (75), vector (245, 13 ignored), view (7), and write (63) tests pass after mandatory wrapper forwarding and typed-conflict assertion repairs. Ten KMS/signing tests pass. Earlier full database suites through merge passed; the latest individual-merge DEK candidate passes 217 merge tests. Batch merge is now integrated; its candidate passed all 218 merge tests, including same-CID keys under distinct authorization contexts and bounded batch history. Final combined validation remains pending. Full final combined validation is pending.

At the latest #1955 CI checkpoint, only Native ACP consumer was still running; every other reported check passed or was skipped. This supersedes earlier historical CI snapshots above. The new #1956 CI has just been triggered. External basic/encryption/ACP, counter conformance and fresh performance gates remain open. The failed earlier head-cache performance gate is not waived.

### Remaining implicit-batch retry gap

The step-5 audit found that QueryRunner’s implicit multi-statement / transactional-ACP batch still uses ordinary `DbTxn::commit`, with HTTP and embedded request-level conflict replay. This is separate from `create_many` and remote batch merge, which already use engine retries. A whole-batch execution callback is being implemented so the engine owns the complete sequence, with request-scoped preparation and attempt-local reads, writes and callbacks. Interactive transactions keep returning conflicts to their caller. Do not mark step 5 complete until this path and its forced-conflict regressions are integrated.

At `e0f9c9b9`, native db/storage/p2p all-targets Clippy and WASM Clippy pass. The external Rust checkpoint passes ACP (110, seven ignored), basic (36), and encryption (12), and nine enabled counter-conformance cases pass (eight ignored probes/parity cases). Six mixed Go/Rust encryption cases initially failed before execution because Go binaries were missing; exact pinned Go revision `53f0e76a3` has now been built for reruns. Later full-batch changes require another combined validation.

## Step 6 draft checkpoint (#1959)

Complete head-cache removal is present in ordinary, retry and scoped transaction paths. A restack omission was caught before publication and restored. The final candidate passes native acp/datastore/query/storage/db all-targets Clippy, defra-wasm Clippy, four schema/OCC conflict tests, and the ordered ACP registration regression. Peer conflict transitions now use engine retry, preserving point-read/generation fences; all 37 peerstore tests pass. Namespaced retry I/O scopes iterators and unwraps the original capability for commit. Legacy retry migration is removed, per the new-installation scope. Full combined suites are running on the published branch.

At the preceding step-5 checkpoint, exact Go pin `53f0e76a3` resolves the setup failures: all 12 selected Go/mixed encryption tests pass, as do 95 selected Go ACP cases (16 ignored) and 19 basic cases. These complement the earlier Rust results; final batch conversion still requires new combined checks.

Allocation checkpoint before lock removal: `cargo bench -p benches --bench allocs --profile release-fast` reports **4062.7 allocations/create** (16 fields, 200 operations, source `e0f9c9b9` plus docs-only changes), 269.0 get-by-ID, 36.0 map encoding, 76.0 JSON decoding. The bundle's historical main reference is 2997 allocations/create. Comparable current-main and final-candidate runs, plus overhead investigation, are in progress. No allocation gate pass is claimed.

## Full draft stack checkpoint (#1961)

Steps 7 and 8 are published as #1960 and #1961. Telemetry reuses caller statistics/listeners, reads native commit/policy tickers and records bounded conflict reasons without user keys; duplicate adapter counters are removed. All three telemetry regressions pass, including exact native conflict/winner accounting, with native and WASM Clippy. Proof retirement keeps HeadSet/Core, removes cache/serialized-counter/queue models, and binds arrival ordering to pinned CommitOrderedAppend. All 163 TLC verdicts matched expectations; Lean and registry checks passed on the candidate. Lean passes again after the final restack; registry rerun passes all three tests. Boundary models do not prove the Rust adapter or remote authorization behavior.

The combined step-6 database run caught a real failure: `create_index_keeps_pace_with_concurrent_writers` receives a permanent cached-schema mismatch during concurrent index creation (57 other index tests pass). A same-snapshot metadata refresh fix is in progress; validation will not be weakened. Independent DB/core suites continue. This failure and the implicit batch gap block readiness even though all step PRs exist.

Comparable allocation runs confirm a regression: current main `532aac3f` measures 2985.7 allocations/create and 165/get-by-ID; step4 `3c8bed28` measures 3622.0/269; step5 measures 4062.7/269; step6+peer candidate `97e2a3a3` measures 4242.0/269. All use the same release-fast allocation harness; codecs remain 36 map/76 JSON. Investigation is active. The earlier failed head-cache throughput/conflict gate remains open, and no fresh per-removal throughput gate is claimed.

Independent step-6 validation completed after the index failure: DB merge/read/search/transaction/vector/view/write suites pass **679 tests** (14 ignored); query/storage/zanzibar suites pass **1275 tests**. The earlier combined run passed **694 tests** before the one index race failure. These do not waive that failure. External step-6 basic/ACP/encryption is running on a freshly built CLI. Proof registry rerun on the published tip passes 3/3, and Lean build passes.

## Latest validation after index-race repair

Step 6 now includes `36ac4cc8`: each engine attempt reloads collection/index actions from its own snapshot, allows only index-list drift, and rejects changed versions or other schema content. Backfill also verifies its target index descriptor. All 59 index tests and four schema OCC tests pass; native DB and WASM Clippy pass. The deterministic regression exercises two DB handles with a stale writer cache across index creation, then verifies create/update/delete index maintenance. The fix is carried through steps 7 and 8.

External step-6 validation before this repair passes 204 ACP tests (16 ignored), 55 basic tests, and all 25 encryption tests using exact Go pin `53f0e76a3`. The remaining basic version-JSON smoke test initially read this workstation's old `rocksdb` config; it passes with a clean `DEFRA_ROOTDIR`. Earlier independent DB suites passed 679 tests (14 ignored), and core query/storage/authorization suites passed 1275. These checkpoint results do not replace final combined validation after whole-batch retry integration.

Whole-batch retry implementation and allocation investigation remain active. A proposed full-document allocation optimization was held back during review because writable callers need their field-level validation dependencies preserved. No performance gate is waived.

Latest CI checkpoint: #1953 (`230c36c1`) and #1922 (`199daa57`) have all reported checks passing. #1955 has no failures, with Native ACP consumer still running. Lower middle PRs have successful current lint/feature checks and full suites skipped by stack-scope policy; old cancelled runs can still appear in the rollup. Step-5 CI found an unused DB telemetry dependency, fixed locally at `0f219851` with cargo-machete passing, plus concurrent HTTP retry metric-test interference being addressed alongside the batch path. These results supersede the historical failing-CI snapshots above for unchanged heads.
