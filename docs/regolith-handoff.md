# Regolith adoption handoff

[PR #1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) is the permanent review entry point; the current stack tip is [#1961](https://github.com/sourcenetwork/defradb.rs/pull/1961). The stack represents prep step 0 and all eight §5.2 adoption steps. **All PRs remain drafts and none are merged.** Merge bottom-up, retargeting each next PR only after its base lands.

Scope is new installations only. Existing-store format compatibility and migrations are explicitly waived. Step 9's worker-pool redesign is excluded. Regolith is pinned to `d1e36aa747913f6b1ecc14c821f4e5e6314674cf`; the integrated main base is `8a38e96f2`.

## Merge order and step map

| Order | Step | PR | Validated code/CI head | Base |
|---:|---|---|---|---|
| 1 | Prep / step 0 | [#1922](https://github.com/sourcenetwork/defradb.rs/pull/1922) | `7b7fb91a` | `main` (`8a38e96f2`) |
| 2 | 1a: Regolith API, RepeatableRead | [#1949](https://github.com/sourcenetwork/defradb.rs/pull/1949) | `ea27f833` | #1922 |
| 3 | 1b: engine encryption | [#1920](https://github.com/sourcenetwork/defradb.rs/pull/1920) | `e309501d` | #1949 |
| 4 | 1c: key classifier / DefraLevel | [#1951](https://github.com/sourcenetwork/defradb.rs/pull/1951) | `7668ba87` | #1920 |
| 5 | 2: transactional decisions | [#1952](https://github.com/sourcenetwork/defradb.rs/pull/1952) | `b65de363` | #1951 |
| 6 | 3: engine mechanisms | [#1953](https://github.com/sourcenetwork/defradb.rs/pull/1953) | `78e48a85` | #1952 |
| 7 | 4: receipt-ordered events | [#1955](https://github.com/sourcenetwork/defradb.rs/pull/1955) | `ad642726` | #1953 |
| 8 | 5: engine-owned retries | [#1956](https://github.com/sourcenetwork/defradb.rs/pull/1956) | `913e98b2` | #1955 |
| 9 | 6: lock removal | [#1959](https://github.com/sourcenetwork/defradb.rs/pull/1959) | `89d71e2b` | #1956 |
| 10 | 7: engine telemetry | [#1960](https://github.com/sourcenetwork/defradb.rs/pull/1960) | `133a308a` | #1959 |
| 11 | 8: proof alignment | [#1961](https://github.com/sourcenetwork/defradb.rs/pull/1961) | `f7813d07` | #1960 |

The table records the validated code heads; subsequent documentation-only commits on #1961 publish this handoff and evidence. CI results below refer to those recorded heads, not automatically to later commits.

The #1920 branch name predates the final implementation; its current content is engine encryption. Do not merge superseded generated PRs #1897, #1899, or #1911.

## Implementation status

- **Step 0:** write preparation, bounded shared lens execution, shared engine ticker lifecycle, and lazy transaction-local lens transforms. The 100 ms epoch-ticker interval is provisional: a bounded measurement observed 213 ms median and 220 ms p95 interruption after two ticks, not an optimal interval or a complex-lens latency guarantee ([measurement](benchmarks/lens-epoch-period/measurement.md)).
- **Step 1:** API upgrade, direct engine key providers, and key classification are split across #1949/#1920/#1951. DefraLevel is used for writes; only genuine block CID keys are content-addressed.
- **Step 2:** transaction-snapshot reads and writes cover document/schema/relationship/NAC/downsample decisions, local authorization, and schema/index decision scans. Schema-head cache publication follows successful commit; schema decisions use a validated scan in the write transaction. Explicit GraphQL statement savepoints make later failure, timeout, or cancellation roll back only that statement while preserving prior statements; failed-statement blob blocks are removed, while allocated IDs may leave gaps. Direct registry DDL has only pending GraphQL cleanup, not an independent operation savepoint.
- **Step 3:** commit-ordered arrivals, engine ID allocation and value merges cover counters, full text, vectors, and document fields. It adds document head markers and write-free read-only lenses. General query/output and conditional reads remain conservative; do not claim zero incidental conflicts.
- **Step 4:** events dispatch by actual commit receipt sequence. Ordered fallible ACP hooks run before related events; hook failure suppresses those events. A later receipt waits for earlier active receipts, including external ACP work.
- **Step 5:** engine-owned retries cover writes, merges, lenses, backfill, pending-DAG transactions, and selected mutation batches. Attempt callbacks can rerun. Losing transaction writes and callback bundles are discarded; only the winner’s registered post-commit hooks and effect bundles are transferred for dispatch. Inputs such as IDs, DEKs, ciphertext, and counter nonces remain stable across replay; request-prepared external inputs are bounded and typed. Interactive transactions return conflicts to callers.
- **Step 6:** the planned storage/cache/collection locks and forced-read APIs are retired or replaced by snapshot validation, OCC, immutable ACP projections, and peer-generation fences. Explicit-ID scalar GraphQL writes project known changed fields and `_docID` outputs; unsupported query shapes and conditional updates retain broader validation. Index-list drift alone is tolerated while every non-index schema field remains checked. Index backfill captures its cutoff after schema publication, and an interrupted action with no progress row recaptures a safe post-publication cutoff. Native Vera local tombstones retain shared registration/grants across a replica-local delete; explicit `ArchiveObject` remains global, and other providers retain their default behavior. Issued archive errors still propagate.
- **Step 7:** engine transaction/policy tickers and bounded conflict reasons are exposed; caller-side measurements remain distinct from engine replay counts.
- **Step 8:** obsolete cache/finalizer proof models are retired, while HeadSet remains. At the published proof head, all 167 configured TLC cases have their expected verdicts; five registry/library checks and two Lean contract checks pass. The restack leaves proof source unchanged. These finite boundary models do not prove Rust behavior or remote authorization.

Accepted D39/D56 supersede the earlier synchronous-trait wording: data paths remain async and use `transact_async`; the existing native blocking bridge stays in place until excluded step 9. Scoped retry I/O cannot commit/discard independently, and retained views fail closed before returning the real engine transaction.

## Validation

All eleven published exact heads are draft, clean/mergeable, have the expected preceding-PR base, and have auto-merge disabled. Exact-head CI and Performance workflows passed for all eleven; Native consumer passed for all eleven. Applicable macOS workflows passed; #1949, #1920, #1951, and #1952 have explicit workflow-level skips (a skipped `Unit tests` job with no steps), not failures. The [full-stack exact-head audit](validation/regolith-full-stack-ci.json) records SHA mapping, run URLs, bases, skips, and conclusions. The three upper PRs have fresh green [Native ACP runs](https://github.com/sourcenetwork/defradb.rs/actions/runs/38703087623), [#1960](https://github.com/sourcenetwork/defradb.rs/actions/runs/38703089156), [#1961](https://github.com/sourcenetwork/defradb.rs/actions/runs/38703087302), plus green CI, macOS, and hosted quick benchmark runs.

At pre-native-fix checkpoint `887502472`, validation passed 2,377 core tests (33 ignored), 358 HTTP/embedded tests (5 ignored), five benchmark-accounting tests, native all-target Clippy, and WASM Clippy. See [portable logs, checksums, and reproduction commands](validation/regolith-887502472/README.md). The [ten-checkpoint CLI matrix](validation/regolith-cli-checkpoints/README.md) passes basic 56, ACP 204 (16 ignored), encryption 25, and counter 9 (8 ignored) at each checkpoint through telemetry. The same artifact records the independent prep0 schema fixture check: all 183 schema tests pass. The [native ACP fix record](validation/regolith-native-acp-89d71e2b.json) includes pinned Vera provenance, the failing baseline, passing fix, exact selectors, and hashes/provenance for private logs and executables (the private logs themselves are not included); on the fix, Vera passes 67/67 and all seven unchanged native qualification cases pass. Provider/CLI and native-test Clippy pass. The fix changes optional Vera behavior; the default DB/query/storage benchmark build does not include Vera.

## Remaining gates and limits

**All 22 checkpoints are complete, and the strict performance gates failed. The stack is not merge-ready.** The [complete measurement pack](benchmarks/regolith-adoption-8a38e96f2/README.md) contains 792 workload samples, 176 completed command runs, raw Criterion/allocation data, guard records, source and executable identities, final comparisons, and a Git bundle preserving frozen revisions. There are no missing comparisons. Load-guard refusals and the step1b runner pause are recorded; passing command-start guards do not establish an idle host throughout each sample. Comparisons use strict medians/point estimates, without a statistical-significance claim or regression waiver.

| Final runtime versus main | Main | Final |
|---|---:|---:|
| 64-writer creates, commits/sec | 247.019 | 7,366.801 |
| 64-writer shared updates, commits/sec | 242.428 | 493.410 |
| 64-writer counters/indexes, commits/sec | 232.505 | 299.086 |
| Shared-update exhausted requests / 16,384 | 577 (legacy HTTP) | 4,775 (engine) |
| Counter/index exhausted requests / 16,384 | 543 (legacy HTTP) | 8,434 (engine) |
| Allocations/create, median | 2,991.135 | 3,133.550 |

Final creates have zero observed conflicts, but eight strict workload comparisons, three create Criterion comparisons, and allocations regress against main. Latency samples include successes and failures; legacy HTTP and current engine counters are distinct. Shared updates include real same-field contention, so this aggregate workload does not establish the separate disjoint-field target. [Full results](benchmarks/regolith-adoption-8a38e96f2/results.md) retain every checkpoint's gates. The optional create-validation optimization is **not adopted**: small allocation/create-latency improvements did not pass throughput and cold-start nonregression checks.

Theo's next work is to resolve the performance gates, not rerun missing checkpoints:

1. Classify conflict/access reasons at retries and exhaustion, separating true same-field contention from unnecessarily broad dependencies.
2. Compare equal total attempt budgets and zero-delay versus bounded engine backoff. Current autocommit uses four conflict-budgeted attempts without backoff; historical HTTP allowed six executions with jitter. This is a plausible contributor, not an established cause. Cache-validation replays do not consume that budget. The pinned engine supports deterministic exponential backoff; jitter would require a separate engine change. See the [source-pinned retry investigation](benchmarks/regolith-adoption-8a38e96f2/retry-policy-findings.md).
3. Profile the allocation/create-latency regressions while preserving statement atomicity, validated dependencies, and winner-only post-commit effects. Compare commit throughput, exhaustion, and all-request latency together on a guarded host. Keep all PRs draft until failures are resolved or explicitly accepted by the reviewer.

General query/output validation remains conservative where dependencies are uncertain. Counter reads remain validated because document/index mirrors depend on them. Writable projections cover changed fields, touched-index components, immutable checks, downsample sources, and embeddings; unsupported or conditional changes fall back to broader validation. These choices favor correct conflict detection over a blanket low-conflict claim.

The P2P `pending_metadata_writer` mutex remains. It coordinates multi-operation durable-cap updates with in-memory root/scope caches; engine retries cover individual pending-store transactions, not the whole protocol. It is outside the listed step-6 removals and remains a network-runtime follow-up, not a claim that every mutex has been removed. Durable P2P operations are fenced by peer incarnation and marker generation, but already-admitted external I/O cannot be revoked.

Ordered event delivery is not durable: cancellation after commit can still lose in-memory event delivery. Proofs are finite boundary models, not verification of Rust or remote ACP behavior. The optional native tombstone change preserves shared authorization state on a local replica delete; globally archiving an ACP object remains an explicit provider operation. Do not change these semantics to make local replica cleanup look like global revocation.

Historical allocation and benchmark investigations remain in the linked evidence and Git history. The complete measurement pack above is the final record for this handoff; no measured regression has been waived.
