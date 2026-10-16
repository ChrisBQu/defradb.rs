# Frozen performance results

Source: `analysis/analysis-final-22.json` in `support-and-analysis.tar.gz`. All 22 labels are structurally valid with 36 samples across 8 commands each (792 samples; 176 command runs). Values are point estimates; no tolerance or statistical-significance claim is made. Latency p50/p99 include all requests, successful and failed.

## Writer=64 point estimates

| Label | Workload | commits/s | storage conflicts/commit | engine retries/commit | engine exhausted requests/commit (count) | legacy HTTP conflicts/commit | legacy HTTP exhausted/commit (requests) | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| main-8a38e96f2 | counters_indexes | 232.505 | 0 | n/a | n/a (n/a) | 1.89315 | 0.0342781 (543) | 33.0418 | 2973.39 |
| main-8a38e96f2 | creates | 247.019 | 0 | n/a | n/a (n/a) | 0 | 0 (0) | 253.971 | 518.889 |
| main-8a38e96f2 | shared_updates | 242.428 | 0 | n/a | n/a (n/a) | 1.84017 | 0.0365028 (577) | 28.8783 | 2866.34 |
| step8-051fd5fc3 | counters_indexes | 299.086 | 5.43436 | 4.37399 | 1.06088 (8434) | n/a | n/a (n/a) | 71.3677 | 284.935 |
| step8-051fd5fc3 | creates | 7366.8 | 0 | 0 | 0 (0) | n/a | n/a (n/a) | 8.25125 | 15.8838 |
| step8-051fd5fc3 | shared_updates | 493.41 | 2.66224 | 2.25093 | 0.411319 (4775) | n/a | n/a (n/a) | 55.7684 | 292.656 |
| createfast-3a90482b2 | counters_indexes | 299.015 | 5.38205 | 4.33379 | 1.04826 (8385) | n/a | n/a (n/a) | 70.7542 | 286.108 |
| createfast-3a90482b2 | creates | 7694.63 | 0 | 0 | 0 (0) | n/a | n/a (n/a) | 7.9995 | 14.6725 |
| createfast-3a90482b2 | shared_updates | 495.349 | 2.67554 | 2.25946 | 0.416076 (4814) | n/a | n/a (n/a) | 55.6073 | 285.145 |

Legacy HTTP conflict/exhaustion counters are reported only on the pre-engine main layer; `n/a` means that layer was retired and is not measured. Current-layer engine retries/exhaustions are separate counters. Creates show 0 exhaustion in the current layer.

## Allocation and Criterion comparisons

| Label | document-create allocation samples (alloc/op) | median |
|---|---|---:|
| main-8a38e96f2 | 2991.14, 2984.99, 2984.19, 2991.58, 3000.05 | 2991.14 |
| step8-051fd5fc3 | 3133.61, 3133.57, 3133.55, 3133.54, 3133.55 | 3133.55 |
| createfast-3a90482b2 | 3111.55, 3111.55, 3111.47, 3111.49, 3111.54 | 3111.54 |

Criterion point estimates, final published step8 vs main:

| Benchmark | Main ns | Step8 ns | Delta ns |
|---|---:|---:|---:|
| document_create/4 | 22588.394 | 37769.660 | +15181.266 |
| document_create/16 | 77405.256 | 100358.631 | +22953.375 |
| document_create_branchable/14 | 29816.940 | 45964.848 | +16147.908 |
| startup/cold_open/embedded/10_collections | 8561020.800 | 8503847.250 | -57173.550 |

## Final step8 gates vs main

Overall baseline nonregression: **observed_fail**, 48 comparable cells, 8 regressions, 0 missing comparisons. Allocation: **observed_fail** (3133.55 vs 2991.14 alloc/op). Criterion: **observed_fail** (3 regressions, 0 missing comparisons). Create conflict coverage complete: `True`; current create conflicts zero proven: `True`.

Baseline regression cells (workload / writers / metric):

- `creates` / 1 / `commits_per_second`: 249.096 → 248.136 (Δ -0.960816).
- `shared_updates` / 1 / `commits_per_second`: 256.071 → 247.611 (Δ -8.46056).
- `shared_updates` / 4 / `storage_conflicts_per_commit`: 0 → 0.00195312 (Δ +0.00195312).
- `shared_updates` / 16 / `storage_conflicts_per_commit`: 0 → 0.126899 (Δ +0.126899).
- `shared_updates` / 64 / `storage_conflicts_per_commit`: 0 → 2.66224 (Δ +2.66224).
- `counters_indexes` / 4 / `storage_conflicts_per_commit`: 0 → 0.00976562 (Δ +0.00976562).
- `counters_indexes` / 16 / `storage_conflicts_per_commit`: 0 → 1.15374 (Δ +1.15374).
- `counters_indexes` / 64 / `storage_conflicts_per_commit`: 0 → 5.43436 (Δ +5.43436).

The optional createfast candidate is **not adopted**: its strict baseline and Criterion comparisons fail against the published step8, despite reducing allocation median by 22.01 alloc/op (3133.55 → 3111.54). It has 9 baseline nonregression failures versus step8 and one Criterion regression (cold startup); these are point comparisons only.

## Per-checkpoint gate status vs main

| Checkpoint | Baseline gate | Regressions | Missing | Allocation | Criterion | Create-conflict gate |
|---|---|---:|---:|---|---|---|
| main-8a38e96f2 | reference | — | — | reference | reference | observed_pass |
| step0-d6f58d624 | observed_fail | 13 | 0 | observed_fail | observed_fail | observed_fail |
| step1a-3162318e2 | observed_fail | 17 | 0 | observed_fail | observed_fail | observed_fail |
| step1b-1cf6a0b23 | observed_fail | 21 | 0 | observed_fail | observed_fail | observed_fail |
| step1-45c81aed5 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_fail |
| step2-23c8fde54 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_fail |
| step3-711555a37 | observed_fail | 13 | 0 | observed_pass | observed_fail | observed_pass |
| step4-965fb4e6e | observed_fail | 13 | 0 | observed_pass | observed_fail | observed_pass |
| step5-c2502fc5c | observed_fail | 14 | 0 | observed_fail | observed_fail | observed_pass |
| forcedread-d230f33fb | observed_fail | 14 | 0 | observed_fail | observed_fail | observed_pass |
| arrival-bafceb60c | observed_fail | 13 | 0 | observed_fail | observed_fail | observed_pass |
| acpcounter-a4487cef0 | observed_fail | 13 | 0 | observed_fail | observed_fail | observed_pass |
| docqueue-1ceaefaca | observed_fail | 7 | 0 | observed_fail | observed_fail | observed_pass |
| collectionlock-b33f571c7 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_pass |
| peergeneration-6262270d1 | observed_fail | 7 | 0 | observed_fail | observed_fail | observed_pass |
| acpengine-d7d8d0c17 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_pass |
| deferredacp-5ae8448a4 | observed_fail | 7 | 0 | observed_fail | observed_fail | observed_pass |
| headcache-5af484fb3 | observed_fail | 7 | 0 | observed_fail | observed_fail | observed_pass |
| step6-47a35b111 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_pass |
| step7-11b71efcd | observed_fail | 7 | 0 | observed_fail | observed_fail | observed_pass |
| step8-051fd5fc3 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_pass |
| createfast-3a90482b2 | observed_fail | 8 | 0 | observed_fail | observed_fail | observed_pass |

The complete analyzer output and original per-label reports are preserved in the archives indexed by `index.json`; this summary is a compact restatement of those results.
