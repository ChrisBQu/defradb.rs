# Allocation counts: block-link sorting and schema sharing

These are allocation counts per operation from `benches/allocs`, built with the `release-fast` profile. Each value below is a separate executable run. No timing columns are reported or used.

The first row is an older step6 checkpoint (`3bd4fdcd`) and serves only as historical reference. The schema comparison is a paired measurement on canonical base `7e633102e`: both variants include the CID-sort patch, and only the immutable-schema patch differs. Do not compare the historical row to the later rows as a causal before/after; other code changed between those revisions.

## Revisions and executable SHA-256

- Older step6 reference `3bd4fdcd`: `/tmp/defra-perf-artifacts/bin/step6-3bd4fdcd/allocs`; SHA-256 `54422e9900f5b0f7582d9b0ec7f80f8db06a639342d55e3cc67d495c97f89bcf`.
- Canonical step6 + CID-sort, no Arc-schema change: base `7e633102e` plus CID-sort commit `d9f5a45bc` (rebased as commit `046a74a99`); `/tmp/defra-step6-cid-noarc/release-fast/deps/allocs-8978f3dd8ac8be84`; SHA-256 `c529810b8f47a75480fdc59b93e7a5a3fab0cf4791a60848754d64550b1fc555`.
- Same canonical base + CID-sort + Arc-schema change: combined revision `05fa4d004` (Arc change `8905a6085` plus CID-sort); `/tmp/defra-step6-cid-arc/release-fast/deps/allocs-8978f3dd8ac8be84`; SHA-256 `a37c156a1f3babf0e3e01fdf546ca242f64dc18068e4c2bdf4d3bf5fdc6fd9cd`.

Each listed executable was built with `cargo bench -p benches --bench allocs --profile release-fast --no-run` and run five times.

## Raw samples

| Revision | Document create allocations/op | Get by ID | To map | From JSON |
|---|---:|---:|---:|---:|
| Older step6 reference `3bd4fdcd` | 4237.8, 4246.6, 4231.2, 4238.4, 4227.6 | 198 each | 36 each | 76 each |
| Canonical step6 + CID-sort, no Arc | 3612.4, 3612.3, 3612.3, 3612.3, 3612.3 | 198 each | 36 each | 76 each |
| Same base + CID-sort + Arc schema | 3346.4, 3346.4, 3346.4, 3346.5, 3346.3 | 160 each | 36 each | 76 each |

For the paired canonical-base comparison, the five-sample create medians are 3612.3 without Arc schema sharing and 3346.4 with it, a reduction of 265.9 allocations/op. Get-by-ID drops by 38 allocations/op. The other reported counters are unchanged.

These are observations from one host and five runs, not confidence bounds. Allocation counts are reported without timing claims.

## Downsample-plan snapshot filtering

A follow-up compares the same combined candidate at `05fa4d004` with the downsample-plan filter change at `fc7800ad7`. Both were built with `cargo bench -p benches --bench allocs --profile release-fast --no-run`; each executable was run five times. This is a paired allocation-count comparison, with no timing claims.

- Base `05fa4d004`: `/tmp/defra-step6-cid-arc/release-fast/deps/allocs-8978f3dd8ac8be84`; SHA-256 `a37c156a1f3babf0e3e01fdf546ca242f64dc18068e4c2bdf4d3bf5fdc6fd9cd`.
- Downsample filter candidate `fc7800ad7`: `/tmp/defra-downsample-alloc/release-fast/deps/allocs-8978f3dd8ac8be84`; SHA-256 `30dba1727df0e09e031a05379049ed36e991ae3067729677cebdcb4f07c58d78`.

Raw document-create samples were 3346.4, 3346.4, 3346.4, 3346.5, 3346.3 on the base, and 3307.4, 3307.5, 3307.3, 3307.4, 3307.4 on the candidate. The five-sample medians are 3346.4 and 3307.4, a reduction of 39 allocations/op. Get-by-ID remained 160 allocations/op; document-to-map remained 36; document-from-JSON remained 76.

## Frozen step3–step5 checkpoints

These historical checkpoint binaries were supplied from the frozen adoption builds. Each was run five times for allocation counts only; no timing values were collected or used. Build profile was `release-fast`.

| Checkpoint | Executable SHA-256 | Document create allocations/op | Get by ID | To map | From JSON |
|---|---|---:|---:|---:|---:|
| Step3 `081806acb` | `a601407a3f39650bcce38754cc9759238c9e2a52d9f98b03ba2d6f752b9fee35` | 2980.7, 2980.6, 2980.7, 2980.8, 2980.8 | 269 each | 36 each | 76 each |
| Step4 `5feca8675` | `8718846fab15a9f3a764f7222c31f434554cff9f07cd5429e5158da2dd2ec49a` | 2982.7, 2982.7, 2982.8, 2982.8, 2982.8 | 269 each | 36 each | 76 each |
| Step5 `56ea4db1a` | `a17f3f9c26851611664fe08dc5094f7552b431e29a29d48f4057f6beef796820` | 3411.4, 3411.4, 3411.3, 3411.5, 3411.4 | 269 each | 36 each | 76 each |

Executable paths were `/tmp/defra-perf-artifacts/bin/step3-081806acb/allocs`, `.../step4-5feca8675/allocs`, and `.../step5-56ea4db1a/allocs`. The five-sample medians for create are 2980.7, 2982.8, and 3411.4 respectively. Treat these only as checkpoint observations; unrelated changes between checkpoints prevent attributing the differences to one patch.
