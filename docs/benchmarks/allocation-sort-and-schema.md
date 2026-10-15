# Allocation counts: block link sorting and schema sharing

These are allocation counts per operation from the `benches/allocs` harness, built with the `release-fast` profile. Each listed value is a separate executable run. No timing columns are reported or used; this document makes no throughput claim.

The baseline is the older step6 checkpoint `3bd4fdcd`. The combined candidate uses the immutable-collection-schema change from `8905a6085` and the CID-sort change from `d9f5a45bc`. They are separate revisions: the baseline predates both candidate changes. The counts attribute the combined difference from that baseline; the CID-only paired runs further isolate the sort change.

## Revisions and executable hashes

- Older step6 baseline `3bd4fdcd`: `/tmp/defra-perf-artifacts/bin/step6-3bd4fdcd/allocs`; SHA-256 `54422e9900f5b0f7582d9b0ec7f80f8db06a639342d55e3cc67d495c97f89bcf`.
- Step6 plus CID-sort only `d9f5a45bc907198ba48b74dcae2b1b785503996a`: `/tmp/defra-step6-cid-opt/release-fast/deps/allocs-440666495d215433`; SHA-256 `72f501171545ef1a90c72ec767d2b51414a1031e7d6686998db927cd9dea55cf`.
- Immutable schema (`8905a6085`) plus CID-sort (`d9f5a45bc`), combined revision `05fa4d004446e88f2c52cb04170caa38712b65b4`: `/tmp/defra-step6-cid-arc/release-fast/deps/allocs-8978f3dd8ac8be84`; SHA-256 `a37c156a1f3babf0e3e01fdf546ca242f64dc18068e4c2bdf4d3bf5fdc6fd9cd`.

Each executable was built with `cargo bench -p benches --bench allocs --profile release-fast --no-run`, then run five times.

## Raw samples

| Revision | Document create allocations/op | Get by ID | To map | From JSON |
|---|---:|---:|---:|---:|
| Older step6 baseline `3bd4fdcd` | 4237.8, 4246.6, 4231.2, 4238.4, 4227.6 | 198 each | 36 each | 76 each |
| Step6 + CID-sort `d9f5a45bc` | 3588.4, 3588.4, 3588.3, 3588.4, 3588.2 | 198 each | 36 each | 76 each |
| Immutable schema + CID-sort `05fa4d004` | 3346.4, 3346.4, 3346.4, 3346.5, 3346.3 | 160 each | 36 each | 76 each |

The five-sample medians for document create are 4237.8, 3588.4, and 3346.4 respectively. The CID-sort candidate saves 649.4 allocations/op against the older baseline. Adding immutable schema sharing saves another 242.0 allocations/op against CID-sort alone. The combined candidate is 891.4 allocations/op (21.0%) below the older baseline. Get-by-ID drops by 38 allocations/op only in the immutable-schema candidate; the other reported counters remain unchanged.

These counts are observations from one host and five runs, not confidence bounds. Use the raw samples rather than interpreting the rounded medians as a guarantee.
