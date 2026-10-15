# Create retry input sharing allocation counts

This paired measurement compares the computed-create buffer checkpoint with immutable input sharing across create retries. Both binaries were built from the same `release-fast` target in the same worktree; the only production change between revisions is commit `4e5ab1dcf`.

The harness was built with `cargo bench -p benches --bench allocs --profile release-fast --no-run`. Each executable was run five times. These are allocation counts only; no timing values were collected or used.

| Revision | Binary SHA-256 | Create allocations/op (five runs) | Median | Get by ID | To map | From JSON |
|---|---|---|---:|---:|---:|---:|
| Base `8f58623b5` | `dfceb092cf40926ec73f4a4a1e7009b4c46dc870f99d53eecf4b84806905be51` | 3296.3, 3296.4, 3296.4, 3296.4, 3296.4 | 3296.4 | 160 each | 36 each | 76 each |
| Candidate `4e5ab1dcf` | `da1254bdbf816e595c60f4584b54ade01ef06c772f3e6280aa3e744e27a1a838` | 3231.4, 3231.3, 3231.4, 3231.3, 3231.3 | 3231.3 | 160 each | 36 each | 76 each |

The median create count decreases by 65.1 allocations/op. The candidate keeps the source document ID-less through validation and shares it by `Arc` across retry attempts. The count is an observation from one host and five runs, not a confidence bound.
