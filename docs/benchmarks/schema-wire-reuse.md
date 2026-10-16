# Collection schema wire reuse allocation sample

This records deterministic allocation counts from the `allocs` benchmark. It is
not a timing benchmark. Both executables were built independently from clean
worktree targets and then run five times each on the same host.

| Revision | Source | Executable SHA-256 | Create allocations/op (five runs) | Mean |
| --- | --- | --- | --- | ---: |
| `bb7e47732` | baseline | `e0a0585460fc5a08406e4d766cc80d0cc344e5ce3385f8be91d253e8dd02029b` | 3231.3, 3231.4, 3231.4, 3231.4, 3231.4 | 3231.38 |
| `9700f6a96` | schema wire reuse | `4e5fe4f160a9f4e58eb837c6e21b81b21e5ce769dc3ba6fa3fb4e8642d997ff4` | 3189.3, 3189.4, 3189.3, 3189.4, 3189.3 | 3189.34 |

The candidate used 42.04 fewer allocations per create operation on average
(about 1.30%). Other benchmark rows were unchanged in every run: get-by-id
160.0, document-to-map 36.0, and document-from-JSON 76.0 allocations/op.

Build and run command: `cargo bench -p benches --bench allocs`, followed by five
invocations of the resulting executable. Candidate build used
`CARGO_TARGET_DIR=/tmp/defra-schema-wire-candidate-target`; baseline used a
separate clean worktree and target. The executable hashes identify the exact
sampled binaries.
