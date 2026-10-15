# Document index key capacity allocation comparison

This compares the allocation benchmark on the same checkout and target before
and after preallocating the document index key buffer. Only
`doc_id_index_key` changed between the two source revisions. No timing values
were collected or used.

| Source | Binary SHA-256 | Create allocations/op (five runs) | Median | Get-by-ID allocations/op (five runs) |
| --- | --- | --- | ---: | --- |
| Base `d406ef5e61382ac8362fa253efd5dfb00405981b` | `295c51fa881e9c2c7102a1cc9e152abac94e560b43d355478c7a9065801c5016` | 3189.3, 3189.3, 3189.3, 3189.4, 3189.3 | 3189.3 | 160.0, 160.0, 160.0, 160.0, 160.0 |
| Candidate `00112cd3c47bb2f495a5a2972f91c4827f8b51dd` | `19fb45e0d29df36da97215d68d32063a6f0c2c98fd538524cea19bdb7534b2a2` | 3130.3, 3130.4, 3130.4, 3130.3, 3130.4 | 3130.4 | 157.0, 157.0, 157.0, 157.0, 157.0 |

The median create count decreased by 58.9 allocations/op (about 1.85%). The
get-by-ID count decreased by 3.0 allocations/op. Other reported rows were
unchanged at 36.0 allocations/op for document-to-map and 76.0 for
document-from-JSON in every run. The benchmark reports process-wide observed
allocations per operation; these counts are comparable within this paired
setup, but they do not attribute every allocation to the key helper.

Both executables used the `release-fast` profile and were built with:

```text
CARGO_TARGET_DIR=/tmp/defra-schema-wire-candidate-target cargo bench -p benches --bench allocs --profile release-fast --no-run -j 4
```

Each executable was then run five times from the same worktree. The base binary
was copied and hashed before changing the source; the candidate binary was
copied and hashed after rebuilding. Key-layout parity is covered by
`storage::keys::doc_id_index::tests::test_key_layouts` and
`test_alias_prefix`; all eight tests in that module passed on the candidate.
