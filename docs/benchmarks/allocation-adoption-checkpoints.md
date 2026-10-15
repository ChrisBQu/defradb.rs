# Allocation counts across frozen adoption checkpoints

These results use the prepared `allocs` benchmark executable for each frozen source revision. The binary was built with the `release-fast` profile and run from its matching source worktree. Each executable was run five times. No throughput or wall-clock measurements were collected.

The benchmark reports whole-process allocations per operation. Document-create counts varied across repeated executions even on an otherwise idle run, so the table reports the median and observed range; treat small differences cautiously. The get-by-ID and codec rows were stable across these runs.

| Checkpoint | Source revision | `allocs` binary SHA-256 | Document create allocations/op (median; range) | Get by ID allocations/op |
|---|---|---|---:|---:|
| main-532aac3f | `532aac3fd06a61c319c2d5fbde84da42aab67f0d` | `a1b68c22e10f4cb75f8e0f091bc35636b2d02fc2bd0ba0a7a05bd0ed807f99ed` | 2991.5 (2984.6–3005.3) | 165 |
| step0-199daa57 | `199daa577c6750165571d256a16c6625c42af057` | `a9a74abc7c04bd4766bcd5f2f51edb71c7e96e862102653c791e5194cd9cf90c` | 3002.3 (2989.1–3009.4) | 165 |
| step1-5ed6e2fa | `5ed6e2fa8034b42faa586c913d5d77dcfb3d41b4` | `ffd7d21cc17207354c0aa7c0c239153b98e1fa18f68a218e566811e1a16afc83` | 3025.0 (3019.9–3036.8) | 165 |
| step2-82047ca2 | `82047ca26fd714bb1f213ec652679234e265d639` | `78c5e15ca2d4c65244157ad90cbb7fc238da42f0686164dfe31ff7808e53df11` | 3049.7 (3033.7–3058.5) | 169 |
| step3-230c36c1 | `230c36c1d9c95833f8c24109aa1d8675a24ce1b1` | `616942fef9d5fc2e328b1e81d38d36978fdefd761a609e2a7c8527f5980f7298` | 3622.3 (3617.3–3646.1) | 269 |
| step4-3c8bed28 | `3c8bed2824e305b4d5c02da47ade8169d490c6fa` | `d2c45850fe9161e26b39da87eb576d6a21108c4a592cef54467524fbb55681c1` | 3624.3 (3609.8–3640.4) | 269 |

The first large step change is between step2 and step3: median create rises by about 573 allocations/op and get-by-ID rises from 169 to 269. Step4 changes the create median by about two allocations/op relative to step3. These are count results only, not throughput claims.

Build command for all executables:
```sh
cargo bench -p benches --bench allocs --bench write_baseline --bench document_write --bench startup --profile release-fast --no-run
```

## Raw allocation output

Each section contains the unedited stdout from five consecutive executions of the matching binary.


### main-532aac3f

Revision `532aac3fd06a61c319c2d5fbde84da42aab67f0d`; `allocs` SHA-256 `a1b68c22e10f4cb75f8e0f091bc35636b2d02fc2bd0ba0a7a05bd0ed807f99ed`.

```text
  document create              2991.5 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              2990.7 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              2984.6 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              2996.9 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3005.3 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

### step0-199daa57

Revision `199daa577c6750165571d256a16c6625c42af057`; `allocs` SHA-256 `a9a74abc7c04bd4766bcd5f2f51edb71c7e96e862102653c791e5194cd9cf90c`.

```text
  document create              3009.4 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3000.4 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3002.9 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3002.3 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              2989.1 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

### step1-5ed6e2fa

Revision `5ed6e2fa8034b42faa586c913d5d77dcfb3d41b4`; `allocs` SHA-256 `ffd7d21cc17207354c0aa7c0c239153b98e1fa18f68a218e566811e1a16afc83`.

```text
  document create              3025.0 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3024.8 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3019.9 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3035.3 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3036.8 allocations/op
  document get by id            165.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

### step2-82047ca2

Revision `82047ca26fd714bb1f213ec652679234e265d639`; `allocs` SHA-256 `78c5e15ca2d4c65244157ad90cbb7fc238da42f0686164dfe31ff7808e53df11`.

```text
  document create              3053.9 allocations/op
  document get by id            169.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3046.6 allocations/op
  document get by id            169.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3049.7 allocations/op
  document get by id            169.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3033.7 allocations/op
  document get by id            169.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3058.5 allocations/op
  document get by id            169.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

### step3-230c36c1

Revision `230c36c1d9c95833f8c24109aa1d8675a24ce1b1`; `allocs` SHA-256 `616942fef9d5fc2e328b1e81d38d36978fdefd761a609e2a7c8527f5980f7298`.

```text
  document create              3618.7 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3628.7 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3617.3 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3622.3 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3646.1 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

### step4-3c8bed28

Revision `3c8bed2824e305b4d5c02da47ade8169d490c6fa`; `allocs` SHA-256 `d2c45850fe9161e26b39da87eb576d6a21108c4a592cef54467524fbb55681c1`.

```text
  document create              3624.3 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3639.2 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3640.4 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3609.8 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
  document create              3618.6 allocations/op
  document get by id            269.0 allocations/op
  document to map                36.0 allocations/op
  document from json             76.0 allocations/op
```

