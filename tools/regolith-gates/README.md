# DefraLevel adoption gates

These scripts preserve the source revision, frozen executable hashes, raw
samples, load guards, Criterion estimates and allocation counts for §5.3 of
the adoption plan. Run all timings serially on one quiet host. Do not run
builds or other test suites during a timing window.

Create a dedicated detached worktree and use one build target directory for
that worktree throughout the comparisons. Sharing a Cargo target directory
between different worktree paths can reuse stale workspace artifacts.

```sh
git worktree add --detach ../defra-gates 532aac3f
python3 tools/regolith-gates/build.py --worktree ../defra-gates \
  --target-dir /tmp/defra-gates-target --binary-root /tmp/defra-gates/bin \
  main-532aac3f step0-199daa57
python3 tools/regolith-gates/run.py --worktree ../defra-gates \
  --binary-root /tmp/defra-gates/bin --artifact-root /tmp/defra-gates/results \
  --guard-script tools/perf-site/loadguard.py main-532aac3f step0-199daa57
python3 tools/regolith-gates/analyze.py --main main-532aac3f \
  --binary-root /tmp/defra-gates/bin --gate-root /tmp/defra-gates/results \
  --json /tmp/defra-gates/report.json --markdown /tmp/defra-gates/report.md \
  main-532aac3f step0-199daa57
```

Supply all adoption and removal checkpoints in their intended comparison
order. The labels end in an existing commit ID. Existing frozen outputs are
never overwritten. The runner's `--resume` skips completed commands and can
retry an unstarted command after a failed quiet-host guard; failed guards
remain in the evidence. A failed or interrupted benchmark requires inspection.

The analyzer distinguishes invalid evidence (exit 2) from an observed gate
failure (exit 3). Comparisons against main and the preceding supplied label
use strict observed medians, with no invented noise tolerance. Historical
#218 numbers are descriptive references: their hardware and build profile
are not established as comparable. A passing workload report does not prove
the disjoint-field correctness target; the storage, DB and GraphQL regression
tests provide that evidence. Keep all raw outputs alongside the report.
