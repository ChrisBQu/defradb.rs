# Final runtime validation evidence

These compressed raw logs record validation at `887502472c23f547eef75354decc2d44dfaf0c1f`.
The boundary-repaired `051fd5fc3d794844d083feba219c8d5e2b0eb710` has the identical Git tree;
Subsequent changes to handoff documentation and two schema test fixtures do not change runtime code. `manifest.json` records
source provenance and checksums for both compressed and original logs.

All commands in `run-validation.py` completed successfully. The script preserves the
original local paths and environment; adapt those paths when reproducing. WASM Clippy
was run separately with Homebrew LLVM and `cargo clippy -p defra-wasm
--target wasm32-unknown-unknown -- -D warnings`.

Results: 2,377 core tests (33 ignored), 358 HTTP/embedded tests (5 ignored),
five benchmark-accounting tests, native all-target Clippy, WASM Clippy, a fresh CLI build,
basic 56, ACP 204 (16 ignored), encryption 25, counter 9 (8 ignored), and
one write-skew protection test pass. This is local correctness/lint evidence;
remote CI and performance gates are reported separately in the handoff.
