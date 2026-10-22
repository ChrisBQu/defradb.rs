# Lens Migrations

Schemas evolve; stored documents must not be rewritten to follow. A **lens** is a
WASM module that transforms a document from one schema version to another, applied at
*query time* (`crates/lens`): documents stay on disk in the version they were written
in, and readers see them through the lens chain to the target version.

## The pipeline

Given a collection's version history (a DAG of schema versions), the lens history
builder prunes it to one path per source version toward the target, and the pipeline
applies the transform chain lazily: one document per `next()` call, on the caller's
task, inside the reader's transaction (the lensed fetcher, `crates/db/src/read/lensed/`).
Explicit migration runs (backfills) use the same pipeline; nothing is rewritten at
migration-*set* time, so setting a migration is metadata, not a data rewrite.

## The engine and the sandbox

One shared wasmtime `LensEngine` per database: one engine, one linker, one epoch
clock; modules are compiled once at registration into an `InstancePre`, and each call
instantiates and runs on a fiber the caller polls (`crates/lens/src/engine/`). The
default transform store is restrictive-sandboxed WASM; without the wasmtime runtime
feature, the store refuses everything (a build without lens support fails loud, not
silently passing documents through).

## The epoch tick

A lens module is untrusted code, so it runs under two bounds: **fuel/ticks** (a call
out of ticks traps) and the **epoch clock**: `EpochTick::tick()` bumps the engine's
epoch, every running instance yields at its next check, and a module that exceeds its
tick allowance traps deterministically. Tick sources: a periodic job on a worker
pool's deadline queue (`PoolTicks`), or, with no pool, a dedicated parked thread
started on the first lens call (`ThreadTicks`) - a database that never runs a lens
starts no thread.

## Registration and use

Lens modules are registered over HTTP (`POST /lens`) and attached between versions
(`POST /lens/set` or the collections migration route). At query time the lens runs for
documents whose version differs from the target; explicit transactions pin their lens
chain (`PreparedLens`) so a multi-statement transaction sees one consistent transform.

> **Why transform-on-read instead of rewrite-on-migrate.** A rewrite is a full-dataset
> transaction: unbounded work, a fail window, and a rollback problem. Lenses make
> migration O(new writes), keep every old version intact (time travel still works),
> and let a bad migration be replaced (it is code, not data damage). The cost is
> transform work per read of an old document, paid only by readers that actually read
> old versions, and the epoch/fuel bounds keep that cost bounded per call.
