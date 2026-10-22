# Memory Reclamation

defradb.rs holds a lot of derived data: every version of every field, index entries,
blocks shared across documents, advisory counters. This chapter is how data *leaves*,
and why nothing leaves by accident.

## No compaction filter, on principle

The engine offers a compaction filter (drop entries during SST compaction). defradb.rs
installs none, and the reasoning is written beside the decision
(`crates/storage/src/backends/regolith/mod.rs`):

- a head-set entry's meaning depends on writes that follow it;
- nothing expires by time alone (a pending DAG record leaves when its root merges or
  is quarantined; downsample retention is cut from other keys);
- a document part's tombstone carries the priority a late replicated merge is ordered
  against - dropping it would resurrect deleted data;
- index entries, blocks, ACP tuples and guards each *decide something* until another
  key says otherwise.

A filter that dropped any of them would change what a reader, or a later merge, sees.
Retirement is a business decision with a policy behind it, and those run as ordinary
transactions:

## Block ownership

Blocks are shared: the same field block can back multiple documents' histories. The
systemstore keeps a CID -> DocID ownership map (`/d/b/{cid}/{docID}`), written at every
create and merge. Deleting history removes *ownership* first
(`crates/db/src/block/cleanup.rs`): a block's bytes die only when its last owner
mapping is gone and it is not reachable from anything retained. `referenced_blocks`
computes the reachable set (commit + links + encryption links) of what stays; deletes
keep blocks in that set even when their own owner is gone - the fix for the class of
bug where pruning one document's history pulled a block out from under another's.

## Downsample GC

Downsampled collections roll source documents into windowed targets; retention then
prunes source history (`crates/db/src/downsample/gc.rs`):

- plans are grouped per source; a source with any consumer lacking a retention policy
  is skipped entirely (never partially pruned);
- a height is pruneable only when *every* plan for the source agrees (the
  intersection), and only below its retention cutoff;
- current head CIDs are never pruned (they are the present, not history);
- pruned composite commits lose their priority-index entries and their ownership, with
  field blocks deleted only when unreferenced by anything kept;
- the whole prune is one transaction, discarded on error: a half-pruned history is
  never published.

## Collection delete and truncate

Whole-collection removal walks the full reachable DAG and deletes unowned, unretained
blocks (`delete_owned_dag`); truncate does the same filtered by a predicate. Index
entries go page by page through the index manager.

## Tombstones and markers

Deletion markers (`d/del/`) are read before materializing a document and survive until
their history is pruned. LWW tombstones (empty bytes, priority intact) survive in the
stored document for the same reason: a late merge from a replica must still lose to
the delete.

## Memory in the small

Reclamation of *RAM* follows the same bounded-by-design rules: iterators page by bytes
and entries, streaming reads hold a page, caches are byte-bounded (the KMS tier, the
vector code caches), the legacy-store rewrite copies in bounded batches, and the
store's statistics deliberately keep no histograms (the LSM chapter). The wasm and
memory environments size to their profiles.

> **Why explicit ownership instead of refcounting or GC.** Reference counting says "no
> one pointed at it *at that instant*"; replication makes that instant meaningless (a
> peer may re-deliver the block later). Ownership mappings are durable, replicated
> facts, pruned by transactions that validate what stays - so "may I delete this
> block?" is answered by the same machinery that answers every other question, and a
> wrong answer loses a commit instead of losing data.
