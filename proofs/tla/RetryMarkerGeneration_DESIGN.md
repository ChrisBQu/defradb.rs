# Peer retry marker generations

The model anchors to `crates/storage/src/stores/peerstore.rs`: registration
allocates and stores a fresh generation in `RegisterScopeMarkerAttempt`,
`complete_retry_scope` compare-deletes only the generation it observed, and
`clear_retry_peer_once` reads the persisted replicator key in the transaction
that clears retry metadata. Forget/re-add changes the peer incarnation and
must invalidate an older clear transaction.

GREEN checks that an old acknowledgement cannot erase a newer marker and that
a clear begun for an old peer incarnation cannot finish after forget/re-add.
The two RED configurations remove one fence at a time. This small-state model
does not model storage isolation internals; it expresses the logical generation
and incarnation preconditions checked by the transaction adapter.
