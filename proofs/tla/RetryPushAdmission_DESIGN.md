# Queued push admission

`Peerstore::admit_push_scope` reads the replicator, retry schedule and exact scope-marker generation from one snapshot. The recorder returns a generation only when those checks pass, and `run_push_job` treats absence as denial before transport I/O.

The model checks queued work across marker replacement, completion, forget/re-add and backpressure. GREEN requires a live peer and marker, the queued generation, and an elapsed deadline. Each RED removes one condition and must violate `INV_QueuedPushOwnsLiveMarker`.

This is a finite boundary model, not verification of Rust, storage snapshot isolation or transport authorization. Backpressure is abstracted to a Boolean. Admission is the linearization boundary: a later forget cannot retract already-admitted external I/O. The recorder-level regression checks the implementation using real peer storage.
