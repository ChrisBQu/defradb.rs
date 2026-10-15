//! Post-commit work that failed after its commit was durable.
//!
//! A durable commit's outcome stands. A hook that fails, panics or is
//! cancelled, an event that panics, and a regolith callback that panics
//! after its outcome are each reported here, as one warn line and one count
//! of `defradb.storage.events` by kind, and never turned into a failed write
//! or raised in another caller.

use telemetry::StorageEvent;

pub(super) fn report(kind: StorageEvent, detail: &dyn std::fmt::Display) {
    tracing::warn!(
        kind = kind.label(),
        %detail,
        "Post-commit work failed after its commit was durable; the commit stands"
    );
    telemetry::record_storage_event(kind, 0);
}
