//! The store's database-wide transaction hooks: how each transaction ended.
//!
//! regolith runs exactly one of `on_commit` and `on_abort` for every
//! transaction of the database, whatever ends it. These count each outcome
//! as `defradb.storage.events` by kind: a commit, or an abort by its reason.
//! A dropped transaction is not counted: every retry attempt that opens its
//! scoped IO drops a stand-in transaction, so that count would say nothing.

use regolith::{AbortReason, CommitInfo, TransactionHooks};
use telemetry::StorageEvent;

pub(super) struct StoreHooks;

impl TransactionHooks for StoreHooks {
    fn on_commit(&self, _info: &CommitInfo) {
        telemetry::record_storage_event(StorageEvent::Commit, 0);
    }

    fn on_abort(&self, reason: &AbortReason<'_>) {
        let event = match reason {
            AbortReason::Dropped => return,
            AbortReason::Rollback => StorageEvent::AbortRollback,
            AbortReason::Conflict(_) => StorageEvent::AbortConflict,
            AbortReason::Closed => StorageEvent::AbortClosed,
            AbortReason::CallbackPanicked => StorageEvent::AbortCallbackPanicked,
            _ => StorageEvent::AbortError,
        };
        telemetry::record_storage_event(event, 0);
    }
}
