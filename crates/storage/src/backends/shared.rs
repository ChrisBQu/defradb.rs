//! Pieces every store needs that are not the store itself: transaction
//! lifecycle callbacks, and the diagnostics a node reports.
//!
//! What used to live here was a second concurrency-control implementation
//! layered over backends that had none: a conflict tracker, a read set, a
//! publication gate, and the version bookkeeping to drive them. regolith
//! validates its own transactions, so all of it is gone. What remains is
//! the callback bookkeeping DefraDB's transaction API promises, and
//! counters for what actually happened.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use kovan_queue::seg_queue::SegQueue;
use serde::{Deserialize, Serialize};

use crate::corekv::{AsyncTxnCallback, TxnCallback};

/// When a write is made durable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum DurabilityMode {
    /// Fsync before the commit returns, so a committed write survives a
    /// power cut.
    #[default]
    Immediate,
    /// Hand the write to the kernel without an fsync. Survives a process
    /// crash, not a power cut.
    Eventual,
}

/// How many callbacks of each kind a transaction is carrying.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CallbackCounts {
    /// Synchronous success callbacks.
    pub success: usize,
    /// Asynchronous success callbacks.
    pub success_async: usize,
    /// Synchronous error callbacks.
    pub error: usize,
    /// Asynchronous error callbacks.
    pub error_async: usize,
    /// Synchronous discard callbacks.
    pub discard: usize,
    /// Asynchronous discard callbacks.
    pub discard_async: usize,
}

impl CallbackCounts {
    /// Callbacks registered across every kind.
    pub fn total(&self) -> usize {
        self.success
            + self.success_async
            + self.error
            + self.error_async
            + self.discard
            + self.discard_async
    }
}

/// The six queues, allocated together the first time one is used.
#[derive(Default)]
struct CallbackLists {
    success: SegQueue<TxnCallback>,
    success_async: SegQueue<AsyncTxnCallback>,
    error: SegQueue<TxnCallback>,
    error_async: SegQueue<AsyncTxnCallback>,
    discard: SegQueue<TxnCallback>,
    discard_async: SegQueue<AsyncTxnCallback>,
}

/// Callbacks a transaction runs when it resolves.
///
/// Registration takes `&self` because a transaction is shareable. Each list
/// is a FIFO queue so callbacks run in registration order without a lock.
///
/// The queues are built on the first registration rather than with the
/// transaction. `SegQueue::new` allocates a 32-slot segment up front and
/// frees it through an epoch-pinned walk, and a transaction carries six of
/// them, so building them eagerly cost a read-only transaction (which
/// registers nothing) six allocations, six frees and seven epoch pins on a
/// path whose whole budget is a few hundred nanoseconds. Behind the box this
/// is two words in the transaction, and an unused one is an atomic load.
#[derive(Default)]
pub(crate) struct CallbackManager {
    lists: OnceLock<Box<CallbackLists>>,
}

impl CallbackManager {
    fn lists(&self) -> &CallbackLists {
        self.lists.get_or_init(Box::default)
    }

    fn registered(&self) -> Option<&CallbackLists> {
        self.lists.get().map(Box::as_ref)
    }

    pub(crate) fn on_success(&self, callback: TxnCallback) {
        self.lists().success.push(callback);
    }

    pub(crate) fn on_success_async(&self, callback: AsyncTxnCallback) {
        self.lists().success_async.push(callback);
    }

    pub(crate) fn on_error(&self, callback: TxnCallback) {
        self.lists().error.push(callback);
    }

    pub(crate) fn on_error_async(&self, callback: AsyncTxnCallback) {
        self.lists().error_async.push(callback);
    }

    pub(crate) fn on_discard(&self, callback: TxnCallback) {
        self.lists().discard.push(callback);
    }

    pub(crate) fn on_discard_async(&self, callback: AsyncTxnCallback) {
        self.lists().discard_async.push(callback);
    }

    pub(crate) fn counts(&self) -> CallbackCounts {
        let Some(lists) = self.registered() else {
            return CallbackCounts::default();
        };
        CallbackCounts {
            success: lists.success.len(),
            success_async: lists.success_async.len(),
            error: lists.error.len(),
            error_async: lists.error_async.len(),
            discard: lists.discard.len(),
            discard_async: lists.discard_async.len(),
        }
    }

    /// Drop callbacks registered after a transaction savepoint.
    pub(crate) fn truncate(&self, counts: CallbackCounts) {
        let Some(lists) = self.registered() else {
            return;
        };
        use crate::corekv::truncate_queue as trim;
        trim(&lists.success, counts.success);
        trim(&lists.success_async, counts.success_async);
        trim(&lists.error, counts.error);
        trim(&lists.error_async, counts.error_async);
        trim(&lists.discard, counts.discard);
        trim(&lists.discard_async, counts.discard_async);
    }

    pub(crate) fn total(&self) -> usize {
        self.counts().total()
    }

    /// Run the success callbacks, synchronous ones first.
    pub(crate) async fn run_success(&self) {
        let Some(lists) = self.registered() else {
            return;
        };
        Self::run(&lists.success);
        while let Some(callback) = lists.success_async.pop() {
            callback().await;
        }
    }

    /// Run the error callbacks.
    pub(crate) async fn run_error(&self) {
        let Some(lists) = self.registered() else {
            return;
        };
        Self::run(&lists.error);
        while let Some(callback) = lists.error_async.pop() {
            callback().await;
        }
    }

    /// Run the discard callbacks.
    ///
    /// `discard` is synchronous in DefraDB's API, so an async callback
    /// cannot be awaited here. On a runtime it is spawned, which is the
    /// long-standing fire-and-forget contract. Without one there is
    /// nowhere to run it, and that is said out loud rather than dropped
    /// quietly.
    pub(crate) fn run_discard(&self) {
        let Some(lists) = self.registered() else {
            return;
        };
        Self::run(&lists.discard);
        if lists.discard_async.is_empty() {
            return;
        }
        let mut pending = Vec::with_capacity(lists.discard_async.len());
        while let Some(callback) = lists.discard_async.pop() {
            pending.push(callback);
        }
        #[cfg(not(target_arch = "wasm32"))]
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    for callback in pending {
                        callback().await;
                    }
                });
            }
            Err(_) => tracing::warn!(
                count = pending.len(),
                "async discard callbacks dropped: discard() ran outside a tokio runtime"
            ),
        }
        #[cfg(target_arch = "wasm32")]
        tracing::warn!(
            count = pending.len(),
            "async discard callbacks dropped: no runtime to spawn them on"
        );
    }

    fn run(callbacks: &SegQueue<TxnCallback>) {
        while let Some(callback) = callbacks.pop() {
            callback();
        }
    }
}

/// Engine-owned commit/policy counters and listener-owned conflict reasons.
/// Snapshots are observational: concurrent commits may advance between reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransactionStatsSnapshot {
    pub backend: &'static str,
    /// Native transaction commits; read-only adapter snapshots do not commit.
    pub commits: u64,
    pub conflicts: u64,
    /// Regolith's commit.* and policy.* tickers, using their native names.
    pub engine: std::collections::BTreeMap<&'static str, u64>,
    /// Conflict counts by losing access and winning write, without key data.
    pub conflict_reasons: std::collections::BTreeMap<String, u64>,
}

const ACCESS_LABELS: [&str; 10] = [
    "read",
    "read_parts",
    "read_presence",
    "read_for_update",
    "scanned_then_wrote",
    "scanned_range",
    "put",
    "delete",
    "merge",
    "other",
];
const WRITE_LABELS: [&str; 5] = ["put", "delete", "range_delete", "merge", "other"];

struct TransactionMetrics {
    engine: Arc<regolith::Statistics>,
    reasons: [[AtomicU64; 5]; 10],
    /// Held, never read: the engine's tickers stay in the OpenTelemetry
    /// export while this store's diagnostics are reachable.
    _export: telemetry::EngineTickerExport,
}

/// Cloneable handle for reading a live store's engine diagnostics.
#[derive(Clone)]
pub struct TransactionStatsHandle {
    metrics: Arc<TransactionMetrics>,
}

impl TransactionStatsHandle {
    pub(crate) fn from_engine(engine: Arc<regolith::Statistics>) -> Self {
        Self {
            metrics: Arc::new(TransactionMetrics {
                _export: telemetry::export_engine_tickers(&engine),
                engine,
                reasons: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            }),
        }
    }

    pub(crate) fn record_conflict(&self, conflict: &regolith::Conflict) {
        use regolith::{Access, WriteKind};
        let mine = match conflict.mine() {
            Access::Read => 0,
            Access::ReadParts => 1,
            Access::ReadPresence => 2,
            Access::ReadForUpdate => 3,
            Access::ScannedThenWrote => 4,
            Access::ScannedRange => 5,
            Access::Put => 6,
            Access::Delete => 7,
            Access::Merge => 8,
            _ => 9,
        };
        let theirs = match conflict.theirs() {
            WriteKind::Put => 0,
            WriteKind::Delete => 1,
            WriteKind::RangeDelete => 2,
            WriteKind::Merge => 3,
            _ => 4,
        };
        self.metrics.reasons[mine][theirs].fetch_add(1, Ordering::Relaxed);
        telemetry::record_storage_conflict("regolith", ACCESS_LABELS[mine], WRITE_LABELS[theirs]);
    }

    pub fn snapshot(&self) -> TransactionStatsSnapshot {
        let engine = &self.metrics.engine;
        let mut conflict_reasons = std::collections::BTreeMap::new();
        for (mine, access) in ACCESS_LABELS.iter().enumerate() {
            for (theirs, write) in WRITE_LABELS.iter().enumerate() {
                let count = self.metrics.reasons[mine][theirs].load(Ordering::Relaxed);
                if count != 0 {
                    conflict_reasons.insert(format!("{access}/{write}"), count);
                }
            }
        }
        TransactionStatsSnapshot {
            backend: "regolith",
            commits: engine.get_ticker(regolith::Ticker::CommitCount),
            conflicts: engine.get_ticker(regolith::Ticker::CommitConflicts),
            engine: telemetry::exported_engine_tickers()
                .map(|ticker| (ticker.name(), engine.get_ticker(ticker)))
                .collect(),
            conflict_reasons,
        }
    }
}
