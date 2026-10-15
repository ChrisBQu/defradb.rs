//! The transactions a store has handed out and not yet seen end.
//!
//! `close` waits for them on a [`Latch`] the last one counts down, with a
//! deadline, rather than polling a count. Every transaction holds a
//! [`TxnSlot`] for as long as it lives, its commit and post-commit work
//! included, and gives it back by dropping it, so a slot is returned exactly
//! once on every path.
//!
//! The count and the closing flag are read in opposite orders by `close`
//! and by the transaction that ends last (each writes one, then reads the
//! other, all sequentially consistent), so at least one of them sees the
//! other: either `close` finds nothing open, or the last transaction finds
//! the store closing and opens the latch.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use regolith::sync::Latch;

use crate::corekv::{Error, Result};

pub(super) struct TxnSlots {
    open: AtomicUsize,
    closing: AtomicBool,
    idle: Latch,
}

/// One transaction's place in its store's count, given back on drop.
pub(super) struct TxnSlot(Arc<TxnSlots>);

impl Drop for TxnSlot {
    fn drop(&mut self) {
        if self.0.open.fetch_sub(1, Ordering::SeqCst) == 1 && self.0.closing.load(Ordering::SeqCst)
        {
            self.0.idle.count_down();
        }
    }
}

impl TxnSlots {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            open: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            idle: Latch::new(1),
        })
    }

    /// A slot for a new transaction, refused once the store is closing.
    pub(super) fn enter(self: &Arc<Self>) -> Result<TxnSlot> {
        if self.is_closing() {
            return Err(Error::DBClosed);
        }
        self.open.fetch_add(1, Ordering::SeqCst);
        let slot = TxnSlot(Arc::clone(self));
        // Closing between the check and the count: the slot handed back
        // here is what `close` may be waiting for.
        if self.is_closing() {
            return Err(Error::DBClosed);
        }
        Ok(slot)
    }

    pub(super) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    /// Marks the store closing; `false` when it already was.
    pub(super) fn begin_close(&self) -> bool {
        !self.closing.swap(true, Ordering::SeqCst)
    }

    /// Waits until every slot is given back, or `timeout` passes.
    pub(super) async fn quiesce(&self, timeout: Duration) -> Result<()> {
        if self.open.load(Ordering::SeqCst) == 0 {
            return Ok(());
        }
        // vertexia: the deadline uses the edge runtime's timer through
        // n0_future; the worker pool's deadline queue replaces it (step 9).
        n0_future::time::timeout(timeout, self.idle.wait())
            .await
            .map_err(|_| {
                Error::Backend(format!(
                    "closed with {} transaction(s) still in flight",
                    self.open.load(Ordering::SeqCst)
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_taken_while_closing_is_refused_and_given_back() {
        let slots = TxnSlots::new();
        let held = slots.enter().unwrap();
        assert!(slots.begin_close());
        assert!(!slots.begin_close());
        assert!(matches!(slots.enter(), Err(Error::DBClosed)));
        assert_eq!(slots.open.load(Ordering::SeqCst), 1);
        assert!(!slots.idle.is_set());
        drop(held);
        assert!(slots.idle.is_set());
    }

    #[test]
    fn slots_given_back_before_close_leave_the_latch_shut() {
        let slots = TxnSlots::new();
        drop(slots.enter().unwrap());
        assert!(!slots.idle.is_set());
        assert!(slots.begin_close());
        assert_eq!(slots.open.load(Ordering::SeqCst), 0);
    }
}
