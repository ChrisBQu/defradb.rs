//! A transaction held by one handle that may also give it back (a fetcher or
//! a mutator its caller commits through).
//!
//! The handle keeps the transaction's strong reference in a take-only slot
//! and reaches it, for each operation, through a weak one. Giving it back
//! takes the strong reference out of the slot and unwraps it: that succeeds
//! only once no other handle and no operation in flight holds the
//! transaction, and otherwise puts the reference back and says so. Nothing
//! waits and nothing locks; an operation that starts after the transaction
//! was given back finds it gone.

use std::sync::{Arc, Weak};

use regolith::sync::SegQueue;
use storage::corekv::Store;

use super::{DbTxn, LentTxn};

pub(crate) struct Held<S>
where
    S: Store + 'static,
{
    strong: SegQueue<LentTxn<S>>,
    weak: Weak<DbTxn<S>>,
}

impl<S> Held<S>
where
    S: Store + 'static,
{
    /// Holds `txn`, which other handles may share too.
    pub(crate) fn new(txn: LentTxn<S>) -> Self {
        let weak = Arc::downgrade(&txn);
        let strong = SegQueue::new();
        strong.push(txn);
        Self { strong, weak }
    }

    /// A handle that holds no transaction: every operation finds none.
    pub(crate) fn none() -> Self {
        Self {
            strong: SegQueue::new(),
            weak: Weak::new(),
        }
    }

    /// The transaction, for one operation or for another handle to share.
    /// `None` once it was given back.
    pub(crate) fn get(&self) -> Option<LentTxn<S>> {
        self.weak.upgrade()
    }

    /// Gives the transaction back, by value. `None` when it was given back
    /// already, or while another handle or an operation still holds it, in
    /// which case it stays held here.
    pub(crate) fn take(&self) -> Option<DbTxn<S>> {
        let txn = self.strong.pop()?;
        match Arc::try_unwrap(txn) {
            Ok(txn) => Some(txn),
            Err(txn) => {
                self.strong.push(txn);
                None
            }
        }
    }

    /// Whether the transaction was given back.
    pub(crate) fn is_taken(&self) -> bool {
        self.weak.strong_count() == 0
    }
}
