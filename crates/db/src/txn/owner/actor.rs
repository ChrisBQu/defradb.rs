//! One transaction's task on its owner, and the messages it takes.
//!
//! The task holds the transaction by value for its whole life and takes the
//! messages of its mailbox one at a time, in the order they arrived: a
//! request runs only after every request queued before it has finished, and
//! the end of the transaction (commit, rollback, abandon, an idle sweep)
//! comes after everything queued before that end. Nothing else ever touches
//! the transaction, so it needs no lock.
//!
//! Between requests the transaction waits in a one-place slot it shares with
//! its table entry, so an abandon from any thread drops an idle transaction
//! at once; a request finding the slot empty was abandoned. A commit or
//! rollback takes the entry out of the table before it is queued, so a
//! request either is queued before the end or finds nothing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use defra_core::thread_bounds::MaybeSend;
use futures::future::LocalBoxFuture;
use regolith::sync::{ArrayQueue, UnboundedReceiver, UnboundedSender};
use web_time::Instant;

use super::{Shared, TxnId};

/// A request's work on the transaction.
#[cfg(not(target_family = "wasm"))]
pub(super) type Work<T> = Box<dyn for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, ()> + Send>;
/// A request's work on the transaction.
#[cfg(target_family = "wasm")]
pub(super) type Work<T> = Box<dyn for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, ()>>;

/// The work that ends the transaction, given it by value.
#[cfg(not(target_family = "wasm"))]
pub(super) type Last<T> = Box<dyn FnOnce(T) -> LocalBoxFuture<'static, ()> + Send>;
/// The work that ends the transaction, given it by value.
#[cfg(target_family = "wasm")]
pub(super) type Last<T> = Box<dyn FnOnce(T) -> LocalBoxFuture<'static, ()>>;

pub(super) enum Message<T> {
    /// A request.
    Run(Work<T>),
    /// The transaction's end: commit or rollback. A caller that stopped
    /// waiting before its end began (`cancelled`) gets an abandon instead.
    Finish {
        last: Last<T>,
        cancelled: Arc<AtomicBool>,
    },
    /// The caller let go of the transaction without finishing it.
    Abandon,
    /// End the transaction if no request has finished for at least
    /// this long; the reply says whether it ended.
    Expire {
        max_idle: Duration,
        reply: Option<UnboundedSender<bool>>,
    },
}

/// A request's work, its signature fixed for every lifetime of the borrow.
pub(super) fn work<T, F>(work: F) -> Work<T>
where
    F: for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, ()> + MaybeSend + 'static,
{
    Box::new(work)
}

/// Runs the transaction waiting in `idle`, named `id`, on worker `worker`
/// until a message ends it, it is abandoned, or every sender of its mailbox
/// is gone.
/// Dropping `txn` is how it ends without a commit: its holder discards it
/// then. The task holds its registry weakly, so a dropped registry ends every
/// transaction it owned.
pub(super) async fn own<T, K>(
    idle: Arc<ArrayQueue<T>>,
    id: TxnId,
    worker: usize,
    mailbox: UnboundedReceiver<Message<T>>,
    shared: Weak<Shared<T, K>>,
) where
    T: 'static,
    K: Clone + PartialEq + 'static,
{
    let mut active = Instant::now();
    // Out of the table, then emptied: a request is sent on this worker, so
    // it is queued before this or finds no transaction. A message left in the
    // mailbox would keep its reply open until kovan reclaims the channel, so
    // each is dropped here and its sender hears the transaction is gone.
    let leave = || {
        if let Some(shared) = shared.upgrade() {
            shared.tables[worker].take(id);
        }
        while mailbox.try_recv().is_some() {}
    };
    while let Some(message) = mailbox.recv_async().await {
        match message {
            Message::Run(work) => {
                let Some(mut txn) = idle.pop() else { break };
                work(&mut txn).await;
                let _ = idle.push(txn);
                active = Instant::now();
            }
            Message::Finish { last, cancelled } => {
                if cancelled.load(Ordering::Acquire) {
                    break;
                }
                let Some(txn) = idle.pop() else { break };
                leave();
                last(txn).await;
                return;
            }
            Message::Abandon => break,
            Message::Expire { max_idle, reply } => {
                let stale = active.elapsed() >= max_idle;
                if let Some(reply) = reply {
                    let _ = reply.try_send(stale);
                }
                if stale {
                    break;
                }
            }
        }
    }
    // Ended before the mailbox empties, so a request still queued behind the
    // end is told "not found" only once the transaction is already gone.
    drop(idle.pop());
    leave();
}
