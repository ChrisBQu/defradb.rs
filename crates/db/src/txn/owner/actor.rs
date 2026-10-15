//! One transaction's task on its owner, and the messages it takes.
//!
//! The task holds the transaction by value for its whole life and takes the
//! messages of its mailbox one at a time, in the order they arrived: a
//! request runs only after every request queued before it has finished, and
//! the end of the transaction (commit, rollback, abandon, an idle sweep)
//! comes after everything queued before that end. Nothing else ever touches
//! the transaction, so it needs no lock.
//!
//! The task leaves its worker's table in the same poll that decides the
//! transaction ends, so a request either finds the mailbox and is queued
//! before the end, or finds nothing.

use std::sync::Weak;
use std::time::Duration;

use futures::future::LocalBoxFuture;
use regolith::sync::{UnboundedReceiver, UnboundedSender};
use web_time::Instant;

use super::{Shared, TxnId};

/// A request's work on the transaction.
pub(super) type Work<T> = Box<dyn for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, ()> + Send>;

/// The work that ends the transaction, given it by value.
pub(super) type Last<T> = Box<dyn FnOnce(T) -> LocalBoxFuture<'static, ()> + Send>;

/// Whether a request may use the transaction (its caller owns it). A refused
/// request is dropped unrun, and its caller is told the transaction is not
/// found; it does not count as activity.
pub(super) type Admit<T> = Box<dyn FnOnce(&T) -> bool + Send>;

pub(super) enum Message<T> {
    /// A request.
    Run { admit: Admit<T>, work: Work<T> },
    /// The transaction's end: commit or rollback.
    Finish { admit: Admit<T>, last: Last<T> },
    /// The caller let go of the transaction without finishing it.
    Abandon,
    /// End the transaction if no admitted request has finished for at least
    /// this long; the reply says whether it ended.
    Expire {
        max_idle: Duration,
        reply: Option<UnboundedSender<bool>>,
    },
}

/// A request's work, its signature fixed for every lifetime of the borrow.
pub(super) fn work<T, F>(work: F) -> Work<T>
where
    F: for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, ()> + Send + 'static,
{
    Box::new(work)
}

/// Runs the transaction `txn` named `id` on worker `worker` until a message
/// ends it or every sender of its mailbox is gone (the registry was dropped).
/// Dropping `txn` is how it ends without a commit: its holder discards it
/// then. The task holds its registry weakly, so a dropped registry ends every
/// transaction it owned.
pub(super) async fn own<T>(
    mut txn: T,
    id: TxnId,
    worker: usize,
    mailbox: UnboundedReceiver<Message<T>>,
    shared: Weak<Shared<T>>,
) where
    T: 'static,
{
    let mut active = Instant::now();
    let leave = || {
        if let Some(shared) = shared.upgrade() {
            shared.tables[worker].remove(id);
        }
    };
    while let Some(message) = mailbox.recv_async().await {
        match message {
            Message::Run { admit, work } => {
                if admit(&txn) {
                    work(&mut txn).await;
                    active = Instant::now();
                }
            }
            Message::Finish { admit, last } => {
                if admit(&txn) {
                    leave();
                    last(txn).await;
                    return;
                }
            }
            Message::Abandon => break,
            Message::Expire { max_idle, reply } => {
                let idle = active.elapsed() >= max_idle;
                if let Some(reply) = reply {
                    let _ = reply.try_send(idle);
                }
                if idle {
                    break;
                }
            }
        }
    }
    leave();
    // Ended before the mailbox goes, so a request still queued behind the end
    // is told "not found" only once the transaction is already gone.
    drop(txn);
    drop(mailbox);
}
