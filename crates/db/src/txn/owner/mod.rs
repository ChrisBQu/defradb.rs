//! Explicit transactions, each owned by one worker for its whole life.
//!
//! A transaction is begun on a worker of the pool and stays there: one task
//! on that worker (its actor, `actor`) holds it by value and is the only code
//! that ever touches it. Its ID names the worker (`id`), so a request for it
//! goes from the edge straight to that worker with `Target::Worker`, one hop
//! in and one out, and no shared lookup. On the worker the request finds the
//! actor's mailbox in the worker's own table (`table`) and queues there: the
//! mailbox order is the transaction's order, so requests run one at a time,
//! a commit or rollback runs after every request queued before it, and the
//! idle sweep's ask (`expire_idle`) ends a transaction only when it is the
//! next message and nothing admitted has run for the idle limit, so a request
//! that arrived first keeps its transaction.
//!
//! Nothing here knows what a transaction is: the registry gives the value
//! (`T`), how a request is admitted (its caller owns the transaction), what a
//! request does with it, and what ending it means. Dropping the value is how
//! a transaction ends without a commit (abandoned, swept, the registry or the
//! pool gone), so its holder's `Drop` discards it, every time.

mod actor;
mod id;
mod table;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use defra_core::thread_bounds::MaybeSend;
use futures::future::LocalBoxFuture;
use regolith::sync::{unbounded, ArrayQueue};
use workers::Target;
#[cfg(not(target_family = "wasm"))]
use workers::{Pool, Submit};

use actor::Message;
use table::{Entry, Table};

pub(crate) use id::TxnId;

/// Why a request could not reach its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unavailable {
    /// No such transaction on its owner: it never existed, it ended, or the
    /// caller does not own it.
    NotFound,
    /// Every transaction ID was handed out.
    Exhausted,
    /// The pool could not run the request.
    Pool(workers::Error),
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unavailable::NotFound => f.write_str("transaction not found"),
            Unavailable::Exhausted => f.write_str("no transaction IDs are left on this worker"),
            Unavailable::Pool(error) => write!(f, "{error}"),
        }
    }
}

impl From<workers::Error> for Unavailable {
    fn from(error: workers::Error) -> Self {
        Unavailable::Pool(error)
    }
}

/// Every worker's table, shared by the registry and the requests on their
/// way; actors hold it weakly, so dropping the registry ends every
/// transaction it owns.
pub(super) struct Shared<T, K>
where
    T: 'static,
    K: 'static,
{
    tables: Box<[Table<T, K>]>,
}

/// The transactions of one registry, spread over the pool's workers. Each is
/// kept with the key of the caller that may use it (`K`): a caller with
/// another key is told the transaction does not exist, before anything is
/// sent to its owner.
///
/// In a browser the page's thread is the one worker: every call already runs
/// there, so the work runs in place, and nothing needs to be `Send`.
pub(crate) struct Owners<T, K>
where
    T: MaybeSend + 'static,
    K: Clone + PartialEq + Send + Sync + 'static,
{
    #[cfg(not(target_family = "wasm"))]
    pool: Pool,
    shared: Arc<Shared<T, K>>,
}

impl<T, K> Owners<T, K>
where
    T: MaybeSend + 'static,
    K: Clone + PartialEq + Send + Sync + 'static,
{
    /// The transactions of a registry on the workers of `pool`.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn new(pool: Pool) -> Self {
        let tables = (0..pool.workers()).map(|_| Table::new()).collect();
        Self {
            pool,
            shared: Arc::new(Shared { tables }),
        }
    }

    /// The transactions of a registry on the page's one worker.
    #[cfg(target_family = "wasm")]
    pub(crate) fn here() -> Self {
        Self {
            shared: Arc::new(Shared {
                tables: Box::new([Table::new()]),
            }),
        }
    }

    /// How many workers own transactions.
    fn workers(&self) -> usize {
        self.shared.tables.len()
    }

    fn table(&self, id: TxnId) -> &Table<T, K> {
        &self.shared.tables[id.owner(self.workers())]
    }

    /// Runs the work `make` builds on `target`, a worker of the pool, and
    /// gives back its output.
    #[cfg(not(target_family = "wasm"))]
    async fn place<F, Fut>(
        &self,
        target: Target,
        name: &'static str,
        make: F,
    ) -> Result<Fut::Output, Unavailable>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        let placed = self.pool.submit_with(target, named(name), make).await?;
        Ok(placed.await?)
    }

    /// The page's one worker is the calling thread: the work runs in place.
    #[cfg(target_family = "wasm")]
    async fn place<F, Fut>(
        &self,
        _target: Target,
        _name: &'static str,
        make: F,
    ) -> Result<Fut::Output, Unavailable>
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future + 'static,
    {
        Ok(make().await)
    }

    /// Runs the work `make` builds on the least loaded worker, outside every
    /// transaction (a standalone read), and gives back its output.
    pub(crate) async fn run_anywhere<F, Fut>(
        &self,
        name: &'static str,
        make: F,
    ) -> Result<Fut::Output, Unavailable>
    where
        F: FnOnce() -> Fut + MaybeSend + 'static,
        Fut: Future + 'static,
        Fut::Output: MaybeSend + 'static,
    {
        self.place(Target::LeastLoaded, name, make).await
    }

    /// Opens a transaction with `open`, given its ID, on the least loaded
    /// worker, which owns it from then on for the caller `key` names, and
    /// returns the ID. An error from `open` comes back as is. A caller that
    /// stops waiting leaves a transaction nobody knows the ID of; the idle
    /// sweep ends it.
    pub(crate) async fn begin<O, Fut, E>(
        &self,
        key: K,
        open: O,
    ) -> Result<Result<TxnId, E>, Unavailable>
    where
        O: FnOnce(TxnId) -> Fut + MaybeSend + 'static,
        Fut: Future<Output = Result<T, E>> + 'static,
        E: MaybeSend + 'static,
    {
        let shared = Arc::downgrade(&self.shared);
        let workers = self.workers();
        self.place(Target::LeastLoaded, "txn.begin", move || async move {
            let worker = here()?;
            let sequence = match shared.upgrade() {
                Some(tables) => tables.tables[worker].draw(),
                None => return Err(Unavailable::NotFound),
            };
            let id = TxnId::new(sequence, worker, workers).ok_or(Unavailable::Exhausted)?;
            let txn = match open(id).await {
                Ok(txn) => txn,
                Err(error) => return Ok(Err(error)),
            };
            let (mailbox, inbox) = unbounded();
            let idle = Arc::new(ArrayQueue::new(1));
            // The slot is new and holds one: the push cannot find it full.
            let _ = idle.push(txn);
            match shared.upgrade() {
                Some(tables) => tables.tables[worker].insert(
                    id,
                    Entry {
                        mailbox,
                        idle: Arc::clone(&idle),
                        key,
                    },
                ),
                None => return Err(Unavailable::NotFound),
            }
            if let Err(error) = start(actor::own(idle, id, worker, inbox, shared.clone())) {
                if let Some(tables) = shared.upgrade() {
                    tables.tables[worker].take(id);
                }
                return Err(Unavailable::Pool(error));
            }
            Ok(Ok(id))
        })
        .await?
    }

    /// Runs `work` on transaction `id`, on its owner, after every request for
    /// it that arrived earlier and before every one that arrives later. A
    /// caller whose `key` does not own it gets [`Unavailable::NotFound`], as
    /// for a transaction that does not exist.
    pub(crate) async fn run<R, W>(&self, id: TxnId, key: &K, work: W) -> Result<R, Unavailable>
    where
        R: MaybeSend + 'static,
        W: for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, R> + MaybeSend + 'static,
    {
        if !self.table(id).admits(id, key) {
            return Err(Unavailable::NotFound);
        }
        self.send(id, "txn.request", move |reply| {
            Message::Run(actor::work(move |txn| {
                Box::pin(async move {
                    let _ = reply.try_send(work(txn).await);
                })
            }))
        })
        .await
    }

    /// Ends transaction `id` with `last`, which gets it by value, after every
    /// request for it that arrived earlier. The transaction leaves the table
    /// before this first waits, so requests that arrive later, and a racing
    /// end, find no transaction. A caller whose `key` does not own it is told
    /// it does not exist, and the transaction stays. A caller that stops
    /// waiting before the end begins abandons the transaction instead: it
    /// ends unfinished.
    pub(crate) async fn finish<R, L>(&self, id: TxnId, key: &K, last: L) -> Result<R, Unavailable>
    where
        R: MaybeSend + 'static,
        L: FnOnce(T) -> LocalBoxFuture<'static, R> + MaybeSend + 'static,
    {
        let table = self.table(id);
        if !table.admits(id, key) {
            return Err(Unavailable::NotFound);
        }
        let entry = table.take(id).ok_or(Unavailable::NotFound)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let abandon_on_drop = AbandonOnDrop {
            idle: Arc::clone(&entry.idle),
            cancelled: Some(Arc::clone(&cancelled)),
        };
        let (reply, answer) = unbounded();
        let queued = entry.mailbox.try_send(Message::Finish {
            last: Box::new(move |txn| {
                Box::pin(async move {
                    let _ = reply.try_send(last(txn).await);
                })
            }),
            cancelled,
        });
        let ended = match queued {
            Ok(()) => answer.recv_async().await.ok_or(Unavailable::NotFound),
            Err(_) => Err(Unavailable::NotFound),
        };
        abandon_on_drop.disarm();
        ended
    }

    /// Lets go of transaction `id` without finishing it, at once and without
    /// waiting: it leaves its owner's table, so no later request finds it and
    /// it no longer counts, and its owner ends it unfinished after the
    /// requests already queued for it. Safe from any thread and with no
    /// runtime (a guard's destructor).
    pub(crate) fn abandon(&self, id: TxnId) {
        self.table(id).abandon(id);
    }

    /// Ends every transaction no request has used for at least `max_idle`,
    /// each after the requests queued before this sweep reached it, and says
    /// how many ended.
    pub(crate) async fn expire_idle(&self, max_idle: Duration) -> Result<usize, Unavailable> {
        let sweeps = (0..self.workers()).map(|worker| {
            let shared = Arc::downgrade(&self.shared);
            self.place(Target::Worker(worker), "txn.sweep", move || async move {
                let Some(shared) = shared.upgrade() else {
                    return 0;
                };
                let (reply, answers) = unbounded();
                let asked = shared.tables[worker].send_all(|| Message::Expire {
                    max_idle,
                    reply: Some(reply.clone()),
                });
                drop(reply);
                drop(shared);
                let mut ended = 0;
                for _ in 0..asked {
                    match answers.recv_async().await {
                        Some(true) => ended += 1,
                        Some(false) => {}
                        None => break,
                    }
                }
                ended
            })
        });
        let mut ended = 0;
        for swept in futures::future::join_all(sweeps).await {
            ended += swept?;
        }
        Ok(ended)
    }

    /// How many transactions are open now, on every worker.
    pub(crate) fn len(&self) -> usize {
        self.shared.tables.iter().map(Table::len).sum()
    }

    /// Queues the message `build` makes for transaction `id` on its owner and
    /// waits for the reply the message carries back. No reply (the message
    /// was dropped, or never queued) means no such transaction.
    async fn send<R, B>(&self, id: TxnId, name: &'static str, build: B) -> Result<R, Unavailable>
    where
        R: MaybeSend + 'static,
        B: FnOnce(regolith::sync::UnboundedSender<R>) -> Message<T> + MaybeSend + 'static,
    {
        let shared = Arc::downgrade(&self.shared);
        let owner = id.owner(self.workers());
        self.place(Target::Worker(owner), name, move || {
            let (reply, answer) = unbounded();
            let queued = shared
                .upgrade()
                .is_some_and(|shared| shared.tables[owner].send(id, build(reply)).is_ok());
            async move {
                match queued {
                    true => answer.recv_async().await,
                    false => None,
                }
            }
        })
        .await?
        .ok_or(Unavailable::NotFound)
    }
}

impl<T, K> Drop for Owners<T, K>
where
    T: MaybeSend + 'static,
    K: Clone + PartialEq + Send + Sync + 'static,
{
    /// A registry that goes ends every transaction it owns.
    fn drop(&mut self) {
        self.shared.tables.iter().for_each(Table::abandon_all);
    }
}

/// While a finish waits: if its caller stops waiting, the transaction is
/// abandoned instead of finished, and dropped at once while still idle.
struct AbandonOnDrop<T>
where
    T: 'static,
{
    idle: Arc<ArrayQueue<T>>,
    cancelled: Option<Arc<AtomicBool>>,
}

impl<T> AbandonOnDrop<T>
where
    T: 'static,
{
    /// The finish got its answer: nothing to abandon.
    fn disarm(mut self) {
        self.cancelled = None;
    }
}

impl<T> Drop for AbandonOnDrop<T>
where
    T: 'static,
{
    fn drop(&mut self) {
        let Some(cancelled) = self.cancelled.take() else {
            return;
        };
        cancelled.store(true, Ordering::Release);
        drop(self.idle.pop());
    }
}

#[cfg(not(target_family = "wasm"))]
fn named(name: &'static str) -> Submit {
    Submit {
        name: Some(name),
        bytes: None,
    }
}

/// The index of the calling worker, which owns what it begins.
#[cfg(not(target_family = "wasm"))]
fn here() -> Result<usize, Unavailable> {
    workers::current_worker()
        .map(|worker| worker.index)
        .ok_or(Unavailable::Pool(workers::Error::NotOnWorker))
}

/// The page's thread is the one worker, whether or not it is driving the
/// pool when a transaction begins.
#[cfg(target_family = "wasm")]
fn here() -> Result<usize, Unavailable> {
    Ok(0)
}

/// Starts a transaction's actor on the calling worker, as a daemon: pool
/// shutdown does not wait for it, and it is dropped (its transaction
/// discarded) when the worker stops.
#[cfg(not(target_family = "wasm"))]
fn start<F>(actor: F) -> Result<(), workers::Error>
where
    F: Future<Output = ()> + 'static,
{
    workers::spawn_daemon(actor)
}

/// On the page's worker the actor is one of its daemons; called while the
/// page itself runs (a test, a call that did not come through the pool) the
/// browser's executor runs it, on the same thread.
#[cfg(target_family = "wasm")]
fn start<F>(actor: F) -> Result<(), workers::Error>
where
    F: Future<Output = ()> + 'static,
{
    if workers::on_worker() {
        return workers::spawn_daemon(actor);
    }
    drop(n0_future::task::spawn(actor));
    Ok(())
}
