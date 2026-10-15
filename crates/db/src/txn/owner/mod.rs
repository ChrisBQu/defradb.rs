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
//! idle sweep, a timer on each worker (`every`), ends a transaction only when
//! the sweep's message is the next one and nothing admitted has run for the
//! idle limit, so a request that arrived first keeps its transaction.
//!
//! Nothing here knows what a transaction is: the registry gives the value
//! (`T`), how a request is admitted (its caller owns the transaction), what a
//! request does with it, and what ending it means. Dropping the value is how
//! a transaction ends without a commit (abandoned, swept, the registry or the
//! pool gone), so its holder's `Drop` discards it, every time.

mod actor;
mod fallback;
mod id;
mod table;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::future::LocalBoxFuture;
use regolith::sync::{unbounded, Event};
use workers::{Pool, Submit, Target};

use actor::Message;
use table::Table;

pub(crate) use fallback::one_worker;
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
/// way; actors and sweeps hold it weakly, so dropping the registry ends
/// every transaction it owns.
pub(super) struct Shared<T>
where
    T: 'static,
{
    tables: Box<[Table<T>]>,
}

/// The transactions of one registry, spread over the pool's workers.
pub(crate) struct Owners<T>
where
    T: 'static,
{
    pool: Pool,
    shared: Arc<Shared<T>>,
}

impl<T> Owners<T>
where
    T: 'static,
{
    pub(crate) fn new(pool: Pool) -> Self {
        let tables = (0..pool.workers()).map(|_| Table::new()).collect();
        Self {
            pool,
            shared: Arc::new(Shared { tables }),
        }
    }

    /// The pool the transactions live on.
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Opens a transaction with `open`, given its ID, on the least loaded
    /// worker, which owns it from then on, and returns the ID. An error from
    /// `open` comes back as is. A caller that stops waiting leaves a
    /// transaction nobody knows the ID of; the idle sweep ends it.
    pub(crate) async fn begin<O, Fut, E>(&self, open: O) -> Result<Result<TxnId, E>, Unavailable>
    where
        O: FnOnce(TxnId) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + 'static,
        E: Send + 'static,
    {
        let shared = Arc::downgrade(&self.shared);
        let workers = self.pool.workers();
        let opened = self
            .pool
            .submit_with(
                Target::LeastLoaded,
                named("txn.begin"),
                move || async move {
                    let worker = workers::current_worker()
                        .map(|worker| worker.index)
                        .ok_or(Unavailable::Pool(workers::Error::NotOnWorker))?;
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
                    match shared.upgrade() {
                        Some(tables) => tables.tables[worker].insert(id, mailbox),
                        None => return Err(Unavailable::NotFound),
                    }
                    if let Err(error) =
                        workers::spawn_daemon(actor::own(txn, id, worker, inbox, shared.clone()))
                    {
                        if let Some(tables) = shared.upgrade() {
                            tables.tables[worker].remove(id);
                        }
                        return Err(Unavailable::Pool(error));
                    }
                    Ok(Ok(id))
                },
            )
            .await?;
        opened.await?
    }

    /// Runs `work` on transaction `id`, on its owner, after every request for
    /// it that arrived earlier and before every one that arrives later.
    /// `admit` decides first whether the caller may use it; a refused caller
    /// gets [`Unavailable::NotFound`], as for a transaction that does not
    /// exist.
    pub(crate) async fn run<R, A, W>(&self, id: TxnId, admit: A, work: W) -> Result<R, Unavailable>
    where
        R: Send + 'static,
        A: FnOnce(&T) -> bool + Send + 'static,
        W: for<'t> FnOnce(&'t mut T) -> LocalBoxFuture<'t, R> + Send + 'static,
    {
        self.send(id, "txn.request", move |reply| Message::Run {
            admit: Box::new(admit),
            work: actor::work(move |txn| {
                Box::pin(async move {
                    let _ = reply.try_send(work(txn).await);
                })
            }),
        })
        .await
    }

    /// Ends transaction `id` with `last`, which gets it by value, after every
    /// request for it that arrived earlier. Requests that arrive later find
    /// no transaction. A caller `admit` refuses leaves it as it was.
    pub(crate) async fn finish<R, A, L>(
        &self,
        id: TxnId,
        admit: A,
        last: L,
    ) -> Result<R, Unavailable>
    where
        R: Send + 'static,
        A: FnOnce(&T) -> bool + Send + 'static,
        L: FnOnce(T) -> LocalBoxFuture<'static, R> + Send + 'static,
    {
        self.send(id, "txn.finish", move |reply| Message::Finish {
            admit: Box::new(admit),
            last: Box::new(move |txn| {
                Box::pin(async move {
                    let _ = reply.try_send(last(txn).await);
                })
            }),
        })
        .await
    }

    /// Lets go of transaction `id` without finishing it, at once and without
    /// waiting: it leaves its owner's table, so no later request finds it and
    /// it no longer counts, and its owner ends it unfinished after the
    /// requests already queued for it. Safe from any thread and with no
    /// runtime (a guard's destructor).
    pub(crate) fn abandon(&self, id: TxnId) {
        self.shared.tables[id.owner(self.pool.workers())].abandon(id);
    }

    /// Ends every transaction no admitted request has used for at least
    /// `max_idle`, each after the requests queued before this sweep reached
    /// it, and says how many ended.
    pub(crate) async fn expire_idle(&self, max_idle: Duration) -> Result<usize, Unavailable> {
        let mut replies = Vec::with_capacity(self.pool.workers());
        for worker in 0..self.pool.workers() {
            let shared = Arc::downgrade(&self.shared);
            let completion = self
                .pool
                .submit_with(
                    Target::Worker(worker),
                    named("txn.sweep"),
                    move || async move {
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
                    },
                )
                .await?;
            replies.push(completion);
        }
        let mut ended = 0;
        for reply in replies {
            ended += reply.await?;
        }
        Ok(ended)
    }

    /// Starts the idle sweep, a timer on each worker: every `every`, the
    /// worker asks each of its own transactions to end if no admitted request
    /// has used it for `max_idle`. The ask is a message, queued behind the
    /// requests already there. Runs until [`Sweep::stop`], or the registry or
    /// the pool goes; dropping the handle leaves it running.
    pub(crate) fn sweep(&self, max_idle: Duration, every: Duration) -> Result<Sweep, Unavailable> {
        let stop = Arc::new(Event::new());
        for worker in 0..self.pool.workers() {
            let shared = Arc::downgrade(&self.shared);
            let stopped = stop.clone();
            // The start runs on its worker by itself; nothing waits for it.
            drop(self.pool.try_submit_with(
                Target::Worker(worker),
                named("txn.sweep.start"),
                move || {
                    std::future::ready(workers::spawn_daemon(async move {
                        loop {
                            let Ok(tick) = workers::sleep(every) else {
                                return;
                            };
                            let woke =
                                futures::future::select(tick, Box::pin(stopped.wait())).await;
                            if matches!(woke, futures::future::Either::Right(_)) {
                                return;
                            }
                            let Some(shared) = shared.upgrade() else {
                                return;
                            };
                            shared.tables[worker].send_all(|| Message::Expire {
                                max_idle,
                                reply: None,
                            });
                        }
                    }))
                },
            )?);
        }
        Ok(Sweep { stop })
    }

    /// How many transactions are open now, on every worker.
    pub(crate) fn len(&self) -> usize {
        self.shared.tables.iter().map(Table::len).sum()
    }

    /// Queues the message `build` makes for transaction `id` on its owner and
    /// waits for the reply the message carries back. No reply (the message
    /// was refused or never queued) means no such transaction.
    async fn send<R, B>(&self, id: TxnId, name: &'static str, build: B) -> Result<R, Unavailable>
    where
        R: Send + 'static,
        B: FnOnce(regolith::sync::UnboundedSender<R>) -> Message<T> + Send + 'static,
    {
        let shared = Arc::downgrade(&self.shared);
        let owner = id.owner(self.pool.workers());
        let replied = self
            .pool
            .submit_with(Target::Worker(owner), named(name), move || {
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
            .await?;
        replied.await?.ok_or(Unavailable::NotFound)
    }
}

impl<T> Drop for Owners<T>
where
    T: 'static,
{
    /// A registry that goes ends every transaction it owns.
    fn drop(&mut self) {
        self.shared.tables.iter().for_each(Table::abandon_all);
    }
}

/// The idle sweep's handle. Dropping it leaves the sweep running.
pub struct Sweep {
    stop: Arc<Event>,
}

impl Sweep {
    /// A sweep that does not run.
    pub fn stopped() -> Self {
        let stop = Arc::new(Event::new());
        stop.set();
        Self { stop }
    }

    /// Stops the sweep on every worker.
    pub fn stop(&self) {
        self.stop.set();
    }

    /// [`stop`](Self::stop), under the name a spawned task's handle uses.
    pub fn abort(&self) {
        self.stop();
    }
}

fn named(name: &'static str) -> Submit {
    Submit {
        name: Some(name),
        bytes: None,
    }
}
