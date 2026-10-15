//! A worker: one thread running a local executor (or, in driven mode, a
//! caller's thread driving one). Tasks are spawned onto it with
//! [`spawn_local`] and run there until they finish: no task stealing, and a
//! waker from any thread puts its task back on this worker's run queue.
//!
//! Each round a worker takes a batch from its inboxes, polls a batch of ready
//! tasks (each under `catch_unwind`, publishing a heartbeat), polls its hook
//! (the regolith `IoQueue`) between tasks, and fires due timers. With nothing
//! to do it spins briefly, then parks until a push, a wake, a timer or its
//! hook needs it. Everything here is touched only by the worker's own thread
//! except [`WorkerShared`], its face to other threads.

mod heartbeat;
mod keyed;
mod shared;
mod signal;
mod slab;
#[cfg(test)]
mod tests;
mod timers;
mod yield_now;

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub(crate) use heartbeat::REPORTED;
use keyed::Keyed;
pub(crate) use shared::{bump, Admitted, Job, LocalFuture, WorkerShared};
pub use shared::WorkerStats;
pub(crate) use signal::{Parker, Signal};
use signal::TaskWaker;
use slab::{Key, Slab};
use timers::Timers;
pub use timers::{every, sleep, Sleep};
pub use yield_now::{yield_now, Budget, YieldNow};

use crate::clock::Clock;
use crate::completion::{self, Completion, Fail};
use crate::hook::Hook;
use crate::{Error, WorkerId};

/// What the worker reads each round, fixed at start.
#[derive(Clone, Debug)]
pub(crate) struct Tuning {
    pub inbox_batch: usize,
    pub run_batch: usize,
    pub io_budget: usize,
    pub idle_spin: Duration,
    pub stuck_after: Duration,
    /// Another thread watches this worker's heartbeat. Without one (a single
    /// worker), the worker measures each poll itself when it returns.
    pub monitored: bool,
}

/// What a worker attaches to a task.
struct Meta {
    key: Option<u64>,
    /// Held for its drop: gives the worker's admission back when the task ends.
    _admitted: Option<Admitted>,
    /// Shutdown does not wait for it; it is dropped when the worker stops.
    daemon: bool,
}

struct Task {
    future: Option<LocalFuture>,
    waker: Arc<TaskWaker>,
    fail: Option<Arc<dyn Fail>>,
    name: &'static str,
    meta: Meta,
    /// Already reported as stuck: never reported again.
    reported: bool,
}

/// A worker's state, owned by its thread and shared there by `Rc` with the
/// futures (timers) that need it.
pub(crate) struct Local {
    shared: Arc<WorkerShared>,
    clock: Arc<dyn Clock>,
    tuning: Tuning,
    tasks: RefCell<Slab<Task>>,
    timers: RefCell<Timers>,
    keyed: RefCell<Keyed<Job>>,
    /// Tasks shutdown waits for (everything but daemons).
    live: Cell<usize>,
    draining: Cell<bool>,
    closed: Cell<bool>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Local>>> = const { RefCell::new(None) };
}

fn current() -> Option<Rc<Local>> {
    CURRENT.with(|current| current.borrow().clone())
}

/// Whether the calling thread is a worker, or drives one.
pub fn on_worker() -> bool {
    CURRENT.with(|current| current.borrow().is_some())
}

/// The worker the calling thread is, if it is one.
pub fn current_worker() -> Option<WorkerId> {
    CURRENT.with(|current| current.borrow().as_ref().map(|local| local.shared.id))
}

/// Runs `future` on the calling worker until it finishes; it never moves to
/// another thread, so it need not be `Send`. Errors off a worker.
pub fn spawn_local<F>(future: F) -> Result<Completion<F::Output>, Error>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let local = current().ok_or(Error::NotOnWorker)?;
    local.spawn(std::any::type_name::<F>(), future, false)
}

/// [`spawn_local`] for background work (a periodic job, a sweep) that
/// shutdown does not wait for: it is dropped when the worker stops.
pub fn spawn_daemon<F>(future: F) -> Result<(), Error>
where
    F: Future<Output = ()> + 'static,
{
    let local = current().ok_or(Error::NotOnWorker)?;
    local.spawn(std::any::type_name::<F>(), future, true).map(drop)
}

/// The text of a panic payload.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_owned())
}

/// Drops `value` without letting a panic in its destructor reach the worker.
fn drop_quietly<T>(value: T) {
    let _ = panic::catch_unwind(AssertUnwindSafe(move || drop(value)));
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Reports a poll that held its worker past the limit: the one log line for
/// it, wherever it was detected.
pub(crate) fn report_long_poll(worker: usize, task: &str, elapsed: u64, limit: Duration) {
    tracing::warn!(
        task,
        worker,
        elapsed_ms = elapsed / 1000,
        limit_ms = u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
        "a task held its worker past the limit; move blocking work to spawn_blocking"
    );
}

impl Local {
    pub(crate) fn now(&self) -> u64 {
        self.clock.now_micros()
    }

    fn spawn<F>(&self, name: &'static str, future: F, daemon: bool) -> Result<Completion<F::Output>, Error>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        if self.closed.get() {
            return Err(Error::ShutDown);
        }
        let (sender, completion) = completion::channel();
        let fail: Arc<dyn Fail> = sender.shared();
        let future: LocalFuture = Box::pin(async move { sender.send(future.await) });
        self.insert(name, future, Some(fail), Meta { key: None, _admitted: None, daemon });
        Ok(completion)
    }

    fn insert(&self, name: &'static str, future: LocalFuture, fail: Option<Arc<dyn Fail>>, meta: Meta) {
        let mut tasks = self.tasks.borrow_mut();
        let key = tasks.next_key();
        if !meta.daemon {
            self.live.set(self.live.get() + 1);
        }
        let waker = TaskWaker::new(self.shared.signal.clone(), key);
        tasks.insert(Task { future: Some(future), waker, fail, name, meta, reported: false });
        drop(tasks);
        bump(&self.shared.counters.tasks);
        self.shared.signal.schedule(key);
    }

    /// Starts a submitted job, or queues it behind its key's running task.
    fn start(&self, job: Job) {
        if self.closed.get() {
            return drop_quietly(job);
        }
        let job = match job.key {
            Some(key) => match self.keyed.borrow_mut().admit(key, job) {
                Some(job) => job,
                None => return,
            },
            None => job,
        };
        self.launch(job);
    }

    fn launch(&self, job: Job) {
        let Job { start, fail, name, key, admitted, .. } = job;
        let fail: Arc<dyn Fail> = fail;
        self.insert(name, start(), Some(fail), Meta { key, _admitted: Some(admitted), daemon: false });
    }

    /// Polls the task `key`; `false` when it no longer exists (a stale wake).
    fn poll(&self, key: Key) -> bool {
        let taken = self.tasks.borrow_mut().get_mut(key).and_then(|task| {
            task.waker.clear();
            let future = task.future.take()?;
            Some((future, Waker::from(task.waker.clone()), task.name, task.reported))
        });
        let Some((mut future, waker, name, reported)) = taken else {
            return false;
        };
        let start = self.now();
        let flag = if reported { REPORTED } else { 0 };
        self.shared.heartbeat.begin(start, key.to_bits() | flag, name);
        bump(&self.shared.counters.polls);
        let mut cx = Context::from_waker(&waker);
        let result = panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx)));
        let long = self.after_poll(name, start, reported);
        match result {
            Ok(Poll::Pending) => match self.tasks.borrow_mut().get_mut(key) {
                Some(task) => {
                    task.future = Some(future);
                    task.reported |= long;
                }
                None => drop_quietly(future),
            },
            Ok(Poll::Ready(())) => self.finish(key, future, None),
            Err(payload) => self.finish(key, future, Some(panic_message(payload.as_ref()))),
        }
        true
    }

    /// Whether the poll that started at `start` ran past the limit: flagged by
    /// the monitor while it ran, or measured here when nothing watches.
    fn after_poll(&self, name: &'static str, start: u64, reported: bool) -> bool {
        let shared = &self.shared;
        if shared.stuck.load(Ordering::Relaxed) {
            shared.stuck.store(false, Ordering::Relaxed);
            return true;
        }
        if self.tuning.monitored {
            return false;
        }
        let elapsed = self.now().saturating_sub(start);
        if elapsed <= micros(self.tuning.stuck_after) {
            return false;
        }
        bump(&shared.counters.long_polls);
        if !reported {
            report_long_poll(shared.id.index, name, elapsed, self.tuning.stuck_after);
        }
        true
    }

    fn finish(&self, key: Key, future: LocalFuture, panic: Option<String>) {
        let task = self.tasks.borrow_mut().remove(key);
        if let (Some(message), Some(task)) = (panic, &task) {
            bump(&self.shared.counters.panics);
            tracing::warn!(
                task = task.name,
                worker = self.shared.id.index,
                message = message.as_str(),
                "a task panicked; its caller gets the error and the worker goes on"
            );
            if let Some(fail) = &task.fail {
                fail.fail(Error::Panicked { task: task.name, message });
            }
        }
        drop_quietly(future);
        let Some(task) = task else { return };
        if !task.meta.daemon {
            self.live.set(self.live.get().saturating_sub(1));
        }
        let next = task.meta.key.and_then(|key| self.keyed.borrow_mut().finished(key));
        drop_quietly(task);
        if let Some(job) = next {
            self.launch(job);
        }
    }

    /// Nothing admitted is left to run.
    fn drained(&self) -> bool {
        self.live.get() == 0 && self.keyed.borrow().is_empty() && !self.shared.has_messages()
    }

    fn timers_due(&self) -> bool {
        self.timers.borrow_mut().next_due().is_some_and(|due| due <= self.now())
    }
}

/// A worker's hook and when it must next be polled.
struct HookState {
    hook: Option<Box<dyn Hook>>,
    due: Option<u64>,
    budget: usize,
}

impl HookState {
    /// Polls the hook; the completions it delivered.
    fn poll(&mut self, local: &Local) -> usize {
        let Some(hook) = self.hook.as_mut() else { return 0 };
        let progress = hook.poll(self.budget);
        self.due = progress.wake_in.map(|wait| local.now().saturating_add(micros(wait)));
        progress.completed
    }
}

/// A running worker on the calling thread. Dropping it stops the worker:
/// every task it holds is dropped (failing its completion), and every
/// message it has not taken is failed.
pub(crate) struct Worker {
    local: Rc<Local>,
    hook: HookState,
    unpark: Waker,
    woken: Vec<Waker>,
}

impl Worker {
    /// Makes the calling thread `shared`'s worker. Errors if it already is
    /// one.
    pub(crate) fn new(
        shared: Arc<WorkerShared>,
        clock: Arc<dyn Clock>,
        tuning: Tuning,
        hook: Option<Box<dyn Hook>>,
    ) -> Result<Worker, Error> {
        if on_worker() {
            return Err(Error::OnWorker);
        }
        let unpark = Waker::from(shared.signal.clone());
        let budget = tuning.io_budget;
        let local = Rc::new(Local {
            shared,
            clock,
            tuning,
            tasks: RefCell::new(Slab::new()),
            timers: RefCell::new(Timers::new()),
            keyed: RefCell::new(Keyed::new()),
            live: Cell::new(0),
            draining: Cell::new(false),
            closed: Cell::new(false),
        });
        CURRENT.with(|current| *current.borrow_mut() = Some(local.clone()));
        Ok(Worker { local, hook: HookState { hook, due: None, budget }, unpark, woken: Vec::new() })
    }

    pub(crate) fn shared(&self) -> &Arc<WorkerShared> {
        &self.local.shared
    }

    /// Spawns a daemon from outside any task (the pool's own periodic jobs).
    pub(crate) fn spawn_daemon<F>(&self, name: &'static str, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let _ = self.local.spawn(name, future, true);
    }

    /// Spawns a task from outside any task (the driving thread in driven
    /// mode).
    pub(crate) fn spawn<F>(&self, future: F) -> Result<Completion<F::Output>, Error>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.local.spawn(std::any::type_name::<F>(), future, false)
    }

    /// One round: a batch from the inboxes, a batch of ready tasks with the
    /// hook polled between them, then due timers. Returns how much it did;
    /// zero means the worker may idle.
    pub(crate) fn turn(&mut self) -> usize {
        let local = &self.local;
        let mut done = 0;
        for i in 0..local.tuning.inbox_batch {
            match local.shared.take(i % 2 == 1) {
                Some(shared::Message::Job(job)) => local.start(job),
                Some(shared::Message::Shutdown) => local.draining.set(true),
                None => break,
            }
            done += 1;
        }
        let mut polled = 0;
        while polled < local.tuning.run_batch {
            let Some(key) = local.shared.signal.pop() else { break };
            if local.poll(key) {
                polled += 1;
                done += self.hook.poll(local);
            }
        }
        if polled == 0 {
            done += self.hook.poll(local);
        }
        done += polled;
        if !local.timers.borrow().is_empty() {
            let now = local.now();
            local.timers.borrow_mut().fire(now, &mut self.woken);
            done += self.woken.len();
            self.woken.drain(..).for_each(Waker::wake);
        }
        local.shared.heartbeat.idle();
        done
    }

    /// Whether the worker should stop: told to abandon, or told to shut down
    /// and nothing admitted is left.
    pub(crate) fn finished(&self) -> bool {
        self.local.shared.abandon.load(Ordering::Acquire)
            || (self.local.draining.get() && self.local.drained())
    }

    /// Something is ready to run now.
    pub(crate) fn has_work(&self) -> bool {
        self.local.shared.signal.has_ready() || self.local.shared.has_messages() || self.local.timers_due()
    }

    /// When the worker must run next with nothing else waking it: its
    /// earliest timer or the hook's deadline, on the pool's clock.
    pub(crate) fn next_due(&self) -> Option<u64> {
        let timer = self.local.timers.borrow_mut().next_due();
        match (timer, self.hook.due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// How long until [`next_due`](Self::next_due), from now.
    pub(crate) fn wait_for(&self) -> Option<Duration> {
        self.next_due()
            .map(|due| Duration::from_micros(due.saturating_sub(self.local.now())))
    }

    /// Runs until told to stop.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn run(mut self) {
        loop {
            let done = self.turn();
            if self.finished() {
                break;
            }
            if done == 0 {
                self.idle();
            }
        }
    }

    /// The worker is about to rest: marks it parked, so the next push, wake
    /// or hook completion wakes it, and hands the hook its waker. Returns
    /// `false`, leaving it running, when work arrived meanwhile.
    pub(crate) fn rest(&mut self) -> bool {
        let signal = &self.local.shared.signal;
        signal.parking();
        if self.has_work() || self.finished() {
            signal.running();
            return false;
        }
        if let Some(hook) = self.hook.hook.as_mut() {
            hook.idle(&self.unpark);
        }
        true
    }

    /// Nothing to do: spin briefly, then park until a push, a wake, a timer
    /// or the hook needs the worker.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn idle(&mut self) {
        let spin = micros(self.local.tuning.idle_spin);
        if spin > 0 {
            let until = self.local.now().saturating_add(spin);
            let mut round = 0u32;
            loop {
                if self.has_work() {
                    return;
                }
                round = round.wrapping_add(1);
                if round % 64 == 0 {
                    if self.hook.poll(&self.local) > 0 {
                        return;
                    }
                    if self.local.now() >= until {
                        break;
                    }
                }
                std::hint::spin_loop();
            }
        }
        if !self.rest() {
            return;
        }
        match self.wait_for() {
            Some(wait) => std::thread::park_timeout(wait),
            None => std::thread::park(),
        }
        self.local.shared.signal.running();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let local = &self.local;
        local.closed.set(true);
        let tasks = local.tasks.borrow_mut().drain();
        tasks.into_iter().for_each(drop_quietly);
        local.live.set(0);
        let waiting = local.keyed.borrow_mut().drain();
        waiting.into_iter().for_each(drop_quietly);
        local.timers.borrow_mut().clear();
        local.shared.heartbeat.idle();
        local.shared.exit();
        drop_quietly(self.hook.hook.take());
        CURRENT.with(|current| *current.borrow_mut() = None);
    }
}
