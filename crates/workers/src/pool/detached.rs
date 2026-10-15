//! Spawning from anywhere: a future that runs to its end whether or not
//! anyone awaits it (a periodic job, a backfill, the callbacks of a
//! transaction dropped where nothing can await).
//!
//! Called on a worker, the work is a task of that worker: it stays on the
//! core that started it. Called anywhere else, it goes to the pool the
//! caller names, or, for code that runs where no pool was set up (a test, a
//! one-shot tool), to a small process-wide fallback pool started on first
//! use. A submission that finds no room waits for it on the calling plain
//! thread, so spawned work is never dropped.

use std::future::Future;

use super::{Pool, Submit, Target};
use crate::Error;

/// Runs the future `make` builds to its end: on the calling worker when
/// called on one; otherwise on `pool`, or on the fallback pool when `pool` is
/// `None`. Returns its completion; dropping it leaves the work running.
/// Errors only when no pool can take it: the pool shut down, or no pool can
/// start on this target.
///
/// ```no_run
/// # async fn demo(pool: &workers::Pool) -> Result<(), workers::Error> {
/// let done = workers::spawn(Some(pool), "backfill", || async { 7 })?;
/// assert_eq!(done.await?, 7);
/// # Ok(())
/// # }
/// ```
pub fn spawn<F, Fut>(
    pool: Option<&Pool>,
    name: &'static str,
    make: F,
) -> Result<crate::Completion<Fut::Output>, Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    if crate::worker::on_worker() {
        return crate::worker::spawn_named(name, make());
    }
    let pool = match pool {
        Some(pool) => pool,
        None => fallback()?,
    };
    let options = Submit {
        name: Some(name),
        bytes: None,
    };
    submit_now(pool, Target::LeastLoaded, options, make)
}

/// [`spawn`] for work nobody awaits (a periodic job, a backfill, the
/// callbacks of a transaction dropped where nothing can await).
///
/// ```no_run
/// # fn demo(pool: &workers::Pool) -> Result<(), workers::Error> {
/// workers::spawn_detached(Some(pool), "redrive", || async {
///     // one page per transaction, yielding between pages
/// })?;
/// # Ok(())
/// # }
/// ```
pub fn spawn_detached<F, Fut>(pool: Option<&Pool>, name: &'static str, make: F) -> Result<(), Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    spawn(pool, name, make).map(drop)
}

/// Submits `make` from a caller that cannot await (off any worker): with
/// room now, at once; without, waiting for room on the calling plain thread.
pub(super) fn submit_now<F, Fut>(
    pool: &Pool,
    target: Target,
    options: Submit,
    make: F,
) -> Result<crate::Completion<Fut::Output>, Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    match pool
        .handle
        .shared
        .admit_now(target, std::mem::size_of::<F>())?
    {
        Some(admitted) => Ok(Pool::launch(target, options, admitted, make)),
        None => wait_for_room(pool, target, options, make),
    }
}

/// No room now: wait for it on this plain thread, which is never a worker.
#[cfg(not(target_family = "wasm"))]
fn wait_for_room<F, Fut>(
    pool: &Pool,
    target: Target,
    options: Submit,
    make: F,
) -> Result<crate::Completion<Fut::Output>, Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    crate::completion::block_on(pool.submit_with(target, options, make))?
}

/// A browser has no thread to wait on.
#[cfg(target_family = "wasm")]
fn wait_for_room<F, Fut>(
    _pool: &Pool,
    _target: Target,
    _options: Submit,
    _make: F,
) -> Result<crate::Completion<Fut::Output>, Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    Err(Error::Full)
}

/// The process-wide pool for work submitted where no pool runs: one worker,
/// unplaced ([`Config::unplaced`](crate::Config::unplaced)), started on first
/// use and kept for the life of the process. It serves code that runs off
/// every pool (a test, a one-shot tool) and the timers of waits made off a
/// worker, such as the deadline of a pool's own [`Pool::stop`].
#[cfg(not(target_family = "wasm"))]
pub fn fallback() -> Result<&'static Pool, Error> {
    static FALLBACK: regolith::sync::OnceCell<Pool> = regolith::sync::OnceCell::new();
    if let Some(pool) = FALLBACK.get() {
        return Ok(pool);
    }
    let pool = Pool::start(crate::Config {
        thread_name: "defra-fallback".to_owned(),
        ..crate::Config::unplaced(1)
    })?;
    // A racing start loses here; its pool shuts down when dropped.
    let _ = FALLBACK.set(pool);
    FALLBACK.get().ok_or(Error::ShutDown)
}

/// A browser starts no threads: its one worker is the page's own, driven by
/// its event loop, and work submitted off it has nowhere else to go.
#[cfg(target_family = "wasm")]
pub fn fallback() -> Result<&'static Pool, Error> {
    Err(Error::NotOnWorker)
}
