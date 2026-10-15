//! Deadlines a `Send` future can carry: a delay is a timer task on a
//! worker's deadline queue, awaited through its [`Completion`], so the
//! awaiting future holds nothing tied to one thread.
//!
//! On a worker the timer is a task of that worker, so it fires on the core
//! that waits for it. Anywhere else (a test, a one-shot tool) it is a task of
//! the fallback pool.

use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use super::detached::{fallback, submit_now};
use super::{Submit, Target};
use crate::{Completion, Error};

/// Completes once `duration` has passed on the pool's clock. The completion
/// is `Send`, so a future that must be `Send` can await it.
pub fn delay(duration: Duration) -> Result<Completion<()>, Error> {
    let tick = move || async move {
        if let Ok(sleep) = crate::worker::sleep(duration) {
            sleep.await;
        }
    };
    if crate::worker::on_worker() {
        return crate::worker::spawn_named("workers::delay", tick());
    }
    let options = Submit {
        name: Some("workers::delay"),
        bytes: None,
    };
    submit_now(fallback()?, Target::LeastLoaded, options, tick)
}

/// `future`'s output, or [`Error::TimedOut`] once `limit` passes first; the
/// future is dropped then.
///
/// ```no_run
/// # async fn demo() -> Result<(), workers::Error> {
/// let answer = workers::timeout(std::time::Duration::from_secs(1), async { 42 }).await?;
/// assert_eq!(answer, 42);
/// # Ok(())
/// # }
/// ```
pub async fn timeout<F>(limit: Duration, future: F) -> Result<F::Output, Error>
where
    F: Future,
{
    let mut deadline = pin!(delay(limit)?);
    let mut future = pin!(future);
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        match deadline.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(Error::TimedOut)),
            // The timer's pool stopped before it fired: no deadline is left
            // to keep, and waiting on would hang, so the wait ends here.
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}
