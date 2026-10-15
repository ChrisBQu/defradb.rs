//! A timer on the runtime's clock that reports a stopping runtime instead
//! of polling a timer driver that is going away.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use n0_future::time::{Instant, Sleep};

use crate::error::EdgeError;
use crate::shared::Shared;

/// Stands in for "never" when a limit does not fit the clock.
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

pub(crate) struct Timer {
    sleep: Pin<Box<Sleep>>,
    shared: Arc<Shared>,
}

impl Timer {
    pub(crate) fn after(shared: &Arc<Shared>, from: Instant, limit: Duration) -> Self {
        Self {
            sleep: Box::pin(shared.rt.sleep_until(deadline_at(from, limit))),
            shared: Arc::clone(shared),
        }
    }

    pub(crate) fn reset(&mut self, from: Instant, limit: Duration) {
        self.sleep.as_mut().reset(deadline_at(from, limit));
    }

    /// Ready once `limit` passed, as the deadline error it counts, or once
    /// the runtime is stopping, as [`EdgeError::Closed`].
    pub(crate) fn poll_limit(&mut self, cx: &mut Context<'_>, limit: Duration) -> Poll<EdgeError> {
        self.poll(cx).map(|elapsed| {
            if elapsed {
                self.shared.stats.deadline();
                EdgeError::Deadline(limit)
            } else {
                EdgeError::Closed
            }
        })
    }

    /// Ready with `true` once the time passed, `false` once the runtime is
    /// stopping. A stopping runtime's driver panics when polled; the flag
    /// is checked first, and the catch covers the moment between the two.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if self.shared.is_stopped() {
            return Poll::Ready(false);
        }
        match catch_unwind(AssertUnwindSafe(|| self.sleep.as_mut().poll(cx))) {
            Ok(Poll::Ready(())) => Poll::Ready(true),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(false),
        }
    }
}

fn deadline_at(from: Instant, limit: Duration) -> Instant {
    from.checked_add(limit).unwrap_or_else(|| from + FAR_FUTURE)
}
