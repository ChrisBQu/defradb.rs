//! The stuck-poll monitor. Each worker publishes when its current poll began;
//! a scan on a timer (run by two workers, so one of them always watches the
//! other) finds a poll past the limit, reports it once per task (its name,
//! its worker, how long), marks the worker stuck so routing sends it nothing
//! new, and moves the movable jobs still queued in its inbox to free workers
//! on the same node. Bound jobs wait for their owner, and no running task is
//! ever moved.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::PoolShared;
use crate::worker::{report_long_poll, REPORTED};

/// How many workers run the scan.
pub(crate) const WATCHERS: usize = 2;

/// How often a watcher scans, for a stuck limit of `limit`.
pub(crate) fn period(limit: Duration) -> Duration {
    (limit / 2).max(Duration::from_millis(1))
}

/// Scans every worker's heartbeat at `now`; returns how many are stuck.
pub(crate) fn scan(pool: &PoolShared, now: u64) -> usize {
    let limit = u64::try_from(pool.stuck_after.as_micros()).unwrap_or(u64::MAX);
    let mut stuck = 0;
    for worker in pool.workers.iter() {
        let Some(beat) = worker.heartbeat.read() else { continue };
        let elapsed = now.saturating_sub(beat.start);
        if elapsed <= limit {
            continue;
        }
        stuck += 1;
        worker.stuck.store(true, Ordering::Relaxed);
        let task = beat.task & !REPORTED;
        if beat.task & REPORTED == 0 && worker.reported.swap(task, Ordering::AcqRel) != task {
            worker.counters.long_polls.fetch_add(1, Ordering::Relaxed);
            report_long_poll(worker.id.index, beat.name, elapsed, pool.stuck_after);
        }
        while let Some(job) = worker.take_free() {
            match pool.reroute(job, worker) {
                Ok(()) => {
                    worker.counters.moved_out.fetch_add(1, Ordering::Relaxed);
                }
                Err(job) => {
                    worker.push(job);
                    break;
                }
            }
        }
    }
    stuck
}
