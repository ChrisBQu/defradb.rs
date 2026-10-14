//! Receipt ordered dispatch for synchronous post-commit events.
//!
//! A transaction joins the coordinator immediately before entering the native
//! commit. Its floor prevents a later receipt from dispatching while this
//! commit is still in flight. Event-free and idle transactions never join.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use kovan::Atom;

pub(super) type Event = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Clone, Default)]
struct State {
    highest_completed: u64,
    active: BTreeMap<u64, u64>,
    ready: BTreeMap<u64, Vec<Event>>,
}

struct DrainGuard<'a> {
    draining: &'a AtomicBool,
    armed: bool,
}

impl Drop for DrainGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.draining.store(false, Ordering::Release);
        }
    }
}

/// Shared by transactions opened from one store.
#[derive(Default)]
pub(crate) struct EventOrder {
    next_token: AtomicU64,
    draining: AtomicBool,
    state: Atom<State>,
}

pub(super) struct EventToken {
    order: Arc<EventOrder>,
    token: Option<u64>,
    floor: u64,
}

impl EventToken {
    pub(super) fn finish(mut self, seq: u64, events: Vec<Event>) -> bool {
        if let Some(token) = self.token.take() {
            if seq <= self.floor {
                self.order.finish_token(token, None, Vec::new());
                return false;
            }
            self.order.finish_token(token, Some(seq), events);
            return true;
        }
        false
    }
}

impl Drop for EventToken {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.order.finish_token(token, None, Vec::new());
        }
    }
}

impl EventOrder {
    pub(super) fn begin(self: &Arc<Self>) -> EventToken {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.state.rcu(|state| {
            let mut next = state.clone();
            next.active.insert(token, state.highest_completed);
            next
        });
        let floor = self.state.peek(|state| state.active[&token]);
        EventToken {
            order: Arc::clone(self),
            token: Some(token),
            floor,
        }
    }

    fn finish_token(&self, token: u64, seq: Option<u64>, events: Vec<Event>) {
        self.state.rcu(|state| {
            let mut next = state.clone();
            next.active.remove(&token);
            if let Some(seq) = seq {
                next.highest_completed = next.highest_completed.max(seq);
                if !events.is_empty() {
                    next.ready
                        .entry(seq)
                        .or_default()
                        .extend(events.iter().cloned());
                }
            }
            next
        });
        self.drain();
    }

    fn drain(&self) {
        if self
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let mut guard = DrainGuard {
            draining: &self.draining,
            armed: true,
        };
        let mut first_panic = None;

        loop {
            let mut dispatch = None;
            self.state.rcu(|state| {
                let Some((&seq, _)) = state.ready.first_key_value() else {
                    dispatch = None;
                    return state.clone();
                };
                if state.active.values().any(|floor| *floor < seq) {
                    dispatch = None;
                    return state.clone();
                }
                let mut next = state.clone();
                let events = next.ready.remove(&seq).unwrap_or_default();
                dispatch = Some(events);
                next
            });
            let Some(events) = dispatch else {
                self.draining.store(false, Ordering::Release);
                guard.armed = false;
                // Close the race with a finisher that observed us as owner.
                if self.state.peek(|state| {
                    state
                        .ready
                        .first_key_value()
                        .is_some_and(|(seq, _)| !state.active.values().any(|floor| *floor < *seq))
                }) && self
                    .draining
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    guard.armed = true;
                    continue;
                }
                if let Some(payload) = first_panic {
                    resume_unwind(payload);
                }
                return;
            };
            for event in events {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| event())) {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kovan::Atom;

    fn event(out: Arc<Atom<Vec<u64>>>, value: u64) -> Event {
        Arc::new(move || {
            out.rcu(|values| {
                let mut next = values.clone();
                next.push(value);
                next
            })
        })
    }

    #[test]
    fn delayed_earlier_receipt_holds_later_events() {
        let order = Arc::new(EventOrder::default());
        let out = Arc::new(Atom::new(Vec::new()));
        let earlier = order.begin();
        let later = order.begin();
        later.finish(2, vec![event(out.clone(), 2)]);
        assert!(out.load().is_empty());

        earlier.finish(1, vec![event(out.clone(), 1)]);
        assert_eq!(*out.load(), vec![1, 2]);
    }

    #[test]
    fn failed_commit_releases_its_floor_and_idle_work_does_not_register() {
        let order = Arc::new(EventOrder::default());
        let out = Arc::new(Atom::new(Vec::new()));
        let failed = order.begin();
        let later = order.begin();
        later.finish(2, vec![event(out.clone(), 2)]);
        assert!(out.load().is_empty());
        drop(failed);
        assert_eq!(*out.load(), vec![2]);

        // An open interactive transaction with no eventful commit has no
        // coordinator token and therefore cannot hold a ready event.
        let next = order.begin();
        next.finish(3, vec![event(out.clone(), 3)]);
        assert_eq!(*out.load(), vec![2, 3]);
    }

    #[test]
    fn events_from_one_commit_keep_registration_order() {
        let order = Arc::new(EventOrder::default());
        let out = Arc::new(Atom::new(Vec::new()));
        let token = order.begin();
        assert!(token.finish(1, vec![event(out.clone(), 10), event(out.clone(), 11)]));
        assert_eq!(*out.load(), vec![10, 11]);
    }

    #[test]
    fn receipt_must_advance_past_the_completed_floor() {
        let order = Arc::new(EventOrder::default());
        let out = Arc::new(Atom::new(Vec::new()));
        assert!(order.begin().finish(1, vec![event(out.clone(), 1)]));
        assert!(!order.begin().finish(1, vec![event(out.clone(), 2)]));
        assert_eq!(*out.load(), vec![1]);
        assert!(order.begin().finish(2, vec![event(out.clone(), 3)]));
        assert_eq!(*out.load(), vec![1, 3]);
    }

    #[test]
    fn callback_panic_does_not_drop_later_events_or_stick_the_drainer() {
        let order = Arc::new(EventOrder::default());
        let out = Arc::new(Atom::new(Vec::new()));
        let token = order.begin();
        let panic_event: Event = Arc::new(|| panic!("subscriber panic"));
        let second = event(out.clone(), 2);
        assert!(std::panic::catch_unwind(AssertUnwindSafe(|| {
            token.finish(1, vec![panic_event, second]);
        }))
        .is_err());
        assert_eq!(*out.load(), vec![2]);

        order.begin().finish(2, vec![event(out.clone(), 3)]);
        assert_eq!(*out.load(), vec![2, 3]);
    }
}
