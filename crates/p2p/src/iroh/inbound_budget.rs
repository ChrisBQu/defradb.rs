//! Room in memory for inbound message bodies, taken before a body is read.
//!
//! Each [`Priority`] has its own byte budget, and each peer may hold at most
//! half of any one budget, however many connections it opens. A body's room is
//! held until its event has been handled. Modeled in `proofs/tla/InboundAdmission.tla`; the accounting is
//! proved in `proofs/lean/ByteBudget/Accounting.lean`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use iroh::EndpointId;
use kovan::Atom;
use rapidhash::RapidHashMap;
use regolith::sync::{OwnedSemaphorePermit, Semaphore};

use super::services::Priority;
use crate::transport::InboundClassSnapshot;

/// Bytes each priority may hold, in [`Priority::ALL`] order. Half of each
/// must fit that priority's largest message.
const BUDGET_BYTES: [usize; 4] = [
    32 * 1024 * 1024,
    64 * 1024 * 1024,
    64 * 1024 * 1024,
    128 * 1024 * 1024,
];

/// Room is counted in KiB, so the largest budget fits a semaphore on 32-bit
/// targets, where one holds fewer than 64 Mi permits.
const PERMIT_BYTES: usize = 1024;

/// The permits a `len`-byte body takes: its size in KiB, rounded up.
const fn permits_for(len: usize) -> usize {
    len.div_ceil(PERMIT_BYTES)
}

/// How long a stream waits for room before it is reset.
pub const ADMIT_DEADLINE: Duration = Duration::from_secs(5);

/// How long a body may go without a single byte arriving. A sender that
/// stalls is reset so it cannot hold its room.
pub const BODY_IDLE_DEADLINE: Duration = Duration::from_secs(10);

/// The slowest average rate, in bytes per second, at which a body may arrive.
/// A sender that trickles bytes often enough to dodge the idle deadline still
/// has to meet it, or it would hold its room for as long as it likes.
const MIN_BODY_RATE: usize = 32 * 1024;

/// The longest a `len`-byte body may take to arrive in full: the idle
/// deadline plus the time it needs at the slowest accepted rate.
pub fn body_deadline(len: usize) -> Duration {
    let at_minimum_rate =
        Duration::from_millis((len as u64).saturating_mul(1000) / MIN_BODY_RATE as u64);
    BODY_IDLE_DEADLINE + at_minimum_rate
}

/// The node-wide inbound byte budgets.
#[derive(Debug)]
pub struct InboundBudget {
    classes: [ClassBudget; 4],
    /// Each peer's share, alive while any of its connections or leases is.
    peers: Atom<RapidHashMap<EndpointId, Weak<PeerShare>>>,
}

#[derive(Debug)]
struct ClassBudget {
    room: Arc<Semaphore>,
    stats: Arc<ClassStats>,
}

#[derive(Debug, Default)]
struct ClassStats {
    held: AtomicUsize,
    high_water: AtomicUsize,
    busy_resets: AtomicU64,
    oversized_resets: AtomicU64,
}

/// One peer's share of every class budget, common to all its connections.
#[derive(Debug)]
pub struct PeerShare {
    room: [Arc<Semaphore>; 4],
}

/// No room was free before the deadline.
#[derive(Debug, PartialEq, Eq)]
pub struct Busy;

/// Room for one body, given back when dropped.
#[derive(Debug)]
pub struct InboundLease {
    _share: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
    /// Keeps the peer's share alive, so a peer that reconnects while its
    /// bodies are still held gets the same share back.
    _peer: Arc<PeerShare>,
    len: usize,
    stats: Arc<ClassStats>,
}

impl Drop for InboundLease {
    fn drop(&mut self) {
        self.stats.held.fetch_sub(self.len, Ordering::Relaxed);
    }
}

impl Default for InboundBudget {
    fn default() -> Self {
        Self {
            classes: BUDGET_BYTES.map(|capacity| ClassBudget {
                room: Arc::new(Semaphore::new(permits_for(capacity))),
                stats: Arc::default(),
            }),
            peers: Atom::default(),
        }
    }
}

impl InboundBudget {
    /// The share of `peer`, half of each class budget: the one its other
    /// connections or held bodies already use, or a fresh one.
    pub fn peer_share(&self, peer: EndpointId) -> Arc<PeerShare> {
        let mut fresh = None;
        loop {
            let current = self.peers.load();
            if let Some(share) = current.get(&peer).and_then(Weak::upgrade) {
                return share;
            }
            let share = fresh
                .get_or_insert_with(|| {
                    Arc::new(PeerShare {
                        room: BUDGET_BYTES.map(|capacity| {
                            Arc::new(Semaphore::new(permits_for(share_of(capacity))))
                        }),
                    })
                })
                .clone();
            let mut next: RapidHashMap<_, _> = current
                .iter()
                .filter(|(_, share)| share.strong_count() > 0)
                .map(|(id, share)| (*id, share.clone()))
                .collect();
            next.insert(peer, Arc::downgrade(&share));
            if self.peers.compare_and_swap(&current, next).is_ok() {
                return share;
            }
        }
    }

    /// Waits up to `deadline` for room for a `len`-byte body. The peer's share
    /// is taken first, so a peer already at its share never holds class bytes
    /// while it waits.
    pub async fn admit(
        &self,
        share: &Arc<PeerShare>,
        priority: Priority,
        len: usize,
        deadline: Duration,
    ) -> Result<InboundLease, Busy> {
        let class = &self.classes[priority.index()];
        let permits = permits_for(len);
        let acquire = async {
            let share_bytes = share.room[priority.index()].acquire_owned(permits).await;
            let class_bytes = class.room.acquire_owned(permits).await;
            (share_bytes, class_bytes)
        };
        let Ok((share_bytes, class_bytes)) = n0_future::time::timeout(deadline, acquire).await
        else {
            class.stats.busy_resets.fetch_add(1, Ordering::Relaxed);
            return Err(Busy);
        };
        let held = class.stats.held.fetch_add(len, Ordering::Relaxed) + len;
        class.stats.high_water.fetch_max(held, Ordering::Relaxed);
        Ok(InboundLease {
            _share: share_bytes,
            _class: class_bytes,
            _peer: Arc::clone(share),
            len,
            stats: Arc::clone(&class.stats),
        })
    }

    pub fn record_oversized(&self, priority: Priority) {
        self.classes[priority.index()]
            .stats
            .oversized_resets
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> [InboundClassSnapshot; 4] {
        Priority::ALL.map(|priority| {
            let stats = &self.classes[priority.index()].stats;
            InboundClassSnapshot {
                class: priority.name(),
                capacity_bytes: BUDGET_BYTES[priority.index()],
                held_bytes: stats.held.load(Ordering::Relaxed),
                held_bytes_high_water: stats.high_water.load(Ordering::Relaxed),
                busy_resets: stats.busy_resets.load(Ordering::Relaxed),
                oversized_resets: stats.oversized_resets.load(Ordering::Relaxed),
            }
        })
    }
}

const fn share_of(capacity: usize) -> usize {
    capacity / 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iroh::services::SERVICES;

    const SHORT: Duration = Duration::from_millis(20);

    fn peer() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    #[test]
    fn every_message_fits_a_peer_share() {
        for service in SERVICES {
            assert!(
                service.max_message <= share_of(BUDGET_BYTES[service.priority.index()]),
                "{:?} cannot fit its largest message",
                String::from_utf8_lossy(service.tag)
            );
        }
    }

    #[test]
    fn a_body_deadline_grows_with_the_declared_length() {
        assert_eq!(body_deadline(0), BODY_IDLE_DEADLINE);
        assert_eq!(
            body_deadline(MIN_BODY_RATE * 30),
            BODY_IDLE_DEADLINE + Duration::from_secs(30)
        );
    }

    #[test]
    fn every_budget_fits_a_32_bit_semaphore() {
        // regolith keeps 6 flag bits beside the count, so a 32-bit
        // `Semaphore::MAX_PERMITS` is `u32::MAX >> 6`.
        let max_32_bit = (u32::MAX >> 6) as usize;
        for capacity in BUDGET_BYTES {
            assert!(permits_for(capacity) <= max_32_bit);
        }
    }

    #[tokio::test]
    async fn a_lease_holds_its_bytes_until_dropped() {
        let budget = InboundBudget::default();
        let share = budget.peer_share(peer());
        let len = share_of(BUDGET_BYTES[0]);

        let lease = budget
            .admit(&share, Priority::Control, len, SHORT)
            .await
            .unwrap();
        assert_eq!(budget.snapshot()[0].held_bytes, len);
        assert_eq!(
            budget
                .admit(&share, Priority::Control, 1, SHORT)
                .await
                .unwrap_err(),
            Busy
        );

        drop(lease);
        assert_eq!(budget.snapshot()[0].held_bytes, 0);
        assert_eq!(budget.snapshot()[0].held_bytes_high_water, len);
        assert_eq!(budget.snapshot()[0].busy_resets, 1);
        budget
            .admit(&share, Priority::Control, 1, SHORT)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn one_peer_cannot_take_a_whole_class() {
        let budget = InboundBudget::default();
        let greedy = budget.peer_share(peer());
        let other = budget.peer_share(peer());
        let half = share_of(BUDGET_BYTES[0]);

        let _held = budget
            .admit(&greedy, Priority::Control, half, SHORT)
            .await
            .unwrap();
        assert_eq!(
            budget
                .admit(&greedy, Priority::Control, 1, SHORT)
                .await
                .unwrap_err(),
            Busy
        );
        budget
            .admit(&other, Priority::Control, half, SHORT)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_full_class_does_not_block_another() {
        let budget = InboundBudget::default();
        let shares = [budget.peer_share(peer()), budget.peer_share(peer())];
        let half = share_of(BUDGET_BYTES[3]);
        let _bulk = [
            budget
                .admit(&shares[0], Priority::Bulk, half, SHORT)
                .await
                .unwrap(),
            budget
                .admit(&shares[1], Priority::Bulk, half, SHORT)
                .await
                .unwrap(),
        ];

        assert_eq!(
            budget
                .admit(&shares[0], Priority::Bulk, 1, SHORT)
                .await
                .unwrap_err(),
            Busy
        );
        budget
            .admit(&shares[0], Priority::Control, 1, SHORT)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_failed_admission_keeps_no_share() {
        let budget = InboundBudget::default();
        let shares = [budget.peer_share(peer()), budget.peer_share(peer())];
        let third = budget.peer_share(peer());
        let half = share_of(BUDGET_BYTES[3]);
        let _bulk = [
            budget
                .admit(&shares[0], Priority::Bulk, half, SHORT)
                .await
                .unwrap(),
            budget
                .admit(&shares[1], Priority::Bulk, half, SHORT)
                .await
                .unwrap(),
        ];

        assert_eq!(
            budget
                .admit(&third, Priority::Bulk, half, SHORT)
                .await
                .unwrap_err(),
            Busy
        );
        assert_eq!(third.room[3].available_permits(), permits_for(half));
    }

    #[tokio::test]
    async fn every_connection_of_a_peer_draws_on_one_share() {
        let budget = InboundBudget::default();
        let greedy = peer();
        let first = budget.peer_share(greedy);
        let second = budget.peer_share(greedy);
        let half = share_of(BUDGET_BYTES[3]);

        let _held = budget
            .admit(&first, Priority::Bulk, half, SHORT)
            .await
            .unwrap();
        assert_eq!(
            budget
                .admit(&second, Priority::Bulk, 1, SHORT)
                .await
                .unwrap_err(),
            Busy
        );
        budget
            .admit(&budget.peer_share(peer()), Priority::Bulk, half, SHORT)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_peer_that_reconnects_gets_its_held_share_back() {
        let budget = InboundBudget::default();
        let greedy = peer();
        let half = share_of(BUDGET_BYTES[3]);
        let held = budget
            .admit(&budget.peer_share(greedy), Priority::Bulk, half, SHORT)
            .await
            .unwrap();

        assert_eq!(
            budget
                .admit(&budget.peer_share(greedy), Priority::Bulk, 1, SHORT)
                .await
                .unwrap_err(),
            Busy
        );
        drop(held);
        budget
            .admit(&budget.peer_share(greedy), Priority::Bulk, half, SHORT)
            .await
            .unwrap();
        assert_eq!(budget.peers.peek(|peers| peers.len()), 1);
    }
}
