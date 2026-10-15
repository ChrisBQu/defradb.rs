//! A service: one input kind's remote side behind single-flight, batching, a
//! byte-bounded cache, deadlines, retries, an in-flight cap and metrics.
//!
//! A caller's keys go through the cache, then the flight map: a key already
//! in flight is joined, a new one gets a flight and waits in its group's
//! lane. Round trips run as tasks on the service's network, each taking the
//! oldest waiting keys of a lane, so keys that arrive while one is in flight
//! ride the next. Callers suspend on their flights' events until the answers
//! come or their deadline passes; giving up cancels nothing others share.

use std::hash::BuildHasher;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use n0_future::FutureExt;
use rapidhash::fast::{RandomState, RapidHashSet};
use regolith::sync::{HopscotchMap, Semaphore};

use crate::cache::Cache;
use crate::config::Config;
use crate::error::Error;
use crate::flight::{Answer, Flight};
use crate::lane::Lane;
use crate::network::Runtime;
use crate::retry::Jitter;
use crate::source::Source;
use crate::stats::Stats;

type ResultOf<S> = Result<<S as Source>::Value, Error<<S as Source>::Error>>;
type FlightOf<S> = Arc<Flight<<S as Source>::Value, <S as Source>::Error>>;
type LaneOf<S> = Arc<Lane<<S as Source>::Key>>;

/// One input kind's service. Cloning shares it.
pub struct Service<S>
where
    S: Source,
{
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Service<S>
where
    S: Source,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<S>
where
    S: Source,
{
    source: S,
    config: Config,
    runtime: Runtime,
    flights: HopscotchMap<S::Key, FlightOf<S>, RandomState>,
    lanes: HopscotchMap<S::Group, LaneOf<S>, RandomState>,
    cache: Cache<S::Key, S::Value>,
    in_flight: Semaphore,
    trips: AtomicU64,
    jitter: Jitter,
    stats: Stats,
}

enum Slot<S>
where
    S: Source,
{
    Ready(ResultOf<S>),
    Waiting(FlightOf<S>),
}

impl<S> Service<S>
where
    S: Source,
{
    /// A service over `source`, running its round trips on `runtime`.
    pub fn new(source: S, config: Config, runtime: Runtime) -> Self {
        let config = config.normalized();
        let seed = RandomState::default().hash_one(runtime.now());
        Self {
            inner: Arc::new(Inner {
                source,
                cache: Cache::new(config.cache_bytes, config.ttl),
                in_flight: Semaphore::new(config.max_in_flight),
                config,
                runtime,
                flights: HopscotchMap::with_hasher(RandomState::default()),
                lanes: HopscotchMap::with_hasher(RandomState::default()),
                trips: AtomicU64::new(0),
                jitter: Jitter::new(seed),
                stats: Stats::default(),
            }),
        }
    }

    /// `key`'s answer.
    pub async fn get(&self, key: S::Key) -> ResultOf<S> {
        self.get_many(vec![key])
            .await
            .into_iter()
            .next()
            .unwrap_or(Err(Error::Answers {
                expected: 1,
                got: 0,
            }))
    }

    /// The answers for `keys`, in their order. Keys not cached or in flight
    /// go out together, in as few round trips as the batch cap allows.
    pub async fn get_many(&self, keys: Vec<S::Key>) -> Vec<ResultOf<S>> {
        let inner = &self.inner;
        let start = inner.runtime.now();
        Stats::add(&inner.stats.requests, keys.len() as u64);
        let mut touched: Vec<(S::Group, LaneOf<S>)> = Vec::new();
        let slots: Vec<Slot<S>> = keys
            .into_iter()
            .map(|key| inner.slot(key, start, &mut touched))
            .collect();
        for (group, lane) in touched {
            inner.pump(group, lane);
        }
        inner.wait(slots, start).await
    }

    /// Starts the round trips `keys` need, without waiting for them: their
    /// answers land in the cache (where the source allows), and a later
    /// [`get_many`](Self::get_many) of a key still in flight joins it.
    pub fn prefetch(&self, keys: Vec<S::Key>) {
        let inner = &self.inner;
        let now = inner.runtime.now();
        Stats::add(&inner.stats.requests, keys.len() as u64);
        let mut touched: Vec<(S::Group, LaneOf<S>)> = Vec::new();
        for key in keys {
            inner.slot(key, now, &mut touched);
        }
        for (group, lane) in touched {
            inner.pump(group, lane);
        }
    }

    /// `key`'s cached answer, without asking the remote side.
    pub fn cached(&self, key: &S::Key) -> Option<S::Value> {
        self.inner.cache.get(key, self.inner.runtime.now())
    }

    /// Caches an answer learned another way (a key this node just made),
    /// when the source allows caching it.
    pub fn remember(&self, key: S::Key, value: S::Value) -> bool {
        let inner = &self.inner;
        let Some(bytes) = inner.source.cache_bytes(&key, &value) else {
            return false;
        };
        let generation = inner.cache.generation();
        inner
            .cache
            .insert(key, value, bytes, inner.runtime.now(), generation)
    }

    /// Drops every cached answer whose key matches, and keeps answers
    /// fetched in the meantime from being cached. Returns how many.
    pub fn invalidate<F>(&self, matches: F) -> usize
    where
        F: Fn(&S::Key) -> bool,
    {
        self.inner.cache.invalidate(matches)
    }

    /// The service's metrics.
    pub fn stats(&self) -> &Stats {
        &self.inner.stats
    }

    /// The bytes the cache holds now.
    pub fn cache_bytes(&self) -> usize {
        self.inner.cache.bytes()
    }

    /// The remote side.
    pub fn source(&self) -> &S {
        &self.inner.source
    }

    /// The bounds the service runs with.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }
}

impl<S> Inner<S>
where
    S: Source,
{
    /// Where `key`'s answer comes from: the cache, a flight in progress, or
    /// a new flight queued in its group's lane.
    fn slot(&self, key: S::Key, now: u64, touched: &mut Vec<(S::Group, LaneOf<S>)>) -> Slot<S> {
        if let Some(value) = self.cache.get(&key, now) {
            Stats::add(&self.stats.cache_hits, 1);
            return Slot::Ready(Ok(value));
        }
        let fresh: FlightOf<S> = Arc::new(Flight::new(self.cache.generation()));
        if let Some(existing) = self
            .flights
            .insert_if_absent(key.clone(), Arc::clone(&fresh))
        {
            Stats::add(&self.stats.joined, 1);
            return Slot::Waiting(existing);
        }
        // Cached between the probe and the claim: settle the new flight
        // with it, for anyone who joined it meanwhile.
        if let Some(value) = self.cache.get(&key, now) {
            fresh.settle(Answer {
                result: Ok(value.clone()),
                trip: 0,
            });
            self.flights
                .remove_if(&key, |flight| Arc::ptr_eq(flight, &fresh));
            Stats::add(&self.stats.cache_hits, 1);
            return Slot::Ready(Ok(value));
        }
        let group = self.source.group(&key);
        let lane = self
            .lanes
            .get_or_insert(group.clone(), Arc::new(Lane::new()));
        lane.push(key);
        if !touched.iter().any(|(_, seen)| Arc::ptr_eq(seen, &lane)) {
            touched.push((group, lane));
        }
        Slot::Waiting(fresh)
    }

    /// Starts round trips for `lane`'s waiting keys, as many as its slots
    /// and its batches allow. The keys a caller pushed are always driven:
    /// either here, or by a round trip in flight that takes them next.
    fn pump(self: &Arc<Self>, group: S::Group, lane: LaneOf<S>) {
        let batches = lane.waiting().div_ceil(self.config.max_batch).max(1);
        for _ in 0..batches {
            if !lane.claim(self.config.flights_per_group) {
                return;
            }
            let inner = Arc::clone(self);
            let group = group.clone();
            let lane = Arc::clone(&lane);
            self.runtime
                .network
                .spawn(Box::pin(async move { inner.drive(group, lane).await }));
        }
    }

    /// One round-trip slot of a lane: takes the oldest waiting keys, makes
    /// their round trip, settles their flights, and goes on while keys wait.
    async fn drive(&self, group: S::Group, lane: LaneOf<S>) {
        let mut driving = Driving {
            inner: self,
            lane: &lane,
            holding: true,
            batch: Vec::new(),
        };
        loop {
            driving.batch = lane.take(self.config.max_batch);
            if driving.batch.is_empty() {
                driving.holding = false;
                lane.release();
                // A key pushed after the take saw the slot taken and left
                // its round trip to this one.
                if lane.has_waiting() && lane.claim(self.config.flights_per_group) {
                    driving.holding = true;
                    continue;
                }
                if lane.is_idle() {
                    self.lanes.remove_if(&group, |current| {
                        Arc::ptr_eq(current, &lane) && lane.is_idle()
                    });
                }
                return;
            }
            let trip = self.trips.fetch_add(1, Ordering::Relaxed) + 1;
            let outcome = {
                let _permit = self.in_flight.acquire(1).await;
                self.round_trip(&group, &driving.batch).await
            };
            let batch = std::mem::take(&mut driving.batch);
            self.deliver(batch, outcome, trip);
        }
    }

    /// One batch's round trips: the first, then retries while the source
    /// allows them and the deadline leaves time.
    async fn round_trip(
        &self,
        group: &S::Group,
        keys: &[S::Key],
    ) -> Result<Vec<Result<S::Value, S::Error>>, Error<S::Error>> {
        let started = self.runtime.now();
        let budget = micros(self.config.deadline);
        Stats::add(&self.stats.fetched, keys.len() as u64);
        self.stats.batch_sizes.record(keys.len() as u64);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let remaining = budget.saturating_sub(self.runtime.now().saturating_sub(started));
            if remaining == 0 {
                return Err(Error::Deadline(self.config.deadline));
            }
            Stats::add(&self.stats.round_trips, 1);
            if attempt > 1 {
                Stats::add(&self.stats.retries, 1);
            }
            let sent = self.runtime.now();
            let fetch = AssertUnwindSafe(self.source.fetch(group, keys)).catch_unwind();
            let timer = self.runtime.network.sleep(Duration::from_micros(remaining));
            let outcome = async { Some(fetch.await) }
                .or(async {
                    timer.await;
                    None
                })
                .await;
            self.stats
                .round_trip_micros
                .record(self.runtime.now().saturating_sub(sent));
            let error = match outcome {
                None => return Err(Error::Deadline(self.config.deadline)),
                Some(Err(_panic)) => {
                    tracing::warn!(
                        input = self.source.name(),
                        "a remote input request panicked"
                    );
                    return Err(Error::Panicked);
                }
                Some(Ok(Ok(answers))) if answers.len() == keys.len() => return Ok(answers),
                Some(Ok(Ok(answers))) => {
                    return Err(Error::Answers {
                        expected: keys.len(),
                        got: answers.len(),
                    })
                }
                Some(Ok(Err(error))) => error,
            };
            let left = budget.saturating_sub(self.runtime.now().saturating_sub(started));
            if attempt >= self.config.attempts || !self.source.retryable(&error) || left == 0 {
                return Err(Error::Source(error));
            }
            let wait = self
                .jitter
                .backoff(&self.config, attempt)
                .min(Duration::from_micros(left));
            tracing::debug!(
                input = self.source.name(),
                attempt,
                wait_micros = wait.as_micros() as u64,
                %error,
                "remote input request failed; retrying"
            );
            self.runtime.network.sleep(wait).await;
        }
    }

    fn deliver(
        &self,
        keys: Vec<S::Key>,
        outcome: Result<Vec<Result<S::Value, S::Error>>, Error<S::Error>>,
        trip: u64,
    ) {
        let now = self.runtime.now();
        match outcome {
            Ok(answers) => {
                for (key, answer) in keys.into_iter().zip(answers) {
                    self.settle(key, answer.map_err(Error::Source), trip, now);
                }
            }
            Err(error) => {
                Stats::add(&self.stats.failures, 1);
                tracing::debug!(
                    input = self.source.name(),
                    keys = keys.len(),
                    %error,
                    "remote input request ended without answers"
                );
                for key in keys {
                    self.settle(key, Err(error.clone()), trip, now);
                }
            }
        }
    }

    fn settle(&self, key: S::Key, result: ResultOf<S>, trip: u64, now: u64) {
        let Some(flight) = self.flights.get(&key) else {
            return;
        };
        if let Ok(value) = &result {
            if let Some(bytes) = self.source.cache_bytes(&key, value) {
                if self
                    .cache
                    .insert(key.clone(), value.clone(), bytes, now, flight.generation)
                {
                    Stats::add(&self.stats.cached, 1);
                }
            }
        }
        flight.settle(Answer { result, trip });
        self.flights
            .remove_if(&key, |current| Arc::ptr_eq(current, &flight));
    }

    /// Waits for every slot's answer, together bounded by the deadline.
    async fn wait(&self, slots: Vec<Slot<S>>, start: u64) -> Vec<ResultOf<S>> {
        let mut timer = None;
        let mut expired = false;
        let mut trips = RapidHashSet::default();
        let mut results = Vec::with_capacity(slots.len());
        for slot in slots {
            let flight = match slot {
                Slot::Ready(result) => {
                    results.push(result);
                    continue;
                }
                Slot::Waiting(flight) => flight,
            };
            if flight.answer().is_none() && !expired {
                let left = micros(self.config.deadline)
                    .saturating_sub(self.runtime.now().saturating_sub(start));
                let timer = timer
                    .get_or_insert_with(|| self.runtime.network.sleep(Duration::from_micros(left)));
                let settled = async {
                    flight.settled().await;
                    true
                }
                .or(async {
                    timer.await;
                    false
                })
                .await;
                expired = !settled;
            }
            match flight.answer() {
                Some(answer) => {
                    if answer.trip != 0 {
                        trips.insert(answer.trip);
                    }
                    results.push(answer.result.clone());
                }
                None => {
                    Stats::add(&self.stats.deadlines, 1);
                    results.push(Err(Error::Deadline(self.config.deadline)));
                }
            }
        }
        self.stats
            .wait_micros
            .record(self.runtime.now().saturating_sub(start));
        self.stats.trips_per_call.record(trips.len() as u64);
        results
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// A drive in progress. Dropped mid-round-trip (its runtime stopping), it
/// settles the batch it carried as [`Error::Closed`] and frees its slot, so
/// no waiter or lane is left behind.
struct Driving<'a, S>
where
    S: Source,
{
    inner: &'a Inner<S>,
    lane: &'a Lane<S::Key>,
    holding: bool,
    batch: Vec<S::Key>,
}

impl<S> Drop for Driving<'_, S>
where
    S: Source,
{
    fn drop(&mut self) {
        if !self.batch.is_empty() {
            let now = self.inner.runtime.now();
            for key in std::mem::take(&mut self.batch) {
                self.inner.settle(key, Err(Error::Closed), 0, now);
            }
        }
        if self.holding {
            self.lane.release();
        }
    }
}
