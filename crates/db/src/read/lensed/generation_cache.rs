//! A cache of values derived from one generation of their source, read
//! without a lock.
//!
//! The lensed fetchers derive values (a collection's version facts, its
//! targeted history, its migration context) from a source that changes in
//! generations: the committed migration graph, or a transaction's own schema
//! writes. The cache publishes one immutable snapshot per generation through
//! an `Atom`: a reader loads it without waiting, a computed value is added by
//! publishing a copy that holds it, a newer generation starts from an empty
//! snapshot, and a value computed under an older generation is never
//! published.

use std::sync::Arc;

use rapidhash::{HashMapExt, RapidHashMap};
use regolith::sync::Atom;

/// Values keyed by string, one snapshot per generation.
///
/// The generation is either the source's (the committed migration graph's,
/// passed to [`get`](Self::get) and [`publish`](Self::publish)) or the
/// cache's own, read with [`generation`](Self::generation) and advanced with
/// [`invalidate`](Self::invalidate). One cache uses one kind.
pub(crate) struct GenerationCache<V>
where
    V: Send + Sync + 'static,
{
    snapshot: Atom<Snapshot<V>>,
}

impl<V> Default for GenerationCache<V>
where
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            snapshot: Atom::new(Snapshot::default()),
        }
    }
}

impl<V> GenerationCache<V>
where
    V: Send + Sync + 'static,
{
    /// The generation of the snapshot published last.
    pub(crate) fn generation(&self) -> u64 {
        self.snapshot.peek(|snapshot| snapshot.generation)
    }

    /// The value cached for `key` at `generation`.
    pub(crate) fn get(&self, generation: u64, key: &str) -> Option<Arc<V>> {
        self.snapshot.peek(|snapshot| snapshot.get(generation, key))
    }

    /// Publishes `value`, computed at `generation`, under `key`. Returns
    /// false, publishing nothing, when a newer generation is cached already:
    /// the value may be stale under it.
    pub(crate) fn publish(&self, generation: u64, key: &str, value: &Arc<V>) -> bool {
        let mut published = false;
        self.snapshot.rcu(|snapshot| {
            let next = snapshot.with(generation, key, value);
            published = next.is_some();
            next.unwrap_or_else(|| snapshot.clone())
        });
        published
    }

    /// Starts the next generation of a cache whose generations are its own:
    /// every value cached so far is dropped, and a value computed before this
    /// call is refused by [`publish`](Self::publish).
    pub(crate) fn invalidate(&self) {
        self.snapshot.rcu(|snapshot| Snapshot {
            generation: snapshot.generation + 1,
            values: RapidHashMap::new(),
        });
    }
}

/// The values computed at one generation, never changed once published.
struct Snapshot<V> {
    generation: u64,
    values: RapidHashMap<String, Arc<V>>,
}

impl<V> Default for Snapshot<V> {
    fn default() -> Self {
        Self {
            generation: 0,
            values: RapidHashMap::new(),
        }
    }
}

impl<V> Clone for Snapshot<V> {
    fn clone(&self) -> Self {
        Self {
            generation: self.generation,
            values: self.values.clone(),
        }
    }
}

impl<V> Snapshot<V> {
    fn get(&self, generation: u64, key: &str) -> Option<Arc<V>> {
        if self.generation != generation {
            return None;
        }
        self.values.get(key).map(Arc::clone)
    }

    /// This snapshot with `value` added at `generation`, or `None` when this
    /// snapshot is of a newer generation. The values of an older one are
    /// dropped whole.
    fn with(&self, generation: u64, key: &str, value: &Arc<V>) -> Option<Self> {
        if self.generation > generation {
            return None;
        }
        let mut values = if self.generation == generation {
            self.values.clone()
        } else {
            RapidHashMap::with_capacity(1)
        };
        values.insert(key.to_owned(), Arc::clone(value));
        Some(Self { generation, values })
    }
}

#[cfg(test)]
#[path = "../../../tests/read/lensed_generation_cache.rs"]
mod tests;
