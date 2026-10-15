//! The lensed fetcher's cache of migration contexts.
//!
//! A context (whether a collection version has lens migrations, and its
//! targeted history) is computed from the committed migration graph, so it
//! holds for one generation of that graph. The cache publishes one immutable
//! snapshot per generation through an `Atom`: a reader loads it without
//! waiting, a computed context is added by publishing a copy that holds it,
//! a newer generation starts from an empty snapshot, and a context computed
//! under an older generation is never published.

use std::sync::Arc;

use lens::TargetedHistoryLink;
use rapidhash::{HashMapExt, RapidHashMap};
use regolith::sync::Atom;

/// Whether a collection version has lens migrations, and its targeted
/// history when it has.
pub(super) type MigrationContext = (bool, Option<RapidHashMap<String, TargetedHistoryLink>>);

/// Migration contexts keyed by `"{collection_id}:{version_id}"`, one snapshot
/// per committed migration-graph generation.
#[derive(Default)]
pub(super) struct MigrationCache {
    snapshot: Atom<Snapshot>,
}

impl MigrationCache {
    /// The context cached for `key` at `generation`.
    pub(super) fn get(&self, generation: u64, key: &str) -> Option<Arc<MigrationContext>> {
        self.snapshot.peek(|snapshot| snapshot.get(generation, key))
    }

    /// Publishes `context`, computed at `generation`, under `key`. Returns
    /// false, publishing nothing, when a newer generation is cached already:
    /// the context may be stale under it.
    pub(super) fn publish(
        &self,
        generation: u64,
        key: &str,
        context: &Arc<MigrationContext>,
    ) -> bool {
        let mut published = false;
        self.snapshot.rcu(|snapshot| {
            let next = snapshot.with(generation, key, context);
            published = next.is_some();
            next.unwrap_or_else(|| snapshot.clone())
        });
        published
    }
}

/// The contexts computed at one generation, never changed once published.
#[derive(Clone, Default)]
struct Snapshot {
    generation: u64,
    contexts: RapidHashMap<String, Arc<MigrationContext>>,
}

impl Snapshot {
    fn get(&self, generation: u64, key: &str) -> Option<Arc<MigrationContext>> {
        if self.generation != generation {
            return None;
        }
        self.contexts.get(key).map(Arc::clone)
    }

    /// This snapshot with `context` added at `generation`, or `None` when this
    /// snapshot is of a newer generation. The contexts of an older one are
    /// dropped whole.
    fn with(&self, generation: u64, key: &str, context: &Arc<MigrationContext>) -> Option<Self> {
        if self.generation > generation {
            return None;
        }
        let mut contexts = if self.generation == generation {
            self.contexts.clone()
        } else {
            RapidHashMap::with_capacity(1)
        };
        contexts.insert(key.to_owned(), Arc::clone(context));
        Some(Self {
            generation,
            contexts,
        })
    }
}

#[cfg(test)]
#[path = "../../../../tests/read/lensed_migration_cache.rs"]
mod tests;
