//! Immutable permission-engine snapshots. No epoch guard survives an await.
use kovan::Atom;
use std::sync::Arc;
use zanzibar::{engine::PermissionEngine, store::ZanzibarStore};

pub(crate) struct PolicyCache<S: ZanzibarStore + ?Sized> {
    store: Arc<S>,
    current: Atom<Arc<zanzibar::lookup::PolicyLookupTable>>,
}

impl<S: ZanzibarStore + ?Sized> PolicyCache<S> {
    pub(crate) fn new(store: Arc<S>) -> Self {
        Self {
            store,
            current: Atom::new(Arc::new(zanzibar::lookup::PolicyLookupTable::new())),
        }
    }

    pub(crate) fn snapshot(&self) -> PermissionEngine<S> {
        PermissionEngine::with_lookup(self.store.clone(), self.current.load_clone())
    }

    pub(crate) fn edit(&self, edit: impl Fn(&mut PermissionEngine<S>)) {
        self.current.rcu(|old| {
            let mut next = PermissionEngine::with_lookup(self.store.clone(), old.clone());
            edit(&mut next);
            next.lookup
        });
    }

    // A fetch begun before invalidation/reload must not publish over it.
    pub(crate) fn publish_if_current(
        &self,
        expected: &PermissionEngine<S>,
        next: PermissionEngine<S>,
    ) -> bool {
        let current = self.current.load();
        if !Arc::ptr_eq(&current, &expected.lookup) {
            return false;
        }
        self.current.compare_and_swap(&current, next.lookup).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zanzibar::{MemoryZanzibarStore, Policy};

    #[test]
    fn invalidation_rejects_stale_reload_and_keeps_existing_snapshot_immutable() {
        let cache = PolicyCache::new(Arc::new(MemoryZanzibarStore::new()));
        cache.edit(|engine| engine.add_policy(&Policy::new("policy", "old")));
        let before_fetch = cache.snapshot();
        let mut fetched = before_fetch.clone();
        fetched.add_policy(&Policy::new("policy", "stale"));
        cache.edit(|engine| engine.remove_policy("policy"));
        assert!(!cache.publish_if_current(&before_fetch, fetched));
        assert!(!cache.snapshot().lookup.has_policy("policy"));
        assert!(before_fetch.lookup.has_policy("policy"));
    }

    #[test]
    fn independent_policy_publications_do_not_lose_updates() {
        let cache = PolicyCache::new(Arc::new(MemoryZanzibarStore::new()));
        std::thread::scope(|scope| {
            for id in 0..16 {
                let cache = &cache;
                scope.spawn(move || {
                    let policy = Policy::new(id.to_string(), "policy");
                    cache.edit(|engine| engine.add_policy(&policy));
                });
            }
        });
        let snapshot = cache.snapshot();
        for id in 0..16 {
            assert!(snapshot.lookup.has_policy(&id.to_string()));
        }
    }
}
