//! A peerstore placed on a worker pool: a caller on the network edge (the
//! retry sweep, the failure recorder, a replay's sends) runs its peerstore
//! calls there, one crossing per call ([`Peerstore::run`]).

use std::future::Future;
use std::sync::Arc;

use super::Peerstore;
use crate::corekv::{Result, Store};
use crate::namespace::NamespacedStore;

impl<S: Store> Clone for Peerstore<S> {
    fn clone(&self) -> Self {
        Self {
            store: NamespacedStore::new(Arc::clone(self.store.inner()), self.store.namespace()),
            retry_schedule: self.retry_schedule.clone(),
            retry: self.retry,
            pool: self.pool.clone(),
        }
    }
}

impl<S: Store> Peerstore<S> {
    /// Runs the calls made through [`run`](Self::run) on `pool`.
    pub fn placed_on(mut self, pool: Option<&workers::Pool>) -> Self {
        self.pool = pool.cloned();
        self
    }

    /// The pool it is placed on.
    pub fn pool(&self) -> Option<&workers::Pool> {
        self.pool.as_ref()
    }
}

impl<S: Store + 'static> Peerstore<S> {
    /// Runs `call` on this peerstore on the pool it is placed on: in place on
    /// a worker, or without a pool.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn run<T, F, Fut>(&self, call: F) -> Result<T>
    where
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + 'static,
        T: Send + 'static,
    {
        let peerstore = self.clone();
        let target = workers::Target::LeastLoaded;
        workers::run_on(self.pool.as_ref(), target, move || call(peerstore))
            .await
            .map_err(|error| match error {
                workers::Error::ShutDown => crate::corekv::Error::DBClosed,
                error => crate::corekv::Error::Backend(error.to_string()),
            })?
    }

    /// A browser's one worker is the page's thread: the call runs in place.
    #[cfg(target_arch = "wasm32")]
    pub async fn run<T, F, Fut>(&self, call: F) -> Result<T>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        call(self.clone()).await
    }
}
