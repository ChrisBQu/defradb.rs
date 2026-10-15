use rapidhash::RapidHashMap;
use regolith::sync::OnceCell;
use std::sync::Arc;

use async_trait::async_trait;
use lens::{LensConfig, LensDocResultStream, LensDocStream, TransformId, TransformStore};

/// Transaction-local transform store overlay.
///
/// Reads fall back to the committed global store, while writes remain isolated to
/// the transaction-local store until commit promotion runs. The local store comes
/// from the global one, so it shares the database's lens runtime and epoch clock.
pub(crate) struct TxnLensStore {
    base: Arc<dyn TransformStore>,
    local: OnceCell<Arc<dyn TransformStore>>,
}

impl TxnLensStore {
    pub(crate) fn new(base: Arc<dyn TransformStore>) -> Self {
        Self {
            base,
            local: OnceCell::new(),
        }
    }

    fn local(&self) -> &Arc<dyn TransformStore> {
        self.local
            .get_or_init_racy(|| self.base.new_isolated_store())
    }

    fn initialized_local(&self) -> Option<&Arc<dyn TransformStore>> {
        self.local.get()
    }
}

#[async_trait]
impl TransformStore for TxnLensStore {
    fn new_isolated_store(&self) -> Arc<dyn TransformStore> {
        self.base.new_isolated_store()
    }

    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        self.local().add(config).await
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> lens::Result<()> {
        self.local().add_with_id(id, config).await
    }

    async fn list(&self) -> lens::Result<RapidHashMap<String, lens::LensModule>> {
        let mut result = self.base.list().await?;
        if let Some(local) = self.initialized_local() {
            result.extend(local.list().await?);
        }
        Ok(result)
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        if self
            .initialized_local()
            .is_some_and(|local| local.has_transform(id))
        {
            self.initialized_local().unwrap().transform(id, docs)
        } else {
            self.base.transform(id, docs)
        }
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        if self
            .initialized_local()
            .is_some_and(|local| local.has_transform(id))
        {
            self.initialized_local().unwrap().inverse(id, docs)
        } else {
            self.base.inverse(id, docs)
        }
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.initialized_local()
            .is_some_and(|local| local.has_transform(id))
            || self.base.has_transform(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        if self
            .initialized_local()
            .is_some_and(|local| local.has_transform(id))
        {
            self.initialized_local().unwrap().remove(id).await
        } else {
            self.base.remove(id).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transaction_local_store_is_created_only_for_registration() {
        let base: Arc<dyn TransformStore> = Arc::new(lens::MemoryTransformStore::new());
        let store = TxnLensStore::new(base.clone());

        assert!(store.local.get().is_none());
        assert!(store.list().await.unwrap().is_empty());
        assert!(!store.has_transform(&TransformId::new("missing")));
        assert!(store.local.get().is_none());

        // The registration lands in an empty store of the base's own kind. A
        // memory store accepts a module no WASM runtime would compile, so no
        // private engine was built for the transaction.
        let id = store
            .add(LensConfig::new("v1", "v2", lens::LensModule::default()))
            .await
            .unwrap();
        assert!(store.local.get().is_some());
        assert!(store.has_transform(&id));
        assert!(!base.has_transform(&id));
        assert_eq!(store.list().await.unwrap().len(), 1);
    }
}
