use rapidhash::RapidHashMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
#[cfg(not(feature = "wasmtime-runtime"))]
use lens::UnsupportedTransformStore;
#[cfg(feature = "wasmtime-runtime")]
use lens::WasmTransformStore;
use lens::{LensConfig, LensDocResultStream, LensDocStream, TransformId, TransformStore};

#[cfg(feature = "wasmtime-runtime")]
use crate::Error;
use crate::Result;

/// Transaction-local transform store overlay.
///
/// Reads fall back to the committed global store, while writes remain isolated to
/// the transaction-local store until commit promotion runs.
pub(crate) struct TxnLensStore {
    base: Arc<dyn TransformStore>,
    local: OnceLock<std::result::Result<Arc<dyn TransformStore>, String>>,
}

impl TxnLensStore {
    pub(crate) fn new(base: Arc<dyn TransformStore>) -> Result<Self> {
        Ok(Self {
            base,
            local: OnceLock::new(),
        })
    }

    fn local(&self) -> lens::Result<&Arc<dyn TransformStore>> {
        self.local
            .get_or_init(|| {
                self.base.new_isolated_store().map(Ok).unwrap_or_else(|| {
                    Self::create_local_store().map_err(|error| error.to_string())
                })
            })
            .as_ref()
            .map_err(|error| lens::Error::WasmLoad(error.clone()))
    }

    fn initialized_local(&self) -> Option<&Arc<dyn TransformStore>> {
        self.local.get().and_then(|result| result.as_ref().ok())
    }

    #[cfg(feature = "wasmtime-runtime")]
    fn create_local_store() -> Result<Arc<dyn TransformStore>> {
        let store = WasmTransformStore::with_sandbox(Some(lens::WasmSandboxConfig::restrictive()))
            .map_err(|e| Error::Lens(format!("failed to create transaction lens store: {}", e)))?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "wasmtime-runtime"))]
    fn create_local_store() -> Result<Arc<dyn TransformStore>> {
        Ok(Arc::new(UnsupportedTransformStore))
    }
}

#[async_trait]
impl TransformStore for TxnLensStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        self.local()?.add(config).await
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> lens::Result<()> {
        self.local()?.add_with_id(id, config).await
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
        let store = TxnLensStore::new(base).unwrap();

        assert!(store.local.get().is_none());
        assert!(store.list().await.unwrap().is_empty());
        assert!(!store.has_transform(&TransformId::new("missing")));
        assert!(store.local.get().is_none());

        // Attempting a registration initializes the local map even though the
        // deliberately incomplete config is rejected by the runtime.
        assert!(store
            .add(LensConfig::new("v1", "v2", lens::LensModule::default()))
            .await
            .is_err());
        assert!(store.local.get().is_some());
    }
}
