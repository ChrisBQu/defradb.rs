//! Adapter to bridge DbTransactionRegistry to HTTP's TransactionOperations trait.
//!
//! The underlying `DbTransactionRegistry` methods hold a transaction lock guard
//! (`DbTxn<S>`) across await points. Because `DbTxn<S>` contains non-`Sync` types
//! (closures), the resulting futures are not `Send`. They run pinned to one
//! thread of the network runtime ([`Edge::spawn_pinned`]), which needs no `Send`.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::TransactionOperations;
use edge::Edge;
use identity::Did;
use storage::corekv::Store;

/// Adapter that implements TransactionOperations using a shared DbTransactionRegistry.
pub struct TxnRegistryAdapter<S>
where
    S: Store + 'static,
{
    registry: Arc<db::DbTransactionRegistry<S>>,
    document_acp: Option<Arc<dyn acp::DocumentACP>>,
    edge: Edge,
}

impl<S> TxnRegistryAdapter<S>
where
    S: Store + 'static,
{
    /// Create an Arc-wrapped adapter backed by the shared transaction registry.
    pub fn new_arc(
        registry: Arc<db::DbTransactionRegistry<S>>,
        edge: Edge,
    ) -> Arc<dyn TransactionOperations> {
        Arc::new(Self {
            registry,
            document_acp: None,
            edge,
        })
    }

    /// Create an Arc-wrapped adapter with document ACP registration support.
    pub fn new_arc_with_acp(
        registry: Arc<db::DbTransactionRegistry<S>>,
        document_acp: Arc<dyn acp::DocumentACP>,
        edge: Edge,
    ) -> Arc<dyn TransactionOperations> {
        Arc::new(Self {
            registry,
            document_acp: Some(document_acp),
            edge,
        })
    }

    /// Runs the registry call `run` builds pinned to one thread of the network
    /// runtime, with the caller's acting identity set on that thread.
    async fn pinned<T, R, Fut>(&self, run: R) -> Result<T, String>
    where
        R: FnOnce(Arc<db::DbTransactionRegistry<S>>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, String>> + 'static,
        T: Send + 'static,
    {
        let registry = self.registry.clone();
        let acting_identity = defra_core::current_identity::get_effective_identity();
        self.edge
            .spawn_pinned(move || {
                let identity_guard =
                    defra_core::current_identity::scoped_current_identity(acting_identity);
                async move {
                    let _identity_guard = identity_guard;
                    run(registry).await
                }
            })
            .await
            .map_err(|e| format!("task join error: {}", e))?
    }
}

#[async_trait]
impl<S> TransactionOperations for TxnRegistryAdapter<S>
where
    S: Store + 'static,
{
    async fn set_migration_in_txn(&self, txn_id: &str, config: &str) -> Result<String, String> {
        let lens_config: lens::LensConfig = serde_json::from_str(config)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;
        let txn_id = txn_id.to_string();
        self.pinned(move |registry| async move {
            registry
                .set_migration_in_txn(&txn_id, lens_config, None)
                .await
                .map(|id| id.to_string())
                .map_err(|e| format!("{}", e))
        })
        .await
    }

    async fn get_collections_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        let txn_id = txn_id.to_string();
        self.pinned(move |registry| async move {
            registry
                .get_collections_in_txn(&txn_id)
                .await
                .map_err(|e| format!("{}", e))
        })
        .await
    }

    async fn add_schema_in_txn(
        &self,
        txn_id: &str,
        sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        let document_acp = self.document_acp.clone();
        // Fail closed: a present-but-malformed ambient identity must error, not
        // silently degrade a permissioned collection to public (unregistered).
        let creator = match defra_core::current_identity::get_effective_identity() {
            Some(raw) => {
                Some(Did::new(raw).map_err(|e| format!("malformed ambient identity: {}", e))?)
            }
            None => None,
        };
        let txn_id = txn_id.to_string();
        let sdl = sdl.to_string();
        self.pinned(move |registry| async move {
            registry
                .add_schema_in_txn_with_acp(&txn_id, &sdl, document_acp, creator)
                .await
                .map_err(|e| format!("{}", e))
        })
        .await
    }
}
