//! Adapter to bridge DbTransactionRegistry to HTTP's TransactionOperations trait.
//!
//! The underlying `DbTransactionRegistry` methods hold a transaction lock guard
//! (`DbTxn<S>`) across await points. Because `DbTxn<S>` contains non-`Sync` types
//! (closures), the resulting futures are not `Send`. They run pinned to one
//! thread of the network runtime ([`Edge::spawn_pinned`]), which needs no `Send`,
//! in the context of the request the adapter is bound to.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use defra_core::RequestContext;

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
    /// The request this adapter serves, for one bound to it.
    context: Option<RequestContext>,
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
            context: None,
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
            context: None,
        })
    }

    /// The context of the request this adapter serves.
    fn context(&self) -> RequestContext {
        RequestContext::bound_or_ambient(self.context.as_ref()).into_owned()
    }

    /// Runs the registry call `run` builds pinned to one thread of the network
    /// runtime, in the context of the request this adapter serves.
    async fn pinned<T, R, Fut>(&self, run: R) -> Result<T, String>
    where
        R: FnOnce(Arc<db::DbTransactionRegistry<S>>, RequestContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, String>> + 'static,
        T: Send + 'static,
    {
        let registry = self.registry.clone();
        let context = self.context();
        self.edge
            .spawn_pinned(move || run(registry, context))
            .await
            .map_err(|e| format!("task join error: {}", e))?
    }
}

#[async_trait]
impl<S> TransactionOperations for TxnRegistryAdapter<S>
where
    S: Store + 'static,
{
    fn bind_request(&self, context: &RequestContext) -> Option<Arc<dyn TransactionOperations>> {
        Some(Arc::new(Self {
            registry: Arc::clone(&self.registry),
            document_acp: self.document_acp.clone(),
            edge: self.edge.clone(),
            context: Some(context.clone()),
        }))
    }

    async fn set_migration_in_txn(&self, txn_id: &str, config: &str) -> Result<String, String> {
        let lens_config: lens::LensConfig = serde_json::from_str(config)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;
        let txn_id = txn_id.to_string();
        self.pinned(move |registry, context| async move {
            registry
                .set_migration_in_txn_as(&context, &txn_id, lens_config)
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
        self.pinned(move |registry, context| async move {
            registry
                .get_collections_in_txn_as(&context, &txn_id)
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
        // Fail closed: a present-but-malformed acting identity must error, not
        // silently degrade a permissioned collection to public (unregistered).
        let creator = match self.context().identity() {
            Some(raw) => {
                Some(Did::new(raw).map_err(|e| format!("malformed acting identity: {}", e))?)
            }
            None => None,
        };
        let txn_id = txn_id.to_string();
        let sdl = sdl.to_string();
        self.pinned(move |registry, context| async move {
            registry
                .add_schema_in_txn_with_acp_as(&context, &txn_id, &sdl, document_acp, creator)
                .await
                .map_err(|e| format!("{}", e))
        })
        .await
    }
}
