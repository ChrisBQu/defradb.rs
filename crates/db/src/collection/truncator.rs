use std::sync::Arc;

use async_trait::async_trait;
use storage::corekv::Store;

use crate::DB;

/// Query-layer adapter for collection truncate mutations.
pub struct DbCollectionTruncator<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> DbCollectionTruncator<S> {
    pub fn new_arc(db: Arc<DB<S>>) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> query::CollectionTruncator for DbCollectionTruncator<S> {
    async fn truncate(
        &self,
        context: &defra_core::RequestContext,
        collection_name: &str,
        filter: Option<query::Filter>,
    ) -> query::Result<()> {
        let result = match filter {
            Some(filter) => {
                self.db
                    .truncate_collection_with_filter(context, collection_name, filter)
                    .await
            }
            None => self.db.truncate_collection(context, collection_name).await,
        };
        result.map_err(|error| query::QueryError::execution(error.to_string()))
    }
}
