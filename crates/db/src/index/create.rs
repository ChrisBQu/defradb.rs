//! Index creation commits a schema-version dependency and its `BACKFILL_INDEX`
//! action; the documents that already exist are indexed afterwards by the
//! batches in [`backfill`](crate::index::backfill).

use std::sync::Arc;

use futures::channel::oneshot;
use schema::{IndexDescription, IndexKind, IndexedFieldDescription};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

use crate::database::action::ActionExecutionLease;
use crate::database::spawn::spawn_task;
use crate::error::{Error, Result};
use crate::index::backfill::BackfillPlan;
use crate::index::IndexManager;
use crate::DB;

impl<S: Store> DB<S> {
    /// Create an index of `kind` over `fields` on `collection_name`, then
    /// index every document the collection already holds. A missing or empty
    /// `name` is generated.
    ///
    /// The definition and in-progress action commit together, and the cache
    /// reloads before the new fence is captured. This makes writes that land
    /// before publication part of the backfill range and lets later writes
    /// maintain the index themselves. Backfill then runs in bounded
    /// transactions detached from this future so a caller that gives up on
    /// waiting does not stop it; the planner uses the index once the action
    /// completes. A failed backfill leaves the definition in place and the
    /// failure on the action.
    pub async fn create_index(
        self: &Arc<Self>,
        collection_name: &str,
        name: Option<&str>,
        fields: Vec<IndexedFieldDescription>,
        kind: IndexKind,
    ) -> Result<IndexDescription>
    where
        S: 'static,
    {
        let (index, lease, plan) = self
            .define_index(collection_name, name, fields, kind)
            .await?;
        let (done, outcome) = oneshot::channel();
        let db = Arc::clone(self);
        spawn_task(async move {
            let _ = done.send(db.backfill_index(lease, plan).await);
        });
        outcome
            .await
            .map_err(|_| Error::Other("the index backfill ended without reporting".into()))??;
        Ok(index)
    }

    async fn define_index(
        &self,
        collection_name: &str,
        name: Option<&str>,
        fields: Vec<IndexedFieldDescription>,
        kind: IndexKind,
    ) -> Result<(IndexDescription, ActionExecutionLease, BackfillPlan)> {
        let collection = self.require_collection(collection_name)?;
        let collection_id = collection.collection_id().to_string();

        let mut txn = self.new_txn(false).await?;
        txn.validate_collection_for_write(&collection).await?;
        let (index, lease, committed_schema) = {
            let datastore = txn.datastore()?;
            let systemstore = txn.systemstore()?;
            let mut manager = IndexManager::from_collection(
                collection.schema().resolved_root_id(),
                collection.schema(),
            )?;
            let index = manager
                .create_index_of_kind(
                    &datastore,
                    collection_name,
                    name.unwrap_or("").to_string(),
                    fields,
                    kind,
                    &collection.schema().fields,
                )
                .await?;
            let mut schema = collection.schema().clone();
            schema.indexes.push(index.clone());
            let data = serde_json::to_vec(&schema).map_err(|error| {
                Error::collection_schema_json(
                    format!("failed to serialize schema for collection '{collection_name}'"),
                    error,
                )
            })?;
            systemstore
                .set(&CollectionKey::new(&schema.version_id).bytes(), &data)
                .await?;
            systemstore
                .set(
                    &CollectionNameKey::new(collection_name).bytes(),
                    schema.version_id.as_bytes(),
                )
                .await?;
            let lease = self
                .stage_action(
                    &systemstore,
                    &collection_id,
                    defra_core::Action::BACKFILL_INDEX,
                    &index.id.to_string(),
                )
                .await?;
            (index, lease, schema)
        };
        let cached_collection = crate::collection::Collection::load_index_actions(
            committed_schema,
            &txn.systemstore()?,
        )
        .await?;
        let cache = std::sync::Arc::clone(&self.collections);
        txn.on_success_ordered(std::sync::Arc::new(move || {
            cache.rcu(|old| {
                let mut cache = old.clone();
                cache.put(cached_collection.clone());
                cache
            });
        }))?;
        txn.commit().await?;
        // Publish the index definition before taking the backfill fence. A
        // create that commits before this definition becomes visible may use
        // the old schema; capturing the fence afterward guarantees its ID is
        // included in the scan. If the process stops before the first batch
        // persists progress, startup captures a fresh fence after publication.
        let fence = self.reserve_doc_short_id_fence().await?;
        self.publish_started_action(&lease);
        let plan = BackfillPlan::new(collection_name, &collection_id, &index, fence, 0);
        Ok((index, lease, plan))
    }
}
