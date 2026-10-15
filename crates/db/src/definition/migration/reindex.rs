//! Datastore materialization and reindexing after schema migration.

use rapidhash::HashMapExt;
use storage::corekv::Store;
use tracing::warn;

use super::helpers::{cache_document_version, cache_migrated_document, lens_doc_to_document};
use crate::definition::loader::get_collections_by_collection_id;
use crate::error::{Error, Result};
use crate::index::IndexManager;
use crate::DB;

impl<S: Store> DB<S> {
    /// Whether a schema change leaves `collection`'s secondary indexes to
    /// rebuild: it has indexes and, for a migration (`migration_destination`),
    /// the active version's history contains the migration's destination, an
    /// ancestor version included. A version switch always needs it.
    pub(super) async fn reindex_needed(
        &self,
        collection: &crate::collection::Collection,
        migration_destination: Option<&str>,
    ) -> Result<bool> {
        if collection.get_indexes().is_empty() {
            return Ok(false);
        }
        let Some(destination) = migration_destination else {
            return Ok(true);
        };
        let read_txn = self.new_txn(true).await?;
        let versions = {
            let systemstore = read_txn.systemstore()?;
            get_collections_by_collection_id(&systemstore, collection.collection_id()).await?
        };
        let _ = read_txn.discard();
        Ok(
            crate::definition::lens::build_collection_history(&versions, collection.version_id())
                .is_some_and(|history| history.contains_key(destination)),
        )
    }

    /// Rebuild a collection's indexes, clearing the reindex owed to
    /// `collection_id` under `token` in the transaction that rebuilds them.
    pub(super) async fn rebuild_settling(
        &self,
        collection_name: &str,
        collection_id: &str,
        token: u64,
    ) -> Result<()> {
        self.materialize_collection_inner(
            collection_name,
            false,
            Some((collection_id.to_string(), token)),
        )
        .await
        .map(|_| ())
    }

    /// Rebuild secondary indexes for a collection using lens-migrated documents.
    ///
    /// Documents that cross a registered transform are also written back to the
    /// datastore so the rebuilt indexes and cached values cannot diverge.
    pub async fn reindex_collection_with_migrations(&self, collection_name: &str) -> Result<()> {
        if self.get_collection(collection_name)?.is_none() {
            return Ok(());
        }
        self.materialize_collection_inner(collection_name, false, None)
            .await
            .map(|_| ())
    }

    /// Eagerly advance every known-version document in a collection to its active version.
    ///
    /// This writes only the datastore blob, version key, and secondary indexes. It does not
    /// create CRDT commits, update heads, emit mutation events, or gossip data to peers.
    ///
    /// Returns the number of documents whose stored version was advanced.
    pub async fn materialize_collection(
        &self,
        context: &defra_core::RequestContext,
        collection_name: &str,
    ) -> Result<usize> {
        self.check_node_access(context, acp::nac::NodePermission::CollectionPatch)
            .await?;
        self.materialize_collection_inner(collection_name, true, None)
            .await
    }

    /// `settles` names a reindex owed to a collection, by its ID and token,
    /// that the rebuild clears in its own transaction.
    async fn materialize_collection_inner(
        &self,
        collection_name: &str,
        materialize_identity_paths: bool,
        settles: Option<(String, u64)>,
    ) -> Result<usize> {
        let collection = self
            .get_collection(collection_name)?
            .ok_or_else(|| Error::CollectionNotFound(collection_name.to_string()))?;
        let attempt = MaterializeAttempt {
            collection,
            collection_name: collection_name.to_owned(),
            materialize_identity_paths,
            lens_store: self.lens_store.clone(),
            settles,
        };
        match self
            .store()
            .transact_retry(self.options().retry_options(), Box::new(attempt))
            .await
        {
            Ok(value) => value
                .downcast::<usize>()
                .map(|value| *value)
                .map_err(|_| Error::Other("invalid materialization retry result".into())),
            Err(error) => match error.downcast_abort::<Error>() {
                Ok(error) => Err(error),
                Err(error) => Err(Error::Storage(error.into_storage())),
            },
        }
    }
}

struct MaterializeAttempt {
    collection: crate::collection::Collection,
    collection_name: String,
    materialize_identity_paths: bool,
    lens_store: std::sync::Arc<dyn lens::TransformStore>,
    settles: Option<(String, u64)>,
}

impl storage::corekv::RetryAttemptFn for MaterializeAttempt {
    fn run<'a>(
        &'a mut self,
        retry: &'a mut dyn storage::corekv::RetryTxn,
    ) -> storage::corekv::RetryAttemptFuture<'a> {
        Box::pin(async move {
            let io = retry.begin_attempt_io()?;
            let mut txn = datastore::BasicTxn::from_retry_attempt_io(io, 0);
            let count = self
                .materialize(&mut txn)
                .await
                .map_err(|error| storage::corekv::RetryAttemptError::Abort(Box::new(error)))?;
            let (io, callbacks) = txn.into_retry_attempt()?;
            retry.finish_attempt_io(io, callbacks)?;
            Ok(Box::new(count) as storage::corekv::RetryAttemptValue)
        })
    }
}

impl MaterializeAttempt {
    async fn materialize(&self, write_txn: &mut datastore::BasicTxn) -> Result<usize> {
        let collection = &self.collection;
        let collection_name = &self.collection_name;
        let materialize_identity_paths = self.materialize_identity_paths;
        collection
            .validate_write_definition(&write_txn.datastore(), true)
            .await?;
        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();
        let short_id = collection.resolved_root_id();

        let systemstore = write_txn.systemstore();
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        drop(systemstore);

        let history =
            crate::definition::lens::build_collection_history(&versions, &target_version_id)
                .ok_or_else(|| {
                    Error::Lens(format!(
                        "failed to build migration history for collection '{}'",
                        collection_name
                    ))
                })?;

        let mut materialized_count = 0usize;

        {
            let datastore = write_txn.datastore();
            let txn_systemstore = write_txn.systemstore();

            let raw_docs = collection
                .get_all_with_datastore_short_ids(&datastore, &txn_systemstore, false)
                .await?;

            let mut migrated_docs = Vec::with_capacity(raw_docs.len());
            let index_manager = IndexManager::from_indexes(
                short_id,
                collection.schema(),
                collection.write_indexes(),
            )
            .map_err(|e| Error::Other(format!("failed to create index manager: {}", e)))?;
            let mut original_keys = rapidhash::RapidHashMap::new();
            for (doc_short_id, doc, _) in raw_docs {
                let Some(doc_version) = doc.schema_version_id().map(str::to_string) else {
                    if materialize_identity_paths {
                        let mut restamped = doc.clone();
                        restamped.set_schema_version_id(&target_version_id);
                        if cache_document_version(&datastore, &txn_systemstore, collection, &doc)
                            .await?
                        {
                            materialized_count += 1;
                        }
                        migrated_docs.push((doc_short_id, restamped));
                    } else {
                        migrated_docs.push((doc_short_id, doc));
                    }
                    continue;
                };

                if doc_version == target_version_id {
                    migrated_docs.push((doc_short_id, doc));
                    continue;
                }

                if !history.contains_key(&doc_version) {
                    warn!(
                        doc_id = ?doc.id(),
                        doc_version = %doc_version,
                        target_version = %target_version_id,
                        "Skipping materialization for a document version unknown locally"
                    );
                    migrated_docs.push((doc_short_id, doc));
                    continue;
                }

                let path_has_transform = crate::definition::lens::migration_path_has_transform(
                    &history,
                    &doc_version,
                    &target_version_id,
                );
                if !path_has_transform {
                    if materialize_identity_paths {
                        let cached =
                            cache_document_version(&datastore, &txn_systemstore, collection, &doc)
                                .await?;
                        let mut restamped = doc;
                        restamped.set_schema_version_id(&target_version_id);
                        if cached {
                            materialized_count += 1;
                        }
                        migrated_docs.push((doc_short_id, restamped));
                    } else {
                        migrated_docs.push((doc_short_id, doc));
                    }
                    continue;
                }

                let lens_doc = crate::definition::lens::doc_to_lens_doc(&doc).ok_or_else(|| {
                    Error::Lens(format!(
                        "failed to convert document {:?} for materialization",
                        doc.id()
                    ))
                })?;
                let migrated_lens_doc = lens::transform_to_target(
                    &self.lens_store,
                    &target_version_id,
                    &history,
                    lens::LensInput::new(&doc_version, lens_doc),
                )
                .await
                .map_err(|error| Error::Lens(error.to_string()))?;
                let migrated = lens_doc_to_document(migrated_lens_doc, &doc, collection);
                original_keys.insert(
                    doc_short_id,
                    index_manager.unique_index_keys(&doc, collection.schema())?,
                );
                if cache_migrated_document(&datastore, &txn_systemstore, collection, &migrated)
                    .await?
                {
                    materialized_count += 1;
                }
                migrated_docs.push((doc_short_id, migrated));
            }

            if !collection.write_indexes().is_empty() {
                for index_desc in collection.write_indexes() {
                    if let Some(index) = index_manager.get_index(&index_desc.name) {
                        index
                            .remove_all(&mut datastore.clone())
                            .await
                            .map_err(Error::Storage)?;
                    }

                    index_manager
                        .bulk_index_resolving_unique_conflicts(
                            &datastore,
                            &txn_systemstore,
                            &index_desc.name,
                            &migrated_docs,
                            collection.schema(),
                            &original_keys,
                        )
                        .await?;
                }
            }

            if let Some((collection_id, token)) = &self.settles {
                super::owed_reindex::clear_owed_reindex(&txn_systemstore, collection_id, *token)
                    .await?;
            }

            tracing::debug!(
                collection = %collection_name,
                doc_count = migrated_docs.len(),
                materialized_count,
                index_count = collection.get_indexes().len(),
                materialize_identity_paths,
                "Materialized collection datastore and rebuilt indexes"
            );
        }

        Ok(materialized_count)
    }
}
