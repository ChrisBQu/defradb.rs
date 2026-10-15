/// Database transaction wrapper matching Go's internal/db/txn.go.
///
/// DbTxn wraps a BasicTxn and adds:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache (lazy loading)
/// - Reference to the database for collection operations
use crate::collection::cache::CollectionCache;
use crate::collection::{populate_collection_root_id, Collection};
use crate::error::{Error, Result};
use datastore::{BasicTxn, NamespaceView, RootView};
use rapidhash::{HashSetExt, RapidHashSet};
use schema::CollectionVersion;
use std::sync::Arc;

use crate::index::vector::engine::diskann::CodeCaches;
use storage::corekv::{IterOptions, Key, OnAbort, OnCommit, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

pub mod context;
pub(crate) mod lenses;
pub mod registry;

#[derive(Clone)]
struct PendingCollectionAcpRegistration {
    acp: Arc<dyn acp::DocumentACP>,
    creator: identity::Did,
    request_bearer_token: Option<String>,
    schemas: Vec<CollectionVersion>,
}

struct StatementSavepoint {
    collection_cache: CollectionCache,
    locally_created_collection_ids: RapidHashSet<String>,
    pending_collection_acp_registrations: Vec<PendingCollectionAcpRegistration>,
}

impl PendingCollectionAcpRegistration {
    async fn run(self) -> Result<()> {
        let creator = self.creator;
        let result = defra_core::signing::scope_request_bearer_token(
            Some(creator.to_string()),
            self.request_bearer_token,
            async {
                for schema in &self.schemas {
                    crate::collection::acp::register_collection_if_needed(
                        self.acp.as_ref(),
                        Some(&creator),
                        schema,
                    )
                    .await?;
                }
                Ok::<_, acp::Error>(())
            },
        )
        .await;

        result.map_err(Error::from)
    }
}

/// Database transaction wrapper.
///
/// This wraps a BasicTxn and provides:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache with lazy loading
/// - Access to the underlying store for collection operations
///
/// Explicit transactions are created by the user and must be explicitly
/// committed or discarded. When a method receives an explicit transaction,
/// it should NOT commit or discard it.
///
/// Implicit transactions are created internally by database methods.
/// They are automatically committed on success and discarded on error.
///
/// Transaction liveness is tracked by the `txn` field:
/// - `Some(txn)` = transaction is active
/// - `None` = transaction has been committed or discarded
///
/// The collection cache is populated lazily from the SystemStore on first
/// access, matching the Go DefraDB pattern for transaction isolation.
pub struct DbTxn<S: Store> {
    /// The underlying BasicTxn. `None` after commit/discard.
    txn: Option<BasicTxn>,
    /// Whether this is an explicit transaction.
    explicit: bool,
    /// Transaction-scoped collection cache (lazy loading from SystemStore).
    collection_cache: CollectionCache,
    /// Collection IDs created inside this transaction.
    locally_created_collection_ids: RapidHashSet<String>,
    /// Branchable collection ACP registrations that must succeed before the
    /// storage commit, otherwise a protected collection could be persisted
    /// without its collection-level ACP object.
    pending_collection_acp_registrations: Vec<PendingCollectionAcpRegistration>,
    /// The database's resident DiskANN codes, for searches in this transaction.
    vector_codes: Arc<CodeCaches>,
    /// The database's committed collections, so a collection this transaction
    /// loads with the same definition reuses the cached handle's decoded state.
    shared_collections: Option<Arc<crate::collection::epoch::EpochedCollections>>,
    statement_savepoint: Option<StatementSavepoint>,
    /// Phantom data for the store type.
    _marker: std::marker::PhantomData<S>,
}

impl<S: Store> DbTxn<S> {
    /// Create a new implicit DbTxn.
    pub fn new(txn: BasicTxn) -> Self {
        Self {
            txn: Some(txn),
            explicit: false,
            collection_cache: CollectionCache::new(),
            locally_created_collection_ids: RapidHashSet::new(),
            pending_collection_acp_registrations: Vec::new(),
            vector_codes: Arc::default(),
            shared_collections: None,
            statement_savepoint: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn with_vector_codes(mut self, codes: Arc<CodeCaches>) -> Self {
        self.vector_codes = codes;
        self
    }

    pub(crate) fn vector_codes(&self) -> &Arc<CodeCaches> {
        &self.vector_codes
    }

    pub(crate) fn with_shared_collections(
        mut self,
        collections: Arc<crate::collection::epoch::EpochedCollections>,
    ) -> Self {
        self.shared_collections = Some(collections);
        self
    }

    /// The database's committed handle for `name`, if it has one.
    pub(crate) fn shared_collection(&self, name: &str) -> Option<Collection> {
        self.shared_collections
            .as_ref()?
            .peek(|map| map.get(name).cloned())
    }

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn) -> Self {
        Self {
            txn: Some(txn),
            explicit: true,
            collection_cache: CollectionCache::new(),
            locally_created_collection_ids: RapidHashSet::new(),
            pending_collection_acp_registrations: Vec::new(),
            vector_codes: Arc::default(),
            shared_collections: None,
            statement_savepoint: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Consume this DB transaction into the engine's scoped retry attempt.
    /// Namespace views must already be dropped; the returned callback bundle
    /// is held by storage until the winning commit receipt is available.
    pub fn into_retry_attempt(
        mut self,
    ) -> storage::corekv::Result<(
        Box<dyn storage::corekv::RetryAttemptIO>,
        storage::corekv::TxnCallbacks,
    )> {
        if !self.pending_collection_acp_registrations.is_empty() {
            return Err(storage::corekv::Error::Other(
                "retry attempts cannot carry pending collection ACP registrations".into(),
            ));
        }
        let txn = self
            .txn
            .take()
            .ok_or(storage::corekv::Error::DiscardedTxn)?;
        let (io, callbacks) = txn.into_retry_attempt()?;

        Ok((io, callbacks))
    }

    /// Mark this transaction as explicit.
    ///
    /// Explicit transactions are not automatically committed/discarded
    /// when passed to database methods.
    pub fn make_explicit(&mut self) {
        self.explicit = true;
    }

    pub(crate) fn set_statement_savepoint(&mut self) -> Result<()> {
        if !self.explicit || self.statement_savepoint.is_some() {
            return Err(Error::Other(
                "statement savepoint requires an explicit transaction with no active statement"
                    .into(),
            ));
        }
        let snapshot = StatementSavepoint {
            collection_cache: self.collection_cache.clone(),
            locally_created_collection_ids: self.locally_created_collection_ids.clone(),
            pending_collection_acp_registrations: self.pending_collection_acp_registrations.clone(),
        };
        self.txn
            .as_mut()
            .ok_or(Error::TxnNotActive)?
            .set_savepoint()?;
        self.statement_savepoint = Some(snapshot);
        Ok(())
    }

    pub(crate) fn rollback_statement_savepoint(&mut self) -> Result<()> {
        if self.statement_savepoint.is_none() {
            return Err(Error::Other("no statement savepoint is active".into()));
        }
        self.txn
            .as_mut()
            .ok_or(Error::TxnNotActive)?
            .rollback_to_savepoint()?;
        let snapshot = self
            .statement_savepoint
            .take()
            .expect("savepoint checked above");
        self.collection_cache = snapshot.collection_cache;
        self.locally_created_collection_ids = snapshot.locally_created_collection_ids;
        self.pending_collection_acp_registrations = snapshot.pending_collection_acp_registrations;
        Ok(())
    }

    pub(crate) fn release_statement_savepoint(&mut self) -> Result<()> {
        if self.statement_savepoint.is_none() {
            return Err(Error::Other("no statement savepoint is active".into()));
        }
        self.txn
            .as_mut()
            .ok_or(Error::TxnNotActive)?
            .release_savepoint()?;
        self.statement_savepoint = None;
        Ok(())
    }

    /// Check if this is an explicit transaction.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Get the transaction ID.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn id(&self) -> Result<u64> {
        self.txn.as_ref().map(|t| t.id()).ok_or(Error::TxnNotActive)
    }

    /// Check if this is a read-only transaction.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn is_readonly(&self) -> Result<bool> {
        self.txn
            .as_ref()
            .map(|t| t.is_readonly())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the blockstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn blockstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.blockstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the datastore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn datastore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.datastore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the encstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn encstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.encstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the headstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn headstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.headstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the peerstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn peerstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.peerstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the systemstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn systemstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.systemstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the access-control policy store.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn acpstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.acpstore())
            .ok_or(Error::TxnNotActive)
    }

    /// The registrations this transaction's creates owe an external ACP
    /// provider, read through its own writes. A transaction no longer active
    /// shows none.
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub fn acp_registration_projection(&self) -> Arc<query::txn::AcpRegistrationProjection> {
        Arc::new(match self.acpstore() {
            Ok(view) => {
                query::txn::AcpRegistrationProjection::new(acp::PendingRegistrations::new(&view))
            }
            Err(_) => query::txn::AcpRegistrationProjection::detached(),
        })
    }

    /// Get the rootstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn rootstore(&self) -> Result<RootView> {
        self.txn
            .as_ref()
            .map(|t| t.rootstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Register `callback` to run once if the transaction commits. A
    /// read-only transaction refuses ordered work.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_commit(&mut self, callback: OnCommit) -> Result<()> {
        self.txn
            .as_mut()
            .ok_or(Error::TxnNotActive)?
            .on_commit(callback)?;
        Ok(())
    }

    /// Register `callback` to run once if the transaction ends without
    /// committing.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_abort(&mut self, callback: OnAbort) -> Result<()> {
        self.txn
            .as_mut()
            .ok_or(Error::TxnNotActive)?
            .on_abort(callback);
        Ok(())
    }

    // =========================================================================
    // Collection Cache Methods (Transaction-scoped caching)
    // =========================================================================

    /// Get a collection by name, loading from SystemStore if not in cache.
    ///
    /// This implements lazy loading - collections are loaded on first access.
    /// Returns `None` if the collection doesn't exist in the store.
    ///
    /// Note: This method is structured to avoid holding `&mut self` across awaits,
    /// which allows futures using this method to be `Send`.
    ///
    /// Storage layout:
    /// - `/collection/name/{name}` - Maps name to version_id (string)
    /// - `/collection/id/{version_id}` - Full collection JSON
    pub async fn get_collection(&mut self, name: &str) -> Result<Option<&Collection>> {
        // Check cache first
        if self.collection_cache.contains(name) {
            return Ok(self.collection_cache.get(name));
        }

        // Cache miss: extract systemstore synchronously, then do async operation
        let systemstore = self.systemstore()?;
        let name_key = CollectionNameKey::new(name);

        // Step 1: Get version_id from name mapping
        let maybe_version_id = systemstore
            .get(&name_key.bytes())
            .await
            .map_err(Error::Storage)?;

        let version_id = match maybe_version_id {
            Some(data) => {
                // Try to parse as version_id string first (new format)
                match std::str::from_utf8(&data) {
                    Ok(vid) if !vid.starts_with('{') => vid.to_string(), // Not JSON, it's a version_id
                    _ => {
                        // Fallback: Old format where full JSON is stored at name key
                        // This handles backward compatibility during migration
                        let mut schema: CollectionVersion =
                            serde_json::from_slice(&data).map_err(|e| {
                                tracing::error!(
                                    error = ?e,
                                    collection_name = %name,
                                    "Failed to deserialize schema for collection"
                                );
                                Error::collection_schema_json(
                                    format!(
                                        "failed to deserialize schema for collection '{}'",
                                        name
                                    ),
                                    e,
                                )
                            })?;
                        populate_collection_root_id(&systemstore, &mut schema).await?;
                        self.collection_cache.add(Collection::new(schema));
                        return Ok(self.collection_cache.get(name));
                    }
                }
            }
            None => return Ok(None),
        };

        // Step 2: Get full collection from /collection/id/{version_id}
        let collection_key = CollectionKey::new(&version_id);
        let maybe_data = systemstore
            .get(&collection_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Process result and update cache
        if let Some(data) = maybe_data {
            let mut schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    version_id = %version_id,
                    "Failed to deserialize schema for collection"
                );
                Error::collection_schema_json(
                    format!("failed to deserialize schema for collection '{}'", name),
                    e,
                )
            })?;
            populate_collection_root_id(&systemstore, &mut schema).await?;
            self.collection_cache.add(Collection::new(schema));
            return Ok(self.collection_cache.get(name));
        }

        // version_id found but no collection data - inconsistent state
        tracing::warn!(
            collection_name = %name,
            version_id = %version_id,
            "Collection version_id found but definition missing"
        );
        Ok(None)
    }

    /// Load all collections from SystemStore into the cache.
    ///
    /// This is called when listing collections or when we need to iterate
    /// over all collections. After calling this, `is_fully_populated()` returns true.
    ///
    /// Storage layout:
    /// - `/collection/name/{name}` - Maps name to version_id (string)
    /// - `/collection/id/{version_id}` - Full collection JSON
    pub async fn load_all_collections(&mut self) -> Result<()> {
        if self.collection_cache.is_fully_populated() {
            return Ok(());
        }

        let prefix = CollectionNameKey::name_prefix();
        let mut collections = Vec::new();

        let systemstore = self.systemstore()?;
        let opts = IterOptions::new().with_prefix(prefix.clone());

        let mut iter = systemstore.iterator(opts).await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to create iterator during collection load");
            Error::Storage(e)
        })?;

        // Collect version_ids first (to avoid holding iterator across lookups)
        let mut name_version_pairs: Vec<(String, String)> = Vec::new();

        while let Some(pair) = iter.next().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to iterate collections during load");
            Error::Storage(e)
        })? {
            // Validate UTF-8 in key
            let key_str = String::from_utf8(pair.key.to_vec()).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    key_bytes = ?&pair.key[..pair.key.len().min(50)],
                    "Collection key contains invalid UTF-8"
                );
                Error::text_decode("collection key contains invalid UTF-8", e)
            })?;

            let prefix_str = String::from_utf8(prefix.clone()).map_err(|e| {
                Error::Other(format!("internal error: prefix is not valid UTF-8: {}", e))
            })?;

            let name = key_str
                .strip_prefix(&prefix_str)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "collection key '{}' does not match expected prefix '{}'",
                        key_str, prefix_str
                    ))
                })?
                .to_string();

            // Check if value is a version_id string or full JSON (backward compat)
            if let Ok(s) = std::str::from_utf8(&pair.value) {
                if !s.starts_with('{') {
                    // New format: value is version_id
                    name_version_pairs.push((name, s.to_string()));
                    continue;
                }
            }

            // Old format: value is full JSON
            let schema: CollectionVersion = serde_json::from_slice(&pair.value).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to deserialize schema for collection '{}': {}",
                    name,
                    e
                );
                Error::collection_schema_json(
                    format!("failed to deserialize schema for collection '{}'", name),
                    e,
                )
            })?;
            let mut schema = schema;
            populate_collection_root_id(&systemstore, &mut schema).await?;

            collections.push(Collection::new(schema));
        }

        iter.close().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to close iterator during collection load");
            Error::Storage(e)
        })?;

        // Load collections from version_ids (new format)
        for (name, version_id) in name_version_pairs {
            let collection_key = CollectionKey::new(&version_id);
            match systemstore.get(&collection_key.bytes()).await {
                Ok(Some(data)) => {
                    let schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            version_id = %version_id,
                            "Failed to deserialize schema"
                        );
                        Error::collection_schema_json(
                            format!("failed to deserialize schema for collection '{}'", name),
                            e,
                        )
                    })?;
                    let mut schema = schema;
                    populate_collection_root_id(&systemstore, &mut schema).await?;
                    collections.push(Collection::new(schema));
                }
                Ok(None) => {
                    tracing::warn!(
                        collection_name = %name,
                        version_id = %version_id,
                        "Collection version_id found but definition missing"
                    );
                }
                Err(e) => {
                    return Err(Error::Storage(e));
                }
            }
        }

        self.collection_cache.populate(collections);
        Ok(())
    }

    /// Add a collection to the transaction-scoped cache.
    ///
    /// The key is derived from the collection's name to prevent key-name mismatches.
    pub fn cache_collection(&mut self, collection: Collection) {
        self.collection_cache.add(collection);
    }

    /// Remove a collection from the transaction-scoped cache.
    ///
    /// Called by delete_collection to update the cache after writing to store.
    pub fn uncache_collection(&mut self, name: &str) {
        self.collection_cache.remove(name);
    }

    /// Get the collection cache.
    ///
    /// Use this for read-only access to iterate over cached collections.
    pub fn collection_cache(&self) -> &CollectionCache {
        &self.collection_cache
    }

    /// Get mutable access to the collection cache.
    ///
    /// Use this for advanced cache manipulation (e.g., populate from snapshot).
    pub fn collection_cache_mut(&mut self) -> &mut CollectionCache {
        &mut self.collection_cache
    }

    /// Mark a collection as created inside this transaction.
    pub fn mark_collection_created(&mut self, collection_id: impl Into<String>) {
        self.locally_created_collection_ids
            .insert(collection_id.into());
    }

    /// Check whether a collection was created inside this transaction.
    pub fn was_collection_created(&self, collection_id: &str) -> bool {
        self.locally_created_collection_ids.contains(collection_id)
    }

    /// Load the collection and its index-action state from this transaction's
    /// snapshot for an engine-owned write attempt. The expected schema version
    /// keeps a retry from applying a previously prepared document to a changed
    /// collection definition, while index-only updates (which preserve the
    /// version ID) use the current snapshot's write/query index sets.
    pub(crate) async fn load_collection_for_write_attempt(
        &mut self,
        name: &str,
        expected: &Collection,
    ) -> Result<Collection> {
        let systemstore = self.systemstore()?;
        let collection = crate::collection::loader::load_collection_for_write_attempt(
            &systemstore,
            name,
            expected,
        )
        .await?;
        self.collection_cache.add(collection.clone());
        Ok(collection)
    }

    /// Validate the active collection name and immutable schema through this
    /// transaction so concurrent DDL changes conflict at commit.
    pub(crate) async fn validate_collection_for_write(
        &mut self,
        collection: &Collection,
    ) -> Result<()> {
        let systemstore = self.systemstore()?;
        let version_id = systemstore
            .get(&CollectionNameKey::new(collection.name()).bytes())
            .await
            .map_err(Error::Storage)?
            .and_then(|bytes| String::from_utf8(bytes.into()).ok());
        if version_id.as_deref() != Some(collection.version_id()) {
            return Err(Error::CollectionNotFound(collection.name().to_string()));
        }
        drop(systemstore);
        self.validate_collection_schema_for_write(collection).await
    }

    /// Validate an immutable schema key for a remote merge, even if it is not
    /// the currently active version associated with its collection name.
    pub(crate) async fn validate_collection_schema_for_write(
        &mut self,
        collection: &Collection,
    ) -> Result<()> {
        let systemstore = self.systemstore()?;
        let schema_bytes = systemstore
            .get(&CollectionKey::new(collection.version_id()).bytes())
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| Error::CollectionNotFound(collection.name().to_string()))?;
        let mut persisted_schema: CollectionVersion = serde_json::from_slice(&schema_bytes)
            .map_err(|error| {
                Error::collection_schema_json(
                    format!(
                        "failed to deserialize schema for collection '{}'",
                        collection.name()
                    ),
                    error,
                )
            })?;
        populate_collection_root_id(&systemstore, &mut persisted_schema).await?;
        let mut cached_schema = collection.schema().clone();
        cached_schema.indexes = cached_schema
            .indexes
            .into_iter()
            .map(schema::IndexDescription::normalized)
            .collect();
        if persisted_schema != cached_schema {
            return Err(Error::Other(format!(
                "cached schema differs from persisted version '{}'",
                collection.version_id()
            )));
        }
        let statuses = crate::collection::IndexStatuses::load_actions(
            &systemstore,
            collection.collection_id(),
        )
        .await?;
        let persisted = Collection::with_index_actions(persisted_schema, &statuses);
        if !persisted.same_index_statuses(collection) {
            return Err(Error::Other(format!(
                "cached index actions differ for collection '{}'",
                collection.name()
            )));
        }
        Ok(())
    }

    /// Stage branchable collection ACP registration to run before commit.
    pub(crate) fn stage_collection_acp_registration(
        &mut self,
        acp: Arc<dyn acp::DocumentACP>,
        creator: identity::Did,
        schemas: Vec<CollectionVersion>,
    ) {
        if schemas.is_empty() {
            return;
        }
        let request_bearer_token = defra_core::signing::get_request_bearer_token(creator.as_str());
        self.pending_collection_acp_registrations
            .push(PendingCollectionAcpRegistration {
                acp,
                creator,
                request_bearer_token,
                schemas,
            });
    }

    async fn run_pending_collection_acp_registrations(&mut self) -> Result<()> {
        for mut registration in std::mem::take(&mut self.pending_collection_acp_registrations) {
            if let Some(scoped) = registration.acp.bind_transaction(self.acpstore()?) {
                registration.acp = scoped;
            }
            registration.run().await?;
        }
        Ok(())
    }

    // =========================================================================
    // Transaction Lifecycle Methods
    // =========================================================================

    /// Commit the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_commit()` instead.
    /// Returns an error if the transaction is not active.
    pub async fn commit(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        if let Err(error) = self.run_pending_collection_acp_registrations().await {
            if let Err(discard_error) = self.discard_active_txn() {
                tracing::warn!(
                    error = %discard_error,
                    "failed to discard transaction after collection ACP registration error"
                );
            }
            return Err(error);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Discard the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_discard()` instead.
    /// Returns an error if the transaction is not active.
    pub fn discard(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually commit the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub async fn force_commit(mut self) -> Result<()> {
        if let Err(error) = self.run_pending_collection_acp_registrations().await {
            if let Err(discard_error) = self.discard_active_txn() {
                tracing::warn!(
                    error = %discard_error,
                    "failed to discard transaction after collection ACP registration error"
                );
            }
            return Err(error);
        }
        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually discard the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub fn force_discard(mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    fn discard_active_txn(&mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => txn.discard().map_err(Error::Datastore),
            None => Err(Error::TxnNotActive),
        }
    }
}
