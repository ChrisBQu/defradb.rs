/// Database transaction wrapper matching Go's internal/db/txn.go.
///
/// DbTxn wraps a BasicTxn and adds:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache (lazy loading)
/// - Reference to the database for collection operations
use crate::collection::cache::CollectionCache;
use crate::collection::{populate_collection_root_id, Collection};
use crate::error::{Error, Result};
use async_lock::{RwLockReadGuardArc, RwLockWriteGuardArc};
use datastore::{AsyncCallback, BasicTxn, NamespaceView, RootView, TxnCallback};
use rapidhash::{HashSetExt, RapidHashSet};
use schema::CollectionVersion;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::index::vector::engine::diskann::CodeCaches;
use storage::corekv::{IterOptions, Key, Store};
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
    collection_guard_ids: Vec<String>,
}

enum CollectionGuard {
    Read { _guard: RwLockReadGuardArc<()> },
    Write { _guard: RwLockWriteGuardArc<()> },
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
    /// Collection locks held until this transaction commits or rolls back.
    collection_guards: BTreeMap<String, CollectionGuard>,
    /// Branchable collection ACP registrations that must succeed before the
    /// storage commit, otherwise a protected collection could be persisted
    /// without its collection-level ACP object.
    pending_collection_acp_registrations: Vec<PendingCollectionAcpRegistration>,
    /// The database's resident DiskANN codes, for searches in this transaction.
    vector_codes: Arc<CodeCaches>,
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
            collection_guards: BTreeMap::new(),
            pending_collection_acp_registrations: Vec::new(),
            vector_codes: Arc::default(),
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

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn) -> Self {
        Self {
            txn: Some(txn),
            explicit: true,
            collection_cache: CollectionCache::new(),
            locally_created_collection_ids: RapidHashSet::new(),
            collection_guards: BTreeMap::new(),
            pending_collection_acp_registrations: Vec::new(),
            vector_codes: Arc::default(),
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
        storage::corekv::RetryCallbacks,
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
        let (io, mut callbacks) = txn.into_retry_attempt()?;

        let collection_guards = std::mem::take(&mut self.collection_guards);
        if !collection_guards.is_empty() {
            callbacks.push_sync(Box::new(move || drop(collection_guards)));
        }
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
            collection_guard_ids: self.collection_guards.keys().cloned().collect(),
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
        self.collection_guards
            .retain(|key, _| snapshot.collection_guard_ids.contains(key));
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

    /// Get the rootstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn rootstore(&self) -> Result<RootView> {
        self.txn
            .as_ref()
            .map(|t| t.rootstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Register a callback for successful commit.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_success(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a synchronous post-commit event for receipt-ordered dispatch.
    pub fn on_success_ordered(
        &mut self,
        callback: storage::corekv::OrderedTxnCallback,
    ) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success_ordered(callback)?;
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register an async post-commit hook that must finish before ordered events publish.
    pub fn on_success_ordered_async(
        &mut self,
        callback: storage::corekv::AsyncOrderedTxnCallback,
    ) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success_ordered_async(callback)?;
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Move ordered callbacks out of a completed mutation attempt.
    pub fn take_ordered_callbacks(&mut self) -> Result<storage::corekv::OrderedTxnCallbacks> {
        self.txn
            .as_mut()
            .map(BasicTxn::take_ordered_callbacks)
            .ok_or(Error::TxnNotActive)
    }

    /// Register callbacks from a winning attempt on this commit transaction.
    pub fn register_ordered_callbacks(
        &mut self,
        callbacks: storage::corekv::OrderedTxnCallbacks,
    ) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.register_ordered_callbacks(callbacks)?;
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for commit error.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_error(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_error(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register an async callback for successful commit.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_success_async(&mut self, callback: AsyncCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success_async(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a receipt-ordered publication callback for a retry attempt.
    pub fn on_retry_success_ordered(
        &mut self,
        callback: storage::corekv::OrderedRetryCallback,
    ) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_retry_success_ordered(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register an async postcommit hook that must complete before ordered
    /// event publication. Its error is returned after the durable commit.
    pub fn on_retry_success_ordered_async(
        &mut self,
        callback: storage::corekv::AsyncOrderedRetryCallback,
    ) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_retry_success_ordered_async(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for discard.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_discard(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_discard(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
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

    pub(crate) fn has_collection_guard(&self, collection_id: &str) -> bool {
        self.collection_guards.contains_key(collection_id)
    }

    pub(crate) fn has_collection_write_guard(&self, collection_id: &str) -> bool {
        matches!(
            self.collection_guards.get(collection_id),
            Some(CollectionGuard::Write { .. })
        )
    }

    pub(crate) fn insert_collection_read_guard(
        &mut self,
        collection_id: String,
        guard: RwLockReadGuardArc<()>,
    ) {
        self.collection_guards
            .entry(collection_id)
            .or_insert(CollectionGuard::Read { _guard: guard });
    }

    pub(crate) fn insert_collection_write_guard(
        &mut self,
        collection_id: String,
        guard: RwLockWriteGuardArc<()>,
    ) {
        self.collection_guards
            .insert(collection_id, CollectionGuard::Write { _guard: guard });
    }

    pub(crate) fn remove_collection_guard(&mut self, collection_id: &str) {
        self.collection_guards.remove(collection_id);
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
                self.release_collection_guards();
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
                self.release_collection_guards();
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
                self.release_collection_guards();
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
                self.release_collection_guards();
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Release mutation guards after the transaction is durably committed or discarded.
    fn release_collection_guards(&mut self) {
        self.collection_guards.clear();
    }

    fn discard_active_txn(&mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => {
                let result = txn.discard().map_err(Error::Datastore);
                self.release_collection_guards();
                result
            }
            None => Err(Error::TxnNotActive),
        }
    }
}
