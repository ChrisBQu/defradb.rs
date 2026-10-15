//! Durable storage for pending-DAG registrations (#1099).
//!
//! A hub success-acks a PushLog whose DAG has missing links on the strength of
//! its pending-DAG registration; the pusher then deletes its persisted retry
//! record. If the registration lives only in process memory, a crash between
//! the ack and Bitswap completion silently loses the document (modeled in
//! `proofs/tla/PendingDagRestart.tla`). Installing a store makes each
//! push-originated registration durable: it is written before the success
//! reply, deleted only when the root is successfully marked merged or
//! quarantined (#1128 — a deterministic content rejection that will never
//! succeed on replay; never at DagReady emission, TTL eviction, or clear),
//! and re-driven at startup and by the periodic resync sweep via
//! `resync_persisted_pending_dags`.
//!
//! Pull-originated registrations (DocSync/BranchableSync) are not persisted —
//! no remote retry state is destroyed by their acks, so restart loss is
//! recoverable by re-issuing the pull.

use async_trait::async_trait;
use cid::Cid;
use defra_core::thread_bounds::MaybeSendSync;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use storage::corekv::{
    IterOptions, Key, Reader, RetryAttemptFn, RetryAttemptFuture, RetryAttemptValue, RetryOptions,
    RetryTxn, Store,
};
use storage::keys::systemstore::{P2PPendingDagKey, P2PQuarantinedDagKey};
use storage::stores::Systemstore;
use web_time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::ExplicitReplayAuthorization;

const PENDING_STORE_TXN_MAX_ATTEMPTS: u32 = 3;

/// Maximum number of same-root announcers retained in addition to the
/// immutable origin provider. Each unavailable provider consumes a bounded
/// selective-CAR attempt, so this durable list must remain bounded too.
pub(crate) const MAX_PENDING_DAG_ALTERNATE_PROVIDERS: usize = 3;

struct PendingStoreWriteAttempt {
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl RetryAttemptFn for PendingStoreWriteAttempt {
    fn run<'a>(&'a mut self, retry: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        Box::pin(async move {
            let io = retry.begin_attempt_io()?;
            let txn = datastore::BasicTxn::from_retry_attempt_io(io, 0);
            let systemstore = txn.systemstore();
            for (key, value) in &self.writes {
                match value {
                    Some(value) => systemstore.set(key, value).await?,
                    None => systemstore.delete(key).await?,
                }
            }
            drop(systemstore);
            let (io, callbacks) = txn.into_retry_attempt()?;
            retry.finish_attempt_io(io, callbacks)?;
            Ok(Box::new(()) as RetryAttemptValue)
        })
    }
}

/// Verified explicit-replay authorization carried by a persisted registration.
/// The original proof is retained so a restored replay can authenticate a
/// reverse KMS request without trusting the request's claimed DID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedReplayAuthorization {
    pub source_peer_id: String,
    pub target_peer_id: String,
    pub collection_id: String,
    pub authorizer_did: String,
    pub expires_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
}

impl From<&ExplicitReplayAuthorization> for PersistedReplayAuthorization {
    fn from(auth: &ExplicitReplayAuthorization) -> Self {
        Self {
            source_peer_id: auth.source_peer_id.clone(),
            target_peer_id: auth.target_peer_id.clone(),
            collection_id: auth.collection_id.clone(),
            authorizer_did: auth.authorizer_did.clone(),
            expires_at: auth.expires_at,
            capability: auth.capability.clone(),
        }
    }
}

impl From<PersistedReplayAuthorization> for ExplicitReplayAuthorization {
    fn from(auth: PersistedReplayAuthorization) -> Self {
        Self {
            source_peer_id: auth.source_peer_id,
            target_peer_id: auth.target_peer_id,
            collection_id: auth.collection_id,
            authorizer_did: auth.authorizer_did,
            expires_at: auth.expires_at,
            capability: auth.capability,
        }
    }
}

/// Compact durable form of one pending-DAG registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedPendingDag {
    pub doc_id: String,
    pub collection_id: String,
    #[serde(default)]
    pub head_priority: Option<u64>,
    pub creator: String,
    pub source_peer: Option<String>,
    /// Additional authenticated peers that announced this exact root.
    /// The original source remains immutable; alternates only extend the
    /// recovery rotation and therefore cannot hijack ownership.
    #[serde(default)]
    pub alternate_providers: Vec<String>,
    pub is_explicit_replicator: bool,
    pub explicit_replay_authorization: Option<PersistedReplayAuthorization>,
}

impl PersistedPendingDag {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        defra_core::cbor::to_vec(self).map_err(|e| Error::Storage(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        defra_core::cbor::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
    }
}

/// A pending-DAG record whose merge failed deterministically (e.g. a unique-index
/// rejection): moved out of the live keyspace, retained for forensics and counted,
/// and never re-driven by the resync sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedQuarantinedDag {
    pub record: PersistedPendingDag,
    pub reason: String,
    pub quarantined_at_unix_secs: u64,
}

impl PersistedQuarantinedDag {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        defra_core::cbor::to_vec(self).map_err(|e| Error::Storage(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        defra_core::cbor::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
    }

    /// Current wall-clock time as Unix seconds, for stamping `quarantined_at_unix_secs`.
    pub fn now_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// Durable KV backing for push-originated pending-DAG registrations.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PendingDagStorage: MaybeSendSync {
    async fn put(&self, root_cid: &Cid, record: &PersistedPendingDag) -> Result<()>;
    /// Atomically install `root_cid` and retire an older head from the same
    /// sender/scope. A crash must observe either the old obligation or the new
    /// one, never neither and never an ever-growing per-root ledger.
    async fn replace_scope_head(
        &self,
        superseded_root: Option<&Cid>,
        root_cid: &Cid,
        record: &PersistedPendingDag,
    ) -> Result<()>;
    async fn remove(&self, root_cid: &Cid) -> Result<()>;
    async fn load_all(&self) -> Result<Vec<(Cid, PersistedPendingDag)>>;

    /// Move a terminally-rejected root into the quarantine keyspace. The caller
    /// is responsible for deleting the live `/p2p/pending_dag/` record (write
    /// quarantine first, delete live second, so a crash mid-transition leaves
    /// a re-drivable live record rather than a silently lost one).
    async fn quarantine(&self, root_cid: &Cid, entry: &PersistedQuarantinedDag) -> Result<()>;
    async fn is_quarantined(&self, root_cid: &Cid) -> Result<bool>;
    async fn load_quarantined(&self) -> Result<Vec<(Cid, PersistedQuarantinedDag)>>;
    async fn remove_quarantined(&self, root_cid: &Cid) -> Result<()>;
}

/// `PendingDagStorage` over the systemstore keyspace (`/p2p/pending_dag/`).
pub struct PendingDagStore<S: Store> {
    store: Arc<S>,
    systemstore: Systemstore<S>,
}

impl<S: Store> PendingDagStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            systemstore: Systemstore::new(Arc::clone(&store)),
            store,
        }
    }

    async fn write_value(
        &self,
        operation: &'static str,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<()> {
        let writes = vec![(key.to_vec(), value.map(<[u8]>::to_vec))];
        self.transact_write(operation, writes).await
    }

    async fn transact_write(
        &self,
        operation: &'static str,
        writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<()> {
        let attempt = PendingStoreWriteAttempt { writes };
        match self
            .store
            .transact_retry(
                RetryOptions::new(PENDING_STORE_TXN_MAX_ATTEMPTS),
                Box::new(attempt),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if matches!(error, storage::corekv::RetryAttemptError::Exhausted(_)) {
                    tracing::warn!(
                        operation,
                        max_attempts = PENDING_STORE_TXN_MAX_ATTEMPTS,
                        "Pending-DAG transaction conflicts exhausted"
                    );
                }
                Err(Error::Storage(error.into_storage().to_string()))
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> PendingDagStorage for PendingDagStore<S> {
    async fn put(&self, root_cid: &Cid, record: &PersistedPendingDag) -> Result<()> {
        let key = P2PPendingDagKey::new(root_cid.to_string());
        let value = record.to_bytes()?;
        self.write_value("pending_store.put", &key.bytes(), Some(&value))
            .await
    }

    async fn replace_scope_head(
        &self,
        superseded_root: Option<&Cid>,
        root_cid: &Cid,
        record: &PersistedPendingDag,
    ) -> Result<()> {
        let new_key = P2PPendingDagKey::new(root_cid.to_string()).bytes();
        let old_key = superseded_root
            .filter(|old| *old != root_cid)
            .map(|old| P2PPendingDagKey::new(old.to_string()).bytes());
        let value = record.to_bytes()?;
        let mut writes = vec![(new_key, Some(value))];
        if let Some(old_key) = old_key {
            writes.push((old_key, None));
        }
        self.transact_write("pending_store.replace_scope_head", writes)
            .await
    }

    async fn remove(&self, root_cid: &Cid) -> Result<()> {
        let key = P2PPendingDagKey::new(root_cid.to_string());
        self.write_value("pending_store.remove", &key.bytes(), None)
            .await
    }

    async fn load_all(&self) -> Result<Vec<(Cid, PersistedPendingDag)>> {
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let opts = IterOptions::new().with_prefix(P2PPendingDagKey::p2p_pending_dag_prefix());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut records = Vec::new();
        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let Some(cid_str) = key_str.strip_prefix("/p2p/pending_dag/") else {
                continue;
            };
            let Ok(root_cid) = cid_str.parse::<Cid>() else {
                tracing::warn!(key = %key_str, "Skipping persisted pending DAG with invalid CID key");
                continue;
            };
            match PersistedPendingDag::from_bytes(&pair.value) {
                Ok(record) => records.push((root_cid, record)),
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Skipping undecodable persisted pending DAG record"
                    );
                }
            }
        }
        iter.close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(records)
    }

    async fn quarantine(&self, root_cid: &Cid, entry: &PersistedQuarantinedDag) -> Result<()> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        let value = entry.to_bytes()?;
        self.write_value("pending_store.quarantine", &key.bytes(), Some(&value))
            .await
    }

    async fn is_quarantined(&self, root_cid: &Cid) -> Result<bool> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let value = txn
            .get(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(value.is_some())
    }

    async fn load_quarantined(&self) -> Result<Vec<(Cid, PersistedQuarantinedDag)>> {
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let opts =
            IterOptions::new().with_prefix(P2PQuarantinedDagKey::p2p_quarantined_dag_prefix());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut records = Vec::new();
        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let Some(cid_str) = key_str.strip_prefix("/p2p/quarantined_dag/") else {
                continue;
            };
            let Ok(root_cid) = cid_str.parse::<Cid>() else {
                tracing::warn!(key = %key_str, "Skipping quarantined DAG with invalid CID key");
                continue;
            };
            match PersistedQuarantinedDag::from_bytes(&pair.value) {
                Ok(record) => records.push((root_cid, record)),
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Skipping undecodable quarantined DAG record"
                    );
                }
            }
        }
        iter.close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(records)
    }

    async fn remove_quarantined(&self, root_cid: &Cid) -> Result<()> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        self.write_value("pending_store.remove_quarantined", &key.bytes(), None)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use multihash_codetable::{Code, MultihashDigest};
    use storage::RegolithStore;

    struct ConflictInjectingStore {
        inner: Arc<RegolithStore>,
        conflict_key: Vec<u8>,
        conflicts_remaining: Arc<AtomicUsize>,
        attempt_calls: Arc<AtomicUsize>,
    }

    impl storage::corekv::private::Sealed for ConflictInjectingStore {}

    #[async_trait]
    impl Store for ConflictInjectingStore {
        async fn allocate(
            &self,
            key: &[u8],
            count: u64,
        ) -> storage::corekv::Result<std::ops::Range<u64>> {
            self.inner.allocate(key, count).await
        }

        fn transaction_stats_handle(&self) -> Option<storage::backends::TransactionStatsHandle> {
            self.inner.transaction_stats_handle()
        }

        async fn new_txn(
            &self,
            readonly: bool,
        ) -> storage::corekv::Result<Box<dyn storage::corekv::Txn>> {
            self.inner.new_txn(readonly).await
        }

        async fn transact_retry(
            &self,
            options: RetryOptions,
            attempt: storage::corekv::RetryAttempt,
        ) -> std::result::Result<RetryAttemptValue, storage::corekv::RetryAttemptError> {
            let wrapped = Box::new(InjectCommitConflict {
                inner: Some(attempt),
                store: Arc::clone(&self.inner),
                conflict_key: self.conflict_key.clone(),
                conflicts_remaining: Arc::clone(&self.conflicts_remaining),
                attempt_calls: Arc::clone(&self.attempt_calls),
            });
            self.inner.transact_retry(options, wrapped).await
        }

        async fn close(&self) -> storage::corekv::Result<()> {
            self.inner.close().await
        }
    }

    struct InjectCommitConflict {
        inner: Option<storage::corekv::RetryAttempt>,
        store: Arc<RegolithStore>,
        conflict_key: Vec<u8>,
        conflicts_remaining: Arc<AtomicUsize>,
        attempt_calls: Arc<AtomicUsize>,
    }

    impl RetryAttemptFn for InjectCommitConflict {
        fn run<'a>(&'a mut self, retry: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
            Box::pin(async move {
                self.attempt_calls.fetch_add(1, Ordering::SeqCst);
                let output = self.inner.as_mut().unwrap().run(retry).await?;
                let mut remaining = self.conflicts_remaining.load(Ordering::SeqCst);
                let mut inject_conflict = false;
                while remaining > 0 {
                    match self.conflicts_remaining.compare_exchange_weak(
                        remaining,
                        remaining - 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            inject_conflict = true;
                            break;
                        }
                        Err(current) => remaining = current,
                    }
                }
                if inject_conflict {
                    let mut competitor = self.store.new_txn(false).await?;
                    competitor
                        .set(&self.conflict_key, b"competing write")
                        .await?;
                    competitor.commit().await?;
                }
                Ok(output)
            })
        }
    }

    fn conflict_injecting_store(
        conflicts: usize,
    ) -> (Arc<ConflictInjectingStore>, Arc<AtomicUsize>) {
        let inner = Arc::new(RegolithStore::in_memory().unwrap());
        let root = cid(b"bounded retry test");
        let key = P2PPendingDagKey::new(root.to_string()).bytes();
        let mut conflict_key = Vec::with_capacity(1 + key.len());
        conflict_key.push(storage::namespace::Namespace::Systemstore.prefix());
        conflict_key.extend_from_slice(&key);
        let attempts = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(ConflictInjectingStore {
            inner,
            conflict_key,
            conflicts_remaining: Arc::new(AtomicUsize::new(conflicts)),
            attempt_calls: Arc::clone(&attempts),
        });
        (store, attempts)
    }

    fn record(doc: &str) -> PersistedPendingDag {
        PersistedPendingDag {
            doc_id: doc.to_string(),
            collection_id: "collection".to_string(),
            head_priority: None,
            creator: "creator".to_string(),
            source_peer: Some("peer-1".to_string()),
            alternate_providers: Vec::new(),
            is_explicit_replicator: true,
            explicit_replay_authorization: Some(PersistedReplayAuthorization {
                source_peer_id: "peer-1".to_string(),
                target_peer_id: "peer-2".to_string(),
                collection_id: "collection".to_string(),
                authorizer_did: "did:key:z123".to_string(),
                expires_at: 42,
                capability: Some("signed-capability".to_string()),
            }),
        }
    }

    fn cid(seed: &[u8]) -> Cid {
        Cid::new_v1(0x55, Code::Sha2_256.digest(seed))
    }

    #[test]
    fn legacy_pending_record_decodes_with_no_alternate_providers() {
        #[derive(Serialize)]
        struct LegacyPendingDag {
            doc_id: String,
            collection_id: String,
            head_priority: Option<u64>,
            creator: String,
            source_peer: Option<String>,
            is_explicit_replicator: bool,
            explicit_replay_authorization: Option<PersistedReplayAuthorization>,
        }

        let bytes = defra_core::cbor::to_vec(&LegacyPendingDag {
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            head_priority: Some(1),
            creator: "creator".to_string(),
            source_peer: Some("origin".to_string()),
            is_explicit_replicator: true,
            explicit_replay_authorization: None,
        })
        .expect("encode legacy record");

        let decoded = PersistedPendingDag::from_bytes(&bytes).expect("decode legacy record");
        assert_eq!(decoded.source_peer.as_deref(), Some("origin"));
        assert!(decoded.alternate_providers.is_empty());
    }

    #[tokio::test]
    async fn pending_store_retries_a_commit_conflict_once_then_persists_value() {
        let (root, attempts) = conflict_injecting_store(1);
        let store = PendingDagStore::new(root);
        let root_cid = cid(b"bounded retry test");

        store.put(&root_cid, &record("doc-a")).await.unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded, vec![(root_cid, record("doc-a"))]);
    }

    #[tokio::test]
    async fn pending_store_preserves_three_attempt_conflict_bound() {
        let (root, attempts) = conflict_injecting_store(PENDING_STORE_TXN_MAX_ATTEMPTS as usize);
        let store = PendingDagStore::new(root);
        let root_cid = cid(b"bounded retry test");

        let error = store.put(&root_cid, &record("doc-a")).await.unwrap_err();

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            PENDING_STORE_TXN_MAX_ATTEMPTS as usize
        );
        assert!(
            matches!(error, Error::Storage(ref message) if message.contains("retry")),
            "unexpected terminal error: {error:?}"
        );
        let key = P2PPendingDagKey::new(root_cid.to_string());
        let txn = store.systemstore.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(&key.bytes()).await.unwrap(),
            Some(bytes::Bytes::from_static(b"competing write"))
        );
        txn.discard();
    }

    #[tokio::test]
    async fn replace_scope_head_is_atomic_through_engine_retry_api() {
        let store = PendingDagStore::new(Arc::new(RegolithStore::in_memory().unwrap()));
        let old_root = cid(b"old");
        let new_root = cid(b"new");
        store.put(&old_root, &record("doc-a")).await.unwrap();

        store
            .replace_scope_head(Some(&old_root), &new_root, &record("doc-a"))
            .await
            .unwrap();

        let entries = store.load_all().await.unwrap();
        assert_eq!(entries, vec![(new_root, record("doc-a"))]);
    }

    #[tokio::test]
    async fn put_load_remove_roundtrip() {
        let store = PendingDagStore::new(Arc::new(RegolithStore::in_memory().unwrap()));
        let root_a = cid(b"a");
        let root_b = cid(b"b");

        store.put(&root_a, &record("doc-a")).await.unwrap();
        store.put(&root_b, &record("doc-b")).await.unwrap();

        let mut loaded = store.load_all().await.unwrap();
        loaded.sort_by(|(_, x), (_, y)| x.doc_id.cmp(&y.doc_id));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].1, record("doc-a"));
        assert_eq!(loaded[0].0, root_a);

        store.remove(&root_a).await.unwrap();
        let remaining = store.load_all().await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, root_b);

        // Removing an absent record is a no-op, not an error.
        store.remove(&root_a).await.unwrap();
    }

    #[tokio::test]
    async fn put_overwrites_existing_record() {
        let store = PendingDagStore::new(Arc::new(RegolithStore::in_memory().unwrap()));
        let root = cid(b"a");
        store.put(&root, &record("doc-old")).await.unwrap();
        store.put(&root, &record("doc-new")).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1.doc_id, "doc-new");
    }

    fn quarantined(doc: &str, reason: &str) -> PersistedQuarantinedDag {
        PersistedQuarantinedDag {
            record: record(doc),
            reason: reason.to_string(),
            quarantined_at_unix_secs: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn quarantine_load_remove_roundtrip() {
        let store = PendingDagStore::new(Arc::new(RegolithStore::in_memory().unwrap()));
        let root = cid(b"a");

        assert!(!store.is_quarantined(&root).await.unwrap());

        let entry = quarantined("doc-a", "unique constraint violation");
        store.quarantine(&root, &entry).await.unwrap();

        assert!(store.is_quarantined(&root).await.unwrap());

        let loaded = store.load_quarantined().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, root);
        assert_eq!(loaded[0].1, entry);

        store.remove_quarantined(&root).await.unwrap();
        assert!(!store.is_quarantined(&root).await.unwrap());
        assert!(store.load_quarantined().await.unwrap().is_empty());

        // Removing an absent record is a no-op, not an error.
        store.remove_quarantined(&root).await.unwrap();
    }

    /// The quarantine keyspace is deliberately outside `/p2p/pending_dag/`:
    /// the live resync sweep's prefix scan (`load_all`) must never observe a
    /// quarantined root, or it would re-drive a merge known to fail every time.
    #[tokio::test]
    async fn quarantined_root_is_absent_from_live_load_all() {
        let store = PendingDagStore::new(Arc::new(RegolithStore::in_memory().unwrap()));
        let live_root = cid(b"live");
        let quarantined_root = cid(b"quarantined");

        store.put(&live_root, &record("doc-live")).await.unwrap();
        store
            .quarantine(
                &quarantined_root,
                &quarantined("doc-quarantined", "unique constraint violation"),
            )
            .await
            .unwrap();

        let live = store.load_all().await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, live_root);

        let quarantined_records = store.load_quarantined().await.unwrap();
        assert_eq!(quarantined_records.len(), 1);
        assert_eq!(quarantined_records[0].0, quarantined_root);
    }
}
