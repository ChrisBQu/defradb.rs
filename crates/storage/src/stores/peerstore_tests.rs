use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::*;

struct EngineConflictAttempt {
    store: Arc<RegolithStore>,
    attempts: Arc<AtomicUsize>,
}

impl crate::corekv::RetryAttemptFn for EngineConflictAttempt {
    fn run<'a>(
        &'a mut self,
        txn: &'a mut dyn crate::corekv::RetryTxn,
    ) -> crate::corekv::RetryAttemptFuture<'a> {
        let store = self.store.clone();
        let attempts = self.attempts.clone();
        Box::pin(async move {
            let current = txn.get(b"engine-retry").await?;
            let first = attempts.fetch_add(1, AtomicOrdering::Relaxed) == 0;
            if first {
                assert!(txn.previous_conflict().is_none());
                txn.set(b"engine-retry", b"loser").await?;
                let mut winner = store.new_txn(false).await?;
                winner.set(b"engine-retry", b"winner").await?;
                winner.commit().await?;
            } else {
                let conflict = txn
                    .previous_conflict()
                    .expect("retry retains conflict facts");
                assert!(conflict.latest_seq > conflict.observed_seq);
                assert_eq!(conflict.winning_write, regolith::WriteKind::Put);
                assert_eq!(current.as_deref(), Some(b"winner".as_slice()));
                txn.set(b"engine-retry", b"winner-retried").await?;
            }
            Ok(Box::new(()) as crate::corekv::RetryAttemptValue)
        })
    }
}

struct EngineAbortAttempt(Arc<AtomicUsize>);

impl crate::corekv::RetryAttemptFn for EngineAbortAttempt {
    fn run<'a>(
        &'a mut self,
        _txn: &'a mut dyn crate::corekv::RetryTxn,
    ) -> crate::corekv::RetryAttemptFuture<'a> {
        self.0.fetch_add(1, AtomicOrdering::Relaxed);
        Box::pin(async {
            Err(crate::corekv::RetryAttemptError::Abort(Box::new(
                RetryAbortSentinel("needs-input"),
            )))
        })
    }
}
use crate::backends::RegolithStore;
use crate::corekv::Key;
use crate::keys::peerstore::ReplicatorKey;

#[tokio::test]
async fn engine_retry_attempt_restarts_after_commit_conflict() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let interfering_store = store.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempt_count = attempts.clone();

    let result = store
        .transact_retry(
            crate::corekv::RetryOptions::new(3),
            Box::new(EngineConflictAttempt {
                store: interfering_store,
                attempts: attempt_count,
            }),
        )
        .await
        .unwrap();

    result.downcast::<()>().unwrap();
    assert_eq!(attempts.load(AtomicOrdering::Relaxed), 2);
    let read = store.new_txn(true).await.unwrap();
    assert_eq!(
        read.get(b"engine-retry").await.unwrap().as_deref(),
        Some(b"winner-retried".as_slice())
    );
    read.discard();
}

#[derive(Debug, PartialEq, Eq)]
struct RetryAbortSentinel(&'static str);

#[tokio::test]
async fn engine_retry_attempt_returns_typed_business_abort_unchanged() {
    let store = RegolithStore::in_memory().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempt_count = attempts.clone();
    let result = store
        .transact_retry(
            crate::corekv::RetryOptions::new(3),
            Box::new(EngineAbortAttempt(attempt_count)),
        )
        .await;

    let Err(error) = result else {
        panic!("attempt should abort with its typed business outcome");
    };
    assert_eq!(
        error.downcast_abort::<RetryAbortSentinel>().unwrap(),
        RetryAbortSentinel("needs-input")
    );
    assert_eq!(attempts.load(AtomicOrdering::Relaxed), 1);
}

#[tokio::test]
async fn scoped_retry_io_restores_the_real_transaction_before_engine_commit() {
    let store = RegolithStore::in_memory().unwrap();
    let result = store
        .transact_retry(
            crate::corekv::RetryOptions::new(2),
            Box::new(ScopedRetryIoAttempt {
                retain_iterator: false,
                abort_after_write: false,
            }),
        )
        .await
        .unwrap();
    result.downcast::<()>().unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"scoped-retry").await.unwrap().as_deref(),
        Some(b"persisted".as_slice())
    );
    txn.discard();
}

#[tokio::test]
async fn scoped_retry_io_rejects_an_iterator_retained_past_attempt() {
    let store = RegolithStore::in_memory().unwrap();
    let result = store
        .transact_retry(
            crate::corekv::RetryOptions::new(2),
            Box::new(ScopedRetryIoAttempt {
                retain_iterator: true,
                abort_after_write: false,
            }),
        )
        .await;
    assert!(result.is_err());

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"must-rollback").await.unwrap(), None);
    txn.discard();
}

#[tokio::test]
async fn scoped_retry_io_business_abort_rolls_back_actual_transaction() {
    let store = RegolithStore::in_memory().unwrap();
    let result = store
        .transact_retry(
            crate::corekv::RetryOptions::new(2),
            Box::new(ScopedRetryIoAttempt {
                retain_iterator: false,
                abort_after_write: true,
            }),
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::corekv::RetryAttemptError::Abort(_))
    ));

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"scoped-retry").await.unwrap(), None);
    txn.discard();
}

struct ScopedRetryIoAttempt {
    retain_iterator: bool,
    abort_after_write: bool,
}

impl crate::corekv::RetryAttemptFn for ScopedRetryIoAttempt {
    fn run<'a>(
        &'a mut self,
        retry: &'a mut dyn crate::corekv::RetryTxn,
    ) -> crate::corekv::RetryAttemptFuture<'a> {
        Box::pin(async move {
            let mut io = retry.begin_attempt_io()?;
            let key = if self.retain_iterator {
                b"must-rollback".as_slice()
            } else {
                b"scoped-retry".as_slice()
            };
            let value = if self.retain_iterator {
                b"value".as_slice()
            } else {
                b"persisted".as_slice()
            };
            io.set(key, value).await?;
            if self.abort_after_write {
                return Err(crate::corekv::RetryAttemptError::Abort(Box::new(
                    RetryAbortSentinel("scoped-abort"),
                )));
            }
            if !self.retain_iterator {
                assert_eq!(io.get(key).await?.as_deref(), Some(value));
            }
            let _retained = if self.retain_iterator {
                Some(io.iterator(crate::corekv::IterOptions::default()).await?)
            } else {
                None
            };
            retry.finish_attempt_io(io, crate::corekv::RetryCallbacks::default())?;
            Ok(Box::new(()) as crate::corekv::RetryAttemptValue)
        })
    }
}

#[tokio::test]
async fn transient_marker_io_failure_recovers_durable_scopes() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store.clone());
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    for (doc_id, collection_id, error) in [
        (
            "doc",
            "collection",
            crate::corekv::Error::Io("injected transient marker failure".to_string()),
        ),
        (
            "",
            "collection",
            crate::corekv::Error::Backend("injected transient marker failure".to_string()),
        ),
    ] {
        let attempts = AtomicUsize::new(0);
        retry_scope_marker_write(|| async {
            if attempts.fetch_add(1, AtomicOrdering::Relaxed) == 0 {
                Err(error.clone())
            } else {
                peerstore
                    .register_scope_marker_once("peer", doc_id, collection_id, &retry_info)
                    .await
            }
        })
        .await
        .unwrap();
        assert_eq!(attempts.load(AtomicOrdering::Relaxed), 2);
    }

    let restarted = Peerstore::new(store);
    assert!(restarted.get_retry_info("peer").await.unwrap().is_some());
    let markers = restarted.get_retry_documents("peer").await.unwrap();
    assert_eq!(markers.len(), 2);
    assert!(markers.iter().any(|marker| marker.doc_id == "doc"));
    assert!(markers.iter().any(|marker| marker.is_collection_commit()));
}

#[tokio::test]
async fn marker_io_failures_stop_at_bound() {
    let attempts = AtomicUsize::new(0);

    let error = retry_scope_marker_write(|| async {
        attempts.fetch_add(1, AtomicOrdering::Relaxed);
        Err::<(), _>(crate::corekv::Error::Backend(
            "injected persistent marker failure".to_string(),
        ))
    })
    .await
    .unwrap_err();

    assert!(matches!(error, crate::corekv::Error::Backend(_)));
    assert_eq!(
        attempts.load(AtomicOrdering::Relaxed),
        PUSH_RETRY_TXN_MAX_ATTEMPTS
    );
}

#[tokio::test]
async fn test_peerstore_basic() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    let key = ReplicatorKey::new("replicator_1");

    // Write
    let mut txn = peerstore.new_txn(false).await.unwrap();
    txn.set(&key.bytes(), b"replicator_config").await.unwrap();
    txn.commit().await.unwrap();

    // Read
    let txn = peerstore.new_txn(true).await.unwrap();
    let value = txn.get(&key.bytes()).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"replicator_config")));
}

#[tokio::test]
async fn test_set_get_replicator() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";
    let data = b"replicator_config_data";

    // Set
    peerstore.create_replicator(peer_id, data).await.unwrap();

    // Get
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(Bytes::from(data.to_vec())));

    // Has
    assert!(peerstore.has_replicator(peer_id).await.unwrap());
    assert!(!peerstore.has_replicator("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_delete_replicator() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";
    let data = b"replicator_config_data";

    // Set
    peerstore.create_replicator(peer_id, data).await.unwrap();
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();
    peerstore
        .record_push_failure(peer_id, "doc", "collection", &retry_info)
        .await
        .unwrap();
    peerstore
        .record_push_failure(peer_id, "", "collection", &retry_info)
        .await
        .unwrap();
    assert!(peerstore.has_replicator(peer_id).await.unwrap());

    // Delete
    peerstore.delete_replicator(peer_id).await.unwrap();
    assert!(!peerstore.has_replicator(peer_id).await.unwrap());

    // Get returns None
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, None);
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents(peer_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn forget_and_readd_preserves_marker_generation_fencing() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store.clone());
    let peer_id = "coordinated-peer";
    peerstore
        .create_replicator(peer_id, b"replicator")
        .await
        .unwrap();

    let old_generation = peerstore
        .observe_push_head_with_generation(peer_id, "doc", "collection")
        .await
        .unwrap()
        .unwrap();
    peerstore.delete_replicator(peer_id).await.unwrap();
    peerstore
        .create_replicator(peer_id, b"new replicator incarnation")
        .await
        .unwrap();
    let new_generation = peerstore
        .observe_push_head_with_generation(peer_id, "doc", "collection")
        .await
        .unwrap()
        .unwrap();
    assert!(new_generation > old_generation);
    assert!(!peerstore
        .complete_retry_scope(peer_id, "doc", "collection", false, old_generation)
        .await
        .unwrap());
    let markers = peerstore.get_retry_documents(peer_id).await.unwrap();
    assert_eq!(markers[0].generation, new_generation);
}

#[tokio::test]
async fn concurrent_scope_registrations_get_distinct_generations() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store.clone());
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        peerstore.observe_push_head_with_generation("peer", "doc", "collection"),
        peerstore.observe_push_head_with_generation("peer", "doc", "collection"),
    );
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_ne!(first, second);
    let current = peerstore.get_retry_documents("peer").await.unwrap()[0].generation;
    assert!(current == first || current == second);
    let stale = if current == first { second } else { first };
    assert!(!peerstore
        .complete_retry_scope("peer", "doc", "collection", false, stale)
        .await
        .unwrap());
}

#[tokio::test]
async fn stale_ack_cannot_erase_a_concurrent_dirty_registration() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store.clone());
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let old_generation = peerstore
        .observe_push_head_with_generation("peer", "doc", "collection")
        .await
        .unwrap()
        .unwrap();

    let ack_store = Peerstore::new(store.clone());
    let update_store = Peerstore::new(store);
    let (ack, update) = tokio::join!(
        ack_store.complete_retry_scope("peer", "doc", "collection", false, old_generation),
        update_store.observe_push_head_with_generation("peer", "doc", "collection"),
    );
    let _ = ack.unwrap();
    let update_generation = update.unwrap().unwrap();
    let markers = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(markers.len(), 1);
    assert_eq!(markers[0].generation, update_generation);
    assert_ne!(update_generation, old_generation);
}

#[tokio::test]
async fn delete_replicator_clears_orphaned_retry_state() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let system_store = crate::namespace::NamespacedStore::new(
        store.clone(),
        crate::namespace::Namespace::Systemstore,
    );
    let peerstore = Peerstore::new(store);
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .create_replicator("orphan", b"replicator")
        .await
        .unwrap();

    peerstore
        .record_push_failure("orphan", "doc", "collection", &retry_info)
        .await
        .unwrap();
    let unrelated_key = b"/rep/retry/doc/orphan/unrelated";
    let mut unrelated = system_store.new_txn(false).await.unwrap();
    unrelated.set(unrelated_key, b"keep").await.unwrap();
    unrelated.commit().await.unwrap();
    peerstore.delete_replicator("orphan").await.unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents("orphan")
        .await
        .unwrap()
        .is_empty());
    let unrelated = system_store.new_txn(true).await.unwrap();
    assert_eq!(unrelated.get(unrelated_key).await.unwrap().unwrap(), "keep");
    unrelated.discard();
}

#[tokio::test]
async fn retry_sweep_peers_require_persisted_replicators() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    for peer_id in ["active", "orphan"] {
        peerstore
            .create_replicator(peer_id, b"replicator")
            .await
            .unwrap();
        peerstore
            .record_push_failure(peer_id, "doc", "collection", &retry_info)
            .await
            .unwrap();
    }
    peerstore.delete_replicator("orphan").await.unwrap();

    let peers = peerstore.get_replicator_retry_peers().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].0, "active");
}

#[tokio::test]
async fn test_list_replicators() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    // Add multiple replicators
    peerstore
        .create_replicator("peer1", b"config1")
        .await
        .unwrap();
    peerstore
        .create_replicator("peer2", b"config2")
        .await
        .unwrap();
    peerstore
        .create_replicator("peer3", b"config3")
        .await
        .unwrap();

    // Get all
    let all = peerstore.list_replicators().await.unwrap();
    assert_eq!(all.len(), 3);

    // Check they're all present (order may vary)
    let peer_ids: Vec<&str> = all.iter().map(|(id, _)| id.as_str()).collect();
    assert!(peer_ids.contains(&"peer1"));
    assert!(peer_ids.contains(&"peer2"));
    assert!(peer_ids.contains(&"peer3"));
}

#[tokio::test]
async fn test_list_replicators_empty() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    let all = peerstore.list_replicators().await.unwrap();
    assert!(all.is_empty());
}

#[tokio::test]
async fn test_update_replicator() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";

    // Set initial
    peerstore
        .create_replicator(peer_id, b"config_v1")
        .await
        .unwrap();
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(Bytes::from_static(b"config_v1")));

    // Update
    peerstore
        .create_replicator(peer_id, b"config_v2")
        .await
        .unwrap();
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(Bytes::from_static(b"config_v2")));

    // Still only one replicator
    let all = peerstore.list_replicators().await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn scope_markers_are_generation_fenced_and_share_the_peer_clock() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "", "collection", &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "doc", "collection")
        .await
        .unwrap();

    let txn = peerstore.store.new_txn(true).await.unwrap();
    let document_marker = txn
        .get(&ReplicatorRetryDocIDKey::new("peer", "doc").bytes())
        .await
        .unwrap()
        .unwrap();
    let collection_marker = txn
        .get(&ReplicatorRetryCollectionKey::new("peer", "collection").bytes())
        .await
        .unwrap()
        .unwrap();
    assert!(decode_retry_marker_generation(&document_marker).is_some());
    assert!(decode_retry_marker_generation(&collection_marker).is_some());
    assert_ne!(document_marker, collection_marker);
    drop(txn);

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 2);
    assert_eq!(
        retries[0].retry_info.next_retry_unix,
        retries[1].retry_info.next_retry_unix
    );
}

#[tokio::test]
async fn completing_one_scope_preserves_other_scope_and_peer_clock() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "", "collection", &initial)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    let doc = retries
        .iter()
        .find(|retry| !retry.is_collection_commit())
        .unwrap();
    peerstore
        .complete_retry_document("peer", doc)
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();

    let remaining = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].is_collection_commit());
    assert_eq!(peerstore.get_all_retry_peers().await.unwrap().len(), 1);

    peerstore
        .complete_retry_document("peer", &remaining[0])
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
}

#[tokio::test]
async fn collection_updates_coalesce_to_one_rederivable_scope() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", &initial)
        .await
        .unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", &initial)
        .await
        .unwrap();
    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert!(retries[0].is_collection_commit());
    assert_eq!(retries[0].collection_id, "collection");
}

#[tokio::test]
async fn retry_marker_stats_report_scope_counts_and_oldest_peer_clock() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    for peer_id in ["peer-a", "peer-b"] {
        peerstore
            .create_replicator(peer_id, b"replicator")
            .await
            .unwrap();
    }
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer-a", "doc-a", "collection", &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer-a", "", "collection", &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer-b", "doc-b", "collection", &initial)
        .await
        .unwrap();

    let stats = peerstore.push_retry_marker_stats().await.unwrap();
    assert_eq!(stats.document_markers, 2);
    assert_eq!(stats.collection_markers, 1);
    assert_eq!(stats.scheduled_peers, 2);
    assert!(stats.oldest_scheduled_retry_unix.is_some());
}

/// An empty scope has nothing to replay and must not create state.
#[tokio::test]
async fn versionless_empty_document_failure_creates_no_retry_state() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "", &initial)
        .await
        .unwrap();
    peerstore.observe_push_head("peer", "", "").await.unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn peer_retry_cursor_rotates_bounded_marker_prefix_after_failure() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();
    for doc_id in ["a", "b", "c", "d"] {
        peerstore
            .record_push_failure("peer", doc_id, "collection", &initial)
            .await
            .unwrap();
    }

    let first = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(first[0].doc_id, "a");
    assert!(peerstore
        .reschedule_retry_peer("peer", None, 1)
        .await
        .unwrap());

    let second = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(second[0].doc_id, "b");
    let schedule = super::super::RetryInfo::from_bytes(
        &peerstore.get_retry_info("peer").await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(schedule.dispatch_cursor, 1);
}

#[tokio::test]
async fn capacity_reschedule_rotates_without_advancing_failure_ladder() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();
    peerstore
        .record_push_failure("peer", "doc", "collection", &initial)
        .await
        .unwrap();
    let before = super::super::RetryInfo::from_bytes(
        &peerstore.get_retry_info("peer").await.unwrap().unwrap(),
    )
    .unwrap();

    let now = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(peerstore
        .reschedule_retry_peer("peer", Some(std::time::Duration::from_secs(2)), 1)
        .await
        .unwrap());

    let after = super::super::RetryInfo::from_bytes(
        &peerstore.get_retry_info("peer").await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(after.num_retries, before.num_retries);
    assert_eq!(after.dispatch_cursor, before.dispatch_cursor + 1);
    assert!(after.next_retry_unix >= now + 2);
}

#[tokio::test]
async fn sweep_clear_removes_preexisting_empty_document_retry() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    txn.set(
        &ReplicatorRetryIDKey::new("peer").bytes(),
        &super::super::RetryInfo::new_initial().to_bytes().unwrap(),
    )
    .await
    .unwrap();
    txn.set(
        &ReplicatorRetryDocIDKey::new("peer", "").bytes(),
        b"legacy-payload",
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    peerstore.clear_retry_peer("peer").await.unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    let txn = peerstore.store.new_txn(true).await.unwrap();
    assert!(txn
        .get(&ReplicatorRetryDocIDKey::new("peer", "").bytes())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sweep_clear_cannot_orphan_a_concurrently_registered_marker() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let clear_store = Peerstore::new(Arc::clone(&store));
    let register_store = Peerstore::new(Arc::clone(&store));
    let peerstore = Peerstore::new(store);
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    txn.set(&ReplicatorKey::new("peer").bytes(), b"replicator")
        .await
        .unwrap();
    txn.set(
        &ReplicatorRetryIDKey::new("peer").bytes(),
        &super::super::RetryInfo::new_initial().to_bytes().unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let (clear_result, register_result) = tokio::join!(
        clear_store.clear_retry_peer("peer"),
        register_store.observe_push_head("peer", "doc", "collection")
    );
    clear_result.unwrap();
    register_result.unwrap();

    assert_eq!(
        peerstore.get_retry_documents("peer").await.unwrap().len(),
        1
    );
    assert!(peerstore.get_retry_info("peer").await.unwrap().is_some());
}

#[tokio::test]
async fn peer_reconnect_activates_schedule_without_resetting_ladder() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let peerstore = Peerstore::new(store);
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();
    let mut retry = super::super::RetryInfo::new_initial();
    retry.bump();
    retry.bump();
    let original_rung = retry.num_retries;
    peerstore
        .record_push_failure("peer", "doc", "collection", &retry.to_bytes().unwrap())
        .await
        .unwrap();

    assert!(peerstore.activate_retry_peer("peer").await.unwrap());
    let bytes = peerstore.get_retry_info("peer").await.unwrap().unwrap();
    let activated = super::super::RetryInfo::from_bytes(&bytes).unwrap();
    assert_eq!(activated.num_retries, original_rung + 1);
    assert!(activated.is_due());
    assert!(!peerstore.activate_retry_peer("absent").await.unwrap());
}
