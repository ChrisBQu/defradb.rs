use db::database::DB;
use db::txn::registry::DbTransactionRegistry;
use defra_core::RequestContext;
use query::error::TransactionError;
use query::txn::{GetTransactionResult, TransactionRegistry};
use std::sync::Arc;
use std::time::Duration;
use storage::RegolithStore;

/// A request `identity` acts in.
fn acting(identity: Option<String>) -> RequestContext {
    RequestContext::default().with_identity(identity)
}

#[tokio::test]
async fn transaction_ownership_rejects_foreign_access_without_consuming_handle() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let registry = DbTransactionRegistry::new(db);
    for owner in [None, Some("did:key:alice".to_string())] {
        let owning = acting(owner.clone());
        let handle = registry.begin(&owning, false).await.unwrap();
        let ctx = registry.get_ctx(&owning, handle.as_str()).unwrap().unwrap();
        let last_seen = ctx.last_request_seen();
        for caller in [None, Some("did:key:bob".to_string())] {
            if caller == owner {
                continue;
            }
            let foreign = acting(caller);
            assert!(matches!(
                registry.get(&foreign, &handle),
                GetTransactionResult::NotFound
            ));
            assert!(registry
                .get_ctx(&foreign, handle.as_str())
                .unwrap()
                .is_none());
            assert!(matches!(
                registry.commit(&foreign, &handle).await,
                Err(TransactionError::NotFound(_))
            ));
            assert!(matches!(
                registry.rollback(&foreign, &handle).await,
                Err(TransactionError::NotFound(_))
            ));
            assert!(matches!(
                registry.finish_implicit_read(&foreign, &handle, true).await,
                Err(TransactionError::NotFound(_))
            ));
            assert_eq!(ctx.last_request_seen(), last_seen);
            assert!(!ctx.is_consumed().await);
        }
        registry.commit(&owning, &handle).await.unwrap();
        assert!(ctx.is_consumed().await);
    }
    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}

#[tokio::test]
async fn transaction_ownership_preserves_implicit_reads_and_idle_cleanup() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let registry = DbTransactionRegistry::new(db);
    let alice = acting(Some("did:key:alice".to_string()));
    let handle = registry.begin_implicit_read(&alice).await.unwrap();
    assert!(matches!(
        registry.get(&alice, &handle),
        GetTransactionResult::Found(_)
    ));
    registry
        .finish_implicit_read(&alice, &handle, true)
        .await
        .unwrap();
    registry.begin(&alice, false).await.unwrap();
    let result = registry
        .cleanup_stale_transactions(Duration::ZERO)
        .await
        .unwrap();
    assert!(result.is_complete());
    assert_eq!(result.cleaned, 1);
    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}
