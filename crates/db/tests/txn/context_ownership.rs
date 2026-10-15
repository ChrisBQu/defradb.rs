//! A transaction belongs to the identity acting in the context that began it,
//! and only a request whose context names that identity reaches it again.

use db::database::DB;
use db::txn::registry::DbTransactionRegistry;
use defra_core::RequestContext;
use query::error::TransactionError;
use query::txn::{txn_job, TransactionHandle, TransactionRegistry};
use std::sync::Arc;
use storage::RegolithStore;

fn acting(did: Option<&str>) -> RequestContext {
    RequestContext::default().with_identity(did.map(str::to_owned))
}

/// Whether a request in `context` reaches transaction `handle`.
async fn reaches(
    registry: &DbTransactionRegistry<RegolithStore>,
    context: &RequestContext,
    handle: &TransactionHandle,
) -> bool {
    let job = txn_job(|turn| Box::pin(async move { turn.context().is_ok() }));
    match registry.run_as(context, handle, None, job).await {
        Ok(lent) => lent,
        Err(TransactionError::NotFound(_)) => false,
        Err(error) => panic!("unexpected registry error: {error}"),
    }
}

#[tokio::test]
async fn only_the_context_that_began_a_transaction_reaches_it() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let registry = DbTransactionRegistry::new(db);
    let alice = acting(Some("did:key:alice"));
    let handle = registry.begin_as(&alice, false).await.unwrap();

    for stranger in [acting(None), acting(Some("did:key:bob"))] {
        assert!(!reaches(&registry, &stranger, &handle).await);
        assert!(registry
            .get_collections_in_txn_as(&stranger, handle.as_str())
            .await
            .is_err());
        assert!(matches!(
            registry.commit_as(&stranger, &handle).await,
            Err(TransactionError::NotFound(_))
        ));
    }

    // The owner set the old way, on this thread, reaches nothing either.
    defra_core::current_identity::set_current_identity(Some("did:key:alice".to_string()));
    let anonymous = reaches(&registry, &acting(None), &handle).await;
    defra_core::current_identity::set_current_identity(None);
    assert!(!anonymous);

    assert!(reaches(&registry, &alice, &handle).await);
    registry.commit_as(&alice, &handle).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}
