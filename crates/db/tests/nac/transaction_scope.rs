use acp::{nac::NodePermission, PersistentZanzibarStore};
use db::{BasicTxn, NacConfig, NacManager, NacManagerApi};
use identity::Did;
use std::sync::Arc;
use storage::RegolithStore;

#[tokio::test]
async fn node_permission_revocation_invalidates_authorized_write() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let manager = NacManager::new(
        Arc::new(PersistentZanzibarStore::from_store(store.clone())),
        NacConfig::new().with_enabled(),
    );
    let owner = Did::new("did:key:owner").unwrap();
    let writer = Did::new("did:key:writer").unwrap();
    manager.initialize(Some(&owner)).await.unwrap();
    manager
        .add_permission_grant(&owner, &writer, NodePermission::DocumentUpdate)
        .await
        .unwrap();
    let txn = BasicTxn::new(store.as_ref(), 1, false).await.unwrap();
    assert!(manager
        .check_permission_in_txn(txn.acpstore(), &writer, NodePermission::DocumentUpdate)
        .await
        .unwrap());
    manager
        .remove_permission_grant(&owner, &writer, NodePermission::DocumentUpdate)
        .await
        .unwrap();
    txn.datastore()
        .set(b"authorized-document", b"changed")
        .await
        .unwrap();
    assert!(txn.commit().await.is_err());
    let retry = BasicTxn::new(store.as_ref(), 2, false).await.unwrap();
    assert!(!manager
        .check_permission_in_txn(retry.acpstore(), &writer, NodePermission::DocumentUpdate)
        .await
        .unwrap());
    retry.discard().unwrap();
}
