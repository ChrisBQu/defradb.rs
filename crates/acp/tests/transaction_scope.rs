use acp::{
    DocumentACP, DocumentPermission, Identity, PersistentZanzibarStore, ZanzibarDocumentACP,
};
use datastore::BasicTxn;
use identity::Did;
use std::sync::Arc;
use storage::{RegolithStore, RegolithStoreOptions};
use zanzibar::store::ZanzibarStore;
use zanzibar::types::Relationship;

fn did(name: &str) -> Did {
    Did::new(format!("did:key:{name}")).unwrap()
}

#[tokio::test]
async fn registrations_share_the_document_commit_and_have_one_winner() {
    for encrypted in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = RegolithStoreOptions::default();
        let opts = if encrypted {
            opts.with_encryption_key([42; 32])
        } else {
            opts
        };
        let store = Arc::new(RegolithStore::open_with_options(dir.path(), opts).unwrap());
        let persistent = Arc::new(PersistentZanzibarStore::from_store(store.clone()));
        persistent.store_policy(&ZanzibarDocumentACP::<PersistentZanzibarStore<RegolithStore>>::create_default_policy("p", "items")).await.unwrap();
        let acp = ZanzibarDocumentACP::new(persistent);
        let first = BasicTxn::new(store.as_ref(), 1, false).await.unwrap();
        let second = BasicTxn::new(store.as_ref(), 2, false).await.unwrap();
        let first_acp = acp.bind_transaction(first.acpstore()).unwrap();
        let second_acp = acp.bind_transaction(second.acpstore()).unwrap();
        first_acp
            .register_doc_object(&did("alice"), "p", "items", "doc")
            .await
            .unwrap();
        second_acp
            .register_doc_object(&did("bob"), "p", "items", "doc")
            .await
            .unwrap();
        first.datastore().set(b"document", b"body").await.unwrap();
        assert!(!acp.is_doc_registered("p", "items", "doc").await.unwrap());
        drop(first_acp);
        first.commit().await.unwrap();
        drop(second_acp);
        assert!(second.commit().await.is_err());
        assert_eq!(
            acp.get_doc_owner("p", "items", "doc").await.unwrap(),
            Some(did("alice"))
        );
        let retry = BasicTxn::new(store.as_ref(), 3, false).await.unwrap();
        let retry_acp = acp.bind_transaction(retry.acpstore()).unwrap();
        retry_acp
            .register_doc_object(&did("alice"), "p", "items", "doc")
            .await
            .unwrap();
        assert!(matches!(
            retry_acp
                .register_doc_object(&did("bob"), "p", "items", "doc")
                .await,
            Err(acp::Error::DocumentAlreadyRegistered(_))
        ));
        drop(retry_acp);
        retry.commit().await.unwrap();
    }
}

#[tokio::test]
async fn revocation_invalidates_writer_but_read_only_authorization_keeps_its_snapshot() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let persistent = Arc::new(PersistentZanzibarStore::from_store(store.clone()));
    let acp = ZanzibarDocumentACP::new(persistent.clone());
    acp.register_doc_object(&did("alice"), "p", "items", "doc")
        .await
        .unwrap();
    let grant = Relationship::with_entity("items", "doc", "updater", did("bob"));
    persistent.store_relationship("p", &grant).await.unwrap();
    let write = BasicTxn::new(store.as_ref(), 1, false).await.unwrap();
    let read = BasicTxn::new(store.as_ref(), 2, true).await.unwrap();
    let writer_acp = acp.bind_transaction(write.acpstore()).unwrap();
    let reader_acp = acp.bind_transaction(read.acpstore()).unwrap();
    let bob = Identity::Authenticated(did("bob"));
    assert!(writer_acp
        .check_doc_access(&bob, DocumentPermission::Update, "p", "items", "doc")
        .await
        .unwrap());
    assert!(reader_acp
        .check_doc_access(&bob, DocumentPermission::Update, "p", "items", "doc")
        .await
        .unwrap());
    persistent.delete_relationship("p", &grant).await.unwrap();
    write
        .datastore()
        .set(b"document", b"changed")
        .await
        .unwrap();
    drop(writer_acp);
    assert!(write.commit().await.is_err());
    assert!(reader_acp
        .check_doc_access(&bob, DocumentPermission::Update, "p", "items", "doc")
        .await
        .unwrap());
    drop(reader_acp);
    read.commit().await.unwrap();
}

#[tokio::test]
async fn policy_identity_cannot_be_overwritten() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let persistent = PersistentZanzibarStore::from_store(store.clone());
    let policy =
        ZanzibarDocumentACP::<PersistentZanzibarStore<RegolithStore>>::create_default_policy(
            "p", "items",
        );
    persistent.store_policy(&policy).await.unwrap();
    persistent.store_policy(&policy).await.unwrap();
    let different =
        ZanzibarDocumentACP::<PersistentZanzibarStore<RegolithStore>>::create_default_policy(
            "p", "other",
        );
    assert!(matches!(
        persistent.store_policy(&different).await,
        Err(zanzibar::error::Error::PolicyAlreadyExists(_))
    ));
    let txn = BasicTxn::new(store.as_ref(), 1, false).await.unwrap();
    let scoped = acp::zanzibar::store::TransactionZanzibarStore::new(txn.acpstore());
    assert!(matches!(
        scoped.store_policy(&different).await,
        Err(zanzibar::error::Error::PolicyAlreadyExists(_))
    ));
    drop(scoped);
    txn.discard().unwrap();
}
