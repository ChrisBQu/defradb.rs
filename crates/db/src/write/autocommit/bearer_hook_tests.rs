use super::*;
use acp::{DocumentPermission, Identity};
use identity::Did;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use std::sync::Mutex;

const DID: &str = "did:key:autocommit-hook-test";
const TOKEN: &str = "bearer-captured-when-staged";

#[derive(Default)]
struct TokenObserver(Mutex<Vec<Option<String>>>);

#[async_trait]
impl acp::DocumentACP for TokenObserver {
    async fn register_doc_object(
        &self,
        identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        assert_eq!(identity.as_str(), DID);
        self.0
            .lock()
            .unwrap()
            .push(defra_core::signing::get_request_bearer_token(DID));
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        self.0
            .lock()
            .unwrap()
            .push(defra_core::signing::get_request_bearer_token(DID));
        Ok(false)
    }

    async fn check_doc_access(
        &self,
        _identity: &Identity,
        _permission: DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(defra_core::signing::get_request_bearer_token(DID));
        Ok(())
    }
}

#[tokio::test]
async fn deferred_registration_hook_uses_bearer_captured_before_commit() {
    let db = Arc::new(DB::new(storage::RegolithStore::in_memory().unwrap()).unwrap());
    let mut schema = CollectionVersion::new(
        "BearerHookDoc",
        "v1",
        "bearer-hook-doc",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.policy = Some(PolicyDescription::new(
        "bearer-hook-policy",
        "BearerHookDoc",
    ));
    db.create_collection(schema).await.unwrap();
    let collection = db.get_collection("BearerHookDoc").unwrap().unwrap();
    let observer = Arc::new(TokenObserver::default());
    db.set_document_acp(Arc::new(acp::ExternalDocumentAcp::new(
        observer.clone(),
        db.store().clone(),
    )));

    let mut txn = db.new_txn(false).await.unwrap();
    acp::PendingRegistrations::new(&txn.acpstore().unwrap())
        .record(&acp::PendingRegistration::new(
            Did::new(DID).unwrap(),
            "bearer-hook-policy",
            "BearerHookDoc",
            "doc-1",
        ))
        .await
        .unwrap();
    let context = defra_core::RequestContext::default().with_creator(Some(DID.to_string()));
    defra_core::signing::scope_request_bearer_token(
        Some(DID.to_string()),
        Some(TOKEN.to_string()),
        async {
            AutoCommitMutator::new(db.clone())
                .register_created_doc_with_acp(&context, &mut txn, &collection, "doc-1".into())
                .unwrap();
        },
    )
    .await;

    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
    txn.commit().await.unwrap();
    assert_eq!(
        *observer.0.lock().unwrap(),
        vec![Some(TOKEN.to_string()), Some(TOKEN.to_string())]
    );
    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
}

#[tokio::test]
async fn create_retry_callback_uses_bearer_captured_before_commit() {
    let observer = Arc::new(TokenObserver::default());
    let creator = Did::new(DID).unwrap();
    let store = Arc::new(storage::RegolithStore::in_memory().unwrap());
    let owed = datastore::BasicTxn::new(store.as_ref(), 0, false)
        .await
        .unwrap();
    acp::PendingRegistrations::new(&owed.acpstore())
        .record(&acp::PendingRegistration::new(
            creator.clone(),
            "policy",
            "resource",
            "doc-2",
        ))
        .await
        .unwrap();
    owed.commit().await.unwrap();
    let callback = super::create::acp_registration_retry_callback(
        Arc::new(acp::ExternalDocumentAcp::new(observer.clone(), store)),
        creator,
        "policy".into(),
        "resource".into(),
        "doc-2".into(),
        Some(TOKEN.to_string()),
    );

    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
    callback().await.unwrap();
    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
    let observed = observer.0.lock().unwrap().clone();
    assert_eq!(
        observed,
        vec![Some(TOKEN.to_string()), Some(TOKEN.to_string())]
    );
}

#[tokio::test]
async fn deferred_unregistration_hook_uses_bearer_captured_before_commit() {
    let db = Arc::new(DB::new(storage::RegolithStore::in_memory().unwrap()).unwrap());
    let mut schema = CollectionVersion::new(
        "BearerHookDeleteDoc",
        "v1",
        "bearer-hook-delete-doc",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.policy = Some(PolicyDescription::new(
        "bearer-hook-delete-policy",
        "BearerHookDeleteDoc",
    ));
    db.create_collection(schema).await.unwrap();
    let collection = db.get_collection("BearerHookDeleteDoc").unwrap().unwrap();
    let observer = Arc::new(TokenObserver::default());
    db.set_document_acp(observer.clone());

    let mut txn = db.new_txn(false).await.unwrap();
    let context = defra_core::RequestContext::default().with_creator(Some(DID.to_string()));
    defra_core::signing::scope_request_bearer_token(
        Some(DID.to_string()),
        Some(TOKEN.to_string()),
        async {
            AutoCommitMutator::new(db.clone())
                .register_deleted_doc_with_acp(&context, &mut txn, &collection, "doc-1".into())
                .unwrap();
        },
    )
    .await;

    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
    txn.commit().await.unwrap();
    assert_eq!(*observer.0.lock().unwrap(), vec![Some(TOKEN.to_string())]);
    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
}
