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
    db.set_document_acp(observer.clone());

    let mut txn = db.new_txn(false).await.unwrap();
    defra_core::signing::scope_broadcast_creator_did(Some(DID.to_string()), async {
        defra_core::signing::scope_request_bearer_token(
            Some(DID.to_string()),
            Some(TOKEN.to_string()),
            async {
                AutoCommitMutator::new(db.clone())
                    .register_created_doc_with_acp(&mut txn, &collection, "doc-1".into())
                    .unwrap();
            },
        )
        .await;
    })
    .await;

    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
    txn.commit().await.unwrap();
    assert_eq!(
        *observer.0.lock().unwrap(),
        vec![Some(TOKEN.to_string()), Some(TOKEN.to_string())]
    );
    assert_eq!(defra_core::signing::get_request_bearer_token(DID), None);
}
