use crate::common::{fixture::make_test_db_with_bus, schema::test_collection};
use db::AutoCommitMutator;
use defra_core::{batch_signing, Block, RequestContext};
use document::{Document, NormalValue};
use query::mutator::DocMutator;

#[tokio::test]
async fn create_many_collects_field_and_composite_cids_for_both_documents() {
    let (db, _) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let session = "create-many-field-and-composite-cids";
    batch_signing::batch_start(session);
    let result = AutoCommitMutator::new(db)
        .create_many(
            &in_session(session),
            "TestDoc",
            vec![
                Document::from_json_str(r#"{"x":1}"#).unwrap(),
                Document::from_json_str(r#"{"x":2}"#).unwrap(),
            ],
        )
        .await;
    let collected = batch_signing::batch_take_cids(session).unwrap();
    let results = result.unwrap();
    let mut expected = Vec::new();
    for result in results {
        expected.push(result.commit_cid.unwrap());
        let block = Block::from_dag_cbor(result.commit_block.as_ref().unwrap()).unwrap();
        expected.extend(block.links.unwrap().into_iter().map(|link| link.link));
    }
    assert_eq!(expected.len(), 4);
    expected.sort();
    let mut collected = collected;
    collected.sort();
    assert_eq!(collected, expected);
}

/// A request whose blocks are collected into `session`.
fn in_session(session: &str) -> RequestContext {
    RequestContext::default().with_batch_session(Some(session.to_string()))
}

async fn collect_failed_write<F, Fut>(session: &str, write: F) -> Vec<cid::Cid>
where
    F: FnOnce(RequestContext) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    batch_signing::batch_start(session);
    let failed = write(in_session(session)).await;
    assert!(failed, "the write must fail and roll back");
    batch_signing::batch_take_cids(session).unwrap()
}

#[tokio::test]
async fn create_many_rollback_collects_no_cids() {
    let (db, _) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let mutator = AutoCommitMutator::new(db);
    let collected = collect_failed_write("create-many-rollback", |context| async move {
        mutator
            .create_many(
                &context,
                "TestDoc",
                vec![
                    Document::from_json_str(r#"{"x":1}"#).unwrap(),
                    Document::from_json_str(r#"{"x":1}"#).unwrap(),
                ],
            )
            .await
            .is_err()
    })
    .await;
    assert_eq!(collected, Vec::new());
}

#[tokio::test]
async fn create_rollback_collects_no_cids() {
    let (db, _) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let mutator = AutoCommitMutator::new(db);
    let doc = || Document::from_json_str(r#"{"x":1}"#).unwrap();
    mutator
        .create(&defra_core::RequestContext::default(), "TestDoc", doc())
        .await
        .unwrap();
    let collected = collect_failed_write("create-rollback", |context| async move {
        mutator.create(&context, "TestDoc", doc()).await.is_err()
    })
    .await;
    assert_eq!(collected, Vec::new());
}

#[tokio::test]
async fn creates_updates_and_deletes_collect_into_their_requests_session() {
    let (db, _) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let mutator = AutoCommitMutator::new(db);
    let session = "every-write-collects";
    let context = in_session(session);

    batch_signing::batch_start(session);
    let created = mutator
        .create(
            &context,
            "TestDoc",
            Document::from_json_str(r#"{"x":1}"#).unwrap(),
        )
        .await
        .unwrap();
    assert!(batch_signing::batch_take_cids(session)
        .unwrap()
        .contains(&created.commit_cid.unwrap()));

    batch_signing::batch_start(session);
    let mut doc = created.document;
    doc.set("x", NormalValue::Int(2));
    let updated = mutator
        .update(
            &context,
            "TestDoc",
            doc,
            ["x".to_string()].into_iter().collect(),
        )
        .await
        .unwrap();
    assert!(batch_signing::batch_take_cids(session)
        .unwrap()
        .contains(&updated.commit_cid.unwrap()));

    batch_signing::batch_start(session);
    let deleted = mutator
        .delete(&context, "TestDoc", &created.doc_id)
        .await
        .unwrap();
    assert!(batch_signing::batch_take_cids(session)
        .unwrap()
        .contains(&deleted.commit_cid.unwrap()));

    batch_signing::batch_start(session);
    mutator
        .create(
            &RequestContext::default(),
            "TestDoc",
            Document::from_json_str(r#"{"x":3}"#).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        batch_signing::batch_take_cids(session).unwrap(),
        Vec::new(),
        "a request outside the session collected into it"
    );
}
