//! Each write collects into the batch signing session its request's context
//! names, and a request outside that session collects nothing into it.

use crate::common::{fixture::make_test_db_with_bus, schema::test_collection};
use db::AutoCommitMutator;
use defra_core::{batch_signing, RequestContext};
use document::{Document, NormalValue};
use query::mutator::DocMutator;

#[tokio::test]
async fn creates_updates_and_deletes_collect_into_their_requests_session() {
    let (db, _) = make_test_db_with_bus().await;
    db.create_collection(test_collection()).await.unwrap();
    let mutator = AutoCommitMutator::new(db);
    let session = "every-write-collects";
    let context = RequestContext::default().with_batch_session(Some(session.to_string()));

    batch_signing::batch_start(session);
    let created = mutator
        .create_as(
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
        .update_as(
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
        .delete_as(&context, "TestDoc", &created.doc_id)
        .await
        .unwrap();
    assert!(batch_signing::batch_take_cids(session)
        .unwrap()
        .contains(&deleted.commit_cid.unwrap()));

    batch_signing::batch_start(session);
    mutator
        .create_as(
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
