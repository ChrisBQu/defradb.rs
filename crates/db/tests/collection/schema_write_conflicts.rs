use std::sync::Arc;

use db::{DbDocMutator, DB};
use document::Document;
use query::mutator::DocMutator;
use schema::{
    CollectionVersion, FieldDescription, FieldKind, IndexKind, IndexedFieldDescription,
    OrderedIndexDescription,
};
use storage::RegolithStore;

fn schema(name: &str, version: &str, id: &str) -> CollectionVersion {
    CollectionVersion::new(
        name,
        version,
        id,
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "value", FieldKind::string()),
        ],
    )
}

async fn stage_create(db: &Arc<DB<RegolithStore>>, collection: &str) -> db::DbTxn<RegolithStore> {
    let mutator = DbDocMutator::new(db.clone(), db.new_txn(false).await.unwrap());
    let mut doc = Document::new();
    doc.set("value", "staged");
    mutator.create(collection, doc).await.unwrap();
    mutator.take_txn().await.unwrap()
}

async fn open() -> Arc<DB<RegolithStore>> {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    Arc::new(DB::from_arc(store).unwrap())
}

#[tokio::test]
async fn document_write_conflicts_with_collection_drop() {
    let db = open().await;
    db.create_collection(schema("race", "race-v1", "race-id"))
        .await
        .unwrap();
    let writer = stage_create(&db, "race").await;

    db.delete_collection("race").await.unwrap();

    assert!(matches!(
        writer.commit().await,
        Err(db::Error::Datastore(datastore::Error::Storage(
            storage::Error::TxnConflict
        )))
    ));
}

#[tokio::test]
async fn document_write_conflicts_with_schema_patch() {
    let db = open().await;
    db.create_collection(schema("race", "race-v1", "race-id"))
        .await
        .unwrap();
    let writer = stage_create(&db, "race").await;

    db.patch_collection(
        "race",
        r#"[{"op":"add","path":"/race/Fields/-","value":{"Name":"extra","Kind":"String"}}]"#,
        None,
    )
    .await
    .unwrap();

    assert!(matches!(
        writer.commit().await,
        Err(db::Error::Datastore(datastore::Error::Storage(
            storage::Error::TxnConflict
        )))
    ));
}

#[tokio::test]
async fn document_write_conflicts_with_index_definition() {
    let db = open().await;
    db.create_collection(schema("race", "race-v1", "race-id"))
        .await
        .unwrap();
    let writer = stage_create(&db, "race").await;

    db.create_index(
        "race",
        Some("by_value"),
        vec![IndexedFieldDescription {
            name: "value".into(),
            descending: false,
        }],
        IndexKind::Ordered(OrderedIndexDescription { unique: false }),
    )
    .await
    .unwrap();

    assert!(matches!(
        writer.commit().await,
        Err(db::Error::Datastore(datastore::Error::Storage(
            storage::Error::TxnConflict
        )))
    ));
}

#[tokio::test]
async fn disjoint_collection_writes_both_commit() {
    let db = open().await;
    db.create_collection(schema("left", "left-v1", "left-id"))
        .await
        .unwrap();
    db.create_collection(schema("right", "right-v1", "right-id"))
        .await
        .unwrap();
    let left = stage_create(&db, "left").await;
    let right = stage_create(&db, "right").await;

    left.commit().await.unwrap();
    right.commit().await.unwrap();
}
