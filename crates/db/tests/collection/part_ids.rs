//! An update reads what it needs once: its collection version's part-ID map is
//! decoded once and shared, and the document's heads are scanned once for both
//! its value operands and its blocks.

use std::sync::Arc;

use db::{AutoCommitMutator, DbDocMutator, DB};
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use rapidhash::RapidHashSet;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::keys::HeadstoreDocKey;
use storage::namespace::Namespace;
use storage::RegolithStore;

use crate::common::counting_store::CountingStore;

type CountingDb = Arc<DB<CountingStore<RegolithStore>>>;

fn collection() -> CollectionVersion {
    CollectionVersion::new(
        "Parted",
        "parted-v1",
        "col-parted",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "left", FieldKind::int()),
            FieldDescription::new("3", "right", FieldKind::int()),
        ],
    )
}

async fn seeded() -> (CountingDb, DocID) {
    let db = Arc::new(DB::new(CountingStore::new(RegolithStore::in_memory().unwrap())).unwrap());
    db.create_collection(&defra_core::RequestContext::default(), collection())
        .await
        .unwrap();
    let created = AutoCommitMutator::new(Arc::clone(&db))
        .create(
            &defra_core::RequestContext::default(),
            "Parted",
            Document::from_json_str(r#"{"left": 0, "right": 0}"#).unwrap(),
        )
        .await
        .unwrap();
    (db, created.doc_id)
}

async fn update_in_explicit_txn(db: &CountingDb, doc_id: &DocID, field: &str, value: i64) {
    let mutator = DbDocMutator::new(Arc::clone(db), db.new_txn(false).await.unwrap());
    let mut doc = mutator
        .get_for_update("Parted", doc_id)
        .await
        .unwrap()
        .unwrap();
    doc.set(field, NormalValue::Int(value));
    mutator
        .update(
            &defra_core::RequestContext::default(),
            "Parted",
            doc,
            RapidHashSet::from_iter([field.to_string()]),
        )
        .await
        .unwrap();
    mutator.take_txn().await.unwrap().commit().await.unwrap();
}

#[tokio::test]
async fn an_update_reads_its_part_id_map_at_most_once() {
    let (db, doc_id) = seeded().await;
    let version = db
        .get_collection("Parted")
        .unwrap()
        .unwrap()
        .version_id()
        .to_string();
    let mut part_key = vec![Namespace::Systemstore.prefix()];
    part_key.extend_from_slice(format!("/parts/{version}").as_bytes());

    for (field, value) in [("left", 1), ("right", 2)] {
        db.store().take_point_keys();
        update_in_explicit_txn(&db, &doc_id, field, value).await;
        let reads = db
            .store()
            .take_point_keys()
            .iter()
            .filter(|key| **key == part_key)
            .count();
        assert!(
            reads <= 1,
            "an update of {field} read the part-ID map {reads} times"
        );
    }
}

#[tokio::test]
async fn an_update_scans_the_document_heads_once() {
    let (db, doc_id) = seeded().await;
    let collection = db.get_collection("Parted").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let short_id = collection
        .resolve_doc_short_id(&txn.systemstore().unwrap(), &doc_id)
        .await
        .unwrap()
        .unwrap();
    txn.discard().unwrap();
    let mut heads_prefix = vec![Namespace::Headstore.prefix()];
    heads_prefix.extend_from_slice(&HeadstoreDocKey::document_prefix(short_id));

    db.store().take_scan_prefixes();
    update_in_explicit_txn(&db, &doc_id, "left", 1).await;
    let scans = db
        .store()
        .take_scan_prefixes()
        .iter()
        .filter(|prefix| **prefix == heads_prefix)
        .count();
    assert_eq!(
        scans, 1,
        "an update scanned its document's heads {scans} times"
    );
}
