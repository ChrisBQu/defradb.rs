use document::stored::{StoredDocument, StoredField, ValueKind};
use document::NormalValue;
use regolith::MergeOperator;
use storage::corekv::Store;
use storage::merge::{document_parts, DefraMergeOperator};
use storage::RegolithStore;

fn field(id: u32, priority: u64, value: NormalValue) -> StoredDocument {
    StoredDocument {
        fields: [(
            id,
            StoredField::lww(format!("field{id}"), priority, &value).unwrap(),
        )]
        .into(),
    }
}

#[test]
fn priorities_ties_and_tombstones_survive_materialization() {
    let base = field(1, 1, NormalValue::Int(10));
    let replacement = field(1, 2, NormalValue::Int(20));
    let tombstone = field(1, 3, NormalValue::Null);
    let stale = field(1, 2, NormalValue::Int(99));
    let operands: Vec<_> = [&replacement, &tombstone, &stale]
        .into_iter()
        .map(|value| document_parts(value).unwrap())
        .collect();
    let bytes = DefraMergeOperator
        .full_merge(
            b"doc",
            Some(&base.encode().unwrap()),
            &operands.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        )
        .unwrap();
    let stored = StoredDocument::decode(&bytes).unwrap();
    assert_eq!(stored.fields[&1].priority, 3);
    assert!(stored.fields[&1].value.is_empty());
    assert!(stored.materialize().unwrap().get("field1").is_none());
    let a = document_parts(&field(1, 4, NormalValue::String("a".into()))).unwrap();
    let b = document_parts(&field(1, 4, NormalValue::String("b".into()))).unwrap();
    assert_eq!(
        DefraMergeOperator.full_merge(b"doc", Some(&bytes), &[&a, &b]),
        DefraMergeOperator.full_merge(b"doc", Some(&bytes), &[&b, &a])
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn document_folds_and_touches_obey_engine_contracts() {
    use regolith::testing::proptest::prelude::*;
    let values = (0u32..4, any::<u64>(), any::<i64>())
        .prop_map(|(id, priority, value)| field(id, priority, NormalValue::Int(value)));
    let operands = values
        .clone()
        .prop_map(|value| document_parts(&value).unwrap());
    let checked = regolith::testing::check_merge_operator(
        &DefraMergeOperator,
        Just(b"doc".to_vec()),
        values.prop_map(|value| value.encode().unwrap()),
        operands.clone(),
    )
    .unwrap();
    assert!(checked.folds > 0);
    let checked = regolith::testing::check_touches(
        &DefraMergeOperator,
        Just(b"doc".to_vec()),
        operands,
        regolith::testing::proptest::collection::vec(0u32..5, 0..6),
    )
    .unwrap();
    assert!(checked.folds > 0);
}

#[tokio::test]
async fn disjoint_document_operands_commit_and_projected_reads_validate_only_named_fields() {
    let store = RegolithStore::in_memory().unwrap();
    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(b"doc", &field(1, 1, NormalValue::Int(10)).encode().unwrap())
        .await
        .unwrap();
    seed.commit().await.unwrap();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    first.get_parts(b"doc", &[1]).await.unwrap();
    first
        .merge(
            b"doc",
            &document_parts(&field(1, 2, NormalValue::Int(11))).unwrap(),
        )
        .await
        .unwrap();
    second
        .merge(
            b"doc",
            &document_parts(&field(2, 1, NormalValue::Int(22))).unwrap(),
        )
        .await
        .unwrap();
    second.commit().await.unwrap();
    first.commit().await.unwrap();
    let mut stale = store.new_txn(false).await.unwrap();
    stale.get_parts(b"doc", &[1]).await.unwrap();
    let mut update = store.new_txn(false).await.unwrap();
    update
        .merge(
            b"doc",
            &document_parts(&field(1, 3, NormalValue::Int(12))).unwrap(),
        )
        .await
        .unwrap();
    update.commit().await.unwrap();
    stale.set(b"decision", b"old").await.unwrap();
    assert!(stale.commit().await.is_err());
    let reader = store.new_txn(true).await.unwrap();
    let stored = StoredDocument::decode(&reader.get(b"doc").await.unwrap().unwrap()).unwrap();
    let doc = stored.materialize().unwrap();
    assert_eq!(doc.get("field1"), Some(&NormalValue::Int(12)));
    assert_eq!(doc.get("field2"), Some(&NormalValue::Int(22)));
}

#[test]
fn numeric_document_parts_add_without_adopting_a_stale_mirror() {
    let counter = |value: i64| StoredDocument {
        fields: [(
            7,
            StoredField {
                name: "count".into(),
                kind: ValueKind::Int64,
                priority: 1,
                value: value.to_be_bytes().to_vec(),
            },
        )]
        .into(),
    };
    let bytes = DefraMergeOperator
        .full_merge(
            b"doc",
            Some(&counter(10).encode().unwrap()),
            &[
                &document_parts(&counter(3)).unwrap(),
                &document_parts(&counter(5)).unwrap(),
            ],
        )
        .unwrap();
    assert_eq!(
        StoredDocument::decode(&bytes)
            .unwrap()
            .materialize()
            .unwrap()
            .get("count"),
        Some(&NormalValue::Int(18))
    );
}

#[test]
fn corrupt_encodings_are_refused_without_trusting_lengths() {
    let bytes = field(1, 1, NormalValue::Int(1)).encode().unwrap();
    for end in 0..bytes.len() {
        assert!(StoredDocument::decode(&bytes[..end]).is_none());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(StoredDocument::decode(&trailing).is_none());
    let mut huge = bytes;
    huge[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(StoredDocument::decode(&huge).is_none());
    assert!(DefraMergeOperator.touches(b"doc", b"DM\x01\x05broken", &[1]));
}

#[tokio::test]
async fn encrypted_document_operands_survive_reopen_and_schema_put_terminates_old_operands() {
    let dir = tempfile::tempdir().unwrap();
    let options = storage::RegolithStoreOptions::default().with_encryption_key([23; 32]);
    {
        let store = RegolithStore::open_with_options(dir.path(), options.clone()).unwrap();
        let mut seed = store.new_txn(false).await.unwrap();
        seed.set(b"doc", &field(1, 1, NormalValue::Int(10)).encode().unwrap())
            .await
            .unwrap();
        seed.commit().await.unwrap();
        let mut update = store.new_txn(false).await.unwrap();
        update
            .merge(
                b"doc",
                &document_parts(&field(1, 2, NormalValue::Int(20))).unwrap(),
            )
            .await
            .unwrap();
        update.commit().await.unwrap();
        store.close().await.unwrap();
    }
    let store = RegolithStore::open_with_options(dir.path(), options).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();
    let bytes = txn.get(b"doc").await.unwrap().unwrap();
    let doc = StoredDocument::decode(&bytes)
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        doc.to_cbor().unwrap(),
        document::Document::from_json_str(r#"{"field1":20}"#)
            .unwrap()
            .to_cbor()
            .unwrap()
    );
    txn.merge(
        b"doc",
        &document_parts(&field(1, 3, NormalValue::Int(30))).unwrap(),
    )
    .await
    .unwrap();
    // A version transition replaces both values and ordering metadata.
    txn.set(b"doc", &field(2, 1, NormalValue::Int(40)).encode().unwrap())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let reader = store.new_txn(true).await.unwrap();
    let doc = StoredDocument::decode(&reader.get(b"doc").await.unwrap().unwrap())
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(doc.get("field1"), None);
    assert_eq!(doc.get("field2"), Some(&NormalValue::Int(40)));
}
