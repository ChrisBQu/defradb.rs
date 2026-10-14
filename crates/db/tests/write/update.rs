use db::AutoCommitMutator;
use db::DB;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use rapidhash::RapidHashSet;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::sync::Arc;
use storage::RegolithStore;

fn test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Patch",
        "v1",
        "col-patch",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "left", FieldKind::int()),
            FieldDescription::new("3", "right", FieldKind::int()),
        ],
    )
}

/// A query plan materializes the document before entering the auto-commit
/// mutator. Two concurrent plans can therefore hand the mutator separate
/// stale copies of the same document. `modified_fields` is the patch
/// boundary: applying the second copy must not replace an unrelated field
/// written by the first update.
#[tokio::test]
async fn stale_disjoint_field_update_without_precondition_preserves_committed_fields() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).expect("create db"));
    db.create_collection(test_collection())
        .await
        .expect("schema");
    let mutator = AutoCommitMutator::new(Arc::clone(&db));

    let initial = Document::from_json_str(r#"{"left": 0, "right": 0}"#).expect("initial document");
    let created = mutator.create("Patch", initial).await.expect("create");

    let mut stale_left = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch left copy")
        .expect("left copy");
    let mut stale_right = stale_left.clone();

    stale_left.set("left", NormalValue::Int(1));
    mutator
        .update(
            "Patch",
            stale_left,
            RapidHashSet::from_iter(["left".to_string()]),
        )
        .await
        .expect("update left");

    stale_right.set("right", NormalValue::Int(1));
    mutator
        .update(
            "Patch",
            stale_right,
            RapidHashSet::from_iter(["right".to_string()]),
        )
        .await
        .expect("update right");

    let final_doc = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch final")
        .expect("final document");
    assert_eq!(final_doc.get("left"), Some(&NormalValue::Int(1)));
    assert_eq!(final_doc.get("right"), Some(&NormalValue::Int(1)));
}

/// Query mutations carry the snapshot against which their filter was
/// validated. If that snapshot changes while the update waits for the
/// per-document guard, the mutation must conflict instead of applying a
/// predicate that is no longer known to hold.
#[tokio::test]
async fn stale_conditional_update_returns_conflict() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).expect("create db"));
    db.create_collection(test_collection())
        .await
        .expect("schema");
    let mutator = AutoCommitMutator::new(Arc::clone(&db));

    let initial = Document::from_json_str(r#"{"left": 0, "right": 0}"#).expect("initial document");
    let created = mutator.create("Patch", initial).await.expect("create");

    let mut first = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch first copy")
        .expect("first copy");
    let expected_second = first.clone();
    let mut second = first.clone();

    first.set("left", NormalValue::Int(1));
    mutator
        .update(
            "Patch",
            first,
            RapidHashSet::from_iter(["left".to_string()]),
        )
        .await
        .expect("update left");

    second.set("right", NormalValue::Int(1));
    let error = mutator
        .update_if_unchanged(
            "Patch",
            expected_second,
            second,
            RapidHashSet::from_iter(["right".to_string()]),
        )
        .await
        .expect_err("stale conditional update must conflict");
    assert!(matches!(
        error,
        query::error::QueryError::TransactionConflict(ref message)
            if message == "transaction conflict. Please retry"
    ));

    let final_doc = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch final")
        .expect("final document");
    assert_eq!(final_doc.get("left"), Some(&NormalValue::Int(1)));
    assert_eq!(final_doc.get("right"), Some(&NormalValue::Int(0)));
}

/// A delete, local or merged from a peer, can commit after the query plan
/// materialized the document but before the update reloads it under the
/// per-document guard. That is the same stale snapshot as a concurrent
/// field change, so it must conflict and let the retry observe the deletion.
#[tokio::test]
async fn conditional_update_of_concurrently_deleted_document_returns_conflict() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).expect("create db"));
    db.create_collection(test_collection())
        .await
        .expect("schema");
    let mutator = AutoCommitMutator::new(Arc::clone(&db));

    let initial = Document::from_json_str(r#"{"left": 0, "right": 0}"#).expect("initial document");
    let created = mutator.create("Patch", initial).await.expect("create");

    let expected = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch snapshot")
        .expect("snapshot");
    let mut patched = expected.clone();
    patched.set("right", NormalValue::Int(1));

    mutator
        .delete("Patch", &created.doc_id)
        .await
        .expect("concurrent delete");

    let error = mutator
        .update_if_unchanged(
            "Patch",
            expected,
            patched,
            RapidHashSet::from_iter(["right".to_string()]),
        )
        .await
        .expect_err("update of a document deleted after its snapshot must conflict");
    assert!(
        matches!(
            error,
            query::error::QueryError::TransactionConflict(ref message)
                if message == "transaction conflict. Please retry"
        ),
        "unexpected error: {error:?}"
    );
}

/// Inject an external committed write immediately before the next mutation
/// transaction opens. This bypasses the application document queue, as a
/// storage-level writer can, and pins the old read-before-begin race.
struct ChangeBeforeBegin {
    inner: RegolithStore,
    pending: std::sync::Mutex<Option<(Vec<u8>, Vec<u8>)>>,
}

impl storage::corekv::private::Sealed for ChangeBeforeBegin {}

#[async_trait::async_trait]
impl storage::corekv::Store for ChangeBeforeBegin {
    async fn allocate(&self, key: &[u8], count: u64) -> storage::Result<std::ops::Range<u64>> {
        self.inner.allocate(key, count).await
    }

    async fn new_txn(&self, readonly: bool) -> storage::Result<Box<dyn storage::Txn>> {
        let pending = if readonly {
            None
        } else {
            self.pending.lock().unwrap().take()
        };
        if let Some((key, value)) = pending {
            let mut change = self.inner.new_txn(false).await?;
            change.set(&key, &value).await?;
            change.commit().await?;
        }
        self.inner.new_txn(readonly).await
    }

    async fn close(&self) -> storage::Result<()> {
        self.inner.close().await
    }
}

#[tokio::test]
async fn update_reads_fresh_values_in_the_transaction_that_writes_them() {
    let store = Arc::new(ChangeBeforeBegin {
        inner: RegolithStore::in_memory().unwrap(),
        pending: std::sync::Mutex::new(None),
    });
    let db = Arc::new(DB::open_from_arc(store.clone()).await.unwrap());
    db.create_collection(test_collection()).await.unwrap();
    let mutator = AutoCommitMutator::new(db.clone());
    let created = mutator
        .create(
            "Patch",
            Document::from_json_str(r#"{"left":0,"right":0}"#).unwrap(),
        )
        .await
        .unwrap();
    let collection = db.get_collection("Patch").unwrap().unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let short_id = collection
        .resolve_doc_short_id(&txn.systemstore().unwrap(), &created.doc_id)
        .await
        .unwrap()
        .unwrap();
    txn.discard().unwrap();
    let mut key = vec![b'd'];
    key.extend(collection.doc_key(short_id));
    let mut external = created.document.clone();
    external.set("left", 7_i64);
    *store.pending.lock().unwrap() = Some((key, external.to_cbor().unwrap()));
    let mut patch = created.document;
    patch.set("right", 9_i64);
    mutator
        .update("Patch", patch, ["right".to_string()].into_iter().collect())
        .await
        .unwrap();
    let result = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.get("left"), Some(&NormalValue::Int(7)));
    assert_eq!(result.get("right"), Some(&NormalValue::Int(9)));
    assert!(store.pending.lock().unwrap().is_none());
}
