//! A lens whose config commits durably stays committed when registering it in
//! the lens store fails afterwards: it is owed, known as a migration, and
//! registered when it is next used.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use db::database::DB;
use db::txn::registry::DbTransactionRegistry;
use futures::TryStreamExt;
use lens::{
    LensConfig, LensDoc, LensDocResultStream, LensDocStream, LensModule, MemoryTransformStore,
    TransformId, TransformStore,
};
use query::txn::TransactionRegistry;
use rapidhash::RapidHashMap;
use storage::RegolithStore;

/// A transform store that refuses to register a lens under its ID while
/// `refuse` is set, as a store does when a module cannot be read or compiled
/// at that moment. Transactions register into a store that never refuses.
struct RefusingStore {
    inner: MemoryTransformStore,
    refuse: AtomicBool,
}

#[async_trait]
impl TransformStore for RefusingStore {
    fn new_isolated_store(&self) -> Arc<dyn TransformStore> {
        Arc::new(MemoryTransformStore::new())
    }

    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        self.inner.add(config).await
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> lens::Result<()> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(lens::Error::Pipeline("registration refused".to_string()));
        }
        self.inner.add_with_id(id, config).await
    }

    async fn list(&self) -> lens::Result<RapidHashMap<String, LensModule>> {
        self.inner.list().await
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        self.inner.transform(id, docs)
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.inner.inverse(id, docs)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.inner.has_transform(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        self.inner.remove(id).await
    }
}

/// A database with a `Users` collection whose lens store refuses registration.
async fn node() -> (Arc<DB<RegolithStore>>, Arc<RefusingStore>) {
    let store = Arc::new(RefusingStore {
        inner: MemoryTransformStore::new(),
        refuse: AtomicBool::new(true),
    });
    let mut db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    db.set_lens_store(store.clone());
    db.create_collections_atomic(query::parse_sdl("type Users { name: String }").unwrap())
        .await
        .unwrap();
    (Arc::new(db), store)
}

fn config(source: &str, destination: &str) -> LensConfig {
    LensConfig::new(
        source,
        destination,
        LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
    )
}

async fn run(db: &DB<RegolithStore>, id: &TransformId) -> lens::Result<Vec<LensDoc>> {
    let mut doc = LensDoc::new();
    doc.insert("name".to_string(), serde_json::json!("Ada"));
    let docs: LensDocStream = Box::pin(futures::stream::iter(vec![doc]));
    db.lens_store().transform(id, docs)?.try_collect().await
}

/// The committed lens `id` is owed: known, refused while registration fails,
/// and registered by its first use once registration succeeds.
async fn assert_owed_until_used(db: &DB<RegolithStore>, store: &RefusingStore, id: &TransformId) {
    assert!(!store.inner.has_transform(id), "registration was refused");
    assert!(
        db.has_migration(id),
        "a committed migration is known while its registration is owed"
    );
    assert!(db
        .lens_store()
        .list()
        .await
        .unwrap()
        .contains_key(&id.to_string()));

    let error = run(db, id)
        .await
        .expect_err("a use while registration fails reports why");
    assert!(
        error.to_string().contains("registration refused"),
        "{error}"
    );
    assert!(!store.inner.has_transform(id));

    store.refuse.store(false, Ordering::SeqCst);
    let docs = run(db, id)
        .await
        .expect("the owed lens registers when it is used");
    assert_eq!(docs.len(), 1);
    assert!(store.inner.has_transform(id));
    assert!(db.has_migration(id));
}

#[tokio::test]
async fn a_transaction_migration_commits_when_its_registration_fails() {
    let (db, store) = node().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let txn_id = registry.begin(false).await.unwrap();
    let id = registry
        .set_migration_in_txn(&txn_id, config("users-v1", "users-v2"), None)
        .await
        .unwrap();

    registry
        .commit(&txn_id)
        .await
        .expect("the commit stands when the registration after it fails");

    assert_owed_until_used(&db, &store, &id).await;
}

#[tokio::test]
async fn a_patch_migration_commits_when_its_registration_fails() {
    let (db, store) = node().await;
    let patched = db
        .patch_collection_with_migration(
            "Users",
            r#"[{"op": "add", "path": "/Users/Fields/-", "value": {"Name": "age", "Kind": "Int"}}]"#,
            Some(config("", "")),
            None,
        )
        .await
        .expect("the patch stands when the registration after it fails");
    let id = TransformId::new(
        patched
            .previous_version
            .and_then(|previous| previous.transform)
            .unwrap(),
    );

    assert_owed_until_used(&db, &store, &id).await;
}

#[tokio::test]
async fn an_added_lens_is_returned_when_its_registration_fails() {
    let (db, store) = node().await;
    let id = db
        .add_lens(config("", ""))
        .await
        .expect("the lens is committed, so adding it succeeds");

    assert_owed_until_used(&db, &store, &id).await;
}
