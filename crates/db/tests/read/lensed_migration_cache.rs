//! The auto-commit lensed fetcher's migration contexts: one immutable
//! snapshot per committed migration-graph generation, read without a lock.

use std::sync::Arc;

use super::super::LensedAutoCommitFetcher;
use crate::collection::Collection;
use crate::DB;
use storage::RegolithStore;

fn users(version_id: &str) -> Collection {
    Collection::new(schema::CollectionVersion::new(
        "Users",
        version_id,
        "users-collection",
        vec![schema::FieldDescription::new(
            "1",
            "name",
            schema::FieldKind::string(),
        )],
    ))
}

fn fetcher() -> (
    Arc<DB<RegolithStore>>,
    LensedAutoCommitFetcher<RegolithStore>,
) {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let fetcher = LensedAutoCommitFetcher::new(Arc::clone(&db));
    (db, fetcher)
}

#[tokio::test]
async fn a_hit_is_served_from_the_snapshot_of_its_generation() {
    let (db, fetcher) = fetcher();
    let collection = users("v1");

    let (generation, has_migrations, history) =
        fetcher.load_migration_context(&collection).await.unwrap();
    assert!(!has_migrations);
    assert!(history.is_none());
    assert_eq!(fetcher.migration_cache.generation(), generation);
    let first = fetcher
        .migration_cache
        .get(generation, "users-collection:v1")
        .unwrap();

    fetcher.load_migration_context(&collection).await.unwrap();
    let second = fetcher
        .migration_cache
        .get(generation, "users-collection:v1")
        .unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "a hit reads the published context, it does not compute it again"
    );

    db.bump_migration_generation();
    let (next, _, _) = fetcher.load_migration_context(&collection).await.unwrap();
    assert_eq!(next, generation + 1);
    assert_eq!(
        fetcher.migration_cache.generation(),
        next,
        "a new generation starts its own snapshot"
    );
    assert!(fetcher
        .migration_cache
        .get(generation, "users-collection:v1")
        .is_none());
    assert!(fetcher
        .migration_cache
        .get(next, "users-collection:v1")
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_loads_publish_every_context_into_one_snapshot() {
    let (_db, fetcher) = fetcher();
    let fetcher = Arc::new(fetcher);
    let mut tasks = Vec::new();
    for index in 0..32 {
        let fetcher = Arc::clone(&fetcher);
        tasks.push(tokio::spawn(async move {
            let collection = users(&format!("v{}", index % 8));
            fetcher.load_migration_context(&collection).await.unwrap().0
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 0);
    }
    assert_eq!(fetcher.migration_cache.generation(), 0);
    for version in 0..8 {
        assert!(
            fetcher
                .migration_cache
                .get(0, &format!("users-collection:v{version}"))
                .is_some(),
            "the context of v{version} was lost to a concurrent publish"
        );
    }
}
