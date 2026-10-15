//! The lensed fetcher's migration-context cache: one immutable snapshot per
//! committed migration-graph generation, read without a lock.

use std::sync::Arc;

use super::super::{LensedAutoCommitFetcher, MigrationCache, MigrationContext};
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

fn fetcher() -> (Arc<DB<RegolithStore>>, LensedAutoCommitFetcher<RegolithStore>) {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let fetcher = LensedAutoCommitFetcher::new(Arc::clone(&db));
    (db, fetcher)
}

fn snapshot(fetcher: &LensedAutoCommitFetcher<RegolithStore>) -> (u64, usize) {
    fetcher
        .migration_cache
        .peek(|cache| (cache.generation, cache.contexts.len()))
}

fn cached(
    fetcher: &LensedAutoCommitFetcher<RegolithStore>,
    generation: u64,
    key: &str,
) -> Option<Arc<MigrationContext>> {
    fetcher
        .migration_cache
        .peek(|cache| cache.get(generation, key))
}

#[tokio::test]
async fn a_hit_is_served_from_the_snapshot_of_its_generation() {
    let (db, fetcher) = fetcher();
    let collection = users("v1");

    let (generation, has_migrations, history) =
        fetcher.load_migration_context(&collection).await.unwrap();
    assert!(!has_migrations);
    assert!(history.is_none());
    assert_eq!(snapshot(&fetcher), (generation, 1));
    let first = cached(&fetcher, generation, "users-collection:v1").unwrap();

    fetcher.load_migration_context(&collection).await.unwrap();
    let second = cached(&fetcher, generation, "users-collection:v1").unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "a hit reads the published context, it does not compute it again"
    );

    db.bump_migration_generation();
    let (next, _, _) = fetcher.load_migration_context(&collection).await.unwrap();
    assert_eq!(next, generation + 1);
    assert_eq!(
        snapshot(&fetcher),
        (next, 1),
        "a new generation starts from an empty snapshot"
    );
    assert!(cached(&fetcher, generation, "users-collection:v1").is_none());
}

#[test]
fn a_context_is_published_only_at_its_generation_or_after_an_older_one() {
    let context: Arc<MigrationContext> = Arc::new((false, None));
    let at_one = MigrationCache::default().with(1, "a", &context).unwrap();
    assert_eq!((at_one.generation, at_one.contexts.len()), (1, 1));

    let both = at_one.with(1, "b", &context).unwrap();
    assert_eq!(both.contexts.len(), 2);
    assert_eq!(at_one.contexts.len(), 1, "a published snapshot never changes");

    let at_two = both.with(2, "c", &context).unwrap();
    assert_eq!(
        (at_two.generation, at_two.contexts.len()),
        (2, 1),
        "the contexts of an older generation are dropped whole"
    );
    assert!(
        at_two.with(1, "a", &context).is_none(),
        "a context computed under an older generation is never published"
    );
    assert!(at_two.get(1, "c").is_none());
    assert!(at_two.get(2, "c").is_some());
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
    assert_eq!(snapshot(&fetcher), (0, 8));
}
