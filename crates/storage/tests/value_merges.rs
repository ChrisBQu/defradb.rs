use regolith::MergeOperator;
use storage::corekv::Store;
use storage::merge::{counter_i64, DefraMergeOperator};
use storage::{RegolithStore, RegolithStoreOptions};

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn integer_operand_folding_obeys_the_engine_contract() {
    use regolith::testing::proptest::prelude::*;
    let result = regolith::testing::check_merge_operator(
        &DefraMergeOperator,
        Just(b"d/data/counter".to_vec()),
        any::<i64>().prop_map(|value| value.to_be_bytes().to_vec()),
        any::<i64>().prop_map(counter_i64),
    )
    .unwrap();
    assert!(result.folds > 0);
}

#[test]
fn floating_point_operands_are_not_folded() {
    let left = storage::merge::counter_f64(1e20);
    let right = storage::merge::counter_f64(-1e20);
    assert!(DefraMergeOperator
        .partial_merge(b"counter", &left, &right)
        .is_none());
    let base = 1.0f64.to_be_bytes();
    assert_eq!(
        DefraMergeOperator
            .full_merge(b"counter", Some(&base), &[&left, &right])
            .unwrap(),
        0.0f64.to_be_bytes()
    );
}

#[tokio::test]
async fn encrypted_concurrent_operands_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let options = RegolithStoreOptions::default().with_encryption_key([17; 32]);
    {
        let store = RegolithStore::open_with_options(dir.path(), options.clone()).unwrap();
        let mut first = store.new_txn(false).await.unwrap();
        let mut second = store.new_txn(false).await.unwrap();
        first.merge(b"counter", &counter_i64(7)).await.unwrap();
        second.merge(b"counter", &counter_i64(11)).await.unwrap();
        first.commit().await.unwrap();
        second.commit().await.unwrap();
        store.close().await.unwrap();
    }
    let store = RegolithStore::open_with_options(dir.path(), options).unwrap();
    let reader = store.new_txn(true).await.unwrap();
    assert_eq!(
        reader.get(b"counter").await.unwrap().unwrap().as_ref(),
        &18i64.to_be_bytes()
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn non_folding_operators_obey_the_engine_contract() {
    use regolith::testing::proptest::prelude::*;
    for single in [true, false] {
        regolith::testing::check_merge_operator(
            &DefraMergeOperator,
            Just(b"counter".to_vec()),
            any::<i32>().prop_map(move |value| {
                if single {
                    (value as f32).to_be_bytes().to_vec()
                } else {
                    f64::from(value).to_be_bytes().to_vec()
                }
            }),
            any::<i32>().prop_map(move |value| {
                if single {
                    storage::merge::counter_f32(value as f32)
                } else {
                    storage::merge::counter_f64(f64::from(value))
                }
            }),
        )
        .unwrap();
    }
    regolith::testing::check_merge_operator(
        &DefraMergeOperator,
        Just(b"fulltext/stats".to_vec()),
        (any::<i32>(), any::<i32>()).prop_map(|(documents, length)| {
            [
                i128::from(documents).to_be_bytes(),
                i128::from(length).to_be_bytes(),
            ]
            .concat()
        }),
        (any::<i32>(), any::<i32>()).prop_map(|(documents, length)| {
            storage::merge::stats_delta(documents.into(), length.into())
        }),
    )
    .unwrap();
}

#[test]
fn malformed_operands_and_stats_overflow_are_rejected() {
    let integer = counter_i64(1);
    let float = storage::merge::counter_f64(1.0);
    assert!(DefraMergeOperator
        .full_merge(b"key", None, &[&integer, &float])
        .is_none());
    assert!(DefraMergeOperator
        .full_merge(b"key", None, &[b"DM\x01\x01"])
        .is_none());
    let stats = [i128::MAX.to_be_bytes(), 0i128.to_be_bytes()].concat();
    let delta = storage::merge::stats_delta(1, 0);
    assert!(DefraMergeOperator
        .full_merge(b"key", Some(&stats), &[&delta])
        .is_none());
}
