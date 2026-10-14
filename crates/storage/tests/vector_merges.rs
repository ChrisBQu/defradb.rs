use regolith::MergeOperator;
use storage::merge::{vector, DefraMergeOperator};
use storage::vector::{decode_meta, decode_node, encode_meta, encode_node, Meta, Node, NodeId};

#[test]
fn links_preserve_structure_and_counts_preserve_routing() {
    let node = Node::new(NodeId(7), vec![0.25, 0.75], 1);
    let base = encode_node(&node);
    let a = vector::links(0, 4);
    let b = vector::links(0, 9);
    let self_link = vector::links(0, 7);
    let result = DefraMergeOperator
        .full_merge(b"node", Some(&base), &[&a, &b, &a, &self_link])
        .unwrap();
    let stored = decode_node(&result).unwrap();
    assert_eq!(stored.vector, node.vector);
    assert_eq!(stored.layers, vec![vec![NodeId(4), NodeId(9)], vec![]]);
    assert_eq!(stored.id, node.id);
    assert!(!stored.deleted);
    assert!(!DefraMergeOperator.touches(b"node", &a, &[0]));
    assert!(DefraMergeOperator.touches(b"node", &a, &[1]));
    let meta = Meta {
        entry_point: NodeId(7),
        top_layer: 1,
        live: 2,
        waste: 0,
        rebuilds: 3,
    };
    let added = vector::counts(5, 2);
    let removed = vector::counts(-1, 1);
    let result = DefraMergeOperator
        .full_merge(b"meta", Some(&encode_meta(&meta)), &[&added, &removed])
        .unwrap();
    assert_eq!(
        decode_meta(&result).unwrap(),
        Meta {
            live: 6,
            waste: 3,
            ..meta
        }
    );
    assert!(DefraMergeOperator
        .partial_merge(b"meta", &added, &removed)
        .is_none());
}

#[test]
fn malformed_vector_operands_fail_without_allocating_from_unchecked_lengths() {
    let base = encode_node(&Node::new(NodeId(0), vec![1.0], 0));
    let valid = vector::links(0, 1);
    for end in 0..valid.len() {
        assert!(DefraMergeOperator
            .full_merge(b"node", Some(&base), &[&valid[..end]])
            .is_none());
    }
    assert!(DefraMergeOperator
        .full_merge(b"node", Some(&base), &[&vector::links(u32::MAX, 1)])
        .is_none());
    assert!(DefraMergeOperator
        .full_merge(b"node", None, &[&valid])
        .is_none());
    let mut corrupt = base.clone();
    corrupt.extend_from_slice(b"extra");
    assert!(decode_node(&corrupt).is_err());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn vector_operator_folding_and_touches_obey_engine_contracts() {
    use regolith::testing::proptest::prelude::*;
    let operands = (0u32..4, any::<u64>()).prop_map(|(layer, id)| vector::links(layer, id));
    let bases = any::<u64>().prop_map(|id| encode_node(&Node::new(NodeId(id), vec![1.0, 0.0], 3)));
    let checked = regolith::testing::check_merge_operator(
        &DefraMergeOperator,
        Just(b"node".to_vec()),
        bases,
        operands.clone(),
    )
    .unwrap();
    assert!(checked.folds > 0);
    let checked = regolith::testing::check_touches(
        &DefraMergeOperator,
        Just(b"node".to_vec()),
        operands,
        regolith::testing::proptest::collection::vec(0u32..3, 0..4),
    )
    .unwrap();
    assert!(checked.folds > 0);
}

#[tokio::test]
async fn encrypted_links_reopen_and_structural_put_terminates_operands() {
    use storage::corekv::Store;
    let dir = tempfile::tempdir().unwrap();
    let options = storage::RegolithStoreOptions::default().with_encryption_key([29; 32]);
    {
        let store = storage::RegolithStore::open_with_options(dir.path(), options.clone()).unwrap();
        let mut seed = store.new_txn(false).await.unwrap();
        seed.set(b"node", &encode_node(&Node::new(NodeId(0), vec![1.0], 0)))
            .await
            .unwrap();
        seed.commit().await.unwrap();
        let mut update = store.new_txn(false).await.unwrap();
        update.merge(b"node", &vector::links(0, 1)).await.unwrap();
        update.commit().await.unwrap();
        store.close().await.unwrap();
    }
    let store = storage::RegolithStore::open_with_options(dir.path(), options).unwrap();
    let mut txn = store.new_txn(false).await.unwrap();
    assert_eq!(
        decode_node(&txn.get(b"node").await.unwrap().unwrap())
            .unwrap()
            .layers[0],
        vec![NodeId(1)]
    );
    txn.merge(b"node", &vector::links(0, 2)).await.unwrap();
    let mut replacement = Node::new(NodeId(0), vec![0.0, 1.0], 0);
    replacement.layers[0] = vec![NodeId(3)];
    txn.set(b"node", &encode_node(&replacement)).await.unwrap();
    txn.commit().await.unwrap();
    let read = store.new_txn(true).await.unwrap();
    assert_eq!(
        decode_node(&read.get(b"node").await.unwrap().unwrap()).unwrap(),
        replacement
    );
}
