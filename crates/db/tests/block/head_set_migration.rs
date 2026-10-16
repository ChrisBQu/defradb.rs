//! The move from the marker layout to merged head sets at open: the same live
//! heads and priorities, markers whose parent was never published held ahead,
//! the old keys gone, and a second open a no-op.

use std::sync::Arc;

use cid::Cid;
use db::block::heads::{collection_set, document};
use db::DB;
use storage::corekv::{IterOptions, Store};
use storage::keys::doc_id_index::encode_doc_short_id;
use storage::keys::headstore::HeadstoreDocKey;
use storage::RegolithStore;

const DOC: u64 = 47;
const COLLECTION: u32 = 7;

fn cid(value: &[u8]) -> Cid {
    defra_core::block::generate_cid_from_bytes(value).unwrap()
}

fn document_marker(parent: Cid, child: Cid) -> Vec<u8> {
    [
        b"/ds/".as_slice(),
        &encode_doc_short_id(DOC),
        format!("/name/{parent}/{child}").as_bytes(),
    ]
    .concat()
}

async fn count(store: &RegolithStore, prefixes: &[&[u8]]) -> usize {
    let txn = store.new_txn(true).await.unwrap();
    let mut count = 0;
    for prefix in prefixes {
        let mut iter = txn
            .iterator(IterOptions::new().with_prefix([b"h".as_slice(), prefix].concat()))
            .await
            .unwrap();
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn legacy_heads_and_markers_open_as_the_same_live_heads() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let (a, b, c) = (cid(b"a"), cid(b"b"), cid(b"c"));
    let (early_parent, reclaimed_parent, early_child) =
        (cid(b"early parent"), cid(b"reclaimed"), cid(b"early child"));
    let (x, y, z, late) = (cid(b"x"), cid(b"y"), cid(b"z"), cid(b"late"));
    {
        let seed = DB::from_arc(store.clone()).unwrap();
        let txn = seed.new_txn(false).await.unwrap();
        let headstore = txn.headstore().unwrap();
        let head = |cid| HeadstoreDocKey::new(DOC, "name", cid);
        for (cid, priority) in [(a, 1u8), (b, 2), (c, 3)] {
            headstore
                .set(&storage::corekv::Key::bytes(&head(cid)), &[priority])
                .await
                .unwrap();
        }
        // b supersedes a; c is concurrent; an early child holds its parent
        // ahead; a reclaimed parent (its guard is there, its head is not) is
        // dropped.
        headstore.set(&document_marker(a, b), &[]).await.unwrap();
        headstore
            .set(&document_marker(early_parent, early_child), &[])
            .await
            .unwrap();
        headstore
            .set(&document_marker(reclaimed_parent, b), &[])
            .await
            .unwrap();
        headstore
            .set(&document::publication_key(&head(reclaimed_parent)), &[])
            .await
            .unwrap();
        for (cid, priority) in [(x, 1u8), (y, 2)] {
            headstore
                .set(format!("/c/{COLLECTION}/{cid}").as_bytes(), &[priority])
                .await
                .unwrap();
        }
        headstore
            .set(format!("/cs/{COLLECTION}/{x}/{y}").as_bytes(), &[])
            .await
            .unwrap();
        headstore
            .set(format!("/cs/{COLLECTION}/{z}/{late}").as_bytes(), &[])
            .await
            .unwrap();
        drop(headstore);
        txn.commit().await.unwrap();
    }

    for open in 0..2 {
        let db = DB::open_from_arc(store.clone()).await.unwrap();
        let txn = db.new_txn(true).await.unwrap();
        let headstore = txn.headstore().unwrap();
        let docs = document::load(&headstore, DOC).await.unwrap();
        let live: Vec<_> = docs
            .live()
            .map(|(name, priority)| (name.to_vec(), priority))
            .collect();
        let mut expected = vec![
            (document::name("name", &b), 2),
            (document::name("name", &c), 3),
        ];
        expected.sort();
        assert_eq!(live, expected, "open {open}");
        assert_eq!(
            docs.ahead().collect::<Vec<_>>(),
            [document::name("name", &early_parent).as_slice()],
            "open {open}"
        );
        let collection = collection_set(&headstore, COLLECTION).await.unwrap();
        let live: Vec<_> = collection
            .live()
            .map(|(name, priority)| (name.to_vec(), priority))
            .collect();
        assert_eq!(live, [(y.to_string().into_bytes(), 2)], "open {open}");
        assert_eq!(
            collection.ahead().collect::<Vec<_>>(),
            [z.to_string().as_bytes()],
            "open {open}"
        );
        drop(headstore);
        txn.discard().unwrap();
        assert_eq!(count(&store, &[b"/d/", b"/ds/", b"/c/", b"/cs/"]).await, 0);
    }
}
