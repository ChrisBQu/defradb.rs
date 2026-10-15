use storage::corekv::{IterOptions, Iterator, Reader, Result};
use storage::keys::headstore::{HeadstoreColKey, HeadstoreColSuperseded};

pub(super) async fn head_iterators<R: Reader + ?Sized>(
    reader: &R,
    collection: u32,
) -> Result<(Box<dyn Iterator>, Box<dyn Iterator>)> {
    let heads = reader
        .iterator(IterOptions::new().with_prefix(HeadstoreColKey::collection_prefix(collection)))
        .await?;
    let markers = reader
        .iterator(
            IterOptions::new()
                .with_prefix(HeadstoreColSuperseded::collection_prefix(collection))
                .with_keys_only(true),
        )
        .await?;
    Ok((heads, markers))
}
