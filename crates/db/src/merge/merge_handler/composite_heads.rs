use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;
use storage::corekv::Key;
use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    pub(crate) async fn update_heads(
        &self,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &CompositeMergeState,
    ) -> std::result::Result<(), MergeError> {
        let head = HeadstoreDocKey::new(context.doc_short_id, "C", *context.cid);
        crate::block::heads::document::publish(
            headstore,
            &head,
            &encode_priority_varint(context.payload.priority),
            context.block.heads.as_deref().unwrap_or_default(),
        )
        .await
        .map_err(|e| MergeError::Storage(e.to_string()))?;
        headstore
            .set(
                &HeadstorePriorityKey::new(
                    context.doc_short_id,
                    context.payload.priority,
                    *context.cid,
                )
                .bytes(),
                &[],
            )
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?;

        if let Some(links) = &context.block.links {
            for link in links {
                if !state.linked_field_cids.contains(&link.link) {
                    continue;
                }
                let priority = state.field_block_priorities[&link.link];
                let head = HeadstoreDocKey::new(context.doc_short_id, &link.name, link.link);
                crate::block::heads::document::publish(
                    headstore,
                    &head,
                    &encode_priority_varint(priority),
                    state
                        .field_block_heads
                        .get(&link.name)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )
                .await
                .map_err(|e| MergeError::Storage(e.to_string()))?;
                headstore
                    .set(
                        &HeadstorePriorityKey::new(context.doc_short_id, priority, link.link)
                            .bytes(),
                        &[],
                    )
                    .await
                    .map_err(|e| MergeError::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }
}
