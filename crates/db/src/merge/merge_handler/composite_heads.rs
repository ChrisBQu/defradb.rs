use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::*;
use storage::keys::headstore::HeadstoreDocKey;

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Publish the composite and each field block it merged into the
    /// document's head set, in one operand.
    pub(crate) async fn update_heads(
        &self,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &mut CompositeMergeState,
    ) -> std::result::Result<(), MergeError> {
        let storage_error = |e: storage::corekv::Error| MergeError::Storage(e.to_string());
        let set = state
            .reads
            .heads(headstore, context.doc_short_id)
            .await
            .map_err(storage_error)?;
        let mut write = storage::merge::head_set::Write::default();
        let head = HeadstoreDocKey::new(context.doc_short_id, "C", *context.cid);
        crate::block::heads::document::publish(
            headstore,
            set,
            &mut write,
            &head,
            context.payload.priority,
            context.block.heads.as_deref().unwrap_or_default(),
        )
        .await
        .map_err(storage_error)?;

        if let Some(links) = &context.block.links {
            for link in links {
                if !state.linked_field_cids.contains(&link.link) {
                    continue;
                }
                let head = HeadstoreDocKey::new(context.doc_short_id, &link.name, link.link);
                crate::block::heads::document::publish(
                    headstore,
                    set,
                    &mut write,
                    &head,
                    state.field_block_priorities[&link.link],
                    state
                        .field_block_heads
                        .get(&link.name)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )
                .await
                .map_err(storage_error)?;
            }
        }
        crate::block::heads::document::commit(headstore, context.doc_short_id, &write)
            .await
            .map_err(storage_error)?;
        state.reads.wrote_heads();
        Ok(())
    }
}
