//! The heads an update or delete builds its blocks on, decided here for
//! every caller.
//!
//! The first attempt builds on heads read once before the transaction: the
//! block it signs is the block every pass of the request writes, signed once.
//! A retry after a conflict lost to another write of the document, so it
//! reads the heads again in its own transaction and builds on the winner,
//! one priority above it, instead of forking beside it at equal priority.
//! Only a retry pays the second read.

use std::borrow::Cow;

use datastore::NamespaceView;
use document::DocID;
use rapidhash::RapidHashMap;
use storage::corekv::RetryTxn;

use crate::block::builder::DocHeadsSnapshot;
use crate::collection::Collection;

/// Heads read before an update's or delete's transaction, by document short
/// ID.
pub(crate) struct BaseHeads(RapidHashMap<u64, DocHeadsSnapshot>);

fn failed(error: &dyn std::fmt::Display) -> query::error::QueryError {
    query::error::QueryError::execution(format!("failed to read document heads: {error}"))
}

impl BaseHeads {
    /// The heads of `doc_ids`' documents, read at one snapshot. A document
    /// not found here is read by its attempt.
    pub(crate) async fn read<S>(
        db: &crate::database::DB<S>,
        collection: &Collection,
        doc_ids: &[DocID],
    ) -> query::error::Result<Self>
    where
        S: storage::corekv::Store + 'static,
    {
        let txn = db.new_txn(true).await.map_err(|error| failed(&error))?;
        let mut heads = RapidHashMap::default();
        {
            let systemstore = txn.systemstore().map_err(|error| failed(&error))?;
            let headstore = txn.headstore().map_err(|error| failed(&error))?;
            for doc_id in doc_ids {
                let Some((doc_short_id, _)) = collection
                    .resolve_doc_identity(&systemstore, doc_id)
                    .await
                    .map_err(|error| failed(&error))?
                else {
                    continue;
                };
                let snapshot = DocHeadsSnapshot::load(&headstore, doc_short_id)
                    .await
                    .map_err(|error| failed(&error))?;
                heads.insert(doc_short_id, snapshot);
            }
        }
        txn.discard().map_err(|error| failed(&error))?;
        Ok(Self(heads))
    }

    /// [`Self::read`] when `context` signs its blocks. An unsigned block
    /// need not be the same in every pass, so nothing is read here and each
    /// attempt reads the heads in its own transaction.
    pub(crate) async fn read_if_signed<S>(
        db: &crate::database::DB<S>,
        collection: &Collection,
        doc_ids: &[DocID],
        context: &defra_core::RequestContext,
    ) -> query::error::Result<Self>
    where
        S: storage::corekv::Store + 'static,
    {
        if context.signing().is_none() {
            return Ok(Self(RapidHashMap::default()));
        }
        Self::read(db, collection, doc_ids).await
    }

    /// The canonical ID of `doc_id`'s document and the heads its update
    /// builds on, read at one snapshot. A document that is not there is
    /// [`query::error::QueryError::document_not_found`].
    pub(crate) async fn read_identified<S>(
        db: &crate::database::DB<S>,
        collection: &Collection,
        doc_id: &DocID,
    ) -> query::error::Result<(DocID, Self)>
    where
        S: storage::corekv::Store + 'static,
    {
        let txn = db.new_txn(true).await.map_err(|error| failed(&error))?;
        let read: query::error::Result<(DocID, Self)> = async {
            let systemstore = txn.systemstore().map_err(|error| failed(&error))?;
            let headstore = txn.headstore().map_err(|error| failed(&error))?;
            let (doc_short_id, canonical) = collection
                .require_doc_identity(&systemstore, doc_id)
                .await
                .map_err(|error| match error {
                    crate::error::Error::DocumentNotFound(id) => {
                        query::error::QueryError::document_not_found(id)
                    }
                    other => query::error::QueryError::execution(other.to_string()),
                })?;
            let snapshot = DocHeadsSnapshot::load(&headstore, doc_short_id)
                .await
                .map_err(|error| failed(&error))?;
            let mut heads = RapidHashMap::default();
            heads.insert(doc_short_id, snapshot);
            Ok((canonical, Self(heads)))
        }
        .await;
        let discarded = txn.discard();
        let read = read?;
        discarded.map_err(|error| failed(&error))?;
        Ok(read)
    }

    /// The heads `doc_short_id`'s blocks build on in the attempt `retry` is
    /// running: those read before the transaction, unless this attempt is a
    /// retry after a conflict or the document was not found then, when they
    /// are read from the attempt's `headstore`.
    pub(crate) async fn for_attempt<'a>(
        &'a self,
        retry: &dyn RetryTxn,
        headstore: &NamespaceView,
        doc_short_id: u64,
    ) -> query::error::Result<Cow<'a, DocHeadsSnapshot>> {
        if retry.previous_conflict().is_none() {
            if let Some(heads) = self.0.get(&doc_short_id) {
                return Ok(Cow::Borrowed(heads));
            }
        }
        DocHeadsSnapshot::load(headstore, doc_short_id)
            .await
            .map(Cow::Owned)
            .map_err(|error| failed(&error))
    }
}
