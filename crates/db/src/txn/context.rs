//! One request's view of an explicit transaction, on the worker that owns it.
//!
//! The owner lends the transaction to the request ([`OwnedTxn`]'s turn), and
//! the context hands it to the request's handles: one lensed fetcher for the
//! whole request, a mutator and a collection provider made on demand. They
//! share the transaction through `&self`, with no lock, and are all dropped
//! when the request ends, which gives the transaction back to its owner.
//!
//! [`OwnedTxn`]: crate::txn::registry::owned::OwnedTxn

use std::sync::Arc;

use query::fetcher::CollectionProvider;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use query::txn::{AcpRegistrationProjection, TransactionContext};
use storage::corekv::Store;

use crate::collection::provider::TxnCollectionProvider;
use crate::database::DB;
use crate::event::emission::TxnBroadcaster;
use crate::read::lensed::fetcher::{LensedDocFetcher, SharedVersionCache};
use crate::txn::LentTxn;
use crate::write::doc::DbDocMutator;

/// One request's view of an explicit transaction.
pub struct DbTransactionContext<S: Store + 'static> {
    db: Arc<DB<S>>,
    id: String,
    readonly: bool,
    txn: LentTxn<S>,
    fetcher: Arc<LensedDocFetcher<S>>,
    broadcaster: Option<Arc<dyn TxnBroadcaster>>,
}

impl<S: Store + 'static> DbTransactionContext<S> {
    pub(crate) fn new(
        db: Arc<DB<S>>,
        id: String,
        readonly: bool,
        txn: LentTxn<S>,
        lens_store: Arc<dyn lens::TransformStore>,
        version_cache: SharedVersionCache,
        broadcaster: Option<Arc<dyn TxnBroadcaster>>,
    ) -> Self {
        let fetcher = Arc::new(LensedDocFetcher::lent(
            db.clone(),
            Arc::clone(&txn),
            lens_store,
            version_cache,
        ));
        Self {
            db,
            id,
            readonly,
            txn,
            fetcher,
            broadcaster,
        }
    }
}

impl<S: Store + 'static> TransactionContext for DbTransactionContext<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_readonly(&self) -> bool {
        self.readonly
    }

    fn doc_fetcher(&self) -> Arc<dyn DocFetcher> {
        self.fetcher.clone()
    }

    fn doc_mutator(&self) -> Option<Arc<dyn DocMutator>> {
        (!self.readonly).then(|| {
            Arc::new(DbDocMutator::from_shared_txn_with_broadcaster(
                self.db.clone(),
                Arc::clone(&self.txn),
                self.broadcaster.clone(),
            )) as Arc<dyn DocMutator>
        })
    }

    fn collection_provider(&self) -> Option<Arc<dyn CollectionProvider>> {
        Some(Arc::new(TxnCollectionProvider::new(
            self.db.clone(),
            Arc::clone(&self.txn),
        )))
    }

    fn acp_registration_projection(&self) -> Option<Arc<AcpRegistrationProjection>> {
        Some(self.txn.acp_registration_projection())
    }
}
