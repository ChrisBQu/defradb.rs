//! Go's collection lookup (`GetCollections`), shared by every call that
//! takes `CollectionOptions`.

use std::sync::Arc;

use schema::CollectionVersion;

use crate::request::Request;
use crate::state::{FfiDatabase, FfiTransactionRegistry};
use crate::types::DefraResult;

/// What a lookup that must find one collection reports when it finds none.
pub(crate) const ERR_COLLECTION_NOT_FOUND: &str = "collection not found";

/// What a lookup that must find one collection reports when it finds several.
const ERR_AMBIGUOUS_COLLECTION: &str = "more than one collection matches the given criteria";

/// Where a lookup reads collections: the node's store, or a transaction.
pub(crate) enum Source {
    Node(Arc<FfiDatabase>),
    Txn {
        registry: Arc<FfiTransactionRegistry>,
        txn_id: String,
    },
}

impl Source {
    /// The source `request` reads from.
    pub(crate) fn of(request: &Request) -> Result<Self, DefraResult> {
        Ok(match request.target.txn_id() {
            None => Source::Node(request.database()?),
            Some(txn_id) => Source::Txn {
                registry: request.txn_registry()?,
                txn_id: txn_id.to_string(),
            },
        })
    }
}

/// The collection versions `selector` selects, sorted by name and version,
/// as Go's `GetCollections` returns them.
pub(crate) async fn collections(
    source: &Source,
    context: &defra_core::RequestContext,
    selector: &db::CollectionSelector,
) -> Result<Vec<CollectionVersion>, String> {
    let versions = match source {
        Source::Txn { registry, txn_id } => registry
            .get_collections_in_txn_as(context, txn_id)
            .await
            .map_err(|e| e.to_string())?,
        // Inactive versions cost a scan of every stored version; a selector
        // that does not ask for them is answered from the active listing.
        Source::Node(database) if selector.needs_all_versions() => database
            .get_all_collection_versions()
            .await
            .map_err(|e| e.to_string())?,
        Source::Node(database) => database
            .get_active_collection_versions()
            .map_err(|e| e.to_string())?,
    };

    // Go propagates not-found only from the direct version lookup: an
    // unknown version ID errors, an unknown name or collection ID selects
    // nothing.
    if selector.resolves_by_version_lookup() {
        let version_id = selector.version_id.as_ref();
        if !versions.iter().any(|v| Some(&v.version_id) == version_id) {
            return Err(ERR_COLLECTION_NOT_FOUND.to_string());
        }
    }

    let mut selected: Vec<_> = versions
        .into_iter()
        .filter(|version| selector.selects(version))
        .collect();
    selected.sort_by(|a, b| (&a.name, &a.version_id).cmp(&(&b.name, &b.version_id)));
    Ok(selected)
}

/// The one collection version `selector` selects, as Go's `getCollection`
/// requires.
pub(crate) async fn collection(
    source: &Source,
    context: &defra_core::RequestContext,
    selector: &db::CollectionSelector,
) -> Result<CollectionVersion, String> {
    let mut selected = collections(source, context, selector).await?;
    match selected.len() {
        0 => Err(ERR_COLLECTION_NOT_FOUND.to_string()),
        1 => Ok(selected.remove(0)),
        _ => Err(ERR_AMBIGUOUS_COLLECTION.to_string()),
    }
}
