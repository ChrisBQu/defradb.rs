//! Node lifecycle: create, close, and report options.

pub(crate) mod options;
mod peer_key;
mod sanitized;
mod transport;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use kovan::AtomOption;
use zeroize::Zeroizing;

use crate::ffi_entry;
use crate::state::{FfiStore, NodeState, P2PState, PolicyStore, NODES, TXNS};
use crate::target::{resolve_node, ERR_INVALID_HANDLE};
use crate::try_ffi;
use crate::types::{DefraNewNodeResult, DefraResult, NodeInitOptions};

use options::{NodeSetup, StoreLocation};

/// Creates and starts an embedded DefraDB node.
///
/// # Safety
///
/// Every non-null pointer in `options` must be valid for the call: strings
/// null-terminated, byte buffers at least their stated length.
#[no_mangle]
pub unsafe extern "C" fn defra_node_new(options: NodeInitOptions) -> DefraNewNodeResult {
    ffi_entry! {
        let Some(rt) = crate::runtime::edge() else {
            return DefraNewNodeResult::error(crate::helpers::ERR_RUNTIME_NOT_INITIALIZED);
        };
        let prepared = options::read(&options).and_then(|setup| {
            let transport = transport::read(&options)?;
            let options_json = sanitized::options_json(&options, &setup, &transport);
            Ok((setup, transport, options_json))
        });
        let (setup, transport, options_json) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return DefraNewNodeResult::error(error),
        };

        let result = crate::helpers::block_on(
            rt,
            build_node_state(setup, transport, options_json, rt.clone()),
        )
        .map(|state| NODES.insert(state));

        match result {
            Ok(handle) => DefraNewNodeResult::success(handle),
            Err(error) => DefraNewNodeResult::error(error),
        }
    }
}

/// Builds a node whose network work, P2P included, runs on `edge`.
pub(crate) async fn build_node_state(
    setup: NodeSetup,
    transport: embedded::TransportConfig,
    options_json: serde_json::Value,
    edge: edge::Edge,
) -> Result<NodeState, String> {
    let NodeSetup {
        store,
        store_file_size,
        store_encryption_key,
        mut config,
        signing_enabled,
        searchable_encryption_key,
        peer_key,
    } = setup;

    // The data path's worker pool comes before the store, which places its
    // compaction threads by the pool's plan.
    let pool = embedded::start_node_pool(None, Some(&edge)).map_err(|error| error.to_string())?;
    let (store, persistence, db_path) =
        open_store(store, store_file_size, store_encryption_key, &pool)?;
    config.persistence = persistence;
    config.transport = with_derived_iroh_key_path(transport, &db_path);
    config.edge = Some(edge);
    if let Some(key) = peer_key {
        config.peer_keyring = Some(embedded::PeerKeyring::new(Arc::new(
            peer_key::CallerPeerKey::new(key),
        )));
    }

    let node = embedded::build_with_store_on_pool(store, config, pool)
        .await
        .map_err(|error| error.to_string())?;

    let state = NodeState {
        database: node.database.clone(),
        background_tasks: node.background_tasks(),
        txn_registry: node.txn_registry.clone(),
        query_runner: node.query_runner.clone(),
        nac_manager: node.nac_manager.clone(),
        document_acp: node.document_acp.clone(),
        event_bus: node.event_bus.clone(),
        policy_store: Arc::new(PolicyStore::new()),
        local_zanzibar_store: node.local_zanzibar_store.clone(),
        p2p: node
            .p2p
            .clone()
            .map(|system| Arc::new(P2PState::new(system))),
        node_identity_did: node
            .node_identity_did
            .clone()
            .map_or_else(AtomOption::none, AtomOption::some),
        signing_enabled,
        #[cfg(feature = "vera")]
        vera_acp: node.vera_acp.clone(),
        query_limits: node.query_limits,
        se_encryption_key: searchable_encryption_key
            .map_or_else(AtomOption::none, AtomOption::some),
        options_json,
    };
    state.sync_replicator_push_options()?;
    Ok(state)
}

/// Opens the node's regolith store, in memory or at its path.
fn open_store(
    location: StoreLocation,
    file_size: Option<u64>,
    encryption_key: Option<Zeroizing<[u8; 32]>>,
    pool: &workers::Pool,
) -> Result<(Arc<FfiStore>, embedded::Persistence, Option<String>), String> {
    let (mut options, persistence, path) = match location {
        StoreLocation::Memory => (
            storage::RegolithStoreOptions::memory(),
            embedded::Persistence::Memory,
            None,
        ),
        StoreLocation::Path(path) => (
            storage::RegolithStoreOptions::default(),
            embedded::Persistence::Persistent,
            Some(path),
        ),
    };
    if let Some(bytes) = file_size {
        options.engine = options.engine.target_file_size(bytes);
    }
    if let Some(key) = encryption_key {
        options = options
            .with_encryption_keys(*key, [])
            .map_err(|error| format!("invalid store_encryption_key: {}", error))?;
    }
    let options = options.placed_on(pool);
    let store = match &path {
        None => storage::RegolithStore::open_with_options("regolith-memory", options)
            .map_err(|error| format!("failed to open in-memory regolith store: {}", error))?,
        Some(path) => storage::RegolithStore::open_with_options(path, options)
            .map_err(|error| format!("failed to open regolith store at '{}': {}", path, error))?,
    };
    Ok((Arc::new(FfiStore::Regolith(store)), persistence, path))
}

fn with_derived_iroh_key_path(
    transport: embedded::TransportConfig,
    _db_path: &Option<String>,
) -> embedded::TransportConfig {
    #[cfg(feature = "iroh")]
    if let embedded::TransportConfig::Iroh(mut config) = transport {
        if config.secret_key_path.is_none() {
            if let Some(path) = _db_path {
                config.secret_key_path = Some(std::path::PathBuf::from(format!("{path}.iroh.key")));
            }
        }
        return embedded::TransportConfig::Iroh(config);
    }

    transport
}

/// Closes the node and releases its handle, with every transaction and
/// subscription still open on it.
#[no_mangle]
pub extern "C" fn defra_node_close(node_ptr: usize) -> DefraResult {
    ffi_entry! {
        use crate::state::GRAPHQL_SUBSCRIPTIONS;

        let rt = try_ffi!(crate::helpers::get_rt());
        try_ffi!(resolve_node(node_ptr));

        for sub_state in GRAPHQL_SUBSCRIPTIONS.remove_for_node(node_ptr) {
            sub_state.service.stop();
            NODES.get(node_ptr, |state| {
                state.event_bus.unsubscribe(sub_state.event_sub_id);
            });
        }
        TXNS.remove_for_node(node_ptr);

        let state = match NODES.remove(node_ptr) {
            Some(state) => state,
            None => return DefraResult::error(ERR_INVALID_HANDLE),
        };

        let result = crate::helpers::block_on(rt, async {
            state.background_tasks.shutdown().await;
            if let Some(p2p) = state.p2p.as_ref() {
                p2p.system.shutdown().await;
            }
            state.event_bus.close();
            state
                .database
                .close()
                .await
                .map_err(|error| format!("failed to close database: {}", error))
        });
        drop(state);

        match result {
            Ok(()) => DefraResult::ok(),
            Err(error) => DefraResult::error(error),
        }
    }
}

/// Returns the node's configuration options as JSON, with every value that
/// could locate or unlock the node redacted.
#[no_mangle]
pub extern "C" fn defra_node_options(node_ptr: usize) -> DefraResult {
    ffi_entry! {
        try_ffi!(resolve_node(node_ptr));
        match NODES.get(node_ptr, |state| state.options_json.to_string()) {
            Some(json) => DefraResult::success(json),
            None => DefraResult::error(ERR_INVALID_HANDLE),
        }
    }
}
