//! A node's transport runs on the network runtime its config names; a node
//! without networking needs none.

#![cfg(feature = "libp2p")]

use std::sync::Arc;

use anyhow::Result;
use embedded::{EmbeddedNodeConfig, Libp2pConfig, TransportConfig};

fn memory_store() -> Result<storage::RegolithStore> {
    Ok(storage::RegolithStore::open_with_options(
        "memory",
        storage::RegolithStoreOptions::memory(),
    )?)
}

fn libp2p() -> TransportConfig {
    TransportConfig::Libp2p(Libp2pConfig {
        listen_addr: "/ip4/127.0.0.1/tcp/0".to_string(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_without_a_network_runtime_is_refused() -> Result<()> {
    let config = EmbeddedNodeConfig {
        transport: libp2p(),
        ..Default::default()
    };

    let error = embedded::build_with_store(Arc::new(memory_store()?), config)
        .await
        .err()
        .expect("a transport without a network runtime must not start");

    assert!(
        error.to_string().contains("set EmbeddedNodeConfig::edge"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_runs_on_the_network_runtime_it_is_given() -> Result<()> {
    let config = EmbeddedNodeConfig {
        transport: libp2p(),
        edge: Some(edge::testing::edge()),
        ..Default::default()
    };

    let node = embedded::build_with_store(Arc::new(memory_store()?), config).await?;

    node.shutdown().await;
    Ok(())
}
