//! BitswapStore adapter for DefraBlockstore.
//!
//! This module implements the `Store` trait from iroh-bitswap
//! for our async `DefraBlockstore`. The trait methods are async, which maps
//! directly to our async blockstore interface.
//!
//! # Go Compatibility
//!
//! Go DefraDB uses Bitswap for block exchange. This adapter enables the same
//! block exchange protocol in Rust, ensuring interoperability.

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use iroh_bitswap::{Block, Store};

use blockstore::Blockstore;

use crate::Error;

/// Adapter that implements iroh_bitswap::Store for any `Blockstore`.
///
/// The Store trait from iroh-bitswap uses async methods,
/// which maps directly to our async DefraBlockstore interface.
///
/// # Thread Safety
///
/// This adapter is thread-safe and can be cloned. The underlying blockstore
/// is shared via `Arc`.
#[derive(Debug)]
pub struct BitswapStoreAdapter<B: Blockstore> {
    blockstore: Arc<B>,
}

impl<B: Blockstore> Clone for BitswapStoreAdapter<B> {
    fn clone(&self) -> Self {
        Self {
            blockstore: self.blockstore.clone(),
        }
    }
}

impl<B: Blockstore> BitswapStoreAdapter<B> {
    /// Create a new BitswapStore adapter.
    ///
    /// # Arguments
    ///
    /// * `blockstore` - The underlying async blockstore
    pub fn new(blockstore: Arc<B>) -> Self {
        Self { blockstore }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<B: Blockstore + Debug + 'static> Store for BitswapStoreAdapter<B> {
    /// Get the size of a block by CID.
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        Ok(self
            .blockstore
            .get_size(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))?
            .ok_or_else(|| Error::BlockNotFound(cid.to_string()))?)
    }

    /// Get block data by CID.
    async fn get(&self, cid: &Cid) -> Result<Block> {
        let data = self
            .blockstore
            .get(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))?
            .ok_or_else(|| Error::BlockNotFound(cid.to_string()))?;
        Ok(Block::new(data, *cid))
    }

    /// Check if the blockstore contains a block with the given CID.
    async fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(self
            .blockstore
            .has(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))?)
    }
}
