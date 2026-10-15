//! IPFS-compatible blockstore with merge tracking for DefraDB
//!
//! This crate provides a public API layer for IPLD block storage with
//! CRDT merge tracking support for P2P synchronization.
//!
//! # Architecture
//!
//! ```text
//! Application Layer
//!     ↓
//! Blockstore Trait (this crate - public API)
//!     ↓
//! storage::stores::Blockstore (internal implementation)
//!     ↓
//! CoreKV Backend (Memory/RocksDB)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use blockstore::{Blockstore, DefraBlockstore};
//! use storage::RegolithStore;
//! use std::sync::Arc;
//!
//! // Create a blockstore
//! let store = Arc::new(RegolithStore::in_memory().unwrap());
//! let blockstore = DefraBlockstore::new(store, false);
//!
//! // Store a block
//! let cid = /* compute CID from data */;
//! blockstore.put(&cid, b"block data").await?;
//!
//! // Retrieve a block
//! let data = blockstore.get(&cid).await?;
//!
//! // Enable hash verification on read
//! blockstore.hash_on_read(true);
//! let verified_data = blockstore.get(&cid).await?; // Will verify hash
//! ```

mod error;
mod traits;
mod verify;

pub use error::{Error, Result};
pub use traits::Blockstore;
pub use verify::verify_block_cid;

use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use multihash_codetable::Code;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::corekv::{IterOptions, Key, RetryOptions, Store};
use storage::keys::blockstore::{BlockstoreKey, ToMergeIndexKey};
use storage::stores::blockstore::BlockstoreTxn;
use storage::stores::BlockWrite;
use storage::stores::Blockstore as InternalBlockstore;
use tracing::instrument;

/// DefraDB blockstore implementation
///
/// Wraps the internal storage::stores::Blockstore with a clean public API.
/// Supports both P2P mode (with merge tracking) and local mode (no tracking).
///
/// It keeps no cache of its own: every read goes to the engine, whose block
/// cache and zero-copy reads serve a repeated read, so every handle over one
/// store sees the same blocks and merge marks.
pub struct DefraBlockstore<S: Store> {
    store: InternalBlockstore<S>,
    /// Whether to verify hash on read
    rehash: AtomicBool,
    /// The engine retry policy every block write runs under.
    retry: RetryOptions,
}

impl<S: Store> fmt::Debug for DefraBlockstore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefraBlockstore")
            .field("rehash", &self.rehash.load(Ordering::Relaxed))
            .finish()
    }
}

impl<S: Store + 'static> DefraBlockstore<S> {
    /// Create a new blockstore
    ///
    /// # Arguments
    ///
    /// * `store` - The underlying key-value store
    /// * `is_p2p` - If true, enables merge tracking for P2P synchronization.
    ///   Blocks put in P2P mode are initially marked as "unmerged"
    ///   until explicitly merged via `mark_as_merged`.
    pub fn new(store: Arc<S>, is_p2p: bool) -> Self {
        Self {
            store: InternalBlockstore::new(store, is_p2p),
            rehash: AtomicBool::new(false),
            retry: storage::corekv::engine_retry_options(storage::corekv::DEFAULT_MAX_TXN_RETRIES),
        }
    }

    /// Re-run a conflicting block write up to `max_txn_retries` times, the
    /// node's `datastore.max_txn_retries`.
    pub fn with_max_txn_retries(mut self, max_txn_retries: u32) -> Self {
        self.retry = storage::corekv::engine_retry_options(max_txn_retries);
        self
    }

    /// Check if hash verification on read is currently enabled
    pub fn rehash_enabled(&self) -> bool {
        self.rehash.load(Ordering::Relaxed)
    }

    /// Create a new transaction on the underlying store (for testing)
    pub async fn new_store_txn(&self, read_only: bool) -> Result<Box<dyn storage::corekv::Txn>> {
        Ok(self.store.new_txn(read_only).await?)
    }

    /// Verify that data matches the CID hash
    ///
    /// # Supported Algorithms
    ///
    /// Currently supports SHA2-256 (code 0x12), which is the most common hash
    /// algorithm used in IPFS/IPLD and DefraDB.
    ///
    /// # Go Compatibility Note
    ///
    /// The Go implementation uses `cid.Prefix().Sum(data)` which delegates to the
    /// go-multihash library and supports all registered hash algorithms. This Rust
    /// implementation explicitly handles SHA2-256 only. Unsupported algorithms are
    /// logged and skipped (verification passes) rather than erroring, matching the
    /// principle of being permissive on read. If DefraDB ever uses non-SHA256 hashes,
    /// this function should be extended to support them.
    fn verify_hash(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        use sha2::{Digest, Sha256};

        let mh = cid.hash();
        let code = mh.code();

        // Compute hash based on the multihash code
        if code == u64::from(Code::Sha2_256) {
            let mut hasher = Sha256::new();
            hasher.update(data);
            let computed_digest = hasher.finalize();

            if mh.digest() != computed_digest.as_slice() {
                return Err(Error::HashMismatch {
                    cid: cid.to_string(),
                });
            }
            Ok(())
        } else {
            tracing::warn!(
                hash_code = code,
                cid = %cid,
                "Hash verification skipped: unsupported hash algorithm"
            );
            Ok(())
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> Blockstore for DefraBlockstore<S> {
    #[instrument(level = "trace", skip(self))]
    async fn get(&self, cid: &Cid) -> Result<Option<Bytes>> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.get_block(cid).await?;

        if self.rehash.load(Ordering::Acquire) {
            if let Some(ref data) = result {
                self.verify_hash(cid, data)?;
            }
        }

        Ok(result)
    }

    #[instrument(level = "trace", skip(self))]
    async fn put(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        self.store
            .write_blocks(
                self.retry,
                vec![BlockWrite::Put {
                    cid: *cid,
                    data: Bytes::copy_from_slice(data),
                }],
            )
            .await?;
        Ok(())
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let writes = blocks
            .iter()
            .map(|(cid, data)| BlockWrite::Put {
                cid: **cid,
                data: Bytes::copy_from_slice(data),
            })
            .collect();
        self.store.write_blocks(self.retry, writes).await?;
        Ok(())
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.has_block(cid).await?;
        Ok(result)
    }

    async fn delete(&self, cid: &Cid) -> Result<()> {
        self.store
            .write_blocks(self.retry, vec![BlockWrite::Delete(*cid)])
            .await?;
        Ok(())
    }

    async fn get_size(&self, cid: &Cid) -> Result<Option<usize>> {
        let txn = self.store.new_txn(true).await?;
        let block_key = BlockstoreKey::new(*cid);
        let result = txn.get_size(&block_key.bytes()).await?;
        Ok(result)
    }

    async fn all_cids(&self) -> Result<Vec<Cid>> {
        let txn = self.store.new_txn(true).await?;

        // Iterate all keys that don't start with merge prefix
        // Block keys are raw CID bytes, merge keys start with 'm'
        let opts = IterOptions::new().with_keys_only(true);
        let mut iter = txn.iterator(opts).await?;

        let mut cids = Vec::new();
        while let Some(pair) = iter.next().await? {
            // Skip merge marker keys (start with 'm')
            if ToMergeIndexKey::is_merge_key(&pair.key) {
                continue;
            }

            // Parse the key as a CID
            match BlockstoreKey::from_bytes(&pair.key) {
                Ok(key) => cids.push(key.cid),
                Err(e) => {
                    tracing::warn!(
                        key_bytes = ?pair.key,
                        error = %e,
                        "Skipping key that could not be parsed as CID"
                    );
                }
            }
        }

        Ok(cids)
    }

    fn hash_on_read(&self, enabled: bool) {
        self.rehash.store(enabled, Ordering::Release);
    }

    async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;

        let has_block = bs_txn.has_block(cid).await?;
        if !has_block {
            return Ok(false);
        }

        Ok(bs_txn.is_merged(cid).await?)
    }

    async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.store
            .write_blocks(self.retry, vec![BlockWrite::MarkMerged(*cid)])
            .await?;
        Ok(())
    }

    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }
        let writes = cids.iter().copied().map(BlockWrite::MarkMerged).collect();
        self.store.write_blocks(self.retry, writes).await?;
        Ok(())
    }

    async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.get_unmerged_cids().await?;
        Ok(result)
    }
}
