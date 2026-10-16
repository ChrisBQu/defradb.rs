//! The one module that names `anyhow`: the external bitswap `Store` trait returns `anyhow::Result`.
//!
//! A store implements the typed methods `block_size`, `block` and `contains`, each returning p2p's
//! own `Result`, and invokes [`impl_bitswap_store`] to get its `Store` impl.

macro_rules! impl_bitswap_store {
    ([$($generics:tt)*] $ty:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<$($generics)*> iroh_bitswap::Store for $ty {
            async fn get_size(&self, cid: &cid::Cid) -> anyhow::Result<usize> {
                Ok(self.block_size(cid).await?)
            }

            async fn get(&self, cid: &cid::Cid) -> anyhow::Result<iroh_bitswap::Block> {
                Ok(self.block(cid).await?)
            }

            async fn has(&self, cid: &cid::Cid) -> anyhow::Result<bool> {
                Ok(self.contains(cid).await?)
            }
        }
    };
}

pub(crate) use impl_bitswap_store;
