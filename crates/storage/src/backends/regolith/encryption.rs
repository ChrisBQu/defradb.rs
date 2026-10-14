//! The node's preloaded at-rest key, exposed to the engine without I/O.

use regolith::{KeyId, KeyMaterial, KeyProvider};

/// The existing keyring stores one at-rest key. Its id is stable across
/// opens; changing the key requires rewriting the store, not reusing its id.
const AT_REST_KEY: KeyId = KeyId(1);

pub(super) struct AtRestKeyProvider(KeyMaterial);

impl AtRestKeyProvider {
    pub(super) fn new(key: [u8; 32]) -> Self {
        Self(KeyMaterial::new(key))
    }
}

impl KeyProvider for AtRestKeyProvider {
    fn current(&self) -> KeyId {
        AT_REST_KEY
    }

    fn key(&self, id: KeyId) -> Option<KeyMaterial> {
        (id == AT_REST_KEY).then(|| self.0.clone())
    }
}
