use std::sync::Arc;

use defra_core::signing::{SigningConfig, SigningKeyType};
use identity::{Identity, RawIdentity};
use kovan_map::HopscotchMap;
use rapidhash::fast::RandomState;
use regolith::sync::Lazy;
use zeroize::Zeroizing;

/// Type alias for identity handles (opaque to FFI callers).
pub type IdentityHandle = usize;

/// An identity a C caller holds a handle to: a key pair, or a public key
/// whose private key the host keeps, and the DID its public key derives.
pub struct IdentityState {
    /// The identity's `did:key`.
    pub did: String,
    /// The identity's key type; `None` for one a bearer token stands for.
    pub key_type: Option<SigningKeyType>,
    /// The raw private key; `None` when the host keeps it (a remote signer).
    pub private_key: Option<Zeroizing<Vec<u8>>>,
    /// The hex-encoded public key.
    pub public_key_hex: String,
}

impl IdentityState {
    /// The state of `identity`, also registered in the signing store under
    /// its DID, which is how a request names the identity that signs it.
    pub fn register(identity: &RawIdentity) -> Result<Self, String> {
        let did = identity
            .did()
            .map_err(|error| format!("failed to derive DID: {}", error))?
            .to_string();
        let key_type = match identity.key_type() {
            crypto::KeyType::Ed25519 => SigningKeyType::Ed25519,
            crypto::KeyType::Secp256k1 => SigningKeyType::Secp256k1,
            crypto::KeyType::Secp256r1 => SigningKeyType::Secp256r1,
            other => return Err(format!("unsupported identity key type: {}", other)),
        };
        let private_key = Zeroizing::new(identity.private_key_bytes());
        let public_key_bytes = identity.public_key_bytes();
        let public_key_hex = hex::encode(&public_key_bytes);

        defra_core::signing::store_identity(
            &did,
            SigningConfig {
                key_type,
                private_key_bytes: SigningConfig::private_key_bytes_from_vec(private_key.to_vec()),
                public_key_bytes,
                public_key_hex: public_key_hex.clone(),
                remote_signer: None,
                signing_authorization: None,
            },
        );

        Ok(Self {
            did,
            key_type: Some(key_type),
            private_key: Some(private_key),
            public_key_hex,
        })
    }
}

/// Global registry of identities.
pub struct IdentityRegistry {
    identities: HopscotchMap<IdentityHandle, Arc<IdentityState>, RandomState>,
}

impl IdentityRegistry {
    fn new() -> Self {
        Self {
            identities: HopscotchMap::with_hasher(RandomState::default()),
        }
    }

    /// Insert an identity and return its handle.
    pub fn insert(&self, state: IdentityState) -> IdentityHandle {
        let handle = super::handle::next_handle();
        self.identities.insert(handle, Arc::new(state));
        handle
    }

    /// The identity a handle names, if it names one.
    pub fn get(&self, handle: IdentityHandle) -> Option<Arc<IdentityState>> {
        self.identities.get(&handle)
    }

    /// Remove and return an identity.
    pub fn remove(&self, handle: IdentityHandle) -> Option<Arc<IdentityState>> {
        self.identities.remove(&handle)
    }
}

/// Global IDENTITIES registry.
pub static IDENTITIES: Lazy<IdentityRegistry> = Lazy::new(IdentityRegistry::new);
