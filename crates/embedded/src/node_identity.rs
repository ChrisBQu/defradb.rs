use crate::error::{Error, Result, ResultExt};
use identity::Identity;

use crate::{SigningConfig, SigningKey};

pub(crate) fn create_node_identity(
    config: &SigningConfig,
) -> Result<(Option<identity::RawIdentity>, Option<String>)> {
    match config {
        SigningConfig::Disabled => Ok((None, None)),
        SigningConfig::Enabled { key } => {
            let raw_identity = match key {
                Some(SigningKey::Secp256k1(bytes)) => {
                    let private_key = crypto::Secp256k1PrivateKey::from_bytes(bytes)
                        .context("failed to load secp256k1 key")?;
                    identity::RawIdentity::from_secp256k1(private_key)
                        .context("failed to create node identity")?
                }
                Some(SigningKey::Secp256r1(bytes)) => {
                    let private_key = crypto::Secp256r1PrivateKey::from_bytes(bytes)
                        .context("failed to load secp256r1 key")?;
                    identity::RawIdentity::from_secp256r1(private_key)
                        .context("failed to create node identity")?
                }
                Some(SigningKey::Ed25519(bytes)) => {
                    let private_key = crypto::Ed25519PrivateKey::from_bytes(bytes)
                        .context("failed to load ed25519 key")?;
                    identity::RawIdentity::from_ed25519(private_key)
                        .context("failed to create node identity")?
                }
                None => {
                    let private_key = crypto::generate_secp256k1()
                        .context("failed to generate node signing key")?;
                    identity::RawIdentity::from_secp256k1(private_key)
                        .context("failed to create node identity")?
                }
            };

            let did = raw_identity.did().context("failed to derive node DID")?;
            let did_str = did.to_string();
            let key_type = match key {
                Some(SigningKey::Ed25519(_)) => defra_core::signing::SigningKeyType::Ed25519,
                Some(SigningKey::Secp256r1(_)) => defra_core::signing::SigningKeyType::Secp256r1,
                _ => defra_core::signing::SigningKeyType::Secp256k1,
            };

            defra_core::signing::store_identity(
                &did_str,
                defra_core::signing::SigningConfig {
                    key_type,
                    private_key_bytes:
                        defra_core::signing::SigningConfig::private_key_bytes_from_vec(
                            raw_identity.private_key_bytes(),
                        ),
                    public_key_bytes: raw_identity.public_key_bytes().to_vec(),
                    public_key_hex: hex::encode(raw_identity.public_key_bytes()),
                    remote_signer: None,
                    signing_authorization: None,
                },
            );

            Ok((Some(raw_identity), Some(did_str)))
        }
        SigningConfig::RegisteredIdentity { did } => {
            let stored = defra_core::signing::get_identity(did).ok_or_else(|| {
                Error::msg(format_args!(
                    "no signing identity registered for DID: {}",
                    did
                ))
            })?;
            if !stored.has_local_private_key() && !stored.has_remote_signer() {
                return Err(Error::msg(format_args!(
                    "registered identity {} has neither a local key nor a remote signer",
                    did
                )));
            }

            let raw_identity = if stored.has_local_private_key() {
                let raw_identity = raw_identity_from_stored_config(&stored)?;
                let derived_did = raw_identity
                    .did()
                    .context("failed to derive stored identity DID")?;
                if derived_did.as_str() != did {
                    return Err(Error::msg(format_args!(
                        "registered identity DID mismatch: expected {}, derived {}",
                        did, derived_did
                    )));
                }
                Some(raw_identity)
            } else {
                None
            };

            Ok((raw_identity, Some(did.clone())))
        }
    }
}

pub(crate) fn raw_identity_from_stored_config(
    config: &defra_core::signing::SigningConfig,
) -> Result<identity::RawIdentity> {
    match config.key_type {
        defra_core::signing::SigningKeyType::Ed25519 => {
            let private_key = crypto::Ed25519PrivateKey::from_bytes(&config.private_key_bytes)
                .context("failed to load stored ed25519 key")?;
            identity::RawIdentity::from_ed25519(private_key)
                .context("failed to create stored ed25519 identity")
        }
        defra_core::signing::SigningKeyType::Secp256k1 => {
            let private_key = crypto::Secp256k1PrivateKey::from_bytes(&config.private_key_bytes)
                .context("failed to load stored secp256k1 key")?;
            identity::RawIdentity::from_secp256k1(private_key)
                .context("failed to create stored secp256k1 identity")
        }
        defra_core::signing::SigningKeyType::Secp256r1 => {
            let private_key = crypto::Secp256r1PrivateKey::from_bytes(&config.private_key_bytes)
                .context("failed to load stored secp256r1 key")?;
            identity::RawIdentity::from_secp256r1(private_key)
                .context("failed to create stored secp256r1 identity")
        }
        defra_core::signing::SigningKeyType::BlsAugV1 => Err(Error::msg("stored identity bls cannot be used as a node identity")),
        other => Err(Error::msg(format_args!(
            "stored identity {} cannot be used as a node identity",
            other
        ))),
    }
}
