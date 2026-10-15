//! The state one request carries down its call chain.
//!
//! A boundary (HTTP, the embedded API, FFI, the CLI, a P2P handler) builds one
//! [`RequestContext`] per request and hands it down as a value: inside the
//! request message, bound into the handle that serves the request, or as an
//! argument. Nothing about a request lives in a thread-local, a task-local or a
//! process global, so a task built on one thread and run on another, or two
//! requests interleaved on one thread, each see exactly their own request.
//!
//! A context is immutable once built and cheap to clone: the fields sit behind
//! one `Arc`, and each `with_*` call copies them only when the context is
//! shared.

use std::fmt;
use std::sync::Arc;

use crate::encryption::EncryptionConfig;
use crate::signing::SigningConfig;

/// What one request carries: who acts, with which privileges, and how its
/// writes are encrypted and signed.
///
/// The default context is an anonymous request with nothing granted.
#[derive(Clone, Default)]
pub struct RequestContext(Arc<Fields>);

#[derive(Clone, Default)]
struct Fields {
    dac_bypass: bool,
    encryption: Option<EncryptionConfig>,
    signing: Option<SigningConfig>,
    batch_session: Option<String>,
    creator: Option<String>,
}

impl RequestContext {
    /// Whether node access control grants this request's identity the
    /// `bypass-dac` permission, so document access control lets an
    /// authenticated caller read and write every document. Resolved once, at
    /// the boundary, for the request's identity.
    pub fn dac_bypass(&self) -> bool {
        self.0.dac_bypass
    }

    /// This context with the DAC bypass set to `bypass`.
    #[must_use]
    pub fn with_dac_bypass(mut self, bypass: bool) -> Self {
        Arc::make_mut(&mut self.0).dac_bypass = bypass;
        self
    }

    /// Which fields of the documents a mutation writes it encrypts, when the
    /// mutation asks for encryption. Without one a write inherits each
    /// field's encryption from its previous block.
    pub fn encryption(&self) -> Option<&EncryptionConfig> {
        self.0.encryption.as_ref()
    }

    /// This context with the encryption set to `encryption`.
    #[must_use]
    pub fn with_encryption(mut self, encryption: Option<EncryptionConfig>) -> Self {
        Arc::make_mut(&mut self.0).encryption = encryption;
        self
    }

    /// The key this request signs the blocks it writes with; unsigned blocks
    /// without one.
    pub fn signing(&self) -> Option<&SigningConfig> {
        self.0.signing.as_ref()
    }

    /// This context with the signing set to `signing`.
    #[must_use]
    pub fn with_signing(mut self, signing: Option<SigningConfig>) -> Self {
        Arc::make_mut(&mut self.0).signing = signing;
        self
    }

    /// The batch signing session the CIDs of the blocks this request commits
    /// are collected into (see [`crate::batch_signing`]), if it runs in one.
    pub fn batch_session(&self) -> Option<&str> {
        self.0.batch_session.as_deref()
    }

    /// This context with the batch signing session set to `session`.
    #[must_use]
    pub fn with_batch_session(mut self, session: Option<String>) -> Self {
        Arc::make_mut(&mut self.0).batch_session = session;
        self
    }

    /// The DID that creates what this request writes: document ACP registers
    /// it as the owner of the documents it creates, and P2P broadcasts name it
    /// as their creator, so a receiving node registers the same owner. P2P
    /// names the node's peer ID instead when there is none.
    pub fn creator(&self) -> Option<&str> {
        self.0.creator.as_deref()
    }

    /// This context with the creator set to `creator`.
    #[must_use]
    pub fn with_creator(mut self, creator: Option<String>) -> Self {
        Arc::make_mut(&mut self.0).creator = creator;
        self
    }
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("dac_bypass", &self.0.dac_bypass)
            .field("encryption", &self.0.encryption)
            .field(
                "signing",
                &self.0.signing.as_ref().map(|signing| signing.key_type),
            )
            .field("batch_session", &self.0.batch_session)
            .field("creator", &self.0.creator)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_context_grants_nothing() {
        let context = RequestContext::default();
        assert!(!context.dac_bypass());
        assert!(context.encryption().is_none());
        assert!(context.signing().is_none());
        assert!(context.batch_session().is_none());
        assert!(context.creator().is_none());
    }

    #[test]
    fn debug_never_shows_the_signing_key() {
        let context = RequestContext::default().with_signing(Some(SigningConfig {
            key_type: crate::signing::SigningKeyType::Ed25519,
            private_key_bytes: vec![0xAB; 32],
            public_key_bytes: Vec::new(),
            public_key_hex: "public".to_string(),
            remote_signer: None,
            signing_authorization: None,
        }));
        let shown = format!("{context:?}");
        assert!(shown.contains("Ed25519"));
        assert!(!shown.contains("171"), "{shown}");
    }

    #[test]
    fn with_encryption_sets_and_clears_the_encryption() {
        let encrypted = RequestContext::default().with_encryption(Some(EncryptionConfig {
            encrypt_doc: false,
            encrypt_fields: vec!["secret".to_string()],
        }));
        assert!(encrypted
            .encryption()
            .is_some_and(|config| config.should_encrypt_field("secret")));
        assert!(encrypted.with_encryption(None).encryption().is_none());
    }

    #[test]
    fn with_dac_bypass_leaves_the_context_it_was_cloned_from_unchanged() {
        let anonymous = RequestContext::default();
        let admin = anonymous.clone().with_dac_bypass(true);
        assert!(admin.dac_bypass());
        assert!(!anonymous.dac_bypass());
    }
}
