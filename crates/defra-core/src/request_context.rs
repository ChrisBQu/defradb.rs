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

/// What one request carries: who acts, with which privileges, and how its
/// writes are encrypted.
///
/// The default context is an anonymous request with nothing granted.
#[derive(Clone, Default)]
pub struct RequestContext(Arc<Fields>);

#[derive(Clone, Default)]
struct Fields {
    dac_bypass: bool,
    encryption: Option<EncryptionConfig>,
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
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("dac_bypass", &self.0.dac_bypass)
            .field("encryption", &self.0.encryption)
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
