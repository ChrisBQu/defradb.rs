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

/// What one request carries: who acts and with which privileges.
///
/// The default context is an anonymous request with nothing granted.
#[derive(Clone, Default)]
pub struct RequestContext(Arc<Fields>);

#[derive(Clone, Default)]
struct Fields {
    dac_bypass: bool,
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
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestContext")
            .field("dac_bypass", &self.0.dac_bypass)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_context_grants_nothing() {
        assert!(!RequestContext::default().dac_bypass());
    }

    #[test]
    fn with_dac_bypass_leaves_the_context_it_was_cloned_from_unchanged() {
        let anonymous = RequestContext::default();
        let admin = anonymous.clone().with_dac_bypass(true);
        assert!(admin.dac_bypass());
        assert!(!anonymous.dac_bypass());
    }
}
