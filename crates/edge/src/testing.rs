//! One network runtime a test binary's tests share, for tests of
//! components that take an [`Edge`] at construction.

use std::num::NonZeroUsize;

use regolith::sync::Lazy;

use crate::{Edge, EdgeConfig, EdgeRuntime};

static RUNTIME: Lazy<EdgeRuntime> = Lazy::new(start);

fn start() -> EdgeRuntime {
    EdgeRuntime::start(EdgeConfig {
        threads: NonZeroUsize::new(2),
        thread_name: "defra-edge-test".to_string(),
        ..EdgeConfig::default()
    })
    .expect("the test network runtime starts")
}

/// A handle onto the runtime this test binary shares, started on first
/// use and stopped when the process exits.
pub fn edge() -> Edge {
    RUNTIME.edge().clone()
}
