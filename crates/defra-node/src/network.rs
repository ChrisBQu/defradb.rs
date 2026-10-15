//! The node's share of the network runtime.
//!
//! A node runs its network work (signed queries, the HTTP server, P2P) on
//! the runtime the application passes to
//! [`NodeBuilder::with_edge`](crate::NodeBuilder::with_edge). Given none,
//! it starts one itself, and only when it has network work to run: a node
//! with no identity, no HTTP server and no P2P starts no runtime at all.
//! Either way the node's work goes through a scope of its own, so shutting
//! the node down stops accepting and drains the node's work alone, then
//! stops a runtime the node started.

use std::time::Duration;

use edge::{Edge, EdgeConfig, EdgeRuntime, Scope};
use kovan_queue::seg_queue::SegQueue;

/// How long shutdown waits for the node's in-flight network work.
pub(crate) const NETWORK_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct NodeEdge {
    scope: Scope,
    owned: SegQueue<EdgeRuntime>,
}

impl NodeEdge {
    /// The node's network handle, a scope of `given` or of a runtime the
    /// node starts, or `None` when the node has no network work and was
    /// given no runtime.
    pub(crate) fn start(given: Option<Edge>, needed: bool) -> anyhow::Result<Option<Self>> {
        let owned = SegQueue::new();
        let edge = match given {
            Some(edge) => edge,
            None if needed => {
                let runtime = EdgeRuntime::start(EdgeConfig::default())
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                let edge = runtime.edge().clone();
                owned.push(runtime);
                edge
            }
            None => return Ok(None),
        };
        Ok(Some(Self {
            scope: edge.scope(),
            owned,
        }))
    }

    /// The handle the node's network work is submitted through.
    pub(crate) fn edge(&self) -> &Edge {
        self.scope.edge()
    }

    /// Stops accepting the node's network work, which stops its services,
    /// then waits up to [`NETWORK_DRAIN_TIMEOUT`] for the rest.
    pub(crate) async fn drain(&self) {
        let drained = self.scope.drain(NETWORK_DRAIN_TIMEOUT).await;
        if !drained.is_complete() {
            tracing::warn!(
                remaining = drained.remaining,
                timeout_secs = NETWORK_DRAIN_TIMEOUT.as_secs(),
                "timed out draining the node's network work during shutdown"
            );
        }
    }

    /// Shuts down the runtime the node started, if it started one.
    pub(crate) async fn stop_owned(&self) {
        let Some(runtime) = self.owned.pop() else {
            return;
        };
        if let Err(error) = runtime.shutdown_async().await {
            tracing::warn!(%error, "the node's network runtime did not shut down cleanly");
        }
    }
}
