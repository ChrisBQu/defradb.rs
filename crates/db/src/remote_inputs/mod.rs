//! The remote inputs a write fetches before its transaction, each through
//! its own `inputs` service: remote signatures and embeddings. (KMS keys
//! have theirs inside the KMS; ACP decisions inside the ACP provider.)
//!
//! The services run their round trips on the network runtime the node gives
//! the database ([`DB::set_network`](crate::DB::set_network)). Without one,
//! signatures run in the requesting task (a signer brings its own runtime,
//! or none), and embeddings, which are HTTP requests, go through the
//! process's shared embedding service on its on-demand network runtime
//! ([`embeddings::shared`]). The services are built on first use, so the
//! runtime is set before the first write that needs one.

mod embeddings;
mod signatures;

pub(crate) use embeddings::{embedding_error, shared as shared_embeddings, Embedding, Embeddings};
pub(crate) use signatures::{RemoteSignatures, Signature, Signer};

use regolith::sync::OnceCell;

/// A database's remote-input services.
#[derive(Default)]
pub(crate) struct RemoteInputs {
    runtime: OnceCell<inputs::Runtime>,
    signatures: OnceCell<inputs::Service<RemoteSignatures>>,
    embeddings: OnceCell<inputs::Service<Embeddings>>,
}

impl RemoteInputs {
    /// Runs every round trip on `runtime`. Only before the first one: a
    /// service already built keeps its runtime. Returns whether it took.
    pub(crate) fn set_runtime(&self, runtime: inputs::Runtime) -> bool {
        self.runtime.set(runtime).is_ok()
    }

    /// The network runtime the node gave, else inline.
    fn runtime(&self) -> inputs::Runtime {
        self.runtime
            .get()
            .cloned()
            .unwrap_or_else(inputs::Runtime::inline)
    }

    /// Remote signatures, one call per wave and signer.
    pub(crate) fn signatures(&self) -> &inputs::Service<RemoteSignatures> {
        self.signatures.get_or_init_racy(|| {
            inputs::Service::new(RemoteSignatures, signatures::config(), self.runtime())
        })
    }

    /// Embeddings, single-flighted and cached by text: on the network
    /// runtime the node gave, else through the process's shared service.
    pub(crate) fn embeddings(&self) -> &inputs::Service<Embeddings> {
        self.embeddings
            .get_or_init_racy(|| match self.runtime.get() {
                Some(runtime) => {
                    inputs::Service::new(Embeddings, embeddings::config(), runtime.clone())
                }
                None => embeddings::shared(),
            })
    }
}
