//! Embeddings as an [`inputs::Source`]: single-flight and a byte-bounded
//! cache keyed by (provider, normalized URL, model, SHA-256 of the text), a
//! deadline on every target, and retries on 429, 5xx and transport errors.
//!
//! Each text is its own request, all of a wave in flight at once under the
//! cap: a wave takes one round trip of time, whatever its size.
//!
//! A model replaced under the same name keeps serving the old vectors from
//! the cache until their time to live ends.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use defra_core::thread_bounds::MaybeBoxFuture;
use sha2::{Digest, Sha256};

use crate::search::embedding::{normalized_embedding_url, EmbeddingCallError, EmbeddingInput};

/// One embedding: what it is keyed by, and the request that makes it.
#[derive(Clone)]
pub(crate) struct Embedding {
    endpoint: Endpoint,
    digest: [u8; 32],
    input: Arc<EmbeddingInput>,
}

impl Embedding {
    pub(crate) fn new(input: EmbeddingInput) -> Self {
        Self {
            endpoint: Endpoint {
                provider: Arc::from(input.provider.as_str()),
                url: Arc::from(normalized_embedding_url(&input.url)),
                model: Arc::from(input.model.as_str()),
            },
            digest: Sha256::digest(input.text.as_bytes()).into(),
            input: Arc::new(input),
        }
    }
}

impl PartialEq for Embedding {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint && self.digest == other.digest
    }
}

impl Eq for Embedding {}

impl Hash for Embedding {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.endpoint.hash(state);
        self.digest.hash(state);
    }
}

/// Where an embedding is made: provider, normalized URL and model.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Endpoint {
    provider: Arc<str>,
    url: Arc<str>,
    model: Arc<str>,
}

pub(crate) struct Embeddings;

pub(super) fn config() -> inputs::Config {
    inputs::Config {
        // Past one request's own timeout, so a retry fits.
        deadline: Duration::from_secs(75),
        attempts: 3,
        backoff: Duration::from_millis(250),
        max_backoff: Duration::from_secs(5),
        max_batch: 1,
        flights_per_group: 32,
        max_in_flight: 32,
        cache_bytes: 16 << 20,
        ttl: Some(Duration::from_secs(600)),
    }
}

impl inputs::Source for Embeddings {
    type Key = Embedding;
    type Group = Endpoint;
    type Value = Arc<[f64]>;
    type Error = EmbeddingCallError;

    fn name(&self) -> &'static str {
        "embeddings"
    }

    fn group(&self, key: &Embedding) -> Endpoint {
        key.endpoint.clone()
    }

    fn fetch<'a>(
        &'a self,
        _endpoint: &'a Endpoint,
        keys: &'a [Embedding],
    ) -> MaybeBoxFuture<'a, inputs::Answers<Arc<[f64]>, EmbeddingCallError>> {
        Box::pin(async move {
            let answers = futures::future::join_all(keys.iter().map(|key| key.input.call())).await;
            // Every request failed in a way worth another try: the round
            // trip as a whole goes again.
            if let Some(error) = answers
                .iter()
                .map(|answer| answer.as_ref().err().filter(|error| error.retryable))
                .collect::<Option<Vec<_>>>()
                .and_then(|errors| errors.first().map(|error| (*error).clone()))
            {
                return Err(error);
            }
            Ok(answers
                .into_iter()
                .map(|answer| answer.map(Arc::from))
                .collect())
        })
    }

    fn retryable(&self, error: &EmbeddingCallError) -> bool {
        error.retryable
    }

    fn cache_bytes(&self, key: &Embedding, value: &Arc<[f64]>) -> Option<usize> {
        Some(
            value.len() * std::mem::size_of::<f64>()
                + key.endpoint.provider.len()
                + key.endpoint.url.len()
                + key.endpoint.model.len()
                + 32,
        )
    }
}
