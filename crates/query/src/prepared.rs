use std::sync::{Arc, Mutex};

use chrono::{DateTime, FixedOffset};
use document::{Document, WritePreparation};
use rapidhash::RapidHashMap;

/// A network input request, represented by its complete wire request bytes.
/// Keeping the exact bytes in the key avoids accidental reuse when any
/// dependency (including provider configuration) changes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PreparedInputRequest {
    kind: PreparedInputKind,
    request: Arc<[u8]>,
}

impl PreparedInputRequest {
    pub fn new(kind: PreparedInputKind, exact_request: impl Into<Arc<[u8]>>) -> Self {
        Self {
            kind,
            request: exact_request.into(),
        }
    }

    pub fn kind(&self) -> PreparedInputKind {
        self.kind
    }

    pub fn exact_request(&self) -> &[u8] {
        &self.request
    }
}

impl std::fmt::Debug for PreparedInputRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedInputRequest")
            .field("kind", &self.kind)
            .field("request", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PreparedInputKind {
    Embedding,
    KmsKey,
    Signature,
    Encryption,
    AcpDecision,
    WriteValidation,
    NodePermission,
    ReadValidation,
}

const MAX_PREPARED_INPUTS: usize = 64;
const MAX_PREPARED_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_INPUT_DEPENDENCY_MISSES: usize = 8;

#[derive(Default)]
struct PreparedInputMemoState {
    values: RapidHashMap<PreparedInputRequest, Arc<[u8]>>,
    bytes: usize,
    dependency_misses: usize,
}

/// Exact-request memo shared by all attempts of one logical request.
/// Values are the provider's exact response bytes; parsing happens at the
/// domain boundary that requested them.
#[derive(Clone, Default)]
pub struct PreparedInputMemo(Arc<Mutex<PreparedInputMemoState>>);

impl std::fmt::Debug for PreparedInputMemo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedInputMemo")
            .field("entries", &self.len())
            .field("request_bytes", &"[redacted]")
            .field("response_bytes", &"[redacted]")
            .finish()
    }
}

impl PreparedInputMemo {
    pub fn get(&self, request: &PreparedInputRequest) -> Option<Arc<[u8]>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values
            .get(request)
            .cloned()
    }

    /// Store a response if it fits the per-request entry and byte budgets.
    /// Returns false when the caller must stop retrying instead of evicting an
    /// exact response and risking another provider call for the same request.
    pub fn insert(&self, request: PreparedInputRequest, response: impl Into<Arc<[u8]>>) -> bool {
        let response = response.into();
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_size = request.exact_request().len().saturating_add(response.len());
        let old_size = state
            .values
            .get(&request)
            .map(|old| request.exact_request().len().saturating_add(old.len()))
            .unwrap_or_default();
        let next_bytes = state
            .bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
        if !state.values.contains_key(&request) && state.values.len() >= MAX_PREPARED_INPUTS
            || next_bytes > MAX_PREPARED_INPUT_BYTES
        {
            return false;
        }
        state.bytes = next_bytes;
        state.values.insert(request, response);
        true
    }

    pub fn len(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Count a cache miss that requires leaving the current transaction
    /// attempt. Commit-conflict retries do not consume this budget.
    pub fn register_dependency_miss(&self) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.dependency_misses >= MAX_INPUT_DEPENDENCY_MISSES {
            return false;
        }
        state.dependency_misses += 1;
        true
    }
}

/// Immutable write inputs shared by attempts of one mutation request.
#[derive(Debug, Clone)]
pub struct PreparedMutations {
    pub request_time: DateTime<FixedOffset>,
    pub mutations: RapidHashMap<String, PreparedMutation>,
    pub inputs: PreparedInputMemo,
    /// The selected implicit batch controller owns commit-conflict retries.
    pub engine_managed_retry: bool,
}

impl Default for PreparedMutations {
    fn default() -> Self {
        Self {
            request_time: chrono::Utc::now().fixed_offset(),
            mutations: RapidHashMap::default(),
            inputs: PreparedInputMemo::default(),
            engine_managed_retry: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PreparedMutation {
    pub creates: Vec<Document>,
    pub updates: RapidHashMap<String, Arc<WritePreparation>>,
    pub embeddings: RapidHashMap<String, Vec<PreparedEmbedding>>,
    pub doc_ids: Option<Vec<String>>,
}

/// An embedding computed during request preparation together with the exact
/// source values it consumed. Attempts reuse the vector only while these
/// dependencies still match their current document.
#[derive(Debug, Clone)]
pub struct PreparedEmbedding {
    pub field: String,
    pub value: document::NormalValue,
    pub dependencies: RapidHashMap<String, document::NormalValue>,
    pub request: PreparedInputRequest,
}

pub(crate) fn prepared_doc_id(write: &WritePreparation) -> document::DocID {
    document::DocID::new_v0_from_seed(&format!(
        "key-preparation/{}/{}",
        write.collection_short_id, write.doc_short_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_input_memo_reuses_only_identical_wire_requests() {
        let memo = PreparedInputMemo::default();
        let mut provider_calls = 0;
        let resolve =
            |memo: &PreparedInputMemo, request: PreparedInputRequest, calls: &mut usize| {
                if let Some(response) = memo.get(&request) {
                    return response;
                }
                *calls += 1;
                let response: Arc<[u8]> = Arc::from(b"provider response".as_slice());
                assert!(memo.insert(request, Arc::clone(&response)));
                response
            };

        let first = PreparedInputRequest::new(
            PreparedInputKind::Embedding,
            br#"{"model":"m1","input":"source A"}"#.as_slice(),
        );
        let first_response = resolve(&memo, first.clone(), &mut provider_calls);
        let retry_response = resolve(&memo, first, &mut provider_calls);
        assert_eq!(&*first_response, &*retry_response);
        assert_eq!(provider_calls, 1);

        // A changed source field or model changes the exact wire bytes and
        // therefore cannot reuse an embedding from the previous dependency set.
        let changed = PreparedInputRequest::new(
            PreparedInputKind::Embedding,
            br#"{"model":"m1","input":"source B"}"#.as_slice(),
        );
        resolve(&memo, changed, &mut provider_calls);
        let changed_model = PreparedInputRequest::new(
            PreparedInputKind::Embedding,
            br#"{"model":"m2","input":"source B"}"#.as_slice(),
        );
        resolve(&memo, changed_model, &mut provider_calls);
        assert_eq!(provider_calls, 3);
        assert_eq!(memo.len(), 3);
    }

    #[test]
    fn prepared_input_request_debug_redacts_wire_bytes() {
        let request = PreparedInputRequest::new(
            PreparedInputKind::KmsKey,
            b"secret-dependency-and-provider-context".as_slice(),
        );
        let debug = format!("{request:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("secret-dependency"));
    }

    #[test]
    fn prepared_input_memo_bounds_entries_bytes_and_dependency_retries() {
        let memo = PreparedInputMemo::default();
        for index in 0..MAX_PREPARED_INPUTS {
            let request = PreparedInputRequest::new(
                PreparedInputKind::Embedding,
                index.to_be_bytes().to_vec(),
            );
            assert!(memo.insert(request, [1_u8].as_slice()));
        }
        let over_count =
            PreparedInputRequest::new(PreparedInputKind::Embedding, b"over-count".as_slice());
        assert!(!memo.insert(over_count, [1_u8].as_slice()));

        let byte_limited = PreparedInputMemo::default();
        let large_request = PreparedInputRequest::new(
            PreparedInputKind::Signature,
            vec![2_u8; MAX_PREPARED_INPUT_BYTES],
        );
        assert!(!byte_limited.insert(large_request, [3_u8].as_slice()));

        for _ in 0..MAX_INPUT_DEPENDENCY_MISSES {
            assert!(byte_limited.register_dependency_miss());
        }
        assert!(!byte_limited.register_dependency_miss());
    }
}
