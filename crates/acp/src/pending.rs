//! Registrations a create owes an external ACP provider.
//!
//! An external provider (Vera) cannot join the create's transaction, so the
//! create writes a record of the registration it owes into that transaction.
//! The record commits with the document: until the provider confirms the
//! registration and the record is cleared, the document is readable only by
//! its creator, and a crash or a failed provider call leaves the record for a
//! redrive to complete. A record exists only for a document its transaction
//! created, so an upsert that found an existing document owes nothing.

use datastore::{NamespaceView, WeakNamespaceView};
use identity::Did;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Start of every owed-registration key in the ACP namespace.
pub(crate) const PENDING_PREFIX: &[u8] = b"/acp-pending/";

/// One owed registration: `creator` becomes the owner of `doc_id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRegistration {
    pub creator: Did,
    pub policy_id: String,
    pub resource_name: String,
    pub doc_id: String,
}

impl PendingRegistration {
    pub fn new(creator: Did, policy_id: &str, resource_name: &str, doc_id: &str) -> Self {
        Self {
            creator,
            policy_id: policy_id.to_string(),
            resource_name: resource_name.to_string(),
            doc_id: doc_id.to_string(),
        }
    }

    /// The key this registration is recorded under.
    pub fn key(&self) -> Vec<u8> {
        pending_key(&self.policy_id, &self.resource_name, &self.doc_id)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// The key of the registration owed for one document. Each part is preceded by
/// its length, so no two (policy, resource, document) triples share a key.
pub fn pending_key(policy_id: &str, resource_name: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        PENDING_PREFIX.len() + 24 + policy_id.len() + resource_name.len() + doc_id.len(),
    );
    key.extend_from_slice(PENDING_PREFIX);
    for part in [policy_id, resource_name, doc_id] {
        key.extend_from_slice(&(part.len() as u64).to_be_bytes());
        key.extend_from_slice(part.as_bytes());
    }
    key
}

/// Owed registrations as one transaction sees them: its own writes over its
/// snapshot. Reads are validated point reads, so a registration settled or
/// recorded by a transaction that commits first aborts a decision made on it.
/// The handle does not keep the transaction in use: once it is committed,
/// rolled back or dropped, nothing is owed through it.
#[derive(Clone)]
pub struct PendingRegistrations {
    view: WeakNamespaceView,
}

impl PendingRegistrations {
    /// `view` is the transaction's ACP namespace.
    pub fn new(view: &NamespaceView) -> Self {
        Self {
            view: view.downgrade(),
        }
    }

    fn active_view(&self) -> Result<NamespaceView> {
        self.view
            .upgrade()
            .ok_or_else(|| Error::Storage("the transaction is no longer active".into()))
    }

    /// Record, in this transaction, the registration its create owes.
    pub async fn record(&self, pending: &PendingRegistration) -> Result<()> {
        self.active_view()?
            .set(&pending.key(), &pending.encode()?)
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    /// The registration owed for a document, if any.
    pub async fn get(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Option<PendingRegistration>> {
        let Some(view) = self.view.upgrade() else {
            return Ok(None);
        };
        view.get(&pending_key(policy_id, resource_name, doc_id))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .map(|bytes| PendingRegistration::decode(&bytes))
            .transpose()
    }

    /// Drop the registration owed for a document this transaction deletes.
    pub async fn clear(&self, policy_id: &str, resource_name: &str, doc_id: &str) -> Result<()> {
        self.active_view()?
            .delete(&pending_key(policy_id, resource_name, doc_id))
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_of_distinct_triples_differ() {
        let triples = [
            ("ab", "c", "d"),
            ("a", "bc", "d"),
            ("a", "b", "cd"),
            ("", "abc", "d"),
            ("abc", "", "d"),
        ];
        for (i, left) in triples.iter().enumerate() {
            for right in &triples[i + 1..] {
                assert_ne!(
                    pending_key(left.0, left.1, left.2),
                    pending_key(right.0, right.1, right.2),
                    "{left:?} and {right:?} share a key"
                );
            }
        }
    }

    #[test]
    fn a_record_round_trips_and_names_its_key() {
        let pending = PendingRegistration::new(
            Did::new("did:key:creator").unwrap(),
            "policy",
            "users",
            "bae-1",
        );
        assert_eq!(
            PendingRegistration::decode(&pending.encode().unwrap()).unwrap(),
            pending
        );
        assert_eq!(pending.key(), pending_key("policy", "users", "bae-1"));
        assert!(pending.key().starts_with(PENDING_PREFIX));
    }
}
