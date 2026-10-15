use std::collections::BTreeMap;

use super::*;
use defra_core::block::{ImmutableDeltaPayload, IMMUTABLE_LINK_NAME};

/// A document's Immutable block, computed at create.
pub(crate) struct ImmutableBlock {
    pub cid: Cid,
    pub bytes: Bytes,
    pub signature: Option<(Cid, Bytes)>,
    /// Fields the block carries; they get no LWW block of their own.
    pub fields: rapidhash::RapidHashSet<String>,
}

impl ImmutableBlock {
    pub fn link(&self) -> DAGLink {
        DAGLink::new(IMMUTABLE_LINK_NAME, self.cid)
    }

    /// Head entries: the block is the head of every field it carries, so a
    /// peer holding one of them as mutable chains its update to it. Plus the
    /// block's priority index entry.
    pub fn head_entries(&self, doc_short_id: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.fields
            .iter()
            .map(|field| {
                (
                    HeadstoreDocKey::new(doc_short_id, field, self.cid).bytes(),
                    encode_priority_varint(1),
                )
            })
            .chain([(priority_index_key(doc_short_id, 1, self.cid), vec![])])
            .collect()
    }
}

/// Attach the collection to a document about to be written when it declares
/// `@immutable` fields, which the block builders read from the document.
pub fn attach_immutable_schema(doc: &mut Document, schema: &schema::CollectionVersion) {
    if schema.fields.iter().any(|field| field.immutable) {
        doc.set_collection(schema.clone());
    }
}

/// Whether `field_name` is declared `@immutable` by the document's collection.
///
/// A document without its collection set reports no immutable fields, so its
/// fields keep one LWW block each.
pub(crate) fn is_schema_immutable(doc: &Document, field_name: &str) -> bool {
    doc.collection().is_some_and(|collection| {
        collection
            .fields
            .iter()
            .any(|field| field.immutable && field.name == field_name)
    })
}

/// Build the Immutable block for a document being created.
///
/// It holds every `@immutable` field the encryption config leaves in
/// plaintext: a block carries one encryption link, so an encrypted field
/// keeps its own block and key. Returns `None` when no field qualifies.
pub(crate) fn compute_immutable_block(
    doc: &Document,
    schema_version_id: &str,
    encryption_config: Option<&EncryptionConfig>,
    signing_config: Option<&SigningConfig>,
) -> Result<Option<ImmutableBlock>, String> {
    let mut fields = BTreeMap::new();
    for (field_name, field_value) in doc.values() {
        if field_name == "_docID"
            || !is_schema_immutable(doc, field_name)
            || encryption_config.is_some_and(|enc| enc.should_encrypt_field(field_name))
        {
            continue;
        }
        let data = encode_value_as_cbor(field_value.value())?;
        fields.insert(field_name.clone(), serde_bytes::ByteBuf::from(data));
    }
    if fields.is_empty() {
        return Ok(None);
    }

    let names = fields.keys().cloned().collect();
    let delta = CrdtDelta::Immutable(ImmutableDeltaPayload {
        priority: 1,
        schema_version_id: schema_version_id.to_string(),
        fields,
    });
    let mut block = Block::new(delta, vec![], vec![]);
    let signature = match signing_config {
        Some(signer) => compute_signature(&block, signer)?,
        None => None,
    };
    block.signature = signature.as_ref().map(|(cid, _)| *cid);

    let bytes = block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode immutable block: {}", e))?;
    let cid = generate_cid_from_bytes(&bytes)
        .map_err(|e| format!("Failed to generate immutable block CID: {}", e))?;
    Ok(Some(ImmutableBlock {
        cid,
        bytes: bytes.into(),
        signature,
        fields: names,
    }))
}
