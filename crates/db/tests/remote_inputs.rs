//! Round trips of the remote inputs a write or a merge fetches before its
//! transaction, pinned with counting fakes: a composite with F encrypted
//! fields makes one KMS request; a create of N documents makes two signing
//! waves; time scales with waves, not items.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use blockstore::Blockstore as _;
use blockstore::DefraBlockstore;
use crypto::PrivateKey as _;
use db::database::DB;
use db::merge::merge_handler::DbMergeHandler;
use db::AutoCommitMutator;
use defra_core::block::{
    Block, CompositeDeltaPayload, CrdtDelta, DAGLink, Encryption, LwwDeltaPayload,
};
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use defra_core::signing::{RemoteSigner, SignFailure, SigningAuthorization};
use defra_core::thread_bounds::MaybeBoxFuture;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::RegolithStore;

const FIELDS: [&str; 4] = ["a", "b", "c", "d"];

async fn database() -> Arc<DB<RegolithStore>> {
    database_on(Arc::new(RegolithStore::in_memory().unwrap())).await
}

async fn database_on(store: Arc<RegolithStore>) -> Arc<DB<RegolithStore>> {
    let db = Arc::new(DB::from_arc(store).unwrap());
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for (index, name) in FIELDS.iter().enumerate() {
        fields.push(FieldDescription::new(
            (index + 2).to_string(),
            *name,
            FieldKind::string(),
        ));
    }
    db.create_collection(CollectionVersion::new("Things", "v1", "col-things", fields))
        .await
        .unwrap();
    db
}

/// A merge handler over one store, as a node has.
async fn handler() -> (
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = database_on(Arc::clone(&store)).await;
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    (DbMergeHandler::new(db, Arc::clone(&blockstore)), blockstore)
}

/// A KMS that knows a fixed set of keys and counts the requests it serves.
struct CountingKms {
    keys: Vec<(cid::Cid, [u8; 32])>,
    requests: AtomicUsize,
}

#[async_trait]
impl kms::KmsService for CountingKms {
    async fn get_keys(
        &self,
        _: &kms::RequestContext,
        cids: &[kms::EncryptionCid],
    ) -> kms::Result<kms::KeyResults> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        let (results, sender) = kms::KeyResults::new(cids.len().max(1));
        for cid in cids {
            let key = self
                .keys
                .iter()
                .find(|(known, _)| known == cid)
                .map(|(_, key)| *key)
                .ok_or(kms::Error::KeyUnavailable);
            sender.send(key.map(|key| (*cid, key))).await.unwrap();
        }
        Ok(results)
    }

    async fn generate_key(
        &self,
        _: &kms::RequestContext,
        _: kms::KeyScope,
    ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
        Err(kms::Error::Unsupported("merges never generate keys"))
    }

    async fn serve_request(
        &self,
        _: kms::PeerIdentity,
        _: kms::FetchEncryptionKeyRequest,
    ) -> kms::Result<kms::FetchEncryptionKeyReply> {
        Err(kms::Error::Unsupported("no peers"))
    }
}

/// A composite linking one encrypted field block per field, each under its
/// own key, stored in `blockstore`. Returns the composite and the keys.
async fn encrypted_composite(
    blockstore: &DefraBlockstore<RegolithStore>,
) -> (cid::Cid, Vec<u8>, Vec<(cid::Cid, [u8; 32])>) {
    let mut links = Vec::new();
    let mut keys = Vec::new();
    for (index, field) in FIELDS.iter().enumerate() {
        let key = [u8::try_from(index + 1).unwrap(); 32];
        let encryption = Encryption::new(key.to_vec());
        let encryption_cid = encryption.generate_cid().unwrap();
        let mut plaintext = Vec::new();
        ciborium::into_writer(
            &NormalValue::String(format!("value {field}")),
            &mut plaintext,
        )
        .unwrap();
        let (ciphertext, _) =
            crypto::encryption::aes::encrypt_aes(&plaintext, &key, &[], true).unwrap();
        let block = Block::new_with_options(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: field.to_string(),
                schema_version_id: "v1".into(),
                priority: 1,
                data: ciphertext,
            }),
            vec![],
            vec![],
            Some(encryption_cid),
            None,
        );
        let block_cid = block.generate_cid().unwrap();
        blockstore
            .put(&block_cid, &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
        links.push(DAGLink::new(*field, block_cid));
        keys.push((encryption_cid, key));
    }
    let composite = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "v1".into(),
            priority: 1,
            status: 1,
        }),
        vec![],
        links,
    );
    let cid = composite.generate_cid().unwrap();
    let data = composite.to_dag_cbor().unwrap();
    blockstore.put(&cid, &data).await.unwrap();
    (cid, data, keys)
}

#[tokio::test]
async fn a_merged_composite_with_four_encrypted_fields_makes_one_kms_request() {
    let (handler, blockstore) = handler().await;
    let (cid, data, keys) = encrypted_composite(&blockstore).await;
    let kms = Arc::new(CountingKms {
        keys,
        requests: AtomicUsize::new(0),
    });
    handler.set_kms(kms.clone());
    let doc_id = db::block::builder::derive_doc_id(&cid);
    let metadata =
        BlockMetadata::normal(&doc_id, "col-things", "did:key:z6MkMergeKeys", None, false);

    let outcome = handler.handle_block(&cid, &data, metadata).await.unwrap();

    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        kms.requests.load(Ordering::SeqCst),
        1,
        "every field's key in one request, before the merge transaction"
    );
}

#[tokio::test]
async fn a_composite_merged_in_its_own_transaction_makes_one_kms_request() {
    let (handler, blockstore) = handler().await;
    let (cid, data, keys) = encrypted_composite(&blockstore).await;
    let kms = Arc::new(CountingKms {
        keys,
        requests: AtomicUsize::new(0),
    });
    handler.set_kms(kms.clone());
    let doc_id = db::block::builder::derive_doc_id(&cid);
    let metadata =
        BlockMetadata::normal(&doc_id, "col-things", "did:key:z6MkMergeKeys", None, false);
    let block = Block::from_dag_cbor(&data).unwrap();
    let CrdtDelta::Composite(payload) = &block.delta else {
        unreachable!("built as a composite")
    };

    let outcome = handler
        .process_composite_delta(&cid, &block, payload, &metadata, false, 0)
        .await
        .unwrap();

    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(kms.requests.load(Ordering::SeqCst), 1);
}

/// A remote signer that counts its waves and the signatures in them, and
/// takes `latency` per wave.
struct WaveSigner {
    key: crypto::Ed25519PrivateKey,
    waves: AtomicUsize,
    signatures: AtomicUsize,
    latency: Duration,
}

impl RemoteSigner for WaveSigner {
    fn sign_sync(&self, data: &[u8], _: Option<&SigningAuthorization>) -> Result<Vec<u8>, String> {
        self.signatures.fetch_add(1, Ordering::SeqCst);
        self.key.sign(data).map_err(|error| error.to_string())
    }

    fn sign_batch(
        &self,
        payloads: Vec<Arc<[u8]>>,
        authorization: Option<SigningAuthorization>,
    ) -> MaybeBoxFuture<'static, Vec<Result<Vec<u8>, SignFailure>>> {
        self.waves.fetch_add(1, Ordering::SeqCst);
        let signatures: Vec<_> = payloads
            .iter()
            .map(|payload| {
                self.sign_sync(payload, authorization.as_ref())
                    .map_err(SignFailure::refused)
            })
            .collect();
        let latency = self.latency;
        Box::pin(async move {
            tokio::time::sleep(latency).await;
            signatures
        })
    }
}

fn signer(latency: Duration) -> Arc<WaveSigner> {
    Arc::new(WaveSigner {
        key: crypto::generate_ed25519().unwrap(),
        waves: AtomicUsize::new(0),
        signatures: AtomicUsize::new(0),
        latency,
    })
}

fn sign_with(signer: &Arc<WaveSigner>) {
    let public = signer.key.public_key();
    defra_core::signing::set_signing_config(Some(defra_core::signing::SigningConfig {
        key_type: defra_core::signing::SigningKeyType::Ed25519,
        private_key_bytes: Vec::new(),
        public_key_bytes: public.raw_owned(),
        public_key_hex: hex::encode(public.raw()),
        remote_signer: Some(signer.clone() as Arc<dyn RemoteSigner>),
        signing_authorization: None,
    }));
}

fn documents(count: usize) -> Vec<Document> {
    (0..count)
        .map(|index| {
            let mut doc = Document::new();
            for field in FIELDS {
                doc.set(field, format!("{field} {index}"));
            }
            doc
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_create_of_n_documents_signs_in_two_waves_and_takes_two_round_trips() {
    const DOCUMENTS: usize = 8;
    let latency = Duration::from_millis(200);
    let db = database().await;
    let signer = signer(latency);
    sign_with(&signer);

    let started = tokio::time::Instant::now();
    let created = AutoCommitMutator::new(Arc::clone(&db))
        .create_many("Things", documents(DOCUMENTS))
        .await;
    let elapsed = started.elapsed();
    defra_core::signing::set_signing_config(None);

    assert_eq!(created.unwrap().len(), DOCUMENTS);
    assert_eq!(
        signer.waves.load(Ordering::SeqCst),
        2,
        "every field block in the first wave, every composite in the second"
    );
    assert_eq!(
        signer.signatures.load(Ordering::SeqCst),
        DOCUMENTS * (FIELDS.len() + 1),
        "each block signed once"
    );
    assert!(
        elapsed >= latency * 2 && elapsed < latency * 3,
        "two waves of {latency:?}, not one per block: {elapsed:?}"
    );
}
