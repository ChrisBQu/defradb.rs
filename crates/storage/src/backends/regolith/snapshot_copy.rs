//! A bounded, verified copy of a store's visible snapshot into a fresh store.
//!
//! This operates beneath the Defra key classifier. It is only for copying an
//! offline snapshot into a new store: preserving existing log keys and their
//! assigned cursors is different from admitting new writes to those keys.

use std::path::Path;

use regolith::{Db, DurabilityMode, Options, StreamOptions};
use sha2::{Digest, Sha256};

use crate::corekv::{Error, Result};

const BATCH_BYTES: usize = 4 << 20;

#[derive(Default)]
struct Tally {
    entries: u64,
    digest: Sha256,
}

impl Tally {
    fn add(&mut self, key: &[u8], value: &[u8]) {
        for part in [key, value] {
            self.digest.update((part.len() as u64).to_be_bytes());
            self.digest.update(part);
        }
        self.entries += 1;
    }

    fn finish(self) -> (u64, [u8; 32]) {
        (self.entries, self.digest.finalize().into())
    }
}

fn backend(error: regolith::Error, action: &str) -> Error {
    Error::Backend(format!("snapshot copy could not {action}: {error}"))
}

/// Copy one stable source snapshot into a new keyed Regolith database.
///
/// The target must not exist. The caller must keep the source offline for the
/// duration of the copy and retain it until the caller accepts the copy.
/// Existing log keys are copied as already-assigned records; normal runtime
/// writes still pass through the Defra key classifier and append protocol.
pub(super) fn copy(
    source: &Db,
    target: &Path,
    source_permissions: &std::fs::Permissions,
    options: &Options,
    progress: &mut dyn FnMut(),
) -> Result<()> {
    match std::fs::create_dir(target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::Backend(format!(
                "snapshot copy target already exists: {}",
                target.display()
            )))
        }
        Err(error) => {
            return Err(Error::Backend(format!(
                "snapshot copy could not create target {}: {error}",
                target.display()
            )))
        }
    }
    std::fs::set_permissions(target, source_permissions.clone()).map_err(|error| {
        Error::Backend(format!(
            "snapshot copy could not set target permissions {}: {error}",
            target.display()
        ))
    })?;

    let source_snapshot = source.snapshot();
    let target_db = Db::open(target, options.clone())
        .map_err(|error| backend(error, "open the fresh target"))?;
    let copy_result = (|| {
        let mut writer = target_db.streaming_writer(StreamOptions {
            max_buffered_bytes: BATCH_BYTES,
            durability: Some(DurabilityMode::Immediate),
        });
        let mut tally = Tally::default();
        let mut bytes = 0usize;
        for entry in source_snapshot.owned_iter() {
            let (key, value) = entry.map_err(|error| backend(error, "read the source snapshot"))?;
            bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
            tally.add(&key, &value);
            writer
                .put_owned(&key, value.to_vec())
                .map_err(|error| backend(error, "write the target"))?;
            if bytes >= BATCH_BYTES {
                progress();
                bytes = 0;
            }
        }
        writer
            .finish()
            .map_err(|error| backend(error, "finish the target"))?;
        if bytes > 0 {
            progress();
        }
        Ok::<_, Error>(tally.finish())
    })();
    let closed = target_db
        .close()
        .map_err(|error| backend(error, "close the target"));
    drop(target_db);
    let expected = copy_result?;
    closed?;

    let verified_db = Db::open(target, options.clone())
        .map_err(|error| backend(error, "reopen the target for verification"))?;
    let verified = (|| {
        let mut tally = Tally::default();
        for entry in verified_db.snapshot().owned_iter() {
            let (key, value) = entry.map_err(|error| backend(error, "read the target snapshot"))?;
            tally.add(&key, &value);
        }
        if tally.finish() != expected {
            return Err(Error::Backend(
                "snapshot copy verification failed; source was left unchanged".into(),
            ));
        }
        Ok(())
    })();
    let closed = verified_db
        .close()
        .map_err(|error| backend(error, "close the verified target"));
    drop(verified_db);
    verified?;
    closed
}

#[cfg(test)]
mod tests {
    use crate::backends::{RegolithStore, RegolithStoreOptions};
    use crate::corekv::{AppendLog, Store};

    const HEAD: &[u8] = b"s/arrival/7/head";
    const ROW_PREFIX: &[u8] = b"s/arrival/7/row/";
    const DOC_ONE: &[u8] = b"s/arrival/7/doc/doc-1";
    const DOC_TWO: &[u8] = b"s/arrival/7/doc/doc-2";
    const DOC_THREE: &[u8] = b"s/arrival/7/doc/doc-3";

    fn row(cursor: u64) -> Vec<u8> {
        format!("s/arrival/7/row/{cursor:020}").into_bytes()
    }

    async fn append(store: &RegolithStore, log: &AppendLog, doc: &[u8], once: &[u8]) {
        let mut txn = store.new_txn(false).await.unwrap();
        txn.append(log, doc, Some(once)).await.unwrap();
        txn.commit().await.unwrap();
    }

    async fn value(store: &RegolithStore, key: &[u8]) -> Option<Vec<u8>> {
        let txn = store.new_txn(true).await.unwrap();
        let value = txn.get(key).await.unwrap().map(|value| value.to_vec());
        txn.discard();
        value
    }

    #[tokio::test]
    async fn copies_arrival_state_and_keeps_append_and_once_semantics_after_encrypted_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let source = RegolithStore::open(temp.path().join("source")).unwrap();
        let log = AppendLog::new(HEAD.to_vec(), ROW_PREFIX.to_vec());
        append(&source, &log, b"doc-1", DOC_ONE).await;
        append(&source, &log, b"doc-2", DOC_TWO).await;

        let target_path = temp.path().join("encrypted-copy");
        let target_options = RegolithStoreOptions::default().with_encryption_key([41; 32]);
        source
            .copy_snapshot_to(&target_path, &target_options, &mut || {})
            .unwrap();
        let target =
            RegolithStore::open_with_options(&target_path, target_options.clone()).unwrap();
        let initial_keys = [
            HEAD.to_vec(),
            row(1),
            row(2),
            DOC_ONE.to_vec(),
            DOC_TWO.to_vec(),
        ];
        for key in &initial_keys {
            assert_eq!(
                value(&source, key).await,
                value(&target, key).await,
                "{key:?}"
            );
        }

        append(&target, &log, b"doc-3", DOC_THREE).await;
        assert_eq!(
            value(&target, &row(3)).await.as_deref(),
            Some(&b"doc-3"[..])
        );
        append(&target, &log, b"doc-1", DOC_ONE).await;
        assert_eq!(
            value(&target, HEAD).await.as_deref(),
            Some(&3u64.to_be_bytes()[..])
        );
        assert_eq!(
            value(&target, &row(3)).await.as_deref(),
            Some(&b"doc-3"[..])
        );

        target.close().await.unwrap();
        drop(target);
        let reopened = RegolithStore::open_with_options(&target_path, target_options).unwrap();
        for (key, expected) in [
            (HEAD.to_vec(), 3u64.to_be_bytes().to_vec()),
            (row(1), b"doc-1".to_vec()),
            (row(2), b"doc-2".to_vec()),
            (row(3), b"doc-3".to_vec()),
            (DOC_ONE.to_vec(), 1u64.to_be_bytes().to_vec()),
            (DOC_TWO.to_vec(), 2u64.to_be_bytes().to_vec()),
            (DOC_THREE.to_vec(), 3u64.to_be_bytes().to_vec()),
        ] {
            assert_eq!(value(&reopened, &key).await, Some(expected), "{key:?}");
        }
        reopened.close().await.unwrap();
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn refuses_to_copy_over_an_existing_destination() {
        let temp = tempfile::tempdir().unwrap();
        let source = RegolithStore::open(temp.path().join("source")).unwrap();
        let target = temp.path().join("existing");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("custody"), b"keep").unwrap();

        let error = source
            .copy_snapshot_to(
                &target,
                &RegolithStoreOptions::default().with_encryption_key([42; 32]),
                &mut || {},
            )
            .unwrap_err();
        assert!(error.to_string().contains("target already exists"));
        assert_eq!(std::fs::read(target.join("custody")).unwrap(), b"keep");
        source.close().await.unwrap();
    }
}
