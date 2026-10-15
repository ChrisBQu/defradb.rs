//! The write path: transactional mutations.
#[path = "../common/mod.rs"]
mod common;

mod batch;
mod batch_retry;
mod batch_signing;
mod doc_suite;
mod embedding;
mod embedding_reload;
mod identical_create;
mod identical_delta;
mod kms;
mod kms_authorization;
mod kms_request;
mod update;
