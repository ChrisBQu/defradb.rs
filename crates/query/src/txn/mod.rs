//! Transaction management for query execution.
//!
//! Plan-layer primitives (`TransactionContext`, `TransactionHandle`, and
//! `TransactionRegistry`) live in the sibling modules here. `TransactionGuard`
//! lives in `guard`; it is generic over the runner-layer `QueryExecutor` trait.

mod context;
mod guard;
mod handle;
mod read_access;
mod registry;
mod result;

#[cfg(test)]
mod tests;

pub use context::{
    check_doc_access_with_prepared_inputs, check_doc_access_with_projection,
    check_doc_read_access_with_prepared_inputs, is_doc_registered_with_projection,
    AcpRegistrationProjection, RequestBearerTokenGuard, TransactionContext,
};
pub use guard::TransactionGuard;
pub use handle::TransactionHandle;
pub use read_access::OverlayChecker;
pub use registry::{NoOpTransactionRegistry, TransactionRegistry};
pub use result::GetTransactionResult;
