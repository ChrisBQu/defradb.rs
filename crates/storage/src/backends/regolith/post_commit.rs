//! Post-commit work that failed after its commit was durable.
//!
//! A durable commit's outcome stands. A hook that fails, panics or is
//! cancelled, an event that panics, and a regolith callback that panics
//! after its outcome are each reported here, as one warn line and one count
//! of `defradb.storage.events` by kind, and never turned into a failed write
//! or raised in another caller.

use telemetry::StorageEvent;

pub(super) fn report(kind: StorageEvent, detail: &dyn std::fmt::Display) {
    tracing::warn!(
        kind = kind.label(),
        %detail,
        "Post-commit work failed after its commit was durable; the commit stands"
    );
    telemetry::record_storage_event(kind, 0);
}

/// Runs one hook to its end. A failure or a panic in it is reported, never
/// returned: its commit is durable by now.
pub(super) async fn run_hook<F>(hook: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    #[cfg(not(target_arch = "wasm32"))]
    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(hook)).await;
    // A browser build aborts on a panic, so there is nothing to catch.
    #[cfg(target_arch = "wasm32")]
    let outcome: Result<Result<(), String>, Box<dyn std::any::Any + Send>> = Ok(hook.await);
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => report(StorageEvent::PostCommitHookFailed, &error),
        Err(panic) => report(
            StorageEvent::PostCommitHookPanicked,
            &super::event_order::panic_message(panic.as_ref()),
        ),
    }
}
