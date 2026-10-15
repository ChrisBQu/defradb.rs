//! The network runtime a process starts the first time something needs
//! network I/O and was given no [`Edge`]: started once, shared by every
//! later caller, kept until the process exits. A program that never reaches
//! the network starts no threads, and a component given an `Edge` never
//! starts this one.

use std::num::NonZeroUsize;

use regolith::sync::OnceCell;

use crate::{Edge, EdgeConfig, EdgeError, EdgeRuntime};

/// Threads of the on-demand runtime: network I/O waits, it does not compute.
const THREADS: usize = 2;

static RUNTIME: OnceCell<EdgeRuntime> = OnceCell::new();

/// A handle onto the process's on-demand network runtime, started on first
/// use. One caller starts it while the others wait for it; a start that
/// fails is returned, and the next caller tries again.
pub async fn on_demand() -> Result<Edge, EdgeError> {
    RUNTIME
        .get_or_try_init(async {
            EdgeRuntime::start(EdgeConfig {
                threads: NonZeroUsize::new(THREADS),
                thread_name: "defra-edge-on-demand".to_string(),
                ..EdgeConfig::default()
            })
        })
        .await
        .map(|runtime| runtime.edge().clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::on_demand;

    #[test]
    fn the_on_demand_runtime_starts_once_and_is_shared() {
        let (first, second) = n0_future::future::block_on(async {
            (on_demand().await.unwrap(), on_demand().await.unwrap())
        });
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
        let answer = n0_future::future::block_on(first.spawn(async { 6 * 7 })).unwrap();
        assert_eq!(answer, 42);
    }
}
