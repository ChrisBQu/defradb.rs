use super::runtime;
use crate::EdgeError;

#[test]
fn block_on_refuses_to_stall_a_single_threaded_executor() {
    let runtime = runtime();
    let edge = runtime.edge().clone();
    let current = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let refused = current.block_on(async { edge.block_on(async { 1 }) });

    assert_eq!(refused, Err(EdgeError::WouldBlock));
}

#[test]
fn block_on_from_a_task_of_the_runtime_hands_its_thread_over() {
    let runtime = runtime();
    let edge = runtime.edge().clone();
    let inner = edge.clone();

    let nested = edge.spawn(async move { inner.block_on(async { 3 }) });

    assert_eq!(edge.block_on(nested).unwrap(), Ok(Ok(3)));
}
