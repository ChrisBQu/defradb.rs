//! The pool a registry uses when it is built without one: one worker, driven
//! by a thread of its own, so tests and the embedded build run the same
//! owner code a node runs on its full pool. The thread is not pinned and
//! keeps its priority: the pool is driven, not started. It ends when the
//! last handle to the pool is dropped.

use std::time::Duration;

use workers::{Config, Pool, Workers};

/// A one-worker pool driven by a thread of its own.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn one_worker() -> Result<Pool, workers::Error> {
    let (handed, pool) = regolith::sync::unbounded();
    std::thread::Builder::new()
        .name("defra-txn-owner".to_owned())
        .spawn(move || match Pool::driven_here(config()) {
            Ok((pool, mut driver)) => {
                let _ = handed.try_send(Ok(pool));
                drop(handed);
                // Runs the worker until the pool is shut down or every handle
                // to it is gone. Parking here is the worker's idle park: a
                // push, a wake or a due timer unparks this thread.
                loop {
                    let done = driver.turn();
                    if driver.finished() {
                        break;
                    }
                    if done == 0 {
                        match driver.next_due() {
                            Some(wait) => std::thread::park_timeout(wait),
                            None => std::thread::park(),
                        }
                    }
                }
            }
            Err(error) => {
                let _ = handed.try_send(Err(error));
            }
        })
        .map_err(|error| workers::Error::Spawn(error.to_string()))?;
    workers::block_on(pool.recv_async())?.unwrap_or(Err(workers::Error::ShutDown))
}

fn config() -> Config {
    Config {
        workers: Workers::Count(1),
        blocking_threads: 1,
        idle_spin: Duration::from_micros(20),
        thread_name: "defra-txn-owner".to_owned(),
        ..Config::default()
    }
}
