//! Opening and closing a store costs the same however many stores are open.
//!
//! The export of engine tickers once copied the list of every live engine on
//! each open and each close, so a process holding many stores paid for all of
//! them on every one. This counts the bytes one export and its drop allocate
//! on this thread, with no other store open and with thousands open.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards every call to the system allocator with the same layout;
// the thread-locals it touches are const-initialized and own no destructor,
// so counting never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            BYTES.with(|bytes| bytes.set(bytes.get() + layout.size()));
            LARGEST.with(|largest| largest.set(largest.get().max(layout.size())));
        }
        // SAFETY: the caller's contract, passed through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract, passed through.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[derive(Debug)]
struct Sample {
    insert_bytes: usize,
    insert_largest: usize,
    drop_bytes: usize,
    drop_largest: usize,
}

fn measure<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    BYTES.with(|bytes| bytes.set(0));
    LARGEST.with(|largest| largest.set(0));
    COUNTING.with(|on| on.set(true));
    let value = f();
    COUNTING.with(|on| on.set(false));
    (value, BYTES.with(Cell::get), LARGEST.with(Cell::get))
}

/// Sample both operations separately over a few tries, so failure diagnostics
/// can distinguish map insertion from retirement on drop.
fn export_and_drop_samples(statistics: &Arc<regolith::Statistics>) -> Vec<Sample> {
    (0..8)
        .map(|_| {
            let (export, insert_bytes, insert_largest) =
                measure(|| telemetry::export_engine_tickers(statistics));
            let (_, drop_bytes, drop_largest) = measure(|| drop(export));
            Sample {
                insert_bytes,
                insert_largest,
                drop_bytes,
                drop_largest,
            }
        })
        .collect()
}

fn minimum_total(samples: &[Sample]) -> usize {
    samples
        .iter()
        .map(|sample| sample.insert_bytes + sample.drop_bytes)
        .min()
        .unwrap_or(usize::MAX)
}

fn maximum_single_allocation(samples: &[Sample]) -> usize {
    samples
        .iter()
        .map(|sample| sample.insert_largest.max(sample.drop_largest))
        .max()
        .unwrap_or(0)
}

#[test]
fn an_export_allocates_the_same_with_thousands_of_stores_open() {
    let statistics = Arc::new(regolith::Statistics::new());
    let alone_samples = export_and_drop_samples(&statistics);
    let alone = minimum_total(&alone_samples);
    let open: Vec<_> = (0..4096)
        .map(|_| telemetry::export_engine_tickers(&statistics))
        .collect();
    let crowded_samples = export_and_drop_samples(&statistics);
    let crowded = minimum_total(&crowded_samples);
    drop(open);
    let inferred_source_bytes = std::mem::size_of::<std::sync::Weak<regolith::Statistics>>()
        + std::mem::size_of::<std::sync::atomic::AtomicU64>() * regolith::Ticker::ALL.len();
    assert!(
        crowded <= alone + 1024,
        "one export and its drop allocated {crowded} bytes with 4096 stores open, \
         {alone} with none; minimum per-sample bytes with none: {alone_samples:?}; \
         with 4096 open: {crowded_samples:?}; inferred Source payload: \
         {inferred_source_bytes} bytes (Weak<Statistics> plus one AtomicU64 per \
         Ticker::ALL entry); largest single allocation, alone/crowded: \
         {}/{}",
        maximum_single_allocation(&alone_samples),
        maximum_single_allocation(&crowded_samples)
    );
}
