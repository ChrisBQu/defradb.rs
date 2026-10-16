//! Every bench runs on the allocator the defra binaries ship with (mimalloc:
//! a heap per thread, a block freed on another thread returned to its heap
//! without a lock), so a bench measures the allocator the node runs on.
//! `allocs` counts on top of the same allocator instead.

#[cfg(not(target_arch = "wasm32"))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
