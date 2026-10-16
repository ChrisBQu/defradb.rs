//! Waiting out what a non-blocking regolith call hands back.

use regolith::{Error, WouldBlock};

/// Runs `call` until it stops asking to wait: on [`Error::WouldBlock`] it
/// awaits what the error carries (a read's `IoWait`, a stall's `StallWait`,
/// a key lock's `LockWait`) and calls again; any other outcome is returned.
/// The [`regolith::Error`] counterpart of `regolith::ready`.
pub(crate) async fn settle<T>(
    mut call: impl FnMut() -> regolith::Result<T>,
) -> regolith::Result<T> {
    loop {
        match call() {
            Err(Error::WouldBlock(would_block)) => wait_out(would_block).await?,
            other => return other,
        }
    }
}

/// Awaits what `would_block` asks to wait on; a kind this build does not know
/// is handed back as the error it came in.
pub(crate) async fn wait_out(would_block: WouldBlock) -> regolith::Result<()> {
    match would_block {
        WouldBlock::Io(wait) => wait.await,
        WouldBlock::Stall(wait) => wait.await,
        WouldBlock::Locked(wait) => wait.await,
        other => return Err(Error::WouldBlock(other)),
    }
    Ok(())
}
