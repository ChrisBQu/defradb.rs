//! The arrival journal's keys, as regolith's commit-ordered append sees them.
//!
//! An [`AppendLog`] names a log's head key and its entry prefix; the key of
//! the entry at a position is the prefix followed by the position as 20
//! zero-padded decimal digits, which sort in position order and hold every
//! `u64`.

use regolith::LogLayout;

use crate::corekv::AppendLog;

impl LogLayout for AppendLog {
    fn head_key(&self) -> &[u8] {
        &self.head_key
    }

    fn entry_key(&self, position: u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.entry_prefix);
        out.extend_from_slice(format!("{position:020}").as_bytes());
    }

    fn max_entry_key_len(&self) -> usize {
        self.entry_prefix.len() + 20
    }
}
