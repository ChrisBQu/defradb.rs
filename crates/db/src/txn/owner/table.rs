//! A worker's own transactions: the mailbox of each, by ID, and the
//! sequence its IDs are drawn from. Only that worker reads or writes its
//! table; the map is the lock-free one only because the registry that holds
//! every worker's table is shared.

use std::sync::atomic::{AtomicU64, Ordering};

use rapidhash::fast::RandomState;
use regolith::sync::{HopscotchMap, UnboundedSender};

use super::actor::Message;
use super::TxnId;

pub(super) struct Table<T>
where
    T: 'static,
{
    next: AtomicU64,
    mailboxes: HopscotchMap<u64, UnboundedSender<Message<T>>, RandomState>,
}

impl<T> Table<T>
where
    T: 'static,
{
    pub(super) fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            mailboxes: HopscotchMap::with_hasher(RandomState::default()),
        }
    }

    /// The next sequence number for an ID.
    pub(super) fn draw(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn insert(&self, id: TxnId, mailbox: UnboundedSender<Message<T>>) {
        self.mailboxes.insert(id.get(), mailbox);
    }

    /// Queues `message` for transaction `id`; hands it back when the
    /// transaction is not here.
    pub(super) fn send(&self, id: TxnId, message: Message<T>) -> Result<(), Message<T>> {
        match self.mailboxes.get(&id.get()) {
            Some(mailbox) => mailbox.try_send(message),
            None => Err(message),
        }
    }

    /// Queues one message, built per transaction, for every transaction here.
    pub(super) fn send_all(&self, mut message: impl FnMut() -> Message<T>) -> usize {
        self.mailboxes
            .iter()
            .filter(|(_, mailbox)| mailbox.try_send(message()).is_ok())
            .count()
    }

    pub(super) fn remove(&self, id: TxnId) {
        self.mailboxes.remove(&id.get());
    }

    pub(super) fn len(&self) -> usize {
        self.mailboxes.len()
    }
}
