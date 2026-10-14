//! Opening options for the regolith backend.

use std::time::Duration;

use regolith::{DurabilityMode as EngineDurability, IsolationLevel, Options};

use crate::backends::shared::DurabilityMode;

/// How a [`super::RegolithStore`] opens and commits.
#[derive(Clone)]
pub struct RegolithStoreOptions {
    /// Engine options. Built from a regolith profile, so a target that
    /// needs a different memory budget picks it up by construction rather
    /// than by a caller remembering to tune it.
    pub engine: Options,
    /// Commit-time validation. Defaults to [`IsolationLevel::RepeatableRead`]:
    /// every point read is validated, which is what the index and docid
    /// paths need, and a scan is recorded per stretch rather than per key.
    /// The head set is a scan over entries that concurrent appends add to
    /// and reclamation removes by design; recorded per key it would abort
    /// appends no serial order needed to abort, and hold one commit check
    /// per head and marker walked.
    pub isolation: IsolationLevel,
    /// How long `close` waits for in-flight transactions to finish.
    pub close_timeout: Duration,
}

impl RegolithStoreOptions {
    /// Seal engine files with a key resolved before opening the store.
    ///
    /// Values remain plaintext inside the engine, so equality checks,
    /// allocations, appends and merge operands retain their meaning.
    /// This does not decrypt values written by the legacy `EncryptedStore`
    /// wrapper; those stores must be migrated before using this option.
    pub fn with_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.engine = self.engine.key_provider(std::sync::Arc::new(
            super::encryption::AtRestKeyProvider::new(key),
        ));
        self
    }

    /// Server defaults: repeatable read, fsync on every commit.
    pub fn new() -> Self {
        Self {
            engine: Options::default().durability(EngineDurability::Immediate),
            isolation: IsolationLevel::RepeatableRead,
            close_timeout: Duration::from_secs(30),
        }
    }

    /// A 1-4 MiB working set, for a phone or an edge device.
    pub fn embedded() -> Self {
        Self {
            engine: Options::embedded().durability(EngineDurability::Immediate),
            ..Self::new()
        }
    }

    /// A browser or wasi module. The caller supplies the filesystem: on
    /// `wasm32-unknown-unknown` there is none, so mount OPFS and set
    /// [`RegolithStoreOptions::engine`]'s `env` before opening.
    pub fn wasm() -> Self {
        Self {
            engine: Options::wasm().durability(EngineDurability::Immediate),
            ..Self::new()
        }
    }

    /// Keep the database in memory. For tests and for a node that is
    /// explicitly ephemeral.
    pub fn memory() -> Self {
        // Take the environment constraints from the engine's own preset,
        // which is where the knowledge lives: `MemEnv` starts no threads, so
        // a compaction worker cannot exist, and nothing is being made durable,
        // so an fsync per commit would buy nothing.
        //
        // The size limits stay the server ones. `Options::memory()` is sized
        // from the embedded profile, whose 256 KiB value ceiling is a limit a
        // caller can hit rather than a memory-tuning knob: a document larger
        // than that is ordinary here, and an in-memory store that refuses what
        // the on-disk one accepts is a different database, not a faster one.
        //
        // With no worker, a write that a stall stops has to run the
        // compaction itself, or nothing ever clears the stall.
        let preset = Options::memory();
        let mut opts = Self::new();
        opts.engine = opts
            .engine
            .env(preset.get_env().clone())
            .max_background_compactions(preset.get_max_background_compactions())
            .inline_compaction(true)
            .durability(preset.get_durability());
        opts
    }

    /// Change commit validation. `RepeatableRead` is the default because it is
    /// what the merge, index and head-set paths need together; a caller
    /// that knows its unit of work is a blind write can ask for less.
    pub fn with_isolation(mut self, isolation: IsolationLevel) -> Self {
        self.isolation = isolation;
        self
    }

    /// Trade durability for throughput: `Eventual` survives a process
    /// crash but not a power cut.
    pub fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.engine = self.engine.durability(match durability {
            DurabilityMode::Immediate => EngineDurability::Immediate,
            DurabilityMode::Eventual => EngineDurability::Eventual,
        });
        self
    }
}

impl Default for RegolithStoreOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RegolithStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegolithStoreOptions")
            .field("isolation", &self.isolation)
            .field("durability", &self.engine.get_durability())
            .field("close_timeout", &self.close_timeout)
            .finish_non_exhaustive()
    }
}
