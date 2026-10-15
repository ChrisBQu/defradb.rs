//! The database's lens store: the lenses it has durably committed.
//!
//! A lens config commits under its `LensConfigKey` in the transaction that
//! registers it. Registering the lens in the in-memory transform store is the
//! follow-up: it runs once the commit is durable, and at open for every
//! persisted config. It can fail (a module path that cannot be read now, a
//! compile that runs out of resources), and the commit stands regardless: the
//! failure is reported once and the lens is owed. An owed lens is known
//! (`has_transform`, `list`), so nothing treats a committed migration as
//! absent; each use of it registers it first, and fails loud with the reason
//! while registration still fails. A crash loses nothing: the next open
//! registers every persisted config again.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use kovan_map::HopscotchMap;
use lens::{LensConfig, LensDocResultStream, LensDocStream, TransformId, TransformStore};
use rapidhash::fast::RandomState;
use rapidhash::RapidHashMap;
use storage::corekv::Store;

use crate::DB;

/// Committed lenses whose registration is owed, with their configs.
type Owed = HopscotchMap<TransformId, Arc<LensConfig>, RandomState>;

/// The database's transform store, holding the registrations its commits owe.
pub(crate) struct CommittedLensStore {
    base: Arc<dyn TransformStore>,
    owed: Arc<Owed>,
}

impl CommittedLensStore {
    pub(crate) fn wrap(base: Arc<dyn TransformStore>) -> Arc<dyn TransformStore> {
        Arc::new(Self {
            base,
            owed: Arc::new(HopscotchMap::with_hasher(RandomState::default())),
        })
    }

    fn run(
        &self,
        id: &TransformId,
        docs: LensDocStream,
        inverse: bool,
    ) -> lens::Result<LensDocResultStream> {
        match self.owed.get(id) {
            Some(config) => Ok(self.run_after_registration(id.clone(), config, docs, inverse)),
            None if inverse => self.base.inverse(id, docs),
            None => self.base.transform(id, docs),
        }
    }

    /// Registers the owed lens `id`, then runs it over `docs`.
    fn run_after_registration(
        &self,
        id: TransformId,
        config: Arc<LensConfig>,
        docs: LensDocStream,
        inverse: bool,
    ) -> LensDocResultStream {
        let base = Arc::clone(&self.base);
        let owed = Arc::clone(&self.owed);
        Box::pin(
            futures::stream::once(async move {
                register(base.as_ref(), &owed, &id, &config)
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(
                            transform_id = %id,
                            %error,
                            "An owed lens registration failed again"
                        );
                    })?;
                tracing::info!(transform_id = %id, "Registered an owed committed lens");
                if inverse {
                    base.inverse(&id, docs)
                } else {
                    base.transform(&id, docs)
                }
            })
            .try_flatten(),
        )
    }
}

/// Registers `id` in `base`. A registered lens is owed no longer.
async fn register(
    base: &dyn TransformStore,
    owed: &Owed,
    id: &TransformId,
    config: &LensConfig,
) -> lens::Result<()> {
    base.add_with_id(id.clone(), config.clone()).await?;
    owed.remove(id);
    Ok(())
}

#[async_trait]
impl TransformStore for CommittedLensStore {
    fn new_isolated_store(&self) -> Arc<dyn TransformStore> {
        self.base.new_isolated_store()
    }

    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        self.base.add(config).await
    }

    /// Registers a committed lens. One that cannot be registered now is owed,
    /// and the error is returned for the caller to report.
    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> lens::Result<()> {
        let config = Arc::new(config);
        register(self.base.as_ref(), &self.owed, &id, &config)
            .await
            .inspect_err(|_| {
                self.owed.insert(id.clone(), Arc::clone(&config));
            })
    }

    async fn list(&self) -> lens::Result<RapidHashMap<String, lens::LensModule>> {
        let mut modules = self.base.list().await?;
        for (id, config) in self.owed.iter() {
            if let Some(module) = config.lens() {
                modules
                    .entry(id.to_string())
                    .or_insert_with(|| module.clone());
            }
        }
        Ok(modules)
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        self.run(id, docs, false)
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        self.run(id, docs, true)
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.base.has_transform(id) || self.owed.contains_key(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        let owed = self.owed.remove(id).is_some();
        match self.base.remove(id).await {
            Err(lens::Error::TransformNotFound(_)) if owed => Ok(()),
            outcome => outcome,
        }
    }
}

impl<S> DB<S>
where
    S: Store,
{
    /// Registers a lens whose config this database has durably committed. The
    /// commit stands whatever happens here: a registration that fails is
    /// reported and owed, and runs again when the lens is next used.
    pub(crate) async fn register_committed_lens(&self, id: TransformId, config: LensConfig) {
        if let Err(error) = self.lens_store.add_with_id(id.clone(), config).await {
            tracing::warn!(
                transform_id = %id,
                %error,
                "A committed lens could not be registered yet; it is registered when next used"
            );
        }
    }
}
