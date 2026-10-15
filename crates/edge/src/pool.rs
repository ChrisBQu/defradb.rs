//! The edge's timers serving the worker pool: the pool's stuck-poll monitor
//! runs here, so no worker spends polls watching the others.

use std::sync::Arc;

use crate::handle::Edge;

impl Edge {
    /// A driver for [`workers::Config::monitor`]: runs the pool's stuck-poll
    /// scan every period on this runtime's timer, until the pool is gone or
    /// this runtime stops accepting work.
    ///
    /// ```no_run
    /// # fn demo(edge: &edge::Edge) -> Result<(), workers::Error> {
    /// let pool = workers::Pool::start(workers::Config {
    ///     monitor: Some(edge.pool_monitor()),
    ///     ..workers::Config::default()
    /// })?;
    /// # let _ = pool;
    /// # Ok(())
    /// # }
    /// ```
    pub fn pool_monitor(&self) -> workers::MonitorDriver {
        let edge = self.clone();
        Arc::new(move |monitor: workers::Monitor| {
            let timer = edge.clone();
            // Detached: it ends with the pool or with the runtime.
            drop(edge.spawn(async move {
                loop {
                    timer.sleep(monitor.period()).await;
                    if timer.is_closed() || monitor.scan().is_none() {
                        return;
                    }
                }
            }));
        })
    }
}
