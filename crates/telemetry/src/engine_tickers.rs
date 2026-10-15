//! The storage engine's tickers, exported as one OpenTelemetry counter.
//!
//! Every open store hands its engine statistics to [`export_engine_tickers`]
//! and holds the returned [`EngineTickerExport`] while it is open. When the
//! guard drops, what that engine counted moves into the retired totals, so an
//! exported total never goes back when a store closes. The
//! `defradb.storage.engine.tickers` counter reports, per exported ticker, the
//! retired total plus what every live engine counts now
//! ([`engine_ticker_totals`]).
//!
//! The exported tickers are the engine's commit and policy families, read from
//! `regolith::Ticker::ALL` by [`exported_engine_tickers`]; a store's statistics
//! snapshot reports the same list through the same function.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use regolith::sync::{Atom, OnceCell};
use regolith::{Statistics, Ticker};

/// Name prefixes of the ticker families reported outside the engine.
const EXPORTED_FAMILIES: [&str; 2] = ["regolith.commit.", "regolith.policy."];

const TICKERS: usize = Ticker::ALL.len();

/// The engine tickers reported outside the engine: its commit and policy
/// families, in `Ticker::ALL` order.
pub fn exported_engine_tickers() -> impl Iterator<Item = Ticker> {
    Ticker::ALL
        .iter()
        .copied()
        .filter(|ticker| is_exported(*ticker))
}

fn is_exported(ticker: Ticker) -> bool {
    EXPORTED_FAMILIES
        .iter()
        .any(|family| ticker.name().starts_with(family))
}

/// The engines whose tickers are exported, and what the closed ones counted.
#[derive(Clone)]
struct Sources {
    live: Vec<(u64, Arc<Statistics>)>,
    /// Final totals of the engines that closed, by position in `Ticker::ALL`.
    retired: [u64; TICKERS],
}

static SOURCES: OnceCell<Atom<Sources>> = OnceCell::new();
static NEXT_SOURCE: AtomicU64 = AtomicU64::new(0);

fn sources() -> &'static Atom<Sources> {
    SOURCES.get_or_init_racy(|| {
        Atom::new(Sources {
            live: Vec::new(),
            retired: [0; TICKERS],
        })
    })
}

/// Keeps one engine's tickers in the export until it is dropped; then what the
/// engine counted stays in the exported totals.
#[must_use = "the engine leaves the export when this guard is dropped"]
pub struct EngineTickerExport {
    id: u64,
}

/// Export `statistics`' tickers until the returned guard is dropped.
pub fn export_engine_tickers(statistics: &Arc<Statistics>) -> EngineTickerExport {
    let id = NEXT_SOURCE.fetch_add(1, Ordering::Relaxed);
    sources().rcu(|current| {
        let mut next = current.clone();
        next.live.push((id, Arc::clone(statistics)));
        next
    });
    EngineTickerExport { id }
}

impl Drop for EngineTickerExport {
    fn drop(&mut self) {
        sources().rcu(|current| {
            let mut next = current.clone();
            if let Some(at) = next.live.iter().position(|(id, _)| *id == self.id) {
                let (_, statistics) = next.live.swap_remove(at);
                for (total, ticker) in next.retired.iter_mut().zip(Ticker::ALL) {
                    *total = total.wrapping_add(statistics.get_ticker(*ticker));
                }
            }
            next
        });
    }
}

/// Every exported ticker's name and total: what closed engines counted plus
/// what each live engine counts now.
pub fn engine_ticker_totals() -> Vec<(&'static str, u64)> {
    let sources = sources().load();
    Ticker::ALL
        .iter()
        .zip(sources.retired)
        .filter(|(ticker, _)| is_exported(**ticker))
        .map(|(ticker, retired)| {
            let total = sources
                .live
                .iter()
                .fold(retired, |sum, (_, statistics)| {
                    sum.wrapping_add(statistics.get_ticker(*ticker))
                });
            (ticker.name(), total)
        })
        .collect()
}

/// Register the counter that reports [`engine_ticker_totals`] at every
/// collection, one series per ticker.
#[cfg(feature = "otlp")]
pub(crate) fn register(meter: &opentelemetry::metrics::Meter) {
    let _ = meter
        .u64_observable_counter("defradb.storage.engine.tickers")
        .with_description("Storage engine commit and policy counters, by ticker")
        .with_callback(|observer| {
            for (name, total) in engine_ticker_totals() {
                observer.observe(total, &[opentelemetry::KeyValue::new("ticker", name)]);
            }
        })
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_ids() -> Vec<u64> {
        sources().load().live.iter().map(|(id, _)| *id).collect()
    }

    #[test]
    fn the_export_names_the_commit_and_policy_families_of_ticker_all() {
        let exported: Vec<Ticker> = exported_engine_tickers().collect();
        assert!(exported.contains(&Ticker::CommitCount));
        assert!(exported.contains(&Ticker::CommitConflicts));
        assert!(exported.contains(&Ticker::PolicyBlindMergesCommuted));
        assert!(!exported.contains(&Ticker::BytesWritten));
        let names: Vec<&str> = engine_ticker_totals()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        let expected: Vec<&str> = exported.iter().map(|ticker| ticker.name()).collect();
        assert_eq!(names, expected);
    }

    #[test]
    fn an_engine_is_exported_until_its_guard_drops() {
        let statistics = Arc::new(Statistics::new());
        let export = export_engine_tickers(&statistics);
        let id = export.id;
        assert!(live_ids().contains(&id));
        drop(export);
        assert!(!live_ids().contains(&id));
    }
}
