//! A batch's document blocks computed in parallel: the documents are split
//! into runs spread over a pool's workers and the calling task, which
//! computes its share while the workers compute theirs, one run per poll.
//! Each run is moved to where it is computed and comes back with its blocks,
//! so no document is copied, each worker is handed its runs with one wake,
//! and the results keep the documents' order.

use defra_core::RequestContext;
use document::Document;
use query::prepared::PreparedInputMemo;

use crate::block::builder::{
    compute_document_blocks_with_inputs, BuildError, ComputedBlocks, DocStorageIdentity,
};

/// Documents per run at most: a few milliseconds of compute, so a run never
/// holds a worker near its stuck limit.
const RUN: usize = 32;

/// A document and its blocks, or why they could not be built.
pub(super) type Computed = (Document, Result<ComputedBlocks, BuildError>);

/// Every document's blocks, in the documents' order: the runs of every lane
/// but the first on `pool`'s workers, the first lane's here; all of them here
/// without a pool.
pub(super) async fn compute_blocks(
    pool: Option<&workers::Pool>,
    docs: Vec<(Document, DocStorageIdentity)>,
    schema: &str,
    context: &RequestContext,
    memo: Option<&PreparedInputMemo>,
) -> query::error::Result<Vec<Computed>> {
    let count = docs.len();
    let lanes = pool.map_or(0, workers::Pool::workers) + 1;
    let runs = count.div_ceil(RUN).div_ceil(lanes) * lanes;
    let per_run = count.div_ceil(runs.max(1)).max(1);
    let mut docs = docs.into_iter();
    let mut here = Vec::new();
    let mut there = Vec::new();
    let mut order = Vec::with_capacity(runs);
    for index in 0..runs {
        let run: Vec<_> = docs.by_ref().take(per_run).collect();
        if run.is_empty() {
            break;
        }
        let lane = index % lanes;
        order.push(lane == 0);
        if lane == 0 {
            here.push(run);
            continue;
        }
        let (schema, context, memo) = (schema.to_owned(), context.clone(), memo.cloned());
        let job = move || async move { compute_run(run, &schema, &context, memo.as_ref()) };
        there.push((workers::Target::Worker(lane - 1), job));
    }
    let pending = match pool {
        Some(pool) if !there.is_empty() => pool.submit_batch(there).await,
        _ => Vec::new(),
    };
    let mut here = here.into_iter();
    let mut pending = pending.into_iter();
    let failed = |error: workers::Error| {
        query::error::QueryError::execution(format!("block computation task failed: {error}"))
    };
    let mut computed = Vec::with_capacity(count);
    let last = order.len().saturating_sub(1);
    for (index, is_here) in order.into_iter().enumerate() {
        let run = if is_here {
            let run = here
                .next()
                .map(|run| compute_run(run, schema, context, memo));
            if index < last {
                // One run per poll: the other tasks of this core run between.
                workers::yield_now().await;
            }
            run
        } else {
            match pending.next() {
                Some(completion) => Some(completion.map_err(failed)?.await.map_err(failed)?),
                None => None,
            }
        };
        computed.extend(run.into_iter().flatten());
    }
    if computed.len() != count {
        return Err(query::error::QueryError::execution(format!(
            "block computation returned {} of {count} documents",
            computed.len()
        )));
    }
    Ok(computed)
}

fn compute_run(
    run: Vec<(Document, DocStorageIdentity)>,
    schema: &str,
    context: &RequestContext,
    memo: Option<&PreparedInputMemo>,
) -> Vec<Computed> {
    run.into_iter()
        .map(|(doc, identity)| {
            let blocks = compute_document_blocks_with_inputs(
                &doc,
                schema,
                identity,
                context.encryption(),
                context.signing(),
                memo,
            );
            (doc, blocks)
        })
        .collect()
}
