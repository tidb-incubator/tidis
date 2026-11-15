//! Fallible, paginated transaction scans.
//!
//! The pinned client's streams turn page errors into EOF. Use its fallible
//! bounded API instead, and finish reading before exposing any rows. Besides
//! preventing partial results, this keeps list shifts from reading their own
//! writes on a later page. This buffers the result, not just one page; it does
//! not impose a total transaction size limit.

use futures::future::BoxFuture;
use futures::FutureExt;
use std::ops::{Bound, RangeBounds};
use tikv_client::{BoundRange, Key, KvPair, Result, Transaction};

const PAGE_SIZE: u32 = 256;

pub async fn scan(
    txn: &mut Transaction,
    range: BoundRange,
    limit: u32,
) -> Result<std::vec::IntoIter<KvPair>> {
    Ok(collect_pages(txn, range, limit, |txn, range, limit| {
        async move { Ok(txn.scan(range, limit).await?.collect()) }.boxed()
    })
    .await?
    .into_iter())
}

pub async fn scan_keys(
    txn: &mut Transaction,
    range: BoundRange,
    limit: u32,
) -> Result<std::vec::IntoIter<Key>> {
    Ok(collect_pages(txn, range, limit, |txn, range, limit| {
        async move {
            Ok(txn
                .scan_keys(range, limit)
                .await?
                .map(|key| KvPair::new(key, Vec::new()))
                .collect())
        }
        .boxed()
    })
    .await?
    .into_iter()
    .map(KvPair::into_key)
    .collect::<Vec<_>>()
    .into_iter())
}

pub async fn scan_reverse(
    txn: &mut Transaction,
    range: BoundRange,
    limit: u32,
) -> Result<impl Iterator<Item = KvPair>> {
    // The pinned client's non-streaming reverse RPC uses forward-only region
    // sharding. Read forwards, then reverse, so ranges spanning regions work.
    // Apply the limit AFTER reversing to select the greatest keys.
    let rows = scan(txn, range, if limit == 0 { 0 } else { u32::MAX }).await?;
    Ok(rows.rev().take(limit as usize))
}

async fn collect_pages<S: ?Sized, F>(
    source: &mut S,
    mut range: BoundRange,
    mut remaining: u32,
    mut fetch: F,
) -> Result<Vec<KvPair>>
where
    F: for<'a> FnMut(&'a mut S, BoundRange, u32) -> BoxFuture<'a, Result<Vec<KvPair>>>,
{
    let mut rows = Vec::new();
    while remaining > 0 {
        let limit = remaining.min(PAGE_SIZE);
        #[cfg(test)]
        failure::fail_page()?;
        let page = fetch(source, range.clone(), limit).await?;
        if let Some(last) = page.last() {
            range = (
                Bound::Excluded(last.key().clone()),
                range.end_bound().cloned(),
            )
                .into();
        }
        let count = page.len() as u32;
        rows.extend(page);
        remaining -= count;
        if count < limit {
            break;
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod failure {
    use super::*;

    // Tokio 1.20's macro generates a const RefCell initializer that the pinned
    // Clippy flags. Keep the allowance on that generated code only.
    #[allow(clippy::declare_interior_mutable_const)]
    mod local {
        tokio::task_local! {
            pub(super) static FAIL_AFTER_PAGES: std::cell::Cell<usize>;
        }
    }
    use local::FAIL_AFTER_PAGES;

    pub(super) fn fail_page() -> Result<()> {
        let fail = FAIL_AFTER_PAGES
            .try_with(|remaining| {
                let n = remaining.get();
                remaining.set(n.saturating_sub(1));
                n == 0
            })
            .unwrap_or(false);
        if fail {
            return Err(tikv_client::Error::StringError(
                "injected scan page failure".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn fail_after_pages<T>(
        pages: usize,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        // Task-local so injection cannot affect unrelated requests or tests.
        FAIL_AFTER_PAGES
            .scope(std::cell::Cell::new(pages), future)
            .await
    }
}

#[cfg(test)]
pub(crate) use failure::fail_after_pages;

#[cfg(test)]
mod tests;
