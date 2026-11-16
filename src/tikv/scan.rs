//! Fallible, paginated transaction scans.
//!
//! The pinned client's streams turn page errors into EOF. Use its fallible
//! bounded API within one Region at a time, and finish reading before exposing
//! any rows. Besides preventing partial results, this keeps list shifts from reading their own
//! writes on a later page. This buffers the result, not just one page; it does
//! not impose a total transaction size limit.

use futures::future::BoxFuture;
use futures::FutureExt;
use tikv_client::{BoundRange, Key, KvPair, Result, Transaction};

mod region;
pub(super) use region::connect;

const PAGE_SIZE: u32 = 256;

struct Page {
    rows: Vec<KvPair>,
    next: Option<BoundRange>,
}

pub async fn scan(
    txn: &mut Transaction,
    range: BoundRange,
    limit: u32,
) -> Result<std::vec::IntoIter<KvPair>> {
    Ok(collect_pages(txn, range, limit, |txn, range, limit| {
        fetch_page(txn, range, limit, false).boxed()
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
        fetch_page(txn, range, limit, true).boxed()
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

/// Stop after the first matching page. Callers that move entries must collect
/// their movement range separately, before buffering overlapping writes.
pub async fn find(
    txn: &mut Transaction,
    mut range: BoundRange,
    mut predicate: impl FnMut(&KvPair) -> bool,
) -> Result<Option<KvPair>> {
    loop {
        let page = fetch_page(txn, range, PAGE_SIZE, false).await?;
        if let Some(row) = page.rows.into_iter().find(&mut predicate) {
            return Ok(Some(row));
        }
        match page.next {
            Some(next) => range = next,
            None => return Ok(None),
        }
    }
}

async fn fetch_page(
    txn: &mut Transaction,
    range: BoundRange,
    limit: u32,
    key_only: bool,
) -> Result<Page> {
    let (start, end) = range.clone().into_keys();
    // into_keys retains the pinned client's empty upper-key sentinel. Normalize
    // it before checking emptiness, clipping, and constructing the next range.
    let end = end.filter(|key| !key.is_empty());
    let range: BoundRange = (start.clone(), end.clone()).into();
    if end.as_ref().map_or(false, |end| start >= *end) {
        return Ok(Page {
            rows: vec![],
            next: None,
        });
    }
    // Refresh for each page so splits do not leave a cached whole-suffix range.
    // A split racing this lookup is handled by the client's existing retries.
    let region_end = region::end_key(&start).await?;
    let page_end = match (end, region_end) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    let page_range: BoundRange = (start, page_end.clone()).into();
    #[cfg(test)]
    failure::fail_page()?;
    let rows = if key_only {
        txn.scan_keys(page_range, limit)
            .await?
            .map(|key| KvPair::new(key, Vec::new()))
            .collect()
    } else {
        txn.scan(page_range, limit).await?.collect()
    };
    Ok(finish_page(range, page_end, limit, rows))
}

fn finish_page(range: BoundRange, page_end: Option<Key>, limit: u32, rows: Vec<KvPair>) -> Page {
    let (_, end) = range.into_keys();
    let next = if rows.len() == limit as usize {
        rows.last().map(|row| row.key().next_key())
    } else {
        page_end
    };
    let next = next
        .filter(|next| end.as_ref().map_or(true, |end| next < end))
        .map(|next| (next, end).into());
    Page { rows, next }
}

async fn collect_pages<S: ?Sized, F>(
    source: &mut S,
    mut range: BoundRange,
    mut remaining: u32,
    mut fetch: F,
) -> Result<Vec<KvPair>>
where
    F: for<'a> FnMut(&'a mut S, BoundRange, u32) -> BoxFuture<'a, Result<Page>>,
{
    let mut rows = Vec::new();
    while remaining > 0 {
        let limit = remaining.min(PAGE_SIZE);
        let page = fetch(source, range, limit).await?;
        remaining -= page.rows.len() as u32;
        rows.extend(page.rows);
        match page.next {
            Some(next) => range = next,
            None => break,
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
