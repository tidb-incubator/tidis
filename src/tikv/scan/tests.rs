use super::*;

fn rows() -> Vec<KvPair> {
    (0..600u32)
        .map(|n| KvPair::new(n.to_be_bytes().to_vec(), n.to_be_bytes().to_vec()))
        .collect()
}

fn fetch(data: &mut [KvPair], range: BoundRange, limit: u32) -> BoxFuture<'_, Result<Vec<KvPair>>> {
    async move {
        Ok(data
            .iter()
            .filter(|kv| range.contains(kv.key()))
            .take(limit as usize)
            .cloned()
            .collect())
    }
    .boxed()
}

#[tokio::test]
async fn pagination_preserves_bounds_and_limits() {
    let mut data = rows();
    // Include a key immediately after a page boundary, not just fixed-width keys.
    let mut adjacent: Vec<u8> = data[255].key().clone().into();
    adjacent.push(0);
    data.insert(256, KvPair::new(adjacent, vec![1]));
    for limit in [0, 1, 256, 257, 512, u32::MAX] {
        for range in [
            BoundRange::from((Bound::<Key>::Unbounded, Bound::<Key>::Unbounded)),
            (
                Bound::Excluded(data[0].key().clone()),
                Bound::Included(data[550].key().clone()),
            )
                .into(),
            (data[0].key().clone()..data[550].key().clone()).into(),
        ] {
            let expected = data
                .iter()
                .filter(|kv| range.contains(kv.key()))
                .take(limit as usize)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                collect_pages(data.as_mut_slice(), range, limit, fetch)
                    .await
                    .unwrap(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn second_page_error_discards_first_page() {
    let mut calls = 0;
    let result = collect_pages(&mut calls, (..).into(), u32::MAX, |calls, _, limit| {
        async move {
            *calls += 1;
            if *calls == 2 {
                return Err(tikv_client::Error::StringError("page two".into()));
            }
            Ok(rows().into_iter().take(limit as usize).collect())
        }
        .boxed()
    })
    .await;
    assert_eq!(calls, 2);
    assert!(matches!(result, Err(tikv_client::Error::StringError(s)) if s == "page two"));
}
