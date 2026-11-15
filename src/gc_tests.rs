//! Run only against a disposable PD/TiKV API-v1 cluster:
//! TIDIS_TEST_PD=127.0.0.1:2379 cargo test --locked scan_failure_contract -- --ignored
//! This test writes data and splits regions through PD's HTTP API.

use super::*;
use crate::tikv::hash::HashCommandCtx;
use crate::tikv::list::ListCommandCtx;
use crate::tikv::scan::{fail_after_pages, find, scan, scan_keys, scan_reverse};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use tikv_client::{Key, KvPair, TransactionClient};

async fn contents(client: &TransactionClient, range: tikv_client::BoundRange) -> Vec<KvPair> {
    let mut txn = client.begin_optimistic().await.unwrap();
    let rows = txn.scan(range, 10000).await.unwrap().collect();
    txn.rollback().await.unwrap();
    rows
}

async fn split_regions(pd: &str, keys: &[Key]) {
    let keys = keys
        .iter()
        .map(|key| {
            let raw: Vec<u8> = key.clone().into();
            format!("\"{}\"", hex::encode(KEY_ENCODER.encode_bytes(&raw)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let request = hyper::Request::post(format!("http://{}/pd/api/v1/regions/split", pd))
        .header("content-type", "application/json")
        .body(hyper::Body::from(format!("{{\"split_keys\":[{}]}}", keys)))
        .unwrap();
    let response = hyper::Client::new().request(request).await.unwrap();
    assert!(response.status().is_success());
    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let compact: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(compact.contains("\"processed-percentage\":100"), "{}", body);
}

#[tokio::test]
#[ignore = "requires a disposable PD/TiKV API-v1 cluster; see module documentation"]
async fn scan_failure_contract() {
    let pd = std::env::var("TIDIS_TEST_PD").expect("set TIDIS_TEST_PD");
    crate::config::set_global_config(
        toml::from_str(
            "[server]\nmeta_key_number=1\n[backend]\nconn_concurrency=1\n\
             use_pessimistic_txn=false\n",
        )
        .unwrap(),
    );
    crate::tikv::do_async_txn_connect(vec![pd.clone()])
        .await
        .unwrap();
    let client = TransactionClient::new(vec![pd.clone()], None)
        .await
        .unwrap();
    let key = format!(
        "scan-contract-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let left = 1 << 32;
    let mut txn = client.begin_optimistic().await.unwrap();
    let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
    txn.put(
        meta_key.clone(),
        KEY_ENCODER.encode_txnkv_list_meta_value(0, 0, left, left + 600),
    )
    .await
    .unwrap();
    for n in 0..600u64 {
        txn.put(
            KEY_ENCODER.encode_txnkv_list_data_key(&key, left + n, 0),
            n.to_be_bytes(),
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
    let range = KEY_ENCODER.encode_txnkv_list_data_key_range(&key, 0);
    let original = contents(&client, range.clone()).await;
    // Split inside both the first and second page, then verify exact ordering
    // across three regions, including buffered inserts, overwrites and deletes.
    split_regions(&pd, &[original[100].0.clone(), original[400].0.clone()]).await;
    // The pinned client's empty upper key is unbounded. Exercise every adapter
    // entry point past a page boundary, alongside finite and minimum-key bounds.
    let all = contents(&client, (..).into()).await;
    let start = original[0].0.clone();
    let stop = original[599].0.clone();
    let mut txn = client.begin_optimistic().await.unwrap();
    for bounds in [
        (start.clone()..Key::EMPTY).into(),
        (start.clone()..).into(),
        (Key::EMPTY..Key::EMPTY).into(),
        (Key::EMPTY..stop.clone()).into(),
        (start.clone()..start.clone()).into(),
        (start.clone()..=start.clone()).into(),
        (
            Bound::Excluded(start.clone()),
            Bound::Included(stop.clone()),
        )
            .into(),
        (Bound::Excluded(start.clone()), Bound::Excluded(Key::EMPTY)).into(),
    ] {
        let bounds: tikv_client::BoundRange = bounds;
        let expected: Vec<_> = all
            .iter()
            .filter(|row| bounds.contains(row.key()))
            .cloned()
            .collect();
        assert_eq!(
            scan(&mut txn, bounds.clone(), 257)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected.iter().take(257).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            scan_keys(&mut txn, bounds.clone(), 257)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected
                .iter()
                .take(257)
                .map(|row| row.0.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            scan_reverse(&mut txn, bounds.clone(), 257)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected.iter().rev().take(257).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            find(&mut txn, bounds, |row| row.key() == &stop)
                .await
                .unwrap(),
            expected.iter().find(|row| row.key() == &stop).cloned()
        );
    }
    txn.rollback().await.unwrap();
    let mut txn = client.begin_optimistic().await.unwrap();
    let mut expected: BTreeMap<Key, Vec<u8>> = original
        .iter()
        .map(|kv| (kv.0.clone(), kv.1.clone()))
        .collect();
    for n in [0, 255, 256, 400, 599] {
        txn.delete(original[n].0.clone()).await.unwrap();
        expected.remove(&original[n].0);
    }
    txn.put(original[300].0.clone(), b"updated".to_vec())
        .await
        .unwrap();
    expected.insert(original[300].0.clone(), b"updated".to_vec());
    let mut inserted: Vec<u8> = original[255].0.clone().into();
    inserted.push(0);
    txn.put(inserted.clone(), b"inserted".to_vec())
        .await
        .unwrap();
    expected.insert(inserted.into(), b"inserted".to_vec());
    let expected: Vec<KvPair> = expected.into_iter().map(KvPair::from).collect();
    for limit in [0, 1, 256, 257, u32::MAX] {
        assert_eq!(
            scan(&mut txn, range.clone(), limit)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected
                .iter()
                .take(limit as usize)
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            scan_keys(&mut txn, range.clone(), limit)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected
                .iter()
                .take(limit as usize)
                .map(|kv| kv.0.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            scan_reverse(&mut txn, range.clone(), limit)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            expected
                .iter()
                .rev()
                .take(limit as usize)
                .cloned()
                .collect::<Vec<_>>()
        );
    }
    assert!(fail_after_pages(1, scan(&mut txn, range.clone(), u32::MAX))
        .await
        .is_err());
    assert!(
        fail_after_pages(1, scan_keys(&mut txn, range.clone(), u32::MAX))
            .await
            .is_err()
    );
    assert!(
        fail_after_pages(1, scan_reverse(&mut txn, range.clone(), u32::MAX))
            .await
            .is_err()
    );
    txn.rollback().await.unwrap();

    // The first pivot fits in one page; an unrelated later-page failure must
    // not turn this small operation into a whole-list scan. Roll back the
    // shared transaction so the original movement fixtures stay unchanged.
    let shared = std::sync::Arc::new(tokio::sync::Mutex::new(
        client.begin_optimistic().await.unwrap(),
    ));
    let response = fail_after_pages(
        1,
        ListCommandCtx::new(Some(shared.clone())).do_async_txnkv_linsert(
            &key,
            true,
            &Bytes::from(0u64.to_be_bytes().to_vec()),
            &Bytes::from_static(b"first"),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(response, crate::Frame::Integer(601)));
    shared.lock().await.rollback().await.unwrap();
    let response = fail_after_pages(
        1,
        ListCommandCtx::new(None).do_async_txnkv_linsert(
            &key,
            true,
            &Bytes::from(300u64.to_be_bytes().to_vec()),
            &Bytes::from_static(b"later"),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        response,
        crate::Frame::ErrorOwned(_) | crate::Frame::ErrorString(_)
    ));
    assert_eq!(contents(&client, range.clone()).await, original);

    assert!(
        fail_after_pages(1, ListCommandCtx::new(None).do_async_txnkv_list_del(&key))
            .await
            .is_err()
    );
    assert_eq!(contents(&client, range.clone()).await, original);
    // LREM first scans for matches across three Regions (four pages), then
    // scans to move entries. Fail on its second page, in each direction.
    for from_head in [true, false] {
        let response = fail_after_pages(
            5,
            ListCommandCtx::new(None).do_async_txnkv_lrem(
                &key,
                1,
                from_head,
                &Bytes::from(300u64.to_be_bytes().to_vec()),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            response,
            crate::Frame::ErrorOwned(_) | crate::Frame::ErrorString(_)
        ));
        assert_eq!(contents(&client, range.clone()).await, original);
    }
    let mut txn = client.begin_optimistic().await.unwrap();
    assert!(txn.get(meta_key).await.unwrap().is_some());
    txn.rollback().await.unwrap();

    // Moving more than a page must preserve the original values, even though
    // writes target keys that also belong to the scanned range.
    let inserted = Bytes::from(777u64.to_be_bytes().to_vec());
    let pivot = Bytes::from(300u64.to_be_bytes().to_vec());
    let response = ListCommandCtx::new(None)
        .do_async_txnkv_linsert(&key, true, &pivot, &inserted)
        .await
        .unwrap();
    assert!(matches!(response, crate::Frame::Integer(601)));
    let mut values: Vec<_> = original.iter().map(|kv| kv.1.clone()).collect();
    values.insert(300, inserted.to_vec());
    assert_eq!(
        contents(&client, range.clone())
            .await
            .into_iter()
            .map(|kv| kv.1)
            .collect::<Vec<_>>(),
        values
    );
    for (from_head, value) in [(true, inserted), (false, pivot)] {
        let response = ListCommandCtx::new(None)
            .do_async_txnkv_lrem(&key, 1, from_head, &value)
            .await
            .unwrap();
        assert!(matches!(response, crate::Frame::Integer(1)));
        values.retain(|v| v != value.as_ref());
        assert_eq!(
            contents(&client, range.clone())
                .await
                .into_iter()
                .map(|kv| kv.1)
                .collect::<Vec<_>>(),
            values
        );
    }

    for kind in [
        DataType::Hash,
        DataType::List,
        DataType::Set,
        DataType::Zset,
    ] {
        let key = format!("{}-{:?}", key, kind);
        let mut txn = client.begin_optimistic().await.unwrap();
        let sub_meta = KEY_ENCODER.encode_txnkv_sub_meta_key(&key, 0, 0);
        if !matches!(kind, DataType::List) {
            txn.put(sub_meta.clone(), 600i64.to_be_bytes())
                .await
                .unwrap();
        }
        let mut keys = Vec::new();
        for n in 0..600u64 {
            let member = format!("{:04}", n);
            let data_key = match kind {
                DataType::Hash => KEY_ENCODER.encode_txnkv_hash_data_key(&key, &member, 0),
                DataType::List => KEY_ENCODER.encode_txnkv_list_data_key(&key, n, 0),
                DataType::Set => KEY_ENCODER.encode_txnkv_set_data_key(&key, &member, 0),
                DataType::Zset => {
                    let score_key =
                        KEY_ENCODER.encode_txnkv_zset_score_key(&key, n as f64, &member, 0);
                    txn.put(score_key.clone(), vec![0]).await.unwrap();
                    keys.push(score_key);
                    KEY_ENCODER.encode_txnkv_zset_data_key(&key, &member, 0)
                }
                _ => unreachable!(),
            };
            txn.put(data_key.clone(), n.to_be_bytes()).await.unwrap();
            keys.push(data_key);
        }
        if matches!(kind, DataType::Hash) {
            txn.put(
                KEY_ENCODER.encode_txnkv_meta_key(&key),
                KEY_ENCODER.encode_txnkv_hash_meta_value(0, 0, 1),
            )
            .await
            .unwrap();
        }
        let marker = KEY_ENCODER.encode_txnkv_gc_version_key(&key, 0);
        let gc_key = KEY_ENCODER.encode_txnkv_gc_key(&key);
        txn.put(
            marker.clone(),
            vec![KEY_ENCODER.get_type_bytes(kind.clone())],
        )
        .await
        .unwrap();
        txn.put(gc_key.clone(), 0u16.to_be_bytes()).await.unwrap();
        txn.commit().await.unwrap();
        if matches!(kind, DataType::Hash) {
            assert!(fail_after_pages(
                1,
                HashCommandCtx::new(None).do_async_txnkv_hgetall(&key, true, true)
            )
            .await
            .is_err());
        }
        let (tx, rx) = mpsc::channel(1);
        let worker = GcWorker::new(0, rx, tx);
        let task = GcTask::new(kind.clone(), key.as_bytes().to_vec(), 0);
        // For zsets, also fail after the score index has been fully deleted in
        // the transaction buffer, proving that earlier mutations roll back.
        let failures = if matches!(kind, DataType::Zset) {
            vec![1, 4]
        } else {
            vec![1]
        };
        for page in failures {
            assert!(fail_after_pages(page, worker.handle_task(task.clone()))
                .await
                .is_err());
            let mut txn = client.begin_optimistic().await.unwrap();
            assert!(txn.get(marker.clone()).await.unwrap().is_some());
            assert!(txn.get(gc_key.clone()).await.unwrap().is_some());
            if !matches!(kind, DataType::List) {
                assert!(txn.get(sub_meta.clone()).await.unwrap().is_some());
            }
            assert_eq!(
                txn.batch_get(keys.clone()).await.unwrap().count(),
                keys.len()
            );
            txn.rollback().await.unwrap();
        }
        worker.handle_task(task).await.unwrap();
        let mut txn = client.begin_optimistic().await.unwrap();
        assert!(txn.get(marker).await.unwrap().is_none());
        assert!(txn.get(gc_key).await.unwrap().is_none());
        assert_eq!(txn.batch_get(keys).await.unwrap().count(), 0);
        txn.rollback().await.unwrap();
    }
}
