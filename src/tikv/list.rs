use super::errors::*;
use super::get_txn_client;
use super::KEY_ENCODER;
use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
};
use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;
use crate::utils::{resp_array, resp_bulk, resp_err, resp_int, resp_nil, resp_ok};
use crate::{utils::key_is_expired, Frame};
use bytes::Bytes;
use core::ops::RangeFrom;
use futures::future::FutureExt;
use std::convert::TryInto;
use std::sync::Arc;
use tikv_client::{BoundRange, Key, Transaction};
use tokio::sync::Mutex;

const INIT_INDEX: u64 = 1 << 32;

#[derive(Clone)]
pub struct ListCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl<'a> ListCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        ListCommandCtx { txn }
    }

    pub async fn do_async_txnkv_push(
        mut self,
        key: &str,
        values: &Vec<Bytes>,
        op_left: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let values = values.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, mut left, mut right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                left = INIT_INDEX;
                                right = INIT_INDEX;
                                txn = txn_rc.lock().await;
                            }

                            let mut idx: u64;
                            for value in values {
                                if op_left {
                                    left -= 1;
                                    idx = left;
                                } else {
                                    idx = right;
                                    right += 1;
                                }

                                let data_key =
                                    KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, version);
                                txn.put(data_key, value.to_vec()).await?;
                            }

                            // update meta key
                            let new_meta_value =
                                KEY_ENCODER.encode_txnkv_list_meta_value(ttl, version, left, right);
                            txn.put(meta_key, new_meta_value).await?;

                            Ok(right - left)
                        }
                        None => {
                            let mut left = INIT_INDEX;
                            let mut right = INIT_INDEX;
                            let mut idx: u64;

                            for value in values {
                                if op_left {
                                    left -= 1;
                                    idx = left
                                } else {
                                    idx = right;
                                    right += 1;
                                }

                                // add data key
                                let data_key = KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, 0);
                                txn.put(data_key, value.to_vec()).await?;
                            }

                            // add meta key
                            let meta_value =
                                KEY_ENCODER.encode_txnkv_list_meta_value(0, 0, left, right);
                            txn.put(meta_key, meta_value).await?;

                            Ok(right - left)
                        }
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(n) => Ok(resp_int(n as i64)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_pop(
        mut self,
        key: &str,
        op_left: bool,
        count: i64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut values = Vec::new();
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, mut left, mut right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                return Ok(values);
                            }

                            let mut idx: u64;

                            if count == 1 {
                                if op_left {
                                    idx = left;
                                    left += 1;
                                } else {
                                    right -= 1;
                                    idx = right;
                                }
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, version);
                                // get data and delete
                                let value = txn.get(data_key.clone()).await.unwrap().unwrap();
                                values.push(resp_bulk(value));

                                txn.delete(data_key).await?;

                                if left == right {
                                    // delete meta key
                                    txn.delete(meta_key).await?;
                                } else {
                                    // update meta key
                                    let new_meta_value = KEY_ENCODER
                                        .encode_txnkv_list_meta_value(ttl, version, left, right);
                                    txn.put(meta_key, new_meta_value).await?;
                                }
                                Ok(values)
                            } else {
                                let mut real_count = count as u64;
                                if real_count > right - left {
                                    real_count = right - left;
                                }

                                for _ in 0..real_count {
                                    if op_left {
                                        idx = left;
                                        left += 1;
                                    } else {
                                        idx = right - 1;
                                        right -= 1;
                                    }
                                    let data_key =
                                        KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, version);
                                    // get data and delete
                                    let value = txn.get(data_key.clone()).await.unwrap().unwrap();
                                    values.push(resp_bulk(value));

                                    txn.delete(data_key).await?;

                                    if left == right {
                                        // all elements poped, just delete meta key
                                        txn.delete(meta_key.clone()).await?;
                                    } else {
                                        // update meta key
                                        let new_meta_value = KEY_ENCODER
                                            .encode_txnkv_list_meta_value(
                                                ttl, version, left, right,
                                            );
                                        txn.put(meta_key.clone(), new_meta_value).await?;
                                    }
                                }
                                Ok(values)
                            }
                        }
                        None => Ok(values),
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(values) => {
                if values.is_empty() {
                    Ok(resp_nil())
                } else if values.len() == 1 {
                    Ok(values[0].clone())
                } else {
                    Ok(resp_array(values))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_ltrim(
        mut self,
        key: &str,
        mut start: i64,
        mut end: i64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, mut left, mut right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                return Ok(());
                            }

                            // convert start and end to positive
                            let len = (right - left) as i64;
                            if start < 0 {
                                start += len;
                            }
                            if end < 0 {
                                end += len;
                            }

                            // ensure the op index valid
                            if start < 0 {
                                start = 0;
                            }
                            if start > len - 1 {
                                start = len - 1;
                            }

                            if end < 0 {
                                end = 0;
                            }
                            if end > len - 1 {
                                end = len - 1;
                            }

                            // convert to relative position
                            start += left as i64;
                            end += left as i64;

                            for idx in left..start as u64 {
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, version);
                                txn.delete(data_key).await?;
                            }
                            let left_trim = start - left as i64;
                            if left_trim > 0 {
                                left += left_trim as u64;
                            }

                            // trim end+1->right
                            for idx in (end + 1) as u64..right {
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_list_data_key(&key, idx, version);
                                txn.delete(data_key).await?;
                            }

                            let right_trim = right as i64 - end - 1;
                            if right_trim > 0 {
                                right -= right_trim as u64;
                            }

                            // check key if empty
                            if left >= right {
                                // delete meta key
                                txn.delete(meta_key).await?;
                            } else {
                                // update meta key
                                let new_meta_value = KEY_ENCODER
                                    .encode_txnkv_list_meta_value(ttl, version, left, right);
                                txn.put(meta_key, new_meta_value).await?;
                            }
                            Ok(())
                        }
                        None => Ok(()),
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_lrange(
        self,
        key: &str,
        mut r_left: i64,
        mut r_right: i64,
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_list_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }

                let llen: i64 = (right - left) as i64;

                // convert negative index to positive index
                if r_left < 0 {
                    r_left += llen;
                }
                if r_right < 0 {
                    r_right += llen;
                }
                if r_left > r_right || r_left > llen {
                    return Ok(resp_array(vec![]));
                }

                let bound_range = KEY_ENCODER.encode_txnkv_list_data_key_range(key, version);
                let iter = ss.scan(bound_range, u32::MAX).await?;

                let resp = iter.map(|kv| resp_bulk(kv.1)).collect();
                Ok(resp_array(resp))
            }
            None => Ok(resp_array(vec![])),
        }
    }

    pub async fn do_async_txnkv_llen(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, _, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_list_expire_if_needed(key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let llen: i64 = (right - left) as i64;
                Ok(resp_int(llen))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_lindex(self, key: &str, mut idx: i64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, version, left, right) = KeyDecoder::decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_list_expire_if_needed(key)
                        .await?;
                    return Ok(resp_nil());
                }

                let len = right - left;
                // try convert idx to positive if needed
                if idx < 0 {
                    idx += len as i64;
                }

                let real_idx = left as i64 + idx;

                // get value from data key
                let data_key =
                    KEY_ENCODER.encode_txnkv_list_data_key(key, real_idx as u64, version);
                if let Some(value) = ss.get(data_key).await? {
                    Ok(resp_bulk(value))
                } else {
                    Ok(resp_nil())
                }
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_txnkv_lset(
        mut self,
        key: &str,
        mut idx: i64,
        ele: &Bytes,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let ele = ele.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            let (ttl, version, left, right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                return Err(REDIS_NO_SUCH_KEY_ERR);
                            }

                            // convert idx to positive is needed
                            if idx < 0 {
                                idx += (right - left) as i64;
                            }

                            let uidx = idx + left as i64;
                            if idx < 0 || uidx < left as i64 || uidx > (right - 1) as i64 {
                                return Err(REDIS_INDEX_OUT_OF_RANGE_ERR);
                            }

                            let data_key =
                                KEY_ENCODER.encode_txnkv_list_data_key(&key, uidx as u64, version);
                            // data keys exists, update it to new value
                            txn.put(data_key, ele.to_vec()).await?;
                            Ok(())
                        }
                        None => {
                            // -Err no such key
                            Err(REDIS_NO_SUCH_KEY_ERR)
                        }
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_linsert(
        mut self,
        key: &str,
        before_pivot: bool,
        pivot: &Bytes,
        element: &Bytes,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let pivot = pivot.to_owned();
        let element = element.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            let (ttl, version, mut left, mut right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                return Err(REDIS_NO_SUCH_KEY_ERR);
                            }

                            // get list items bound range
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_list_data_key_range(&key, version);

                            // iter will only return the matched kvpair
                            let mut iter = txn.scan(bound_range, u32::MAX).await?.filter(|kv| {
                                if kv.1 == pivot.to_vec() {
                                    return true;
                                }
                                false
                            });

                            // yeild the first matched kvpair
                            if let Some(kv) = iter.next() {
                                // decode the idx from data key
                                let idx = KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0);

                                // compare the pivot distance to left and right, choose the shorter one
                                let from_left = idx - left < right - idx;

                                let idx_op;
                                if from_left {
                                    idx_op = if before_pivot { idx - 1 } else { idx };
                                    // move data key from left to left-1
                                    // move backwards for elements in idx [left, idx_op], add the new element to idx_op
                                    if idx_op >= left {
                                        let left_range = KEY_ENCODER
                                            .encode_txnkv_list_data_key_idx_range(
                                                &key, left, idx_op, version,
                                            );
                                        let iter = txn.scan(left_range, u32::MAX).await?;

                                        for kv in iter {
                                            let key_idx =
                                                KeyDecoder::decode_key_list_idx_from_datakey(
                                                    &key, kv.0,
                                                );
                                            let new_data_key = KEY_ENCODER
                                                .encode_txnkv_list_data_key(
                                                    &key,
                                                    key_idx - 1,
                                                    version,
                                                );
                                            txn.put(new_data_key, kv.1).await?;
                                        }
                                    }

                                    left -= 1;
                                } else {
                                    idx_op = if before_pivot { idx } else { idx + 1 };
                                    // move data key from right to right+1
                                    // move forwards for elements in idx [idx_op, right-1], add the new element to idx_op
                                    // if idx_op == right, no need to move data key
                                    if idx_op < right {
                                        let right_range = KEY_ENCODER
                                            .encode_txnkv_list_data_key_idx_range(
                                                &key,
                                                idx_op,
                                                right - 1,
                                                version,
                                            );
                                        let iter = txn.scan(right_range, u32::MAX).await?;

                                        for kv in iter {
                                            let key_idx =
                                                KeyDecoder::decode_key_list_idx_from_datakey(
                                                    &key, kv.0,
                                                );
                                            let new_data_key = KEY_ENCODER
                                                .encode_txnkv_list_data_key(
                                                    &key,
                                                    key_idx + 1,
                                                    version,
                                                );
                                            txn.put(new_data_key, kv.1).await?;
                                        }
                                    }

                                    right += 1;
                                }

                                // fill the pivot
                                let pivot_data_key =
                                    KEY_ENCODER.encode_txnkv_list_data_key(&key, idx_op, version);
                                txn.put(pivot_data_key, element.to_vec()).await?;

                                // update meta key
                                let new_meta_value = KEY_ENCODER
                                    .encode_txnkv_list_meta_value(ttl, version, left, right);
                                txn.put(meta_key, new_meta_value).await?;

                                let len = (right - left) as i64;
                                Ok(len)
                            } else {
                                // no matched pivot, ignore
                                Ok(-1)
                            }
                        }
                        None => {
                            // when key does not exist, it is considered an empty list and no operation is performed
                            Ok(0)
                        }
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    /// LREM just support remove element from head to tail for now
    pub async fn do_async_txnkv_lrem(
        mut self,
        key: &str,
        count: usize,
        _from_head: bool,
        ele: &Bytes,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let ele = ele.to_owned();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::List) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            let (ttl, version, left, right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_list_expire_if_needed(&key)
                                    .await?;
                                return Err(REDIS_NO_SUCH_KEY_ERR);
                            }

                            let len = right - left;

                            // get list items bound range
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_list_data_key_range(&key, version);

                            // iter will only return the matched kvpair
                            let iter =
                                txn.scan(bound_range.clone(), u32::MAX).await?.filter(|kv| {
                                    if kv.1 == ele.to_vec() {
                                        return true;
                                    }
                                    false
                                });

                            // hole saves the elements to be removed in order
                            let hole: Vec<u64> = iter
                                .map(|kv| KeyDecoder::decode_key_list_idx_from_datakey(&key, kv.0))
                                .collect();

                            // no matched element, return 0
                            if hole.is_empty() {
                                return Ok(0);
                            }

                            let mut removed_count = 0;

                            let iter = txn.scan(bound_range, u32::MAX).await?;

                            for kv in iter {
                                let key_idx = KeyDecoder::decode_key_list_idx_from_datakey(
                                    &key,
                                    kv.0.clone(),
                                );
                                if hole.is_empty()
                                    || (count > 0 && removed_count == count)
                                    || removed_count == hole.len()
                                {
                                    break;
                                }

                                if hole[removed_count] == key_idx {
                                    txn.delete(kv.0).await?;
                                    removed_count += 1;
                                    continue;
                                }

                                // check if key idx need to be backward move
                                if removed_count > 0 {
                                    let new_data_key = KEY_ENCODER.encode_txnkv_list_data_key(
                                        &key,
                                        key_idx - removed_count as u64,
                                        version,
                                    );
                                    // just override the stale element
                                    txn.put(new_data_key, kv.1).await?;
                                }
                            }

                            // update meta key or delete it if no element left
                            if len == removed_count as u64 {
                                txn.delete(meta_key).await?;
                            } else {
                                let new_meta_value = KEY_ENCODER.encode_txnkv_list_meta_value(
                                    ttl,
                                    version,
                                    left,
                                    right - removed_count as u64,
                                );
                                txn.put(meta_key, new_meta_value).await?;
                            }
                            Ok(removed_count as i64)
                        }
                        None => {
                            // when key does not exist, it is considered an empty list and no operation is performed
                            Ok(0)
                        }
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_list_del(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let (_, version, left, right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            let data_key_start =
                                KEY_ENCODER.encode_txnkv_list_data_key(&key, left, version);
                            let range: RangeFrom<Key> = data_key_start..;
                            let from_range: BoundRange = range.into();
                            let len = right - left;
                            let iter = txn.scan_keys(from_range, len.try_into().unwrap()).await?;

                            for k in iter {
                                txn.delete(k).await?;
                            }
                            txn.delete(meta_key).await?;
                            Ok(1)
                        }
                        None => Ok(0),
                    }
                }
                .boxed()
            })
            .await;

        resp
    }

    pub async fn do_async_txnkv_list_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let (ttl, version, left, right) =
                                KeyDecoder::decode_key_list_meta(&meta_value);
                            if !key_is_expired(ttl) {
                                return Ok(0);
                            }
                            let data_key_start =
                                KEY_ENCODER.encode_txnkv_list_data_key(&key, left, version);
                            let range: RangeFrom<Key> = data_key_start..;
                            let from_range: BoundRange = range.into();
                            let len = right - left;
                            let iter = txn.scan_keys(from_range, len.try_into().unwrap()).await?;

                            for k in iter {
                                txn.delete(k).await?;
                            }
                            txn.delete(meta_key).await?;
                            REMOVED_EXPIRED_KEY_COUNTER
                                .with_label_values(&["list"])
                                .inc();
                            Ok(1)
                        }
                        None => Ok(0),
                    }
                }
                .boxed()
            })
            .await;

        resp
    }
}
