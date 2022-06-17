use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
    errors::RTError,
    KEY_ENCODER,
};
use crate::{
    utils::{resp_bulk, resp_nil, resp_ok},
    Frame,
};
use ::futures::future::FutureExt;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use tikv_client::{Key, KvPair, Transaction, Value};
use tokio::sync::Mutex;

use super::errors::*;
use super::{get_client, get_txn_client};
use super::{hash::HashCommandCtx, list::ListCommandCtx, set::SetCommandCtx, zset::ZsetCommandCtx};
use crate::utils::{key_is_expired, resp_err, resp_int, resp_ok_ignore, sleep, ttl_from_timestamp};
use bytes::Bytes;

use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;

#[derive(Clone)]
pub struct StringCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl StringCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        StringCommandCtx { txn }
    }

    pub async fn do_async_rawkv_get(&self, key: &str) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        match client.get(ekey).await? {
            Some(val) => Ok(Frame::Bulk(val.into())),
            None => Ok(Frame::Null),
        }
    }

    pub async fn do_async_txnkv_get(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(ekey).await? {
            Some(val) => {
                let dt = KeyDecoder::decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                // ttl saved in milliseconds
                let ttl = KeyDecoder::decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    self.do_async_txnkv_string_expire_if_needed(key).await?;
                    return Ok(resp_nil());
                }

                let data = KeyDecoder::decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_rawkv_put(self, key: &str, val: &Bytes) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        let _ = client.put(ekey, val.to_vec()).await?;
        Ok(resp_ok())
    }

    pub async fn do_async_txnkv_put(
        mut self,
        key: &str,
        val: &Bytes,
        timestamp: u64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let eval = KEY_ENCODER.encode_txnkv_string_value(&mut val.to_vec(), timestamp);
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    txn.put(ekey, eval).await?;
                    Ok(())
                }
                .boxed()
            })
            .await;
        resp.map(resp_ok_ignore)
    }

    pub async fn do_async_rawkv_batch_get(self, keys: &[String]) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekeys = KEY_ENCODER.encode_rawkv_strings(keys);
        let result = client.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();

        let values: Vec<Frame> = ekeys
            .into_iter()
            .map(|k| {
                let data = ret.get(k.as_ref());
                match data {
                    Some(val) => Frame::Bulk(val.to_owned().into()),
                    None => Frame::Null,
                }
            })
            .collect();
        Ok(Frame::Array(values))
    }

    pub async fn do_async_txnkv_batch_get(self, keys: &[String]) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekeys = KEY_ENCODER.encode_txnkv_strings(keys);

        let mut ss = match self.txn {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        let result = ss.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();

        let values: Vec<Frame> = ekeys
            .into_iter()
            .map(|k| {
                let data = ret.get(k.as_ref());
                match data {
                    Some(val) => {
                        let ttl = KeyDecoder::decode_key_ttl(val);
                        if key_is_expired(ttl) {
                            return Frame::Null;
                        }
                        let data = KeyDecoder::decode_key_string_value(val);
                        Frame::Bulk(data.into())
                    }
                    None => Frame::Null,
                }
            })
            .collect();
        Ok(Frame::Array(values))
    }

    pub async fn do_async_rawkv_batch_put(self, kvs: Vec<KvPair>) -> AsyncResult<Frame> {
        let client = get_client()?;
        let _ = client.batch_put(kvs).await?;
        Ok(resp_ok())
    }

    pub async fn do_async_txnkv_batch_put(mut self, kvs: Vec<KvPair>) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    for kv in kvs {
                        txn.put(kv.0, kv.1).await?;
                    }
                    Ok(())
                }
                .boxed()
            })
            .await;
        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_rawkv_put_not_exists(
        self,
        key: &str,
        value: &Bytes,
    ) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        let (_, swapped) = client.compare_and_swap(ekey, None, value.to_vec()).await?;
        if swapped {
            Ok(resp_ok())
        } else {
            Ok(resp_nil())
        }
    }

    pub async fn do_async_txnkv_put_not_exists(
        mut self,
        key: &str,
        value: &Bytes,
        return_number: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let ekey = KEY_ENCODER.encode_txnkv_string(&key);
        let eval = KEY_ENCODER.encode_txnkv_string_value(&mut value.to_vec(), 0);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    let old_value = txn.get(ekey.clone()).await?;
                    if let Some(ref v) = old_value {
                        let ttl = KeyDecoder::decode_key_ttl(v);
                        if key_is_expired(ttl) {
                            // no need to delete, just overwrite
                            txn.put(ekey, eval).await?;
                            return Ok(1);
                        }
                        Ok(0)
                    } else {
                        txn.put(ekey, eval).await?;
                        Ok(1)
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(n) => {
                if return_number {
                    return Ok(resp_int(n as i64));
                }
                if n == 0 {
                    Ok(resp_nil())
                } else {
                    Ok(resp_ok())
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_rawkv_exists(self, keys: &[String]) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekeys = KEY_ENCODER.encode_rawkv_strings(keys);
        let result = client.batch_get(ekeys).await?;
        let num_items = result.len();
        Ok(resp_int(num_items as i64))
    }

    pub async fn do_async_txnkv_exists(self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        let mut cnt = 0;
        for key in keys {
            let ekey = KEY_ENCODER.encode_txnkv_string(key);
            match ss.get(ekey).await? {
                Some(v) => {
                    let ttl = KeyDecoder::decode_key_ttl(&v);
                    if key_is_expired(ttl) {
                        self.clone()
                            .do_async_txnkv_string_expire_if_needed(key)
                            .await?;
                    } else {
                        cnt += 1;
                    }
                }
                None => {}
            }
        }
        Ok(resp_int(cnt as i64))
    }

    pub async fn do_async_rawkv_incr(self, key: &str, inc: bool, step: i64) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        let mut new_int: i64 = 0;
        let mut swapped = false;
        for i in 0..2000 {
            let prev: Option<Vec<u8>>;
            let prev_int: i64;
            match client.get(ekey.clone()).await? {
                Some(val) => {
                    prev_int = String::from_utf8_lossy(&val).parse::<i64>()?;
                    prev = Some(val.clone());
                }
                None => {
                    prev = None;
                    prev_int = 0;
                }
            }
            if inc {
                new_int = prev_int + step;
            } else {
                new_int = prev_int - step;
            }
            let new_val = new_int.to_string();
            let (_, ret) = client
                .compare_and_swap(ekey.clone(), prev, new_val.into())
                .await?;
            if ret {
                swapped = true;
                break;
            }
            sleep(std::cmp::min(i, 200)).await;
        }
        if !swapped {
            Err(REDIS_COMPARE_AND_SWAP_EXHAUSTED_ERR)
        } else {
            Ok(resp_int(new_int))
        }
    }

    pub async fn do_async_txnkv_incr(
        mut self,
        key: &str,
        inc: bool,
        step: i64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone())
                    }
                    let prev_int;
                    let mut txn = txn_rc.lock().await;
                    match txn.get(ekey.clone()).await? {
                        Some(val) => {
                            let ttl = KeyDecoder::decode_key_ttl(&val);
                            if key_is_expired(ttl) {
                                self.clone()
                                    .do_async_txnkv_string_expire_if_needed(&key)
                                    .await?;
                                prev_int = 0;
                            } else {
                                let real_value = KeyDecoder::decode_key_string_slice(&val);
                                prev_int = str::from_utf8(real_value)
                                    .map_err(RTError::to_is_not_integer_error)?
                                    .parse::<i64>()?;
                            }
                        }
                        None => {
                            prev_int = 0;
                        }
                    }

                    let new_int = if inc {
                        prev_int + step
                    } else {
                        prev_int - step
                    };
                    let new_val = new_int.to_string();
                    let eval =
                        KEY_ENCODER.encode_txnkv_string_value(&mut new_val.as_bytes().to_vec(), 0);
                    txn.put(ekey, eval).await?;
                    Ok(new_int)
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(n) => Ok(resp_int(n)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_string_del(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    let ekey = KEY_ENCODER.encode_txnkv_string(&key);
                    if (txn.get(ekey.to_owned()).await?).is_some() {
                        txn.delete(ekey).await?;
                        return Ok(1);
                    }
                    Ok(0)
                }
                .boxed()
            })
            .await;

        resp
    }

    pub async fn do_async_txnkv_string_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    let ekey = KEY_ENCODER.encode_txnkv_string(&key);
                    if let Some(v) = txn.get(ekey.to_owned()).await? {
                        let ttl = KeyDecoder::decode_key_ttl(&v);
                        if key_is_expired(ttl) {
                            txn.delete(ekey).await?;
                            REMOVED_EXPIRED_KEY_COUNTER
                                .with_label_values(&["string"])
                                .inc();
                            return Ok(1);
                        }
                    }
                    Ok(0)
                }
                .boxed()
            })
            .await;

        resp
    }

    pub async fn do_async_txnkv_expire(mut self, key: &str, timestamp: u64) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let ekey = KEY_ENCODER.encode_txnkv_string(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(ekey.clone()).await? {
                        Some(meta_value) => {
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            let dt = KeyDecoder::decode_key_type(&meta_value);
                            let version = KeyDecoder::decode_key_version(&meta_value);

                            match dt {
                                DataType::String => {
                                    // check key expired
                                    if key_is_expired(ttl) {
                                        drop(txn);
                                        self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                        return Ok(0);
                                    }
                                    let value = KeyDecoder::decode_key_string_slice(&meta_value);
                                    let new_meta_value =
                                        KEY_ENCODER.encode_txnkv_string_slice(value, timestamp);
                                    txn.put(ekey, new_meta_value).await?;
                                    Ok(1)
                                }
                                DataType::Hash => {
                                    if key_is_expired(ttl) {
                                        drop(txn);
                                        HashCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_hash_expire_if_needed(&key)
                                            .await?;
                                        return Ok(0);
                                    }
                                    let new_meta_value = KEY_ENCODER
                                        .encode_txnkv_hash_meta_value(timestamp, version, 0);
                                    txn.put(ekey, new_meta_value).await?;
                                    Ok(1)
                                }
                                DataType::List => {
                                    if key_is_expired(ttl) {
                                        drop(txn);
                                        ListCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_list_expire_if_needed(&key)
                                            .await?;
                                        return Ok(0);
                                    }
                                    let (_, version, left, right) =
                                        KeyDecoder::decode_key_list_meta(&meta_value);
                                    let new_meta_value = KEY_ENCODER.encode_txnkv_list_meta_value(
                                        timestamp, version, left, right,
                                    );
                                    txn.put(ekey, new_meta_value).await?;
                                    Ok(1)
                                }
                                DataType::Set => {
                                    if key_is_expired(ttl) {
                                        drop(txn);
                                        SetCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_set_expire_if_needed(&key)
                                            .await?;
                                        return Ok(0);
                                    }
                                    let new_meta_value = KEY_ENCODER
                                        .encode_txnkv_set_meta_value(timestamp, version, 0);
                                    txn.put(ekey, new_meta_value).await?;
                                    Ok(1)
                                }
                                DataType::Zset => {
                                    if key_is_expired(ttl) {
                                        drop(txn);
                                        ZsetCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_zset_expire_if_needed(&key)
                                            .await?;
                                        return Ok(0);
                                    }
                                    let new_meta_value = KEY_ENCODER
                                        .encode_txnkv_zset_meta_value(timestamp, version, 0);
                                    txn.put(ekey, new_meta_value).await?;
                                    Ok(1)
                                }
                                _ => Ok(0),
                            }
                        }
                        None => Ok(0),
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

    pub async fn do_async_txnkv_ttl(self, key: &str, is_millis: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(ekey).await? {
            Some(meta_value) => {
                let dt = KeyDecoder::decode_key_type(&meta_value);
                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    match dt {
                        DataType::String => {
                            self.do_async_txnkv_string_expire_if_needed(key).await?;
                        }
                        DataType::Hash => {
                            HashCommandCtx::new(self.txn.clone())
                                .do_async_txnkv_hash_expire_if_needed(key)
                                .await?;
                        }
                        DataType::Set => {
                            SetCommandCtx::new(self.txn.clone())
                                .do_async_txnkv_set_expire_if_needed(key)
                                .await?;
                        }
                        DataType::List => {
                            ListCommandCtx::new(self.txn.clone())
                                .do_async_txnkv_list_expire_if_needed(key)
                                .await?;
                        }
                        DataType::Zset => {
                            ZsetCommandCtx::new(self.txn.clone())
                                .do_async_txnkv_zset_expire_if_needed(key)
                                .await?;
                        }
                        _ => {}
                    }
                    return Ok(resp_int(-2));
                }

                if ttl == 0 {
                    Ok(resp_int(-1))
                } else {
                    let mut ttl = ttl_from_timestamp(ttl) as i64;
                    if !is_millis {
                        ttl /= 1000;
                    }
                    Ok(resp_int(ttl))
                }
            }
            None => Ok(resp_int(-2)),
        }
    }

    pub async fn do_async_txnkv_del(mut self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let keys = keys.to_owned();
        let keys_len = keys.len();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut dts = vec![];
                    let mut txn = txn_rc.lock().await;

                    for key in &keys {
                        let ekey = KEY_ENCODER.encode_txnkv_string(key);
                        let meta_value = txn.get(ekey).await?;
                        if let Some(v) = &meta_value {
                            let dt = KeyDecoder::decode_key_type(v);
                            dts.push(dt);
                        } else {
                            dts.push(DataType::Null);
                        }
                    }
                    drop(txn);

                    let mut resp = 0;
                    for idx in 0..keys_len {
                        match dts[idx] {
                            DataType::String => {
                                self.clone().do_async_txnkv_string_del(&keys[idx]).await?;
                                resp += 1;
                            }
                            DataType::Hash => {
                                HashCommandCtx::new(self.txn.clone())
                                    .do_async_txnkv_hash_del(&keys[idx])
                                    .await?;
                                resp += 1;
                            }
                            DataType::List => {
                                ListCommandCtx::new(self.txn.clone())
                                    .do_async_txnkv_list_del(&keys[idx])
                                    .await?;
                                resp += 1;
                            }
                            DataType::Set => {
                                SetCommandCtx::new(self.txn.clone())
                                    .do_async_txnkv_set_del(&keys[idx])
                                    .await?;
                                resp += 1;
                            }
                            DataType::Zset => {
                                ZsetCommandCtx::new(self.txn.clone())
                                    .do_async_txnk_zset_del(&keys[idx])
                                    .await?;
                                resp += 1;
                            }
                            DataType::Null => {}
                        }
                    }
                    Ok(resp)
                }
                .boxed()
            })
            .await;
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }
}
