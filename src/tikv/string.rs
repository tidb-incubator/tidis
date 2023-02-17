use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
    errors::RTError,
    KEY_ENCODER,
};
use crate::{
    utils::{resp_array, resp_bulk, resp_nil, resp_ok},
    Frame,
};
use ::futures::future::FutureExt;
use regex::bytes::Regex;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use tikv_client::{BoundRange, Key, KvPair, Transaction, Value};
use tokio::sync::Mutex;

use super::errors::*;
use super::{get_client, get_txn_client};
use super::{hash::HashCommandCtx, list::ListCommandCtx, set::SetCommandCtx, zset::ZsetCommandCtx};
use crate::utils::{key_is_expired, resp_err, resp_int, resp_str, sleep, ttl_from_timestamp};
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

    pub async fn do_async_txnkv_get(mut self, key: &str) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();

        // if get is executed from a new transaction, we can do get with latest commit
        if self.txn.is_none() {
            let readonly_txn = client.begin_with_latest();
            self.txn = Some(Arc::new(Mutex::new(readonly_txn)));
        }

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(ekey).await? {
                        Some(val) => {
                            let dt = KeyDecoder::decode_key_type(&val);
                            if !matches!(dt, DataType::String) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            // ttl saved in milliseconds
                            let ttl = KeyDecoder::decode_key_ttl(&val);
                            if key_is_expired(ttl) {
                                // delete key
                                drop(txn);
                                self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                return Ok(resp_nil());
                            }

                            let data = KeyDecoder::decode_key_string_value(&val);
                            Ok(resp_bulk(data))
                        }
                        None => Ok(resp_nil()),
                    }
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_rawkv_type(&self, key: &str) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);

        match client.get(ekey).await? {
            Some(val) => Ok(resp_str(&KeyDecoder::decode_key_type(&val).to_string())),
            None => Ok(resp_str(&DataType::Null.to_string())),
        }
    }

    pub async fn do_async_txnkv_type(mut self, key: &str) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();

        // if get is executed from a new transaction, we can do get with latest commit
        if self.txn.is_none() {
            let readonly_txn = client.begin_with_latest();
            self.txn = Some(Arc::new(Mutex::new(readonly_txn)));
        }

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;
                    match txn.get(ekey).await? {
                        Some(val) => {
                            let ttl = KeyDecoder::decode_key_ttl(&val);
                            if key_is_expired(ttl) {
                                // delete key
                                drop(txn);
                                self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                return Ok(resp_str(&DataType::Null.to_string()));
                            }

                            let dt = KeyDecoder::decode_key_type(&val);
                            Ok(resp_str(&dt.to_string()))
                        }
                        None => Ok(resp_str(&DataType::Null.to_string())),
                    }
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_rawkv_strlen(&self, key: &str) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        match client.get(ekey).await? {
            Some(val) => Ok(Frame::Integer(val.len() as i64)),
            None => Ok(Frame::Integer(0)),
        }
    }

    pub async fn do_async_txnkv_strlen(mut self, key: &str) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();

        // if get is executed from a new transaction, we can do get with latest commit
        if self.txn.is_none() {
            let readonly_txn = client.begin_with_latest();
            self.txn = Some(Arc::new(Mutex::new(readonly_txn)));
        }

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(ekey).await? {
                        Some(val) => {
                            let dt = KeyDecoder::decode_key_type(&val);
                            if !matches!(dt, DataType::String) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            // ttl saved in milliseconds
                            let ttl = KeyDecoder::decode_key_ttl(&val);
                            if key_is_expired(ttl) {
                                // delete key
                                drop(txn);
                                self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                return Ok(resp_int(0));
                            }

                            let data = KeyDecoder::decode_key_string_value(&val);
                            Ok(resp_int(data.len() as i64))
                        }
                        None => Ok(resp_int(0)),
                    }
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_rawkv_put(self, key: &str, val: &Bytes) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KEY_ENCODER.encode_rawkv_string(key);
        client.put(ekey, val.to_vec()).await?;
        Ok(resp_ok())
    }

    pub async fn do_async_txnkv_put(
        mut self,
        key: &str,
        val: &Bytes,
        timestamp: u64,
        return_prev: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();
        let eval = KEY_ENCODER.encode_txnkv_string_value(&mut val.to_vec(), timestamp);
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;

                    // GET option to return overwritten value
                    let prev_value = if return_prev {
                        match txn.get(ekey.clone()).await? {
                            Some(val) => {
                                let dt = KeyDecoder::decode_key_type(&val);
                                if !matches!(dt, DataType::String) {
                                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                                }

                                let ttl = KeyDecoder::decode_key_ttl(&val);
                                if key_is_expired(ttl) {
                                    // delete key
                                    drop(txn);
                                    self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                    txn = txn_rc.lock().await;

                                    None
                                } else {
                                    let data = KeyDecoder::decode_key_string_value(&val);

                                    Some(data)
                                }
                            }
                            None => None,
                        }
                    } else {
                        None
                    };

                    txn.put(ekey, eval).await?;

                    if return_prev {
                        match prev_value {
                            Some(data) => Ok(resp_bulk(data)),
                            None => Ok(resp_nil()),
                        }
                    } else {
                        Ok(resp_ok())
                    }
                }
                .boxed()
            })
            .await;

        resp
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

    pub async fn do_async_txnkv_batch_get(mut self, keys: &[String]) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekeys = KEY_ENCODER.encode_txnkv_strings(keys);

        // if get is executed from a new transaction, we can do get with latest commit
        if self.txn.is_none() {
            let readonly_txn = client.begin_with_latest();
            self.txn = Some(Arc::new(Mutex::new(readonly_txn)));
        }

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;
                    let result = txn.batch_get(ekeys.clone()).await?;
                    let ret: HashMap<Key, Value> =
                        result.into_iter().map(|pair| (pair.0, pair.1)).collect();

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
                .boxed()
            })
            .await
    }

    pub async fn do_async_rawkv_batch_put(self, kvs: Vec<KvPair>) -> AsyncResult<Frame> {
        let client = get_client()?;
        client.batch_put(kvs).await?;
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
        return_prev: bool,
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
                            // Overwrite the expired value
                            txn.put(ekey, eval).await?;

                            Ok((1, None))
                        } else {
                            let data = KeyDecoder::decode_key_string_value(v);

                            Ok((0, Some(data)))
                        }
                    } else {
                        txn.put(ekey, eval).await?;

                        Ok((1, None))
                    }
                }
                .boxed()
            })
            .await;

        // If `return_number` (for SETNX) then return 1 if the value was written
        // or 0 if not.
        // If `return_prev` (SET [GET]) then return the overwritten value or nil
        // if no value was overwritten.
        // If neither (SET [NX]) return OK if the value was written or nil if not.
        match resp {
            Ok((n, prev)) => match (return_number, return_prev) {
                (true, false) => Ok(resp_int(n as i64)),
                (false, true) => match prev {
                    Some(data) => Ok(resp_bulk(data)),
                    None => Ok(resp_nil()),
                },
                (false, false) => match n {
                    0 => Ok(resp_nil()),
                    _ => Ok(resp_ok()),
                },
                (true, true) => Err("Incompatible return options".into()),
            },
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_put_if_exists(
        mut self,
        key: &str,
        value: &Bytes,
        return_prev: bool,
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
                            drop(txn);
                            self.do_async_txnkv_string_expire_if_needed(&key).await?;

                            Ok((0, None))
                        } else {
                            txn.put(ekey, eval).await?;

                            let data = KeyDecoder::decode_key_string_value(v);

                            Ok((1, Some(data)))
                        }
                    } else {
                        Ok((0, None))
                    }
                }
                .boxed()
            })
            .await;

        // `return_prev` for GET option
        match resp {
            Ok((n, prev)) => {
                if return_prev {
                    match prev {
                        Some(data) => Ok(resp_bulk(data)),
                        None => Ok(resp_nil()),
                    }
                } else {
                    match n {
                        0 => Ok(resp_nil()),
                        _ => Ok(resp_ok()),
                    }
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

    pub async fn do_async_txnkv_exists(mut self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let keys = keys.to_owned();

        // if get is executed from a new transaction, we can do get with latest commit
        if self.txn.is_none() {
            let readonly_txn = client.begin_with_latest();
            self.txn = Some(Arc::new(Mutex::new(readonly_txn)));
        }

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut cnt = 0;
                    let ekeys = KEY_ENCODER.encode_txnkv_strings(&keys);
                    let kv_map: HashMap<Key, Value> = txn_rc
                        .lock()
                        .await
                        .batch_get(ekeys.clone())
                        .await?
                        .into_iter()
                        .map(|pair| (pair.0, pair.1))
                        .collect();

                    assert_eq!(ekeys.len(), keys.len());
                    for idx in 0..keys.len() {
                        if let Some(v) = kv_map.get(&ekeys[idx]) {
                            let ttl = KeyDecoder::decode_key_ttl(v);
                            if key_is_expired(ttl) {
                                self.clone()
                                    .do_async_txnkv_string_expire_if_needed(&keys[idx])
                                    .await?;
                            } else {
                                cnt += 1;
                            }
                        }
                    }

                    Ok(resp_int(cnt as i64))
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_rawkv_incr(self, key: &str, step: i64) -> AsyncResult<Frame> {
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

            new_int = prev_int + step;
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

    pub async fn do_async_txnkv_incr(mut self, key: &str, step: i64) -> AsyncResult<Frame> {
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
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_string_expire_if_needed(&key)
                                    .await?;
                                txn = txn_rc.lock().await;
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

                    let new_int = prev_int + step;
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

        client
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
            .await
    }

    pub async fn do_async_txnkv_string_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();

        client
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
            .await
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
                    match txn.get(ekey.clone()).await? {
                        Some(meta_value) => {
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            if timestamp == 0 && ttl == 0 {
                                // this is a persist command
                                // check old ttl first, no need to perform op
                                return Ok(0);
                            }
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

    pub async fn do_async_txnkv_ttl(mut self, key: &str, is_millis: bool) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(key);
        let key = key.to_owned();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(ekey).await? {
                        Some(meta_value) => {
                            let dt = KeyDecoder::decode_key_type(&meta_value);
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                match dt {
                                    DataType::String => {
                                        self.do_async_txnkv_string_expire_if_needed(&key).await?;
                                    }
                                    DataType::Hash => {
                                        HashCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_hash_expire_if_needed(&key)
                                            .await?;
                                    }
                                    DataType::Set => {
                                        SetCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_set_expire_if_needed(&key)
                                            .await?;
                                    }
                                    DataType::List => {
                                        ListCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_list_expire_if_needed(&key)
                                            .await?;
                                    }
                                    DataType::Zset => {
                                        ZsetCommandCtx::new(self.txn.clone())
                                            .do_async_txnkv_zset_expire_if_needed(&key)
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
                .boxed()
            })
            .await
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

                    let mut dts = Vec::with_capacity(keys_len);

                    let ekeys = KEY_ENCODER.encode_txnkv_strings(&keys);
                    let kv_map: HashMap<Key, Value> = txn_rc
                        .lock()
                        .await
                        .batch_get(ekeys.clone())
                        .await?
                        .into_iter()
                        .map(|pair| (pair.0, pair.1))
                        .collect();

                    assert_eq!(ekeys.len(), keys_len);
                    for ekey in &ekeys {
                        match kv_map.get(ekey) {
                            Some(v) => dts.push(KeyDecoder::decode_key_type(v)),
                            None => dts.push(DataType::Null),
                        }
                    }

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

    pub async fn do_async_txnkv_scan(
        mut self,
        start: &str,
        count: u32,
        regex: &str,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let ekey = KEY_ENCODER.encode_txnkv_string(start);
        let re = Regex::new(regex).unwrap();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut keys = vec![];
                    let mut retrieved_key_count = 0;
                    let mut next_key = vec![];
                    let mut txn = txn_rc.lock().await;

                    let mut left_bound = ekey.clone();

                    // set to a non-zore value before loop
                    let mut last_round_iter_count = 1;
                    while retrieved_key_count < count as usize {
                        if last_round_iter_count == 0 {
                            next_key = vec![];
                            break;
                        }

                        let range = left_bound.clone()..KEY_ENCODER.encode_txnkv_keyspace_end();
                        let bound_range: BoundRange = range.into();

                        // the iterator will scan all keyspace include sub metakey and datakey
                        let iter = txn.scan(bound_range, 100).await?;

                        // reset count to zero
                        last_round_iter_count = 0;
                        for kv in iter {
                            // skip the left bound key, this should be exclusive
                            if kv.0 == left_bound {
                                continue;
                            }
                            left_bound = kv.0.clone();
                            // left bound key is exclusive
                            last_round_iter_count += 1;

                            let (userkey, is_meta_key) =
                                KeyDecoder::decode_key_userkey_from_metakey(&kv.0);

                            // skip it if it is not a meta key
                            if !is_meta_key {
                                continue;
                            }

                            let ttl = KeyDecoder::decode_key_ttl(&kv.1);
                            // delete it if it is expired
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_del(&vec![
                                        String::from_utf8_lossy(&userkey).to_string()
                                    ])
                                    .await?;
                                txn = txn_rc.lock().await;
                            }
                            if retrieved_key_count == (count - 1) as usize {
                                next_key = userkey.clone();
                                retrieved_key_count += 1;
                                if re.is_match(&userkey) {
                                    keys.push(resp_bulk(userkey));
                                }
                                break;
                            }
                            retrieved_key_count += 1;
                            if re.is_match(&userkey) {
                                keys.push(resp_bulk(userkey));
                            }
                        }
                    }
                    let resp_next_key = resp_bulk(next_key);
                    let resp_keys = resp_array(keys);

                    Ok(resp_array(vec![resp_next_key, resp_keys]))
                }
                .boxed()
            })
            .await
    }
}
