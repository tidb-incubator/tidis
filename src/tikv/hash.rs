use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
};
use super::{get_txn_client, KEY_ENCODER};
use crate::{
    config_meta_key_number_or_default,
    utils::{key_is_expired, resp_ok},
    Frame,
};

use futures::future::FutureExt;
use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use std::{convert::TryInto, ops::Range, sync::Arc};
use tikv_client::{BoundRange, Key, KvPair, Transaction};
use tokio::sync::Mutex;

use super::errors::*;
use crate::utils::{resp_array, resp_bulk, resp_err, resp_int, resp_nil};

#[derive(Clone)]
pub struct HashCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
    rng: SmallRng,
}

impl<'a> HashCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        HashCommandCtx {
            txn,
            rng: SmallRng::from_entropy(),
        }
    }

    #[inline(always)]
    fn gen_random_index(&mut self) -> u16 {
        let max = config_meta_key_number_or_default();
        self.rng.gen_range(0..max)
    }

    async fn txnkv_sum_key_size(self, key: &str, version: u16) -> AsyncResult<i64> {
        let client = get_txn_client()?;

        let mut ss = match self.txn {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        let bound_range = KEY_ENCODER.encode_txnkv_sub_meta_key_range(key, version);

        let iter = ss.scan(bound_range, u32::MAX).await?;

        let sum = iter
            .map(|kv| i64::from_be_bytes(kv.1.try_into().unwrap()))
            .sum();

        assert!(sum > 0);
        Ok(sum)
    }

    pub async fn do_async_txnkv_hset(
        mut self,
        key: &str,
        fvs: &[KvPair],
        is_hmset: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let fvs_copy = fvs.to_vec();
        let fvs_len = fvs_copy.len();
        let idx = self.gen_random_index();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    // check if key already exists
                    match txn.get(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type is hash
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            // already exists
                            let (ttl, version, _meta_size) =
                                KeyDecoder::decode_key_meta(&meta_value);

                            let mut expired = false;

                            if key_is_expired(ttl) {
                                // release mutex
                                drop(txn);
                                self.do_async_txnkv_hash_expire_if_needed(&key).await?;
                                expired = true;
                                // re-lock mutex
                                txn = txn_rc.lock().await;
                            }

                            let mut added_count: i64 = 0;
                            for kv in fvs_copy {
                                let field: Vec<u8> = kv.0.into();
                                let datakey = KEY_ENCODER.encode_txnkv_hash_data_key(
                                    &key,
                                    &String::from_utf8_lossy(&field),
                                    version,
                                );
                                // check field exists
                                let field_exists = txn.key_exists(datakey.clone()).await?;
                                if !field_exists {
                                    added_count += 1;
                                }
                                txn.put(datakey, kv.1).await?;
                            }

                            // gerate a random index, update sub meta key, create a new sub meta key with this index
                            let sub_meta_key =
                                KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, idx);
                            // create or update it
                            let new_sub_meta_value =
                                txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                    || added_count.to_be_bytes().to_vec(),
                                    |sub_meta_value| {
                                        let sub_size =
                                            i64::from_be_bytes(sub_meta_value.try_into().unwrap());

                                        (sub_size + added_count).to_be_bytes().to_vec()
                                    },
                                );
                            txn.put(sub_meta_key, new_sub_meta_value).await?;
                            if expired {
                                // add meta key
                                let meta_size = config_meta_key_number_or_default();
                                let new_metaval =
                                    KEY_ENCODER.encode_txnkv_hash_meta_value(ttl, 0, meta_size);
                                txn.put(meta_key, new_metaval).await?;
                            }
                        }
                        None => {
                            // not exists
                            let ttl = 0;
                            let mut added_count: i64 = 0;

                            for kv in fvs_copy {
                                let field: Vec<u8> = kv.0.into();
                                let datakey = KEY_ENCODER.encode_txnkv_hash_data_key(
                                    &key,
                                    &String::from_utf8_lossy(&field),
                                    0,
                                );
                                // check field exists
                                let field_exists = txn.key_exists(datakey.clone()).await?;
                                if !field_exists {
                                    added_count += 1;
                                }
                                txn.put(datakey, kv.1).await?;
                            }

                            // set meta key
                            let meta_size = config_meta_key_number_or_default();
                            let new_metaval =
                                KEY_ENCODER.encode_txnkv_hash_meta_value(ttl, 0, meta_size);
                            txn.put(meta_key, new_metaval).await?;

                            // set sub meta key with a random index
                            let sub_meta_key = KEY_ENCODER.encode_txnkv_sub_meta_key(&key, 0, idx);
                            txn.put(sub_meta_key, added_count.to_be_bytes().to_vec())
                                .await?;
                        }
                    }
                    Ok(fvs_len)
                }
                .boxed()
            })
            .await;
        match resp {
            Ok(num) => {
                if is_hmset {
                    Ok(resp_ok())
                } else {
                    Ok(resp_int(num as i64))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_hget(self, key: &str, field: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);

                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_nil());
                }

                let data_key = KEY_ENCODER.encode_txnkv_hash_data_key(&key, &field, version);

                ss.get(data_key)
                    .await?
                    .map_or_else(|| Ok(resp_nil()), |data| Ok(resp_bulk(data)))
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_txnkv_hstrlen(self, key: &str, field: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let data_key = KEY_ENCODER.encode_txnkv_hash_data_key(&key, &field, version);

                ss.get(data_key)
                    .await?
                    .map_or_else(|| Ok(resp_int(0)), |data| Ok(resp_int(data.len() as i64)))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_hexists(self, key: &str, field: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let data_key = KEY_ENCODER.encode_txnkv_hash_data_key(&key, &field, version);

                if ss.key_exists(data_key).await? {
                    Ok(resp_int(1))
                } else {
                    Ok(resp_int(0))
                }
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_hmget(self, key: &str, fields: &[String]) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        let mut resp = Vec::with_capacity(fields.len());

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }

                for field in fields {
                    let data_key = KEY_ENCODER.encode_txnkv_hash_data_key(key, field, version);
                    match ss.get(data_key).await? {
                        Some(data) => {
                            resp.push(resp_bulk(data));
                        }
                        None => {
                            resp.push(resp_nil());
                        }
                    }
                }
            }
            None => {
                for _ in 0..fields.len() {
                    resp.push(resp_nil());
                }
            }
        }
        Ok(resp_array(resp))
    }

    pub async fn do_async_txnkv_hlen(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let meta_size = self.txnkv_sum_key_size(key, version).await?;
                Ok(resp_int(meta_size as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_hgetall(
        self,
        key: &str,
        with_field: bool,
        with_value: bool,
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        if let Some(meta_value) = ss.get(meta_key.to_owned()).await? {
            // check key type and ttl
            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
            }

            let (ttl, version, _meta_size) = KeyDecoder::decode_key_meta(&meta_value);
            if key_is_expired(ttl) {
                self.clone()
                    .do_async_txnkv_hash_expire_if_needed(key)
                    .await?;
                return Ok(resp_nil());
            }

            let range: Range<Key> = KEY_ENCODER.encode_txnkv_hash_data_key_start(key, version)
                ..KEY_ENCODER.encode_txnkv_hash_data_key_end(key, version);
            let bound_range: BoundRange = range.into();
            // scan return iterator
            let iter = ss.scan(bound_range, u32::MAX).await?;

            let resp: Vec<Frame>;
            if with_field && with_value {
                resp = iter
                    .flat_map(|kv| {
                        let field: Vec<u8> =
                            KeyDecoder::decode_key_hash_userkey_from_datakey(key, kv.0);
                        [resp_bulk(field), resp_bulk(kv.1)]
                    })
                    .collect();
            } else if with_field {
                resp = iter
                    .flat_map(|kv| {
                        let field: Vec<u8> =
                            KeyDecoder::decode_key_hash_userkey_from_datakey(key, kv.0);
                        [resp_bulk(field)]
                    })
                    .collect();
            } else {
                resp = iter.flat_map(|kv| [resp_bulk(kv.1)]).collect();
            }

            Ok(resp_array(resp))
        } else {
            Ok(resp_array(vec![]))
        }
    }

    pub async fn do_async_txnkv_hdel(mut self, key: &str, fields: &[String]) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let fields = fields.to_vec();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    match txn.get(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            let (ttl, version, _meta_size) =
                                KeyDecoder::decode_key_meta(&meta_value);

                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_hash_expire_if_needed(&key)
                                    .await?;
                                return Ok(0);
                            }

                            let mut deleted: i64 = 0;

                            for field in &fields {
                                // check data key exists
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_hash_data_key(&key, field, version);
                                if txn.key_exists(data_key.clone()).await? {
                                    // delete in txn
                                    txn.delete(data_key).await?;
                                    deleted += 1;
                                }
                            }

                            let idx = self.gen_random_index();

                            drop(txn);
                            // txn lock will be called in txnkv_sum_key_size, so release txn lock first
                            let old_size = self.txnkv_sum_key_size(&key, version).await?;
                            // re-gain txn lock
                            txn = txn_rc.lock().await;

                            // update sub meta key or clear all meta and sub meta key if needed
                            if old_size <= deleted {
                                txn.delete(meta_key).await?;
                                let bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }
                            } else {
                                // set sub meta key with a random index
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, idx);
                                // create it with negtive value if sub meta key not exists
                                let new_size =
                                    txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                        || -deleted,
                                        |value| {
                                            let sub_size =
                                                i64::from_be_bytes(value.try_into().unwrap());
                                            sub_size - deleted
                                        },
                                    );
                                // new_size may be negtive
                                txn.put(sub_meta_key, new_size.to_be_bytes().to_vec())
                                    .await?;
                            }
                            Ok(deleted)
                        }
                        None => Ok(0),
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(n) => Ok(resp_int(n)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_hincrby(
        mut self,
        key: &str,
        field: &str,
        step: i64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let idx = self.gen_random_index();

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    let prev_int;
                    let data_key;
                    let mut txn = txn_rc.lock().await;
                    match txn.get(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let mut expired = false;

                            let (ttl, version, _meta_size) =
                                KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.do_async_txnkv_hash_expire_if_needed(&key).await?;
                                expired = true;
                                // regain txn mutexguard
                                txn = txn_rc.lock().await;
                            }

                            data_key =
                                KEY_ENCODER.encode_txnkv_hash_data_key(&key, &field, version);

                            match txn.get(data_key.clone()).await? {
                                Some(data_value) => {
                                    // try to convert to int
                                    match String::from_utf8_lossy(&data_value).parse::<i64>() {
                                        Ok(ival) => {
                                            prev_int = ival;
                                        }
                                        Err(_) => {
                                            return Err(REDIS_VALUE_IS_NOT_INTEGER_ERR);
                                        }
                                    }
                                }
                                None => {
                                    // filed not exist
                                    prev_int = 0;
                                    // add size to a random sub meta key
                                    let sub_meta_key =
                                        KEY_ENCODER.encode_txnkv_sub_meta_key(&key, 0, idx);

                                    let sub_size = txn
                                        .get_for_update(sub_meta_key.clone())
                                        .await?
                                        .map_or_else(
                                            || 1,
                                            |value| i64::from_be_bytes(value.try_into().unwrap()),
                                        );

                                    // add or update sub meta key
                                    txn.put(sub_meta_key, sub_size.to_be_bytes().to_vec())
                                        .await?;

                                    // add meta key if needed
                                    if expired {
                                        // add meta key
                                        let meta_size = config_meta_key_number_or_default();
                                        let meta_value = KEY_ENCODER
                                            .encode_txnkv_hash_meta_value(ttl, 0, meta_size);
                                        txn.put(meta_key, meta_value).await?;
                                    }
                                }
                            }
                        }
                        None => {
                            prev_int = 0;
                            // create new meta key first
                            let meta_size = config_meta_key_number_or_default();
                            let meta_value =
                                KEY_ENCODER.encode_txnkv_hash_meta_value(0, 0, meta_size);
                            txn.put(meta_key, meta_value).await?;

                            // add a sub meta key with a random index
                            let sub_meta_key = KEY_ENCODER.encode_txnkv_sub_meta_key(&key, 0, idx);
                            txn.put(sub_meta_key, 1_i64.to_be_bytes().to_vec()).await?;
                            data_key = KEY_ENCODER.encode_txnkv_hash_data_key(&key, &field, 0);
                        }
                    }
                    let new_int = prev_int + step;
                    // update data key
                    txn.put(data_key, new_int.to_string().as_bytes().to_vec())
                        .await?;

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

    pub async fn do_async_txnkv_hash_del(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_arc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_arc.clone());
                    }
                    let mut txn = txn_arc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let (_, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_hash_data_key_range(&key, version);
                            // scan return iterator
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;

                            for k in iter {
                                txn.delete(k).await?;
                            }

                            let sub_meta_bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                            let sub_meta_iter =
                                txn.scan_keys(sub_meta_bound_range, u32::MAX).await?;
                            for k in sub_meta_iter {
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

    pub async fn do_async_txnkv_hash_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_arc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_arc.clone());
                    }
                    let mut txn = txn_arc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let (ttl, version, _meta_size) =
                                KeyDecoder::decode_key_meta(&meta_value);
                            if !key_is_expired(ttl) {
                                return Ok(0);
                            }
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_hash_data_key_range(&key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;

                            for k in iter {
                                txn.delete(k).await?;
                            }

                            let sub_meta_bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                            let sub_meta_iter =
                                txn.scan_keys(sub_meta_bound_range, u32::MAX).await?;
                            for k in sub_meta_iter {
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
}
