use super::get_txn_client;
use super::{
    encoding::{DataType, KeyDecoder, KeyEncoder},
    errors::AsyncResult,
};
use crate::{
    utils::{key_is_expired, resp_ok},
    Frame,
};
use core::ops::RangeFrom;
use futures::future::FutureExt;
use std::sync::Arc;
use tikv_client::{BoundRange, Key, KvPair, Transaction};
use tokio::sync::Mutex;

use super::errors::*;
use crate::utils::{resp_array, resp_bulk, resp_err, resp_int, resp_nil};

#[derive(Clone)]
pub struct HashCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl<'a> HashCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        HashCommandCtx { txn }
    }

    pub async fn do_async_txnkv_hset(
        mut self,
        key: &str,
        fvs: &[KvPair],
        is_hmset: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);
        let fvs_copy = fvs.to_vec();
        let fvs_len = fvs_copy.len();
        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }
                    let mut txn = txn_rc.lock().await;
                    // check if key already exists
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type is hash
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }
                            // already exists
                            let (ttl, mut size) = KeyDecoder::decode_key_hash_meta(&meta_value);
                            if key_is_expired(ttl) {
                                // release mutex
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_hash_expire_if_needed(&key)
                                    .await?;
                                size = 0;
                                // re-lock mutex
                                txn = txn_rc.lock().await;
                            }

                            for kv in fvs_copy {
                                let field: Vec<u8> = kv.0.into();
                                let datakey = KeyEncoder::new().encode_txnkv_hash_data_key(
                                    &key,
                                    &String::from_utf8_lossy(&field),
                                );
                                // check field exists
                                let field_exists = txn.key_exists(datakey.clone()).await?;
                                if !field_exists {
                                    size += 1;
                                }
                                txn.put(datakey, kv.1).await?;
                            }

                            // update meta key
                            let new_metaval =
                                KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size);
                            txn.put(meta_key, new_metaval).await?;
                        }
                        None => {
                            // not exists
                            let ttl = 0;
                            let mut size = 0;

                            for kv in fvs_copy {
                                let field: Vec<u8> = kv.0.into();
                                let datakey = KeyEncoder::new().encode_txnkv_hash_data_key(
                                    &key,
                                    &String::from_utf8_lossy(&field),
                                );
                                // check field exists
                                let field_exists = txn.key_exists(datakey.clone()).await?;
                                if !field_exists {
                                    size += 1;
                                }
                                txn.put(datakey, kv.1).await?;
                            }

                            // set meta key
                            let new_metaval =
                                KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size);
                            txn.put(meta_key, new_metaval).await?;
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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

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

                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_nil());
                }

                let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

                match ss.get(data_key).await? {
                    Some(data) => Ok(resp_bulk(data)),
                    None => Ok(resp_nil()),
                }
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_txnkv_hstrlen(self, key: &str, field: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

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

                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

                match ss.get(data_key).await? {
                    Some(data) => Ok(resp_int(data.len() as i64)),
                    None => Ok(resp_int(0)),
                }
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_hexists(self, key: &str, field: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

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

                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(&key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
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

                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }

                for field in fields {
                    let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(key, field);
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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
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

                let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_hash_expire_if_needed(key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let size = KeyDecoder::decode_key_hash_size(&meta_value);
                Ok(resp_int(size as i64))
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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        if let Some(meta_value) = ss.get(meta_key.to_owned()).await? {
            // check key type and ttl
            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
            }

            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
            if key_is_expired(ttl) {
                self.clone()
                    .do_async_txnkv_hash_expire_if_needed(key)
                    .await?;
                return Ok(resp_nil());
            }

            let size = KeyDecoder::decode_key_hash_size(&meta_value);

            let data_key_start = KeyEncoder::new().encode_txnkv_hash_data_key_start(key);
            let range: RangeFrom<Key> = data_key_start..;
            let from_range: BoundRange = range.into();
            // scan return iterator
            let iter = ss.scan(from_range, size as u32).await?;

            let resp: Vec<Frame>;
            if with_field && with_value {
                resp = iter
                    .flat_map(|kv| {
                        let fields: Vec<u8> =
                            KeyDecoder::decode_key_hash_userkey_from_datakey(key, kv.0);
                        [resp_bulk(fields), resp_bulk(kv.1)]
                    })
                    .collect();
            } else if with_field {
                resp = iter
                    .flat_map(|kv| {
                        let fields: Vec<u8> =
                            KeyDecoder::decode_key_hash_userkey_from_datakey(key, kv.0);
                        [resp_bulk(fields)]
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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, size) = KeyDecoder::decode_key_hash_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_hash_expire_if_needed(&key)
                                    .await?;
                                return Ok(0);
                            }

                            let mut deleted: i64 = 0;

                            for field in fields {
                                // check data key exists
                                let data_key =
                                    KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);
                                if txn.key_exists(data_key.clone()).await? {
                                    // delete in txn
                                    txn.delete(data_key).await?;
                                    deleted += 1;
                                }
                            }
                            // update meta key
                            let new_meta_value = KeyEncoder::new()
                                .encode_txnkv_hash_meta_value(ttl, size - deleted as u64);
                            txn.put(meta_key, new_meta_value).await?;
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
        self,
        key: &str,
        field: &str,
        step: i64,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let field = field.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);
        let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    let prev_int;
                    let mut txn = txn_rc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Hash) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, mut size) = KeyDecoder::decode_key_hash_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_hash_expire_if_needed(&key)
                                    .await?;
                                size = 0;
                                // regain txn mutexguard
                                txn = txn_rc.lock().await;
                            }

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
                                    // update meta key
                                    let new_meta_value = KeyEncoder::new()
                                        .encode_txnkv_hash_meta_value(ttl, size + 1);
                                    txn.put(meta_key, new_meta_value).await?;
                                }
                            }
                        }
                        None => {
                            prev_int = 0;
                            // create new meta key first
                            let meta_value = KeyEncoder::new().encode_txnkv_hash_meta_value(0, 1);
                            txn.put(meta_key, meta_value).await?;
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
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_arc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_arc.clone());
                    }
                    let mut txn = txn_arc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let size = KeyDecoder::decode_key_hash_size(&meta_value);

                            let data_key_start =
                                KeyEncoder::new().encode_txnkv_hash_data_key_start(&key);
                            let range: RangeFrom<Key> = data_key_start..;
                            let from_range: BoundRange = range.into();
                            // scan return iterator
                            let iter = txn.scan_keys(from_range, size as u32).await?;

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

    pub async fn do_async_txnkv_hash_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

        let resp = client
            .exec_in_txn(self.txn.clone(), |txn_arc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_arc.clone());
                    }
                    let mut txn = txn_arc.lock().await;
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            if !key_is_expired(ttl) {
                                return Ok(0);
                            }

                            let size = KeyDecoder::decode_key_hash_size(&meta_value);

                            let data_key_start =
                                KeyEncoder::new().encode_txnkv_hash_data_key_start(&key);
                            let range: RangeFrom<Key> = data_key_start..;
                            let from_range: BoundRange = range.into();
                            // scan return iterator
                            let iter = txn.scan_keys(from_range, size as u32).await?;

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
}
