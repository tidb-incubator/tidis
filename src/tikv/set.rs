use super::client::get_version_for_new;
use super::errors::*;
use super::gen_next_meta_index;
use super::get_txn_client;
use super::KEY_ENCODER;
use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
};
use crate::async_del_set_threshold_or_default;
use crate::async_expire_set_threshold_or_default;
use crate::utils::{key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil};
use crate::Frame;
use ::futures::future::FutureExt;
use rand::prelude::SliceRandom;
use std::convert::TryInto;
use std::sync::Arc;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;

const RANDOM_BASE: i64 = 100;

#[derive(Clone)]
pub struct SetCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl SetCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        SetCommandCtx { txn }
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

    pub async fn do_async_txnkv_sadd(
        mut self,
        key: &str,
        members: &Vec<String>,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let rand_idx = gen_next_meta_index();

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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let mut expired = false;
                            let (ttl, mut version, _meta_size) =
                                KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                expired = true;
                                version = get_version_for_new(&key, txn_rc.clone()).await?;
                                txn = txn_rc.lock().await;
                            }
                            let mut added: i64 = 0;

                            for m in &members {
                                // check member already exists
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_set_data_key(&key, m, version);
                                let member_exists = txn.key_exists(data_key.clone()).await?;
                                if !member_exists {
                                    added += 1;
                                    txn.put(data_key, vec![]).await?;
                                }
                            }
                            // choose a random sub meta key for update, create if not exists
                            let sub_meta_key =
                                KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                            let new_sub_meta_value =
                                txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                    || added,
                                    |value| {
                                        let old_sub_meta_value =
                                            i64::from_be_bytes(value.try_into().unwrap());
                                        old_sub_meta_value + added
                                    },
                                );
                            txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                .await?;

                            // create a new meta key if key already expired above
                            if expired {
                                let new_meta_value =
                                    KEY_ENCODER.encode_txnkv_set_meta_value(0, version, 0);
                                txn.put(meta_key, new_meta_value).await?;
                            }

                            Ok(added)
                        }
                        None => {
                            drop(txn);
                            let version = get_version_for_new(&key, txn_rc.clone()).await?;
                            txn = txn_rc.lock().await;

                            let mut added: i64 = 0;
                            // create new meta key and meta value
                            for m in &members {
                                // check member already exists
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_set_data_key(&key, m, version);
                                let member_exists = txn.key_exists(data_key.clone()).await?;
                                if !member_exists {
                                    txn.put(data_key, vec![]).await?;
                                    added += 1;
                                }
                            }
                            // create meta key
                            let meta_value = KEY_ENCODER.encode_txnkv_set_meta_value(0, version, 0);
                            txn.put(meta_key, meta_value).await?;

                            // create sub meta key with a random index
                            let sub_meta_key =
                                KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                            txn.put(sub_meta_key, added.to_be_bytes().to_vec()).await?;
                            Ok(added)
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

    pub async fn do_async_txnkv_scard(mut self, key: &str) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let key = key.to_owned();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(meta_key).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            drop(txn);
                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                return Ok(resp_int(0));
                            }

                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            Ok(resp_int(size))
                        }
                        None => Ok(resp_int(0)),
                    }
                }
                .boxed()
            })
            .await
    }

    // return interger reply if members.len() == 1, else arrary
    // called by SISMEMBER and SMISMEMBER
    pub async fn do_async_txnkv_sismember(
        mut self,
        key: &str,
        members: &Vec<String>,
        resp_in_arr: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let member_len = members.len();

        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let key = key.to_owned();
        let members = members.to_owned();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(meta_key).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                if !resp_in_arr {
                                    return Ok(resp_int(0));
                                } else {
                                    return Ok(resp_array(vec![resp_int(0); member_len]));
                                }
                            }

                            if !resp_in_arr {
                                let data_key = KEY_ENCODER.encode_txnkv_set_data_key(
                                    &key,
                                    &members[0],
                                    version,
                                );
                                if txn.key_exists(data_key).await? {
                                    Ok(resp_int(1))
                                } else {
                                    Ok(resp_int(0))
                                }
                            } else {
                                let mut resp = vec![];
                                for m in &members {
                                    let data_key =
                                        KEY_ENCODER.encode_txnkv_set_data_key(&key, m, version);
                                    if txn.key_exists(data_key).await? {
                                        resp.push(resp_int(1));
                                    } else {
                                        resp.push(resp_int(0));
                                    }
                                }
                                Ok(resp_array(resp))
                            }
                        }
                        None => {
                            if !resp_in_arr {
                                Ok(resp_int(0))
                            } else {
                                Ok(resp_array(vec![resp_int(0); member_len]))
                            }
                        }
                    }
                }
                .boxed()
            })
            .await
    }

    // The rand is a fake algirithm for performance, the members will be random
    // get from first 100 elements, if count is above 100, `count` members will be
    // returned, so client should not be strongly rely on the random behavior
    pub async fn do_async_txnkv_srandmemeber(
        mut self,
        key: &str,
        count: i64,
        repeatable: bool,
        array_resp: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let key = key.to_owned();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;

                    match txn.get(meta_key).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                return Ok(resp_array(vec![]));
                            }

                            // create random
                            let mut rng = SmallRng::from_entropy();

                            let mut ele_count = RANDOM_BASE;
                            if count > RANDOM_BASE {
                                ele_count = count;
                            }

                            let bound_range =
                                KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);
                            let iter = txn
                                .scan_keys(bound_range, ele_count.try_into().unwrap())
                                .await?;

                            let mut resp: Vec<Frame> = iter
                                .map(|k| {
                                    // decode member from data key
                                    let user_key =
                                        KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                                    resp_bulk(user_key)
                                })
                                .collect();

                            // shuffle the resp vector
                            resp.shuffle(&mut rng);

                            let resp_len = resp.len();

                            if !array_resp {
                                assert!(count == 1);

                                // called with no count argument, return bulk reply
                                // choose a random from resp
                                let rand_idx = rng.gen_range(0..resp_len);
                                return Ok(resp[rand_idx].clone());
                            }

                            // check resp is enough when repeatable is set, fill it with random element in resp vector
                            while repeatable && (resp.len() as i64) < count {
                                let rand_idx = rng.gen_range(0..resp_len);
                                resp.push(resp[rand_idx].clone());
                            }

                            // if count is less than resp.len(), truncate it
                            if count < resp_len as i64 {
                                resp.truncate(count.try_into().unwrap());
                            }

                            Ok(resp_array(resp))
                        }
                        None => {
                            if array_resp {
                                Ok(resp_array(vec![]))
                            } else {
                                assert!(count == 1);
                                Ok(resp_nil())
                            }
                        }
                    }
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_txnkv_smembers(mut self, key: &str) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);
        let key = key.to_owned();

        client
            .exec_in_txn(self.txn.clone(), |txn_rc| {
                async move {
                    if self.txn.is_none() {
                        self.txn = Some(txn_rc.clone());
                    }

                    let mut txn = txn_rc.lock().await;
                    match txn.get(meta_key).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                return Ok(resp_array(vec![]));
                            }

                            let bound_range =
                                KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);

                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;

                            let resp = iter
                                .map(|k| {
                                    // decode member from data key
                                    let user_key =
                                        KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                                    resp_bulk(user_key)
                                })
                                .collect();

                            Ok(resp_array(resp))
                        }
                        None => Ok(resp_array(vec![])),
                    }
                }
                .boxed()
            })
            .await
    }

    pub async fn do_async_txnkv_srem(
        mut self,
        key: &str,
        members: &Vec<String>,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let rand_idx = gen_next_meta_index();

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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                return Ok(0);
                            }

                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;

                            let mut removed: i64 = 0;
                            for member in &members {
                                // check member exists
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_set_data_key(&key, member, version);
                                if txn.key_exists(data_key.clone()).await? {
                                    removed += 1;
                                    txn.delete(data_key).await?;
                                }
                            }
                            // check if all items cleared, delete meta key and all sub meta keys if needed
                            if removed >= size {
                                txn.delete(meta_key).await?;
                                let meta_bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(meta_bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }
                            } else {
                                // choose a random sub meta key, update it
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                                let new_sub_meta_value =
                                    txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                        || -removed,
                                        |v| {
                                            let old_sub_meta_value =
                                                i64::from_be_bytes(v.try_into().unwrap());
                                            old_sub_meta_value - removed
                                        },
                                    );
                                txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                    .await?;
                            }

                            Ok(removed)
                        }
                        None => Ok(0),
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(v) => Ok(resp_int(v as i64)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    /// spop will pop members by alphabetical order
    pub async fn do_async_txnkv_spop(mut self, key: &str, count: u64) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(&key);
        let rand_idx = gen_next_meta_index();

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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Set) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_set_expire_if_needed(&key)
                                    .await?;
                                return Ok(vec![]);
                            }

                            let bound_range =
                                KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);
                            let iter = txn
                                .scan_keys(bound_range, count.try_into().unwrap())
                                .await?;

                            let mut data_key_to_delete = vec![];
                            let resp = iter
                                .map(|k| {
                                    data_key_to_delete.push(k.clone());
                                    // decode member from data key
                                    let member =
                                        KeyDecoder::decode_key_set_member_from_datakey(&key, k);
                                    resp_bulk(member)
                                })
                                .collect();

                            let poped_count = data_key_to_delete.len() as i64;
                            for k in data_key_to_delete {
                                txn.delete(k).await?;
                            }

                            drop(txn);
                            // txn will be lock inner txnkv_sum_key_size, so release it first
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;

                            // update or delete meta key
                            if poped_count >= size {
                                // delete meta key
                                txn.delete(meta_key).await?;
                                // delete all sub meta keys
                                let meta_bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan(meta_bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }
                            } else {
                                // update random meta key
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                                let new_sub_meta_value =
                                    txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                        || -poped_count,
                                        |v| {
                                            let old_sub_meta_value =
                                                i64::from_be_bytes(v.try_into().unwrap());
                                            old_sub_meta_value - poped_count
                                        },
                                    );
                                txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                    .await?;
                            }
                            Ok(resp)
                        }
                        None => Ok(vec![]),
                    }
                }
                .boxed()
            })
            .await;

        match resp {
            Ok(mut v) => {
                if count == 1 {
                    if v.is_empty() {
                        Ok(resp_nil())
                    } else {
                        Ok(v.pop().unwrap())
                    }
                } else {
                    Ok(resp_array(v))
                }
            }
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_set_del(mut self, key: &str) -> AsyncResult<i64> {
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
                            let version = KeyDecoder::decode_key_version(&meta_value);

                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;

                            if size > async_del_set_threshold_or_default() as i64 {
                                // async del set
                                // do async del
                                txn.delete(meta_key).await?;

                                let gc_key = KEY_ENCODER.encode_txnkv_gc_key(&key);
                                txn.put(gc_key, version.to_be_bytes()).await?;

                                let gc_version_key =
                                    KEY_ENCODER.encode_txnkv_gc_version_key(&key, version);
                                txn.put(
                                    gc_version_key,
                                    vec![KEY_ENCODER.get_type_bytes(DataType::Set)],
                                )
                                .await?;
                            } else {
                                let sub_meta_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(sub_meta_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }

                                let data_bound_range =
                                    KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);
                                let iter = txn.scan_keys(data_bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }

                                txn.delete(meta_key).await?;
                            }

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

    pub async fn do_async_txnkv_set_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
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
                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if !key_is_expired(ttl) {
                                return Ok(0);
                            }
                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;

                            if size > async_expire_set_threshold_or_default() as i64 {
                                // async del set
                                txn.delete(meta_key).await?;

                                let gc_key = KEY_ENCODER.encode_txnkv_gc_key(&key);
                                txn.put(gc_key, version.to_be_bytes()).await?;

                                let gc_version_key =
                                    KEY_ENCODER.encode_txnkv_gc_version_key(&key, version);
                                txn.put(
                                    gc_version_key,
                                    vec![KEY_ENCODER.get_type_bytes(DataType::Set)],
                                )
                                .await?;
                            } else {
                                let sub_meta_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(sub_meta_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }

                                let data_bound_range =
                                    KEY_ENCODER.encode_txnkv_set_data_key_range(&key, version);
                                let iter = txn.scan_keys(data_bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }

                                txn.delete(meta_key).await?;
                            }
                            REMOVED_EXPIRED_KEY_COUNTER
                                .with_label_values(&["set"])
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
