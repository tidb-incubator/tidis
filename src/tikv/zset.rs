use super::errors::*;
use super::gen_next_meta_index;
use super::get_txn_client;
use super::KEY_ENCODER;
use super::{
    encoding::{DataType, KeyDecoder},
    errors::AsyncResult,
};
use crate::utils::{key_is_expired, resp_array, resp_bulk, resp_err, resp_int, resp_nil};
use crate::Frame;
use ::futures::future::FutureExt;
use std::convert::TryInto;
use std::sync::Arc;
use tikv_client::{BoundRange, Transaction};
use tokio::sync::Mutex;

use crate::metrics::REMOVED_EXPIRED_KEY_COUNTER;

#[derive(Clone)]
pub struct ZsetCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl ZsetCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        ZsetCommandCtx { txn }
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

        assert!(sum >= 0);
        Ok(sum)
    }

    pub async fn do_async_txnkv_zadd(
        mut self,
        key: &str,
        members: &Vec<String>,
        scores: &Vec<f64>,
        exists: Option<bool>,
        changed_only: bool,
        _incr: bool,
    ) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let scores = scores.to_owned();

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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            let mut expired = false;
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_zset_expire_if_needed(&key)
                                    .await?;
                                expired = true;
                                txn = txn_rc.lock().await;
                            }
                            let mut updated_count = 0;
                            let mut added_count = 0;

                            for idx in 0..members.len() {
                                let data_key = KEY_ENCODER.encode_txnkv_zset_data_key(
                                    &key,
                                    &members[idx],
                                    version,
                                );
                                let new_score = scores[idx];
                                let score_key = KEY_ENCODER.encode_txnkv_zset_score_key(
                                    &key,
                                    new_score,
                                    &members[idx],
                                    version,
                                );
                                let mut member_exists = false;
                                let old_data_value = txn.get(data_key.clone()).await?;
                                let mut old_data_value_data: Vec<u8> = vec![];
                                if let Some(v) = old_data_value {
                                    member_exists = true;
                                    old_data_value_data = v;
                                }

                                if let Some(v) = exists {
                                    // NX|XX
                                    if (v && member_exists) || (!v && !member_exists) {
                                        if !member_exists {
                                            added_count += 1;
                                        }
                                        // XX Only update elements that already exists
                                        // NX Only add elements that not exists
                                        if changed_only {
                                            if !member_exists {
                                                updated_count += 1;
                                            } else {
                                                // check if score updated
                                                let old_score =
                                                    KeyDecoder::decode_key_zset_data_value(
                                                        &old_data_value_data,
                                                    );
                                                if old_score != new_score {
                                                    updated_count += 1;
                                                }
                                            }
                                        }
                                        let data_value =
                                            KEY_ENCODER.encode_txnkv_zset_data_value(new_score);
                                        txn.put(data_key, data_value).await?;

                                        // delete old score key if exists
                                        if member_exists {
                                            let old_score = KeyDecoder::decode_key_zset_data_value(
                                                &old_data_value_data,
                                            );
                                            if old_score != new_score {
                                                let old_score_key = KEY_ENCODER
                                                    .encode_txnkv_zset_score_key(
                                                        &key,
                                                        old_score,
                                                        &members[idx],
                                                        version,
                                                    );
                                                txn.delete(old_score_key).await?;
                                            }
                                        }
                                        txn.put(score_key, members[idx].clone()).await?;
                                    } else {
                                        // do not update member
                                    }
                                } else {
                                    if !member_exists {
                                        added_count += 1;
                                    }
                                    // no NX|XX argument
                                    if changed_only {
                                        if !member_exists {
                                            updated_count += 1;
                                        } else {
                                            // check if score updated
                                            let old_score = KeyDecoder::decode_key_zset_data_value(
                                                &old_data_value_data,
                                            );
                                            if old_score != new_score {
                                                updated_count += 1;
                                            }
                                        }
                                    }
                                    let data_value =
                                        KEY_ENCODER.encode_txnkv_zset_data_value(new_score);
                                    let member = members[idx].clone();
                                    txn.put(data_key, data_value).await?;

                                    // delete old score key if it exists
                                    if member_exists {
                                        let old_score = KeyDecoder::decode_key_zset_data_value(
                                            &old_data_value_data,
                                        );
                                        if old_score != new_score {
                                            let old_score_key = KEY_ENCODER
                                                .encode_txnkv_zset_score_key(
                                                    &key,
                                                    old_score,
                                                    &members[idx],
                                                    version,
                                                );
                                            txn.delete(old_score_key).await?;
                                        }
                                    }
                                    txn.put(score_key, member).await?;
                                }
                            }

                            // update or add sub meta key
                            if added_count > 0 {
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                                let new_sub_meta_value =
                                    txn.get_for_update(sub_meta_key.clone()).await?.map_or_else(
                                        || added_count,
                                        |v| {
                                            let old_sub_meta_value =
                                                i64::from_be_bytes(v.try_into().unwrap());
                                            old_sub_meta_value + added_count
                                        },
                                    );
                                txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                    .await?;
                            }

                            // add meta key if key expired above
                            if expired {
                                let new_meta_value =
                                    KEY_ENCODER.encode_txnkv_zset_meta_value(ttl, 0, 0);
                                txn.put(meta_key, new_meta_value).await?;
                            }

                            if changed_only {
                                Ok(updated_count)
                            } else {
                                Ok(added_count)
                            }
                        }
                        None => {
                            if let Some(ex) = exists {
                                if ex {
                                    // xx flag specified, do not create new key
                                    return Ok(0);
                                }
                            }
                            // create new key
                            for idx in 0..members.len() {
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_zset_data_key(&key, &members[idx], 0);
                                let score = scores[idx];
                                let member = members[idx].clone();
                                let score_key = KEY_ENCODER
                                    .encode_txnkv_zset_score_key(&key, score, &member, 0);
                                // add data key and score key
                                let data_value = KEY_ENCODER.encode_txnkv_zset_data_value(score);
                                txn.put(data_key, data_value).await?;
                                // TODO check old score key exists, in case of zadd same field with different scores?
                                txn.put(score_key, member).await?;
                            }
                            // add sub meta key
                            let sub_meta_key =
                                KEY_ENCODER.encode_txnkv_sub_meta_key(&key, 0, rand_idx);
                            txn.put(sub_meta_key, (members.len() as i64).to_be_bytes().to_vec())
                                .await?;
                            // add meta key
                            let size = members.len() as i64;
                            let new_meta_value = KEY_ENCODER.encode_txnkv_zset_meta_value(0, 0, 0);
                            txn.put(meta_key, new_meta_value).await?;
                            Ok(size)
                        }
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

    pub async fn do_async_txnkv_zcard(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_int(0));
                }

                let size = self.txnkv_sum_key_size(key, version).await?;
                Ok(resp_int(size))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_zcore(self, key: &str, member: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_nil());
                }

                let data_key = KEY_ENCODER.encode_txnkv_zset_data_key(key, member, version);
                match ss.get(data_key).await? {
                    Some(data_value) => {
                        let score = KeyDecoder::decode_key_zset_data_value(&data_value);
                        Ok(resp_bulk(score.to_string().as_bytes().to_vec()))
                    }
                    None => Ok(resp_nil()),
                }
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_txnkv_zcount(
        self,
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }

                let start_key = KEY_ENCODER.encode_txnkv_zset_score_key_score_start(
                    key,
                    min,
                    min_inclusive,
                    version,
                );
                let end_key = KEY_ENCODER.encode_txnkv_zset_score_key_score_end(
                    key,
                    max,
                    max_inclusive,
                    version,
                );
                let range = start_key..=end_key;
                let bound_range: BoundRange = range.into();
                let iter = ss.scan(bound_range, u32::MAX).await?;

                Ok(resp_int(iter.count() as i64))
            }
            None => Ok(resp_int(0)),
        }
    }

    pub async fn do_async_txnkv_zrange(
        self,
        key: &str,
        mut min: i64,
        mut max: i64,
        with_scores: bool,
        reverse: bool,
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        let mut resp = vec![];
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }
                let size = self.txnkv_sum_key_size(key, version).await?;
                // convert index to positive if negtive
                if min < 0 {
                    min += size;
                }
                if max < 0 {
                    max += size;
                }

                if reverse {
                    let r_min = size as i64 - max - 1;
                    let r_max = size as i64 - min - 1;
                    min = r_min;
                    max = r_max;
                }

                let bound_range = KEY_ENCODER.encode_txnkv_zset_score_key_range(key, version);
                let iter = ss.scan(bound_range, size.try_into().unwrap()).await?;

                let mut idx = 0;
                for kv in iter {
                    if idx < min {
                        idx += 1;
                        continue;
                    }
                    if idx > max {
                        break;
                    }
                    idx += 1;

                    // decode member key from data key
                    let member = kv.1;
                    if reverse {
                        resp.insert(0, resp_bulk(member));
                    } else {
                        resp.push(resp_bulk(member));
                    }
                    if with_scores {
                        // decode vec[u8] to f64
                        let score = KeyDecoder::decode_key_zset_score_from_scorekey(key, kv.0);
                        if reverse {
                            resp.insert(1, resp_bulk(score.to_string().as_bytes().to_vec()));
                        } else {
                            resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));
                        }
                    }
                }
                Ok(resp_array(resp))
            }
            None => Ok(resp_array(resp)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn do_async_txnkv_zrange_by_score(
        self,
        key: &str,
        mut min: f64,
        mut min_inclusive: bool,
        mut max: f64,
        mut max_inclusive: bool,
        with_scores: bool,
        reverse: bool,
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        let mut resp = vec![];
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_array(vec![]));
                }

                // if reverse is set, min and max means opposite, exchange them
                if reverse {
                    (min, max) = (max, min);
                    (min_inclusive, max_inclusive) = (max_inclusive, min_inclusive);
                }
                if min > max {
                    return Ok(resp_array(vec![]));
                }

                let size = self.txnkv_sum_key_size(key, version).await?;

                let start_key = KEY_ENCODER.encode_txnkv_zset_score_key_score_start(
                    key,
                    min,
                    min_inclusive,
                    version,
                );
                let end_key = KEY_ENCODER.encode_txnkv_zset_score_key_score_end(
                    key,
                    max,
                    max_inclusive,
                    version,
                );
                let range = start_key..end_key;
                let bound_range: BoundRange = range.into();
                let iter = ss.scan(bound_range, size.try_into().unwrap()).await?;

                for kv in iter {
                    let member = kv.1;
                    if reverse {
                        resp.insert(0, resp_bulk(member));
                    } else {
                        resp.push(resp_bulk(member));
                    }
                    if with_scores {
                        // decode score from score key
                        let score = KeyDecoder::decode_key_zset_score_from_scorekey(key, kv.0);
                        if reverse {
                            resp.insert(0, resp_bulk(score.to_string().as_bytes().to_vec()));
                        } else {
                            resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));
                        }
                    }
                }
                Ok(resp_array(resp))
            }
            None => Ok(resp_array(resp)),
        }
    }

    // pub async fn do_async_txnkv_zrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
    //     Ok(resp_nil())
    // }

    pub async fn do_async_txnkv_zpop(
        mut self,
        key: &str,
        from_min: bool,
        count: u64,
    ) -> AsyncResult<Frame> {
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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_zset_expire_if_needed(&key)
                                    .await?;
                                return Ok(vec![]);
                            }
                            let mut poped_count = 0;
                            let mut resp = vec![];
                            if from_min {
                                let bound_range =
                                    KEY_ENCODER.encode_txnkv_zset_score_key_range(&key, version);
                                let iter = txn
                                    .scan_keys(bound_range, count.try_into().unwrap())
                                    .await?;

                                for k in iter {
                                    let member = KeyDecoder::decode_key_zset_member_from_scorekey(
                                        &key,
                                        k.clone(),
                                    );
                                    let data_key = KEY_ENCODER.encode_txnkv_zset_data_key(
                                        &key,
                                        &String::from_utf8_lossy(&member),
                                        version,
                                    );

                                    // push member to resp
                                    resp.push(resp_bulk(member));
                                    // push score to resp
                                    let score = KeyDecoder::decode_key_zset_score_from_scorekey(
                                        &key,
                                        k.clone(),
                                    );
                                    resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));

                                    txn.delete(data_key).await?;
                                    txn.delete(k).await?;
                                    poped_count += 1;
                                }
                            } else {
                                // TODO not supported yet
                            }

                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;

                            // delete all sub meta keys and meta key if all members poped
                            if poped_count >= size {
                                let bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }

                                txn.delete(meta_key).await?;
                            } else {
                                // update size to a random sub meta key
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
            Ok(v) => Ok(resp_array(v)),
            Err(e) => Ok(resp_err(e)),
        }
    }

    pub async fn do_async_txnkv_zrank(self, key: &str, member: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KEY_ENCODER.encode_txnkv_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await,
        };

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone()
                        .do_async_txnkv_zset_expire_if_needed(key)
                        .await?;
                    return Ok(resp_nil());
                }

                let data_key = KEY_ENCODER.encode_txnkv_zset_data_key(key, member, version);
                match ss.get(data_key).await? {
                    Some(data_value) => {
                        // calculate the score rank in score key index
                        let score = KeyDecoder::decode_key_zset_data_value(&data_value);
                        let score_key =
                            KEY_ENCODER.encode_txnkv_zset_score_key(key, score, member, version);

                        // scan from range start
                        let bound_range =
                            KEY_ENCODER.encode_txnkv_zset_score_key_range(key, version);
                        let iter = ss.scan_keys(bound_range, u32::MAX).await?;
                        let mut rank = 0;
                        for k in iter {
                            if k == score_key {
                                break;
                            }
                            rank += 1;
                        }
                        Ok(resp_int(rank))
                    }
                    None => Ok(resp_nil()),
                }
            }
            None => Ok(resp_nil()),
        }
    }

    pub async fn do_async_txnkv_zrem(
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
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_zset_expire_if_needed(&key)
                                    .await?;
                                return Ok(0);
                            }
                            let mut removed_count = 0;

                            for member in members {
                                let data_key =
                                    KEY_ENCODER.encode_txnkv_zset_data_key(&key, &member, version);
                                // get data key and decode value as score, to generate score key
                                let score = txn.get(data_key.clone()).await?;
                                if score.is_none() {
                                    continue;
                                }

                                // decode the score vec to i64
                                let iscore =
                                    KeyDecoder::decode_key_zset_data_value(&score.unwrap());
                                // remove member and score key
                                let score_key = KEY_ENCODER
                                    .encode_txnkv_zset_score_key(&key, iscore, &member, version);
                                txn.delete(data_key).await?;
                                txn.delete(score_key).await?;
                                removed_count += 1;
                            }

                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;
                            // clear all sub meta keys and meta key if all members removed
                            if removed_count >= size {
                                let bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }
                                txn.delete(meta_key).await?;
                            } else {
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                                let new_sub_meta_value =
                                    txn.get(sub_meta_key.clone()).await?.map_or_else(
                                        || -removed_count,
                                        |v| {
                                            let old_sub_meta_value =
                                                i64::from_be_bytes(v.try_into().unwrap());
                                            old_sub_meta_value - removed_count
                                        },
                                    );
                                txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                    .await?;
                            }

                            Ok(removed_count)
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

    // pub async fn do_async_txnkv_zremrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool) -> AsyncResult<Frame> {
    //     Ok(resp_nil())
    // }

    pub async fn do_async_txnkv_zremrange_by_score(
        mut self,
        key: &str,
        min: f64,
        max: f64,
    ) -> AsyncResult<Frame> {
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
                    match txn.get_for_update(meta_key.clone()).await? {
                        Some(meta_value) => {
                            // check key type and ttl
                            if !matches!(KeyDecoder::decode_key_type(&meta_value), DataType::Zset) {
                                return Err(REDIS_WRONG_TYPE_ERR);
                            }

                            let (ttl, version, _) = KeyDecoder::decode_key_meta(&meta_value);
                            if key_is_expired(ttl) {
                                drop(txn);
                                self.clone()
                                    .do_async_txnkv_zset_expire_if_needed(&key)
                                    .await?;
                                return Ok(0);
                            }

                            // generate score key range to remove, inclusive
                            let score_key_start = KEY_ENCODER
                                .encode_txnkv_zset_score_key_score_start(&key, min, true, version);
                            let score_key_end = KEY_ENCODER
                                .encode_txnkv_zset_score_key_score_end(&key, max, true, version);

                            // remove score key and data key
                            let range = score_key_start..=score_key_end;
                            let bound_range: BoundRange = range.into();
                            let mut removed_count = 0;

                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;

                            // TODO big txn optimization
                            for k in iter {
                                let member = KeyDecoder::decode_key_zset_member_from_scorekey(
                                    &key,
                                    k.clone(),
                                );
                                // fetch this score key member
                                let data_key = KEY_ENCODER.encode_txnkv_zset_data_key(
                                    &key,
                                    &String::from_utf8_lossy(&member),
                                    version,
                                );
                                txn.delete(data_key).await?;
                                txn.delete(k).await?;
                                removed_count += 1;
                            }

                            drop(txn);
                            let size = self.txnkv_sum_key_size(&key, version).await?;
                            txn = txn_rc.lock().await;
                            // delete all sub meta keys and meta key if all members removed
                            if removed_count >= size {
                                let bound_range =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                                let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                                for k in iter {
                                    txn.delete(k).await?;
                                }
                                txn.delete(meta_key).await?;
                            } else {
                                // update a random sub meta key
                                let sub_meta_key =
                                    KEY_ENCODER.encode_txnkv_sub_meta_key(&key, version, rand_idx);
                                let new_sub_meta_value =
                                    txn.get(sub_meta_key.clone()).await?.map_or_else(
                                        || -removed_count,
                                        |v| {
                                            let old_sub_meta_value =
                                                i64::from_be_bytes(v.try_into().unwrap());
                                            old_sub_meta_value - removed_count
                                        },
                                    );
                                txn.put(sub_meta_key, new_sub_meta_value.to_be_bytes().to_vec())
                                    .await?;
                            }

                            Ok(removed_count)
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

    pub async fn do_async_txnk_zset_del(mut self, key: &str) -> AsyncResult<i64> {
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
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_zset_data_key_range(&key, version);
                            let iter = txn.scan(bound_range, u32::MAX).await?;
                            for kv in iter {
                                // kv.0 is member key
                                // kv.1 is score
                                // decode the score vec to i64
                                let score = KeyDecoder::decode_key_zset_data_value(&kv.1);

                                // decode member from data key
                                let member_vec = KeyDecoder::decode_key_zset_member_from_datakey(
                                    &key,
                                    kv.0.clone(),
                                );
                                let member = String::from_utf8_lossy(&member_vec);

                                // remove member and score key
                                let score_key = KEY_ENCODER
                                    .encode_txnkv_zset_score_key(&key, score, &member, version);
                                txn.delete(kv.0).await?;
                                txn.delete(score_key).await?;
                            }

                            // delete all sub meta keys
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
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

    pub async fn do_async_txnkv_zset_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
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
                            let ttl = KeyDecoder::decode_key_ttl(&meta_value);
                            if !key_is_expired(ttl) {
                                return Ok(0);
                            }
                            let version = KeyDecoder::decode_key_version(&meta_value);
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_zset_data_key_range(&key, version);
                            let iter = txn.scan(bound_range, u32::MAX).await?;
                            for kv in iter {
                                // kv.0 is member key
                                // kv.1 is score
                                // decode the score vec to i64
                                let score = KeyDecoder::decode_key_zset_data_value(&kv.1);

                                // decode member from data key
                                let member_vec = KeyDecoder::decode_key_zset_member_from_datakey(
                                    &key,
                                    kv.0.clone(),
                                );
                                let member = String::from_utf8_lossy(&member_vec);

                                // remove member and score key
                                let score_key = KEY_ENCODER
                                    .encode_txnkv_zset_score_key(&key, score, &member, version);
                                txn.delete(kv.0).await?;
                                txn.delete(score_key).await?;
                            }

                            // delete all sub meta keys
                            let bound_range =
                                KEY_ENCODER.encode_txnkv_sub_meta_key_range(&key, version);
                            let iter = txn.scan_keys(bound_range, u32::MAX).await?;
                            for k in iter {
                                txn.delete(k).await?;
                            }
                            txn.delete(meta_key).await?;
                            REMOVED_EXPIRED_KEY_COUNTER
                                .with_label_values(&["zset"])
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
