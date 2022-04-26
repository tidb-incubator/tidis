use std::{convert::TryInto};
use std::sync::Arc;
use tokio::sync::Mutex;
use ::futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Transaction, BoundRange};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};
use crate::utils::{resp_err, resp_int, resp_array, resp_bulk, resp_nil, key_is_expired};
use super::errors::*;

#[derive(Clone)]
pub struct ZsetCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl ZsetCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        ZsetCommandCtx { txn }
    }

    pub async fn do_async_txnkv_zadd(mut self, key: &str, members: &Vec<String>, scores: &Vec<i64>, exists: Option<bool>, changed_only: bool, incr: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let scores = scores.to_owned();
        
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }

            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, mut size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                        size = 0;
                        txn = txn_rc.lock().await;
                    }
                    let mut updated_count = 0;
                    let mut added_count = 0;

                    for idx in 0..members.len() {
                        let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(&key, &members[idx]);
                        let new_score = scores[idx];
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, new_score);
                        let mut member_exists = false;
                        let old_data_value = txn.get(data_key.clone()).await?;
                        if old_data_value.is_some() {
                            member_exists = true;
                        }

                        if !member_exists {
                            added_count += 1;
                        }

                        if let Some(v) = exists {
                            // NX|XX
                            if (v && member_exists) || (!v && !member_exists) {
                                // XX Only update element that already exists
                                // NX Only update element that not exists
                                if changed_only {
                                    if !member_exists {
                                        updated_count += 1;
                                    } else {
                                        // check if score updated
                                        let old_score = KeyDecoder::new().decode_key_zset_data_value(&old_data_value.unwrap());
                                        if old_score != new_score {
                                            updated_count += 1;
                                        }
                                    }
                                }
                                let data_value = KeyEncoder::new().encode_txnkv_zset_data_value(new_score);
                                txn.put(data_key, data_value).await?;
                                txn.put(score_key, members[idx].clone()).await?;
                            } else {
                                // do not update member
                            }
                        } else {
                            // no NX|XX argument
                            if changed_only {
                                if !member_exists {
                                    updated_count += 1;
                                } else {
                                   // check if score updated
                                   let old_score = KeyDecoder::new().decode_key_zset_data_value(&old_data_value.unwrap());
                                   if old_score != new_score {
                                       updated_count += 1;
                                   }
                                }
                            }
                            let data_value = KeyEncoder::new().encode_txnkv_zset_data_value(new_score);
                            let member = members[idx].clone();
                            txn.put(data_key, data_value).await?;
                            txn.put(score_key, member).await?;
                        }
                    }

                    // update meta key
                    if added_count > 0 {
                        let new_meta_value = KeyEncoder::new().encode_txnkv_zset_meta_value(ttl, size + added_count);
                        txn.put(meta_key, new_meta_value).await?;
                    }

                    if changed_only {
                        Ok(updated_count)
                    } else {
                        Ok(added_count)
                    }
                },
                None => {
                    // create new key
                    for idx in 0..members.len() {
                        let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(&key, &members[idx]);
                        let score = scores[idx];
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, score);
                        let member = members[idx].clone();
                        // add data key and score key
                        let data_value = KeyEncoder::new().encode_txnkv_zset_data_value(score);
                        txn.put(data_key, data_value).await?;
                        txn.put(score_key, member).await?;
                    }
                    // add meta key
                    let size = members.len() as u64;
                    let new_meta_value = KeyEncoder::new().encode_txnkv_zset_meta_value(0, size);
                    txn.put(meta_key, new_meta_value).await?;
                    Ok(size)
                }
            }
        }.boxed()).await;

        match resp {
            Ok(v) => {
                Ok(resp_int(v as i64))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnkv_zcard(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                 // check key type and ttl
                 if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_int(0));
                }

                Ok(resp_int(size as i64))
            },
            None => {
                Ok(resp_int(0))
            }
        }
    }

    pub async fn do_async_txnkv_zcore(self, key: &str, member: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                 // check key type and ttl
                 if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, _) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_int(0));
                }

                let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(key, member);
                match ss.get(data_key).await? {
                    Some(data_value) => {
                        let score = KeyDecoder::new().decode_key_zset_data_value(&data_value);
                        Ok(resp_bulk(score.to_string().as_bytes().to_vec()))
                    },
                    None => {
                        Ok(resp_nil())
                    }
                }

            },
            None => {
                Ok(resp_int(0))
            }
        }
    }

    pub async fn do_async_txnkv_zcount(self, key: &str, mut min: i64, min_inclusive: bool, mut max: i64, max_inclusive: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_array(vec![]));
                }
                // update index bound
                if !min_inclusive {
                    min += 1;
                }
                if !max_inclusive {
                    max -= 1;
                }

                let start_key = KeyEncoder::new().encode_txnkv_zset_score_key(key, min);
                let end_key = KeyEncoder::new().encode_txnkv_zset_score_key(key, max);
                let range = start_key..=end_key;
                let bound_range: BoundRange = range.into();
                let iter = ss.scan(bound_range, size.try_into().unwrap()).await?;

                Ok(resp_int(iter.count() as i64))
            },
            None => {
                Ok(resp_int(0))
            }
        }
    }

    pub async fn do_async_txnkv_zrange(self, key: &str, mut min: i64, mut max: i64, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        let mut resp = vec![];
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_int(0));
                }
                // convert index to positive if negtive
                if min < 0 {
                    min += size as i64;
                }
                if max < 0 {
                    max += size as i64;
                }

                let start_key = KeyEncoder::new().encode_txnkv_zset_data_key_start(key);
                let range = start_key..;
                let from_range: BoundRange = range.into();
                let iter = ss.scan(from_range, size.try_into().unwrap()).await?;

                let mut idx = 0;
                for kv in iter {
                    if idx < min {
                        continue;
                    }
                    if idx > max {
                        break;
                    }
                    idx += 1;

                    // decode member key from data key
                    let member = KeyDecoder::new().decode_key_zset_member_from_datakey(key, kv.0);
                    if reverse {
                        resp.insert(0, resp_bulk(member));
                    } else {
                        resp.push(resp_bulk(member));
                    }
                    if with_scores {
                        // decode vec[u8] to i64
                        let score = KeyDecoder::new().decode_key_zset_data_value(&kv.1);
                        if reverse {
                            resp.insert(0, resp_bulk(score.to_string().as_bytes().to_vec()));
                        } else {
                            resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));
                        }
                    }
                }
                Ok(resp_array(resp))
            },
            None => {
                Ok(resp_array(resp))
            }
        }
    }

    pub async fn do_async_txnkv_zrange_by_score(self, key: &str, mut min: i64, min_inclusive: bool, mut max: i64, max_inclusive: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        let mut resp = vec![];
        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_array(vec![]));
                }
                // update index bound
                if !min_inclusive {
                    min += 1;
                }
                if !max_inclusive {
                    max -= 1;
                }

                let start_key = KeyEncoder::new().encode_txnkv_zset_score_key(key, min);
                let end_key = KeyEncoder::new().encode_txnkv_zset_score_key(key, max);
                let range = start_key..=end_key;
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
                        let score = KeyDecoder::new().decode_key_zset_score_from_scorekey(key ,kv.0);
                        if reverse {
                            resp.insert(0, resp_bulk(score.to_string().as_bytes().to_vec()));
                        } else {
                            resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));
                        }
                    }
                }
                Ok(resp_array(resp))
            },
            None => {
                Ok(resp_array(resp))
            }
        }
    }

    pub async fn do_async_txnkv_zrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zpop(mut self, key: &str, from_min: bool, count: u64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                     // check key type and ttl
                     if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                        return Ok(vec![]);
                    }
                    let mut poped_count = 0;
                    let mut resp = vec![];
                    if from_min {
                        let start_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, 0);
                        let range = start_key..;
                        let from_range: BoundRange = range.into();
                        let iter = txn.scan(from_range, count.try_into().unwrap()).await?;

                        for kv in iter {
                            let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(&key, &String::from_utf8_lossy(&kv.1));
                            
                            // push member to resp
                            resp.push(resp_bulk(kv.1));
                            // push score to resp
                            let score = KeyDecoder::new().decode_key_zset_score_from_scorekey(&key, kv.0.clone());
                            resp.push(resp_bulk(score.to_string().as_bytes().to_vec()));

                            txn.delete(data_key).await?;
                            txn.delete(kv.0).await?;
                            poped_count += 1;
                        }
                    } else {
                        // TODO not supported yet
                    }

                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_zset_meta_value(ttl , size - poped_count);
                    txn.put(meta_key, new_meta_value).await?;

                    Ok(resp)
                },
                None => {
                    Ok(vec![])
                }
            }
        }.boxed()).await;
        
        match resp {
            Ok(v) => {
                Ok(resp_array(v))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnkv_zrank(self, key: &str, member: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key.to_owned()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                    return Ok(resp_nil());
                }

                let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(key, member);
                match ss.get(data_key).await? {
                    Some(data_value) => {
                        // calculate the score rank in score key index
                        let score = KeyDecoder::new().decode_key_zset_data_value(&data_value);
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, score);

                        // scan from range start
                        let score_key_start = KeyEncoder::new().encode_txnkv_zset_score_key(&key, 0);
                        let range = score_key_start..;
                        let from_range: BoundRange = range.into();
                        let iter = ss.scan_keys(from_range, size.try_into().unwrap()).await?;
                        let mut rank = 0;
                        for k in iter {
                            if k ==  score_key {
                                break;
                            }
                            rank += 1;
                        }
                        return Ok(resp_int(rank));
                    },
                    None => {
                        return Ok(resp_nil());
                    }
                }
            },
            None => {
                return Ok(resp_nil());
            }
        }
    }

    pub async fn do_async_txnkv_zrem(mut self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let members = members.to_owned();

        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                        return Ok(0);
                    }
                    let mut removed_count = 0;

                    for member in members {
                        let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(&key, &member);
                        // get data key and decode value as score, to generate score key
                        let score = txn.get(data_key.clone()).await?;
                        if score.is_none() {
                            continue;
                        }

                        // decode the score vec to i64
                        let iscore = KeyDecoder::new().decode_key_zset_data_value(&score.unwrap());
                        // remove member and score key
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, iscore);
                        txn.delete(data_key).await?;
                        txn.delete(score_key).await?;
                        removed_count += 1;
                    }

                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_zset_meta_value(ttl, size - removed_count);
                    txn.put(meta_key, new_meta_value).await?;
                    Ok(removed_count)
                },
                None => {
                    Ok(0)
                }
            }
        }.boxed()).await;

        match resp {
            Ok(v) => {
                Ok(resp_int(v as i64))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnkv_zremrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zremrange_by_score(mut self, key: &str, min: i64, max: i64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_zset_expire_if_needed(&key).await?;
                        return Ok(0);
                    }

                    // generate score key range to remove, inclusive
                    let score_key_start = KeyEncoder::new().encode_txnkv_zset_score_key(&key, min);
                    let score_key_end = KeyEncoder::new().encode_txnkv_zset_score_key(&key, max);
                    
                    // remove score key and data key
                    let range = score_key_start..=score_key_end;
                    let bound_range: BoundRange = range.into();
                    let mut removed_count = 0;

                    let iter = txn.scan(bound_range, size.try_into().unwrap()).await?;

                    // TODO big txn optimization
                    for kv in iter {
                        // fetch this score key member
                        let score_value = kv.1;
                        let data_key = KeyEncoder::new().encode_txnkv_zset_data_key(&key, &String::from_utf8_lossy(&score_value));
                        txn.delete(data_key).await?;
                        txn.delete(kv.0).await?;
                        removed_count += 1;
                    }

                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_zset_meta_value(ttl, size - removed_count);
                    txn.put(meta_key, new_meta_value).await?;

                    Ok(removed_count)
                },
                None => {
                    Ok(0)
                }
            }
        }.boxed()).await;

        match resp {
            Ok(v) => {
                Ok(resp_int(v as i64))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnk_zset_del(mut self, key: &str) -> AsyncResult<i64> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }

            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    let size = KeyDecoder::new().decode_key_zset_size(&meta_value);

                    let start_key = KeyEncoder::new().encode_txnkv_zset_data_key_start(&key);
                    let range = start_key..;
                    let from_range: BoundRange = range.into();
                    let iter = txn.scan(from_range, size.try_into().unwrap()).await?;

                    for kv in iter {
                        // kv.0 is member key
                        // kv.1 is score
                        // decode the score vec to i64
                        let score = KeyDecoder::new().decode_key_zset_data_value(&kv.1);
                        // remove member and score key
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, score);
                        txn.delete(kv.0).await?;
                        txn.delete(score_key).await?;
                    }
                    txn.delete(meta_key).await?;
                    Ok(1)
                },
                None => Ok(0)
            }
        }.boxed()).await;
        resp
    }

    pub async fn do_async_txnkv_zset_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }

            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);
                    if !key_is_expired(ttl) {
                        return Ok(0);
                    }
                    let size = KeyDecoder::new().decode_key_zset_size(&meta_value);

                    let start_key = KeyEncoder::new().encode_txnkv_zset_data_key_start(&key);
                    let range = start_key..;
                    let from_range: BoundRange = range.into();
                    let iter = txn.scan(from_range, size.try_into().unwrap()).await?;

                    for kv in iter {
                        // kv.0 is member key
                        // kv.1 is score
                        // decode the score vec to i64
                        let score = KeyDecoder::new().decode_key_zset_data_value(&kv.1);
                        // remove member and score key
                        let score_key = KeyEncoder::new().encode_txnkv_zset_score_key(&key, score);
                        txn.delete(kv.0).await?;
                        txn.delete(score_key).await?;
                    }
                    txn.delete(meta_key).await?;
                    Ok(1)
                },
                None => Ok(0)
            }
        }.boxed()).await;
        resp
    }
}