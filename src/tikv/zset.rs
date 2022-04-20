use std::convert::TryInto;

use ::futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Transaction, BoundRange};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};
use crate::utils::{resp_err, resp_int, resp_array, resp_bulk, resp_nil};
use super::errors::*;

pub struct ZsetCommandCtx<'a> {
    txn: Option<&'a mut Transaction>,
}

impl<'a> ZsetCommandCtx<'a> {
    pub fn new(txn: Option<&'a mut Transaction>) -> Self {
        ZsetCommandCtx { txn }
    }

    pub async fn do_async_txnkv_zadd(self, key: &str, members: &Vec<String>, scores: &Vec<i64>, exists: Option<bool>, changed_only: bool, incr: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let scores = scores.to_owned();
        
        let meta_key = KeyEncoder::new().encode_txnkv_zset_meta_key(&key);
        let resp = client.exec_in_txn(self.txn, |txn| async move {
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);
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

        let mut ss = match self.txn {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                 // check key type and ttl
                 if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_zset_meta(&meta_value);

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

        let mut ss = match self.txn {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                 // check key type and ttl
                 if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Zset) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, _) = KeyDecoder::new().decode_key_zset_meta(&meta_value);

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

    pub async fn do_async_txnkv_zcount(self, key: &str, min: u64, max: u64) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zrange(self, key: &str, min: u64, with_min: bool, max: u64, with_max: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zrange_by_score(self, key: &str, min: u64, with_min: bool, max: u64, with_max: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool, with_scores: bool, reverse: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zpop(self, key: &str, from_min: bool, count: u64) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zrank(self, key: &str, member: &str) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zrem(self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zremrange_by_lex(self, key: &str, min: &str, with_min: bool, max: &str, with_max: bool) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }

    pub async fn do_async_txnkv_zremrange_by_score(self, key: &str, min: i64, max: i64) -> AsyncResult<Frame> {
        Ok(resp_nil())
    }
}