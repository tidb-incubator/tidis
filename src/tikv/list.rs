use futures::future::{FutureExt};
use crate::{Frame, utils::key_is_expired};
use tikv_client::{Key, BoundRange, Transaction};
use core::ops::RangeFrom;
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};
use bytes::Bytes;
use std::{convert::TryInto};
use crate::utils::{resp_err, resp_array, resp_bulk, resp_int, resp_nil, resp_ok};
use super::errors::*;
use std::sync::Arc;
use tokio::sync::Mutex;

const INIT_INDEX: u64 = 1<<32;

#[derive(Clone)]
pub struct ListCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl<'a> ListCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        ListCommandCtx { txn }
    }

    pub async fn do_async_txnkv_push(mut self, key: &str, values: &Vec<Bytes>, op_left: bool) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let values = values.to_owned();
    
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);
    
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }
    
                    let (ttl, mut left, mut right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
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
        
                        let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
                        txn.put(data_key, value.to_vec()).await?;
                    }
    
                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_list_meta_value(ttl, left, right);
                    txn.put(meta_key, new_meta_value).await?;
    
                    Ok(right - left)
                },
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
                        let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
                        txn.put(data_key, value.to_vec()).await?;
                    }
    
                    // add meta key
                    let meta_value = KeyEncoder::new().encode_txnkv_list_meta_value(0, left, right);
                    txn.put(meta_key, meta_value).await?;
    
                    Ok(right - left)
                }
            }
    
        }.boxed()).await;
    
        match resp {
            Ok(n) => Ok(resp_int(n as i64)),
            Err(e) => Ok(resp_err(&e.to_string())),
        }
    }
    
    pub async fn do_async_txnkv_pop(mut self, key: &str, op_left: bool, count: i64) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
    
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);
    
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut values = Vec::new();
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }
    
                    let (ttl, mut left, mut right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
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
                        let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
                        // get data and delete
                        let value = txn.get(data_key.clone()).await.unwrap().unwrap();
                        values.push(resp_bulk(value));
    
                        txn.delete(data_key).await?;
    
                        if left == right {
                            // delete meta key
                            txn.delete(meta_key).await?;
                        } else {
                            // update meta key
                            let new_meta_value = KeyEncoder::new().encode_txnkv_list_meta_value(ttl, left, right);
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
                            let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
                            // get data and delete
                            let value = txn.get(data_key.clone()).await.unwrap().unwrap();
                            values.push(resp_bulk(value));
    
                            txn.delete(data_key).await?;
    
                            if left == right {
                                // all elements poped, just delete meta key
                                txn.delete(meta_key.clone()).await?;
                            } else {
                                // update meta key
                                let new_meta_value = KeyEncoder::new().encode_txnkv_list_meta_value(ttl, left, right);
                                txn.put(meta_key.clone(), new_meta_value).await?;
                            }
                        }
                        Ok(values)
                    }
                },
                None => {
                    Ok(values)
                }
            }
        }.boxed()).await;
    
        match resp {
            Ok(values) => {
                if values.len() == 0 {
                    Ok(resp_nil())
                } else if values.len() == 1 {
                    Ok(values[0].clone())
                } else {
                    Ok(resp_array(values))
                }
            },
            Err(e) => Ok(resp_err(&e.to_string()))
        }
    }

    pub async fn do_async_txnkv_ltrim(mut self, key: &str, mut start: i64, mut end: i64) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
    
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);
    
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }
    
                    let (ttl, mut left, mut right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
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

                    // convert to relative position
                    start += left as i64;
                    end += left as i64;

                    // trim left->start-1
                    for idx in left..start as u64 {
                        let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
                        txn.delete(data_key).await?;
                    }
                    let left_trim = start - left as i64;
                    if left_trim > 0 {
                        left += left_trim as u64;
                    }

                    // trim end+1->right
                    for idx in (end+1) as u64..right {
                        let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, idx);
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
                        let new_meta_value = KeyEncoder::new().encode_txnkv_list_meta_value(ttl, left, right);
                        txn.put(meta_key, new_meta_value).await?;
                    }
                    Ok(())
                },
                None => {
                    Ok(())
                }
            }
        }.boxed()).await;
    
        match resp {
            Ok(_) => {
                Ok(resp_ok())
            },
            Err(e) => Ok(resp_err(&e.to_string()))
        }
    }
    
    pub async fn do_async_txnkv_lrange(self, key: &str, mut r_left: i64, mut r_right: i64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(key);
        
        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
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
                    return Ok(resp_nil())
                }
    
                let real_left = r_left + left as i64;
                let mut real_length = r_right - r_left + 1;
                if real_length > llen {
                    real_length = llen;
                }
    
                let data_key_start = KeyEncoder::new().encode_txnkv_list_data_key(key, real_left as u64);
                let range: RangeFrom<Key> = data_key_start..;
                let from_range: BoundRange = range.into();
                let iter = ss.scan(from_range, real_length.try_into().unwrap()).await?;
    
                let resp = iter.map(|kv|{
                    resp_bulk(kv.1)
                }).collect();
                Ok(resp_array(resp))
            },
            None => {
                Ok(resp_nil())
            }
        }
    }
    
    pub async fn do_async_txnkv_llen(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
    
        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(key);
    
        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
                    return Ok(resp_int(0));
                }
    
                let llen: i64 = (right - left) as i64;
                Ok(resp_int(llen))
            },
            None => {
                Ok(resp_int(0))
            }
        }
    }
    
    pub async fn do_async_txnkv_lindex(self, key: &str, mut idx: i64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
    
        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(key);
    
        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }
                let (ttl, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                if key_is_expired(ttl) {
                    self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
                    return Ok(resp_nil());
                }

                let len = right - left;
                // try convert idx to positive if needed
                if idx < 0 {
                    idx += len as i64;
                }
    
                let real_idx = left as i64 + idx;
    
                // get value from data key
                let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, real_idx as u64);
                if let Some(value) = ss.get(data_key).await? {
                    Ok(resp_bulk(value))
                } else {
                    Ok(resp_nil())
                }
            },
            None => {
                Ok(resp_nil())
            }
        }
    }
    
    pub async fn do_async_txnkv_lset(mut self, key: &str, mut idx: i64, ele: &Bytes) -> AsyncResult<Frame> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let ele = ele.to_owned();
    
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);
        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            Ok(match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }
                    let (ttl, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.clone().do_async_txnkv_list_expire_if_needed(&key).await?;
                        return Err(RTError::StringError(REDIS_NO_SUCH_KEY_ERR.into()));
                    }
    
                    // convert idx to positive is needed
                    if idx < 0 {
                        idx = idx + (right - left) as i64;
                    }
    
                    let uidx = idx + left as i64;
                    if idx < 0 || uidx < left as i64|| uidx > (right - 1) as i64 {
                        return Err(RTError::StringError(REDIS_INDEX_OUT_OF_RANGE.into()));
                    }
    
                    let data_key = KeyEncoder::new().encode_txnkv_list_data_key(&key, uidx as u64);
                    // data keys exists, update it to new value
                    txn.put(data_key, ele.to_vec()).await?;
                    Ok(())
                },
                None => {
                    // -Err no such key
                    Err(RTError::StringError(REDIS_NO_SUCH_KEY_ERR.into()))
                }
            })
        }.boxed()).await;
    
        match resp {
            Ok(_) => Ok(resp_ok()),
            Err(e) => Ok(resp_err(&e.to_string()))
        }
    }

    pub async fn do_async_txnkv_list_del(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }

            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    let (_, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    let data_key_start = KeyEncoder::new().encode_txnkv_list_data_key(&key, left);
                    let range: RangeFrom<Key> = data_key_start..;
                    let from_range: BoundRange = range.into();
                    let len = right - left;
                    let iter = txn.scan(from_range, len.try_into().unwrap()).await?;

                    for k in iter {
                        txn.delete(k).await?;
                    }
                    txn.delete(meta_key).await?;
                    Ok(1)
                },
                None => Ok(0),
            }

        }.boxed()).await;

        resp
    }

    pub async fn do_async_txnkv_list_expire_if_needed(mut self, key: &str) -> AsyncResult<i64> {
        let mut client = get_txn_client()?;
        let key = key.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }

            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    let (ttl, left, right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                    if !key_is_expired(ttl) {
                        return Ok(0)
                    } 
                    let data_key_start = KeyEncoder::new().encode_txnkv_list_data_key(&key, left);
                    let range: RangeFrom<Key> = data_key_start..;
                    let from_range: BoundRange = range.into();
                    let len = right - left;
                    let iter = txn.scan(from_range, len.try_into().unwrap()).await?;

                    for k in iter {
                        txn.delete(k).await?;
                    }
                    txn.delete(meta_key).await?;
                    Ok(1)
                },
                None => Ok(0),
            }

        }.boxed()).await;

        resp
    }
}

