use futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, Value, KvPair, BoundRange};
use core::ops::RangeFrom;
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};
use bytes::Bytes;

use crate::utils::{resp_err, resp_array, resp_bulk, resp_int, resp_nil};
use super::errors::*;

const INIT_INDEX: u64 = 1<<32;

pub async fn do_async_txnkv_push(key: &str, values: &Vec<Bytes>, op_left: bool) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let values = values.to_owned();

    let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);

    let resp = client.exec_in_txn(|txn| async move {
        match txn.get_for_update(meta_key.clone()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                }

                let (ttl, mut left, mut right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                // TODO check ttl and lazy free if expired

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

pub async fn do_async_txnkv_pop(key: &str, op_left: bool, count: i64) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();

    let meta_key = KeyEncoder::new().encode_txnkv_list_meta_key(&key);

    let resp = client.exec_in_txn(|txn| async move {
        let mut values = Vec::new();
        match txn.get_for_update(meta_key.clone()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::List) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                }

                let (ttl, mut left, mut right) = KeyDecoder::new().decode_key_list_meta(&meta_value);
                // TODO check ttl and lazy free if expired

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

pub async fn do_async_txnkv_lrange(key: &str, left: u64, right: u64) -> AsyncResult<Frame> {
    Ok(resp_nil())
}