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
use crate::utils::{resp_err, resp_int, resp_array, resp_bulk};
use super::errors::*;

pub struct SetCommandCtx {
    txn: Option<Arc<Mutex<Transaction>>>,
}

impl<'a> SetCommandCtx {
    pub fn new(txn: Option<Arc<Mutex<Transaction>>>) -> Self {
        SetCommandCtx { txn }
    }

    pub async fn do_async_txnkv_sadd(self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(&key);

        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_set_meta(&meta_value);
                    let mut added: u64 = 0;
                    
                    for m in &members {
                        // check member already exists
                        let data_key = KeyEncoder::new().encode_txnkv_set_data_key(&key, &m);
                        let member_exists = txn.key_exists(data_key.clone()).await?;
                        if !member_exists {
                            added += 1;
                            txn.put(data_key, vec![]).await?;
                        }
                    }
                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_set_meta_value(ttl, size+added);
                    txn.put(meta_key, new_meta_value).await?;
                    Ok(added)
                },
                None => {
                    // create new meta key and meta value
                    for m in &members {
                        // check member already exists
                        let data_key = KeyEncoder::new().encode_txnkv_set_data_key(&key, &m);
                        let member_exists = txn.key_exists(data_key.clone()).await?;
                        if !member_exists {
                            txn.put(data_key, vec![]).await?;
                        }
                    }
                    // create meta key
                    let meta_value = KeyEncoder::new().encode_txnkv_set_meta_value(0, members.len() as u64);
                    txn.put(meta_key, meta_value).await?;
                    Ok(members.len() as u64)
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

    pub async fn do_async_txnkv_scard(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let mut ss = match self.txn {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(key);

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                // TODO check ttl
                let (ttl, size) = KeyDecoder::new().decode_key_set_meta(&meta_value);

                Ok(resp_int(size as i64))
            },
            None => {
                Ok(resp_int(0))
            }
        }
    }

    // return interger reply if members.len() == 1, else arrary
    // called by SISMEMBER and SMISMEMBER
    pub async fn do_async_txnkv_sismember(self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let member_len = members.len();

        let mut ss = match self.txn {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(key);
        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

                if member_len == 1 {
                    let data_key = KeyEncoder::new().encode_txnkv_set_data_key(&key, &members[0]);
                    if ss.key_exists(data_key).await? {
                        Ok(resp_int(1))
                    } else {
                        Ok(resp_int(0))
                    }
                } else {
                    let mut resp = vec![];
                    for m in members {
                        let data_key = KeyEncoder::new().encode_txnkv_set_data_key(&key, m);
                        if ss.key_exists(data_key).await? {
                            resp.push(resp_int(1));
                        } else {
                            resp.push(resp_int(0));
                        }
                    }
                    Ok(resp_array(resp))
                }
            },
            None => {
                if member_len == 1 {
                    Ok(resp_int(0))
                } else {
                    Ok(resp_array(vec![]))
                }
            },
        }
    }

    pub async fn do_async_txnkv_smembers(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(key);

        let mut ss = match self.txn {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_set_meta(&meta_value);

                let start_key = KeyEncoder::new().encode_txnkv_set_data_key_start(key);
                let range = start_key..;
                let from_range: BoundRange = range.into();
                
                let iter = ss.scan_keys(from_range, size.try_into().unwrap()).await?;

                let resp = iter.map(|k|{
                    // decode member from data key
                    let user_key = KeyDecoder::new().decode_key_set_member_from_datakey(key, k);
                    resp_bulk(user_key)
                }).collect();

                Ok(resp_array(resp))
            },
            None => {
                Ok(resp_array(vec![]))
            }
        }

    }

    pub async fn do_async_txnkv_srem(self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(&key);

        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, size) = KeyDecoder::new().decode_key_set_meta(&meta_value);

                    let mut removed: u64 = 0;
                    for member in &members {
                        // check member exists
                        let data_key = KeyEncoder::new().encode_txnkv_set_data_key(&key, &member);
                        if txn.key_exists(data_key.clone()).await? {
                            removed += 1;
                            txn.delete(data_key).await?;
                        }
                    }
                    // update meta
                    let new_meta_value = KeyEncoder::new().encode_txnkv_set_meta_value(ttl, (size - removed) as u64);
                    txn.put(meta_key, new_meta_value).await?;

                    Ok(removed)
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

    /// spop will pop members by alphabetical order
    pub async fn do_async_txnkv_spop(self, key: &str, count: u64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(&key);

        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            match txn.get_for_update(meta_key.clone()).await? {
                Some(meta_value) => {
                    // check key type and ttl
                    if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                        return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                    }

                    let (ttl, mut size) = KeyDecoder::new().decode_key_set_meta(&meta_value);

                    let start_key = KeyEncoder::new().encode_txnkv_set_data_key_start(&key);
                    let range = start_key..;
                    let from_range: BoundRange = range.into();
                    let limit = if count < size { count } else { size };
                    let iter = txn.scan_keys(from_range, limit.try_into().unwrap()).await?;

                    let mut data_key_to_delete = vec![];
                    let resp = iter.map(|k|{
                        data_key_to_delete.push(k.clone());
                        // decode member from data key
                        let member = KeyDecoder::new().decode_key_set_member_from_datakey(&key, k);
                        resp_bulk(member)
                    }).collect();
                    
                    for k in data_key_to_delete {
                        txn.delete(k).await?;
                    }

                    size -= limit;
                    // update or delete meta key
                    if size == 0 {
                        // delete meta key
                        txn.delete(meta_key).await?;
                    } else {
                        // update meta key
                        let new_meta_value = KeyEncoder::new().encode_txnkv_set_meta_value(ttl, size);
                        txn.put(meta_key, new_meta_value).await?;
                    }
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

}