use ::futures::future::{FutureExt};
use crate::{Frame, utils::{resp_bulk, resp_nil}};
use std::{collections::HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tikv_client::{Key, Value, KvPair, Transaction};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use bytes::Bytes;
use super::{get_client, get_txn_client};
use crate::utils::{sleep, resp_err, resp_int, key_is_expired, ttl_from_timestamp};
use super::errors::*;
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
        let ekey = KeyEncoder::new().encode_rawkv_string(key);
        match client.get(ekey).await? {
            Some(val) => Ok(Frame::Bulk(val.into())),
            None => Ok(Frame::Null),
        }
    }
    
    pub async fn do_async_txnkv_get(self, key: &str) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KeyEncoder::new().encode_txnkv_string(key);

        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            },
            None => client.newest_snapshot().await
        };

        match ss.get(ekey).await? {
            Some(val) => {
                let dt = KeyDecoder::new().decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR))
                }

                // ttl saved in milliseconds
                let ttl = KeyDecoder::new().decode_key_ttl(&val);
                if key_is_expired(ttl) {
                    // delete key
                    self.do_async_txnkv_string_del(key).await?;
                    return Ok(resp_nil())
                }
    

                let data = KeyDecoder::new().decode_key_string_value(&val);
                Ok(resp_bulk(data))
            }
            None => Ok(resp_nil()),
        }
    }
    
    pub async fn do_async_rawkv_put(self, key: &str, val: &Bytes) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KeyEncoder::new().encode_rawkv_string(key);
        let _ = client.put(ekey, val.to_vec()).await?;
        Ok(Frame::Integer(1))
    }
    
    pub async fn do_async_txnkv_put(self, key: &str, val: &Bytes) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KeyEncoder::new().encode_txnkv_string(key);
        let eval = KeyEncoder::new().encode_txnkv_string_value(&mut val.to_vec(), 0);
        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            txn.put(ekey, eval).await?;
            Ok(())
        }.boxed()).await;
        match resp {
            Ok(_) => {
                Ok(Frame::Integer(1)) 
            },
            Err(e) => {
                Err(RTError::StringError(e.to_string()))
            }
        }
    }
    
    pub async fn do_async_rawkv_batch_get(self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekeys = KeyEncoder::new().encode_rawkv_strings(keys);
        let result = client.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter()
        .map(|pair| (pair.0, pair.1))
        .collect();
    
        let values: Vec<Frame> = ekeys
        .into_iter()
        .map(|k| {
            let data = ret.get(k.as_ref());
            match data {
                Some(val) => Frame::Bulk(val.to_owned().into()),
                None => Frame::Null,
            }
        }).collect();
        Ok(Frame::Array(values))
    }
    
    pub async fn do_async_txnkv_batch_get(self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekeys = KeyEncoder::new().encode_txnkv_strings(keys);

        let mut ss = match self.txn {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            }
            None => client.newest_snapshot().await
        };
        let result = ss.batch_get(ekeys.clone()).await?;
        let ret: HashMap<Key, Value> = result.into_iter()
        .map(|pair| (pair.0, pair.1))
        .collect();
    
        let values: Vec<Frame> = ekeys
        .into_iter()
        .map(|k| {
            let data = ret.get(k.as_ref());
            match data {
                Some(val) => {
                    let data = KeyDecoder::new().decode_key_string_value(val);
                    Frame::Bulk(data.into())
                }
                None => Frame::Null,
            }
        }).collect();
        Ok(Frame::Array(values))
    }
    
    
    pub async fn do_async_rawkv_batch_put(self, kvs: Vec<KvPair>) -> AsyncResult<Frame> {
        let client = get_client()?;
        let num_keys = kvs.len();
        let _ = client.batch_put(kvs).await?;
        Ok(Frame::Integer(num_keys as i64))
    }
    
    pub async fn do_async_txnkv_batch_put(self, kvs: Vec<KvPair>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let num_keys = kvs.len();
        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            for kv in kvs {
                txn.put(kv.0, kv.1).await?;
            }
            Ok(())
        }.boxed()).await;
        match resp {
            Ok(_) => {
                Ok(Frame::Integer(num_keys as i64))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    
    }
    
    pub async fn do_async_rawkv_put_not_exists(self, key: &str, value: &Bytes) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KeyEncoder::new().encode_rawkv_string(key);
        let (_, swapped) = client.compare_and_swap(ekey, None.into(), value.to_vec()).await?;
        if swapped {
            Ok(Frame::Integer(1))
        } else {
            Ok(Frame::Integer(0))
        } 
    }
    
    pub async fn do_async_txnkv_put_not_exists(self, key: &str, value: &Bytes) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KeyEncoder::new().encode_txnkv_string(key);
        let eval = KeyEncoder::new().encode_txnkv_string_value(&mut value.to_vec(), 0);
    
        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut txn = txn.lock().await;
            let exists = txn.key_exists(ekey.clone()).await?;
            if exists {
                return Ok(0);
            } else {
                txn.put(ekey, eval).await?;
                Ok(1)
            }
        }.boxed()).await;
    
        match resp {
            Ok(n) => {
                Ok(resp_int(n))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
    
        }
    }
    
    pub async fn do_async_rawkv_expire(self, key: &str, value: Option<Bytes>, ttl: i64) ->AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KeyEncoder::new().encode_rawkv_string(&key.clone());
        let mut swapped = false;
        for i in 0..2000 {
            let prev_val = client.get(ekey.clone()).await?;
            let expect_val = match &value {
                Some(v) => Some(v.to_vec()),
                None => {
                    prev_val.clone()
                },
            };
            if let None = expect_val {
                return Ok(resp_int(0));
            }
            
            let (val, ret) = client.compare_and_swap_with_ttl(ekey.clone(), prev_val.clone(), expect_val.unwrap(), ttl).await?;
            if ret {
                swapped = true;
                break;
            } else {
                if let Some(data) = val {
                    if data.eq(&prev_val.unwrap()) {
                        swapped = true;
                        break;
                    }
                } 
            }
            sleep(std::cmp::min(i, 200)).await;
        }
        if !swapped {
            Err(RTError::StringError("Cannot swapped".to_owned()))
        } else {
            Ok(Frame::Integer(1))
        }
    }
    
    pub async fn do_async_rawkv_get_ttl(self, key: &String) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KeyEncoder::new().encode_rawkv_string(&key);
        let val = client.get_ttl(ekey).await?;
        Ok(resp_int(val as i64))
    }
    
    pub async fn do_async_rawkv_exists(self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekeys = KeyEncoder::new().encode_rawkv_strings(keys);
        let result = client.batch_get(ekeys).await?;
        let num_items = result.len();
        Ok(resp_int(num_items as i64))
    }
    
    pub async fn do_async_txnkv_exists(self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let mut ss = match self.txn {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            }
            None => client.newest_snapshot().await
        };
        let mut cnt = 0;
        for key in keys {
            let ekey = KeyEncoder::new().encode_txnkv_string(key);
            match ss.key_exists(ekey).await {
                Ok(true) => cnt += 1,
                Ok(_) => {},
                Err(e) => {
                    return Err(RTError::StringError(e.to_string()))
                }
            }
        }
        Ok(resp_int(cnt as i64))
    }
    
    pub async fn do_async_rawkv_incr(self,
        key: &String, inc: bool, step: i64
    ) -> AsyncResult<Frame> {
        let client = get_client()?;
        let ekey = KeyEncoder::new().encode_rawkv_string(&key.clone());
        let mut new_int: i64 = 0;
        let mut swapped = false;
        for i in 0..2000 {
            let prev: Option<Vec<u8>>;
            let prev_int: i64;
            match client.get(ekey.clone()).await? {
                Some(val) => match String::from_utf8_lossy(&val).parse::<i64>() {
                    Ok(ival) => {
                        prev_int = ival;
                        prev = Some(val.clone());
                    }
                    Err(err) => {
                        return Err(RTError::StringError(err.to_string()));
                    }
                },
                None => {
                    prev = None;
                    prev_int = 0;
                }
            }
            if inc {
                new_int = prev_int + step;
            } else {
                new_int = prev_int - step;
            }
            let new_val = new_int.to_string();
            let (_, ret) = client.compare_and_swap(ekey.clone(), prev, new_val.into()).await?;
            if ret {
                swapped = true;
                break;
            }
            sleep(std::cmp::min(i, 200)).await;
        }
        if !swapped {
            Err(RTError::StringError("Cannot swapped".to_owned()))
        } else {
            Ok(resp_int(new_int))
        }
    }
    
    pub async fn do_async_txnkv_incr(self,
        key: &String, inc: bool, step: i64
    ) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KeyEncoder::new().encode_txnkv_string(&key);
    
        let resp = client.exec_in_txn(self.txn, |txn| async move {
            let mut new_int = 0;
            let mut prev_int = 0;
            let mut txn = txn.lock().await;
            match txn.get(ekey.clone()).await? {
                Some(val) => match String::from_utf8_lossy(&val).parse::<i64>() {
                    Ok(ival) => {
                        prev_int = ival;
                    },
                    Err(err) => {
                        return Err(RTError::StringError(err.to_string()));
                    }
                },
                None => {
                    prev_int = 0;
                }
            }
            if inc {
                new_int = prev_int + step;
            } else {
                new_int = prev_int - step;
            }
            let new_val = new_int.to_string();
            let eval = KeyEncoder::new().encode_txnkv_string_value(&mut new_val.as_bytes().to_vec(), 0);
            txn.put(ekey, eval).await?;
            Ok(new_int)
        }.boxed()).await;
    
        match resp {
            Ok(n) => Ok(resp_int(n)),
            Err(e) => Ok(resp_err(&e.to_string()))
        }
    }

    pub async fn do_async_txnkv_string_del(self, key: &str) -> AsyncResult<i64> {
        let client = get_txn_client()?;
        let key = key.to_owned();

        let resp = client.exec_in_txn(self.txn.clone(), |txn| async move {
            let mut txn = txn.lock().await;
            let ekey = KeyEncoder::new().encode_txnkv_string(&key);
            if let Some(_) = txn.get(ekey.to_owned()).await? {
                txn.delete(ekey).await?;
                return Ok(1);
            }
            return Ok(0); 
        }.boxed()).await;

        resp
    }

    pub async fn do_async_txnkv_string_expire(self, key: &str, ttl: u64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let ekey = KeyEncoder::new().encode_txnkv_string(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn| async move {
            let mut txn = txn.lock().await;
            match txn.get_for_update(ekey.clone()).await? {
                Some(value) => {
                    let old_ttl = KeyDecoder::new().decode_key_ttl(&value);
                    if key_is_expired(old_ttl) {
                        self.do_async_txnkv_string_del(&key).await?;
                        return Ok(0)
                    }
                    let mut val = KeyDecoder::new().decode_key_string_value(&value);
                    let new_val = KeyEncoder::new().encode_txnkv_string_value(&mut val, ttl);
                    txn.put(ekey, new_val).await?;
                    Ok(1)
                },
                None => {
                    Ok(0)
                }
            }
        }.boxed()).await;

        match resp {
            Ok(v) => {
                Ok(resp_int(v))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnkv_expire(mut self, key: &str, timestamp: u64) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let key = key.to_owned();
        let ekey = KeyEncoder::new().encode_txnkv_string(&key);

        let resp = client.exec_in_txn(self.txn.clone(), |txn_rc| async move {
            if self.txn.is_none() {
                self.txn = Some(txn_rc.clone());
            }
            let mut txn = txn_rc.lock().await;
            match txn.get_for_update(ekey.clone()).await? {
                Some(meta_value) => {
                    let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);
                    // check key expired
                    if key_is_expired(ttl) {
                        drop(txn);
                        self.do_async_txnkv_string_del(&key).await?;
                        return Ok(0);
                    }
                    let dt = KeyDecoder::new().decode_key_type(&meta_value);
                    match dt {
                        DataType::String => {
                            let mut value = KeyDecoder::new().decode_key_string_value(&meta_value);
                            let new_meta_value = KeyEncoder::new().encode_txnkv_string_value(&mut value, timestamp);
                            txn.put(ekey, new_meta_value).await?;
                            return Ok(1);
                        },
                        DataType::Hash => {},
                        DataType::List => {},
                        DataType::Set => {},
                        DataType::Zset => {},
                        _ => {}
                    }
                    Ok(1)
                },
                None => Ok(0)
            }
        }.boxed()).await;
        match resp {
            Ok(v) => {
                Ok(resp_int(v))
            },
            Err(e) => {
                Ok(resp_err(&e.to_string()))
            }
        }
    }

    pub async fn do_async_txnkv_ttl(self, key: &str, is_millis: bool) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let ekey = KeyEncoder::new().encode_txnkv_string(key);
        let mut ss = match self.txn.clone() {
            Some(txn) => {
                client.snapshot_from_txn(txn).await
            }
            None => client.newest_snapshot().await
        };

        match ss.get(ekey).await? {
            Some(meta_value) => {
                let dt = KeyDecoder::new().decode_key_type(&meta_value);
                let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);
                match dt {
                    DataType::String => {
                        if key_is_expired(ttl) {
                            self.do_async_txnkv_string_del(&key).await?;
                            return Ok(resp_int(-2));
                        }
                        if ttl == 0 {
                            return Ok(resp_int(-1));
                        } else {
                            let mut ttl = ttl_from_timestamp(ttl) as i64;
                            if !is_millis {
                                ttl /= 1000;
                            }
                            return Ok(resp_int(ttl));
                        }
                    },
                    DataType::Hash => {},
                    DataType::List => {},
                    DataType::Set => {},
                    DataType::Zset => {},
                    _ => {}
                }
                Ok(resp_nil())
            },
            None => Ok(resp_int(-2))
        }
    }

    pub async fn do_async_txnkv_del(mut self, keys: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;
        let keys = keys.to_owned();
        let keys_len = keys.len();

        let resp = client.exec_in_txn(self.txn.clone(), |txn| async move {
            if self.txn.is_none() {
                self.txn = Some(txn.clone());
            }

            let mut dts = vec![];
            let mut txn = txn.lock().await;

            for key in &keys {
                let ekey = KeyEncoder::new().encode_txnkv_string(&key);
                let meta_value = txn.get(ekey).await?;
                if let Some(v) = meta_value {
                    let dt = KeyDecoder::new().decode_key_type(&v);
                    dts.push(dt);
                } else {
                    dts.push(DataType::Null);
                }
            }
            drop(txn);

            let mut resp = 0;
            for idx in 0..keys_len {
                match dts[idx] {
                    DataType::String => {
                        self.clone().do_async_txnkv_string_del(&keys[idx]).await?;
                        resp += 1;
                    },
                    DataType::Hash => {},
                    DataType::List => {},
                    DataType::Set => {},
                    DataType::Zset => {},
                    DataType::Null => {},
                }
            }
            Ok(resp)
        }.boxed()).await;
        match resp {
            Ok(v) => Ok(resp_int(v)),
            Err(e) => Ok(resp_err(&e.to_string()))
        }
    }
}