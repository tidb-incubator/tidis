use ::futures::future::{FutureExt};
use crate::Frame;
use std::collections::HashMap;
use tikv_client::{Key, Value, KvPair, Transaction};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use bytes::Bytes;
use super::{get_client, get_txn_client};
use crate::utils::{sleep, resp_err, resp_int};
use super::errors::*;

pub struct StringCommandCtx<'a> {
    txn: Option<&'a mut Transaction>,
}

impl<'a> StringCommandCtx<'a> {
    pub fn new(txn: Option<&'a mut Transaction>) -> Self {
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

        let mut ss = match self.txn {
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await
        };

        match ss.get(ekey).await? {
            Some(val) => {
                let dt = KeyDecoder::new().decode_key_type(&val);
                if !matches!(dt, DataType::String) {
                    return Ok(resp_err(REDIS_WRONG_TYPE_ERR))
                }
    
                let data = KeyDecoder::new().decode_key_string_value(&val);
                Ok(Frame::Bulk(data.into()))
            }
            None => Ok(Frame::Null),
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
            Some(txn) => client.snapshot_from_txn(txn).await,
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
            Some(txn) => client.snapshot_from_txn(txn).await,
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
}