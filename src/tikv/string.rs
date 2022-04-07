use crate::Frame;
use std::collections::HashMap;
use tikv_client::{Key, Value, KvPair};
use super::{
    encoding::{KeyEncoder}, errors::AsyncResult, errors::RTError,
};
use bytes::Bytes;
use super::get_client;
use crate::utils::{sleep, resp_err, resp_int};

pub async fn do_async_rawkv_get(key: &str) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    match client.get(ekey).await? {
        Some(val) => Ok(Frame::Bulk(val.into())),
        None => Ok(Frame::Null),
    }
}

pub async fn do_async_rawkv_put(key: &str, val: Bytes) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let _ = client.put(ekey, val.to_vec()).await?;
    Ok(Frame::Integer(1))
}

pub async fn do_async_rawkv_batch_get(keys: Vec<String>) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekeys = KeyEncoder::new().encode_strings(keys);
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

pub async fn do_async_rawkv_batch_put(kvs: Vec<KvPair>) -> AsyncResult<Frame> {
    let client = get_client()?;
    let num_keys = kvs.len();
    let _ = client.batch_put(kvs).await?;
    Ok(Frame::Integer(num_keys as i64))
}

pub async fn do_async_rawkv_put_not_exists(key: &str, value: Bytes) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let (_, swapped) = client.compare_and_swap(ekey, None.into(), value.to_vec()).await?;
    if swapped {
        Ok(Frame::Integer(1))
    } else {
        Ok(Frame::Integer(0))
    } 
}

pub async fn do_async_rawkv_expire(key: &str, value: Option<Bytes>, ttl: i64) ->AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key.clone());
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

pub async fn do_async_rawkv_get_ttl(key: &String) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(&key);
    let val = client.get_ttl(ekey).await?;
    Ok(resp_int(val as i64))
}