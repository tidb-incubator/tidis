use crate::Frame;
use std::collections::HashMap;
use tikv_client::{Key, Value, KvPair};
use super::{
    encoding::{KeyEncoder}, errors::AsyncResult,
};
use bytes::Bytes;
use super::get_client;

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
    Ok(Frame::Integer(num_keys as u64))
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