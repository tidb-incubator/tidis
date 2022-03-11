use crate::Frame;

use super::{
    encoding::{KeyEncoder}, errors::AsyncResult,
};
use super::get_client;

pub async fn do_async_rawkv_get(key: &str) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    match client.get(ekey).await? {
        Some(val) => Ok(Frame::Bulk(val.into())),
        None => Ok(Frame::Null),
    }
}

pub async fn do_async_rawkv_put(key: &str, val: &str) -> AsyncResult<Frame> {
    let client = get_client()?;
    let ekey = KeyEncoder::new().encode_string(key);
    let _ = client.put(ekey, val).await?;
    Ok(Frame::Integer(1))
}