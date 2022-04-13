use ::futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, Value, KvPair};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};

use crate::utils::{resp_err, resp_array, resp_bulk, resp_nil};
use super::errors::*;

pub async fn do_async_txnkv_hset(key: &str, fvs: &Vec<KvPair>) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);
    let fvs_copy = fvs.clone();
    let fvs_len = fvs_copy.len();
    let resp = client.exec_in_txn(|txn| async move {
        // check if key already exists
        match txn.get_for_update(meta_key.clone()).await? {
            Some(meta_value) => {
                // check key type is hash
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.to_string()));
                }
                // already exists
                let (mut ttl, mut size) = KeyDecoder::new().decode_key_hash_meta(&meta_value);

                for kv in fvs_copy {
                    let field: Vec<u8> = kv.0.into();
                    let datakey = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &String::from_utf8_lossy(&field));
                    // check field exists
                    let field_exists = txn.key_exists(datakey.clone()).await?;
                    if !field_exists {
                        size += 1;
                    }
                    txn.put(datakey, kv.1).await?;
                }

                // update meta key
                let new_metaval = KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size);
                txn.put(meta_key, new_metaval).await?;
            },
            None => {
                // not exists
                let ttl = 0;
                let mut size = 0;

                for kv in fvs_copy {
                    let field: Vec<u8> = kv.0.into();
                    let datakey = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &String::from_utf8_lossy(&field));
                    // check field exists
                    let field_exists = txn.key_exists(datakey.clone()).await?;
                    if !field_exists {
                        size += 1;
                    }
                    txn.put(datakey, kv.1).await?;
                }

                // set meta key
                let new_metaval = KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size);
                txn.put(meta_key, new_metaval).await?;
            }
        }
        Ok(fvs_len)
    }.boxed()).await;
    match resp {
        Ok(num) => {
            Ok(Frame::Integer(num as i64)) 
        },
        Err(e) => {
            Ok(resp_err(&e.to_string()))
        }
    }
}

pub async fn do_async_txnkv_hget(key: &str, field: &str) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let field = field.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

    let mut ss = client.newest_snapshot().await;
    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.to_string()));
            }

            // TODO check ttl
            let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

            let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

            match ss.get(data_key).await? {
                Some(data) => {
                    return Ok(resp_bulk(data));
                },
                None => {
                    return Ok(resp_nil());
                }
            }
            
        }
        None => {
            return Ok(resp_nil());
        },
    }

}

pub async fn do_async_txnkv_hmget(key: &str, fields: &Vec<String>) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let field = fields.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

    let mut ss = client.newest_snapshot().await;

    let mut resp = Vec::with_capacity(fields.len());

    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.to_string()));
            }

            // TODO check ttl
            let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

            

            for field in fields {
                let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);
                match ss.get(data_key).await? {
                    Some(data) => {
                        resp.push(resp_bulk(data));
                    },
                    None => {
                        resp.push(resp_nil());
                    }
                }
            }
            
        }
        None => {
            for _ in 0..fields.len() {
                resp.push(resp_nil());
            }
        },
    }
    Ok(resp_array(resp))
}