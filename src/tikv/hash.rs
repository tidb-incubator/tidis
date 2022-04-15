use futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, KvPair, BoundRange};
use core::ops::RangeFrom;
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use super::{get_txn_client};

use crate::utils::{resp_err, resp_array, resp_bulk, resp_int, resp_nil};
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
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                }
                // already exists
                let (ttl, mut size) = KeyDecoder::new().decode_key_hash_meta(&meta_value);

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
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
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

pub async fn do_async_txnkv_hstrlen(key: &str, field: &str) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let field = field.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

    let mut ss = client.newest_snapshot().await;
    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
            }

            // TODO check ttl
            let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

            let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

            match ss.get(data_key).await? {
                Some(data) => {
                    return Ok(resp_int(data.len() as i64));
                },
                None => {
                    return Ok(resp_int(0));
                }
            }
        }
        None => {
            return Ok(resp_int(0));
        },
    }

}

pub async fn do_async_txnkv_hexists(key: &str, field: &str) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let field = field.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

    let mut ss = client.newest_snapshot().await;
    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
            }

            // TODO check ttl
            let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

            let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

            if ss.key_exists(data_key).await? {
                Ok(resp_int(1))
            } else {
                Ok(resp_int(0))
            }
        }
        None => {
            return Ok(resp_int(0));
        },
    }

}

pub async fn do_async_txnkv_hmget(key: &str, fields: &Vec<String>) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
    let mut ss = client.newest_snapshot().await;

    let mut resp = Vec::with_capacity(fields.len());

    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
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

pub async fn do_async_txnkv_hlen(key: &str) -> AsyncResult<Frame> {
    let client =get_txn_client()?;
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
    let mut ss = client.newest_snapshot().await;

    match ss.get(meta_key.to_owned()).await? {
        Some(meta_value) => {
            // check key type and ttl
            if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
            }

            // TODO check ttl
            let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

            let size = KeyDecoder::new().decode_key_hash_size(&meta_value);
            Ok(resp_int(size as i64))
        },
        None => Ok(resp_int(0)),
    }
}

pub async fn do_async_txnkv_hgetall(key: &str, with_field: bool, with_value: bool) -> AsyncResult<Frame> {
    let client =get_txn_client()?;
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(key);
    let mut ss = client.newest_snapshot().await;

    if let Some(meta_value) = ss.get(meta_key.to_owned()).await? {
        // check key type and ttl
        if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
            return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
        }

        // TODO check ttl
        let ttl = KeyDecoder::new().decode_key_ttl(&meta_value);

        let size = KeyDecoder::new().decode_key_hash_size(&meta_value);

        let data_key_start = KeyEncoder::new().encode_txnkv_hash_data_key_start(key);
        let range: RangeFrom<Key> = data_key_start..;
        let from_range: BoundRange = range.into();
        // scan return iterator
        let iter = ss.scan(from_range, size as u32).await?;

        let resp: Vec<Frame>;
        if with_field && with_value {
            resp = iter.flat_map(|kv| {
                let fields: Vec<u8> = KeyDecoder::new().decode_key_hash_userkey_from_datakey(key, kv.0);
                [resp_bulk(fields), resp_bulk(kv.1)]
            }).collect();
        } else if with_field{
            resp = iter.flat_map(|kv| {
                let fields: Vec<u8> = KeyDecoder::new().decode_key_hash_userkey_from_datakey(key, kv.0);
                [resp_bulk(fields)]
            }).collect();
        } else {
            resp = iter.flat_map(|kv| [resp_bulk(kv.1)]).collect();
        }

        Ok(resp_array(resp))
    } else {
        Ok(resp_nil())
    }
}

pub async fn do_async_txnkv_hdel(key: &str, field: &str) -> AsyncResult<Frame> {
    let client = get_txn_client()?;
    let key = key.to_owned();
    let field = field.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);

    let resp = client.exec_in_txn(|txn| async move {
        match txn.get_for_update(meta_key.clone()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                }

                let (ttl, mut size) = KeyDecoder::new().decode_key_hash_meta(&meta_value);

                // check data key exists
                let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);
                if txn.key_exists(data_key.clone()).await? {
                    // delete in txn
                    txn.delete(data_key).await?;
                    size -= 1;

                    // update meta key
                    let new_meta_value = KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size);
                    txn.put(meta_key, new_meta_value).await?;

                    return Ok(1)
                }
                Ok(0)
            },
            None => {
                Ok(0)
            }
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

pub async fn do_async_txnkv_hincrby(key: &str, field: &str, step: i64) -> AsyncResult<Frame> {
    let client =  get_txn_client()?;
    let key = key.to_owned();
    let field = field.to_owned();
    let meta_key = KeyEncoder::new().encode_txnkv_hash_meta_key(&key);
    let data_key = KeyEncoder::new().encode_txnkv_hash_data_key(&key, &field);

    let resp = client.exec_in_txn(|txn| async move {
        let mut new_int = 0;
        let mut prev_int = 0;
        match txn.get_for_update(meta_key.clone()).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Hash) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
                }

                let (ttl, size) = KeyDecoder::new().decode_key_hash_meta(&meta_value);

                match txn.get(data_key.clone()).await? {
                    Some(data_value) => {
                        // try to convert to int
                        match String::from_utf8_lossy(&data_value).parse::<i64>() {
                            Ok(ival) => {
                                prev_int = ival;
                            },
                            Err(err) => {
                                return Err(RTError::StringError(err.to_string()));
                            }
                        }
                    }, 
                    None => {
                        // filed not exist
                        prev_int = 0;
                        // update meta key
                        let new_meta_value = KeyEncoder::new().encode_txnkv_hash_meta_value(ttl, size+1);
                        txn.put(meta_key, new_meta_value).await?;
                    }
                }
            },
            None => {
                prev_int = 0;
                // create new meta key first
                let meta_value = KeyEncoder::new().encode_txnkv_hash_meta_value(0, 1);
                txn.put(meta_key, meta_value).await?;
            }
        }
        new_int = prev_int + step;
        // update data key
        txn.put(data_key, new_int.to_string().as_bytes().to_vec()).await?;

        Ok(new_int)
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