use ::futures::future::{FutureExt};
use crate::Frame;
use tikv_client::{Key, Value, KvPair, Transaction};
use super::{
    encoding::{KeyEncoder, KeyDecoder, DataType}, errors::AsyncResult, errors::RTError,
};
use bytes::Bytes;
use super::{get_txn_client};
use crate::utils::{resp_err, resp_int};
use super::errors::*;

pub struct SetCommandCtx<'a> {
    txn: Option<&'a mut Transaction>,
}

impl<'a> SetCommandCtx<'a> {
    pub fn new(txn: Option<&'a mut Transaction>) -> Self {
        SetCommandCtx { txn }
    }

    pub async fn do_async_txnkv_sadd(self, key: &str, members: &Vec<String>) -> AsyncResult<Frame> {
        let client = get_txn_client()?;

        let key = key.to_owned();
        let members = members.to_owned();
        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(&key);

        let resp = client.exec_in_txn(self.txn, |txn| async move {
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
            Some(txn) => client.snapshot_from_txn(txn).await,
            None => client.newest_snapshot().await
        };

        let meta_key = KeyEncoder::new().encode_txnkv_set_meta_key(key);

        match ss.get(meta_key).await? {
            Some(meta_value) => {
                // check key type and ttl
                if !matches!(KeyDecoder::new().decode_key_type(&meta_value), DataType::Set) {
                    return Err(RTError::StringError(REDIS_WRONG_TYPE_ERR.into()));
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

}