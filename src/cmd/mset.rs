use std::sync::Arc;

use crate::tikv::errors::AsyncResult;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use tikv_client::{KvPair, Transaction};
use tokio::sync::Mutex;
use crate::tikv::{
    encoding::{KeyEncoder}
};
use crate::config::is_use_txn_api;

use bytes::Bytes;
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug)]
pub struct Mset {
    keys: Vec<String>,
    vals: Vec<Bytes>,
    valid: bool,
}

impl Mset {
    /// Create a new `Mset` command which fetches `key` vector.
    pub fn new() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: true,
        }
    }

    pub fn new_invalid() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: false,
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn vals(&self) -> &Vec<Bytes> {
        &self.vals
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub fn add_val(&mut self, val: Bytes) {
        self.vals.push(val);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mset> {
        let mut mset = Mset::new();

        loop{
            if let Ok(key) = parse.next_string() {
                mset.add_key(key);
                if let Ok(val) = parse.next_bytes() {
                    mset.add_val(val);
                } else {
                    return Err("protocol error".into());
                }
            } else {
                break;
            }
        }

        Ok(mset)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Mset> {
        if argv.len() % 2 != 0 {
            return Ok(Mset::new_invalid());
        }
        let mut mset = Mset::new();
        for idx in (0..argv.len()).step_by(2) {
            mset.add_key(argv[idx].clone());
            mset.add_val(Bytes::from(argv[idx+1].clone()));
        }
        Ok(mset)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.batch_put(None).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_put(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut kvs = Vec::new();
        if is_use_txn_api() {
            for (idx, key) in self.keys.iter().enumerate() {
                let val = KeyEncoder::new().encode_txnkv_string_value(&mut self.vals[idx].to_vec(), 0);
                let ekey = KeyEncoder::new().encode_txnkv_string(&key);
                let kvpair = KvPair::from((ekey, val.to_vec()));
                kvs.push(kvpair);
            }
            StringCommandCtx::new(txn).do_async_txnkv_batch_put(kvs).await
        } else {
            for (idx, key) in self.keys.iter().enumerate() {
                let val = &self.vals[idx];
                let ekey = KeyEncoder::new().encode_rawkv_string(&key);
                let kvpair = KvPair::from((ekey, val.to_vec()));
                kvs.push(kvpair);
            }
            StringCommandCtx::new(None).do_async_rawkv_batch_put(kvs).await
        }
    }
}