use std::sync::Arc;

use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::tikv::KEY_ENCODER;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use tikv_client::{KvPair, Transaction};
use tokio::sync::Mutex;

use crate::cmd::Invalid;
use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;

#[derive(Debug)]
pub struct Mset {
    keys: Vec<String>,
    vals: Vec<Bytes>,
    valid: bool,
}

impl Mset {
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
        let mut mset = Mset::default();

        while let Ok(key) = parse.next_string() {
            mset.add_key(key);
            if let Ok(val) = parse.next_bytes() {
                mset.add_val(val);
            } else {
                return Err("protocol error".into());
            }
        }

        Ok(mset)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Mset> {
        if argv.len() % 2 != 0 {
            return Ok(Mset::new_invalid());
        }
        let mut mset = Mset::default();
        for idx in (0..argv.len()).step_by(2) {
            mset.add_key(String::from_utf8_lossy(&argv[idx]).to_string());
            mset.add_val(argv[idx + 1].clone());
        }
        Ok(mset)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.batch_put(None).await.unwrap_or_else(Into::into);

        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );

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
                let val = KEY_ENCODER.encode_txnkv_string_value(&mut self.vals[idx].to_vec(), 0);
                let ekey = KEY_ENCODER.encode_txnkv_string(key);
                let kvpair = KvPair::from((ekey, val.to_vec()));
                kvs.push(kvpair);
            }
            StringCommandCtx::new(txn)
                .do_async_txnkv_batch_put(kvs)
                .await
        } else {
            for (idx, key) in self.keys.iter().enumerate() {
                let val = &self.vals[idx];
                let ekey = KEY_ENCODER.encode_rawkv_string(key);
                let kvpair = KvPair::from((ekey, val.to_vec()));
                kvs.push(kvpair);
            }
            StringCommandCtx::new(None)
                .do_async_rawkv_batch_put(kvs)
                .await
        }
    }
}

impl Default for Mset {
    /// Create a new `Mset` command which fetches `key` vector.
    fn default() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mset {
    fn new_invalid() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: false,
        }
    }
}
