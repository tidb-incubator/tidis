use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err, resp_invalid_arguments};

use tokio::sync::Mutex;
use tracing::{debug, instrument};
use tikv_client::{KvPair, Transaction};

#[derive(Debug)]
pub struct Hset {
    key: String,
    field_and_value: Vec<KvPair>,
    valid: bool,
}

impl Hset {
    pub fn new() -> Hset {
        Hset {
            field_and_value:vec![],
            key: String::new(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Hset {
        Hset {
            field_and_value: vec![],
            key: String::new(),
            valid: false,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }

    /// Get the field and value pairs
    pub fn fields(&self) -> &Vec<KvPair> {
        &self.field_and_value
    }

    pub fn add_field_value(&mut self, kv: KvPair) {
        self.field_and_value.push(kv);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hset> {
        let mut hset = Hset::new();

        let key = parse.next_string()?;
        hset.set_key(&key);
        
        loop {
            if let Ok(field) = parse.next_string() {
                if let Ok(value) = parse.next_bytes() {
                    let kv = KvPair::new(field, value.to_vec());
                    hset.add_field_value(kv);
                } else {
                    return Err("protocol error".into());
                }
            } else {
                break;
            }
        }
        Ok(hset)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hset> {
        if argv.len() % 2 != 1 {
            return Ok(Hset::new_invalid());
        }
        let key = &argv[0];
        let mut hset = Hset::new();
        hset.set_key(key);

        for idx in (1..argv.len()).step_by(2) {
            let field = argv[idx].to_owned();
            let value = argv[idx+1].to_owned().as_bytes().to_vec();
            let kv = KvPair::new(field, value);
            hset.add_field_value(kv);
        }
        Ok(hset)
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection, is_hmset: bool) -> crate::Result<()> {
        
        let response = self.hset(None, is_hmset).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hset(&self, txn: Option<Arc<Mutex<Transaction>>>, is_hmset: bool) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            HashCommandCtx::new(txn).do_async_txnkv_hset(&self.key, &self.field_and_value, is_hmset).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
