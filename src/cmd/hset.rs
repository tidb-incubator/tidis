use crate::cmd::{Parse, ParseError};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::{do_async_txnkv_hset};
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{sleep, resp_err, resp_int};

use tracing::{debug, instrument};
use tikv_client::{KvPair};

#[derive(Debug)]
pub struct Hset {
    key: String,
    field_and_value: Vec<KvPair>,
}

impl Hset {
    pub fn new() -> Hset {
        Hset {
            field_and_value:vec![],
            key: String::new(),
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

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hset().await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    async fn hset(&self) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            do_async_txnkv_hset(&self.key, &self.field_and_value).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
