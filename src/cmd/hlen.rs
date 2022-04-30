use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::hash::HashCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err};

use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Hlen {
    key: String,
    valid: bool,
}

impl Hlen {
    pub fn new(key: &str) -> Hlen {
        Hlen {
            key: key.to_string(),
            valid: true,
        }
    }
    pub fn new_invalid() -> Hlen {
        Hlen {
            key: "".to_string(),
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hlen> {
        let key = parse.next_string()?;
        Ok(Hlen{key:key, valid: true})
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hlen> {
        if argv.len() != 1 {
            return Ok(Hlen::new_invalid());
        }
        let key = &argv[0];
        Ok(Hlen::new(key))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hlen(None).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hlen(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(txn).do_async_txnkv_hlen(&self.key).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
