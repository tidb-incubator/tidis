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
pub struct Hgetall {
    key: String,
    valid: bool,
}

impl Hgetall {
    pub fn new(key: &str) -> Hgetall {
        Hgetall {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Hgetall {
        Hgetall {
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


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hgetall> {
        let key = parse.next_string()?;
        Ok(Hgetall{key:key, valid: true})
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hgetall> {
        if argv.len() != 1 {
            return Ok(Hgetall::new_invalid());
        }
        let key = &argv[0];
        Ok(Hgetall::new(key))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hgetall(None).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hgetall(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(txn).do_async_txnkv_hgetall(&self.key, true, true).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
