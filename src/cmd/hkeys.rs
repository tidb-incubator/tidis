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
pub struct Hkeys {
    key: String,
    valid: bool,
}

impl Hkeys {
    pub fn new(key: &str) -> Hkeys {
        Hkeys {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_owned();
    }


    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hkeys> {
        let key = parse.next_string()?;
        Ok(Hkeys{key:key, valid: true})
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hkeys> {
        if argv.len() != 1 {
            return Ok(Hkeys{ key: "".to_owned(), valid: false});
        }
        let key = &argv[0];
        Ok(Hkeys::new(key))
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        
        let response = self.hkeys(None).await?;
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn hkeys(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if is_use_txn_api() {
            HashCommandCtx::new(txn).do_async_txnkv_hgetall(&self.key, true, false).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
