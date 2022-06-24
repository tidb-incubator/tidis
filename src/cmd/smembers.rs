use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Smembers {
    key: String,
    valid: bool,
}

impl Smembers {
    pub fn new(key: &str) -> Smembers {
        Smembers {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Smembers> {
        let key = parse.next_string()?;
        Ok(Smembers::new(&key))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Smembers> {
        if argv.len() != 1 {
            return Ok(Smembers {
                key: "".to_owned(),
                valid: false,
            });
        }
        Ok(Smembers::new(&argv[0]))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.smembers(None).await?;
        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn smembers(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_smembers(&self.key)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Smembers {
    fn new_invalid() -> Smembers {
        Smembers {
            key: "".to_owned(),
            valid: false,
        }
    }
}
