use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Scard {
    key: String,
    valid: bool,
}

impl Scard {
    pub fn new(key: &str) -> Scard {
        Scard {
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

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scard> {
        let key = parse.next_string()?;
        Ok(Scard::new(&key))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Scard> {
        if argv.len() != 1 {
            return Ok(Scard {
                key: "".to_owned(),
                valid: false,
            });
        }
        Ok(Scard::new(&argv[0]))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.scard(None).await?;
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

    pub async fn scard(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_scard(&self.key)
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
