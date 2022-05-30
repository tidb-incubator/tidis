use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::list::ListCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Llen {
    key: String,
    valid: bool,
}

impl Llen {
    pub fn new(key: &str) -> Llen {
        Llen {
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Llen> {
        let key = parse.next_string()?;

        Ok(Llen { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Llen> {
        if argv.len() != 1 {
            return Ok(Llen {
                key: "".to_owned(),
                valid: false,
            });
        }
        let key = &argv[0];
        Ok(Llen::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.llen(None).await?;
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

    pub async fn llen(self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn)
                .do_async_txnkv_llen(&self.key)
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
