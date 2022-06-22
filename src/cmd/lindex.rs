use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::list::ListCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Lindex {
    key: String,
    idx: i64,
    valid: bool,
}

impl Lindex {
    pub fn new(key: &str, idx: i64) -> Lindex {
        Lindex {
            key: key.to_owned(),
            idx,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lindex> {
        let key = parse.next_string()?;
        let idx = parse.next_int()?;

        Ok(Lindex {
            key,
            idx,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Lindex> {
        if argv.len() != 2 {
            return Ok(Lindex::new_invalid());
        }
        let key = &argv[0];
        let idx = match argv[1].parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Lindex::new_invalid()),
        };
        Ok(Lindex::new(key, idx))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lindex(None).await?;
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

    pub async fn lindex(self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn)
                .do_async_txnkv_lindex(&self.key, self.idx)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Lindex {
    fn new_invalid() -> Lindex {
        Lindex {
            key: "".to_owned(),
            idx: 0,
            valid: false,
        }
    }
}
