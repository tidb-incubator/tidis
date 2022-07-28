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

#[derive(Debug, Clone)]
pub struct Ltrim {
    key: String,
    start: i64,
    end: i64,
    valid: bool,
}

impl Ltrim {
    pub fn new(key: &str, start: i64, end: i64) -> Ltrim {
        Ltrim {
            key: key.to_owned(),
            start,
            end,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ltrim> {
        let key = parse.next_string()?;
        let start = parse.next_int()?;
        let end = parse.next_int()?;

        Ok(Ltrim {
            key,
            start,
            end,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Ltrim> {
        if argv.len() != 3 {
            return Ok(Ltrim::new_invalid());
        }
        let key = &argv[0];
        let start = match argv[1].parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Ltrim::new_invalid()),
        };

        let end = match argv[2].parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Ltrim::new_invalid()),
        };
        Ok(Ltrim::new(key, start, end))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.ltrim(None).await?;
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

    pub async fn ltrim(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn)
                .do_async_txnkv_ltrim(&self.key, self.start, self.end)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Ltrim {
    fn new_invalid() -> Ltrim {
        Ltrim {
            key: "".to_owned(),
            start: 0,
            end: 0,
            valid: false,
        }
    }
}
