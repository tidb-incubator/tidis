use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use crate::tikv::client::Transaction;
use slog::debug;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Spop {
    key: String,
    count: i64,
    valid: bool,
}

impl Spop {
    pub fn new(key: &str, count: i64) -> Spop {
        Spop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Spop> {
        let key = parse.next_string()?;

        let mut count = 1;
        if let Ok(v) = parse.next_int() {
            count = v;
        }
        Ok(Spop {
            key,
            count,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Spop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Spop::new_invalid());
        }
        let mut count = 1;
        if argv.len() == 2 {
            match argv[1].parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => return Ok(Spop::new_invalid()),
            }
        }
        Ok(Spop::new(&argv[0], count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.spop(None).await?;
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

    pub async fn spop(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            SetCommandCtx::new(txn)
                .do_async_txnkv_spop(&self.key, self.count as u64)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Spop {
    fn new_invalid() -> Spop {
        Spop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }
}
