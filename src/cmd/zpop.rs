use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zpop {
    key: String,
    count: i64,
    valid: bool,
}

impl Zpop {
    pub fn new(key: &str, count: i64) -> Zpop {
        Zpop {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub fn new_invalid() -> Zpop {
        Zpop {
            key: "".to_string(),
            count: 0,
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zpop> {
        let key = parse.next_string()?;
        // default count is 1
        let mut count = 1;
        if let Ok(c) = parse.next_int() {
            count = c;
        }
        Ok(Zpop {
            key,
            count,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zpop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Zpop::new_invalid());
        }
        let mut count = 1;
        if argv.len() == 2 {
            match argv[1].parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => return Ok(Zpop::new_invalid()),
            }
        }
        Ok(Zpop::new(&argv[0], count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection, from_min: bool) -> crate::Result<()> {
        let response = self.zpop(None, from_min).await?;
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

    pub async fn zpop(
        &self,
        txn: Option<Arc<Mutex<Transaction>>>,
        from_min: bool,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zpop(&self.key, from_min, self.count as u64)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}
