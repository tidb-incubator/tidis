use std::sync::Arc;

use crate::cmd::{Parse};
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::{Connection, Frame};
use crate::config::{is_use_txn_api};
use crate::utils::{resp_err, resp_invalid_arguments};

use tikv_client::Transaction;
use tokio::sync::Mutex;
use crate::config::LOGGER;
use slog::debug;

#[derive(Debug)]
pub struct Zremrangebyscore {
    key: String,
    min: i64,
    max: i64,
    valid: bool,
}

impl Zremrangebyscore {
    pub fn new(key: &str, min: i64, max: i64) -> Zremrangebyscore {
        Zremrangebyscore {
            key: key.to_string(),
            min: min,
            max: max,
            valid: true,
        }
    }

    pub fn new_invalid() -> Zremrangebyscore {
        Zremrangebyscore {
            key: "".to_string(),
            min: 0,
            max: 0,
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyscore> {
        let key = parse.next_string()?;

        // TODO support (/-inf/+inf
        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let z = Zremrangebyscore::new(&key, min, max);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zremrangebyscore> {
        if argv.len() != 3 {
            return Ok(Zremrangebyscore::new_invalid());
        }
        // TODO
        let min;
        let max;

        match argv[1].parse::<i64>() {
            Ok(v) => min = v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid())
        }

        match argv[2].parse::<i64>() {
            Ok(v) => max = v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid())
        }

        Ok(Zremrangebyscore::new(&argv[0], min, max))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyscore(None).await?;
        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zremrangebyscore(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn).do_async_txnkv_zremrange_by_score(&self.key, self.min, self.max).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
