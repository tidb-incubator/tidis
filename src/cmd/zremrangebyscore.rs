use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use crate::tikv::client::Transaction;
use slog::debug;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zremrangebyscore {
    key: String,
    min: f64,
    max: f64,
    valid: bool,
}

impl Zremrangebyscore {
    pub fn new(key: &str, min: f64, max: f64) -> Zremrangebyscore {
        Zremrangebyscore {
            key: key.to_string(),
            min,
            max,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyscore> {
        let key = parse.next_string()?;

        // TODO support (/-inf/+inf
        let min = parse.next_string()?.parse::<f64>()?;
        let max = parse.next_string()?.parse::<f64>()?;

        let z = Zremrangebyscore::new(&key, min, max);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zremrangebyscore> {
        if argv.len() != 3 {
            return Ok(Zremrangebyscore::new_invalid());
        }
        // TODO
        let min = match argv[1].parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid()),
        };

        let max = match argv[2].parse::<f64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyscore::new_invalid()),
        };

        Ok(Zremrangebyscore::new(&argv[0], min, max))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyscore(None).await?;
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

    pub async fn zremrangebyscore(
        &self,
        txn: Option<Arc<Mutex<Transaction>>>,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zremrange_by_score(&self.key, self.min, self.max)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zremrangebyscore {
    fn new_invalid() -> Zremrangebyscore {
        Zremrangebyscore {
            key: "".to_string(),
            min: 0f64,
            max: 0f64,
            valid: false,
        }
    }
}
