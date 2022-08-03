use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Zremrangebyrank {
    key: String,
    min: i64,
    max: i64,
    valid: bool,
}

impl Zremrangebyrank {
    pub fn new(key: &str, min: i64, max: i64) -> Zremrangebyrank {
        Zremrangebyrank {
            key: key.to_string(),
            min,
            max,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zremrangebyrank> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let z = Zremrangebyrank::new(&key, min, max);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zremrangebyrank> {
        if argv.len() != 3 {
            return Ok(Zremrangebyrank::new_invalid());
        }
        let min = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyrank::new_invalid()),
        };

        let max = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zremrangebyrank::new_invalid()),
        };

        Ok(Zremrangebyrank::new(
            &String::from_utf8_lossy(&argv[0]),
            min,
            max,
        ))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zremrangebyrank(None).await?;
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

    pub async fn zremrangebyrank(
        &self,
        txn: Option<Arc<Mutex<Transaction>>>,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zremrange_by_rank(&self.key, self.min, self.max)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zremrangebyrank {
    fn new_invalid() -> Zremrangebyrank {
        Zremrangebyrank {
            key: "".to_string(),
            min: 0,
            max: 0,
            valid: false,
        }
    }
}
