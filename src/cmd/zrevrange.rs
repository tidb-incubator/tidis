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
pub struct Zrevrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    valid: bool,
}

impl Zrevrange {
    pub fn new(key: &str, min: i64, max: i64, withscores: bool) -> Zrevrange {
        Zrevrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrevrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            if v.to_uppercase().as_str() == "WITHSCORES" {
                withscores = true;
            }
        }

        let z = Zrevrange::new(&key, min, max, withscores);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Zrevrange> {
        if argv.len() < 3 {
            return Ok(Zrevrange::new_invalid());
        }
        let min = match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrevrange::new_invalid()),
        };
        let max = match String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrevrange::new_invalid()),
        };
        let mut withscores = false;

        for arg in &argv[2..] {
            // flags implement in single command, such as ZRANGEBYSCORE
            if String::from_utf8_lossy(arg).to_uppercase().as_str() == "WITHSCORES" {
                withscores = true;
            }
        }
        let z = Zrevrange::new(&String::from_utf8_lossy(&argv[0]), min, max, withscores);

        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrevrange(None).await?;
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

    pub async fn zrevrange(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zrange(&self.key, self.min, self.max, self.withscores, true)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zrevrange {
    fn new_invalid() -> Zrevrange {
        Zrevrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            valid: false,
        }
    }
}
