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
            min: min,
            max: max,
            withscores: withscores,
            valid: true,
        }
    }

    pub fn new_invalid() -> Zrevrange {
        Zrevrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrevrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                "WITHSCORES" => {
                    withscores = true;
                },
                _ => {}
            }
        }

        let z = Zrevrange::new(&key, min, max, withscores);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zrevrange> {
        if argv.len() < 3 {
            return Ok(Zrevrange::new_invalid());
        }
        let min;
        let max;
        match argv[1].parse::<i64>() {
            Ok(v) => min = v,
            Err(_) => return Ok(Zrevrange::new_invalid())
        }
        match argv[2].parse::<i64>() {
            Ok(v) => max = v,
            Err(_) => return Ok(Zrevrange::new_invalid())
        }
        let mut withscores = false;

        for arg in &argv[2..] {
            match arg.to_uppercase().as_str() {
                // flags implement in signle command, such as ZRANGEBYSCORE
                "WITHSCORES" => {
                    withscores = true;
                },
                _ => {}
            }
        }
        let z = Zrevrange::new(&argv[0], min, max, withscores);

        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrevrange(None).await?;
        debug!(LOGGER, "res, {} -> {}, {:?}", dst.local_addr(), dst.peer_addr(), response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn zrevrange(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn).do_async_txnkv_zrange(&self.key, self.min, self.max, self.withscores, true).await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
