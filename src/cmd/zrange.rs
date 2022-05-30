use std::sync::Arc;

use crate::cmd::Parse;
use crate::config::is_use_txn_api;
use crate::tikv::errors::AsyncResult;
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Zrange {
    key: String,
    min: i64,
    max: i64,
    withscores: bool,
    reverse: bool,
    valid: bool,
}

impl Zrange {
    pub fn new(key: &str, min: i64, max: i64, withscores: bool, reverse: bool) -> Zrange {
        Zrange {
            key: key.to_string(),
            min,
            max,
            withscores,
            reverse,
            valid: true,
        }
    }

    pub fn new_invalid() -> Zrange {
        Zrange {
            key: "".to_string(),
            min: 0,
            max: 0,
            withscores: false,
            reverse: false,
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrange> {
        let key = parse.next_string()?;

        let min = parse.next_int()?;
        let max = parse.next_int()?;

        let mut withscores = false;
        let mut reverse = false;

        // try to parse other flags
        while let Ok(v) = parse.next_string() {
            match v.to_uppercase().as_str() {
                // flags implement in single command, such as ZRANGEBYSCORE
                "BYSCORE" => {}
                "BYLEX" => {}
                "REV" => {
                    reverse = true;
                }
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }

        let z = Zrange::new(&key, min, max, withscores, reverse);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zrange> {
        if argv.len() < 3 {
            return Ok(Zrange::new_invalid());
        }
        let min = match argv[1].parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrange::new_invalid()),
        };
        let max = match argv[2].parse::<i64>() {
            Ok(v) => v,
            Err(_) => return Ok(Zrange::new_invalid()),
        };
        let mut withscores = false;
        let mut reverse = false;

        for arg in &argv[2..] {
            match arg.to_uppercase().as_str() {
                // flags implement in single command, such as ZRANGEBYSCORE
                "BYSCORE" => {}
                "BYLEX" => {}
                "REV" => {
                    reverse = true;
                }
                "LIMIT" => {}
                "WITHSCORES" => {
                    withscores = true;
                }
                _ => {}
            }
        }
        let z = Zrange::new(&argv[0], min, max, withscores, reverse);

        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zrange(None).await?;
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

    pub async fn zrange(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zrange(&self.key, self.min, self.max, self.withscores, self.reverse)
                .await
        } else {
            Ok(resp_err("not supported yet"))
        }
    }
}
