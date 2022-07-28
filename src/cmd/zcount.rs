use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::zset::ZsetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::{Buf, Bytes};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Zcount {
    key: String,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
    valid: bool,
}

impl Zcount {
    pub fn new(key: &str, min: f64, min_inclusive: bool, max: f64, max_inclusive: bool) -> Zcount {
        Zcount {
            key: key.to_string(),
            min,
            min_inclusive,
            max,
            max_inclusive,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zcount> {
        let key = parse.next_string()?;
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = parse.next_bytes()?;
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        }
        let min = String::from_utf8_lossy(&bmin.to_vec())
            .parse::<f64>()
            .unwrap();

        let mut bmax = parse.next_bytes()?;
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        }
        let max = String::from_utf8_lossy(&bmax.to_vec())
            .parse::<f64>()
            .unwrap();

        let z = Zcount::new(&key, min, min_inclusive, max, max_inclusive);

        Ok(z)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Zcount> {
        if argv.len() < 3 {
            return Ok(Zcount::new_invalid());
        }
        let mut min_inclusive = true;
        let mut max_inclusive = true;

        // parse score range as bytes, to handle exclusive bounder
        let mut bmin = Bytes::from(argv[1].clone());
        // check first byte
        if bmin[0] == b'(' {
            // drain the first byte
            bmin.advance(1);
            min_inclusive = false;
        }
        let min = String::from_utf8_lossy(&bmin.to_vec())
            .parse::<f64>()
            .unwrap();

        let mut bmax = Bytes::from(argv[2].clone());
        if bmax[0] == b'(' {
            bmax.advance(1);
            max_inclusive = false;
        }
        let max = String::from_utf8_lossy(&bmax.to_vec())
            .parse::<f64>()
            .unwrap();

        let z = Zcount::new(&argv[0], min, min_inclusive, max, max_inclusive);
        Ok(z)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.zcount(None).await?;
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

    pub async fn zcount(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ZsetCommandCtx::new(txn)
                .do_async_txnkv_zcount(
                    &self.key,
                    self.min,
                    self.min_inclusive,
                    self.max,
                    self.max_inclusive,
                )
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Zcount {
    fn new_invalid() -> Zcount {
        Zcount {
            key: "".to_string(),
            min: 0f64,
            min_inclusive: false,
            max: 0f64,
            max_inclusive: false,
            valid: false,
        }
    }
}
