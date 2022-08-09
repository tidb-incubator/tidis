use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};
use bytes::Bytes;
use slog::debug;
use std::convert::TryInto;
use std::sync::Arc;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::config::LOGGER;

#[derive(Debug, Clone)]
pub struct Scan {
    start: String,
    count: i64,
    valid: bool,
}

impl Scan {
    pub fn new(start: String, count: i64) -> Scan {
        Scan {
            start,
            count,
            valid: true,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scan> {
        let start = parse.next_string()?;
        let mut count = 10;
        if let Ok(flag) = parse.next_string() {
            if flag.to_uppercase().as_str() == "COUNT" {
                if let Ok(c) = parse.next_int() {
                    count = c;
                };
            }
        }

        Ok(Scan {
            start,
            count,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Scan> {
        if argv.len() < 1 || argv.len() > 3 || (argv.len() == 2 && argv[1] != Bytes::from("COUNT"))
        {
            return Ok(Scan::new_invalid());
        }

        let start = String::from_utf8_lossy(&argv[0]);
        let count = if let Ok(c) = String::from_utf8_lossy(&argv[2]).parse::<i64>() {
            c
        } else {
            10
        };

        Ok(Scan {
            start: start.to_string(),
            count,
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.scan(None).await?;
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

    pub async fn scan(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_scan(&self.start, self.count.try_into().unwrap())
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Scan {
    fn new_invalid() -> Scan {
        Scan {
            start: "".to_owned(),
            count: 0,
            valid: false,
        }
    }
}
