use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::set::SetCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Srandmember {
    key: String,
    count: Option<i64>,
    valid: bool,
}

impl Srandmember {
    pub fn new(key: &str, count: Option<i64>) -> Srandmember {
        Srandmember {
            key: key.to_string(),
            count,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Srandmember> {
        let key = parse.next_string()?;

        let mut count = None;
        if let Ok(v) = parse.next_int() {
            count = Some(v);
        }
        Ok(Srandmember {
            key,
            count,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Srandmember> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Srandmember::new_invalid());
        }
        let mut count = None;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = Some(v),
                Err(_) => return Ok(Srandmember::new_invalid()),
            }
        }
        Ok(Srandmember::new(&String::from_utf8_lossy(&argv[0]), count))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.srandmember(None).await?;
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

    pub async fn srandmember(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            let mut count;
            let repeatable;
            let array_resp;
            if self.count.is_none() {
                repeatable = false;
                count = 1;
                array_resp = false;
            } else {
                array_resp = true;
                count = self.count.unwrap();
                if count > 0 {
                    repeatable = false;
                } else {
                    repeatable = true;
                    count = -count;
                }
            }
            SetCommandCtx::new(txn)
                .do_async_txnkv_srandmemeber(&self.key, count, repeatable, array_resp)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Srandmember {
    fn new_invalid() -> Srandmember {
        Srandmember {
            key: "".to_string(),
            count: None,
            valid: false,
        }
    }
}
