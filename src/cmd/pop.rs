use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::list::ListCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Pop {
    key: String,
    count: i64,
    valid: bool,
}

impl Pop {
    pub fn new(key: &str, count: i64) -> Pop {
        Pop {
            key: key.to_owned(),
            count,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Pop> {
        if argv.is_empty() || argv.len() > 2 {
            return Ok(Pop::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        let mut count = 1;
        if argv.len() == 2 {
            match String::from_utf8_lossy(&argv[1]).parse::<i64>() {
                Ok(v) => count = v,
                Err(_) => {
                    return Ok(Pop::new_invalid());
                }
            }
        }
        Ok(Pop::new(key, count))
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pop> {
        let key = parse.next_string()?;
        let mut count = 1;

        if let Ok(n) = parse.next_int() {
            count = n;
        }

        let pop = Pop::new(&key, count);

        Ok(pop)
    }

    pub(crate) async fn apply(self, dst: &mut Connection, op_left: bool) -> crate::Result<()> {
        let response = self.pop(None, op_left).await?;
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

    pub async fn pop(
        &self,
        txn: Option<Arc<Mutex<Transaction>>>,
        op_left: bool,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn)
                .do_async_txnkv_pop(&self.key, op_left, self.count)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Pop {
    fn new_invalid() -> Pop {
        Pop {
            key: "".to_owned(),
            count: 0,
            valid: false,
        }
    }
}
