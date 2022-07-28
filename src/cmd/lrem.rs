use std::sync::Arc;

use crate::cmd::Parse;
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

use super::Invalid;

#[derive(Debug, Clone)]
pub struct Lrem {
    key: String,
    count: i64,
    element: Bytes,
    valid: bool,
}

impl Lrem {
    pub fn new(key: &str, count: i64, element: Bytes) -> Lrem {
        Lrem {
            key: key.to_owned(),
            count,
            element,
            valid: true,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Lrem> {
        let key = parse.next_string()?;
        let count = parse.next_int()?;
        let element = parse.next_bytes()?;

        Ok(Lrem {
            key,
            count,
            element,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Lrem> {
        if argv.len() != 3 {
            return Ok(Lrem::new_invalid());
        }
        let key = &argv[0];
        let count = argv[1].parse::<i64>()?;

        let element = Bytes::from(argv[2].clone());
        Ok(Lrem::new(key, count, element))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.lrem(None).await?;
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

    pub async fn lrem(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            let mut from_head = true;
            let mut count = self.count;
            if self.count < 0 {
                from_head = false;
                count = -count;
            }
            ListCommandCtx::new(txn)
                .do_async_txnkv_lrem(&self.key, count as usize, from_head, &self.element)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Lrem {
    fn new_invalid() -> Lrem {
        Lrem {
            key: "".to_owned(),
            count: 0,
            element: Bytes::new(),
            valid: false,
        }
    }
}
