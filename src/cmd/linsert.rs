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

#[derive(Debug)]
pub struct Linsert {
    key: String,
    before_pivot: bool,
    pivot: Bytes,
    element: Bytes,
    valid: bool,
}

impl Linsert {
    pub fn new(key: &str, before_pivot: bool, pivot: Bytes, element: Bytes) -> Linsert {
        Linsert {
            key: key.to_owned(),
            before_pivot,
            pivot,
            element,
            valid: true,
        }
    }

    pub fn new_invalid() -> Linsert {
        Linsert {
            key: "".to_owned(),
            before_pivot: false,
            pivot: Bytes::new(),
            element: Bytes::new(),
            valid: false,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Linsert> {
        let key = parse.next_string()?;
        let pos = parse.next_string()?;
        let before_pivot = match pos.to_lowercase().as_str() {
            "before" => true,
            "after" => false,
            _ => {
                return Ok(Linsert::new_invalid());
            }
        };
        let pivot = parse.next_bytes()?;
        let element = parse.next_bytes()?;

        Ok(Linsert {
            key,
            before_pivot,
            pivot,
            element,
            valid: true,
        })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Linsert> {
        if argv.len() != 4 {
            return Ok(Linsert::new_invalid());
        }
        let key = &argv[0];
        let before_pivot = match argv[1].to_lowercase().as_str() {
            "before" => true,
            "after" => false,
            _ => {
                return Ok(Linsert::new_invalid());
            }
        };

        let pivot = Bytes::from(argv[2].clone());
        let element = Bytes::from(argv[3].clone());
        Ok(Linsert::new(key, before_pivot, pivot, element))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.linsert(None).await?;
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

    pub async fn linsert(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            ListCommandCtx::new(txn)
                .do_async_txnkv_linsert(&self.key, self.before_pivot, &self.pivot, &self.element)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}
