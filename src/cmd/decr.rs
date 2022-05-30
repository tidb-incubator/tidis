use std::sync::Arc;

use crate::config::{is_use_txn_api, LOGGER};
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Decr {
    key: String,
    valid: bool,
}

impl Decr {
    pub fn new(key: impl ToString) -> Decr {
        Decr {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn new_invalid() -> Decr {
        Decr {
            key: "".to_owned(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Decr> {
        let key = parse.next_string()?;

        Ok(Decr::new(key))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Decr> {
        if argv.len() != 1 {
            return Ok(Decr::new_invalid());
        }
        let key = &argv[0];
        Ok(Decr::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.decr(None).await.unwrap_or_else(Into::into);

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

    pub async fn decr(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        // check argument parse validation
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_incr(&self.key, true, -1)
                .await
        } else {
            StringCommandCtx::new(None)
                .do_async_rawkv_incr(&self.key, false, -1)
                .await
        }
    }
}
