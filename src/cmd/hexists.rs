use std::sync::Arc;

use crate::cmd::{Invalid, Parse};
use crate::config::is_use_txn_api;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::hash::HashCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame};

use crate::config::LOGGER;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Hexists {
    key: String,
    field: String,
    valid: bool,
}

impl Hexists {
    pub fn new(key: &str, field: &str) -> Hexists {
        Hexists {
            field: field.to_owned(),
            key: key.to_owned(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Hexists> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(Hexists::new(&key, &field))
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Hexists> {
        if argv.len() != 2 {
            return Ok(Hexists::new_invalid());
        }
        Ok(Hexists::new(&argv[0], &argv[1]))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.hexists(None).await?;
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

    pub async fn hexists(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            HashCommandCtx::new(txn)
                .do_async_txnkv_hexists(&self.key, &self.field)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Hexists {
    fn new_invalid() -> Hexists {
        Hexists {
            field: "".to_owned(),
            key: "".to_owned(),
            valid: false,
        }
    }
}
