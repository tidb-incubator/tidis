use std::sync::Arc;

use crate::cmd::Invalid;
use crate::config::is_use_txn_api;
use crate::config::LOGGER;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Persist {
    key: String,
    valid: bool,
}

impl Persist {
    pub fn new(key: impl ToString) -> Persist {
        Persist {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Persist> {
        let key = parse.next_string()?;

        Ok(Persist { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Persist> {
        if argv.len() != 1 {
            return Ok(Persist::new_invalid());
        }
        Ok(Persist {
            key: argv[0].to_owned(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.persist(None).await.unwrap_or_else(Into::into);

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

    pub async fn persist(self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_expire(&self.key, 0)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for Persist {
    fn new_invalid() -> Persist {
        Persist {
            key: "".to_owned(),
            valid: false,
        }
    }
}
