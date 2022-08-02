use std::sync::Arc;

use crate::config::LOGGER;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::cmd::Invalid;
use crate::config::is_use_txn_api;

#[derive(Debug, Clone)]
pub struct Strlen {
    key: String,
    valid: bool,
}

impl Strlen {
    pub fn new(key: impl ToString) -> Strlen {
        Strlen {
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Strlen> {
        let key = parse.next_string()?;

        Ok(Strlen { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Strlen> {
        if argv.len() != 1 {
            return Ok(Strlen::new_invalid());
        }
        let key = &String::from_utf8_lossy(&argv[0]);
        Ok(Strlen::new(key))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.strlen(None).await.unwrap_or_else(Into::into);

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

    pub async fn strlen(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }

        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_strlen(&self.key)
                .await
        } else {
            StringCommandCtx::new(None)
                .do_async_rawkv_strlen(&self.key)
                .await
        }
    }
}

impl Invalid for Strlen {
    fn new_invalid() -> Strlen {
        Strlen {
            key: "".to_owned(),
            valid: false,
        }
    }
}
