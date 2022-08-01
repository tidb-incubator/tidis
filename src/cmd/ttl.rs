use std::sync::Arc;

use crate::cmd::Invalid;
use crate::config::is_use_txn_api;
use crate::config::LOGGER;
use crate::tikv::errors::{AsyncResult, REDIS_NOT_SUPPORTED_ERR};
use crate::tikv::string::StringCommandCtx;
use crate::utils::{resp_err, resp_invalid_arguments};
use crate::{Connection, Frame, Parse};
use bytes::Bytes;
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct TTL {
    key: String,
    valid: bool,
}

impl TTL {
    pub fn new(key: impl ToString) -> TTL {
        TTL {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<TTL> {
        let key = parse.next_string()?;

        Ok(TTL { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<TTL> {
        if argv.len() != 1 {
            return Ok(TTL::new_invalid());
        }
        Ok(TTL {
            key: String::from_utf8_lossy(&argv[0]).to_string(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection, is_millis: bool) -> crate::Result<()> {
        let response = self.ttl(is_millis, None).await.unwrap_or_else(Into::into);

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

    pub async fn ttl(
        self,
        is_millis: bool,
        txn: Option<Arc<Mutex<Transaction>>>,
    ) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_ttl(&self.key, is_millis)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Invalid for TTL {
    fn new_invalid() -> TTL {
        TTL {
            key: "".to_owned(),
            valid: false,
        }
    }
}
