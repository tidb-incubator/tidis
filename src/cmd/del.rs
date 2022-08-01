use std::sync::Arc;

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

use super::Invalid;

#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
    valid: bool,
}

impl Del {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut del = Del::default();
        while let Ok(key) = parse.next_string() {
            del.add_key(key);
        }

        Ok(del)
    }

    pub(crate) fn parse_argv(argv: &Vec<Bytes>) -> crate::Result<Del> {
        if argv.is_empty() {
            return Ok(Del {
                keys: vec![],
                valid: false,
            });
        }
        Ok(Del {
            keys: argv
                .iter()
                .map(|x| String::from_utf8_lossy(x).to_string())
                .collect::<Vec<String>>(),
            valid: true,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.del(None).await.unwrap_or_else(Into::into);

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

    pub async fn del(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_del(&self.keys)
                .await
        } else {
            Ok(resp_err(REDIS_NOT_SUPPORTED_ERR))
        }
    }
}

impl Default for Del {
    /// Create a new `Del` command which fetches `key` vector.
    fn default() -> Self {
        Del {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Del {
    fn new_invalid() -> Self {
        Del {
            keys: vec![],
            valid: false,
        }
    }
}
