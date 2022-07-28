use std::sync::Arc;

use crate::cmd::Invalid;
use crate::config::is_use_txn_api;
use crate::config::LOGGER;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug, Clone)]
pub struct Mget {
    /// Name of the keys to get
    keys: Vec<String>,
    valid: bool,
}

impl Mget {
    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn add_key(&mut self, key: String) {
        self.keys.push(key);
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Mget> {
        // The `MGET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let mut mget = Mget::default();

        while let Ok(key) = parse.next_string() {
            mget.add_key(key);
        }

        Ok(mget)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Mget> {
        if argv.is_empty() {
            return Ok(Mget::new_invalid());
        }
        let mut mget = Mget::default();
        for arg in argv {
            mget.add_key(arg.to_string());
        }
        Ok(mget)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = self.batch_get(None).await.unwrap_or_else(Into::into);

        debug!(
            LOGGER,
            "res, {} -> {}, {:?}",
            dst.local_addr(),
            dst.peer_addr(),
            response
        );

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_get(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_batch_get(&self.keys)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_batch_get(&self.keys)
                .await
        }
    }
}

impl Default for Mget {
    /// Create a new `Mget` command which fetches `key` vector.
    fn default() -> Self {
        Mget {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mget {
    fn new_invalid() -> Mget {
        Mget {
            keys: vec![],
            valid: false,
        }
    }
}
