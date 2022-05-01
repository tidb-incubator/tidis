use std::sync::Arc;

use crate::tikv::errors::AsyncResult;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use crate::tikv::string::StringCommandCtx;
use crate::config::is_use_txn_api;
use tikv_client::Transaction;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Mget {
    /// Name of the keys to get
    keys: Vec<String>,
    valid: bool,
}

impl Mget {
    /// Create a new `Mget` command which fetches `key` vector.
    pub fn new() -> Mget {
        Mget {
            keys: vec![],
            valid: true,
        }
    }

    pub fn new_invalid() -> Mget {
        Mget {
            keys: vec![],
            valid: false,
        }
    }

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
        let mut mget = Mget::new();

        while let Ok(key) = parse.next_string() {
            mget.add_key(key);
        }

        Ok(mget)
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Mget> {
        if argv.len() == 0 {
            return Ok(Mget::new_invalid());
        }
        let mut mget = Mget::new();
        for arg in argv {
            mget.add_key(arg.to_string());
        }
        return Ok(mget);
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.batch_get(None).await {
            Ok(val) => val,
            Err(e) => Frame::Error(e.to_string()),
        };

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub async fn batch_get(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn).do_async_txnkv_batch_get(&self.keys).await
        } else {
            StringCommandCtx::new(txn).do_async_rawkv_batch_get(&self.keys).await
        }
    }
}