use std::sync::Arc;

use crate::config::LOGGER;
use crate::tikv::errors::AsyncResult;
use crate::tikv::string::StringCommandCtx;
use crate::utils::resp_invalid_arguments;
use crate::{Connection, Frame, Parse};
use slog::debug;
use tikv_client::Transaction;
use tokio::sync::Mutex;

use crate::config::is_use_txn_api;

use super::Invalid;

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: String,
    valid: bool,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        Ok(Get { key, valid: true })
    }

    pub(crate) fn parse_argv(argv: &Vec<String>) -> crate::Result<Get> {
        if argv.len() != 1 {
            return Ok(Get::new_invalid());
        }
        let key = &argv[0];
        Ok(Get::new(key))
    }

    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        // Get the value from the shared database state
        let response = self.get(None).await.unwrap_or_else(Into::into);

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

    pub async fn get(&self, txn: Option<Arc<Mutex<Transaction>>>) -> AsyncResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        if is_use_txn_api() {
            StringCommandCtx::new(txn)
                .do_async_txnkv_get(&self.key)
                .await
        } else {
            StringCommandCtx::new(txn)
                .do_async_rawkv_get(&self.key)
                .await
        }
    }
}

impl Invalid for Get {
    fn new_invalid() -> Get {
        Get {
            key: "".to_owned(),
            valid: false,
        }
    }
}
